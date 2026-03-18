"""Bank of America (US) CSV export parser."""

from __future__ import annotations

import csv
import io
from datetime import date
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# BofA exports have a very minimal fixed header row
BOFA_HEADERS = {"Date", "Description", "Amount", "Running Bal."}

# BofA sometimes prepends metadata rows before the actual CSV header
# e.g., lines like "Account Number: xxxxxx" before the column headers
METADATA_PREFIXES = (
    "account number",
    "account name",
    "account type",
    "begin balance",
    "end balance",
    "begin date",
    "end date",
    "symbol",
    "description",
)


class BofABankParser(BaseParser):
    """Parser for Bank of America CSV exports.

    BofA CSV quirks:
    - Extremely minimal format: only 4 columns
    - May have metadata lines BEFORE the actual header row
    - Date format is MM/DD/YYYY
    - Amount is already signed: negative = debit, positive = credit
    - 'Running Bal.' column name has a period and space — unusual
    - Encoding is typically UTF-8 with BOM
    - Some account types (savings, checking) produce identical format
    - Credit card exports look the same but currency is always USD
    - No counterparty IBAN or account info — very stripped-down
    """

    def source_type(self) -> str:
        return "bofa_csv_us"

    def source_label(self) -> str:
        return "Bank of America CSV Export (US)"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        text = text.lstrip("\ufeff")

        # Scan through first 10 lines looking for the header row
        lines = text.split("\n")
        for line in lines[:10]:
            stripped = line.strip()
            if not stripped:
                continue
            headers = {h.strip().strip('"') for h in stripped.split(",")}
            if BOFA_HEADERS.issubset(headers):
                return 0.95
            # Running Bal. is a very distinctive BofA field name
            if "Running Bal." in stripped and "Description" in stripped and "Amount" in stripped:
                return 0.90

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(
                    name="date",
                    dtype="date",
                    description="Transaction date",
                    format="YYYY-MM-DD",
                ),
                FieldAnnotation(
                    name="amount",
                    dtype="decimal",
                    description="Signed transaction amount: negative = debit (money out), positive = credit (money in)",
                    unit="USD",
                    format="dot_decimal",
                    examples=["-52.00", "2500.00", "-9.99"],
                ),
                FieldAnnotation(
                    name="direction",
                    dtype="enum",
                    description="Derived from sign of amount: debit = money out, credit = money in",
                    enum_values=["debit", "credit"],
                ),
                FieldAnnotation(
                    name="description",
                    dtype="string",
                    description="Merchant name or transaction description as provided by BofA",
                    examples=["ONLINE TRANSFER TO CHK", "ZELLE PAYMENT FROM", "WALMART #1234"],
                ),
                FieldAnnotation(
                    name="running_balance",
                    dtype="decimal",
                    description="Running account balance after this transaction",
                    unit="USD",
                    nullable=True,
                ),
            ],
            conventions=[
                "Amount is ALREADY signed: negative = money out (debit), positive = money in (credit).",
                "Date format is MM/DD/YYYY in the raw file. Normalized to YYYY-MM-DD.",
                "Currency is always USD. No currency column in the export.",
                "BofA exports may prepend several metadata lines (account number, dates, balances) before the CSV header. These are skipped during parsing.",
                "The 'Running Bal.' column name includes a period — this is intentional in BofA's format.",
                "No counterparty or IBAN information is provided in BofA exports — description is the only identifier.",
                "Merchant description strings are often truncated and may include BofA's internal reference codes.",
            ],
            notes=[
                "This is one of the most minimal US bank CSV formats. Enrichment should come from description parsing.",
                "BofA credit card exports use the same format but charges appear as negative amounts.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        text = text.lstrip("\ufeff")

        # Find the actual header row — skip metadata lines
        lines = text.split("\n")
        header_idx = None
        for i, line in enumerate(lines):
            stripped = line.strip()
            if not stripped:
                continue
            headers = {h.strip().strip('"') for h in stripped.split(",")}
            if "Running Bal." in headers and "Amount" in headers and "Date" in headers:
                header_idx = i
                break
            # Check if this looks like a metadata line to skip
            lower = stripped.lower()
            if any(lower.startswith(p) for p in METADATA_PREFIXES):
                continue

        if header_idx is None:
            return ParseResult(success=False, error="Could not find BofA header row in file")

        # Rebuild CSV from the header line onward
        csv_text = "\n".join(lines[header_idx:])

        reader = csv.DictReader(io.StringIO(csv_text))
        if not reader.fieldnames:
            return ParseResult(success=False, error="No CSV headers found")

        rows = []
        warnings = []

        for i, row in enumerate(reader):
            try:
                parsed = self._parse_row(row)
                rows.append(parsed)
            except Exception as e:
                warnings.append(f"Row {i + 1}: {e}")

        if not rows:
            return ParseResult(success=False, error="No rows could be parsed")

        envelope = ContextEnvelope(
            schema=self.schema(),
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _parse_row(self, row: dict) -> dict:
        # Date: MM/DD/YYYY
        raw_date = row.get("Date", "").strip().strip('"')
        tx_date = self._parse_date(raw_date)

        # Amount: signed, dot decimal, may have commas as thousand separators
        raw_amount = row.get("Amount", "0").strip().strip('"').replace(",", "")
        try:
            amount = Decimal(raw_amount)
        except InvalidOperation:
            amount = Decimal("0")

        is_debit = amount < 0

        # Running balance
        raw_balance = row.get("Running Bal.", "").strip().strip('"').replace(",", "")
        try:
            balance = Decimal(raw_balance) if raw_balance else None
        except InvalidOperation:
            balance = None

        return {
            "date": str(tx_date),
            "amount": str(amount),
            "currency": "USD",
            "direction": "debit" if is_debit else "credit",
            "description": row.get("Description", "").strip().strip('"'),
            "running_balance": str(balance) if balance is not None else None,
        }

    def _parse_date(self, raw: str) -> date | str:
        """Parse MM/DD/YYYY into date."""
        raw = raw.strip()
        if not raw:
            return raw
        if len(raw) == 10 and raw[2] == "/" and raw[5] == "/":
            try:
                return date(int(raw[6:10]), int(raw[0:2]), int(raw[3:5]))
            except ValueError:
                pass
        return raw

    def _decode(self, content: bytes) -> str | None:
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(BofABankParser())
