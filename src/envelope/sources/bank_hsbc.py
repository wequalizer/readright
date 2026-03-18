"""HSBC CSV export parser (UK/international)."""

from __future__ import annotations

import csv
import io
from datetime import date
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# HSBC UK CSV headers — extremely minimal
HSBC_HEADERS_UK = {"Date", "Description", "Amount"}

# HSBC sometimes includes a 'Balance' column
HSBC_HEADERS_WITH_BALANCE = {"Date", "Description", "Amount", "Balance"}

# HSBC also has an older variant with 'Paid out' and 'Paid in' split columns
HSBC_HEADERS_SPLIT = {"Date", "Description", "Paid out", "Paid in", "Balance"}


class HSBCBankParser(BaseParser):
    """Parser for HSBC CSV exports (UK and international).

    HSBC CSV quirks:
    - Extremely minimal format — sometimes only 3 columns
    - Date format is DD/MM/YYYY (UK/international standard)
    - Some HSBC export variants split debits and credits into 'Paid out' / 'Paid in'
      rather than a single signed Amount column
    - Amount may be signed (negative=debit) OR the file may use separate Paid out/Paid in columns
    - 'Paid out' amounts are typically positive numbers representing money leaving the account
    - Encoding is typically UTF-8 with BOM or latin-1
    - Some exports include a 'Balance' column; others do not
    - HSBC exports may include a header summary block before the data — handled via header scanning
    - Currency is GBP for UK accounts; other currencies possible for international accounts
    """

    def source_type(self) -> str:
        return "hsbc_csv"

    def source_label(self) -> str:
        return "HSBC CSV Export (UK/International)"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        text = text.lstrip("\ufeff")

        # Scan first 10 lines for a recognisable HSBC header
        lines = text.split("\n")
        for line in lines[:10]:
            stripped = line.strip()
            if not stripped:
                continue
            headers = {h.strip().strip('"') for h in stripped.split(",")}

            if HSBC_HEADERS_SPLIT == headers:
                return 0.93  # Split paid-out/paid-in is distinctively HSBC
            if HSBC_HEADERS_WITH_BALANCE == headers:
                return 0.72  # Plausible but generic
            if HSBC_HEADERS_UK == headers:
                # Exact 3-column match: Date, Description, Amount.
                # Very minimal — check data rows for DD/MM/YYYY date pattern to strengthen confidence.
                for data_line in lines[1:5]:
                    data_line = data_line.strip()
                    if not data_line:
                        continue
                    parts = data_line.split(",")
                    if len(parts) >= 1:
                        candidate = parts[0].strip().strip('"')
                        # DD/MM/YYYY: day 01-31 as first two chars
                        if (len(candidate) == 10 and candidate[2] == "/" and candidate[5] == "/"
                                and candidate[:2].isdigit() and int(candidate[:2]) <= 31):
                            return 0.72  # DD/MM/YYYY date confirms non-US bank
                return 0.55  # Header matches but no strong date evidence

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(
                    name="date",
                    dtype="date",
                    description="Transaction date in UK/international format",
                    format="YYYY-MM-DD",
                ),
                FieldAnnotation(
                    name="amount",
                    dtype="decimal",
                    description="Signed transaction amount: negative = debit (money out), positive = credit (money in). Derived from Amount column or Paid out/Paid in columns.",
                    unit="GBP",
                    format="dot_decimal",
                    examples=["-120.00", "2500.00", "-45.99"],
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
                    description="Transaction description or merchant name",
                    examples=["CONTACTLESS PAYMENT", "DIRECT CREDIT SALARY", "STANDING ORDER"],
                ),
                FieldAnnotation(
                    name="balance",
                    dtype="decimal",
                    description="Account balance after this transaction (only in some HSBC export variants)",
                    unit="GBP",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="paid_out",
                    dtype="decimal",
                    description="Gross amount paid out (debit), as a positive number. Only populated in split-column HSBC exports.",
                    unit="GBP",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="paid_in",
                    dtype="decimal",
                    description="Gross amount paid in (credit), as a positive number. Only populated in split-column HSBC exports.",
                    unit="GBP",
                    nullable=True,
                ),
            ],
            conventions=[
                "Date format is DD/MM/YYYY (UK format). Normalized to YYYY-MM-DD. Do NOT assume MM/DD/YYYY.",
                "HSBC has two export variants: (1) single signed Amount column, (2) separate 'Paid out' / 'Paid in' columns. Both are normalized to a single signed amount.",
                "In the split-column variant, 'Paid out' amounts are positive numbers representing money LEAVING the account. They are negated to produce the signed amount.",
                "Currency defaults to GBP. HSBC international accounts may export other currencies but the file contains no currency column.",
                "The 'Balance' column is not present in all HSBC export variants.",
                "Some HSBC exports include account summary lines at the top of the file — these are skipped.",
            ],
            notes=[
                "Due to the minimal format (only 3 columns in some variants), HSBC detection relies partly on the DD/MM/YYYY date format and the absence of columns found in other UK banks.",
                "HSBC business accounts may use a different export format.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        text = text.lstrip("\ufeff")

        # Find the actual header row — HSBC sometimes has summary metadata before it
        lines = text.split("\n")
        header_idx = self._find_header_row(lines)
        if header_idx is None:
            return ParseResult(success=False, error="Could not find HSBC header row in file")

        csv_text = "\n".join(lines[header_idx:])
        reader = csv.DictReader(io.StringIO(csv_text))
        if not reader.fieldnames:
            return ParseResult(success=False, error="No CSV headers found")

        fieldnames = {f.strip().strip('"') for f in reader.fieldnames}
        is_split_format = "Paid out" in fieldnames and "Paid in" in fieldnames

        rows = []
        warnings = []

        for i, row in enumerate(reader):
            try:
                parsed = self._parse_row(row, is_split_format)
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

    def _find_header_row(self, lines: list[str]) -> int | None:
        """Find the index of the actual CSV header row."""
        for i, line in enumerate(lines[:15]):
            stripped = line.strip()
            if not stripped:
                continue
            headers = {h.strip().strip('"') for h in stripped.split(",")}
            if "Date" in headers and "Description" in headers:
                if "Amount" in headers or "Paid out" in headers:
                    return i
        return None

    def _parse_row(self, row: dict, is_split_format: bool) -> dict:
        # Normalize header names: strip quotes and whitespace
        row = {k.strip().strip('"'): v.strip().strip('"') for k, v in row.items() if k}

        # Date: DD/MM/YYYY
        raw_date = row.get("Date", "")
        tx_date = self._parse_date(raw_date)

        if is_split_format:
            # Paid out = positive debit, Paid in = positive credit
            raw_out = row.get("Paid out", "").replace(",", "")
            raw_in = row.get("Paid in", "").replace(",", "")
            try:
                paid_out = Decimal(raw_out) if raw_out else None
            except InvalidOperation:
                paid_out = None
            try:
                paid_in = Decimal(raw_in) if raw_in else None
            except InvalidOperation:
                paid_in = None

            if paid_out is not None and paid_out != 0:
                amount = -abs(paid_out)
                is_debit = True
            elif paid_in is not None and paid_in != 0:
                amount = abs(paid_in)
                is_debit = False
            else:
                amount = Decimal("0")
                is_debit = False
        else:
            raw_amount = row.get("Amount", "0").replace(",", "")
            try:
                amount = Decimal(raw_amount)
            except InvalidOperation:
                amount = Decimal("0")
            is_debit = amount < 0
            paid_out = abs(amount) if is_debit else None
            paid_in = amount if not is_debit else None

        # Balance (optional)
        raw_balance = row.get("Balance", "").replace(",", "")
        try:
            balance = Decimal(raw_balance) if raw_balance else None
        except InvalidOperation:
            balance = None

        return {
            "date": str(tx_date),
            "amount": str(amount),
            "currency": "GBP",
            "direction": "debit" if is_debit else "credit",
            "description": row.get("Description", ""),
            "balance": str(balance) if balance is not None else None,
            "paid_out": str(paid_out) if paid_out is not None else None,
            "paid_in": str(paid_in) if paid_in is not None else None,
        }

    def _parse_date(self, raw: str) -> date | str:
        """Parse DD/MM/YYYY (UK format) into date."""
        raw = raw.strip()
        if not raw:
            return raw
        if len(raw) == 10 and raw[2] == "/" and raw[5] == "/":
            try:
                # UK format: DD/MM/YYYY
                return date(int(raw[6:10]), int(raw[3:5]), int(raw[0:2]))
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


registry.register(HSBCBankParser())
