"""Chase Bank (US) CSV export parser."""

from __future__ import annotations

import csv
import io
from datetime import date
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Chase CSV headers (fixed, always English)
CHASE_HEADERS = {"Details", "Posting Date", "Description", "Amount", "Type", "Balance", "Check or Slip #"}

# Minimum required to confidently detect
DETECT_REQUIRED = {"Posting Date", "Description", "Amount", "Type", "Balance"}


class ChaseBankParser(BaseParser):
    """Parser for Chase Bank (US) CSV exports.

    Chase CSV quirks:
    - Headers are always in English, fixed column order
    - Date format is MM/DD/YYYY
    - Amount is already signed: negative = debit (money out), positive = credit (money in)
    - 'Type' column values include DEBIT, CREDIT, CHECK, ATM, etc.
    - 'Details' column often duplicates 'Type' but may have sub-type info
    - 'Check or Slip #' is empty for non-check transactions
    - Balance reflects balance AFTER the transaction
    - Some Chase exports omit 'Details' or 'Check or Slip #'
    - Encoding is UTF-8, sometimes with BOM
    """

    def source_type(self) -> str:
        return "chase_csv_us"

    def source_label(self) -> str:
        return "Chase Bank CSV Export (US)"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        first_line = text.lstrip("\ufeff").split("\n")[0].strip()
        headers = {h.strip().strip('"') for h in first_line.split(",")}

        if CHASE_HEADERS.issubset(headers):
            return 0.95

        if DETECT_REQUIRED.issubset(headers):
            return 0.85

        # Looser check: key combination unlikely to appear in other US bank exports
        if "Posting Date" in first_line and "Check or Slip #" in first_line:
            return 0.80

        if "Posting Date" in first_line and "Type" in first_line and "Balance" in first_line:
            return 0.60

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(
                    name="date",
                    dtype="date",
                    description="Transaction posting date (the date it settled, not necessarily when it occurred)",
                    format="YYYY-MM-DD",
                ),
                FieldAnnotation(
                    name="amount",
                    dtype="decimal",
                    description="Signed transaction amount: negative = debit (money out), positive = credit (money in)",
                    unit="USD",
                    format="dot_decimal",
                    examples=["-42.50", "1200.00", "-8.99"],
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
                    description="Merchant name or transaction description as provided by Chase",
                    examples=["AMAZON.COM", "STARBUCKS #1234", "PAYROLL DEPOSIT"],
                ),
                FieldAnnotation(
                    name="type",
                    dtype="string",
                    description="Transaction type as reported by Chase",
                    examples=["DEBIT", "CREDIT", "CHECK", "ATM", "DSLIP"],
                ),
                FieldAnnotation(
                    name="details",
                    dtype="string",
                    description="Additional transaction detail or sub-type, often mirrors Type",
                    nullable=True,
                    examples=["DEBIT_CARD", "ACH_DEBIT", "TELLER"],
                ),
                FieldAnnotation(
                    name="balance",
                    dtype="decimal",
                    description="Account balance after this transaction",
                    unit="USD",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="check_number",
                    dtype="string",
                    description="Check or slip number for check transactions; empty for card/ACH",
                    nullable=True,
                ),
            ],
            conventions=[
                "Amount is ALREADY signed: negative = money out (debit), positive = money in (credit). No direction column needed.",
                "Posting Date is when the transaction settled. Pending transactions do not appear in exports.",
                "Date format is MM/DD/YYYY in the raw file. Normalized to YYYY-MM-DD.",
                "Currency is always USD for Chase US accounts. No currency column in the export.",
                "The 'Type' field reflects the payment rail, not just debit/credit direction — e.g., CHECK is a debit but labelled CHECK.",
                "Merchant names may include location codes, store numbers, or truncated identifiers (e.g., 'SQ *COFFEE SHOP').",
                "Balance column may be empty in some older Chase export versions.",
            ],
            notes=[
                "Chase does not export pending transactions — only posted ones.",
                "For credit card accounts, Chase uses a different export format with different headers.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        text = text.lstrip("\ufeff")

        first_line = text.split("\n")[0]
        delimiter = ";" if first_line.count(";") > first_line.count(",") else ","

        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
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
        raw_date = row.get("Posting Date", "").strip().strip('"')
        tx_date = self._parse_date(raw_date)

        # Amount: signed, dot decimal
        raw_amount = row.get("Amount", "0").strip().strip('"').replace(",", "")
        try:
            amount = Decimal(raw_amount)
        except InvalidOperation:
            amount = Decimal("0")

        is_debit = amount < 0

        # Balance
        raw_balance = row.get("Balance", "").strip().strip('"').replace(",", "")
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
            "type": row.get("Type", "").strip().strip('"'),
            "details": row.get("Details", "").strip().strip('"') or None,
            "balance": str(balance) if balance is not None else None,
            "check_number": row.get("Check or Slip #", "").strip().strip('"') or None,
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


registry.register(ChaseBankParser())
