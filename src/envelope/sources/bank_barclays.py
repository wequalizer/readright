"""Barclays (UK) CSV export parser."""

from __future__ import annotations

import csv
import io
from datetime import date
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Barclays UK CSV headers — fixed, always English
BARCLAYS_HEADERS = {"Number", "Date", "Account", "Amount", "Subcategory", "Memo"}

# Minimum required to confidently detect
DETECT_REQUIRED = {"Date", "Account", "Amount", "Subcategory", "Memo"}


class BarclaysBankParser(BaseParser):
    """Parser for Barclays (UK) CSV exports.

    Barclays CSV quirks:
    - Date format is DD/MM/YYYY (UK format — easy to confuse with US MM/DD/YYYY)
    - Amount is already signed: negative = debit, positive = credit
    - 'Number' column is a row sequence number, NOT a check/reference number
    - 'Account' column contains the account sort code + account number
    - 'Subcategory' is Barclays' own category label (e.g., "Shopping", "Bills")
    - 'Memo' is the transaction description/merchant name
    - Encoding is typically UTF-8 with or without BOM
    - Delimiter is always comma
    - Currency is GBP for UK accounts; EUR possible for EUR accounts
    """

    def source_type(self) -> str:
        return "barclays_csv_uk"

    def source_label(self) -> str:
        return "Barclays CSV Export (UK)"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        first_line = text.lstrip("\ufeff").split("\n")[0].strip()
        headers = {h.strip().strip('"') for h in first_line.split(",")}

        if BARCLAYS_HEADERS == headers:
            return 0.97  # Exact match — very distinctive set
        if BARCLAYS_HEADERS.issubset(headers):
            return 0.95
        if DETECT_REQUIRED.issubset(headers):
            return 0.85

        # 'Subcategory' and 'Memo' together with 'Number' is very Barclays-specific
        if "Subcategory" in headers and "Memo" in headers and "Number" in headers:
            return 0.80

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(
                    name="date",
                    dtype="date",
                    description="Transaction date in UK format",
                    format="YYYY-MM-DD",
                ),
                FieldAnnotation(
                    name="amount",
                    dtype="decimal",
                    description="Signed transaction amount: negative = debit (money out), positive = credit (money in)",
                    unit="GBP",
                    format="dot_decimal",
                    examples=["-35.00", "1500.00", "-12.50"],
                ),
                FieldAnnotation(
                    name="direction",
                    dtype="enum",
                    description="Derived from sign of amount: debit = money out, credit = money in",
                    enum_values=["debit", "credit"],
                ),
                FieldAnnotation(
                    name="memo",
                    dtype="string",
                    description="Transaction description or merchant name (the 'Memo' field)",
                    examples=["AMAZON EU SARL", "TFL TRAVEL CHARGE", "DIRECT DEBIT PAYMENT"],
                ),
                FieldAnnotation(
                    name="subcategory",
                    dtype="string",
                    description="Barclays' internal transaction category",
                    nullable=True,
                    examples=["Shopping", "Bills", "Entertainment", "Cash"],
                ),
                FieldAnnotation(
                    name="account",
                    dtype="string",
                    description="Sort code and account number of the Barclays account",
                    examples=["20-00-00 12345678"],
                    nullable=True,
                ),
                FieldAnnotation(
                    name="row_number",
                    dtype="integer",
                    description="Sequential row number assigned by Barclays (not a check or transaction reference)",
                    nullable=True,
                ),
            ],
            conventions=[
                "Date format is DD/MM/YYYY (UK format). Normalized to YYYY-MM-DD. Do NOT assume MM/DD/YYYY.",
                "Amount is ALREADY signed: negative = money out (debit), positive = money in (credit).",
                "Currency is GBP for standard UK Barclays accounts. EUR accounts may export EUR amounts.",
                "The 'Number' column is a row sequence number assigned by Barclays, NOT a transaction reference or check number.",
                "The 'Account' column contains the sort code and account number in format '20-00-00 12345678' — not an IBAN.",
                "The 'Subcategory' field reflects Barclays' own categorisation, not user-defined categories.",
                "Memo strings may be truncated and often include the merchant's payment reference.",
            ],
            notes=[
                "Barclays does not export pending transactions.",
                "For Barclays credit cards, the format may differ — this parser targets current account exports.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        text = text.lstrip("\ufeff")

        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            return ParseResult(success=False, error="No CSV headers found")

        rows = []
        warnings = []

        for i, row in enumerate(reader):
            try:
                parsed = self._parse_row(row, warnings)
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

    def _parse_row(self, row: dict, warnings: list) -> dict:
        # Date: DD/MM/YYYY (UK)
        raw_date = row.get("Date", "").strip().strip('"')
        tx_date = self._parse_date(raw_date)

        # Amount: signed, dot decimal, may have commas as thousand separators
        raw_amount_orig = row.get("Amount", "0").strip().strip('"')
        raw_amount = raw_amount_orig.replace(",", "")
        try:
            amount = Decimal(raw_amount)
        except (InvalidOperation, Exception):
            warnings.append(f"Row {len(warnings)+1}: could not parse amount '{raw_amount_orig}', defaulting to 0")
            amount = Decimal("0")

        is_debit = amount < 0

        # Row number (sequence, not a reference)
        raw_number = row.get("Number", "").strip().strip('"')
        try:
            row_number = int(raw_number) if raw_number else None
        except ValueError:
            row_number = None

        return {
            "date": str(tx_date),
            "amount": str(amount),
            "currency": "GBP",
            "direction": "debit" if is_debit else "credit",
            "memo": row.get("Memo", "").strip().strip('"'),
            "subcategory": row.get("Subcategory", "").strip().strip('"') or None,
            "account": row.get("Account", "").strip().strip('"') or None,
            "row_number": row_number,
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


registry.register(BarclaysBankParser())
