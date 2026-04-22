"""Citibank (US) CSV export parser."""

from __future__ import annotations

import csv
import io
from datetime import date
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Citibank CSV headers — fixed, always English
CITI_HEADERS = {"Status", "Date", "Description", "Debit", "Credit"}


class CitibankParser(BaseParser):
    """Parser for Citibank (US) CSV exports.

    Citibank CSV quirks:
    - Unique format: separate Debit and Credit columns rather than a signed Amount
    - Debit column: positive number = money out; empty when not a debit
    - Credit column: positive number = money in; empty when not a credit
    - A row will have either Debit OR Credit populated — never both (except $0 edge cases)
    - 'Status' column values: 'Cleared', 'Pending' — pending transactions ARE included
    - Date format is MM/DD/YYYY
    - No balance column in the standard Citibank export
    - Encoding is typically UTF-8 with BOM
    - Citibank may export both checking and credit card accounts in this same format
    - For credit cards, a Debit = charge (spending), Credit = payment/refund
    """

    def source_type(self) -> str:
        return "citi_csv_us"

    def source_label(self) -> str:
        return "Citibank CSV Export (US)"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        first_line = text.lstrip("\ufeff").split("\n")[0].strip()
        headers = {h.strip().strip('"') for h in first_line.split(",")}

        if CITI_HEADERS == headers:
            return 0.97  # Exact 5-column match is very distinctive
        if CITI_HEADERS.issubset(headers):
            return 0.93

        # 'Status' + 'Debit' + 'Credit' combination is uniquely Citi among US banks
        if "Status" in headers and "Debit" in headers and "Credit" in headers:
            if "Date" in headers and "Description" in headers:
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
                    description="Signed transaction amount: negative = debit (money out), positive = credit (money in). Derived by negating Debit or using Credit as-is.",
                    unit="USD",
                    format="dot_decimal",
                    examples=["-34.99", "500.00", "-12.00"],
                ),
                FieldAnnotation(
                    name="direction",
                    dtype="enum",
                    description="debit = money out (Debit column was populated), credit = money in (Credit column was populated)",
                    enum_values=["debit", "credit"],
                ),
                FieldAnnotation(
                    name="description",
                    dtype="string",
                    description="Merchant name or transaction description",
                    examples=["AMAZON.COM", "ACH CREDIT PAYROLL", "CITI CREDIT CARD PAYMENT"],
                ),
                FieldAnnotation(
                    name="status",
                    dtype="enum",
                    description="Transaction status — Citibank includes pending transactions in exports",
                    enum_values=["Cleared", "Pending"],
                    examples=["Cleared", "Pending"],
                ),
                FieldAnnotation(
                    name="debit",
                    dtype="decimal",
                    description="Raw debit amount as a positive number (money leaving the account). Null when the transaction is a credit.",
                    unit="USD",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="credit",
                    dtype="decimal",
                    description="Raw credit amount as a positive number (money entering the account). Null when the transaction is a debit.",
                    unit="USD",
                    nullable=True,
                ),
            ],
            conventions=[
                "Citibank uses SEPARATE Debit and Credit columns — unlike most US banks that use a single signed Amount.",
                "Debit column: positive number = money out. Credit column: positive number = money in. Exactly one is populated per row.",
                "Normalized to a single signed 'amount' field: debit values are negated, credit values are kept positive.",
                "Date format is MM/DD/YYYY. Normalized to YYYY-MM-DD.",
                "Currency is always USD for Citibank US accounts. No currency column in the export.",
                "The 'Status' column includes 'Pending' transactions — this is unlike many other US bank exports which only show cleared transactions.",
                "For credit card accounts: Debit = a charge (spending), Credit = a payment or refund.",
                "No balance column is exported by Citibank in the standard format.",
            ],
            notes=[
                "The presence of pending transactions means duplicate detection is important — a pending row may later appear as a cleared row.",
                "Citibank's description field is concise but often includes payment network reference codes.",
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
        # Normalize keys
        row = {k.strip().strip('"') if k else k: (v.strip().strip('"') if v else "")
               for k, v in row.items() if k}

        # Date: MM/DD/YYYY
        raw_date = row.get("Date", "")
        tx_date = self._parse_date(raw_date)

        # Debit and Credit columns — both are positive numbers; only one is populated
        raw_debit_orig = row.get("Debit", "")
        raw_debit = raw_debit_orig.replace(",", "")
        raw_credit_orig = row.get("Credit", "")
        raw_credit = raw_credit_orig.replace(",", "")

        try:
            debit = Decimal(raw_debit) if raw_debit else None
        except (InvalidOperation, Exception):
            warnings.append(f"Row {len(warnings)+1}: could not parse debit '{raw_debit_orig}', defaulting to None")
            debit = None

        try:
            credit = Decimal(raw_credit) if raw_credit else None
        except (InvalidOperation, Exception):
            warnings.append(f"Row {len(warnings)+1}: could not parse credit '{raw_credit_orig}', defaulting to None")
            credit = None

        # Determine direction and signed amount
        if debit is not None and debit != 0:
            amount = -abs(debit)
            is_debit = True
        elif credit is not None and credit != 0:
            amount = abs(credit)
            is_debit = False
        else:
            # Both empty or zero — treat as zero credit
            amount = Decimal("0")
            is_debit = False

        return {
            "date": str(tx_date),
            "amount": str(amount),
            "currency": "USD",
            "direction": "debit" if is_debit else "credit",
            "description": row.get("Description", ""),
            "status": row.get("Status", "") or None,
            "debit": str(debit) if debit is not None else None,
            "credit": str(credit) if credit is not None else None,
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


registry.register(CitibankParser())
