"""ING Bank (Netherlands) CSV export parser."""

from __future__ import annotations

import csv
import io
from datetime import date
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Column name mapping: English → Dutch → normalized key
# ING exports can be in Dutch OR English depending on account language settings
COLUMN_MAP = {
    # Dutch headers
    "Datum": "date",
    "Naam / Omschrijving": "name",
    "Naam/Omschrijving": "name",
    "Rekening": "account",
    "Tegenrekening": "counterparty_iban",
    "Code": "code",
    "Af Bij": "direction",
    "Bedrag (EUR)": "amount",
    "Mutatiesoort": "mutation_type",
    "MutatieSoort": "mutation_type",
    "Mededelingen": "description",
    "Saldo na mutatie": "balance_after",
    "Tag": "tag",
    # English headers
    "Date": "date",
    "Name / Description": "name",
    "Account": "account",
    "Counterparty": "counterparty_iban",
    "Debit/credit": "direction",
    "Amount (EUR)": "amount",
    "Transaction type": "mutation_type",
    "Notifications": "description",
    "Resulting balance": "balance_after",
}

# Direction values in both languages
DEBIT_VALUES = {"af", "debit"}
CREDIT_VALUES = {"bij", "credit"}


class INGBankParser(BaseParser):
    """Parser for ING Bank NL CSV exports.

    ING exports have specific quirks:
    - Amount is ALWAYS positive — a separate column determines debit/credit
    - Headers can be in Dutch OR English depending on account language
    - Decimal separator is comma (Dutch locale)
    - Date format is YYYYMMDD (no separators)
    - Encoding is typically latin-1 or utf-8
    """

    def source_type(self) -> str:
        return "ing_csv_nl"

    def source_label(self) -> str:
        return "ING Bank CSV Export (Netherlands)"

    def detect(self, content: bytes, filename: str) -> float:
        """Detect ING CSV by headers."""
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        first_line = text.split("\n")[0].strip()

        # Dutch headers
        if "Af Bij" in first_line and "Bedrag (EUR)" in first_line:
            return 0.95
        # English headers
        if "Debit/credit" in first_line and "Amount (EUR)" in first_line:
            return 0.95
        # Fallback: ING-specific column combos
        if ("Naam / Omschrijving" in first_line or "Name / Description" in first_line):
            if "Tegenrekening" in first_line or "Counterparty" in first_line:
                return 0.90

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="date", dtype="date", description="Transaction date", format="YYYY-MM-DD"),
                FieldAnnotation(name="amount", dtype="decimal", description="Signed amount: negative = debit (money out), positive = credit (money in)", unit="EUR", format="dot_decimal"),
                FieldAnnotation(name="direction", dtype="enum", description="Debit or credit", enum_values=["debit", "credit"]),
                FieldAnnotation(name="counterparty", dtype="string", description="Name of the other party"),
                FieldAnnotation(name="counterparty_iban", dtype="string", description="IBAN of the counterparty", nullable=True),
                FieldAnnotation(name="description", dtype="string", description="Full transaction description/notifications field"),
                FieldAnnotation(name="balance_after", dtype="decimal", description="Account balance after this transaction", unit="EUR", nullable=True),
                FieldAnnotation(name="mutation_type", dtype="string", description="Type of mutation", examples=["Betaalautomaat", "Payment terminal", "iDEAL", "Transfer", "SEPA direct debit"]),
                FieldAnnotation(name="account_iban", dtype="string", description="Your own account IBAN"),
            ],
            conventions=[
                "Original ING amounts are ALWAYS positive — a separate column ('Af Bij' or 'Debit/credit') determines direction. Normalized to signed amounts (negative = debit).",
                "Headers can be Dutch OR English depending on the account's language setting.",
                "Original date format is YYYYMMDD without separators. Normalized to YYYY-MM-DD.",
                "Dutch decimal separator (comma) is converted to dot.",
                "The description field often contains structured info like terminal ID, card number, merchant name — concatenated without clear delimiters.",
                "ING exports may use semicolon OR comma as CSV delimiter depending on export settings.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        # Strip BOM
        text = text.lstrip("\ufeff")

        # Detect delimiter (ING uses either comma or semicolon)
        first_line = text.split("\n")[0]
        delimiter = ";" if first_line.count(";") > first_line.count(",") else ","

        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        if not reader.fieldnames:
            return ParseResult(success=False, error="No CSV headers found")

        # Build column lookup: normalize headers to our internal keys
        col_lookup = {}
        for header in reader.fieldnames:
            clean = header.strip().strip('"')
            if clean in COLUMN_MAP:
                col_lookup[COLUMN_MAP[clean]] = header

        rows = []
        warnings = []

        for i, row in enumerate(reader):
            try:
                parsed = self._parse_row(row, col_lookup)
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

    def _get(self, row: dict, col_lookup: dict, key: str, default: str = "") -> str:
        """Get a value from a row using the normalized column lookup."""
        header = col_lookup.get(key)
        if header is None:
            return default
        return row.get(header, default).strip()

    def _parse_row(self, row: dict, col_lookup: dict) -> dict:
        """Parse a single ING CSV row into normalized format."""
        # Parse date: YYYYMMDD → date
        raw_date = self._get(row, col_lookup, "date")
        if len(raw_date) == 8 and raw_date.isdigit():
            tx_date = date(int(raw_date[:4]), int(raw_date[4:6]), int(raw_date[6:8]))
        else:
            tx_date = raw_date

        # Parse amount: always positive in ING, comma decimal
        raw_amount = self._get(row, col_lookup, "amount", "0").replace(",", ".")
        try:
            amount = Decimal(raw_amount)
        except InvalidOperation:
            amount = Decimal("0")

        # Direction: "Af"/"Debit" = debit (out), "Bij"/"Credit" = credit (in)
        direction_raw = self._get(row, col_lookup, "direction").lower()
        is_debit = direction_raw in DEBIT_VALUES
        if is_debit:
            amount = -amount

        # Balance after
        raw_balance = self._get(row, col_lookup, "balance_after").replace(",", ".")
        try:
            balance = Decimal(raw_balance) if raw_balance else None
        except InvalidOperation:
            balance = None

        return {
            "date": str(tx_date),
            "amount": str(amount),
            "currency": "EUR",
            "direction": "debit" if is_debit else "credit",
            "counterparty": self._get(row, col_lookup, "name"),
            "counterparty_iban": self._get(row, col_lookup, "counterparty_iban"),
            "description": self._get(row, col_lookup, "description"),
            "balance_after": str(balance) if balance is not None else None,
            "mutation_type": self._get(row, col_lookup, "mutation_type"),
            "account_iban": self._get(row, col_lookup, "account"),
        }

    def _decode(self, content: bytes) -> str | None:
        """Try multiple encodings common for Dutch bank exports."""
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


# Auto-register
registry.register(INGBankParser())
