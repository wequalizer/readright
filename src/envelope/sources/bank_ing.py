"""ING Bank (Netherlands) CSV export parser."""

from __future__ import annotations

import csv
import io
from datetime import date
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


class INGBankParser(BaseParser):
    """Parser for ING Bank NL CSV exports.

    ING exports have specific quirks:
    - Amount is ALWAYS positive — the 'Af Bij' column determines debit/credit
    - Decimal separator is comma (Dutch locale)
    - Date format is YYYYMMDD (no separators)
    - The 'Mededelingen' field contains the actual transaction details
    - Encoding is typically latin-1 or utf-8
    """

    # Known ING CSV headers (both old and new format)
    HEADERS_NEW = {"Datum", "Naam / Omschrijving", "Rekening", "Tegenrekening", "Code", "Af Bij", "Bedrag (EUR)", "Mutatiesoort", "Mededelingen"}
    HEADERS_ALT = {"Datum", "Naam/Omschrijving", "Rekening", "Tegenrekening", "Code", "Af Bij", "Bedrag (EUR)", "MutatieSoort", "Mededelingen"}
    HEADERS_WITH_SALDO = HEADERS_NEW | {"Saldo na mutatie", "Tag"}

    def source_type(self) -> str:
        return "ing_csv_nl"

    def source_label(self) -> str:
        return "ING Bank CSV Export (Netherlands)"

    def detect(self, content: bytes, filename: str) -> float:
        """Detect ING CSV by headers."""
        if not filename.lower().endswith(".csv"):
            return 0.0

        encoding = self.detect_encoding(content)
        try:
            text = content.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            return 0.0

        first_line = text.split("\n")[0].strip().strip('"')

        # Check for ING-specific headers
        if "Af Bij" in first_line and "Bedrag (EUR)" in first_line:
            return 0.95
        if "Naam / Omschrijving" in first_line or "Naam/Omschrijving" in first_line:
            if "Tegenrekening" in first_line:
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
                FieldAnnotation(name="description", dtype="string", description="Full transaction description from Mededelingen field"),
                FieldAnnotation(name="balance_after", dtype="decimal", description="Account balance after this transaction", unit="EUR", nullable=True),
                FieldAnnotation(name="mutation_type", dtype="string", description="Type of mutation", examples=["Betaalautomaat", "iDEAL", "Overschrijving", "Incasso"]),
                FieldAnnotation(name="account_iban", dtype="string", description="Your own account IBAN"),
            ],
            conventions=[
                "Original ING amounts are ALWAYS positive — the 'Af Bij' column determines debit/credit. This parser normalizes to signed amounts (negative = debit).",
                "Original date format is YYYYMMDD without separators. Normalized to YYYY-MM-DD.",
                "Dutch decimal separator (comma) is converted to dot.",
                "The 'Mededelingen' field often contains structured info like terminal ID, card number, merchant name — concatenated without clear delimiters.",
                "ING exports may use semicolon OR comma as CSV delimiter depending on export settings.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        encoding = self.detect_encoding(content)
        try:
            text = content.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            # Fallback encodings common for Dutch bank exports
            for enc in ["latin-1", "cp1252", "utf-8-sig"]:
                try:
                    text = content.decode(enc)
                    break
                except (UnicodeDecodeError, LookupError):
                    continue
            else:
                return ParseResult(success=False, error="Could not decode file encoding")

        # Detect delimiter (ING uses either comma or semicolon)
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
        """Parse a single ING CSV row into normalized format."""
        # Handle both header variants
        name_key = "Naam / Omschrijving" if "Naam / Omschrijving" in row else "Naam/Omschrijving"

        # Parse date: YYYYMMDD → date
        raw_date = row.get("Datum", "").strip()
        if len(raw_date) == 8:
            tx_date = date(int(raw_date[:4]), int(raw_date[4:6]), int(raw_date[6:8]))
        else:
            tx_date = raw_date

        # Parse amount: always positive in ING, comma decimal
        raw_amount = row.get("Bedrag (EUR)", "0").strip().replace(",", ".")
        try:
            amount = Decimal(raw_amount)
        except InvalidOperation:
            amount = Decimal("0")

        # Direction: "Af" = debit (out), "Bij" = credit (in)
        af_bij = row.get("Af Bij", "").strip()
        is_debit = af_bij.lower() == "af"
        if is_debit:
            amount = -amount

        # Balance after (may not exist in all exports)
        raw_balance = row.get("Saldo na mutatie", "").strip().replace(",", ".")
        try:
            balance = Decimal(raw_balance) if raw_balance else None
        except InvalidOperation:
            balance = None

        # Mutation type normalization
        mut_key = "Mutatiesoort" if "Mutatiesoort" in row else "MutatieSoort"

        return {
            "date": str(tx_date),
            "amount": str(amount),
            "currency": "EUR",
            "direction": "debit" if is_debit else "credit",
            "counterparty": row.get(name_key, "").strip(),
            "counterparty_iban": row.get("Tegenrekening", "").strip(),
            "description": row.get("Mededelingen", "").strip(),
            "balance_after": str(balance) if balance is not None else None,
            "mutation_type": row.get(mut_key, "").strip(),
            "account_iban": row.get("Rekening", "").strip(),
        }


# Auto-register
registry.register(INGBankParser())
