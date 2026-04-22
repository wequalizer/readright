"""Rabobank (Netherlands) CSV export parser."""

from __future__ import annotations

import csv
import io
from datetime import date
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


class RabobankParser(BaseParser):
    """Parser for Rabobank NL CSV exports.

    Rabobank CSV quirks:
    - Uses comma as decimal separator (Dutch locale)
    - Delimiter is semicolon in most exports
    - Has both old format and new (2019+) format with different headers
    - Amount can be negative (debit) or positive (credit) — unlike ING
    - IBAN fields use 'IBAN/BBAN' naming
    """

    # Rabobank has 26 columns in current format (2024+)
    DETECT_HEADERS = {"IBAN/BBAN", "Bedrag", "Saldo na trn", "Naam tegenpartij"}

    def source_type(self) -> str:
        return "rabobank_csv_nl"

    def source_label(self) -> str:
        return "Rabobank CSV Export (Netherlands)"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        first_line = text.split("\n")[0].strip()

        if "IBAN/BBAN" in first_line and "Saldo na trn" in first_line:
            return 0.95
        if "Naam tegenpartij" in first_line and "Bedrag" in first_line:
            return 0.85
        if "RABO" in first_line.upper():
            return 0.70

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="date", dtype="date", description="Transaction date", format="YYYY-MM-DD"),
                FieldAnnotation(name="amount", dtype="decimal", description="Signed amount: negative = debit, positive = credit", unit="EUR"),
                FieldAnnotation(name="direction", dtype="enum", description="Debit or credit", enum_values=["debit", "credit"]),
                FieldAnnotation(name="counterparty", dtype="string", description="Name of the counterparty"),
                FieldAnnotation(name="counterparty_iban", dtype="string", description="IBAN of the counterparty", nullable=True),
                FieldAnnotation(name="description", dtype="string", description="Combined description fields (Omschrijving-1 through Omschrijving-3)"),
                FieldAnnotation(name="balance_after", dtype="decimal", description="Balance after transaction", unit="EUR", nullable=True),
                FieldAnnotation(name="account_iban", dtype="string", description="Your own IBAN"),
                FieldAnnotation(name="currency", dtype="string", description="Currency code", examples=["EUR"]),
                FieldAnnotation(name="reference", dtype="string", description="Transaction reference", nullable=True),
            ],
            conventions=[
                "Rabobank amounts are ALREADY signed — negative = debit, positive = credit. No separate direction column.",
                "Decimal separator is comma (Dutch locale). Normalized to dot.",
                "CSV delimiter is semicolon.",
                "Date format is YYYY-MM-DD in new format, DD-MM-YYYY in old format. Normalized to YYYY-MM-DD.",
                "Description may span multiple columns (Omschrijving-1, -2, -3). These are concatenated.",
                "Rabobank includes 'Rentedatum' (interest date) which may differ from transaction date.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        # Strip BOM
        text = text.lstrip("\ufeff")

        # Rabobank can use semicolon OR comma as delimiter
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
        # Parse date — try multiple formats
        raw_date = row.get("Datum", "").strip()
        tx_date = self._parse_date(raw_date)

        # Parse amount — comma decimal, already signed, may have explicit +/- prefix
        raw_amount_orig = row.get("Bedrag", "0").strip()
        raw_amount = raw_amount_orig
        # Remove thousand separators (dots) but keep the sign and comma decimal
        # Rabo format: "+40,70" or "-76,05" or "1.234,56"
        sign = ""
        if raw_amount.startswith(("+", "-")):
            sign = raw_amount[0]
            raw_amount = raw_amount[1:]
        raw_amount = raw_amount.replace(".", "").replace(",", ".")
        raw_amount = sign + raw_amount
        try:
            amount = Decimal(raw_amount)
        except (InvalidOperation, Exception):
            warnings.append(f"Row {len(warnings)+1}: could not parse amount '{raw_amount_orig}', defaulting to 0")
            amount = Decimal("0")

        is_debit = amount < 0

        # Balance — same format
        raw_balance_orig = row.get("Saldo na trn", "").strip()
        raw_balance = raw_balance_orig
        if raw_balance:
            bal_sign = ""
            if raw_balance.startswith(("+", "-")):
                bal_sign = raw_balance[0]
                raw_balance = raw_balance[1:]
            raw_balance = bal_sign + raw_balance.replace(".", "").replace(",", ".")
            try:
                balance = Decimal(raw_balance)
            except (InvalidOperation, Exception):
                warnings.append(f"Row {len(warnings)+1}: could not parse balance '{raw_balance_orig}', defaulting to None")
                balance = None
        else:
            balance = None

        # Combine description fields
        descriptions = []
        for key in ["Omschrijving-1", "Omschrijving-2", "Omschrijving-3"]:
            val = row.get(key, "").strip()
            if val:
                descriptions.append(val)
        description = " ".join(descriptions)

        return {
            "date": str(tx_date),
            "amount": str(amount),
            "currency": row.get("Munt", "EUR").strip(),
            "direction": "debit" if is_debit else "credit",
            "counterparty": row.get("Naam tegenpartij", "").strip(),
            "counterparty_iban": row.get("Tegenrekening IBAN/BBAN", "").strip(),
            "description": description,
            "balance_after": str(balance) if balance is not None else None,
            "account_iban": row.get("IBAN/BBAN", "").strip(),
            "reference": row.get("Transactiereferentie", "").strip(),
        }

    def _decode(self, content: bytes) -> str | None:
        """Try multiple encodings common for Dutch bank exports."""
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None

    def _parse_date(self, raw: str) -> date | str:
        """Parse date from various Rabobank formats."""
        raw = raw.strip().strip('"')
        if not raw:
            return raw

        # YYYY-MM-DD
        if len(raw) == 10 and raw[4] == "-":
            return date(int(raw[:4]), int(raw[5:7]), int(raw[8:10]))
        # DD-MM-YYYY
        if len(raw) == 10 and raw[2] == "-":
            return date(int(raw[6:10]), int(raw[3:5]), int(raw[:2]))
        # YYYYMMDD
        if len(raw) == 8 and raw.isdigit():
            return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))

        return raw


registry.register(RabobankParser())
