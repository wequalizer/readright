"""PayPal CSV export parser (Dutch & English)."""

from __future__ import annotations

import csv
import io
import logging
from datetime import date
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# PayPal headers in Dutch and English — map to normalized keys
COLUMN_MAP = {
    # Dutch
    "Datum": "date",
    "Tijd": "time",
    "Tijdzone": "timezone",
    "Naam": "name",
    "Type": "type",
    "Status": "status",
    "Valuta": "currency",
    "Bedrag": "amount",
    "Kosten": "fee",
    "Totaal": "total",
    "Wisselkoers": "exchange_rate",
    "Ontvangstbewijsreferentie": "receipt_id",
    "Saldo": "balance",
    "Transactiereferentie": "transaction_id",
    "Objecttitel": "item_title",
    # English
    "Date": "date",
    "Time": "time",
    "TimeZone": "timezone",
    "Name": "name",
    "Type": "type",
    "Status": "status",
    "Currency": "currency",
    "Gross": "amount",
    "Fee": "fee",
    "Net": "total",
    "Exchange Rate": "exchange_rate",
    "Receipt ID": "receipt_id",
    "Balance": "balance",
    "Transaction ID": "transaction_id",
    "Item Title": "item_title",
}


class PayPalParser(BaseParser):
    """Parser for PayPal CSV exports.

    PayPal quirks:
    - Headers can be Dutch or English depending on account language
    - Amount uses comma decimal (Dutch) or dot decimal (English)
    - Every payment has a matching 'Bankstorting' (bank funding) row
    - Status can be: Voltooid/Completed, In behandeling/Pending, etc.
    - BOM (byte order mark) is present in most exports
    """

    def source_type(self) -> str:
        return "paypal_csv"

    def source_label(self) -> str:
        return "PayPal Transaction Export (CSV)"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        first_line = text.split("\n")[0].strip()

        # Dutch PayPal headers
        if "Transactiereferentie" in first_line and "Bedrag" in first_line:
            return 0.95
        # English PayPal headers
        if "Transaction ID" in first_line and "Gross" in first_line:
            return 0.95
        # Fallback: PayPal-specific column combos
        if "Tijdzone" in first_line and "Valuta" in first_line:
            return 0.85
        if "TimeZone" in first_line and "Currency" in first_line:
            return 0.85

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="date", dtype="date", description="Transaction date", format="YYYY-MM-DD"),
                FieldAnnotation(name="time", dtype="string", description="Transaction time", format="HH:MM:SS"),
                FieldAnnotation(name="timezone", dtype="string", description="Timezone of the transaction", examples=["CET", "CEST", "PST"]),
                FieldAnnotation(name="name", dtype="string", description="Counterparty name (merchant, person, or empty for bank transfers)"),
                FieldAnnotation(name="type", dtype="string", description="Transaction type", examples=["Vooraf goedgekeurde betaling", "Bankstorting naar PP-rekening", "Betaling"]),
                FieldAnnotation(name="status", dtype="string", description="Transaction status", examples=["Voltooid", "In behandeling", "Completed", "Pending"]),
                FieldAnnotation(name="amount", dtype="decimal", description="Gross amount: negative = payment out, positive = received", unit="EUR"),
                FieldAnnotation(name="fee", dtype="decimal", description="PayPal fee charged", unit="EUR"),
                FieldAnnotation(name="total", dtype="decimal", description="Net amount after fees", unit="EUR"),
                FieldAnnotation(name="currency", dtype="string", description="Currency code", examples=["EUR", "USD"]),
                FieldAnnotation(name="balance", dtype="decimal", description="PayPal balance after transaction", unit="EUR", nullable=True),
                FieldAnnotation(name="transaction_id", dtype="string", description="Unique PayPal transaction reference"),
                FieldAnnotation(name="item_title", dtype="string", description="Item or service title", nullable=True),
            ],
            conventions=[
                "Every outgoing payment typically has a matching 'Bankstorting naar PP-rekening' (bank deposit) row. These are PayPal pulling funds from your bank — not separate transactions.",
                "Amount is already signed: negative = money out, positive = money in.",
                "Dutch exports use comma as decimal separator (10,84). Normalized to dot decimal (10.84).",
                "Status 'In behandeling' / 'Pending' means the bank funding is still processing — the payment itself went through.",
                "Headers can be in Dutch or English depending on PayPal account language.",
                "Empty 'Naam'/'Name' field usually means it's an internal PayPal transfer (bank funding, currency conversion).",
            ],
            notes=[
                "Dutch PayPal exports use completely different column headers than English ones (e.g. 'Bedrag' vs 'Gross').",
                "The PayPal balance currency may differ from the transaction currency when multi-currency is enabled.",
                "Refunds appear as separate positive-amount rows, not as reversals on the original transaction.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        # Strip BOM
        text = text.lstrip("\ufeff")

        # PayPal uses comma delimiter with quoted fields
        reader = csv.DictReader(io.StringIO(text), delimiter=",")
        if not reader.fieldnames:
            return ParseResult(success=False, error="No CSV headers found")

        # Build column lookup
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
        header = col_lookup.get(key)
        if header is None:
            return default
        return row.get(header, default).strip()

    def _parse_amount(self, raw: str) -> Decimal:
        """Parse PayPal amount — handles both comma and dot decimal."""
        raw = raw.strip().replace(" ", "")
        if not raw:
            return Decimal("0")
        # Dutch: 1.234,56 → remove dots, replace comma with dot
        # English: 1,234.56 → remove commas
        if "," in raw and "." in raw:
            # Determine which is decimal: last occurrence wins
            if raw.rfind(",") > raw.rfind("."):
                # Comma is decimal (Dutch): 1.234,56
                raw = raw.replace(".", "").replace(",", ".")
            else:
                # Dot is decimal (English): 1,234.56
                raw = raw.replace(",", "")
        elif "," in raw:
            # Only comma — it's the decimal separator
            raw = raw.replace(",", ".")
        try:
            return Decimal(raw)
        except (InvalidOperation, Exception):
            raise ValueError(f"PayPal: could not parse amount '{raw}'")

    def _parse_row(self, row: dict, col_lookup: dict) -> dict:
        # Date: DD-MM-YYYY
        raw_date = self._get(row, col_lookup, "date")
        tx_date = self._parse_date(raw_date)

        amount = self._parse_amount(self._get(row, col_lookup, "amount", "0"))
        fee = self._parse_amount(self._get(row, col_lookup, "fee", "0"))
        total = self._parse_amount(self._get(row, col_lookup, "total", "0"))
        balance_raw = self._get(row, col_lookup, "balance")
        balance = self._parse_amount(balance_raw) if balance_raw else None

        return {
            "date": str(tx_date),
            "time": self._get(row, col_lookup, "time"),
            "timezone": self._get(row, col_lookup, "timezone"),
            "name": self._get(row, col_lookup, "name"),
            "type": self._get(row, col_lookup, "type"),
            "status": self._get(row, col_lookup, "status"),
            "amount": str(amount),
            "fee": str(fee),
            "total": str(total),
            "currency": self._get(row, col_lookup, "currency", "EUR"),
            "balance": str(balance) if balance is not None else None,
            "transaction_id": self._get(row, col_lookup, "transaction_id"),
            "item_title": self._get(row, col_lookup, "item_title"),
        }

    def _parse_date(self, raw: str) -> date | str:
        raw = raw.strip()
        if not raw:
            return raw
        # DD-MM-YYYY
        if len(raw) == 10 and raw[2] == "-" and raw[5] == "-":
            return date(int(raw[6:10]), int(raw[3:5]), int(raw[:2]))
        # DD/MM/YYYY
        if len(raw) == 10 and raw[2] == "/" and raw[5] == "/":
            return date(int(raw[6:10]), int(raw[3:5]), int(raw[:2]))
        # YYYY-MM-DD
        if len(raw) == 10 and raw[4] == "-":
            return date(int(raw[:4]), int(raw[5:7]), int(raw[8:10]))
        return raw

    def _decode(self, content: bytes) -> str | None:
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(PayPalParser())
