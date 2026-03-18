"""Cash App CSV export parser."""

from __future__ import annotations

import csv
import io
from datetime import datetime
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

COLUMN_MAP = {
    "Transaction ID": "transaction_id",
    "Date": "date",
    "Transaction Type": "transaction_type",
    "Currency": "currency",
    "Amount": "amount",
    "Fee": "fee",
    "Net Amount": "net_amount",
    "Asset Type": "asset_type",
    "Asset Price": "asset_price",
    "Asset Amount": "asset_amount",
    "Status": "status",
    "Notes": "notes",
    "Name of sender/receiver": "counterparty",
    "Account": "account",
}


class CashAppParser(BaseParser):
    """Parser for Cash App transaction CSV exports.

    Cash App quirks:
    - Amount, Fee, Net Amount include a currency symbol prefix (e.g. '$25.00').
    - Amount is signed: negative = money sent, positive = money received.
    - Crypto transactions populate Asset Type, Asset Price, Asset Amount
      while using USD as the currency for the fiat value.
    - Transaction Type values include: 'Cash out', 'Cash in', 'Payment',
      'Received payment', 'Bitcoin boost', 'Stock activity', etc.
    - 'Account' column holds the $Cashtag or bank account name.
    - Date format: MM/DD/YYYY or YYYY-MM-DD depending on export version.
    - Status is almost always 'COMPLETED' but can be 'FAILED' or 'REFUNDED'.
    """

    def source_type(self) -> str:
        return "cashapp_csv"

    def source_label(self) -> str:
        return "Cash App Transaction CSV Export"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        first_line = text.lstrip("\ufeff").split("\n")[0]

        # "Name of sender/receiver" is unusual enough to be highly diagnostic
        if "Name of sender/receiver" in first_line and "Transaction Type" in first_line:
            return 0.97
        if "Asset Type" in first_line and "Asset Price" in first_line and "Net Amount" in first_line:
            return 0.93
        if "Name of sender/receiver" in first_line and "Net Amount" in first_line:
            return 0.85

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="transaction_id", dtype="string", description="Unique Cash App transaction ID", examples=["2XXXXXXXXXXXXXXXXX"]),
                FieldAnnotation(name="date", dtype="date", description="Transaction date", format="YYYY-MM-DD"),
                FieldAnnotation(name="transaction_type", dtype="string", description="Type of transaction", examples=["Payment", "Received Payment", "Cash out", "Cash in", "Bitcoin boost", "Stock activity"]),
                FieldAnnotation(name="currency", dtype="string", description="Fiat currency of the transaction", examples=["USD"]),
                FieldAnnotation(name="amount", dtype="decimal", description="Transaction amount (negative = sent, positive = received)", unit="USD"),
                FieldAnnotation(name="fee", dtype="decimal", description="Fee charged by Cash App", unit="USD"),
                FieldAnnotation(name="net_amount", dtype="decimal", description="Amount minus fee (negative = net sent, positive = net received)", unit="USD"),
                FieldAnnotation(name="asset_type", dtype="string", description="For crypto/stock transactions: asset type", examples=["BTC", "USD"], nullable=True),
                FieldAnnotation(name="asset_price", dtype="decimal", description="Price of the asset at time of transaction", unit="USD", nullable=True),
                FieldAnnotation(name="asset_amount", dtype="decimal", description="Amount of asset bought/sold", nullable=True),
                FieldAnnotation(name="status", dtype="string", description="Transaction status", examples=["COMPLETED", "FAILED", "REFUNDED"]),
                FieldAnnotation(name="notes", dtype="string", description="Transaction memo or note", nullable=True),
                FieldAnnotation(name="counterparty", dtype="string", description="Name of the other party ($Cashtag or display name)", nullable=True),
                FieldAnnotation(name="account", dtype="string", description="Account label or $Cashtag for this account", nullable=True),
            ],
            conventions=[
                "Amount and Net Amount have a '$' prefix in the raw file — stripped during parsing.",
                "Negative amount = money sent. Positive amount = money received.",
                "Cash out (bank transfer) rows have no counterparty name — they represent moving money to a linked bank account.",
                "Crypto transactions (Asset Type = BTC etc.) show the USD equivalent in Amount, and the asset quantity in Asset Amount.",
                "Currency is always USD for standard transactions. Crypto values are also expressed in USD.",
                "Fee is always 0 for standard P2P payments — Cash App charges fees only for instant bank transfers and some crypto.",
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

        col_lookup: dict[str, str] = {}
        for header in reader.fieldnames:
            clean = header.strip().strip('"')
            if clean in COLUMN_MAP:
                col_lookup[COLUMN_MAP[clean]] = header

        rows = []
        warnings = []

        for i, row in enumerate(reader):
            try:
                rows.append(self._parse_row(row, col_lookup))
            except Exception as e:
                warnings.append(f"Row {i + 1}: {e}")

        if not rows:
            return ParseResult(success=False, error="No rows could be parsed")

        envelope = ContextEnvelope(schema=self.schema(), data=rows, warnings=warnings)
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get(self, row: dict, col_lookup: dict, key: str, default: str = "") -> str:
        header = col_lookup.get(key)
        if header is None:
            return default
        return (row.get(header) or "").strip()

    def _parse_amount(self, raw: str) -> Decimal | None:
        """Handle '$25.00', '-$10.00', '25.00'."""
        raw = raw.strip()
        if not raw:
            return None
        negative = raw.startswith("-")
        raw = raw.lstrip("-+").strip().lstrip("$").replace(",", "")
        if not raw:
            return None
        try:
            value = Decimal(raw)
            return -value if negative else value
        except InvalidOperation:
            return None

    def _parse_date(self, raw: str) -> str | None:
        raw = raw.strip()
        if not raw:
            return None
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y"):
            try:
                return datetime.strptime(raw, fmt).date().isoformat()
            except ValueError:
                continue
        return raw

    def _parse_row(self, row: dict, col_lookup: dict) -> dict:
        return {
            "transaction_id": self._get(row, col_lookup, "transaction_id") or None,
            "date": self._parse_date(self._get(row, col_lookup, "date")),
            "transaction_type": self._get(row, col_lookup, "transaction_type") or None,
            "currency": self._get(row, col_lookup, "currency", "USD"),
            "amount": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "amount"))) is not None else None,
            "fee": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "fee"))) is not None else "0",
            "net_amount": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "net_amount"))) is not None else None,
            "asset_type": self._get(row, col_lookup, "asset_type") or None,
            "asset_price": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "asset_price"))) is not None else None,
            "asset_amount": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "asset_amount"))) is not None else None,
            "status": self._get(row, col_lookup, "status") or None,
            "notes": self._get(row, col_lookup, "notes") or None,
            "counterparty": self._get(row, col_lookup, "counterparty") or None,
            "account": self._get(row, col_lookup, "account") or None,
        }

    def _decode(self, content: bytes) -> str | None:
        for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(CashAppParser())
