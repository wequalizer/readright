"""Stripe payout/payment CSV export parser."""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Map Stripe CSV headers to normalized keys.
# Only the columns that carry real signal — skip card address noise etc.
COLUMN_MAP = {
    "id": "id",
    "Description": "description",
    "Seller Message": "seller_message",
    "Created (UTC)": "created_at",
    "Amount": "amount",
    "Amount Refunded": "amount_refunded",
    "Currency": "currency",
    "Converted Amount": "converted_amount",
    "Converted Amount Refunded": "converted_amount_refunded",
    "Converted Currency": "converted_currency",
    "Fee": "fee",
    "Tax": "tax",
    "Mode": "mode",
    "Status": "status",
    "Statement Descriptor": "statement_descriptor",
    "Customer ID": "customer_id",
    "Customer Description": "customer_description",
    "Customer Email": "customer_email",
    "Captured": "captured",
    "Card Last4": "card_last4",
    "Card Brand": "card_brand",
    "Card Funding": "card_funding",
    "Transfer": "transfer_id",
    "Transfer Date (UTC)": "transfer_date",
    "Transfer Group": "transfer_group",
    "Payout ID": "payout_id",
    "Payout Expected Arrival Date": "payout_expected_arrival",
    "Payout Date (UTC)": "payout_date",
    "Payout Type": "payout_type",
    "Payout Status": "payout_status",
    "Payout Description": "payout_description",
    "Payout Destination": "payout_destination",
}


class StripeParser(BaseParser):
    """Parser for Stripe balance transaction / payment CSV exports.

    Stripe quirks:
    - Amounts are in the currency's smallest unit (cents for USD/EUR, etc.)
      BUT in practice Stripe exports them as decimal strings already (e.g. "9.99").
    - 'mode' is 'test' or 'live' — test rows should usually be filtered by callers.
    - A single charge row may have a matching payout row referencing the same payout_id.
    - Created (UTC) and Transfer Date (UTC) are ISO-8601 datetime strings.
    - Refunded amounts are always positive (they represent the absolute refunded value).
    - Net = Amount - Fee - Amount Refunded (not a separate column — derive if needed).
    """

    def source_type(self) -> str:
        return "stripe_csv"

    def source_label(self) -> str:
        return "Stripe Payment / Payout CSV Export"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        first_line = text.lstrip("\ufeff").split("\n")[0]

        # These two together are unique to Stripe
        if "Payout ID" in first_line and "Transfer Group" in first_line:
            return 0.97
        if "Seller Message" in first_line and "Card Fingerprint" in first_line:
            return 0.95
        # Weaker signal
        if "Payout ID" in first_line and "Created (UTC)" in first_line:
            return 0.80
        if "Card Fingerprint" in first_line and "Card Tokenization Method" in first_line:
            return 0.75

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="id", dtype="string", description="Stripe charge ID", examples=["ch_3abc123", "py_3xyz456"]),
                FieldAnnotation(name="description", dtype="string", description="Charge description or product name", nullable=True),
                FieldAnnotation(name="seller_message", dtype="string", description="Stripe-generated result message", examples=["Payment complete.", "Your card was declined."], nullable=True),
                FieldAnnotation(name="created_at", dtype="date", description="Charge creation timestamp (UTC)", format="YYYY-MM-DD HH:MM:SS"),
                FieldAnnotation(name="amount", dtype="decimal", description="Gross charge amount (positive = received)", unit="currency"),
                FieldAnnotation(name="amount_refunded", dtype="decimal", description="Amount refunded (always positive)", unit="currency"),
                FieldAnnotation(name="currency", dtype="string", description="Charge currency code", examples=["usd", "eur", "gbp"]),
                FieldAnnotation(name="converted_amount", dtype="decimal", description="Amount converted to payout currency", unit="converted_currency", nullable=True),
                FieldAnnotation(name="converted_amount_refunded", dtype="decimal", description="Refunded amount in payout currency", unit="converted_currency", nullable=True),
                FieldAnnotation(name="converted_currency", dtype="string", description="Currency amounts were converted to", nullable=True),
                FieldAnnotation(name="fee", dtype="decimal", description="Stripe processing fee", unit="currency"),
                FieldAnnotation(name="tax", dtype="decimal", description="Tax portion of fee", unit="currency", nullable=True),
                FieldAnnotation(name="mode", dtype="enum", description="live or test", enum_values=["live", "test"]),
                FieldAnnotation(name="status", dtype="string", description="Charge status", examples=["Paid", "Refunded", "Failed", "Disputed"]),
                FieldAnnotation(name="statement_descriptor", dtype="string", description="Text that appears on cardholder statement", nullable=True),
                FieldAnnotation(name="customer_id", dtype="string", description="Stripe customer ID", nullable=True),
                FieldAnnotation(name="customer_description", dtype="string", description="Customer description from Stripe dashboard", nullable=True),
                FieldAnnotation(name="customer_email", dtype="string", description="Customer email address", nullable=True),
                FieldAnnotation(name="captured", dtype="boolean", description="Whether charge was captured (False = authorized only)"),
                FieldAnnotation(name="card_last4", dtype="string", description="Last 4 digits of card used", nullable=True),
                FieldAnnotation(name="card_brand", dtype="string", description="Card network", examples=["Visa", "Mastercard", "Amex"], nullable=True),
                FieldAnnotation(name="card_funding", dtype="string", description="Card funding type", examples=["credit", "debit", "prepaid"], nullable=True),
                FieldAnnotation(name="transfer_id", dtype="string", description="Stripe transfer ID linking charge to payout", nullable=True),
                FieldAnnotation(name="transfer_date", dtype="date", description="Transfer date (UTC)", format="YYYY-MM-DD HH:MM:SS", nullable=True),
                FieldAnnotation(name="transfer_group", dtype="string", description="Transfer group label", nullable=True),
                FieldAnnotation(name="payout_id", dtype="string", description="Stripe payout ID", nullable=True),
                FieldAnnotation(name="payout_expected_arrival", dtype="date", description="Expected payout arrival date", format="YYYY-MM-DD", nullable=True),
                FieldAnnotation(name="payout_date", dtype="date", description="Actual payout date (UTC)", format="YYYY-MM-DD HH:MM:SS", nullable=True),
                FieldAnnotation(name="payout_type", dtype="string", description="Payout type", examples=["bank_account", "card"], nullable=True),
                FieldAnnotation(name="payout_status", dtype="string", description="Payout status", examples=["paid", "pending", "failed"], nullable=True),
                FieldAnnotation(name="payout_description", dtype="string", description="Payout description", nullable=True),
                FieldAnnotation(name="payout_destination", dtype="string", description="Bank account or card destination ID", nullable=True),
            ],
            conventions=[
                "Amounts are decimal strings (e.g. '9.99'), not integer cents.",
                "Currency codes are lowercase (usd, eur) — normalize before comparing with other sources.",
                "amount_refunded is always positive. Net = amount - fee - amount_refunded.",
                "Test mode rows (mode='test') are real CSV rows — filter them if only analyzing live revenue.",
                "A charge and its payout share the same payout_id — use this to reconcile receipts with bank deposits.",
                "Disputed charges may show status='Disputed' with amount_refunded=0 until the dispute resolves.",
                "created_at and transfer_date are UTC — convert to local timezone before comparing with bank statements.",
            ],
            notes=[
                "Stripe API uses amounts in cents (smallest currency unit), but CSV exports already use decimal notation.",
                "Test mode transactions (livemode=false / mode='test') may appear in exports alongside live data.",
                "Disputed and refunded transactions each get their own separate rows rather than modifying the original charge row.",
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
        raw = raw.strip().replace(",", "")
        if not raw:
            return None
        try:
            return Decimal(raw)
        except (InvalidOperation, Exception):
            logger.warning("Stripe: could not parse amount '%s', defaulting to None", raw)
            return None

    def _parse_datetime(self, raw: str) -> str:
        raw = raw.strip()
        if not raw:
            return raw
        # Try ISO-8601 with time
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw, fmt).isoformat()
            except ValueError:
                continue
        return raw

    def _parse_row(self, row: dict, col_lookup: dict) -> dict:
        captured_raw = self._get(row, col_lookup, "captured").lower()
        captured = captured_raw in ("true", "yes", "1")

        amount = self._parse_amount(self._get(row, col_lookup, "amount", "0"))
        amount_refunded = self._parse_amount(self._get(row, col_lookup, "amount_refunded", "0"))
        converted_amount = self._parse_amount(self._get(row, col_lookup, "converted_amount"))
        converted_amount_refunded = self._parse_amount(self._get(row, col_lookup, "converted_amount_refunded"))
        fee = self._parse_amount(self._get(row, col_lookup, "fee", "0"))
        tax = self._parse_amount(self._get(row, col_lookup, "tax"))

        return {
            "id": self._get(row, col_lookup, "id"),
            "description": self._get(row, col_lookup, "description") or None,
            "seller_message": self._get(row, col_lookup, "seller_message") or None,
            "created_at": self._parse_datetime(self._get(row, col_lookup, "created_at")),
            "amount": str(amount) if amount is not None else "0",
            "amount_refunded": str(amount_refunded) if amount_refunded is not None else "0",
            "currency": self._get(row, col_lookup, "currency", "").lower(),
            "converted_amount": str(converted_amount) if converted_amount is not None else None,
            "converted_amount_refunded": str(converted_amount_refunded) if converted_amount_refunded is not None else None,
            "converted_currency": self._get(row, col_lookup, "converted_currency") or None,
            "fee": str(fee) if fee is not None else "0",
            "tax": str(tax) if tax is not None else None,
            "mode": self._get(row, col_lookup, "mode", "live"),
            "status": self._get(row, col_lookup, "status"),
            "statement_descriptor": self._get(row, col_lookup, "statement_descriptor") or None,
            "customer_id": self._get(row, col_lookup, "customer_id") or None,
            "customer_description": self._get(row, col_lookup, "customer_description") or None,
            "customer_email": self._get(row, col_lookup, "customer_email") or None,
            "captured": captured,
            "card_last4": self._get(row, col_lookup, "card_last4") or None,
            "card_brand": self._get(row, col_lookup, "card_brand") or None,
            "card_funding": self._get(row, col_lookup, "card_funding") or None,
            "transfer_id": self._get(row, col_lookup, "transfer_id") or None,
            "transfer_date": self._parse_datetime(self._get(row, col_lookup, "transfer_date")) or None,
            "transfer_group": self._get(row, col_lookup, "transfer_group") or None,
            "payout_id": self._get(row, col_lookup, "payout_id") or None,
            "payout_expected_arrival": self._parse_datetime(self._get(row, col_lookup, "payout_expected_arrival")) or None,
            "payout_date": self._parse_datetime(self._get(row, col_lookup, "payout_date")) or None,
            "payout_type": self._get(row, col_lookup, "payout_type") or None,
            "payout_status": self._get(row, col_lookup, "payout_status") or None,
            "payout_description": self._get(row, col_lookup, "payout_description") or None,
            "payout_destination": self._get(row, col_lookup, "payout_destination") or None,
        }

    def _decode(self, content: bytes) -> str | None:
        for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(StripeParser())
