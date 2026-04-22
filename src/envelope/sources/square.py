"""Square transactions CSV export parser."""

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

# Square's POS export has evolved across versions. These are the stable column
# names across the main Dashboard → Transactions CSV and the Sales Summary CSV.
COLUMN_MAP = {
    # Core transaction fields
    "Date": "date",
    "Time": "time",
    "Time Zone": "timezone",
    "Description": "description",
    "Amount": "amount",
    "Tip Amount": "tip_amount",
    "Discount Amount": "discount_amount",
    "Surcharge Amount": "surcharge_amount",
    "Fees": "fees",
    "Net Total": "net_total",
    "Tax Amount": "tax_amount",
    "Transaction ID": "transaction_id",
    "Payment ID": "payment_id",
    "Card Brand": "card_brand",
    "PAN Suffix": "card_last4",
    "Device Name": "device_name",
    "Staff Name": "staff_name",
    "Staff ID": "staff_id",
    "Location": "location",
    "Location ID": "location_id",
    "Currency": "currency",
    "Transaction Status": "status",
    "Source": "source",
    "Card Entry Methods": "card_entry_method",
    "Cash": "cash_tendered",
    "Change Back": "change_back",
    "Gift Card ID": "gift_card_id",
    "Customer ID": "customer_id",
    "Customer Name": "customer_name",
    "Customer Reference ID": "customer_reference_id",
    "Refund Reason": "refund_reason",
    "Details": "details",
    "Event Type": "event_type",
    "Gross Sales": "gross_sales",
    "Returns": "returns",
    "Discounts": "discounts",
    "Net Sales": "net_sales",
    "Taxes": "taxes_total",
    "Partial Refunds": "partial_refunds",
}


class SquareParser(BaseParser):
    """Parser for Square POS transaction CSV exports.

    Square quirks:
    - Two common export formats: 'Transactions' (one row per tender/payment) and
      'Item Sales' (one row per line item). This parser targets the Transactions format.
    - Amounts are always positive — refunds/returns are distinguished by Transaction
      Status ('Refund') or Event Type, not by sign.
    - 'Fees' is Square's processing fee — net_total = amount + tip - discount - fees.
    - PAN Suffix = last 4 digits of card. Card Brand uses Square's names ('Visa', 'Mastercard', etc.).
    - Currency is always the merchant's configured currency (typically USD).
    - Date and Time are in the merchant's local timezone (recorded in Time Zone column).
    - Device Name is the name assigned to the Square reader/terminal in the dashboard.
    - Cash transactions have Card Brand blank and cash_tendered/change_back populated.
    - Square Online orders appear with Source = 'Square Online'.
    """

    def source_type(self) -> str:
        return "square_csv"

    def source_label(self) -> str:
        return "Square POS Transactions CSV Export"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        first_line = text.lstrip("\ufeff").split("\n")[0]

        # 'PAN Suffix' is very Square-specific
        if "PAN Suffix" in first_line and "Transaction ID" in first_line:
            return 0.97
        if "Device Name" in first_line and "Net Total" in first_line:
            return 0.93
        if "PAN Suffix" in first_line and "Fees" in first_line:
            return 0.88
        # Square item sales export
        if "Gross Sales" in first_line and "Net Sales" in first_line and "Device Name" in first_line:
            return 0.82

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="date", dtype="date", description="Transaction date in merchant local timezone", format="MM/DD/YYYY or YYYY-MM-DD"),
                FieldAnnotation(name="time", dtype="string", description="Transaction time in merchant local timezone", format="HH:MM:SS AM/PM"),
                FieldAnnotation(name="timezone", dtype="string", description="Merchant's local timezone", examples=["America/New_York", "America/Los_Angeles"]),
                FieldAnnotation(name="description", dtype="string", description="Transaction description or item summary", nullable=True),
                FieldAnnotation(name="amount", dtype="decimal", description="Gross transaction amount before tips and discounts", unit="currency"),
                FieldAnnotation(name="tip_amount", dtype="decimal", description="Tip added to the transaction", unit="currency"),
                FieldAnnotation(name="discount_amount", dtype="decimal", description="Discount applied (positive value representing reduction)", unit="currency"),
                FieldAnnotation(name="surcharge_amount", dtype="decimal", description="Surcharge added (e.g. credit card surcharge)", unit="currency", nullable=True),
                FieldAnnotation(name="fees", dtype="decimal", description="Square processing fee charged to merchant", unit="currency"),
                FieldAnnotation(name="net_total", dtype="decimal", description="Amount deposited to merchant: amount + tip - discount - fees", unit="currency"),
                FieldAnnotation(name="tax_amount", dtype="decimal", description="Tax collected", unit="currency"),
                FieldAnnotation(name="transaction_id", dtype="string", description="Unique Square transaction ID"),
                FieldAnnotation(name="payment_id", dtype="string", description="Square payment ID (may differ from transaction_id for split tenders)", nullable=True),
                FieldAnnotation(name="card_brand", dtype="string", description="Card network", examples=["Visa", "Mastercard", "Amex", "Discover"], nullable=True),
                FieldAnnotation(name="card_last4", dtype="string", description="Last 4 digits of card (PAN Suffix)", nullable=True),
                FieldAnnotation(name="card_entry_method", dtype="string", description="How card was read", examples=["Chip", "Contactless", "Swipe", "Keyed"], nullable=True),
                FieldAnnotation(name="device_name", dtype="string", description="Name of the Square terminal/reader that processed the transaction", nullable=True),
                FieldAnnotation(name="staff_name", dtype="string", description="Staff member who processed the transaction", nullable=True),
                FieldAnnotation(name="location", dtype="string", description="Merchant location name", nullable=True),
                FieldAnnotation(name="currency", dtype="string", description="Transaction currency", examples=["USD", "CAD", "GBP"]),
                FieldAnnotation(name="status", dtype="string", description="Transaction status", examples=["Complete", "Refund", "Partial Refund", "Failed", "Pending"]),
                FieldAnnotation(name="source", dtype="string", description="Sales channel", examples=["In-Person", "Square Online", "Square Invoices", "Square for Restaurants"], nullable=True),
                FieldAnnotation(name="cash_tendered", dtype="decimal", description="Cash given by customer (cash transactions only)", unit="currency", nullable=True),
                FieldAnnotation(name="change_back", dtype="decimal", description="Change returned to customer (cash transactions only)", unit="currency", nullable=True),
                FieldAnnotation(name="customer_id", dtype="string", description="Square customer ID if customer was on file", nullable=True),
                FieldAnnotation(name="customer_name", dtype="string", description="Customer name from Square directory", nullable=True),
                FieldAnnotation(name="refund_reason", dtype="string", description="Reason recorded for refund transactions", nullable=True),
                FieldAnnotation(name="event_type", dtype="string", description="Type of event (present in some export versions)", examples=["Sale", "Refund", "Void"], nullable=True),
            ],
            conventions=[
                "All amounts are positive — refunds are identified by status='Refund' or event_type='Refund', not by negative sign.",
                "net_total = amount + tip - discount_amount - fees. This is what the merchant actually receives.",
                "Cash transactions have no card_brand or card_last4. cash_tendered and change_back are populated instead.",
                "Date/Time are in the merchant's local timezone (Time Zone column). Convert to UTC for cross-timezone comparisons.",
                "Square Online orders appear with source='Square Online' and may have no device_name.",
                "A refund creates a new row — the original sale row is not modified.",
                "Partial refunds have status='Partial Refund' with the partial refund amount in the Amount column.",
                "taxes in amount column are included in the gross amount — tax_amount is purely informational.",
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
        """Handle '$25.00', '25.00', '(10.00)' for negative, etc."""
        raw = raw.strip()
        if not raw:
            return None
        # Parentheses notation for negatives: (10.00) → -10.00
        negative = raw.startswith("(") and raw.endswith(")")
        raw = raw.strip("()").lstrip("$€£").replace(",", "")
        if not raw:
            return None
        try:
            value = Decimal(raw)
            return -value if negative else value
        except (InvalidOperation, Exception):
            logger.warning("Square: could not parse amount '%s', defaulting to None", raw)
            return None

    def _parse_date(self, raw: str) -> str | None:
        raw = raw.strip()
        if not raw:
            return None
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw, fmt).date().isoformat()
            except ValueError:
                continue
        return raw

    def _parse_time(self, raw: str) -> str:
        """Normalize time to HH:MM:SS — handles '3:45:01 PM' etc."""
        raw = raw.strip()
        if not raw:
            return raw
        for fmt in ("%I:%M:%S %p", "%H:%M:%S", "%I:%M %p", "%H:%M"):
            try:
                return datetime.strptime(raw, fmt).strftime("%H:%M:%S")
            except ValueError:
                continue
        return raw

    def _parse_row(self, row: dict, col_lookup: dict) -> dict:
        return {
            "date": self._parse_date(self._get(row, col_lookup, "date")),
            "time": self._parse_time(self._get(row, col_lookup, "time")),
            "timezone": self._get(row, col_lookup, "timezone") or None,
            "description": self._get(row, col_lookup, "description") or None,
            "amount": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "amount"))) is not None else None,
            "tip_amount": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "tip_amount"))) is not None else "0",
            "discount_amount": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "discount_amount"))) is not None else "0",
            "surcharge_amount": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "surcharge_amount"))) is not None else None,
            "fees": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "fees"))) is not None else None,
            "net_total": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "net_total"))) is not None else None,
            "tax_amount": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "tax_amount"))) is not None else "0",
            "transaction_id": self._get(row, col_lookup, "transaction_id") or None,
            "payment_id": self._get(row, col_lookup, "payment_id") or None,
            "card_brand": self._get(row, col_lookup, "card_brand") or None,
            "card_last4": self._get(row, col_lookup, "card_last4") or None,
            "card_entry_method": self._get(row, col_lookup, "card_entry_method") or None,
            "device_name": self._get(row, col_lookup, "device_name") or None,
            "staff_name": self._get(row, col_lookup, "staff_name") or None,
            "location": self._get(row, col_lookup, "location") or None,
            "currency": self._get(row, col_lookup, "currency", "USD"),
            "status": self._get(row, col_lookup, "status") or None,
            "source": self._get(row, col_lookup, "source") or None,
            "cash_tendered": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "cash_tendered"))) is not None else None,
            "change_back": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "change_back"))) is not None else None,
            "customer_id": self._get(row, col_lookup, "customer_id") or None,
            "customer_name": self._get(row, col_lookup, "customer_name") or None,
            "refund_reason": self._get(row, col_lookup, "refund_reason") or None,
            "event_type": self._get(row, col_lookup, "event_type") or None,
        }

    def _decode(self, content: bytes) -> str | None:
        for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(SquareParser())
