"""Amazon order history CSV export parser."""

from __future__ import annotations

import csv
import io
from datetime import datetime
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

COLUMN_MAP = {
    "Order ID": "order_id",
    "Order Date": "order_date",
    "Purchase Order Number": "purchase_order_number",
    "Currency": "currency",
    "Unit Price": "unit_price",
    "Unit Price Tax": "unit_price_tax",
    "Shipping Charge": "shipping_charge",
    "Total Discounts": "total_discounts",
    "Total Owed": "total_owed",
    "Shipment Item Subtotal": "shipment_item_subtotal",
    "Shipment Item Subtotal Tax": "shipment_item_subtotal_tax",
    "ASIN": "asin",
    "Product Condition": "product_condition",
    "Quantity": "quantity",
    "Payment Instrument Type": "payment_instrument_type",
    "Order Status": "order_status",
    "Shipping Address": "shipping_address",
    "Category": "category",
    "Item Subtotal": "item_subtotal",
    "Item Subtotal Tax": "item_subtotal_tax",
}


class AmazonOrdersParser(BaseParser):
    """Parser for Amazon order history CSV exports.

    Amazon quirks:
    - Each row is one ordered item (not one order). An order with 3 items = 3 rows.
    - 'Order ID' is shared across all items in the same order (e.g. '123-4567890-1234567').
    - 'Total Owed' represents the full order total — it repeats on every row for that order.
      Sum item_subtotal + shipment_item_subtotal instead of total_owed to avoid double-counting.
    - Discounts are negative in 'Total Discounts'.
    - Order Date format: MM/DD/YY (US) or YYYY-MM-DD depending on locale.
    - 'ASIN' is Amazon's internal product ID — useful for product lookups.
    - Shipping Address is a single concatenated string, not split by field.
    - Payment Instrument Type may be masked (e.g. 'Visa ending in 1234').
    """

    def source_type(self) -> str:
        return "amazon_orders_csv"

    def source_label(self) -> str:
        return "Amazon Order History CSV Export"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        first_line = text.lstrip("\ufeff").split("\n")[0]

        # ASIN + Order ID combination is unique to Amazon
        if "ASIN" in first_line and "Order ID" in first_line:
            return 0.97
        if "Total Owed" in first_line and "Shipment Item Subtotal" in first_line:
            return 0.93
        if "ASIN" in first_line and "Order Date" in first_line:
            return 0.80

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="order_id", dtype="string", description="Amazon order ID", examples=["123-4567890-1234567"]),
                FieldAnnotation(name="order_date", dtype="date", description="Date order was placed", format="MM/DD/YY or YYYY-MM-DD"),
                FieldAnnotation(name="purchase_order_number", dtype="string", description="Business purchase order number (blank for consumer accounts)", nullable=True),
                FieldAnnotation(name="currency", dtype="string", description="Currency of the transaction", examples=["USD", "EUR", "GBP"]),
                FieldAnnotation(name="unit_price", dtype="decimal", description="Price per unit of this item", unit="currency"),
                FieldAnnotation(name="unit_price_tax", dtype="decimal", description="Tax on unit price", unit="currency"),
                FieldAnnotation(name="shipping_charge", dtype="decimal", description="Shipping charge for this item", unit="currency"),
                FieldAnnotation(name="total_discounts", dtype="decimal", description="Discounts applied (negative value)", unit="currency"),
                FieldAnnotation(name="total_owed", dtype="decimal", description="Total order amount — repeats on every row for multi-item orders. Do NOT sum this column.", unit="currency"),
                FieldAnnotation(name="shipment_item_subtotal", dtype="decimal", description="Item subtotal within its shipment", unit="currency"),
                FieldAnnotation(name="shipment_item_subtotal_tax", dtype="decimal", description="Tax on shipment item subtotal", unit="currency"),
                FieldAnnotation(name="asin", dtype="string", description="Amazon Standard Identification Number", examples=["B08N5WRWNW"]),
                FieldAnnotation(name="product_condition", dtype="string", description="Item condition", examples=["new", "used", "refurbished"]),
                FieldAnnotation(name="quantity", dtype="integer", description="Quantity ordered"),
                FieldAnnotation(name="payment_instrument_type", dtype="string", description="Payment method used", examples=["Visa ending in 1234", "Amazon Pay"]),
                FieldAnnotation(name="order_status", dtype="string", description="Current order status", examples=["Shipped", "Delivered", "Cancelled", "Returned"]),
                FieldAnnotation(name="shipping_address", dtype="string", description="Full shipping address as a single concatenated string", nullable=True),
                FieldAnnotation(name="category", dtype="string", description="Amazon product category", examples=["Books", "Electronics", "Clothing"]),
                FieldAnnotation(name="item_subtotal", dtype="decimal", description="Subtotal for this item (unit_price * quantity)", unit="currency"),
                FieldAnnotation(name="item_subtotal_tax", dtype="decimal", description="Tax on item subtotal", unit="currency"),
            ],
            conventions=[
                "Each row represents one item, not one order. Group by order_id for order-level analysis.",
                "total_owed repeats for every item in an order — do NOT sum it. Use item_subtotal + shipment_item_subtotal for item-level totals.",
                "total_discounts is negative (e.g. -5.00 means $5 off).",
                "Order date format varies by Amazon locale (US: MM/DD/YY, international: YYYY-MM-DD).",
                "ASIN is Amazon's product identifier — use it to join with product metadata if available.",
                "Shipping address is one unstructured string — not suitable for field-level parsing without additional logic.",
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
        raw = raw.strip().lstrip("$€£").replace(",", "")
        if not raw:
            return None
        try:
            return Decimal(raw)
        except InvalidOperation:
            return None

    def _parse_int(self, raw: str) -> int | None:
        raw = raw.strip()
        if not raw:
            return None
        try:
            return int(float(raw))
        except (ValueError, OverflowError):
            return None

    def _parse_date(self, raw: str) -> str | None:
        raw = raw.strip()
        if not raw:
            return None
        # MM/DD/YY  (US Amazon)
        for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw, fmt).date().isoformat()
            except ValueError:
                continue
        return raw

    def _parse_row(self, row: dict, col_lookup: dict) -> dict:
        return {
            "order_id": self._get(row, col_lookup, "order_id"),
            "order_date": self._parse_date(self._get(row, col_lookup, "order_date")),
            "purchase_order_number": self._get(row, col_lookup, "purchase_order_number") or None,
            "currency": self._get(row, col_lookup, "currency", "USD"),
            "unit_price": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "unit_price"))) is not None else None,
            "unit_price_tax": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "unit_price_tax"))) is not None else None,
            "shipping_charge": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "shipping_charge"))) is not None else None,
            "total_discounts": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "total_discounts"))) is not None else None,
            "total_owed": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "total_owed"))) is not None else None,
            "shipment_item_subtotal": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "shipment_item_subtotal"))) is not None else None,
            "shipment_item_subtotal_tax": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "shipment_item_subtotal_tax"))) is not None else None,
            "asin": self._get(row, col_lookup, "asin") or None,
            "product_condition": self._get(row, col_lookup, "product_condition") or None,
            "quantity": self._parse_int(self._get(row, col_lookup, "quantity")),
            "payment_instrument_type": self._get(row, col_lookup, "payment_instrument_type") or None,
            "order_status": self._get(row, col_lookup, "order_status") or None,
            "shipping_address": self._get(row, col_lookup, "shipping_address") or None,
            "category": self._get(row, col_lookup, "category") or None,
            "item_subtotal": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "item_subtotal"))) is not None else None,
            "item_subtotal_tax": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "item_subtotal_tax"))) is not None else None,
        }

    def _decode(self, content: bytes) -> str | None:
        for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(AmazonOrdersParser())
