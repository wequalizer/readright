"""Shopify orders CSV export parser."""

from __future__ import annotations

import csv
import io
from datetime import datetime
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

COLUMN_MAP = {
    "Name": "order_name",
    "Email": "email",
    "Financial Status": "financial_status",
    "Paid at": "paid_at",
    "Fulfillment Status": "fulfillment_status",
    "Fulfilled at": "fulfilled_at",
    "Currency": "currency",
    "Subtotal": "subtotal",
    "Shipping": "shipping",
    "Taxes": "taxes",
    "Total": "total",
    "Discount Code": "discount_code",
    "Discount Amount": "discount_amount",
    "Lineitem quantity": "lineitem_quantity",
    "Lineitem name": "lineitem_name",
    "Lineitem price": "lineitem_price",
    "Lineitem sku": "lineitem_sku",
    "Billing Name": "billing_name",
    "Billing Street": "billing_street",
    "Billing City": "billing_city",
    "Billing Province": "billing_province",
    "Billing Country": "billing_country",
    "Billing Zip": "billing_zip",
    "Shipping Name": "shipping_name",
    "Shipping Street": "shipping_street",
    "Shipping City": "shipping_city",
    "Shipping Province": "shipping_province",
    "Shipping Country": "shipping_country",
    "Shipping Zip": "shipping_zip",
    "Payment Method": "payment_method",
    "Created at": "created_at",
    "Notes": "notes",
}


class ShopifyParser(BaseParser):
    """Parser for Shopify orders CSV exports.

    Shopify quirks:
    - A single order with multiple line items appears as multiple rows, one per
      line item. Only the first row for an order has the order-level fields
      (total, billing/shipping address, etc.) — subsequent rows for the same
      order have those fields blank.
    - 'Name' is the order number, e.g. '#1001'.
    - Financial Status values: paid, pending, refunded, partially_refunded, voided.
    - Fulfillment Status values: fulfilled, partial, unfulfilled (or blank).
    - Timestamps are ISO-8601 with timezone offset (e.g. '2024-01-15 09:23:11 +0000').
    - Currency is store currency — may differ from customer's local currency.
    """

    def source_type(self) -> str:
        return "shopify_orders_csv"

    def source_label(self) -> str:
        return "Shopify Orders CSV Export"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        first_line = text.lstrip("\ufeff").split("\n")[0]

        if "Financial Status" in first_line and "Lineitem quantity" in first_line:
            return 0.97
        if "Fulfillment Status" in first_line and "Lineitem name" in first_line:
            return 0.93
        if "Lineitem sku" in first_line and "Billing Province" in first_line:
            return 0.80

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="order_name", dtype="string", description="Order number (e.g. #1001)", examples=["#1001", "#1002"]),
                FieldAnnotation(name="email", dtype="string", description="Customer email address", nullable=True),
                FieldAnnotation(name="financial_status", dtype="enum", description="Payment status of the order", enum_values=["paid", "pending", "refunded", "partially_refunded", "voided"]),
                FieldAnnotation(name="paid_at", dtype="date", description="Timestamp when payment was captured", format="YYYY-MM-DD HH:MM:SS +ZZZZ", nullable=True),
                FieldAnnotation(name="fulfillment_status", dtype="string", description="Fulfillment status", examples=["fulfilled", "partial", "unfulfilled"], nullable=True),
                FieldAnnotation(name="fulfilled_at", dtype="date", description="Timestamp when order was fulfilled", format="YYYY-MM-DD HH:MM:SS +ZZZZ", nullable=True),
                FieldAnnotation(name="currency", dtype="string", description="Store currency code", examples=["USD", "EUR", "GBP"]),
                FieldAnnotation(name="subtotal", dtype="decimal", description="Order subtotal before shipping and taxes (blank for non-first line item rows)", unit="currency", nullable=True),
                FieldAnnotation(name="shipping", dtype="decimal", description="Shipping cost", unit="currency", nullable=True),
                FieldAnnotation(name="taxes", dtype="decimal", description="Total taxes charged", unit="currency", nullable=True),
                FieldAnnotation(name="total", dtype="decimal", description="Order total including shipping and taxes (blank for non-first line item rows)", unit="currency", nullable=True),
                FieldAnnotation(name="discount_code", dtype="string", description="Discount code applied", nullable=True),
                FieldAnnotation(name="discount_amount", dtype="decimal", description="Total discount amount", unit="currency", nullable=True),
                FieldAnnotation(name="lineitem_quantity", dtype="integer", description="Quantity of this line item"),
                FieldAnnotation(name="lineitem_name", dtype="string", description="Product title and variant"),
                FieldAnnotation(name="lineitem_price", dtype="decimal", description="Unit price of this line item", unit="currency"),
                FieldAnnotation(name="lineitem_sku", dtype="string", description="SKU of this line item", nullable=True),
                FieldAnnotation(name="billing_name", dtype="string", description="Billing contact name", nullable=True),
                FieldAnnotation(name="billing_country", dtype="string", description="Billing country code", examples=["US", "GB", "NL"], nullable=True),
                FieldAnnotation(name="shipping_name", dtype="string", description="Shipping contact name", nullable=True),
                FieldAnnotation(name="shipping_country", dtype="string", description="Shipping country code", nullable=True),
                FieldAnnotation(name="payment_method", dtype="string", description="Payment gateway used", examples=["Shopify Payments", "PayPal", "Manual"], nullable=True),
                FieldAnnotation(name="created_at", dtype="date", description="Order creation timestamp", format="YYYY-MM-DD HH:MM:SS +ZZZZ"),
                FieldAnnotation(name="notes", dtype="string", description="Order notes from customer", nullable=True),
            ],
            conventions=[
                "Multi-line-item orders appear as multiple rows. Only the first row carries order-level fields (total, billing/shipping address). Subsequent rows are blank for those columns.",
                "Group rows by order_name to reconstruct full orders.",
                "order_name always starts with '#' — strip it for numeric sorting.",
                "All monetary amounts use the store's base currency (currency column). Shopify does not export the customer's local payment currency here.",
                "paid_at is null for unpaid/voided orders.",
                "Discount amount is positive (already subtracted from subtotal in the total).",
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
        except InvalidOperation:
            return None

    def _parse_int(self, raw: str) -> int | None:
        raw = raw.strip()
        if not raw:
            return None
        try:
            return int(raw)
        except ValueError:
            return None

    def _parse_datetime(self, raw: str) -> str | None:
        raw = raw.strip()
        if not raw:
            return None
        # Shopify: "2024-01-15 09:23:11 +0000"
        for fmt in ("%Y-%m-%d %H:%M:%S %z", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw, fmt).isoformat()
            except ValueError:
                continue
        return raw

    def _parse_row(self, row: dict, col_lookup: dict) -> dict:
        return {
            "order_name": self._get(row, col_lookup, "order_name"),
            "email": self._get(row, col_lookup, "email") or None,
            "financial_status": self._get(row, col_lookup, "financial_status") or None,
            "paid_at": self._parse_datetime(self._get(row, col_lookup, "paid_at")),
            "fulfillment_status": self._get(row, col_lookup, "fulfillment_status") or None,
            "fulfilled_at": self._parse_datetime(self._get(row, col_lookup, "fulfilled_at")),
            "currency": self._get(row, col_lookup, "currency"),
            "subtotal": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "subtotal"))) is not None else None,
            "shipping": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "shipping"))) is not None else None,
            "taxes": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "taxes"))) is not None else None,
            "total": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "total"))) is not None else None,
            "discount_code": self._get(row, col_lookup, "discount_code") or None,
            "discount_amount": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "discount_amount"))) is not None else None,
            "lineitem_quantity": self._parse_int(self._get(row, col_lookup, "lineitem_quantity")),
            "lineitem_name": self._get(row, col_lookup, "lineitem_name") or None,
            "lineitem_price": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "lineitem_price"))) is not None else None,
            "lineitem_sku": self._get(row, col_lookup, "lineitem_sku") or None,
            "billing_name": self._get(row, col_lookup, "billing_name") or None,
            "billing_country": self._get(row, col_lookup, "billing_country") or None,
            "shipping_name": self._get(row, col_lookup, "shipping_name") or None,
            "shipping_country": self._get(row, col_lookup, "shipping_country") or None,
            "payment_method": self._get(row, col_lookup, "payment_method") or None,
            "created_at": self._parse_datetime(self._get(row, col_lookup, "created_at")),
            "notes": self._get(row, col_lookup, "notes") or None,
        }

    def _decode(self, content: bytes) -> str | None:
        for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(ShopifyParser())
