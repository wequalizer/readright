"""Monzo (UK) CSV export parser."""

from __future__ import annotations

import csv
import io
from datetime import date
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Monzo exports have a large, distinctive set of headers
MONZO_HEADERS = {
    "Transaction ID",
    "Date",
    "Time",
    "Type",
    "Name",
    "Emoji",
    "Category",
    "Amount",
    "Currency",
    "Local amount",
    "Local currency",
    "Notes and #tags",
    "Address",
    "Receipt",
    "Description",
    "Category split",
    "Money Out",
    "Money In",
    "Balance",
}

# Minimum required subset for detection
DETECT_REQUIRED = {
    "Transaction ID",
    "Date",
    "Type",
    "Amount",
    "Currency",
    "Money Out",
    "Money In",
}


class MonzoBankParser(BaseParser):
    """Parser for Monzo (UK) CSV exports.

    Monzo CSV quirks:
    - Very rich export format with 19 columns — one of the most detailed UK bank exports
    - Date and Time are in separate columns
    - Date format is DD/MM/YYYY (UK format)
    - Amount is signed: negative = debit (money out), positive = credit (money in)
    - ALSO has separate 'Money Out' and 'Money In' columns (positive values, never both populated)
    - 'Local amount' and 'Local currency' capture the original foreign currency amount
    - 'Notes and #tags' contains user-added notes AND hashtags from the Monzo app
    - 'Category' is user-defined or Monzo-inferred (e.g., "eating_out", "transport")
    - 'Category split' handles split transactions between multiple categories
    - 'Emoji' is the merchant emoji from the Monzo app — purely decorative
    - 'Receipt' may contain a receipt URL if linked
    - 'Transaction ID' is Monzo's internal UUID-style identifier
    - Encoding is UTF-8 with BOM
    """

    def source_type(self) -> str:
        return "monzo_csv_uk"

    def source_label(self) -> str:
        return "Monzo CSV Export (UK)"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        first_line = text.lstrip("\ufeff").split("\n")[0].strip()
        headers = {h.strip().strip('"') for h in first_line.split(",")}

        # Monzo's header set is extremely distinctive — near-exact match is very high confidence
        overlap = len(MONZO_HEADERS & headers)
        total = len(MONZO_HEADERS)

        if overlap == total:
            return 0.98
        if overlap >= total - 2:
            return 0.95
        if DETECT_REQUIRED.issubset(headers):
            return 0.90
        # 'Transaction ID', 'Emoji', 'Notes and #tags' together is uniquely Monzo
        if "Transaction ID" in headers and "Emoji" in headers and "Notes and #tags" in headers:
            return 0.88
        if "Transaction ID" in headers and "Money Out" in headers and "Money In" in headers:
            return 0.80

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(
                    name="transaction_id",
                    dtype="string",
                    description="Monzo's internal transaction identifier (UUID-style)",
                    examples=["tx_0000AbCdEfGhIjKlMnOpQr"],
                ),
                FieldAnnotation(
                    name="date",
                    dtype="date",
                    description="Transaction date (UK format in source)",
                    format="YYYY-MM-DD",
                ),
                FieldAnnotation(
                    name="time",
                    dtype="string",
                    description="Transaction time (local UK time)",
                    format="HH:MM:SS",
                    examples=["14:32:01", "09:15:44"],
                ),
                FieldAnnotation(
                    name="amount",
                    dtype="decimal",
                    description="Signed transaction amount: negative = debit (money out), positive = credit (money in)",
                    unit="GBP",
                    format="dot_decimal",
                    examples=["-4.50", "500.00", "-120.00"],
                ),
                FieldAnnotation(
                    name="currency",
                    dtype="string",
                    description="Account currency (GBP for standard Monzo accounts)",
                    examples=["GBP"],
                ),
                FieldAnnotation(
                    name="direction",
                    dtype="enum",
                    description="Derived from sign of amount: debit = money out, credit = money in",
                    enum_values=["debit", "credit"],
                ),
                FieldAnnotation(
                    name="name",
                    dtype="string",
                    description="Merchant or counterparty name as shown in the Monzo app",
                    examples=["Tesco", "Amazon", "Payroll"],
                ),
                FieldAnnotation(
                    name="type",
                    dtype="string",
                    description="Monzo transaction type",
                    examples=["card_payment", "faster_payment", "topup", "pot_transfer"],
                ),
                FieldAnnotation(
                    name="category",
                    dtype="string",
                    description="Monzo's spending category (user-editable in the app)",
                    nullable=True,
                    examples=["eating_out", "transport", "shopping", "bills", "income"],
                ),
                FieldAnnotation(
                    name="description",
                    dtype="string",
                    description="Transaction description — often the merchant's payment reference or a richer description than Name",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="notes_and_tags",
                    dtype="string",
                    description="User-added notes and hashtags from the Monzo app. Hashtags are prefixed with #.",
                    nullable=True,
                    examples=["team lunch #expenses", "#holiday"],
                ),
                FieldAnnotation(
                    name="local_amount",
                    dtype="decimal",
                    description="Transaction amount in the local (foreign) currency. Same as amount for GBP transactions.",
                    nullable=True,
                    examples=["5.00", "45.99"],
                ),
                FieldAnnotation(
                    name="local_currency",
                    dtype="string",
                    description="Currency of the local amount. Populated when spending abroad.",
                    nullable=True,
                    examples=["EUR", "USD", "GBP"],
                ),
                FieldAnnotation(
                    name="money_out",
                    dtype="decimal",
                    description="Amount leaving the account as a positive number. Empty for credits. Redundant with amount but provided for convenience.",
                    unit="GBP",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="money_in",
                    dtype="decimal",
                    description="Amount entering the account as a positive number. Empty for debits. Redundant with amount but provided for convenience.",
                    unit="GBP",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="balance",
                    dtype="decimal",
                    description="Account balance after this transaction",
                    unit="GBP",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="address",
                    dtype="string",
                    description="Physical address of the merchant if available",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="emoji",
                    dtype="string",
                    description="Merchant emoji from the Monzo app — decorative only, not semantic",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="receipt",
                    dtype="string",
                    description="URL to a linked receipt if one was attached",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="category_split",
                    dtype="string",
                    description="JSON-encoded split data when a transaction spans multiple Monzo categories",
                    nullable=True,
                ),
            ],
            conventions=[
                "Date format is DD/MM/YYYY in the raw file. Normalized to YYYY-MM-DD. Do NOT assume MM/DD/YYYY.",
                "Amount is ALREADY signed: negative = money out, positive = money in.",
                "'Money Out' and 'Money In' columns are always positive and redundant with the signed Amount — use Amount for calculations.",
                "Currency is GBP for standard accounts. 'Local currency' and 'Local amount' capture the original amount for foreign transactions.",
                "'Notes and #tags' blends user notes and hashtags — hashtags start with '#' and can be used for filtering.",
                "'Category split' is a raw JSON string for split-category transactions — parse separately if needed.",
                "Pot transfers appear as transactions but do not represent external money movement.",
                "'Emoji' is decorative; never rely on it for categorisation logic.",
                "The Time column reflects UK local time (BST/GMT depending on time of year).",
            ],
            notes=[
                "Monzo is one of the richest CSV export formats from UK banks — particularly useful for expense analysis.",
                "The 'Category' field is user-editable and not always reliable as a primary classifier.",
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
        # Normalize all keys
        row = {k.strip().strip('"') if k else k: v.strip().strip('"') if v else v
               for k, v in row.items() if k}

        # Date: DD/MM/YYYY
        raw_date = row.get("Date", "")
        tx_date = self._parse_date(raw_date)

        # Amount: signed
        raw_amount = row.get("Amount", "0").replace(",", "")
        try:
            amount = Decimal(raw_amount)
        except InvalidOperation:
            amount = Decimal("0")

        is_debit = amount < 0

        # Local amount
        raw_local = row.get("Local amount", "").replace(",", "")
        try:
            local_amount = Decimal(raw_local) if raw_local else None
        except InvalidOperation:
            local_amount = None

        # Money Out / Money In
        raw_out = row.get("Money Out", "").replace(",", "")
        raw_in = row.get("Money In", "").replace(",", "")
        try:
            money_out = Decimal(raw_out) if raw_out else None
        except InvalidOperation:
            money_out = None
        try:
            money_in = Decimal(raw_in) if raw_in else None
        except InvalidOperation:
            money_in = None

        # Balance
        raw_balance = row.get("Balance", "").replace(",", "")
        try:
            balance = Decimal(raw_balance) if raw_balance else None
        except InvalidOperation:
            balance = None

        return {
            "transaction_id": row.get("Transaction ID", "") or None,
            "date": str(tx_date),
            "time": row.get("Time", "") or None,
            "amount": str(amount),
            "currency": row.get("Currency", "GBP") or "GBP",
            "direction": "debit" if is_debit else "credit",
            "name": row.get("Name", "") or None,
            "type": row.get("Type", "") or None,
            "category": row.get("Category", "") or None,
            "description": row.get("Description", "") or None,
            "notes_and_tags": row.get("Notes and #tags", "") or None,
            "local_amount": str(local_amount) if local_amount is not None else None,
            "local_currency": row.get("Local currency", "") or None,
            "money_out": str(money_out) if money_out is not None else None,
            "money_in": str(money_in) if money_in is not None else None,
            "balance": str(balance) if balance is not None else None,
            "address": row.get("Address", "") or None,
            "emoji": row.get("Emoji", "") or None,
            "receipt": row.get("Receipt", "") or None,
            "category_split": row.get("Category split", "") or None,
        }

    def _parse_date(self, raw: str) -> date | str:
        """Parse DD/MM/YYYY (UK format) into date."""
        raw = raw.strip()
        if not raw:
            return raw
        if len(raw) == 10 and raw[2] == "/" and raw[5] == "/":
            try:
                return date(int(raw[6:10]), int(raw[3:5]), int(raw[0:2]))
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


registry.register(MonzoBankParser())
