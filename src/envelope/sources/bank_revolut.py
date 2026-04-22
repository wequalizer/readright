"""Revolut CSV export parser."""

from __future__ import annotations

import csv
import io
import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Revolut CSV headers — fixed, always English, regardless of app locale.
# The export from app / web produces this exact header row:
#   Type,Product,Started Date,Completed Date,Description,Amount,Fee,Currency,State,Balance
#
# Note: Revolut changed their CSV format multiple times.
# - Pre-2021: different column names (e.g. "Paid Out (EUR)" / "Paid In (EUR)")
# - 2021-2023: the format documented here
# - 2024+: same core columns, sometimes with additional metadata columns
#
# This parser handles the 2021+ format (most common in the wild) with fallbacks
# for detecting the older format.

COLUMN_MAP = {
    "Type": "type",
    "Product": "product",
    "Started Date": "started_date",
    "Completed Date": "completed_date",
    "Description": "description",
    "Amount": "amount",
    "Fee": "fee",
    "Currency": "currency",
    "State": "state",
    "Balance": "balance_after",
    # Older format columns (pre-2021)
    "Paid Out (EUR)": "paid_out",
    "Paid In (EUR)": "paid_in",
    "Exchange Out": "exchange_out",
    "Exchange In": "exchange_in",
    "Balance (EUR)": "balance_after",
    # Some newer exports include merchant detail
    "Merchant": "merchant",
    "Category": "category",
    "Reference": "reference",
}

# Revolut transaction states
COMPLETED_STATES = {"COMPLETED", "REVERTED"}
PENDING_STATES = {"PENDING", "IN PROGRESS", "INPROGRESS"}
FAILED_STATES = {"FAILED", "DECLINED", "REVERTED"}


class RevolutParser(BaseParser):
    """Parser for Revolut CSV exports.

    Revolut CSV quirks:
    - Delimiter is comma; headers are NOT quoted.
    - Amount is ALREADY signed: negative = debit (money out), positive = credit.
    - Decimal separator is DOT.
    - Two date columns: 'Started Date' (when initiated) and 'Completed Date'
      (when settled). We use 'Completed Date' as canonical date, falling back
      to 'Started Date' for pending transactions.
    - Date format is 'YYYY-MM-DD HH:MM:SS' (full datetime, not just date).
    - Multi-currency: 'Currency' column tells you which currency the account is in.
      Revolut accounts can hold multiple currencies — each currency is exported
      separately if you choose "All accounts" in the export.
    - 'Fee' is the Revolut fee charged (separate from the amount). It is always
      expressed as a non-negative number; it reduces what you receive / sends extra.
    - 'State' can be COMPLETED, PENDING, FAILED, REVERTED, DECLINED.
    - 'Type' values include: TRANSFER, CARD_PAYMENT, TOPUP, EXCHANGE, REFUND,
      CASHBACK, FEE, REWARD, etc.
    - 'Product' is typically 'Current', 'Savings', or the name of a Pocket.
    - Balance reflects the per-currency pocket balance, not total account value.
    - Encoding is UTF-8, usually without BOM.
    - The 'Description' field often contains merchant names for card payments and
      transfer notes for SEPA/SWIFT. For EXCHANGE type rows, it shows the currency pair.
    """

    def source_type(self) -> str:
        return "revolut_csv"

    def source_label(self) -> str:
        return "Revolut CSV Export"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        text = text.lstrip("\ufeff")
        first_line = text.split("\n")[0].strip()

        # High confidence: the definitive Revolut header signature
        if (
            "Started Date" in first_line
            and "Completed Date" in first_line
            and "State" in first_line
        ):
            return 0.97

        # Also check for older Revolut format
        if "Paid Out (EUR)" in first_line and "Paid In (EUR)" in first_line:
            return 0.90

        # Medium: Type + Product + Currency combo is distinctive
        if (
            first_line.startswith("Type,")
            and "Product" in first_line
            and "Currency" in first_line
        ):
            return 0.85

        # Weak: just State + Fee in a CSV
        if "State" in first_line and "Fee" in first_line and "Balance" in first_line:
            return 0.60

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(
                    name="date",
                    dtype="date",
                    description=(
                        "Transaction date derived from 'Completed Date' when available, "
                        "otherwise 'Started Date'. Use this for accounting."
                    ),
                    format="YYYY-MM-DD",
                ),
                FieldAnnotation(
                    name="started_at",
                    dtype="string",
                    description="Full datetime when the transaction was initiated",
                    format="YYYY-MM-DD HH:MM:SS",
                ),
                FieldAnnotation(
                    name="completed_at",
                    dtype="string",
                    description=(
                        "Full datetime when the transaction completed/settled. "
                        "Empty for pending transactions."
                    ),
                    format="YYYY-MM-DD HH:MM:SS",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="type",
                    dtype="string",
                    description="Transaction type",
                    examples=["TRANSFER", "CARD_PAYMENT", "TOPUP", "EXCHANGE", "REFUND", "FEE", "CASHBACK"],
                ),
                FieldAnnotation(
                    name="product",
                    dtype="string",
                    description="Revolut product/pocket name",
                    examples=["Current", "Savings"],
                ),
                FieldAnnotation(
                    name="description",
                    dtype="string",
                    description=(
                        "Merchant name for card payments; transfer note for SEPA/SWIFT; "
                        "currency pair for EXCHANGE (e.g. 'EUR to USD')."
                    ),
                ),
                FieldAnnotation(
                    name="amount",
                    dtype="decimal",
                    description=(
                        "Signed transaction amount: negative = debit (money out), "
                        "positive = credit (money in). This is the net movement, NOT "
                        "including the fee."
                    ),
                    format="dot_decimal",
                ),
                FieldAnnotation(
                    name="fee",
                    dtype="decimal",
                    description=(
                        "Revolut fee charged for this transaction. Always non-negative. "
                        "Zero for most SEPA transfers and card payments on standard plan."
                    ),
                ),
                FieldAnnotation(
                    name="currency",
                    dtype="string",
                    description=(
                        "Currency of this transaction and balance. Each Revolut currency "
                        "pocket exports separately."
                    ),
                    examples=["EUR", "USD", "GBP", "PLN"],
                ),
                FieldAnnotation(
                    name="state",
                    dtype="enum",
                    description="Transaction state",
                    enum_values=["COMPLETED", "PENDING", "FAILED", "REVERTED", "DECLINED"],
                ),
                FieldAnnotation(
                    name="balance_after",
                    dtype="decimal",
                    description=(
                        "Per-currency pocket balance after this transaction. "
                        "NOT total account value across all currencies."
                    ),
                    nullable=True,
                ),
                FieldAnnotation(
                    name="direction",
                    dtype="enum",
                    description="Derived from sign of amount",
                    enum_values=["debit", "credit"],
                ),
            ],
            conventions=[
                "Amount is ALREADY signed: negative = money out, positive = money in.",
                "Decimal separator is DOT throughout.",
                "Date format in the raw file is 'YYYY-MM-DD HH:MM:SS'. The 'date' output field is normalized to YYYY-MM-DD using Completed Date.",
                "For pending transactions, Completed Date is empty — Started Date is used as fallback date.",
                "EXCHANGE rows appear in PAIRS: one row debits the source currency, another credits the target currency. Both rows are included in the export for the respective currency account.",
                "Fee is a SEPARATE field from Amount — total cost of an outgoing payment is abs(amount) + fee.",
                "Balance shown is the per-currency pocket balance, not your total Revolut portfolio value.",
                "Multi-currency: export a single currency at a time or 'All accounts'. Each currency section has its own running balance.",
                "CARD_PAYMENT transactions at merchants show the merchant name in Description. Terminal IDs are not included.",
                "TOPUP transactions represent money coming INTO Revolut from your linked bank account.",
                "REVERTED transactions have a matching COMPLETED transaction — both appear in the export. Net effect is zero but both rows are present.",
                "State 'PENDING' means the transaction is not yet settled — do not include in finalized accounting.",
            ],
            notes=[
                "Multi-currency accounts show each transaction in its original currency, not converted to a home currency.",
                "FX transactions generate two rows showing both currencies with the applied exchange rate.",
                "Pending transactions have no completed_at timestamp — use started_at as the only available date.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        text = text.lstrip("\ufeff")

        # Revolut uses comma delimiter, unquoted headers
        reader = csv.DictReader(io.StringIO(text), delimiter=",")
        if not reader.fieldnames:
            return ParseResult(success=False, error="No CSV headers found")

        # Build column lookup
        col_lookup: dict[str, str] = {}
        for header in reader.fieldnames:
            clean = header.strip()
            if clean in COLUMN_MAP:
                col_lookup[COLUMN_MAP[clean]] = header

        # Detect format generation
        is_old_format = "paid_out" in col_lookup or "paid_in" in col_lookup

        rows = []
        warnings = []

        for i, row in enumerate(reader):
            try:
                if is_old_format:
                    parsed = self._parse_row_old(row, col_lookup)
                else:
                    parsed = self._parse_row(row, col_lookup)
                rows.append(parsed)
            except Exception as e:
                warnings.append(f"Row {i + 1}: {e}")

        if not rows:
            return ParseResult(success=False, error="No rows could be parsed")

        if is_old_format:
            warnings.insert(0, "Detected older Revolut CSV format (pre-2021). Some fields may be missing.")

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
        return (row.get(header) or default).strip()

    def _parse_row(self, row: dict, col_lookup: dict) -> dict:
        started_raw = self._get(row, col_lookup, "started_date")
        completed_raw = self._get(row, col_lookup, "completed_date")

        started_dt = self._parse_datetime(started_raw)
        completed_dt = self._parse_datetime(completed_raw) if completed_raw else None

        # Use completed date as canonical; fall back to started
        canonical_date = (completed_dt or started_dt)
        if hasattr(canonical_date, "date"):
            tx_date = canonical_date.date()
        else:
            tx_date = canonical_date

        raw_amount = self._get(row, col_lookup, "amount", "0")
        amount = self._parse_amount(raw_amount)
        is_debit = amount < 0

        raw_fee = self._get(row, col_lookup, "fee", "0")
        fee = self._parse_amount(raw_fee)

        raw_balance = self._get(row, col_lookup, "balance_after")
        balance = self._parse_amount(raw_balance) if raw_balance else None

        return {
            "date": str(tx_date),
            "started_at": started_raw,
            "completed_at": completed_raw or None,
            "type": self._get(row, col_lookup, "type"),
            "product": self._get(row, col_lookup, "product"),
            "description": self._get(row, col_lookup, "description"),
            "amount": str(amount),
            "fee": str(fee),
            "currency": self._get(row, col_lookup, "currency", "EUR"),
            "state": self._get(row, col_lookup, "state"),
            "balance_after": str(balance) if balance is not None else None,
            "direction": "debit" if is_debit else "credit",
        }

    def _parse_row_old(self, row: dict, col_lookup: dict) -> dict:
        """Parse the older Revolut CSV format (pre-2021)."""
        # Old format: separate paid_out and paid_in columns
        started_raw = self._get(row, col_lookup, "started_date")
        completed_raw = self._get(row, col_lookup, "completed_date")

        started_dt = self._parse_datetime(started_raw)
        completed_dt = self._parse_datetime(completed_raw) if completed_raw else None
        canonical_date = completed_dt or started_dt
        if hasattr(canonical_date, "date"):
            tx_date = canonical_date.date()
        else:
            tx_date = canonical_date

        paid_out = self._parse_amount(self._get(row, col_lookup, "paid_out", "0"))
        paid_in = self._parse_amount(self._get(row, col_lookup, "paid_in", "0"))
        # Net amount: credit - debit
        amount = paid_in - paid_out
        is_debit = amount < 0

        raw_balance = self._get(row, col_lookup, "balance_after")
        balance = self._parse_amount(raw_balance) if raw_balance else None

        return {
            "date": str(tx_date),
            "started_at": started_raw,
            "completed_at": completed_raw or None,
            "type": self._get(row, col_lookup, "type", "UNKNOWN"),
            "product": self._get(row, col_lookup, "product", ""),
            "description": self._get(row, col_lookup, "description"),
            "amount": str(amount),
            "fee": "0",
            "currency": self._get(row, col_lookup, "currency", "EUR"),
            "state": "COMPLETED",  # Old format has no State column
            "balance_after": str(balance) if balance is not None else None,
            "direction": "debit" if is_debit else "credit",
        }

    def _parse_datetime(self, raw: str) -> datetime | date | str:
        raw = raw.strip()
        if not raw:
            return raw
        # YYYY-MM-DD HH:MM:SS
        if len(raw) == 19 and raw[4] == "-" and raw[10] == " ":
            try:
                return datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass
        # YYYY-MM-DD HH:MM:SS (with T separator)
        if len(raw) >= 19 and raw[4] == "-" and raw[10] == "T":
            try:
                return datetime.strptime(raw[:19], "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                pass
        # YYYY-MM-DD
        if len(raw) == 10 and raw[4] == "-":
            try:
                return date(int(raw[:4]), int(raw[5:7]), int(raw[8:10]))
            except ValueError:
                pass
        # DD/MM/YYYY HH:MM:SS (some locale variants)
        if len(raw) >= 10 and raw[2] == "/" and raw[5] == "/":
            try:
                return datetime.strptime(raw[:19], "%d/%m/%Y %H:%M:%S")
            except ValueError:
                pass
        return raw

    def _parse_amount(self, raw: str) -> Decimal:
        raw = raw.strip().replace(" ", "")
        if not raw:
            return Decimal("0")
        if "," in raw and "." in raw:
            if raw.rfind(",") > raw.rfind("."):
                raw = raw.replace(".", "").replace(",", ".")
            else:
                raw = raw.replace(",", "")
        elif "," in raw:
            raw = raw.replace(",", ".")
        try:
            return Decimal(raw)
        except (InvalidOperation, Exception):
            raise ValueError(f"Revolut: could not parse amount '{raw}'")

    def _decode(self, content: bytes) -> str | None:
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(RevolutParser())
