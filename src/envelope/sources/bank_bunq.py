"""Bunq CSV export parser."""

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

# Bunq CSV headers are fixed and always in English regardless of app language.
# The export from the Bunq app / website produces:
#   "Date","Interest Date","Amount","Account","Counterparty","Name","Description"
#
# Bunq also supports exporting MT940 and CAMT.053, but this parser targets the CSV.
# The mobile app sometimes produces a slightly different column set — we handle both.

COLUMN_MAP = {
    "Date": "date",
    "Interest Date": "interest_date",
    "Amount": "amount",
    "Account": "account_iban",
    "Counterparty": "counterparty_iban",
    "Name": "counterparty",
    "Description": "description",
    # Some export variants also include:
    "Balance After Transaction": "balance_after",
    "Sub-Type": "sub_type",
    "Sub Type": "sub_type",
    "Currency": "currency",
    "Payment Type": "payment_type",
    "Category": "category",
}


class BunqParser(BaseParser):
    """Parser for Bunq CSV exports.

    Bunq CSV quirks:
    - Headers are always English — no locale variant.
    - Delimiter is comma; fields are quoted when they contain commas.
    - Amount is ALREADY signed: negative = debit, positive = credit.
    - Decimal separator is COMMA (e.g. -12,50) in Dutch locale exports
      but DOT in some API / newer exports. We detect both.
    - Date format is DD-MM-YYYY.
    - 'Interest Date' (valutadatum) can differ from transaction date — use 'Date'
      as the booking date for most purposes.
    - The 'Account' column holds YOUR IBAN.
    - 'Counterparty' is the OTHER PARTY's IBAN (may be empty for internal/card).
    - 'Name' is the other party's name.
    - Description is free text entered by the sender.
    - Encoding is UTF-8 (often with BOM from Windows exports).
    - Bunq sub-accounts (called 'Monetary Accounts') each export separately —
      you may receive multiple files for one user.
    """

    def source_type(self) -> str:
        return "bunq_csv"

    def source_label(self) -> str:
        return "Bunq CSV Export"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        text = text.lstrip("\ufeff")
        first_line = text.split("\n")[0].strip()

        # High confidence: the canonical Bunq header signature
        if "Interest Date" in first_line and "Amount" in first_line and "Counterparty" in first_line:
            return 0.97

        # Medium: two strong indicators
        if "Interest Date" in first_line and "Description" in first_line:
            return 0.88

        # Weaker: just the amount + name combo (too generic on its own)
        if (
            first_line.startswith('"Date"')
            and '"Amount"' in first_line
            and '"Name"' in first_line
            and '"Account"' in first_line
        ):
            return 0.75

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(
                    name="date",
                    dtype="date",
                    description="Transaction booking date",
                    format="YYYY-MM-DD",
                ),
                FieldAnnotation(
                    name="interest_date",
                    dtype="date",
                    description=(
                        "Value date (valutadatum) — the date used for interest calculations. "
                        "Can differ from the booking date."
                    ),
                    format="YYYY-MM-DD",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="amount",
                    dtype="decimal",
                    description=(
                        "Signed amount: negative = debit (money out), "
                        "positive = credit (money in)"
                    ),
                    unit="EUR",
                    format="dot_decimal",
                ),
                FieldAnnotation(
                    name="currency",
                    dtype="string",
                    description="Currency code (EUR for most Bunq accounts)",
                    examples=["EUR", "USD", "GBP"],
                    nullable=True,
                ),
                FieldAnnotation(
                    name="account_iban",
                    dtype="string",
                    description="YOUR own Bunq IBAN",
                ),
                FieldAnnotation(
                    name="counterparty_iban",
                    dtype="string",
                    description=(
                        "IBAN of the counterparty. Empty for card payments, "
                        "ATM withdrawals, and some internal transfers."
                    ),
                    nullable=True,
                ),
                FieldAnnotation(
                    name="counterparty",
                    dtype="string",
                    description="Name of the counterparty or merchant",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="description",
                    dtype="string",
                    description="Payment description / remittance information entered by the sender",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="balance_after",
                    dtype="decimal",
                    description="Account balance after the transaction",
                    unit="EUR",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="sub_type",
                    dtype="string",
                    description="Payment sub-type when present (e.g. SEPA_CREDIT_TRANSFER, MASTERCARD)",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="payment_type",
                    dtype="string",
                    description="High-level payment type when present",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="category",
                    dtype="string",
                    description="Auto-categorization label assigned by Bunq (when exported)",
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
                "Amount is ALREADY signed — negative = money out, positive = money in. No separate direction column.",
                "Date format in the raw file is DD-MM-YYYY. Normalized to YYYY-MM-DD.",
                "Decimal separator in the amount field is typically a COMMA in Dutch locale exports (e.g. -12,50). Normalized to dot decimal.",
                "'Interest Date' is the value date (valutadatum), used for interest. For transaction dating, use 'date' (booking date).",
                "'Account' column is YOUR IBAN; 'Counterparty' is the OTHER party's IBAN.",
                "Empty 'Counterparty' IBAN is common for Maestro/Mastercard card payments, ATM, and Bunq internal transfers.",
                "Each Bunq monetary account exports as a separate CSV — multiple files per user are expected.",
                "Currency is almost always EUR for NL accounts; Bunq supports multi-currency pockets which export with their own currency.",
                "Bunq auto-categories in the CSV may differ from what is shown in the app (app uses ML, export may lag).",
            ],
            notes=[
                "Bunq is a Dutch neobank — most users are NL-based and exports reflect Dutch banking conventions.",
                "The Interest Date (valutadatum) can differ from the booking date, affecting interest calculations.",
                "Sub-types include payment request flows (Tikkie-style) which appear as regular incoming transfers.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        text = text.lstrip("\ufeff")

        # Bunq uses comma delimiter with full quoting
        first_line = text.split("\n")[0]
        # Handle the rare tab-delimited variant
        delimiter = "\t" if first_line.count("\t") >= 3 else ","

        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        if not reader.fieldnames:
            return ParseResult(success=False, error="No CSV headers found")

        # Build column lookup
        col_lookup: dict[str, str] = {}
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
        return (row.get(header) or default).strip().strip('"')

    def _parse_row(self, row: dict, col_lookup: dict) -> dict:
        # Date: DD-MM-YYYY
        raw_date = self._get(row, col_lookup, "date")
        tx_date = self._parse_date(raw_date)

        raw_interest = self._get(row, col_lookup, "interest_date")
        interest_date = self._parse_date(raw_interest) if raw_interest else None

        # Amount: already signed, comma decimal
        raw_amount = self._get(row, col_lookup, "amount", "0")
        amount = self._parse_amount(raw_amount)
        is_debit = amount < 0

        # Balance (optional column)
        raw_balance = self._get(row, col_lookup, "balance_after")
        balance = self._parse_amount(raw_balance) if raw_balance else None

        return {
            "date": str(tx_date),
            "interest_date": str(interest_date) if interest_date else None,
            "amount": str(amount),
            "currency": self._get(row, col_lookup, "currency", "EUR") or "EUR",
            "direction": "debit" if is_debit else "credit",
            "account_iban": self._get(row, col_lookup, "account_iban"),
            "counterparty_iban": self._get(row, col_lookup, "counterparty_iban") or None,
            "counterparty": self._get(row, col_lookup, "counterparty") or None,
            "description": self._get(row, col_lookup, "description") or None,
            "balance_after": str(balance) if balance is not None else None,
            "sub_type": self._get(row, col_lookup, "sub_type") or None,
            "payment_type": self._get(row, col_lookup, "payment_type") or None,
            "category": self._get(row, col_lookup, "category") or None,
        }

    def _parse_date(self, raw: str) -> date | str:
        raw = raw.strip().strip('"')
        if not raw:
            return raw
        # DD-MM-YYYY
        if len(raw) == 10 and raw[2] == "-" and raw[5] == "-":
            try:
                return date(int(raw[6:10]), int(raw[3:5]), int(raw[:2]))
            except ValueError:
                pass
        # YYYY-MM-DD
        if len(raw) == 10 and raw[4] == "-":
            try:
                return date(int(raw[:4]), int(raw[5:7]), int(raw[8:10]))
            except ValueError:
                pass
        # YYYYMMDD
        if len(raw) == 8 and raw.isdigit():
            return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
        return raw

    def _parse_amount(self, raw: str) -> Decimal:
        raw = raw.strip().strip('"').replace(" ", "")
        if not raw:
            return Decimal("0")
        # Both separators present: last one is decimal
        if "," in raw and "." in raw:
            if raw.rfind(",") > raw.rfind("."):
                # Dutch: 1.234,56
                raw = raw.replace(".", "").replace(",", ".")
            else:
                # English: 1,234.56
                raw = raw.replace(",", "")
        elif "," in raw:
            raw = raw.replace(",", ".")
        try:
            return Decimal(raw)
        except (InvalidOperation, Exception):
            raise ValueError(f"Bunq: could not parse amount '{raw}'")

    def _decode(self, content: bytes) -> str | None:
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(BunqParser())
