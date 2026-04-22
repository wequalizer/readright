"""Wise (TransferWise) CSV export parser."""

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

# Wise CSV headers — always English, fixed since the TransferWise→Wise rebrand.
# The standard export from wise.com produces:
#   TransferWise ID,Date,Amount,Currency,Description,Payment Reference,
#   Running Balance,Exchange From,Exchange To,Buy Amount,Exchange Rate,Merchant
#
# Notes on variants:
# - Older exports (pre-2021) used "TransferWise ID" — same column, same name.
# - Some exports include a "Category" column at the end.
# - Wise exports are PER CURRENCY BALANCE (one file per currency jar).
# - Currency conversions appear as PAIRS of rows: one debit in source currency,
#   one credit in target currency — each in its respective currency file.

COLUMN_MAP = {
    "TransferWise ID": "transfer_id",
    "Date": "date",
    "Amount": "amount",
    "Currency": "currency",
    "Description": "description",
    "Payment Reference": "payment_reference",
    "Running Balance": "running_balance",
    "Exchange From": "exchange_from",
    "Exchange To": "exchange_to",
    "Buy Amount": "buy_amount",
    "Exchange Rate": "exchange_rate",
    "Merchant": "merchant",
    # Optional columns present in some exports
    "Category": "category",
    "Note": "note",
    "Status": "state",
    "Exchange Rate Applied": "exchange_rate",
    "Payee Account Number": "counterparty_account",
    "Payee Name": "counterparty",
}


class WiseParser(BaseParser):
    """Parser for Wise (TransferWise) CSV exports.

    Wise CSV quirks:
    - Delimiter is comma. Fields with commas are quoted.
    - Amount is ALREADY signed: negative = debit (money out), positive = credit.
    - Decimal separator is DOT.
    - Date format is DD-MM-YYYY (e.g. 15-03-2024).
    - Encoding is UTF-8, typically with BOM.
    - One CSV per currency balance — a Wise account with EUR + USD + GBP
      balances exports three separate files.
    - Currency conversions appear as TWO rows: a debit in the source currency
      (in that currency's file) and a credit in the target currency (in the
      other currency's file). The 'Exchange From', 'Exchange To', 'Buy Amount',
      and 'Exchange Rate' columns only have values on the debit row.
    - 'Running Balance' is the per-currency jar balance after the transaction.
    - 'Payment Reference' is the reference the SENDER put on the transfer.
    - 'Merchant' is populated for card (Wise debit card) transactions.
    - 'Description' contains human-readable summary: e.g. "Card transaction",
      "Sent money to John Doe", "Converted EUR to USD".
    - 'TransferWise ID' uniquely identifies each row within the export.
    """

    def source_type(self) -> str:
        return "wise_csv"

    def source_label(self) -> str:
        return "Wise (TransferWise) CSV Export"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        text = text.lstrip("\ufeff")
        first_line = text.split("\n")[0].strip()

        # High confidence: unique Wise column
        if "TransferWise ID" in first_line:
            # Even stronger if other Wise-specific columns are present
            if "Running Balance" in first_line or "Payment Reference" in first_line:
                return 0.98
            return 0.92

        # Medium: Exchange columns are distinctive
        if "Exchange From" in first_line and "Exchange To" in first_line:
            return 0.88

        # Check for Wise-branded filename patterns
        fn_lower = filename.lower()
        if ("wise" in fn_lower or "transferwise" in fn_lower) and "statement" in fn_lower:
            if "Amount" in first_line and "Currency" in first_line:
                return 0.80

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(
                    name="transfer_id",
                    dtype="string",
                    description="Unique Wise transaction identifier (TransferWise ID column)",
                ),
                FieldAnnotation(
                    name="date",
                    dtype="date",
                    description="Transaction date",
                    format="YYYY-MM-DD",
                ),
                FieldAnnotation(
                    name="amount",
                    dtype="decimal",
                    description=(
                        "Signed amount in the account's currency: "
                        "negative = debit (money out), positive = credit (money in)"
                    ),
                    format="dot_decimal",
                ),
                FieldAnnotation(
                    name="currency",
                    dtype="string",
                    description=(
                        "Currency of this transaction row and running balance. "
                        "Each Wise currency jar exports as a separate file."
                    ),
                    examples=["EUR", "USD", "GBP", "PLN"],
                ),
                FieldAnnotation(
                    name="description",
                    dtype="string",
                    description=(
                        "Human-readable transaction description, e.g. 'Sent money to John Doe', "
                        "'Card transaction', 'Converted EUR to USD'."
                    ),
                ),
                FieldAnnotation(
                    name="payment_reference",
                    dtype="string",
                    description=(
                        "The payment reference the SENDER attached to the transfer. "
                        "Empty for card transactions and outgoing transfers."
                    ),
                    nullable=True,
                ),
                FieldAnnotation(
                    name="running_balance",
                    dtype="decimal",
                    description=(
                        "Per-currency jar balance AFTER this transaction. "
                        "Not total Wise account balance across all currencies."
                    ),
                    nullable=True,
                ),
                FieldAnnotation(
                    name="exchange_from",
                    dtype="string",
                    description=(
                        "Source currency for a conversion (e.g. 'EUR'). "
                        "Only populated on the debit row of a currency conversion."
                    ),
                    nullable=True,
                ),
                FieldAnnotation(
                    name="exchange_to",
                    dtype="string",
                    description=(
                        "Target currency for a conversion (e.g. 'USD'). "
                        "Only populated on the debit row of a currency conversion."
                    ),
                    nullable=True,
                ),
                FieldAnnotation(
                    name="buy_amount",
                    dtype="decimal",
                    description=(
                        "Amount received in the target currency for a conversion. "
                        "Only populated on the debit row of a currency conversion."
                    ),
                    nullable=True,
                ),
                FieldAnnotation(
                    name="exchange_rate",
                    dtype="decimal",
                    description=(
                        "Exchange rate applied for currency conversions. "
                        "Empty for non-conversion transactions."
                    ),
                    nullable=True,
                ),
                FieldAnnotation(
                    name="merchant",
                    dtype="string",
                    description=(
                        "Merchant name for Wise debit card transactions. "
                        "Empty for bank transfers and currency conversions."
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
                "Date format in the raw file is DD-MM-YYYY. Normalized to YYYY-MM-DD.",
                "One CSV file per currency balance. A Wise account with multiple currencies produces multiple export files.",
                "Currency conversions appear as TWO rows across TWO currency files: a debit in the source currency file and a credit in the target currency file. Match them via TransferWise ID (they share the same ID prefix).",
                "Running Balance is the per-currency balance, not total account value.",
                "Payment Reference is the reference the SENDER entered — it may be empty even for incoming transfers.",
                "Merchant is only populated for Wise debit card (Mastercard) transactions, not for SEPA/SWIFT transfers.",
                "Currency conversion rows have Exchange From/To/Rate/Buy Amount populated; regular transfers do not.",
                "Wise charges are shown as separate fee rows with type 'Wise fee' in the Description, not embedded in the Amount.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        text = text.lstrip("\ufeff")

        reader = csv.DictReader(io.StringIO(text), delimiter=",")
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
        # Date: DD-MM-YYYY in raw file
        raw_date = self._get(row, col_lookup, "date")
        tx_date = self._parse_date(raw_date)

        # Amount: dot decimal, already signed
        raw_amount = self._get(row, col_lookup, "amount", "0")
        amount = self._parse_amount(raw_amount)
        is_debit = amount < 0

        # Running balance
        raw_balance = self._get(row, col_lookup, "running_balance")
        balance = self._parse_amount(raw_balance) if raw_balance else None

        # Buy amount (for conversions)
        raw_buy = self._get(row, col_lookup, "buy_amount")
        buy_amount = self._parse_amount(raw_buy) if raw_buy else None

        # Exchange rate
        raw_rate = self._get(row, col_lookup, "exchange_rate")
        exchange_rate = self._parse_amount(raw_rate) if raw_rate else None

        return {
            "transfer_id": self._get(row, col_lookup, "transfer_id"),
            "date": str(tx_date),
            "amount": str(amount),
            "currency": self._get(row, col_lookup, "currency", "EUR"),
            "description": self._get(row, col_lookup, "description"),
            "payment_reference": self._get(row, col_lookup, "payment_reference") or None,
            "running_balance": str(balance) if balance is not None else None,
            "exchange_from": self._get(row, col_lookup, "exchange_from") or None,
            "exchange_to": self._get(row, col_lookup, "exchange_to") or None,
            "buy_amount": str(buy_amount) if buy_amount is not None else None,
            "exchange_rate": str(exchange_rate) if exchange_rate is not None else None,
            "merchant": self._get(row, col_lookup, "merchant") or None,
            "direction": "debit" if is_debit else "credit",
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
        # DD-MM-YYYY HH:MM:SS (some exports include time)
        if len(raw) > 10 and raw[2] == "-" and raw[5] == "-":
            try:
                return date(int(raw[6:10]), int(raw[3:5]), int(raw[:2]))
            except ValueError:
                pass
        # YYYY-MM-DD
        if len(raw) >= 10 and raw[4] == "-":
            try:
                return date(int(raw[:4]), int(raw[5:7]), int(raw[8:10]))
            except ValueError:
                pass
        return raw

    def _parse_amount(self, raw: str) -> Decimal:
        raw = raw.strip().strip('"').replace(" ", "")
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
            raise ValueError(f"Wise: could not parse amount '{raw}'")

    def _decode(self, content: bytes) -> str | None:
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(WiseParser())
