"""N26 CSV export parser."""

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

# N26 CSV headers — always English, fixed format.
# Export from the N26 web app / CSV download produces:
#   "Date","Payee","Account number","Transaction type","Payment reference",
#   "Amount (EUR)","Amount (Foreign Currency)","Type Foreign Currency","Exchange Rate"
#
# N26 changed its CSV format in 2020. The current format (2020+) always includes
# the foreign currency columns even for EUR transactions (empty if not applicable).
# Older exports (pre-2020) may lack the last three columns.
#
# N26 also supports a "Spaces" feature — sub-accounts shown as internal transfers.

COLUMN_MAP = {
    "Date": "date",
    "Payee": "counterparty",
    "Account number": "counterparty_account",
    "Transaction type": "transaction_type",
    "Payment reference": "payment_reference",
    "Amount (EUR)": "amount",
    "Amount (Foreign Currency)": "amount_foreign",
    "Type Foreign Currency": "foreign_currency",
    "Exchange Rate": "exchange_rate",
    # Some exports include these additional columns:
    "Category": "category",
    "Note": "note",
    "Merchant Name": "merchant",
    "Merchant City": "merchant_city",
    "Merchant Country": "merchant_country",
    "MCC": "mcc",
}

# N26 transaction type strings (seen in the wild)
# These are the English versions; N26 always exports in English
KNOWN_TRANSACTION_TYPES = {
    "Income",
    "Outgoing Transfer",
    "Incoming Transfer",
    "MasterCard Payment",
    "Direct Debit",
    "Direct Debit Return",
    "Bank Transfer",
    "Standing Order",
    "Spaces Savings",
    "Spaces Withdrawal",
    "ATM",
    "Presentment",
    "AA",  # internal code sometimes present
}


class N26Parser(BaseParser):
    """Parser for N26 CSV exports.

    N26 CSV quirks:
    - Delimiter is comma. String fields are quoted.
    - Amount is ALREADY signed: negative = debit (money out), positive = credit.
    - Decimal separator is DOT (N26 is a German bank but uses English locale in exports).
    - Date format is YYYY-MM-DD.
    - Encoding is UTF-8, typically with BOM.
    - 'Amount (EUR)' is ALWAYS in EUR even if the transaction was in a foreign currency.
    - 'Amount (Foreign Currency)' + 'Type Foreign Currency' + 'Exchange Rate' are
      populated for card transactions in non-EUR currencies. These are empty for
      EUR transactions.
    - 'Payee' is the counterparty name (the other person/merchant).
    - 'Account number' is the counterparty's IBAN or empty (e.g. for card payments).
    - 'Transaction type' tells you HOW the money moved (card, SEPA, Direct Debit, etc.).
    - 'Payment reference' is the remittance text on SEPA transfers. Empty for card payments.
    - N26 Spaces (sub-accounts) appear as internal transfers with Payee = your own name
      and transaction type 'Spaces Savings' / 'Spaces Withdrawal'.
    - Direct Debit returns have a positive amount and type 'Direct Debit Return'.
    - MCC (Merchant Category Code) is included in some newer exports — useful for
      classifying card purchases.
    """

    def source_type(self) -> str:
        return "n26_csv"

    def source_label(self) -> str:
        return "N26 CSV Export"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        text = text.lstrip("\ufeff")
        first_line = text.split("\n")[0].strip()

        # High confidence: the definitive N26 header signature
        if (
            '"Amount (EUR)"' in first_line
            and '"Transaction type"' in first_line
        ):
            return 0.97

        # Also works without quotes
        if (
            "Amount (EUR)" in first_line
            and "Transaction type" in first_line
        ):
            return 0.97

        # Medium: the foreign currency columns are highly distinctive
        if "Amount (Foreign Currency)" in first_line and "Type Foreign Currency" in first_line:
            return 0.92

        # Weaker: Payee + Payment reference + Date in a CSV
        if (
            "Payee" in first_line
            and "Payment reference" in first_line
            and first_line.startswith('"Date"')
        ):
            return 0.75

        # Check filename pattern: N26 exports are often named "n26-csv-transactions-*.csv"
        fn_lower = filename.lower()
        if "n26" in fn_lower and "transaction" in fn_lower:
            if "Amount" in first_line and "Date" in first_line:
                return 0.80

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
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
                        "Signed amount in EUR: negative = debit (money out), "
                        "positive = credit (money in). ALWAYS in EUR even for "
                        "foreign currency card transactions."
                    ),
                    unit="EUR",
                    format="dot_decimal",
                ),
                FieldAnnotation(
                    name="direction",
                    dtype="enum",
                    description="Derived from sign of amount",
                    enum_values=["debit", "credit"],
                ),
                FieldAnnotation(
                    name="counterparty",
                    dtype="string",
                    description=(
                        "Payee name — the other party's name. For card payments, "
                        "this is the merchant name. For SEPA transfers, it's the "
                        "sender's or recipient's name."
                    ),
                    nullable=True,
                ),
                FieldAnnotation(
                    name="counterparty_account",
                    dtype="string",
                    description=(
                        "Counterparty's IBAN or account number. "
                        "Empty for card payments and ATM withdrawals."
                    ),
                    nullable=True,
                ),
                FieldAnnotation(
                    name="transaction_type",
                    dtype="string",
                    description="Type of transaction",
                    examples=[
                        "Income",
                        "Outgoing Transfer",
                        "MasterCard Payment",
                        "Direct Debit",
                        "Spaces Savings",
                        "ATM",
                    ],
                ),
                FieldAnnotation(
                    name="payment_reference",
                    dtype="string",
                    description=(
                        "Remittance information / payment reference on the SEPA transfer. "
                        "Empty for card payments."
                    ),
                    nullable=True,
                ),
                FieldAnnotation(
                    name="amount_foreign",
                    dtype="decimal",
                    description=(
                        "Original amount in foreign currency for non-EUR card transactions. "
                        "Null for EUR transactions."
                    ),
                    nullable=True,
                ),
                FieldAnnotation(
                    name="foreign_currency",
                    dtype="string",
                    description=(
                        "Currency code of the foreign currency amount. "
                        "Null for EUR transactions."
                    ),
                    nullable=True,
                    examples=["USD", "GBP", "CHF", "SEK"],
                ),
                FieldAnnotation(
                    name="exchange_rate",
                    dtype="decimal",
                    description=(
                        "Exchange rate applied for foreign currency transactions "
                        "(EUR per unit of foreign currency). Null for EUR transactions."
                    ),
                    nullable=True,
                ),
            ],
            conventions=[
                "Amount is ALREADY signed: negative = money out, positive = money in.",
                "Decimal separator is DOT (English locale) despite N26 being a German bank.",
                "Date format is YYYY-MM-DD.",
                "Amount (EUR) is ALWAYS in EUR — even for foreign currency card transactions. The original foreign amount is in the separate 'Amount (Foreign Currency)' column.",
                "Foreign currency columns (Amount, Type, Rate) are EMPTY for EUR transactions — check for null before using.",
                "N26 Spaces (sub-accounts) appear as internal transfers: Payee = your name, type = 'Spaces Savings' or 'Spaces Withdrawal'. Do not count these as real income/expense.",
                "Direct Debit returns have a positive amount and type 'Direct Debit Return' — they are refunds, not new income.",
                "Card transaction amounts in EUR are already converted at the time of settlement — the exchange rate reflects the Mastercard rate on settlement date, not initiation date.",
                "Payment reference is only populated for bank transfers (SEPA credit transfers). Card payment descriptions are in the Payee field.",
                "MCC (Merchant Category Code) is present in newer exports and can be used to classify card spending by category.",
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
        # Date: YYYY-MM-DD
        raw_date = self._get(row, col_lookup, "date")
        tx_date = self._parse_date(raw_date)

        # Main amount: EUR, dot decimal, already signed
        raw_amount = self._get(row, col_lookup, "amount", "0")
        try:
            amount = self._parse_amount(raw_amount)
        except ValueError as e:
            raise ValueError(f"Amount parse error: {e}")
        is_debit = amount < 0

        # Foreign currency columns
        raw_foreign = self._get(row, col_lookup, "amount_foreign")
        amount_foreign = self._parse_amount(raw_foreign) if raw_foreign else None

        raw_rate = self._get(row, col_lookup, "exchange_rate")
        exchange_rate = self._parse_amount(raw_rate) if raw_rate else None

        foreign_currency = self._get(row, col_lookup, "foreign_currency") or None

        return {
            "date": str(tx_date),
            "amount": str(amount),
            "currency": "EUR",
            "direction": "debit" if is_debit else "credit",
            "counterparty": self._get(row, col_lookup, "counterparty") or None,
            "counterparty_account": self._get(row, col_lookup, "counterparty_account") or None,
            "transaction_type": self._get(row, col_lookup, "transaction_type"),
            "payment_reference": self._get(row, col_lookup, "payment_reference") or None,
            "amount_foreign": str(amount_foreign) if amount_foreign is not None else None,
            "foreign_currency": foreign_currency,
            "exchange_rate": str(exchange_rate) if exchange_rate is not None else None,
        }

    def _parse_date(self, raw: str) -> date | str:
        raw = raw.strip().strip('"')
        if not raw:
            return raw
        # YYYY-MM-DD
        if len(raw) == 10 and raw[4] == "-":
            try:
                return date(int(raw[:4]), int(raw[5:7]), int(raw[8:10]))
            except ValueError:
                pass
        # DD-MM-YYYY (fallback for locale-specific exports)
        if len(raw) == 10 and raw[2] == "-":
            try:
                return date(int(raw[6:10]), int(raw[3:5]), int(raw[:2]))
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
            raise ValueError(f"N26: could not parse amount '{raw}'")

    def _decode(self, content: bytes) -> str | None:
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(N26Parser())
