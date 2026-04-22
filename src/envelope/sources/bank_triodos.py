"""Triodos Bank (Netherlands) CSV export parser."""

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

# Triodos Bank NL CSV format.
# Triodos exports from their online banking portal (mijn.triodos.nl) produce a
# semicolon-delimited CSV with Dutch headers. The format has been stable since
# at least 2018.
#
# Canonical header row (Dutch):
#   IBAN;Munt;BIC;Volgnr;Datum;Rentedatum;Bedrag;Saldo na trn;Tegenrekening IBAN;
#   Tegenrekening BIC;Code;Batch ID;Tegenrekening naam;Betalingskenmerk;Omschrijving;Factuurnummer
#
# Field notes:
# - 'IBAN': YOUR account IBAN
# - 'Munt': currency (always EUR for NL accounts)
# - 'BIC': BIC/SWIFT of YOUR bank (always TRIONL2U)
# - 'Volgnr': sequential transaction number within the export
# - 'Datum': booking date (YYYY-MM-DD)
# - 'Rentedatum': value date for interest purposes (YYYY-MM-DD)
# - 'Bedrag': SIGNED amount with comma decimal (Dutch locale)
# - 'Saldo na trn': balance AFTER transaction, comma decimal
# - 'Tegenrekening IBAN': counterparty IBAN (may be empty for cash/card)
# - 'Tegenrekening BIC': counterparty BIC (may be empty)
# - 'Code': transaction code (e.g. 'SEPA overboeking', 'Betaalopdracht', 'SEPA incasso')
# - 'Batch ID': SEPA batch/group reference (often empty for individual transactions)
# - 'Tegenrekening naam': counterparty name
# - 'Betalingskenmerk': payment reference (remittance info)
# - 'Omschrijving': description / notes
# - 'Factuurnummer': invoice number (used for direct debits, often empty)

COLUMN_MAP = {
    # Dutch headers
    "IBAN": "account_iban",
    "Munt": "currency",
    "BIC": "bic",
    "Volgnr": "sequence_number",
    "Datum": "date",
    "Rentedatum": "interest_date",
    "Bedrag": "amount",
    "Saldo na trn": "balance_after",
    "Tegenrekening IBAN": "counterparty_iban",
    "Tegenrekening BIC": "counterparty_bic",
    "Code": "transaction_code",
    "Batch ID": "batch_id",
    "Tegenrekening naam": "counterparty",
    "Betalingskenmerk": "payment_reference",
    "Omschrijving": "description",
    "Factuurnummer": "invoice_number",
}

# Triodos transaction codes seen in production
KNOWN_CODES = {
    "SEPA overboeking",   # SEPA credit transfer
    "Betaalopdracht",     # payment order (outgoing)
    "SEPA incasso",       # SEPA direct debit
    "SEPA incasso terug", # direct debit return
    "Saldobetaling",      # balance payment
    "Rente",              # interest
    "Kosten",             # bank fees
    "Spaarrekening",      # savings account transfer
    "PIN",                # debit card PIN payment
    "Geldautomaat",       # ATM withdrawal
}


class TriodosParser(BaseParser):
    """Parser for Triodos Bank NL CSV exports.

    Triodos CSV quirks:
    - Delimiter is SEMICOLON.
    - Amount ('Bedrag') is ALREADY signed: negative = debit, positive = credit.
    - Decimal separator is COMMA (Dutch locale). Values may include thousand-separator dots.
    - Date format is YYYY-MM-DD.
    - Encoding is UTF-8 (no BOM in most exports, but BOM safe).
    - The BIC column is always 'TRIONL2U' — useful as a strong detection signal.
    - 'Rentedatum' (interest date / value date) can differ from 'Datum' (booking date).
    - 'Code' is a human-readable Dutch string, not a machine code like CAMT.
    - 'Betalingskenmerk' is the payment reference entered by the sender (SEPA remittance info).
    - 'Omschrijving' is a free-text description, often combined with Betalingskenmerk.
    - SEPA direct debit returns show negative amounts on a previously credited amount.
    - Triodos is a Dutch ethical bank — many accounts belong to NGOs, foundations, or
      socially conscious individuals. Description fields often contain structured references.
    """

    def source_type(self) -> str:
        return "triodos_csv_nl"

    def source_label(self) -> str:
        return "Triodos Bank CSV Export (Netherlands)"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        text = text.lstrip("\ufeff")
        first_line = text.split("\n")[0].strip()

        # High confidence: Triodos BIC is unique
        if "TRIONL2U" in text[:500]:  # Check first ~500 chars for BIC in data
            if "Tegenrekening IBAN" in first_line or "Betalingskenmerk" in first_line:
                return 0.98

        # High confidence on headers alone — this column combination is distinctive
        if "Betalingskenmerk" in first_line and "Tegenrekening IBAN" in first_line:
            return 0.95

        if "Betalingskenmerk" in first_line and "Saldo na trn" in first_line:
            return 0.93

        # Medium: Dutch bank headers that could be Triodos (vs. Rabobank — check for
        # Rabobank-specific columns to avoid false positives)
        if (
            "Volgnr" in first_line
            and "Rentedatum" in first_line
            and "IBAN/BBAN" not in first_line  # Rabobank uses IBAN/BBAN
        ):
            return 0.85

        # Check filename pattern
        fn_lower = filename.lower()
        if "triodos" in fn_lower and first_line.count(";") >= 5:
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
                    description="Transaction booking date",
                    format="YYYY-MM-DD",
                ),
                FieldAnnotation(
                    name="interest_date",
                    dtype="date",
                    description=(
                        "Value date (rentedatum) used for interest calculations. "
                        "Can differ from booking date."
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
                    format="comma_decimal",
                ),
                FieldAnnotation(
                    name="currency",
                    dtype="string",
                    description="Currency code (always EUR for NL accounts)",
                    examples=["EUR"],
                ),
                FieldAnnotation(
                    name="balance_after",
                    dtype="decimal",
                    description="Account balance after this transaction",
                    unit="EUR",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="account_iban",
                    dtype="string",
                    description="YOUR Triodos Bank IBAN",
                ),
                FieldAnnotation(
                    name="counterparty_iban",
                    dtype="string",
                    description=(
                        "Counterparty IBAN. Empty for cash (PIN/ATM) transactions "
                        "and some internal transfers."
                    ),
                    nullable=True,
                ),
                FieldAnnotation(
                    name="counterparty",
                    dtype="string",
                    description="Name of the counterparty",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="transaction_code",
                    dtype="string",
                    description=(
                        "Dutch transaction type code, e.g. 'SEPA overboeking', "
                        "'Betaalopdracht', 'SEPA incasso', 'PIN', 'Geldautomaat'."
                    ),
                    nullable=True,
                ),
                FieldAnnotation(
                    name="payment_reference",
                    dtype="string",
                    description=(
                        "Betalingskenmerk — the payment reference / remittance information "
                        "entered by the sender on the SEPA transfer."
                    ),
                    nullable=True,
                ),
                FieldAnnotation(
                    name="description",
                    dtype="string",
                    description="Free-text description / omschrijving",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="invoice_number",
                    dtype="string",
                    description=(
                        "Factuurnummer — invoice number, populated for direct debits "
                        "from some billers. Usually empty."
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
                "Decimal separator is COMMA (Dutch locale). Thousand separator may be a DOT. Normalized to dot decimal.",
                "Delimiter is SEMICOLON.",
                "Date format is YYYY-MM-DD for both 'Datum' and 'Rentedatum'.",
                "'Rentedatum' (value date) can differ from 'Datum' (booking date) by 1-3 days for SEPA transfers.",
                "BIC column always contains 'TRIONL2U' for Triodos Bank NL — used as a strong detection signal.",
                "'Code' is a human-readable Dutch string (not SWIFT/ISO code): 'SEPA overboeking', 'SEPA incasso', 'PIN', etc.",
                "'Betalingskenmerk' is the payment reference on SEPA transfers. This is what the SENDER put in the reference field.",
                "SEPA direct debit returns appear with a positive amount and Code 'SEPA incasso terug'.",
                "Triodos accounts are often used by charities and foundations — payment references and descriptions may contain structured contribution or invoice numbers.",
                "The Triodos export does not include card-level merchant detail (MCC, city) — only merchant name via counterparty.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        text = text.lstrip("\ufeff")

        # Triodos uses semicolon, but verify
        first_line = text.split("\n")[0]
        if first_line.count(";") >= 5:
            delimiter = ";"
        elif first_line.count(",") >= 5:
            delimiter = ","
        else:
            delimiter = ";"  # Default for Triodos

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
        # Date: YYYY-MM-DD
        raw_date = self._get(row, col_lookup, "date")
        tx_date = self._parse_date(raw_date)

        raw_interest = self._get(row, col_lookup, "interest_date")
        interest_date = self._parse_date(raw_interest) if raw_interest else None

        # Amount: comma decimal, already signed, may have thousand-separator dots
        raw_amount = self._get(row, col_lookup, "amount", "0")
        amount = self._parse_dutch_amount(raw_amount)
        is_debit = amount < 0

        # Balance
        raw_balance = self._get(row, col_lookup, "balance_after")
        balance = self._parse_dutch_amount(raw_balance) if raw_balance else None

        return {
            "date": str(tx_date),
            "interest_date": str(interest_date) if interest_date else None,
            "amount": str(amount),
            "currency": self._get(row, col_lookup, "currency", "EUR"),
            "direction": "debit" if is_debit else "credit",
            "balance_after": str(balance) if balance is not None else None,
            "account_iban": self._get(row, col_lookup, "account_iban"),
            "counterparty_iban": self._get(row, col_lookup, "counterparty_iban") or None,
            "counterparty": self._get(row, col_lookup, "counterparty") or None,
            "counterparty_bic": self._get(row, col_lookup, "counterparty_bic") or None,
            "transaction_code": self._get(row, col_lookup, "transaction_code") or None,
            "batch_id": self._get(row, col_lookup, "batch_id") or None,
            "payment_reference": self._get(row, col_lookup, "payment_reference") or None,
            "description": self._get(row, col_lookup, "description") or None,
            "invoice_number": self._get(row, col_lookup, "invoice_number") or None,
            "sequence_number": self._get(row, col_lookup, "sequence_number") or None,
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
        # DD-MM-YYYY
        if len(raw) == 10 and raw[2] == "-":
            try:
                return date(int(raw[6:10]), int(raw[3:5]), int(raw[:2]))
            except ValueError:
                pass
        # YYYYMMDD
        if len(raw) == 8 and raw.isdigit():
            return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
        return raw

    def _parse_dutch_amount(self, raw: str) -> Decimal:
        """Parse Dutch-locale amount: comma decimal, dot thousand separator.

        Examples: -1.234,56 → -1234.56 | +40,70 → 40.70 | 100 → 100
        """
        raw = raw.strip().strip('"').replace(" ", "")
        if not raw:
            return Decimal("0")

        # Extract sign
        sign = ""
        if raw.startswith(("+", "-")):
            sign = raw[0]
            raw = raw[1:]

        # Dutch format: dots as thousands, comma as decimal
        if "," in raw and "." in raw:
            # Last separator is the decimal one
            if raw.rfind(",") > raw.rfind("."):
                # Comma is decimal (Dutch: 1.234,56)
                raw = raw.replace(".", "").replace(",", ".")
            else:
                # Dot is decimal (English: 1,234.56) — rare for Triodos but handle it
                raw = raw.replace(",", "")
        elif "," in raw:
            # Only comma — it's the decimal separator
            raw = raw.replace(",", ".")
        # If only dot: already dot decimal, nothing to do

        raw = sign + raw
        try:
            return Decimal(raw)
        except (InvalidOperation, Exception):
            raise ValueError(f"Triodos: could not parse amount '{sign + raw}'")

    def _decode(self, content: bytes) -> str | None:
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(TriodosParser())
