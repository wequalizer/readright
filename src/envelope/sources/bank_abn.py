"""ABN AMRO (Netherlands) CSV export parser."""

from __future__ import annotations

import csv
import io
from datetime import date
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# ABN AMRO exports use quoted column names. The format is fixed — no Dutch/English
# variant. Both comma-separated and tab-separated versions exist depending on the
# export channel (online banking vs. mobile app vs. older portal).
KNOWN_HEADERS = {
    "transactiondate",
    "valuecurrency",
    "transactionamount",
    "accountname",
    "counterpartyaccountnumber",
    "counterpartyname",
    "description1",
    "description2",
    "description3",
}

# ABN exports use a quoted header row — the real column names (case-insensitive):
#   "transactiondate","valueCurrency","transactionAmount","accountName",
#   "counterPartyAccountNumber","counterPartyName","description1","description2","description3"
#
# The column names in the file use camelCase, but we match case-insensitively
# to handle variation between export versions.


class ABNAMROParser(BaseParser):
    """Parser for ABN AMRO Bank NL CSV exports.

    ABN AMRO CSV quirks:
    - All column headers are quoted; names use camelCase.
    - Delimiter is comma in most exports, but TAB in some older / MT940-adjacent
      exports. We detect both.
    - transactionAmount is ALREADY signed: negative = debit (money out),
      positive = credit (money in).
    - Decimal separator is a DOT (unlike most other Dutch banks).
    - Date format is YYYYMMDD (8 digits, no separators).
    - Encoding is UTF-8 or latin-1; BOM may be present.
    - counterPartyAccountNumber may be an IBAN, BBAN, or empty.
    - Three free-text description columns (description1, description2, description3)
      are concatenated. They often contain structured SEPA remittance fields
      embedded as '/EV/.../' or 'OMSCHRIJVING: ...' style substrings — no
      consistent machine-readable delimiter.
    - The 'accountName' column is YOUR account name, not IBAN. ABN does not
      export your own IBAN directly in the CSV.
    """

    def source_type(self) -> str:
        return "abn_amro_csv_nl"

    def source_label(self) -> str:
        return "ABN AMRO CSV Export (Netherlands)"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        # Strip BOM and normalize
        text = text.lstrip("\ufeff")
        first_line = text.split("\n")[0].strip().lower()

        # Remove surrounding quotes from the header check
        first_line_clean = first_line.replace('"', "")

        # High-confidence: all three description columns + transactiondate present
        if (
            "transactiondate" in first_line_clean
            and "description1" in first_line_clean
            and "counterpartyaccountnumber" in first_line_clean
        ):
            return 0.97

        # Medium confidence: two characteristic columns
        if (
            "transactiondate" in first_line_clean
            and "counterpartyname" in first_line_clean
        ):
            return 0.88

        if (
            "transactionamount" in first_line_clean
            and "accountname" in first_line_clean
        ):
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
                        "Signed transaction amount: negative = debit (money out), "
                        "positive = credit (money in)"
                    ),
                    unit="EUR",
                    format="dot_decimal",
                ),
                FieldAnnotation(
                    name="currency",
                    dtype="string",
                    description="Currency of the transaction (valueCurrency column)",
                    examples=["EUR"],
                ),
                FieldAnnotation(
                    name="direction",
                    dtype="enum",
                    description="Derived from sign of amount",
                    enum_values=["debit", "credit"],
                ),
                FieldAnnotation(
                    name="account_name",
                    dtype="string",
                    description=(
                        "Name of your own ABN AMRO account. Note: this is the account "
                        "NAME, not the IBAN — ABN does not include your own IBAN in the CSV."
                    ),
                ),
                FieldAnnotation(
                    name="counterparty_account",
                    dtype="string",
                    description=(
                        "Account number of the counterparty — may be IBAN, BBAN, or empty "
                        "for cash/card transactions"
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
                    name="description",
                    dtype="string",
                    description=(
                        "Concatenated description1 + description2 + description3 fields. "
                        "May contain SEPA remittance info, terminal IDs, or merchant names."
                    ),
                ),
            ],
            conventions=[
                "Amount is ALREADY signed — negative means money left your account, positive means money entered. No separate direction column in the source.",
                "Decimal separator is DOT (not comma), unlike ING and Rabobank.",
                "Date format in the raw file is YYYYMMDD (e.g. 20240315). Normalized to YYYY-MM-DD.",
                "Three separate description columns (description1/2/3) are concatenated with a space. Individual columns are preserved if needed.",
                "counterPartyAccountNumber can be IBAN, old-style BBAN, or empty (e.g. ATM withdrawals, card payments at POS).",
                "accountName is your account's human name (e.g. 'J. Doe Privé'), NOT your IBAN.",
                "Delimiter is comma in standard exports; some older export tools produce TAB-separated files. Both are handled.",
                "Encoding is typically UTF-8; BOM may be present. Falls back to latin-1/cp1252 for older files.",
                "SEPA structured remittance data is sometimes embedded in description fields using /REMI/ or /EV/ tags — no guarantee of consistent structure.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        # Strip BOM
        text = text.lstrip("\ufeff")

        # Detect delimiter: ABN uses comma or tab
        first_line = text.split("\n")[0]
        if first_line.count("\t") >= 3:
            delimiter = "\t"
        elif first_line.count(";") > first_line.count(","):
            delimiter = ";"
        else:
            delimiter = ","

        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        if not reader.fieldnames:
            return ParseResult(success=False, error="No CSV headers found")

        # Build case-insensitive column lookup
        # Maps lowercased stripped key → actual header string in the file
        col_lookup: dict[str, str] = {}
        for header in reader.fieldnames:
            clean = header.strip().strip('"').lower()
            col_lookup[clean] = header

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

    def _col(self, row: dict, col_lookup: dict[str, str], key: str, default: str = "") -> str:
        """Fetch a value using case-insensitive column lookup."""
        header = col_lookup.get(key.lower())
        if header is None:
            return default
        return (row.get(header) or default).strip().strip('"')

    def _parse_row(self, row: dict, col_lookup: dict[str, str]) -> dict:
        # Date: YYYYMMDD
        raw_date = self._col(row, col_lookup, "transactiondate")
        tx_date = self._parse_date(raw_date)

        # Amount: dot decimal, already signed
        raw_amount = self._col(row, col_lookup, "transactionamount", "0")
        # Some exports use comma decimal despite ABN's documented format — handle both
        amount = self._parse_amount(raw_amount)

        is_debit = amount < 0

        # Three description fields — concatenate non-empty ones
        descriptions = []
        for key in ("description1", "description2", "description3"):
            val = self._col(row, col_lookup, key)
            if val:
                descriptions.append(val)
        description = " ".join(descriptions)

        return {
            "date": str(tx_date),
            "amount": str(amount),
            "currency": self._col(row, col_lookup, "valuecurrency", "EUR"),
            "direction": "debit" if is_debit else "credit",
            "account_name": self._col(row, col_lookup, "accountname"),
            "counterparty_account": self._col(row, col_lookup, "counterpartyaccountnumber") or None,
            "counterparty": self._col(row, col_lookup, "counterpartyname") or None,
            "description": description,
            # Preserve individual description columns for structured parsing
            "description1": self._col(row, col_lookup, "description1"),
            "description2": self._col(row, col_lookup, "description2"),
            "description3": self._col(row, col_lookup, "description3"),
        }

    def _parse_date(self, raw: str) -> date | str:
        raw = raw.strip().strip('"')
        if not raw:
            return raw
        # YYYYMMDD
        if len(raw) == 8 and raw.isdigit():
            return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
        # YYYY-MM-DD (some export variants)
        if len(raw) == 10 and raw[4] == "-":
            return date(int(raw[:4]), int(raw[5:7]), int(raw[8:10]))
        # DD-MM-YYYY
        if len(raw) == 10 and raw[2] == "-":
            return date(int(raw[6:10]), int(raw[3:5]), int(raw[:2]))
        return raw

    def _parse_amount(self, raw: str) -> Decimal:
        """Parse amount handling both dot and comma as decimal separator."""
        raw = raw.strip().strip('"').replace(" ", "")
        if not raw:
            return Decimal("0")
        # If both separators present: last one is decimal
        if "," in raw and "." in raw:
            if raw.rfind(",") > raw.rfind("."):
                # Dutch style: 1.234,56
                raw = raw.replace(".", "").replace(",", ".")
            else:
                # English style: 1,234.56
                raw = raw.replace(",", "")
        elif "," in raw:
            # Comma-only: decimal separator
            raw = raw.replace(",", ".")
        try:
            return Decimal(raw)
        except InvalidOperation:
            return Decimal("0")

    def _decode(self, content: bytes) -> str | None:
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(ABNAMROParser())
