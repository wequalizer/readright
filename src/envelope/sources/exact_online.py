"""Exact Online CSV export parser — Mutaties (journal entries) and Relaties (contacts).

Exact Online is the dominant accounting platform in the Netherlands (~300K businesses).
Their CSV exports use Dutch column names, comma decimals, DD-MM-YYYY dates, and
Exact-specific codes for VAT rates, journal types, etc.

Nobody else parses these exports for AI consumption. This is a strategic parser.
"""

from __future__ import annotations

import csv
import io
import logging
import re
from datetime import date
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# --- Mutaties (journal entries) ---

# Column mapping: Dutch header → normalized English key
MUTATIES_COLUMN_MAP = {
    "Dagboek": "journal_code",
    "Boekstuk": "entry_number",
    "Datum": "date",
    "Grootboekrekening": "ledger_account",
    "Grootboek omschrijving": "ledger_description",
    "Omschrijving": "description",
    "Debet": "debit",
    "Credit": "credit",
    "BTW-code": "vat_code",
    "BTW code": "vat_code",
    "BTW-bedrag": "vat_amount",
    "BTW bedrag": "vat_amount",
    "Kostenplaats": "cost_center",
    "Kostendrager": "cost_unit",
    "Relatiecode": "relation_code",
    "Relatienaam": "relation_name",
    "Factuurnummer": "invoice_number",
    "Boekjaar": "fiscal_year",
    "Periode": "period",
    "Valuta": "currency",
    "Bedrag": "amount",
}

# Known Dagboek (journal) codes
DAGBOEK_CODES = {
    "10": "Inkoop (Purchase)",
    "20": "Verkoop (Sales)",
    "30": "Kas (Cash)",
    "70": "Memoriaal (Memorial/General)",
    "90": "Bank",
}

# Known BTW (VAT) codes
BTW_CODES = {
    "0": "0% (exempt/zero-rated)",
    "1": "21% BTW (standard rate)",
    "2": "9% BTW (reduced rate)",
    "3": "0% export/EU reverse charge",
    "5": "Verlegd (reverse charge domestic)",
    "6": "0% non-EU services",
}

# --- Relaties (contacts) ---

RELATIES_COLUMN_MAP = {
    "Relatienummer": "relation_number",
    "Relatietype": "relation_type",
    "Naam": "name",
    "Adres": "address",
    "Postcode": "postal_code",
    "Plaats": "city",
    "Land": "country",
    "Telefoon": "phone",
    "E-mail": "email",
    "Website": "website",
    "KVK-nummer": "chamber_of_commerce",
    "KvK-nummer": "chamber_of_commerce",
    "BTW-nummer": "vat_number",
    "BTW nummer": "vat_number",
    "IBAN": "iban",
    "BIC": "bic",
    "Betalingsconditie": "payment_terms",
    "Kredietlimiet": "credit_limit",
    "Blokkeren": "blocked",
    "Opmerkingen": "notes",
}


def _parse_dutch_decimal(value: str) -> Decimal | None:
    """Parse Dutch-format number: 1.250,00 → 1250.00"""
    if not value or not value.strip():
        return None
    cleaned = value.strip()
    # Remove thousands separator (period), swap comma for decimal point
    cleaned = cleaned.replace(".", "").replace(",", ".")
    try:
        return Decimal(cleaned)
    except (InvalidOperation, Exception):
        logger.warning("Exact Online: could not parse decimal '%s', defaulting to None", value.strip())
        return None


def _parse_dutch_date(value: str) -> str | None:
    """Parse DD-MM-YYYY or DD/MM/YYYY → YYYY-MM-DD."""
    if not value or not value.strip():
        return None
    cleaned = value.strip()
    # Try DD-MM-YYYY or DD/MM/YYYY
    m = re.match(r"^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$", cleaned)
    if m:
        day, month, year = int(m.group(1)), int(m.group(2)), int(m.group(3))
        try:
            return str(date(year, month, day))
        except ValueError:
            return cleaned
    return cleaned


class ExactOnlineParser(BaseParser):
    """Parser for Exact Online CSV exports (Mutaties and Relaties).

    Exact Online is the dominant Dutch accounting platform. Their exports have
    specific conventions:
    - Dutch column names (Dagboek, Grootboekrekening, etc.)
    - Comma as decimal separator (1.250,00 = 1250.00)
    - Date format DD-MM-YYYY
    - Debet/Credit as separate columns (not signed)
    - Exact-specific BTW (VAT) and Dagboek (journal) codes
    - Semicolon-delimited (common for Dutch locale exports)

    Supports two export types:
    1. Mutaties (journal entries/transactions) — detected by Grootboekrekening column
    2. Relaties (contacts/relations) — detected by Relatienummer column
    """

    def source_type(self) -> str:
        return "exact_online"

    def source_label(self) -> str:
        return "Exact Online Export (Netherlands)"

    def detect(self, content: bytes, filename: str) -> float:
        """Detect Exact Online exports by Dutch accounting column names."""
        if not filename.lower().endswith((".csv", ".txt")):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        first_line = text.split("\n")[0].strip()

        # Mutaties detection: Grootboekrekening is the strongest signal
        if "Grootboekrekening" in first_line:
            return 0.90

        # Dagboek + Boekstuk together is also very specific to Exact Online
        if "Dagboek" in first_line and "Boekstuk" in first_line:
            return 0.90

        # Relaties detection
        if "Relatienummer" in first_line and "Naam" in first_line:
            if "Adres" in first_line or "KVK" in first_line or "KvK" in first_line:
                return 0.90
            return 0.80

        return 0.0

    def schema(self) -> SchemaAnnotation:
        # Return the Mutaties schema by default (most common export)
        return self._mutaties_schema()

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        # Strip BOM
        text = text.lstrip("\ufeff")

        # Detect delimiter: Exact Online typically uses semicolon (Dutch locale)
        first_line = text.split("\n")[0]
        delimiter = ";" if first_line.count(";") > first_line.count(",") else ","

        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        if not reader.fieldnames:
            return ParseResult(success=False, error="No CSV headers found")

        # Determine export type from headers
        headers_set = set(h.strip() for h in reader.fieldnames)

        if "Grootboekrekening" in headers_set or (
            "Dagboek" in headers_set and "Boekstuk" in headers_set
        ):
            return self._parse_mutaties(reader, filename)
        elif "Relatienummer" in headers_set:
            return self._parse_relaties(reader, filename)
        else:
            return ParseResult(
                success=False,
                error=(
                    "Detected as Exact Online but could not determine export type. "
                    f"Headers: {', '.join(reader.fieldnames)}"
                ),
            )

    # ── Mutaties ──────────────────────────────────────────────────────────

    def _mutaties_schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label="Exact Online Mutaties (Journal Entries)",
            fields=[
                FieldAnnotation(
                    name="journal_code",
                    dtype="string",
                    description="Dagboek code — identifies the journal type",
                    examples=["10", "20", "70", "90"],
                    enum_values=list(DAGBOEK_CODES.keys()),
                ),
                FieldAnnotation(
                    name="journal_description",
                    dtype="string",
                    description="Human-readable journal type derived from Dagboek code",
                    examples=list(DAGBOEK_CODES.values())[:3],
                ),
                FieldAnnotation(
                    name="entry_number",
                    dtype="string",
                    description="Boekstuk — unique entry/voucher number within the journal",
                ),
                FieldAnnotation(
                    name="date",
                    dtype="date",
                    description="Transaction date",
                    format="YYYY-MM-DD",
                ),
                FieldAnnotation(
                    name="ledger_account",
                    dtype="string",
                    description="Grootboekrekening — general ledger account number",
                    examples=["1000", "4000", "8000"],
                ),
                FieldAnnotation(
                    name="ledger_description",
                    dtype="string",
                    description="Description of the ledger account (if present in export)",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="description",
                    dtype="string",
                    description="Transaction description/narrative",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="debit",
                    dtype="decimal",
                    description="Debit amount (money going out / expenses). Original Exact exports have Debet and Credit as separate positive columns.",
                    unit="EUR",
                    format="dot_decimal",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="credit",
                    dtype="decimal",
                    description="Credit amount (money coming in / revenue). Original Exact exports have Debet and Credit as separate positive columns.",
                    unit="EUR",
                    format="dot_decimal",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="vat_code",
                    dtype="string",
                    description="BTW-code — Exact Online VAT code",
                    nullable=True,
                    enum_values=list(BTW_CODES.keys()),
                ),
                FieldAnnotation(
                    name="vat_description",
                    dtype="string",
                    description="Human-readable VAT rate derived from BTW code",
                    nullable=True,
                    examples=list(BTW_CODES.values())[:3],
                ),
                FieldAnnotation(
                    name="vat_amount",
                    dtype="decimal",
                    description="BTW-bedrag — VAT amount for this entry",
                    unit="EUR",
                    format="dot_decimal",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="cost_center",
                    dtype="string",
                    description="Kostenplaats — cost center code",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="relation_code",
                    dtype="string",
                    description="Relatiecode — linked customer/supplier code",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="relation_name",
                    dtype="string",
                    description="Relatienaam — linked customer/supplier name",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="invoice_number",
                    dtype="string",
                    description="Factuurnummer — linked invoice number",
                    nullable=True,
                ),
            ],
            conventions=[
                "Source: Exact Online Mutaties export. Column names translated from Dutch.",
                "Debet/Credit are separate columns in the original — both are positive values. A row has either Debet OR Credit filled, not both.",
                "Dutch comma decimals converted to dot notation (1.250,00 → 1250.00).",
                "Original date format DD-MM-YYYY normalized to YYYY-MM-DD.",
                "Dagboek codes: 10=Inkoop(Purchase), 20=Verkoop(Sales), 30=Kas(Cash), 70=Memoriaal(General), 90=Bank.",
                "BTW codes: 0=exempt, 1=21%(standard), 2=9%(reduced), 3=0%(export/EU), 5=reverse charge, 6=0%(non-EU services).",
                "Delimiter is typically semicolon (Dutch locale). Some exports use comma.",
                "Empty Debet/Credit fields mean 0.00 — not null. A journal entry always balances: sum(Debet) = sum(Credit) per Boekstuk.",
            ],
        )

    def _parse_mutaties(self, reader: csv.DictReader, filename: str) -> ParseResult:
        col_lookup = self._build_lookup(reader.fieldnames or [], MUTATIES_COLUMN_MAP)
        rows: list[dict] = []
        warnings: list[str] = []

        for i, row in enumerate(reader):
            try:
                parsed = self._parse_mutatie_row(row, col_lookup)
                rows.append(parsed)
            except Exception as e:
                warnings.append(f"Row {i + 1}: {e}")

        if not rows:
            return ParseResult(success=False, error="No rows could be parsed from Mutaties export")

        schema = self._mutaties_schema()
        schema.source_label = f"Exact Online Mutaties: {filename}" if filename else schema.source_label

        envelope = ContextEnvelope(
            schema=schema,
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _parse_mutatie_row(self, row: dict, col_lookup: dict) -> dict:
        journal_code = self._get(row, col_lookup, "journal_code")
        journal_desc = DAGBOEK_CODES.get(journal_code, "")

        raw_date = self._get(row, col_lookup, "date")
        parsed_date = _parse_dutch_date(raw_date) or raw_date

        debit = _parse_dutch_decimal(self._get(row, col_lookup, "debit"))
        credit = _parse_dutch_decimal(self._get(row, col_lookup, "credit"))

        vat_code = self._get(row, col_lookup, "vat_code")
        vat_desc = BTW_CODES.get(vat_code, "")

        vat_amount = _parse_dutch_decimal(self._get(row, col_lookup, "vat_amount"))

        return {
            "journal_code": journal_code,
            "journal_description": journal_desc,
            "entry_number": self._get(row, col_lookup, "entry_number"),
            "date": parsed_date,
            "ledger_account": self._get(row, col_lookup, "ledger_account"),
            "ledger_description": self._get(row, col_lookup, "ledger_description"),
            "description": self._get(row, col_lookup, "description"),
            "debit": str(debit) if debit is not None else "0.00",
            "credit": str(credit) if credit is not None else "0.00",
            "vat_code": vat_code or None,
            "vat_description": vat_desc or None,
            "vat_amount": str(vat_amount) if vat_amount is not None else None,
            "cost_center": self._get(row, col_lookup, "cost_center") or None,
            "relation_code": self._get(row, col_lookup, "relation_code") or None,
            "relation_name": self._get(row, col_lookup, "relation_name") or None,
            "invoice_number": self._get(row, col_lookup, "invoice_number") or None,
        }

    # ── Relaties ──────────────────────────────────────────────────────────

    def _relaties_schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label="Exact Online Relaties (Contacts)",
            fields=[
                FieldAnnotation(name="relation_number", dtype="string", description="Relatienummer — unique relation/contact ID"),
                FieldAnnotation(name="relation_type", dtype="string", description="Relatietype — customer, supplier, or both", nullable=True),
                FieldAnnotation(name="name", dtype="string", description="Company or person name"),
                FieldAnnotation(name="address", dtype="string", description="Street address", nullable=True),
                FieldAnnotation(name="postal_code", dtype="string", description="Dutch postal code (1234 AB format)", nullable=True),
                FieldAnnotation(name="city", dtype="string", description="City/town", nullable=True),
                FieldAnnotation(name="country", dtype="string", description="Country name or code", nullable=True),
                FieldAnnotation(name="phone", dtype="string", description="Phone number", nullable=True),
                FieldAnnotation(name="email", dtype="string", description="Email address", nullable=True),
                FieldAnnotation(name="website", dtype="string", description="Website URL", nullable=True),
                FieldAnnotation(name="chamber_of_commerce", dtype="string", description="KVK-nummer — Dutch Chamber of Commerce registration number", nullable=True),
                FieldAnnotation(name="vat_number", dtype="string", description="BTW-nummer — VAT registration number", nullable=True),
                FieldAnnotation(name="iban", dtype="string", description="Bank account IBAN", nullable=True),
                FieldAnnotation(name="bic", dtype="string", description="Bank BIC/SWIFT code", nullable=True),
                FieldAnnotation(name="payment_terms", dtype="string", description="Betalingsconditie — payment terms", nullable=True),
                FieldAnnotation(name="credit_limit", dtype="decimal", description="Kredietlimiet — credit limit", unit="EUR", nullable=True),
                FieldAnnotation(name="blocked", dtype="string", description="Blokkeren — whether this relation is blocked", nullable=True),
                FieldAnnotation(name="notes", dtype="string", description="Opmerkingen — free-text notes", nullable=True),
            ],
            conventions=[
                "Source: Exact Online Relaties export. Column names translated from Dutch.",
                "KVK-nummer is the Dutch Chamber of Commerce number (8 digits).",
                "BTW-nummer is the Dutch VAT number (NL + 9 digits + B + 2 digits).",
                "Relation types: D=Debiteur(Customer), C=Crediteur(Supplier), or both.",
            ],
        )

    def _parse_relaties(self, reader: csv.DictReader, filename: str) -> ParseResult:
        col_lookup = self._build_lookup(reader.fieldnames or [], RELATIES_COLUMN_MAP)
        rows: list[dict] = []
        warnings: list[str] = []

        for i, row in enumerate(reader):
            try:
                parsed = self._parse_relatie_row(row, col_lookup)
                rows.append(parsed)
            except Exception as e:
                warnings.append(f"Row {i + 1}: {e}")

        if not rows:
            return ParseResult(success=False, error="No rows could be parsed from Relaties export")

        schema = self._relaties_schema()
        schema.source_label = f"Exact Online Relaties: {filename}" if filename else schema.source_label

        envelope = ContextEnvelope(
            schema=schema,
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _parse_relatie_row(self, row: dict, col_lookup: dict) -> dict:
        credit_limit = _parse_dutch_decimal(self._get(row, col_lookup, "credit_limit"))

        return {
            "relation_number": self._get(row, col_lookup, "relation_number"),
            "relation_type": self._get(row, col_lookup, "relation_type") or None,
            "name": self._get(row, col_lookup, "name"),
            "address": self._get(row, col_lookup, "address") or None,
            "postal_code": self._get(row, col_lookup, "postal_code") or None,
            "city": self._get(row, col_lookup, "city") or None,
            "country": self._get(row, col_lookup, "country") or None,
            "phone": self._get(row, col_lookup, "phone") or None,
            "email": self._get(row, col_lookup, "email") or None,
            "website": self._get(row, col_lookup, "website") or None,
            "chamber_of_commerce": self._get(row, col_lookup, "chamber_of_commerce") or None,
            "vat_number": self._get(row, col_lookup, "vat_number") or None,
            "iban": self._get(row, col_lookup, "iban") or None,
            "bic": self._get(row, col_lookup, "bic") or None,
            "payment_terms": self._get(row, col_lookup, "payment_terms") or None,
            "credit_limit": str(credit_limit) if credit_limit is not None else None,
            "blocked": self._get(row, col_lookup, "blocked") or None,
            "notes": self._get(row, col_lookup, "notes") or None,
        }

    # ── Shared helpers ────────────────────────────────────────────────────

    def _build_lookup(self, fieldnames: list[str], column_map: dict) -> dict:
        """Build column lookup: normalized key → original header name."""
        lookup = {}
        for header in fieldnames:
            clean = header.strip().strip('"')
            if clean in column_map:
                lookup[column_map[clean]] = header
        return lookup

    def _get(self, row: dict, col_lookup: dict, key: str, default: str = "") -> str:
        """Get a value from a row using the normalized column lookup."""
        header = col_lookup.get(key)
        if header is None:
            return default
        return row.get(header, default).strip()

    def _decode(self, content: bytes) -> str | None:
        """Try multiple encodings common for Dutch business exports."""
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


# Auto-register
registry.register(ExactOnlineParser())
