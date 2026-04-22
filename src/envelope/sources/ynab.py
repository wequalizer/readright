"""YNAB (You Need A Budget) CSV export parser."""

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

# YNAB CSV export headers (stable across YNAB 4 and YNAB 5/nYNAB)
_YNAB_HEADERS = {
    "Account", "Flag", "Date", "Payee",
    "Category Group/Category", "Category Group", "Category",
    "Memo", "Outflow", "Inflow", "Cleared",
}

# Minimum required headers to identify a YNAB file
_REQUIRED_HEADERS = {"Account", "Date", "Payee", "Outflow", "Inflow"}

# YNAB 4 legacy headers (slightly different set)
_YNAB4_HEADERS = {"Account", "Flag", "Check Number", "Date", "Payee",
                  "Category", "Memo", "Outflow", "Inflow", "Running Balance"}

_CLEARED_MAP = {
    "C": "cleared",
    "R": "reconciled",
    "U": "uncleared",
    "": "uncleared",
}


def _parse_ynab_date(raw: str) -> str:
    """Parse YNAB date to ISO YYYY-MM-DD.

    YNAB 5 (nYNAB): MM/DD/YYYY or YYYY-MM-DD
    YNAB 4: MM/DD/YYYY
    """
    if not raw:
        return ""
    raw = raw.strip()

    # Already ISO
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return raw

    # MM/DD/YYYY
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", raw)
    if m:
        month, day, year = m.groups()
        try:
            return date(int(year), int(month), int(day)).isoformat()
        except ValueError:
            pass

    # DD/MM/YYYY (some regional YNAB exports)
    m = re.match(r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$", raw)
    if m:
        day, month, year = m.groups()
        try:
            return date(int(year), int(month), int(day)).isoformat()
        except ValueError:
            pass

    return raw


def _parse_ynab_amount(raw: str) -> Decimal:
    """Parse YNAB currency amount.

    YNAB uses locale-dependent formatting:
    - US: 1,234.56 (comma thousands, dot decimal)
    - Some exports use no thousands separator: 1234.56
    - Always non-negative in the Outflow/Inflow columns
    """
    if not raw:
        return Decimal("0")
    # Strip currency symbols and whitespace
    cleaned = re.sub(r"[€$£¥\s]", "", raw.strip())
    # Remove thousands separator (comma before 3 digits before end or dot)
    # Detect: if there are both commas and dots, the rightmost is decimal
    if "," in cleaned and "." in cleaned:
        # e.g. "1,234.56" → remove commas
        cleaned = cleaned.replace(",", "")
    elif "," in cleaned and "." not in cleaned:
        # Could be European: "1.234,56" style (but YNAB typically uses dot)
        # Or just "1,234" — treat comma as thousands if last group is 3 digits
        if re.match(r"^\d{1,3}(,\d{3})+$", cleaned):
            cleaned = cleaned.replace(",", "")
        else:
            # Treat as decimal separator
            cleaned = cleaned.replace(",", ".")

    try:
        return Decimal(cleaned)
    except (InvalidOperation, Exception):
        raise ValueError(f"YNAB: could not parse amount '{raw.strip()}'")


class YNABParser(BaseParser):
    """Parser for YNAB (You Need A Budget) CSV exports.

    YNAB exports transactions as a flat CSV with separate Outflow and Inflow columns
    (both non-negative). This parser normalizes them to a single signed Amount field.

    Supported YNAB versions:
    - YNAB 5 / nYNAB (web app): headers include 'Category Group/Category'
    - YNAB 4 (desktop app): headers include 'Check Number', 'Running Balance'

    Export path in YNAB 5:
        Budget → All Accounts → (select date range) → Export

    Key conventions:
    - Outflow is money leaving the account (expense), Inflow is money coming in
    - Amount is computed as: Inflow - Outflow (negative for expenses)
    - Category Group/Category is a combined field: "Food:Groceries"
    - Payee may be a transfer: "Transfer : Account Name"
    - Flag is a color label (Red, Orange, Yellow, Green, Blue, Purple) or empty
    - Cleared: C=cleared, R=reconciled (blank=uncleared in some exports)
    """

    def source_type(self) -> str:
        return "ynab_csv"

    def source_label(self) -> str:
        return "YNAB Budget Export CSV"

    def detect(self, content: bytes, filename: str) -> float:
        name_lower = filename.lower()

        if not name_lower.endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        text = text.lstrip("\ufeff")
        first_line = text.split("\n")[0]
        headers = {h.strip().strip('"') for h in first_line.split(",")}

        # nYNAB / YNAB 5
        if _REQUIRED_HEADERS.issubset(headers) and "Category Group/Category" in headers:
            return 0.95

        # YNAB 4
        if _REQUIRED_HEADERS.issubset(headers) and "Running Balance" in headers:
            return 0.92

        # Generic YNAB-like (required headers only)
        if _REQUIRED_HEADERS.issubset(headers):
            # Distinguish from other bank exports: YNAB has both Outflow AND Inflow
            if "Outflow" in headers and "Inflow" in headers:
                return 0.85

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="date", dtype="date",
                                description="Transaction date", format="YYYY-MM-DD"),
                FieldAnnotation(name="amount", dtype="decimal",
                                description="Signed amount: negative = expense (outflow), positive = income (inflow)",
                                unit="account currency"),
                FieldAnnotation(name="outflow", dtype="decimal",
                                description="Raw outflow (money out), always non-negative", nullable=True),
                FieldAnnotation(name="inflow", dtype="decimal",
                                description="Raw inflow (money in), always non-negative", nullable=True),
                FieldAnnotation(name="account", dtype="string",
                                description="Budget account name"),
                FieldAnnotation(name="payee", dtype="string",
                                description="Payee name. Transfers show as 'Transfer : Account Name'",
                                nullable=True),
                FieldAnnotation(name="category", dtype="string",
                                description="Full category path: 'Group: Subcategory'", nullable=True),
                FieldAnnotation(name="category_group", dtype="string",
                                description="Category group name", nullable=True),
                FieldAnnotation(name="memo", dtype="string",
                                description="Transaction memo / note", nullable=True),
                FieldAnnotation(name="cleared", dtype="enum",
                                description="Cleared/reconciled status",
                                enum_values=["cleared", "reconciled", "uncleared"]),
                FieldAnnotation(name="flag", dtype="string",
                                description="Color flag label",
                                enum_values=["Red", "Orange", "Yellow", "Green", "Blue", "Purple", ""],
                                nullable=True),
                FieldAnnotation(name="is_transfer", dtype="boolean",
                                description="True if this is an internal account transfer"),
            ],
            conventions=[
                "Amount = Inflow - Outflow. Negative = expense, positive = income.",
                "Transfer transactions (Payee starts with 'Transfer :') appear in both the source and destination accounts — they cancel out when summing total spending.",
                "Category is empty for transfers and for uncategorized transactions.",
                "YNAB 5 combines category group and category in 'Category Group/Category' as 'Group: Category'.",
                "Cleared: 'C' = manually cleared, 'R' = reconciled, '' = uncleared.",
                "Date format is MM/DD/YYYY in most regional settings or YYYY-MM-DD — normalized to YYYY-MM-DD.",
                "Outflow and Inflow are ALWAYS non-negative in the YNAB export — the sign is implicit in which column has a value.",
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

        # Normalize header names (strip whitespace and quotes)
        fieldnames = [f.strip().strip('"') for f in reader.fieldnames]

        # Detect YNAB version
        is_ynab4 = "Running Balance" in fieldnames and "Category Group/Category" not in fieldnames

        rows = []
        warnings = []

        for i, row in enumerate(reader):
            # Re-map with cleaned keys
            clean_row = {k.strip().strip('"'): v for k, v in row.items() if k}
            try:
                parsed = self._parse_row(clean_row, is_ynab4)
                rows.append(parsed)
            except Exception as e:
                warnings.append(f"Row {i + 1}: {e}")

        if not rows:
            return ParseResult(success=False, error="No rows could be parsed")

        if is_ynab4:
            warnings.insert(0, "YNAB 4 export detected (legacy format).")

        envelope = ContextEnvelope(
            schema=self.schema(),
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _parse_row(self, row: dict, is_ynab4: bool) -> dict:
        def get(key: str) -> str:
            return (row.get(key) or "").strip()

        outflow = _parse_ynab_amount(get("Outflow"))
        inflow = _parse_ynab_amount(get("Inflow"))
        amount = inflow - outflow

        # Category: YNAB 5 has 'Category Group/Category', YNAB 4 just 'Category'
        if is_ynab4:
            category = get("Category") or None
            category_group = None
        else:
            combined = get("Category Group/Category")
            # Split "Group: Category" → group and category
            if ": " in combined:
                category_group, category = combined.split(": ", 1)
                category_group = category_group.strip() or None
                category = category.strip() or None
            elif combined:
                category = combined
                category_group = get("Category Group") or None
            else:
                category = None
                category_group = get("Category Group") or None

        payee = get("Payee") or None
        is_transfer = bool(payee and payee.startswith("Transfer :"))

        cleared_raw = get("Cleared").upper()
        cleared = _CLEARED_MAP.get(cleared_raw, "uncleared")

        return {
            "date": _parse_ynab_date(get("Date")),
            "amount": str(amount),
            "outflow": str(outflow) if outflow else None,
            "inflow": str(inflow) if inflow else None,
            "account": get("Account"),
            "payee": payee,
            "category": category,
            "category_group": category_group,
            "memo": get("Memo") or None,
            "cleared": cleared,
            "flag": get("Flag") or None,
            "is_transfer": is_transfer,
        }

    def _decode(self, content: bytes) -> str | None:
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(YNABParser())
