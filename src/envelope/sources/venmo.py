"""Venmo transaction CSV export parser."""

from __future__ import annotations

import csv
import io
from datetime import datetime
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Venmo CSV header names → normalized keys.
# The CSV has a leading unnamed index column (empty header or just a comma).
COLUMN_MAP = {
    "Datetime": "datetime",
    "Type": "type",
    "Status": "status",
    "Note": "note",
    "From": "from_name",
    "To": "to_name",
    "Amount (total)": "amount_total",
    "Amount (tip)": "amount_tip",
    "Amount (tax)": "amount_tax",
    "Tax Rate": "tax_rate",
    "Tax Exempt": "tax_exempt",
    "Funding Source": "funding_source",
    "Destination": "destination",
    "Beginning Balance": "beginning_balance",
    "Ending Balance": "ending_balance",
    "Statement Period Begins": "statement_period_begins",
    "Statement Period Ends": "statement_period_ends",
    "ID": "id",
    "Terminal Location": "terminal_location",
    "Year to Date Venmo Fees": "ytd_fees",
    "Disclaimer": "disclaimer",
}

# Venmo exports contain several non-data rows at the top before the actual
# transaction data. The real header row contains 'Datetime' and 'Type'.
_HEADER_SENTINEL = "Datetime"
_MAX_SKIP_ROWS = 10  # give up searching for real header after this many rows


class VenmoParser(BaseParser):
    """Parser for Venmo transaction CSV exports.

    Venmo quirks:
    - The file starts with 1-3 non-data rows (account info, blank lines) before
      the real CSV header row that contains 'Datetime', 'Type', etc.
    - Amount (total) includes a leading '+ ' or '- ' sign with a space and
      a dollar sign: '+ $25.00' or '- $10.00'. Must strip those.
    - 'Beginning Balance' and 'Ending Balance' only appear on specific summary
      rows, not every transaction row.
    - Type values include: 'Payment', 'Charge', 'Standard Transfer',
      'Instant Transfer', 'Merchant Transaction'.
    - Status values: 'Complete', 'Pending', 'Issued', 'Failed'.
    - The 'From' and 'To' fields are display names, not account IDs.
    - statement_period_begins/ends appear only on the first row.
    """

    def source_type(self) -> str:
        return "venmo_csv"

    def source_label(self) -> str:
        return "Venmo Transaction CSV Export"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        text = text.lstrip("\ufeff")

        # Look for the Venmo header within first several lines
        for line in text.split("\n")[:_MAX_SKIP_ROWS]:
            if "Datetime" in line and "Amount (total)" in line:
                return 0.97
            if "Datetime" in line and "Funding Source" in line:
                return 0.93
        # Venmo-specific column set
        if "Amount (tip)" in text[:2000] and "Funding Source" in text[:2000]:
            return 0.85

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="id", dtype="string", description="Venmo transaction ID", nullable=True),
                FieldAnnotation(name="datetime", dtype="date", description="Transaction date and time", format="YYYY-MM-DDTHH:MM:SS"),
                FieldAnnotation(name="type", dtype="string", description="Transaction type", examples=["Payment", "Charge", "Standard Transfer", "Instant Transfer", "Merchant Transaction"]),
                FieldAnnotation(name="status", dtype="string", description="Transaction status", examples=["Complete", "Pending", "Issued", "Failed"]),
                FieldAnnotation(name="note", dtype="string", description="Note or memo added by sender", nullable=True),
                FieldAnnotation(name="from_name", dtype="string", description="Sender display name", nullable=True),
                FieldAnnotation(name="to_name", dtype="string", description="Recipient display name", nullable=True),
                FieldAnnotation(name="amount_total", dtype="decimal", description="Total amount (positive = received, negative = sent)", unit="USD"),
                FieldAnnotation(name="amount_tip", dtype="decimal", description="Tip portion of transaction", unit="USD", nullable=True),
                FieldAnnotation(name="amount_tax", dtype="decimal", description="Tax portion of transaction", unit="USD", nullable=True),
                FieldAnnotation(name="tax_rate", dtype="string", description="Tax rate applied", nullable=True),
                FieldAnnotation(name="tax_exempt", dtype="boolean", description="Whether transaction is tax exempt", nullable=True),
                FieldAnnotation(name="funding_source", dtype="string", description="Payment source for outgoing transactions", examples=["Venmo balance", "Chase Sapphire Preferred", "Bank Account"], nullable=True),
                FieldAnnotation(name="destination", dtype="string", description="Destination for incoming transfers", examples=["Venmo balance", "Bank Account"], nullable=True),
                FieldAnnotation(name="beginning_balance", dtype="decimal", description="Venmo balance at statement start (appears on first row only)", unit="USD", nullable=True),
                FieldAnnotation(name="ending_balance", dtype="decimal", description="Venmo balance at statement end (appears on last row only)", unit="USD", nullable=True),
                FieldAnnotation(name="statement_period_begins", dtype="date", description="Start of statement period", nullable=True),
                FieldAnnotation(name="statement_period_ends", dtype="date", description="End of statement period", nullable=True),
                FieldAnnotation(name="terminal_location", dtype="string", description="Physical terminal location for in-person payments", nullable=True),
                FieldAnnotation(name="ytd_fees", dtype="decimal", description="Year-to-date Venmo fees", unit="USD", nullable=True),
            ],
            conventions=[
                "Amount (total) uses '+ $25.00' / '- $10.00' format in the raw file. Normalized to signed decimal here.",
                "Positive amount = money received. Negative amount = money sent.",
                "The CSV file starts with 1-3 preamble rows that are not data — they are skipped during parsing.",
                "From/To fields are display names, not unique IDs — the same person can appear with different names.",
                "Transfers to/from a bank account appear as 'Standard Transfer' or 'Instant Transfer' with no counterparty name.",
                "Currency is always USD — Venmo does not support multi-currency.",
                "Balance fields only appear on the first and last summary rows, not on every transaction.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        text = text.lstrip("\ufeff")
        lines = text.split("\n")

        # Find the real header row
        header_line_idx = None
        for i, line in enumerate(lines[:_MAX_SKIP_ROWS]):
            if _HEADER_SENTINEL in line:
                header_line_idx = i
                break

        if header_line_idx is None:
            return ParseResult(success=False, error="Could not locate Venmo header row (Datetime column not found in first 10 lines)")

        # Reconstruct CSV from the real header onwards
        csv_text = "\n".join(lines[header_line_idx:])
        reader = csv.DictReader(io.StringIO(csv_text))

        if not reader.fieldnames:
            return ParseResult(success=False, error="No CSV headers found after skip")

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
                # Skip blank/summary-only rows (no datetime, no type)
                if not parsed.get("datetime") and not parsed.get("type"):
                    continue
                rows.append(parsed)
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
        """Handle '+ $25.00', '- $10.00', '$25.00', '25.00'."""
        raw = raw.strip()
        if not raw:
            return None
        negative = raw.startswith("-")
        # Strip sign, whitespace, currency symbol
        raw = raw.lstrip("+-").strip().lstrip("$").replace(",", "")
        if not raw:
            return None
        try:
            value = Decimal(raw)
            return -value if negative else value
        except InvalidOperation:
            return None

    def _parse_datetime(self, raw: str) -> str | None:
        raw = raw.strip()
        if not raw:
            return None
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(raw, fmt).isoformat()
            except ValueError:
                continue
        return raw

    def _parse_bool(self, raw: str) -> bool | None:
        raw = raw.strip().lower()
        if raw in ("true", "yes", "1"):
            return True
        if raw in ("false", "no", "0"):
            return False
        return None

    def _parse_row(self, row: dict, col_lookup: dict) -> dict:
        return {
            "id": self._get(row, col_lookup, "id") or None,
            "datetime": self._parse_datetime(self._get(row, col_lookup, "datetime")),
            "type": self._get(row, col_lookup, "type") or None,
            "status": self._get(row, col_lookup, "status") or None,
            "note": self._get(row, col_lookup, "note") or None,
            "from_name": self._get(row, col_lookup, "from_name") or None,
            "to_name": self._get(row, col_lookup, "to_name") or None,
            "amount_total": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "amount_total"))) is not None else None,
            "amount_tip": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "amount_tip"))) is not None else None,
            "amount_tax": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "amount_tax"))) is not None else None,
            "tax_rate": self._get(row, col_lookup, "tax_rate") or None,
            "tax_exempt": self._parse_bool(self._get(row, col_lookup, "tax_exempt")),
            "funding_source": self._get(row, col_lookup, "funding_source") or None,
            "destination": self._get(row, col_lookup, "destination") or None,
            "beginning_balance": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "beginning_balance"))) is not None else None,
            "ending_balance": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "ending_balance"))) is not None else None,
            "statement_period_begins": self._parse_datetime(self._get(row, col_lookup, "statement_period_begins")),
            "statement_period_ends": self._parse_datetime(self._get(row, col_lookup, "statement_period_ends")),
            "terminal_location": self._get(row, col_lookup, "terminal_location") or None,
            "ytd_fees": str(v) if (v := self._parse_amount(self._get(row, col_lookup, "ytd_fees"))) is not None else None,
        }

    def _decode(self, content: bytes) -> str | None:
        for enc in ("utf-8-sig", "utf-8", "latin-1", "cp1252"):
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(VenmoParser())
