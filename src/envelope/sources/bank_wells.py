"""Wells Fargo (US) CSV export parser."""

from __future__ import annotations

import csv
import io
from datetime import date
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Wells Fargo exports have NO header row in most account types.
# Columns by position: Date, Amount, *, *, Description
# The middle two columns are typically asterisk placeholders or internal codes.
# Some online banking exports may include a header; we handle both cases.
WELLS_WITH_HEADER = {"Date", "Amount", "Description"}
# Wells Fargo positional column count
WELLS_COLUMN_COUNT = 5


class WellsFargoParser(BaseParser):
    """Parser for Wells Fargo (US) CSV exports.

    Wells Fargo CSV quirks:
    - Most exports have NO header row — columns are positional
    - Column positions: [0]=Date, [1]=Amount, [2]=*, [3]=*, [4]=Description
    - The '*' columns are internal Wells Fargo codes (check type, transaction code)
    - Date format is MM/DD/YYYY
    - Amount is already signed: negative = debit, positive = credit
    - Some online exports include a header row (detected and handled)
    - Encoding is typically UTF-8 or latin-1
    - Description strings often contain internal transaction codes
    """

    # Synthetic header names used when the file has no header row
    SYNTHETIC_HEADERS = ["date", "amount", "wf_code_1", "wf_code_2", "description"]

    def source_type(self) -> str:
        return "wellsfargo_csv_us"

    def source_label(self) -> str:
        return "Wells Fargo CSV Export (US)"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        text = text.lstrip("\ufeff")
        lines = [l.strip() for l in text.split("\n") if l.strip()]
        if not lines:
            return 0.0

        first_line = lines[0]

        # Check for explicit header variant
        headers = {h.strip().strip('"') for h in first_line.split(",")}
        if WELLS_WITH_HEADER.issubset(headers) and "Running Bal." not in headers and "Balance" not in headers:
            # Exclude BofA and Chase which also have Date/Amount/Description
            # Require exactly 5 columns — Wells Fargo header format always has 5
            if len(headers) == 5:
                return 0.55  # Low confidence without stronger signal
            # 3-column or 4-column files with these headers are more likely HSBC/BofA variants
            # Do not claim them

        # Headerless detection: first line should look like a data row
        # with 5 columns, first column a date, second a signed number
        parts = self._split_csv_line(first_line)
        if len(parts) == WELLS_COLUMN_COUNT:
            date_candidate = parts[0].strip().strip('"')
            amount_candidate = parts[1].strip().strip('"')
            # Wells Fargo headerless: middle columns are often empty or "*"
            mid1 = parts[2].strip().strip('"')
            mid2 = parts[3].strip().strip('"')

            date_looks_right = (
                len(date_candidate) == 10
                and date_candidate[2] == "/"
                and date_candidate[5] == "/"
            )
            amount_looks_right = self._looks_like_amount(amount_candidate)
            mid_looks_right = mid1 in ("", "*") and mid2 in ("", "*")

            if date_looks_right and amount_looks_right and mid_looks_right:
                return 0.85

            if date_looks_right and amount_looks_right:
                return 0.65

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
                    description="Signed transaction amount: negative = debit (money out), positive = credit (money in)",
                    unit="USD",
                    format="dot_decimal",
                    examples=["-25.00", "500.00", "-199.99"],
                ),
                FieldAnnotation(
                    name="direction",
                    dtype="enum",
                    description="Derived from sign of amount: debit = money out, credit = money in",
                    enum_values=["debit", "credit"],
                ),
                FieldAnnotation(
                    name="description",
                    dtype="string",
                    description="Transaction description or merchant name from the last column",
                    examples=["PURCHASE AUTHORIZED ON 01/15 COSTCO WHOLESALE", "ACH CREDIT DIRECT DEP"],
                ),
                FieldAnnotation(
                    name="wf_code_1",
                    dtype="string",
                    description="Wells Fargo internal code column 3 (often empty or asterisk)",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="wf_code_2",
                    dtype="string",
                    description="Wells Fargo internal code column 4 (often empty or asterisk)",
                    nullable=True,
                ),
            ],
            conventions=[
                "Wells Fargo exports typically have NO header row. Columns are positional: [0]=Date, [1]=Amount, [2]=internal, [3]=internal, [4]=Description.",
                "Amount is ALREADY signed: negative = money out (debit), positive = money in (credit).",
                "Date format is MM/DD/YYYY in the raw file. Normalized to YYYY-MM-DD.",
                "Currency is always USD. No currency column in the export.",
                "Columns 2 and 3 (wf_code_1, wf_code_2) are Wells Fargo internal codes, often blank or literal asterisk '*'. Their meaning is undocumented.",
                "Description often contains the original transaction date, merchant name, and a Wells Fargo transaction reference — all concatenated.",
                "If a header row is detected, it is used; otherwise synthetic column names are applied.",
            ],
            notes=[
                "Wells Fargo's headerless CSV format makes it one of the trickier US bank formats to detect reliably.",
                "The description field typically contains richer data than the merchant name alone.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        text = text.lstrip("\ufeff")
        lines = [l for l in text.split("\n") if l.strip()]
        if not lines:
            return ParseResult(success=False, error="File is empty")

        first_line = lines[0]
        has_header = self._first_line_is_header(first_line)

        if has_header:
            csv_text = "\n".join(lines)
            reader = csv.DictReader(io.StringIO(csv_text))
        else:
            # Inject synthetic header
            csv_text = "\n".join(lines)
            reader = csv.DictReader(
                io.StringIO(csv_text),
                fieldnames=self.SYNTHETIC_HEADERS,
            )

        rows = []
        warnings = []

        for i, row in enumerate(reader):
            try:
                parsed = self._parse_row(row, has_header, warnings)
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

    def _first_line_is_header(self, line: str) -> bool:
        """Return True if the first line looks like a header rather than data."""
        parts = self._split_csv_line(line)
        if not parts:
            return False
        first = parts[0].strip().strip('"').lower()
        # If the first column is 'date' it's a header
        if first == "date":
            return True
        # If the first column looks like a date value, it's a data row
        candidate = parts[0].strip().strip('"')
        if len(candidate) == 10 and candidate[2] == "/" and candidate[5] == "/":
            return False
        return False

    def _parse_row(self, row: dict, has_header: bool, warnings: list | None = None) -> dict:
        if has_header:
            # Header variant: column names vary, use best guesses
            date_key = next((k for k in row if "date" in k.lower()), None)
            amount_key = next((k for k in row if "amount" in k.lower()), None)
            desc_key = next((k for k in row if "description" in k.lower()), None)
            raw_date = (row.get(date_key, "") if date_key else "").strip().strip('"')
            raw_amount_orig = (row.get(amount_key, "0") if amount_key else "0").strip().strip('"')
            raw_desc = (row.get(desc_key, "") if desc_key else "").strip().strip('"')
            # WF code columns: anything left
            keys = list(row.keys())
            wf_code_1 = row.get(keys[2], "").strip().strip('"') if len(keys) > 2 else None
            wf_code_2 = row.get(keys[3], "").strip().strip('"') if len(keys) > 3 else None
        else:
            raw_date = row.get("date", "").strip().strip('"')
            raw_amount_orig = row.get("amount", "0").strip().strip('"')
            raw_desc = row.get("description", "").strip().strip('"')
            wf_code_1 = row.get("wf_code_1", "").strip().strip('"') or None
            wf_code_2 = row.get("wf_code_2", "").strip().strip('"') or None

        tx_date = self._parse_date(raw_date)

        raw_amount = raw_amount_orig.replace(",", "")
        try:
            amount = Decimal(raw_amount)
        except (InvalidOperation, Exception):
            if warnings is not None:
                warnings.append(f"Row {len(warnings)+1}: could not parse amount '{raw_amount_orig}', defaulting to 0")
            amount = Decimal("0")

        is_debit = amount < 0

        return {
            "date": str(tx_date),
            "amount": str(amount),
            "currency": "USD",
            "direction": "debit" if is_debit else "credit",
            "description": raw_desc,
            "wf_code_1": wf_code_1 if wf_code_1 and wf_code_1 != "*" else None,
            "wf_code_2": wf_code_2 if wf_code_2 and wf_code_2 != "*" else None,
        }

    def _parse_date(self, raw: str) -> date | str:
        """Parse MM/DD/YYYY into date."""
        raw = raw.strip()
        if not raw:
            return raw
        if len(raw) == 10 and raw[2] == "/" and raw[5] == "/":
            try:
                return date(int(raw[6:10]), int(raw[0:2]), int(raw[3:5]))
            except ValueError:
                pass
        return raw

    def _looks_like_amount(self, s: str) -> bool:
        """Return True if s looks like a signed decimal number."""
        s = s.strip().strip('"').replace(",", "")
        if not s:
            return False
        if s[0] in ("+", "-"):
            s = s[1:]
        return s.replace(".", "", 1).isdigit()

    def _split_csv_line(self, line: str) -> list[str]:
        """Split a single CSV line respecting quoted fields."""
        try:
            return next(csv.reader([line]))
        except StopIteration:
            return []

    def _decode(self, content: bytes) -> str | None:
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(WellsFargoParser())
