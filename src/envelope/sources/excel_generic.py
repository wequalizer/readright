"""Generic Excel (.xlsx) parser using openpyxl.

Dependency: openpyxl
    Install: pip install openpyxl
    openpyxl is NOT bundled with envelope — it must be installed separately.
    It is intentionally kept as an optional dependency to avoid forcing a
    large dependency on users who only need CSV/text parsers.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Maximum number of rows to scan when detecting the header row
_HEADER_SCAN_ROWS = 20
# Minimum number of non-empty cells in a row to be considered a header candidate
_MIN_HEADER_CELLS = 2


def _cell_value(cell: Any) -> Any:
    """Extract a clean Python value from an openpyxl cell."""
    val = cell.value
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.isoformat()
    if isinstance(val, date):
        return val.isoformat()
    if isinstance(val, (int, float)):
        return val
    return str(val).strip()


def _normalize_header(raw: Any) -> str:
    """Turn a cell value into a usable column name."""
    if raw is None:
        return ""
    s = str(raw).strip()
    # Replace non-alphanumeric runs with underscore, lowercase
    s = re.sub(r"[^a-zA-Z0-9]+", "_", s).strip("_").lower()
    return s or "column"


def _infer_dtype(values: list[Any]) -> tuple[str, str | None]:
    """Infer column dtype from a sample of values."""
    non_null = [v for v in values if v is not None and v != ""]
    if not non_null:
        return "string", None

    date_count = sum(1 for v in non_null if isinstance(v, str) and re.match(r"^\d{4}-\d{2}-\d{2}", v))
    numeric_count = sum(1 for v in non_null if isinstance(v, (int, float)))

    if date_count / len(non_null) >= 0.7:
        return "date", "ISO8601"
    if numeric_count / len(non_null) >= 0.7:
        return "decimal", None
    return "string", None


class ExcelGenericParser(BaseParser):
    """Generic parser for Excel .xlsx files.

    Handles common spreadsheet patterns:
    - Auto-detects the header row (first row with multiple non-empty string cells)
    - Converts dates, numbers, and strings to normalized Python types
    - Reads all worksheets or the first/active sheet
    - Skips empty rows
    - Handles merged cells (uses top-left value)

    Limitations:
    - .xls (legacy Excel 97-2003) format is NOT supported — only .xlsx
    - Formulas are read as their cached/computed value
    - Charts, images, and pivot tables are ignored
    - Multi-sheet files: only the first (or active) sheet is parsed by default

    openpyxl must be installed: pip install openpyxl
    """

    def source_type(self) -> str:
        return "excel_generic"

    def source_label(self) -> str:
        return "Generic Excel Spreadsheet (.xlsx)"

    def detect(self, content: bytes, filename: str) -> float:
        name_lower = filename.lower()

        # xlsx files are ZIP archives containing XML
        if name_lower.endswith(".xlsx"):
            # xlsx starts with PK (ZIP magic bytes)
            if content[:2] == b"PK":
                return 0.80
            return 0.50

        # .xlsm (macro-enabled) and .xltx (template) have same format
        if name_lower.endswith((".xlsm", ".xltx", ".xltm")):
            if content[:2] == b"PK":
                return 0.75

        # .xls is the old format — we can detect but not parse
        if name_lower.endswith(".xls"):
            # OLE2 magic: D0 CF 11 E0
            if content[:4] == b"\xd0\xcf\x11\xe0":
                return 0.0  # Explicitly 0: we cannot parse this format

        return 0.0

    def schema(self) -> SchemaAnnotation:
        # Schema is built dynamically during parse
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[],
            conventions=[
                "Only the first sheet is parsed by default — additional sheets are ignored.",
                "Header row is auto-detected as the first row with mostly non-empty string cells.",
                "Merged cells may cause unexpected or duplicated values — inspect output carefully.",
                "Date cells are converted to ISO 8601 strings (YYYY-MM-DD or full datetime).",
                "Formula cells show computed/cached values, not the formula expressions themselves.",
                "Column types are inferred from data values — verify before relying on types.",
                "Only .xlsx format is supported. For .xls (legacy), convert to .xlsx first.",
                "openpyxl must be installed: pip install openpyxl",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        try:
            import openpyxl
        except ImportError:
            return ParseResult(
                success=False,
                error=(
                    "openpyxl is not installed. Install it with: pip install openpyxl"
                ),
            )

        import io

        try:
            wb = openpyxl.load_workbook(
                io.BytesIO(content),
                read_only=True,
                data_only=True,  # Read computed values, not formulas
            )
        except Exception as e:
            return ParseResult(success=False, error=f"Could not open Excel file: {e}")

        # Use first worksheet
        sheet = wb.worksheets[0] if wb.worksheets else None
        if sheet is None:
            return ParseResult(success=False, error="Excel file has no worksheets")

        warnings: list[str] = []
        if len(wb.worksheets) > 1:
            warnings.append(
                f"File has {len(wb.worksheets)} sheets — only '{sheet.title}' (first) was parsed."
            )

        # Read all rows into memory (read_only mode streams rows)
        all_rows: list[list[Any]] = []
        for row in sheet.iter_rows():
            all_rows.append([_cell_value(cell) for cell in row])

        wb.close()

        if not all_rows:
            return ParseResult(success=False, error="Worksheet is empty")

        # Detect header row
        header_row_idx = self._find_header_row(all_rows)
        if header_row_idx is None:
            return ParseResult(success=False, error="Could not detect a header row")

        raw_headers = all_rows[header_row_idx]
        headers = self._make_unique_headers(raw_headers)

        # Data rows: everything after the header row, skip empty rows
        data_rows_raw = all_rows[header_row_idx + 1:]
        rows = []
        for raw_row in data_rows_raw:
            # Pad or truncate to match header count
            padded = (list(raw_row) + [None] * len(headers))[: len(headers)]
            record = dict(zip(headers, padded))
            # Skip entirely empty rows
            if all(v is None or v == "" for v in record.values()):
                continue
            rows.append(record)

        if not rows:
            return ParseResult(success=False, error="No data rows found after header")

        # Infer field types
        fields = self._infer_fields(headers, rows)

        schema = SchemaAnnotation(
            source_type=self.source_type(),
            source_label=f"Excel: {filename}" if filename else self.source_label(),
            fields=fields,
            conventions=[
                f"Sheet: '{sheet.title}'" if hasattr(sheet, 'title') else "First sheet",
                f"Header row detected at row {header_row_idx + 1}",
                f"Columns: {len(headers)}, Data rows: {len(rows)}",
                "Column types inferred from data — verify before relying on types.",
            ],
        )

        envelope = ContextEnvelope(
            schema=schema,
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _find_header_row(self, all_rows: list[list[Any]]) -> int | None:
        """Find the first row that looks like a header (multiple non-empty strings)."""
        for idx, row in enumerate(all_rows[:_HEADER_SCAN_ROWS]):
            non_empty = [v for v in row if v is not None and str(v).strip()]
            string_cells = [v for v in non_empty if isinstance(v, str)]
            numeric_cells = [v for v in non_empty if isinstance(v, (int, float))]

            # A header row has mostly strings and at least _MIN_HEADER_CELLS non-empty cells
            if len(non_empty) >= _MIN_HEADER_CELLS and len(string_cells) >= len(numeric_cells):
                return idx

        return None

    def _make_unique_headers(self, raw_headers: list[Any]) -> list[str]:
        """Normalize headers and ensure uniqueness."""
        seen: dict[str, int] = {}
        result = []
        for i, raw in enumerate(raw_headers):
            name = _normalize_header(raw) if raw is not None else f"column_{i}"
            if not name:
                name = f"column_{i}"
            if name in seen:
                seen[name] += 1
                name = f"{name}_{seen[name]}"
            else:
                seen[name] = 0
            result.append(name)
        return result

    def _infer_fields(self, headers: list[str], rows: list[dict]) -> list[FieldAnnotation]:
        """Infer field types from data values."""
        fields = []
        sample = rows[:100]

        for col in headers:
            values = [row.get(col) for row in sample]
            non_null = [v for v in values if v is not None and v != ""]
            dtype, fmt = _infer_dtype(non_null)
            examples = [str(v) for v in non_null[:3]]

            fields.append(FieldAnnotation(
                name=col,
                dtype=dtype,
                description=f"Auto-inferred {dtype} column",
                format=fmt,
                nullable=len(non_null) < len(values),
                examples=examples or None,
            ))
        return fields


registry.register(ExcelGenericParser())
