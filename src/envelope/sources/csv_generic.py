"""Generic CSV parser with auto-detection — fallback when no specific parser matches."""

from __future__ import annotations

import csv
import io
import re
from collections import Counter

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


class GenericCSVParser(BaseParser):
    """Fallback parser for unknown CSV files.

    Detects: delimiter, encoding, header row, column types.
    Annotates: inferred field types and formats.
    """

    def source_type(self) -> str:
        return "csv_generic"

    def source_label(self) -> str:
        return "Generic CSV File"

    def detect(self, content: bytes, filename: str) -> float:
        """Low-confidence match for any CSV-like file."""
        if filename.lower().endswith((".csv", ".tsv", ".txt")):
            # Check if it looks like delimited data
            encoding = self.detect_encoding(content)
            try:
                text = content.decode(encoding)
            except (UnicodeDecodeError, LookupError):
                return 0.0

            lines = text.strip().split("\n")
            if len(lines) < 2:
                return 0.0

            # Count delimiter candidates in first line
            first = lines[0]
            for delim in [",", ";", "\t", "|"]:
                if first.count(delim) >= 1:
                    # Check consistency across lines
                    counts = [line.count(delim) for line in lines[:10]]
                    if len(set(counts)) <= 2:  # Allow 1 variance
                        return 0.30  # Low confidence — specific parsers should win
            return 0.0
        return 0.0

    def schema(self) -> SchemaAnnotation:
        # Will be dynamically built during parse
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[],
            conventions=[
                "This is a generic CSV parse. Column types are inferred from data, not from a known schema.",
                "Type inference is best-effort: date, numeric, and string types are detected from values.",
                "For accurate schema annotation, register a specific parser for this source type.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        encoding = self.detect_encoding(content)
        try:
            text = content.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            for enc in ["utf-8-sig", "latin-1", "cp1252"]:
                try:
                    text = content.decode(enc)
                    break
                except (UnicodeDecodeError, LookupError):
                    continue
            else:
                return ParseResult(success=False, error="Could not decode file encoding")

        # Detect delimiter
        delimiter = self._detect_delimiter(text)

        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        if not reader.fieldnames:
            return ParseResult(success=False, error="No headers found in CSV")

        rows = list(reader)
        if not rows:
            return ParseResult(success=False, error="CSV has headers but no data rows")

        # Infer field types
        fields = self._infer_fields(reader.fieldnames, rows)

        schema = SchemaAnnotation(
            source_type=self.source_type(),
            source_label=f"Generic CSV: {filename}" if filename else self.source_label(),
            fields=fields,
            conventions=[
                "Column types auto-inferred from data values. Verify before relying on types.",
                f"Delimiter: {'tab' if delimiter == chr(9) else repr(delimiter)}",
                f"Encoding: {encoding}",
                f"Columns: {len(fields)}, Rows: {len(rows)}",
            ],
        )

        envelope = ContextEnvelope(
            schema=schema,
            data=rows,
            warnings=["Generic parser used — consider registering a specific parser for this source type."],
        )
        return ParseResult(success=True, envelope=envelope)

    def _detect_delimiter(self, text: str) -> str:
        """Detect CSV delimiter from content."""
        first_lines = text.split("\n")[:5]
        candidates = {",": 0, ";": 0, "\t": 0, "|": 0}

        for line in first_lines:
            for delim in candidates:
                candidates[delim] += line.count(delim)

        if not any(candidates.values()):
            return ","

        return max(candidates, key=candidates.get)

    def _infer_fields(self, fieldnames: list[str], rows: list[dict]) -> list[FieldAnnotation]:
        """Infer field types from data."""
        fields = []
        sample = rows[:100]  # Infer from first 100 rows

        for col in fieldnames:
            values = [row.get(col, "").strip() for row in sample if row.get(col, "").strip()]
            dtype, fmt = self._infer_type(values)
            examples = list(dict.fromkeys(values[:3]))  # Unique examples, preserve order

            fields.append(FieldAnnotation(
                name=col,
                dtype=dtype,
                description=f"Auto-inferred {dtype} column",
                format=fmt,
                nullable=len(values) < len(sample),
                examples=examples,
            ))

        return fields

    def _infer_type(self, values: list[str]) -> tuple[str, str | None]:
        """Infer the most likely type for a column."""
        if not values:
            return "string", None

        type_votes = Counter()

        for val in values:
            if self._looks_like_date(val):
                type_votes["date"] += 1
            elif self._looks_like_number(val):
                type_votes["decimal"] += 1
            elif val.lower() in ("true", "false", "yes", "no", "ja", "nee", "0", "1"):
                type_votes["boolean"] += 1
            else:
                type_votes["string"] += 1

        winner = type_votes.most_common(1)[0][0] if type_votes else "string"

        # Detect format for dates
        fmt = None
        if winner == "date":
            fmt = self._detect_date_format(values[0])

        return winner, fmt

    def _looks_like_date(self, val: str) -> bool:
        date_patterns = [
            r"^\d{4}[-/]\d{2}[-/]\d{2}$",
            r"^\d{2}[-/]\d{2}[-/]\d{4}$",
            r"^\d{2}[-/]\d{2}[-/]\d{2}$",
            r"^\d{8}$",
        ]
        return any(re.match(p, val) for p in date_patterns)

    def _looks_like_number(self, val: str) -> bool:
        cleaned = val.replace(",", ".").replace(" ", "").lstrip("-+").lstrip("€$£")
        if not cleaned:
            return False
        try:
            float(cleaned)
            return True
        except ValueError:
            return False

    def _detect_date_format(self, val: str) -> str | None:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", val):
            return "YYYY-MM-DD"
        if re.match(r"^\d{2}-\d{2}-\d{4}$", val):
            return "DD-MM-YYYY (or MM-DD-YYYY)"
        if re.match(r"^\d{2}/\d{2}/\d{4}$", val):
            return "DD/MM/YYYY (or MM/DD/YYYY)"
        if re.match(r"^\d{8}$", val):
            return "YYYYMMDD"
        return None


registry.register(GenericCSVParser())
