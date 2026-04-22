"""Netflix viewing history CSV parser."""

from __future__ import annotations

import csv
import io
import re
from datetime import datetime

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Netflix date formats by locale
_DATE_FORMATS = [
    ("%m/%d/%Y", re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")),     # US: M/DD/YYYY
    ("%d/%m/%Y", re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$")),     # EU: DD/MM/YYYY (same shape, tried second)
    ("%d-%m-%Y", re.compile(r"^\d{1,2}-\d{1,2}-\d{4}$")),     # EU alt
    ("%Y-%m-%d", re.compile(r"^\d{4}-\d{2}-\d{2}$")),         # ISO (already normalized)
]


def _normalize_date(raw: str) -> str:
    """Normalize Netflix date string to YYYY-MM-DD. Returns raw if unparseable."""
    raw = raw.strip()
    if not raw:
        return raw
    for fmt, pattern in _DATE_FORMATS:
        if pattern.match(raw):
            try:
                return datetime.strptime(raw, fmt).date().isoformat()
            except ValueError:
                continue
    return raw


class NetflixViewingParser(BaseParser):
    """Parser for Netflix viewing history CSV exports.

    Netflix allows users to download their viewing history from
    Account > Profile > Viewing activity > Download all.
    The export is a simple CSV with Title and Date columns.
    """

    def source_type(self) -> str:
        return "netflix_viewing"

    def source_label(self) -> str:
        return "Netflix Viewing History"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        first_line = text.split("\n")[0].strip()

        # Netflix exports have exactly "Title" and "Date" headers
        # Normalize: strip BOM and quotes
        headers = [h.strip().strip('"').strip("'") for h in first_line.split(",")]

        if headers == ["Title", "Date"]:
            return 0.90
        # Also match with extra whitespace
        normalized = [h.strip() for h in headers]
        if normalized == ["Title", "Date"]:
            return 0.90

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(
                    name="title",
                    dtype="string",
                    description="Title of the show or movie watched. For series includes season and episode info.",
                    examples=["Breaking Bad: Season 1: Pilot", "The Matrix"],
                ),
                FieldAnnotation(
                    name="date",
                    dtype="date",
                    description="Date the content was watched",
                    format="M/DD/YYYY or DD/MM/YYYY depending on account locale",
                ),
            ],
            conventions=[
                "Date format depends on account locale — US accounts use M/DD/YYYY, EU accounts typically DD/MM/YYYY.",
                "Series titles are formatted as 'Show Name: Season X: Episode Title'.",
                "The colon-separated format can be split to extract show, season, and episode separately.",
                "The export only includes titles the user actually watched (not browsed).",
                "No duration data is included — only the fact that something was watched on a given date.",
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

        rows = []
        warnings = []

        for i, row in enumerate(reader):
            title = row.get("Title", "").strip()
            date_val = row.get("Date", "").strip()

            if not title and not date_val:
                warnings.append(f"Row {i + 1}: empty row, skipped")
                continue

            rows.append({
                "title": title,
                "date": _normalize_date(date_val),
            })

        if not rows:
            return ParseResult(success=False, error="No viewing entries found")

        envelope = ContextEnvelope(
            schema=self.schema(),
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    # _decode() inherited from BaseParser


registry.register(NetflixViewingParser())
