"""Google Takeout 'My Activity' JSON parser."""

from __future__ import annotations

import json
from datetime import datetime

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Known Google product names for reference
_KNOWN_PRODUCTS = {
    "Search", "Chrome", "YouTube", "Maps", "Gmail", "Drive", "Photos",
    "Calendar", "Play", "News", "Shopping", "Assistant", "Discover",
    "Image Search", "Video Search", "Books", "Finance", "Translate",
}


def _parse_timestamp(raw: str) -> str:
    """Normalize Google Takeout timestamp to ISO 8601.

    Google uses RFC 3339 / ISO 8601 with milliseconds:
    '2024-01-01T12:00:00.000Z'
    """
    if not raw:
        return ""
    # Already valid ISO — normalize to remove trailing Z
    raw = raw.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
        return dt.isoformat()
    except ValueError:
        return raw


def _extract_url(record: dict) -> str | None:
    """Extract URL from titleUrl or subtitles[0].url."""
    if "titleUrl" in record:
        return record["titleUrl"]
    subtitles = record.get("subtitles", [])
    if subtitles and isinstance(subtitles, list):
        return subtitles[0].get("url") if subtitles[0] else None
    return None


def _extract_subtitle(record: dict) -> str | None:
    """Extract subtitle text."""
    subtitles = record.get("subtitles", [])
    if subtitles and isinstance(subtitles, list):
        return subtitles[0].get("name") if subtitles[0] else None
    return None


def _extract_details(record: dict) -> str | None:
    """Extract details list as joined string."""
    details = record.get("details", [])
    if not details:
        return None
    parts = []
    for d in details:
        if isinstance(d, dict):
            text = d.get("name") or d.get("value") or ""
            if text:
                parts.append(str(text))
    return "; ".join(parts) if parts else None


class GoogleTakeoutActivityParser(BaseParser):
    """Parser for Google Takeout 'My Activity' JSON exports.

    Google Takeout exports per-product activity as JSON arrays.
    Each activity record contains:
        header   — product category (Search, YouTube, Maps, etc.)
        title    — human-readable description of the activity
        time     — ISO 8601 timestamp with milliseconds
        products — list of Google products involved
        subtitles — optional list of {name, url} for richer context
        titleUrl — direct URL to the item (e.g. YouTube video link)
        details  — additional structured details

    The export structure varies by product:
    - Search: title = "Searched for <query>"
    - YouTube: title = "Watched <video title>"
    - Maps: title = "Searched for <place>" or "Visited <place>"
    - Chrome: title = "Visited <site>"

    Multiple JSON files may exist (one per product). Each is a flat JSON array.
    """

    def source_type(self) -> str:
        return "google_takeout_activity_json"

    def source_label(self) -> str:
        return "Google Takeout My Activity JSON"

    def detect(self, content: bytes, filename: str) -> float:
        name_lower = filename.lower()

        # Google Takeout activity files are commonly named:
        # MyActivity.json, My Activity.json, Search/MyActivity.json, etc.
        if "myactivity" in name_lower.replace(" ", "") or "my activity" in name_lower:
            if name_lower.endswith(".json"):
                return 0.85

        if not name_lower.endswith(".json"):
            return 0.0

        # Content sniff: must be a JSON array with 'header' and 'title' and 'time' keys
        try:
            text = content[:8192].decode("utf-8", errors="replace")
            # Look for the structural markers
            if '"header"' in text and '"title"' in text and '"time"' in text:
                if '"products"' in text or '"subtitles"' in text:
                    # Verify it parses as a list
                    sample = content[:65536].decode("utf-8", errors="replace").strip()
                    if sample.startswith("["):
                        return 0.80
        except Exception:
            pass

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="time", dtype="date",
                                description="When the activity occurred (ISO 8601)",
                                format="ISO8601"),
                FieldAnnotation(name="header", dtype="string",
                                description="Google product category",
                                examples=["Search", "YouTube", "Maps", "Chrome"]),
                FieldAnnotation(name="title", dtype="string",
                                description="Human-readable activity description",
                                examples=["Searched for climate change", "Watched Never Gonna Give You Up"]),
                FieldAnnotation(name="title_url", dtype="string",
                                description="Direct URL to the item (YouTube video, Maps place, etc.)",
                                nullable=True),
                FieldAnnotation(name="products", dtype="string",
                                description="Google products involved, comma-separated",
                                nullable=True),
                FieldAnnotation(name="subtitle", dtype="string",
                                description="Secondary label (e.g. channel name for YouTube, site domain)",
                                nullable=True),
                FieldAnnotation(name="details", dtype="string",
                                description="Additional structured details, semicolon-separated",
                                nullable=True),
            ],
            conventions=[
                "The 'title' field uses natural-language prefixes: 'Searched for', 'Watched', 'Visited', etc.",
                "To extract the raw search query: strip 'Searched for ' from title where header == 'Search'.",
                "Timestamps are UTC. Convert to local time for meaningful daily/hourly analysis.",
                "title_url is absent for many record types, especially Search queries.",
                "Google may export one file per product or one combined file depending on the export settings.",
                "Records are in reverse-chronological order in the original file.",
                "Deleted or auto-deleted activity will not appear in exports.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        try:
            text = content.decode("utf-8", errors="replace")
        except Exception as e:
            return ParseResult(success=False, error=f"Could not decode content: {e}")

        try:
            raw_data = json.loads(text)
        except json.JSONDecodeError as e:
            return ParseResult(success=False, error=f"JSON parse error: {e}")

        if not isinstance(raw_data, list):
            return ParseResult(success=False, error="Expected a JSON array at the top level")

        rows = []
        warnings = []

        for i, record in enumerate(raw_data):
            if not isinstance(record, dict):
                warnings.append(f"Item {i}: not a dict, skipped")
                continue
            try:
                row = self._parse_record(record)
                rows.append(row)
            except Exception as e:
                warnings.append(f"Item {i}: {e}")

        if not rows:
            return ParseResult(success=False, error="No activity records could be parsed")

        envelope = ContextEnvelope(
            schema=self.schema(),
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _parse_record(self, record: dict) -> dict:
        products = record.get("products", [])
        if isinstance(products, list):
            products_str = ", ".join(str(p) for p in products) if products else None
        else:
            products_str = str(products)

        return {
            "time": _parse_timestamp(record.get("time", "")),
            "header": record.get("header", ""),
            "title": record.get("title", ""),
            "title_url": _extract_url(record),
            "products": products_str,
            "subtitle": _extract_subtitle(record),
            "details": _extract_details(record),
        }


registry.register(GoogleTakeoutActivityParser())
