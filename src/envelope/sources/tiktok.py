"""TikTok data export parser — parses the JSON from TikTok's 'Download your data' feature.

Handles both the full export and individual sections (Video, Activity, etc.).
No external dependencies — uses stdlib json.
"""

from __future__ import annotations

import json
from datetime import datetime

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


def _parse_tiktok_date(raw: str) -> str:
    """Normalize TikTok date to ISO 8601. TikTok uses 'YYYY-MM-DD HH:MM:SS' format."""
    if not raw:
        return ""
    raw = raw.strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw, fmt).isoformat()
        except ValueError:
            continue
    return raw


class TikTokExportParser(BaseParser):
    """Parser for TikTok 'Download your data' JSON exports.

    TikTok exports contain sections like:
    - Activity (browsing history, searches, likes, shares)
    - Video (posted videos with engagement metrics)
    - Direct Messages
    - Profile information
    - Login History

    This parser extracts all sections into a flat row-per-item format.
    """

    def source_type(self) -> str:
        return "tiktok_export"

    def source_label(self) -> str:
        return "TikTok Data Export"

    def detect(self, content: bytes, filename: str) -> float:
        name_lower = filename.lower()

        # Strong filename signals
        if "tiktok" in name_lower and name_lower.endswith(".json"):
            return 0.90
        if name_lower in ("user_data.json", "user_data_tiktok.json"):
            return 0.92

        if not name_lower.endswith(".json"):
            return 0.0

        # Content sniff
        try:
            head = content[:4096].decode("utf-8", errors="replace")
            # TikTok exports have distinctive top-level keys
            tiktok_keys = ('"Activity"', '"Video"', '"Direct Messages"',
                           '"Profile"', '"Follower"', '"Following"',
                           '"Like List"', '"Comment"', '"Login History"',
                           '"Browsing History"', '"Search History"')
            matches = sum(1 for k in tiktok_keys if k in head)
            if matches >= 3:
                return 0.88
            if matches >= 2:
                return 0.70
        except Exception:
            pass

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="section", dtype="string",
                                description="Top-level export section",
                                examples=["Video", "Activity", "Direct Messages", "Login History"]),
                FieldAnnotation(name="subsection", dtype="string",
                                description="Subsection within the section",
                                nullable=True),
                FieldAnnotation(name="date", dtype="date",
                                description="Timestamp of the item",
                                format="ISO8601", nullable=True),
                FieldAnnotation(name="title", dtype="string",
                                description="Item title or description",
                                nullable=True),
                FieldAnnotation(name="url", dtype="string",
                                description="Link to the content (video, profile, etc.)",
                                nullable=True),
                FieldAnnotation(name="value", dtype="string",
                                description="Primary value (search query, message text, etc.)",
                                nullable=True),
                FieldAnnotation(name="extra", dtype="string",
                                description="Additional data as JSON string",
                                nullable=True),
            ],
            conventions=[
                "Each row is one item from the export (one video, one search, one message, etc.).",
                "section + subsection identify the data category.",
                "Dates are normalized to ISO 8601 from TikTok's YYYY-MM-DD HH:MM:SS format.",
                "The 'extra' field contains additional key-value pairs as a JSON string for fields that vary by section.",
                "Direct messages contain text but not media content.",
                "Video section includes engagement metrics (likes, comments, shares) in the extra field when available.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        try:
            text = content.decode("utf-8", errors="replace")
        except Exception as e:
            return ParseResult(success=False, error=f"Could not decode: {e}")

        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            return ParseResult(success=False, error=f"JSON parse error: {e}")

        if not isinstance(data, dict):
            return ParseResult(success=False, error="Expected a JSON object at the top level")

        rows: list[dict] = []
        warnings: list[str] = []
        section_counts: dict[str, int] = {}

        for section_name, section_data in data.items():
            if not isinstance(section_data, (dict, list)):
                continue

            items = self._extract_section(section_name, section_data)
            rows.extend(items)
            section_counts[section_name] = len(items)

        if not rows:
            return ParseResult(success=False, error="No parseable data found in TikTok export")

        conventions = list(self.schema().conventions)
        sections_str = ", ".join(f"{k}: {v} items" for k, v in sorted(section_counts.items()))
        conventions.append(f"Sections found: {sections_str}")

        schema = SchemaAnnotation(
            source_type=self.source_type(),
            source_label=f"TikTok Export: {filename}" if filename else self.source_label(),
            fields=self.schema().fields,
            conventions=conventions,
        )

        envelope = ContextEnvelope(
            schema=schema,
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _extract_section(self, section_name: str, section_data) -> list[dict]:
        """Recursively extract items from a section."""
        rows = []

        if isinstance(section_data, list):
            for item in section_data:
                row = self._item_to_row(section_name, None, item)
                if row:
                    rows.append(row)
        elif isinstance(section_data, dict):
            for subsection_name, subsection_data in section_data.items():
                if isinstance(subsection_data, list):
                    for item in subsection_data:
                        row = self._item_to_row(section_name, subsection_name, item)
                        if row:
                            rows.append(row)
                elif isinstance(subsection_data, dict):
                    # Nested dict — try one more level
                    for sub2_name, sub2_data in subsection_data.items():
                        if isinstance(sub2_data, list):
                            label = f"{subsection_name} > {sub2_name}"
                            for item in sub2_data:
                                row = self._item_to_row(section_name, label, item)
                                if row:
                                    rows.append(row)

        return rows

    def _item_to_row(self, section: str, subsection: str | None, item) -> dict | None:
        """Convert a single TikTok export item to a row."""
        if isinstance(item, str):
            return {
                "section": section,
                "subsection": subsection,
                "date": None,
                "title": None,
                "url": None,
                "value": item,
                "extra": None,
            }

        if not isinstance(item, dict):
            return None

        # Extract common fields with various TikTok key naming conventions
        date = None
        for key in ("Date", "date", "TimeStamp", "timestamp", "Time", "time", "CreatedTime"):
            if key in item:
                date = _parse_tiktok_date(str(item[key]))
                break

        title = item.get("Title") or item.get("title") or item.get("Description") or item.get("description")
        url = (item.get("Link") or item.get("link") or item.get("Url") or item.get("url")
               or item.get("VideoLink") or item.get("video_url"))
        value = (item.get("SearchTerm") or item.get("search_term") or item.get("Text") or item.get("text")
                 or item.get("Content") or item.get("content") or item.get("Comment") or item.get("comment"))

        # Collect remaining fields as extra
        known_keys = {"Date", "date", "TimeStamp", "timestamp", "Time", "time", "CreatedTime",
                       "Title", "title", "Description", "description",
                       "Link", "link", "Url", "url", "VideoLink", "video_url",
                       "SearchTerm", "search_term", "Text", "text",
                       "Content", "content", "Comment", "comment"}
        extra_data = {k: v for k, v in item.items() if k not in known_keys and v}
        extra = json.dumps(extra_data, ensure_ascii=False) if extra_data else None

        return {
            "section": section,
            "subsection": subsection,
            "date": date,
            "title": str(title) if title else None,
            "url": str(url) if url else None,
            "value": str(value) if value else None,
            "extra": extra,
        }


registry.register(TikTokExportParser())
