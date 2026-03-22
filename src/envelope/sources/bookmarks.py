"""Netscape Bookmark HTML format parser."""

from __future__ import annotations

import re
from datetime import datetime

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


class BookmarksParser(BaseParser):
    """Parser for Netscape Bookmark HTML exports.

    This is the de-facto standard for bookmark import/export used by
    Chrome, Firefox, Safari, Edge, and most bookmark managers.
    The format uses a specific DOCTYPE and nested <DL><DT> structures.
    """

    def source_type(self) -> str:
        return "bookmarks_html"

    def source_label(self) -> str:
        return "Netscape Bookmark HTML Export"

    def detect(self, content: bytes, filename: str) -> float:
        text = self._decode(content)
        if text is None:
            return 0.0

        header = text[:2000].upper()

        if "NETSCAPE-BOOKMARK-FILE-1" in header:
            return 0.95

        if "<!DOCTYPE NETSCAPE-BOOKMARK" in header:
            return 0.95

        # Lower confidence: HTML file with bookmark-like structure
        if filename.lower().endswith((".html", ".htm")):
            if "<DT><A HREF=" in text[:5000].upper():
                return 0.70

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="title", dtype="string", description="Bookmark title"),
                FieldAnnotation(name="url", dtype="string", description="Bookmark URL"),
                FieldAnnotation(name="date_added", dtype="datetime", description="When the bookmark was added",
                                format="ISO 8601 (converted from Unix timestamp)", nullable=True),
                FieldAnnotation(name="folder", dtype="string", description="Folder path where the bookmark lives",
                                nullable=True, examples=["Bookmarks Bar", "Bookmarks Bar/Dev/Python"]),
            ],
            conventions=[
                "ADD_DATE attribute is a Unix timestamp (seconds since epoch). Converted to ISO 8601.",
                "Folder path uses '/' as separator, built from nested <DL> structures.",
                "Some bookmarks may have no folder (top-level) — folder will be empty string.",
                "The format is consistent across Chrome, Firefox, Safari, and Edge exports.",
                "Separators (<HR>) in the bookmark file are ignored.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        rows = []
        warnings = []
        folder_stack: list[str] = []

        # Track folder context via <H3> (folder headers) and <DL> nesting
        # We process line by line to track folder hierarchy
        lines = text.split("\n")

        for line_num, line in enumerate(lines, 1):
            stripped = line.strip()

            # Folder start: <DT><H3 ...>Folder Name</H3>
            folder_match = re.search(r"<DT><H3[^>]*>(.+?)</H3>", stripped, re.IGNORECASE)
            if folder_match:
                folder_name = self._html_unescape(folder_match.group(1))
                folder_stack.append(folder_name)
                continue

            # Folder end: </DL> closes a folder level
            if re.match(r"</DL>", stripped, re.IGNORECASE):
                if folder_stack:
                    folder_stack.pop()
                continue

            # Bookmark: <DT><A HREF="..." ADD_DATE="..." ...>Title</A>
            bm_match = re.search(
                r'<DT><A\s+([^>]*)>(.+?)</A>',
                stripped,
                re.IGNORECASE,
            )
            if bm_match:
                attrs_str = bm_match.group(1)
                title = self._html_unescape(bm_match.group(2))

                href = self._extract_attr(attrs_str, "HREF")
                add_date_raw = self._extract_attr(attrs_str, "ADD_DATE")

                date_added = None
                if add_date_raw and add_date_raw.isdigit():
                    try:
                        ts = int(add_date_raw)
                        if ts > 0:
                            date_added = datetime.utcfromtimestamp(ts).isoformat()
                    except (ValueError, OSError, OverflowError):
                        warnings.append(f"Line {line_num}: invalid ADD_DATE '{add_date_raw}'")

                folder_path = "/".join(folder_stack) if folder_stack else ""

                if href:
                    rows.append({
                        "title": title,
                        "url": href,
                        "date_added": date_added,
                        "folder": folder_path,
                    })

        if not rows:
            return ParseResult(success=False, error="No bookmarks found in file")

        envelope = ContextEnvelope(
            schema=self.schema(),
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _extract_attr(self, attrs_str: str, attr_name: str) -> str | None:
        """Extract an HTML attribute value from an attribute string."""
        pattern = rf'{attr_name}\s*=\s*"([^"]*)"'
        match = re.search(pattern, attrs_str, re.IGNORECASE)
        if match:
            return match.group(1)
        # Try single quotes
        pattern = rf"{attr_name}\s*=\s*'([^']*)'"
        match = re.search(pattern, attrs_str, re.IGNORECASE)
        if match:
            return match.group(1)
        return None

    def _html_unescape(self, text: str) -> str:
        """Basic HTML entity unescaping."""
        text = text.replace("&amp;", "&")
        text = text.replace("&lt;", "<")
        text = text.replace("&gt;", ">")
        text = text.replace("&quot;", '"')
        text = text.replace("&#39;", "'")
        text = text.replace("&apos;", "'")
        return text

    def _decode(self, content: bytes) -> str | None:
        for enc in ["utf-8-sig", "utf-8", "latin-1"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(BookmarksParser())
