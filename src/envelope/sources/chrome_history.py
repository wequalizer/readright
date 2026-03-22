"""Google Takeout Chrome browsing history JSON parser."""

from __future__ import annotations

import json
from datetime import datetime

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


class ChromeHistoryParser(BaseParser):
    """Parser for Google Takeout Chrome browsing history JSON exports.

    Google Takeout exports Chrome history as BrowserHistory.json containing
    a "Browser History" key with an array of visit objects.
    """

    def source_type(self) -> str:
        return "chrome_history"

    def source_label(self) -> str:
        return "Google Takeout Chrome Browsing History"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".json"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        try:
            data = json.loads(text)
        except (json.JSONDecodeError, ValueError):
            return 0.0

        if not isinstance(data, dict):
            return 0.0

        if "Browser History" in data:
            history = data["Browser History"]
            if isinstance(history, list):
                return 0.95
            return 0.85

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="title", dtype="string", description="Page title as displayed in the browser"),
                FieldAnnotation(name="url", dtype="string", description="Full URL of the visited page"),
                FieldAnnotation(name="timestamp", dtype="datetime", description="When the page was visited",
                                format="ISO 8601 UTC (converted from Chrome microsecond timestamp)"),
                FieldAnnotation(name="transition_type", dtype="string", description="How the user navigated to the page",
                                examples=["LINK", "TYPED", "AUTO_BOOKMARK", "RELOAD", "FORM_SUBMIT"]),
            ],
            conventions=[
                "time_usec is microseconds since the Unix epoch. Converted to ISO 8601 UTC.",
                "Transition types indicate navigation method: LINK (clicked a link), TYPED (typed URL), AUTO_BOOKMARK (from bookmarks bar), etc.",
                "Page titles may be empty for pages that didn't finish loading or had no <title> tag.",
                "URLs include full query strings and fragments.",
                "The export contains all synced Chrome history, not just one device.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        try:
            data = json.loads(text)
        except (json.JSONDecodeError, ValueError) as e:
            return ParseResult(success=False, error=f"Invalid JSON: {e}")

        if not isinstance(data, dict) or "Browser History" not in data:
            return ParseResult(success=False, error="Missing 'Browser History' key in JSON")

        history = data["Browser History"]
        if not isinstance(history, list):
            return ParseResult(success=False, error="'Browser History' is not an array")

        rows = []
        warnings = []

        for i, entry in enumerate(history):
            if not isinstance(entry, dict):
                warnings.append(f"Entry {i}: not a dict, skipped")
                continue

            # Convert time_usec (microseconds since epoch) to ISO datetime
            timestamp = None
            time_usec = entry.get("time_usec")
            if time_usec is not None:
                try:
                    ts = int(time_usec)
                    timestamp = datetime.utcfromtimestamp(ts / 1_000_000).isoformat() + "Z"
                except (ValueError, OSError, OverflowError):
                    warnings.append(f"Entry {i}: invalid time_usec '{time_usec}'")

            rows.append({
                "title": entry.get("title", ""),
                "url": entry.get("url", ""),
                "timestamp": timestamp,
                "transition_type": entry.get("page_transition", ""),
            })

        if not rows:
            return ParseResult(success=False, error="No history entries found")

        envelope = ContextEnvelope(
            schema=self.schema(),
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _decode(self, content: bytes) -> str | None:
        for enc in ["utf-8-sig", "utf-8", "latin-1"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(ChromeHistoryParser())
