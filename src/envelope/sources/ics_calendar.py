"""ICS/iCal calendar export parser."""

from __future__ import annotations

import re

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


class ICSCalendarParser(BaseParser):
    """Parser for ICS/iCal calendar exports.

    Handles .ics files from Google Calendar, Apple Calendar, Outlook, etc.
    Parses VEVENT blocks manually without external dependencies.
    """

    def source_type(self) -> str:
        return "ics_calendar"

    def source_label(self) -> str:
        return "ICS/iCal Calendar Export"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".ics"):
            # Still check content even without .ics extension
            text = self._decode(content)
            if text is None:
                return 0.0
            if "BEGIN:VCALENDAR" in text[:500]:
                return 0.85
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        if text.strip().startswith("BEGIN:VCALENDAR"):
            return 0.95

        if "BEGIN:VEVENT" in text:
            return 0.90

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="summary", dtype="string", description="Event title/summary"),
                FieldAnnotation(name="dtstart", dtype="datetime", description="Event start date/time", format="ISO 8601 or YYYYMMDD"),
                FieldAnnotation(name="dtend", dtype="datetime", description="Event end date/time", format="ISO 8601 or YYYYMMDD", nullable=True),
                FieldAnnotation(name="location", dtype="string", description="Event location", nullable=True),
                FieldAnnotation(name="description", dtype="string", description="Event description/notes", nullable=True),
                FieldAnnotation(name="uid", dtype="string", description="Unique event identifier", nullable=True),
                FieldAnnotation(name="status", dtype="string", description="Event status", nullable=True,
                                examples=["CONFIRMED", "TENTATIVE", "CANCELLED"]),
                FieldAnnotation(name="organizer", dtype="string", description="Event organizer", nullable=True),
            ],
            conventions=[
                "Date/time values can be date-only (YYYYMMDD), UTC (YYYYMMDDTHHMMSSZ), or with timezone (DTSTART;TZID=...).",
                "All-day events have date-only values without time component.",
                "Recurring events (RRULE) are represented as single entries — recurrence is not expanded.",
                "Description fields may contain escaped characters: \\n for newline, \\, for comma, \\; for semicolon.",
                "Multi-line values use RFC 5545 line folding: continuation lines start with a space or tab.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        # Unfold lines (RFC 5545: continuation lines start with space/tab)
        text = re.sub(r"\r\n[ \t]", "", text)
        text = re.sub(r"\n[ \t]", "", text)
        # Normalize line endings
        text = text.replace("\r\n", "\n").replace("\r", "\n")

        # Split into VEVENT blocks
        events = re.split(r"BEGIN:VEVENT", text)
        if len(events) < 2:
            return ParseResult(success=False, error="No VEVENT blocks found in ICS file")

        rows = []
        warnings = []

        for i, block in enumerate(events[1:], 1):  # Skip everything before first VEVENT
            # Cut at END:VEVENT
            end_idx = block.find("END:VEVENT")
            if end_idx != -1:
                block = block[:end_idx]

            try:
                event = self._parse_event(block)
                rows.append(event)
            except Exception as e:
                warnings.append(f"Event {i}: {e}")

        if not rows:
            return ParseResult(success=False, error="No events could be parsed")

        envelope = ContextEnvelope(
            schema=self.schema(),
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _parse_event(self, block: str) -> dict:
        """Parse a single VEVENT block into a dict."""
        lines = block.strip().split("\n")

        props = {}
        for line in lines:
            line = line.strip()
            if not line or ":" not in line:
                continue

            # Handle properties with parameters like DTSTART;TZID=Europe/Amsterdam:20240101T100000
            key_part, _, value = line.partition(":")
            # Strip parameters from key
            key = key_part.split(";")[0].upper()
            props[key] = value.strip()

        # Unescape ICS values
        def unescape(val: str) -> str:
            return val.replace("\\n", "\n").replace("\\,", ",").replace("\\;", ";").replace("\\\\", "\\")

        summary = unescape(props.get("SUMMARY", ""))
        dtstart = self._normalize_datetime(props.get("DTSTART", ""))
        dtend = self._normalize_datetime(props.get("DTEND", ""))
        location = unescape(props.get("LOCATION", ""))
        description = unescape(props.get("DESCRIPTION", ""))
        uid = props.get("UID", "")
        status = props.get("STATUS", "")
        organizer = props.get("ORGANIZER", "")

        # Clean up mailto: from organizer
        if organizer.lower().startswith("mailto:"):
            organizer = organizer[7:]

        return {
            "summary": summary or None,
            "dtstart": dtstart or None,
            "dtend": dtend or None,
            "location": location or None,
            "description": description or None,
            "uid": uid or None,
            "status": status or None,
            "organizer": organizer or None,
        }

    def _normalize_datetime(self, val: str) -> str:
        """Normalize ICS datetime to a more readable format.

        Input formats:
          20240101                -> 2024-01-01
          20240101T100000         -> 2024-01-01T10:00:00
          20240101T100000Z        -> 2024-01-01T10:00:00Z
        """
        if not val:
            return ""

        val = val.strip()

        # Date only: YYYYMMDD
        if re.match(r"^\d{8}$", val):
            return f"{val[:4]}-{val[4:6]}-{val[6:8]}"

        # DateTime: YYYYMMDDTHHMMSS[Z]
        m = re.match(r"^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})(Z?)$", val)
        if m:
            return f"{m[1]}-{m[2]}-{m[3]}T{m[4]}:{m[5]}:{m[6]}{m[7]}"

        # Already formatted or unknown — return as-is
        return val

    def _decode(self, content: bytes) -> str | None:
        for enc in ["utf-8-sig", "utf-8", "latin-1"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(ICSCalendarParser())
