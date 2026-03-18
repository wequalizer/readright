"""WhatsApp chat export (.txt) parser."""

from __future__ import annotations

import re
from datetime import datetime

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# WhatsApp date patterns vary by locale
# Dutch: DD-MM-YY HH:MM or DD-MM-YYYY HH:MM
# US: MM/DD/YY, HH:MM AM/PM
# ISO: YYYY-MM-DD HH:MM
PATTERNS = [
    # Dutch/EU: DD-MM-YY HH:MM or DD/MM/YY HH:MM
    re.compile(r"^\[?(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}),?\s+(\d{1,2}:\d{2}(?::\d{2})?)\]?\s*[-–—]\s*(.+?):\s(.+)$"),
    # US: MM/DD/YY, HH:MM AM/PM
    re.compile(r"^\[?(\d{1,2}/\d{1,2}/\d{2,4}),?\s+(\d{1,2}:\d{2}(?::\d{2})?\s*[APap][Mm])\]?\s*[-–—]\s*(.+?):\s(.+)$"),
    # System messages (no sender)
    re.compile(r"^\[?(\d{1,2}[-/]\d{1,2}[-/]\d{2,4}),?\s+(\d{1,2}:\d{2}(?::\d{2})?(?:\s*[APap][Mm])?)\]?\s*[-–—]\s*(.+)$"),
]

DATE_FORMATS = [
    "%d-%m-%Y %H:%M",
    "%d-%m-%y %H:%M",
    "%d/%m/%Y %H:%M",
    "%d/%m/%y %H:%M",
    "%m/%d/%Y %I:%M %p",
    "%m/%d/%y %I:%M %p",
    "%d-%m-%Y %H:%M:%S",
    "%d-%m-%y %H:%M:%S",
]

MEDIA_INDICATORS = [
    "<Media omitted>", "<media omitted>",
    "<Media weggelaten>",  # Dutch
    "afbeelding weggelaten", "video weggelaten", "audio weggelaten",
    "image omitted", "video omitted", "audio omitted",
    "sticker omitted", "GIF omitted", "document omitted",
    "Contact card omitted",
]

SYSTEM_INDICATORS = [
    "created group", "added", "removed", "left", "changed the subject",
    "changed this group", "changed the group", "Messages and calls are end-to-end encrypted",
    "security code changed", "groep gemaakt", "heeft", "toegevoegd", "verwijderd",
]


class WhatsAppParser(BaseParser):
    def source_type(self) -> str:
        return "whatsapp_txt"

    def source_label(self) -> str:
        return "WhatsApp Chat Export (.txt)"

    def detect(self, content: bytes, filename: str) -> float:
        if filename.lower().startswith("whatsapp"):
            return 0.80

        encoding = self.detect_encoding(content)
        try:
            text = content.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            return 0.0

        # Check first 20 lines for WhatsApp message patterns
        lines = text.split("\n")[:20]
        matches = 0
        for line in lines:
            for pattern in PATTERNS:
                if pattern.match(line.strip()):
                    matches += 1
                    break

        if matches >= 3:
            return 0.95
        if matches >= 1:
            return 0.60
        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="timestamp", dtype="datetime", description="Message timestamp", format="ISO 8601"),
                FieldAnnotation(name="sender", dtype="string", description="Sender name or phone number"),
                FieldAnnotation(name="text", dtype="string", description="Message text content"),
                FieldAnnotation(name="is_media", dtype="boolean", description="True if this is a media message (image, video, etc.)"),
                FieldAnnotation(name="media_type", dtype="string", description="Type of media if is_media=true", nullable=True),
                FieldAnnotation(name="is_system", dtype="boolean", description="True for system messages (group created, user added/removed, etc.)"),
            ],
            conventions=[
                "WhatsApp .txt exports have locale-dependent date formats. Timestamps are normalized to ISO 8601.",
                "Multi-line messages are joined — continuation lines (no timestamp prefix) are appended to the previous message.",
                "Media messages show '<Media omitted>' (English) or '<Media weggelaten>' (Dutch) in the text.",
                "System messages (group events, encryption notices) have is_system=true and sender='system'.",
                "Phone numbers as sender names indicate contacts not saved in the exporter's phone.",
                "The export does NOT include: read receipts, reactions, message edits, deleted messages.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        encoding = self.detect_encoding(content)
        try:
            text = content.decode(encoding)
        except (UnicodeDecodeError, LookupError):
            for enc in ["utf-8-sig", "utf-8", "latin-1"]:
                try:
                    text = content.decode(enc)
                    break
                except (UnicodeDecodeError, LookupError):
                    continue
            else:
                return ParseResult(success=False, error="Could not decode file encoding")

        # Remove BOM
        text = text.lstrip("\ufeff")

        messages = []
        current_msg = None
        warnings = []

        for line_num, line in enumerate(text.split("\n"), 1):
            line = line.strip()
            if not line:
                continue

            parsed = self._try_parse_line(line)

            if parsed:
                if current_msg:
                    messages.append(current_msg)
                current_msg = parsed
            elif current_msg:
                # Continuation line — append to current message
                current_msg["text"] += "\n" + line
            else:
                warnings.append(f"Line {line_num}: Could not parse, skipped")

        # Don't forget last message
        if current_msg:
            messages.append(current_msg)

        if not messages:
            return ParseResult(success=False, error="No messages could be parsed")

        envelope = ContextEnvelope(
            schema=self.schema(),
            data=messages,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _try_parse_line(self, line: str) -> dict | None:
        """Try all patterns on a line."""
        for pattern in PATTERNS:
            match = pattern.match(line)
            if not match:
                continue

            groups = match.groups()

            if len(groups) == 4:
                # Normal message: date, time, sender, text
                date_str, time_str, sender, text = groups
            elif len(groups) == 3:
                # System message: date, time, text (no sender)
                date_str, time_str, text = groups
                sender = "system"
            else:
                continue

            timestamp = self._parse_datetime(date_str, time_str)
            is_media = any(indicator in text for indicator in MEDIA_INDICATORS)
            is_system = sender == "system" or any(ind in text.lower() for ind in SYSTEM_INDICATORS)

            media_type = ""
            if is_media:
                text_lower = text.lower()
                for mt in ["image", "video", "audio", "sticker", "gif", "document", "contact",
                           "afbeelding", "foto"]:
                    if mt in text_lower:
                        media_type = mt
                        break
                if not media_type:
                    media_type = "unknown"

            return {
                "timestamp": timestamp.isoformat() if isinstance(timestamp, datetime) else timestamp,
                "sender": sender.strip(),
                "text": text.strip(),
                "is_media": is_media,
                "media_type": media_type,
                "is_system": is_system if sender == "system" else False,
            }

        return None

    def _parse_datetime(self, date_str: str, time_str: str) -> datetime | str:
        """Try multiple datetime formats."""
        combined = f"{date_str.strip()} {time_str.strip()}"
        for fmt in DATE_FORMATS:
            try:
                return datetime.strptime(combined, fmt)
            except ValueError:
                continue
        return combined


registry.register(WhatsAppParser())
