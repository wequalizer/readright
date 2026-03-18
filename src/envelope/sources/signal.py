"""Signal chat export parser — plain text format."""

from __future__ import annotations

import re
from datetime import datetime

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Signal plain text export formats:
#
# Bracketed (similar to WhatsApp):
#   [2024-01-15 14:30:00] Sender Name: Message text
#   [2024-01-15, 14:30] Sender: Message
#
# Non-bracketed:
#   2024-01-15 14:30:00 Sender Name: Message text
#   Jan 15, 2024, 2:30 PM - Sender: Message
#
# Note-to-self / system messages may lack a sender.

_PATTERNS = [
    # Bracketed ISO: [YYYY-MM-DD HH:MM:SS] Sender: Text
    re.compile(r"^\[(\d{4}-\d{2}-\d{2})[,\s]+(\d{1,2}:\d{2}(?::\d{2})?)\]\s+(.+?):\s(.+)$"),
    # Bracketed ISO no sender: [YYYY-MM-DD HH:MM:SS] Text
    re.compile(r"^\[(\d{4}-\d{2}-\d{2})[,\s]+(\d{1,2}:\d{2}(?::\d{2})?)\]\s+(.+)$"),
    # Non-bracketed ISO with dash separator: YYYY-MM-DD HH:MM:SS - Sender: Text
    re.compile(r"^(\d{4}-\d{2}-\d{2})\s+(\d{1,2}:\d{2}(?::\d{2})?)\s*[-–]\s*(.+?):\s(.+)$"),
    # Non-bracketed ISO plain: YYYY-MM-DD HH:MM:SS Sender: Text
    re.compile(r"^(\d{4}-\d{2}-\d{2})\s+(\d{1,2}:\d{2}(?::\d{2})?)\s+(.+?):\s(.+)$"),
    # US format with AM/PM: Jan 15, 2024, 2:30 PM - Sender: Text
    re.compile(r"^([A-Z][a-z]{2}\s+\d{1,2},\s+\d{4}),\s+(\d{1,2}:\d{2}\s*[APap][Mm])\s*[-–]\s*(.+?):\s(.+)$"),
]

_DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d %H:%M",
    "%b %d, %Y %I:%M %p",
    "%b %d, %Y %I:%M%p",
]

_SYSTEM_KEYWORDS = [
    "missed call", "group update", "group name changed", "left the group",
    "joined the group", "you changed", "safety number", "verified",
    "this message was deleted", "message request", "note to self",
    "voice message", "attachment",
]

_MEDIA_INDICATORS = [
    "attachment", "image", "photo", "video", "audio", "voice message",
    "sticker", "gif", "file", "document",
    "<media>", "<image>", "<video>", "<audio>",
]


def _parse_signal_datetime(date_str: str, time_str: str) -> str:
    combined = f"{date_str.strip()} {time_str.strip()}"
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(combined, fmt).isoformat()
        except ValueError:
            continue
    return combined


class SignalParser(BaseParser):
    def source_type(self) -> str:
        return "signal_txt"

    def source_label(self) -> str:
        return "Signal Chat Export (Plain Text)"

    def detect(self, content: bytes, filename: str) -> float:
        fname = filename.lower()

        # Filename hints
        if "signal" in fname:
            if fname.endswith(".txt"):
                score_base = 0.70
            else:
                score_base = 0.50
        elif fname.endswith(".txt"):
            score_base = 0.0
        else:
            return 0.0

        try:
            encoding = self.detect_encoding(content)
            text = content.decode(encoding, errors="replace").lstrip("\ufeff")
        except Exception:
            return 0.0

        lines = [l.strip() for l in text.split("\n")[:30] if l.strip()]
        matches = 0
        for line in lines:
            for pattern in _PATTERNS:
                if pattern.match(line):
                    matches += 1
                    break

        if matches >= 5:
            return max(score_base, 0.90)
        if matches >= 3:
            return max(score_base, 0.75)
        if matches >= 1:
            return max(score_base, 0.55)

        return score_base

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="timestamp", dtype="datetime", description="Message timestamp", format="ISO 8601"),
                FieldAnnotation(name="sender", dtype="string", description="Sender display name"),
                FieldAnnotation(name="text", dtype="string", description="Message text content"),
                FieldAnnotation(name="is_media", dtype="boolean", description="True if message references a media attachment"),
                FieldAnnotation(name="media_type", dtype="string", description="Type of media if detectable", nullable=True),
                FieldAnnotation(name="is_system", dtype="boolean", description="True for system events (calls, group changes, safety number updates)"),
            ],
            conventions=[
                "Signal does not have an official desktop export — exports are third-party or from Signal Desktop backup.",
                "Two common formats: bracketed timestamps [YYYY-MM-DD HH:MM:SS] and non-bracketed.",
                "Multi-line messages: continuation lines (no timestamp) are appended to the previous message.",
                "Media files are not embedded — the text may say 'Attachment' or '<media>' as a placeholder.",
                "System events (missed calls, group updates, safety number changes) have is_system=true.",
                "Note-to-self conversations will show your own name as both sender and recipient.",
                "Timestamps are in local time of the device that generated the export.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        encoding = self.detect_encoding(content)
        try:
            text = content.decode(encoding, errors="replace")
        except Exception:
            for enc in ["utf-8-sig", "utf-8", "latin-1"]:
                try:
                    text = content.decode(enc)
                    break
                except (UnicodeDecodeError, LookupError):
                    continue
            else:
                return ParseResult(success=False, error="Could not decode file encoding")

        text = text.lstrip("\ufeff")

        messages = []
        current_msg = None
        warnings = []

        for line_num, raw_line in enumerate(text.split("\n"), 1):
            line = raw_line.strip()
            if not line:
                continue

            parsed = self._try_parse_line(line)

            if parsed:
                if current_msg:
                    messages.append(current_msg)
                current_msg = parsed
            elif current_msg:
                # Continuation — append to current message
                current_msg["text"] += "\n" + line
            else:
                if line_num <= 5:
                    warnings.append(f"Line {line_num}: Could not parse, skipped: {line[:80]!r}")

        if current_msg:
            messages.append(current_msg)

        if not messages:
            return ParseResult(success=False, error="No messages could be parsed")

        envelope = ContextEnvelope(schema=self.schema(), data=messages, warnings=warnings)
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _try_parse_line(self, line: str) -> dict | None:
        for pattern in _PATTERNS:
            match = pattern.match(line)
            if not match:
                continue

            groups = match.groups()

            if len(groups) == 4:
                date_str, time_str, sender, text = groups
            elif len(groups) == 3:
                date_str, time_str, text = groups
                sender = "system"
            else:
                continue

            timestamp = _parse_signal_datetime(date_str, time_str)

            text = text.strip()
            sender = sender.strip()

            text_lower = text.lower()
            is_system = sender == "system" or any(kw in text_lower for kw in _SYSTEM_KEYWORDS)
            is_media = any(ind in text_lower for ind in _MEDIA_INDICATORS)

            media_type = ""
            if is_media:
                for mt in ["image", "photo", "video", "audio", "voice", "sticker", "gif", "document", "file"]:
                    if mt in text_lower:
                        media_type = mt
                        break
                if not media_type:
                    media_type = "attachment"

            return {
                "timestamp": timestamp,
                "sender": sender,
                "text": text,
                "is_media": is_media,
                "media_type": media_type,
                "is_system": is_system,
            }

        return None


registry.register(SignalParser())
