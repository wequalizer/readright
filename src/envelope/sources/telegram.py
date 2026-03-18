"""Telegram chat export parser — JSON and HTML variants."""

from __future__ import annotations

import json
import re
from datetime import datetime

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# HTML tag stripper
_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    return _TAG_RE.sub("", text).strip()


def _parse_telegram_datetime(dt_str: str) -> str:
    """Normalize Telegram ISO datetime to ISO 8601."""
    if not dt_str:
        return ""
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(dt_str[:19], fmt[:len(fmt)]).isoformat()
        except ValueError:
            continue
    return dt_str


def _extract_text(text_field) -> str:
    """Telegram text can be a string or a list of segments (bold, link, etc.)."""
    if isinstance(text_field, str):
        return text_field
    if isinstance(text_field, list):
        parts = []
        for seg in text_field:
            if isinstance(seg, str):
                parts.append(seg)
            elif isinstance(seg, dict):
                parts.append(seg.get("text", ""))
        return "".join(parts)
    return ""


class TelegramParser(BaseParser):
    def source_type(self) -> str:
        return "telegram_json"

    def source_label(self) -> str:
        return "Telegram Chat Export (JSON)"

    def detect(self, content: bytes, filename: str) -> float:
        fname = filename.lower()

        # HTML export variant
        if fname.endswith(".html") and "telegram" in fname:
            return 0.75
        if fname.endswith(".html"):
            try:
                text = content[:2000].decode("utf-8", errors="ignore")
                if "tgme_widget_message" in text or "telegram" in text.lower():
                    return 0.60
            except Exception:
                pass
            return 0.0

        if not fname.endswith(".json"):
            return 0.0

        try:
            text = content[:4096].decode("utf-8", errors="ignore").lstrip("\ufeff")
            data = json.loads(text) if len(content) < 50_000 else json.loads(content[:50_000].decode("utf-8", errors="ignore").lstrip("\ufeff"))
        except Exception:
            return 0.0

        # Telegram JSON export always has a top-level "messages" list
        if not isinstance(data, dict):
            return 0.0
        if "messages" not in data:
            return 0.0

        messages = data["messages"]
        if not isinstance(messages, list) or not messages:
            return 0.0

        first = messages[0] if messages else {}
        if not isinstance(first, dict):
            return 0.0

        # Key structural signals
        score = 0.0
        if "type" in first:
            score += 0.3
        if "date" in first:
            score += 0.3
        if "from" in first or "actor" in first:
            score += 0.2
        if "text" in first:
            score += 0.2
        if "id" in first:
            score += 0.1

        # Telegram-specific fields
        if "name" in data or "type" in data:
            score = min(score + 0.1, 1.0)

        return min(score, 0.95)

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="id", dtype="integer", description="Telegram message ID", nullable=True),
                FieldAnnotation(name="timestamp", dtype="datetime", description="Message timestamp", format="ISO 8601"),
                FieldAnnotation(name="sender", dtype="string", description="Sender display name"),
                FieldAnnotation(name="sender_id", dtype="string", description="Sender user ID", nullable=True),
                FieldAnnotation(name="text", dtype="string", description="Message text content"),
                FieldAnnotation(name="is_media", dtype="boolean", description="True if message has media attachment"),
                FieldAnnotation(name="media_type", dtype="string", description="Type of media: photo, video, audio, document, sticker, poll, etc.", nullable=True),
                FieldAnnotation(name="is_system", dtype="boolean", description="True for service/system messages"),
                FieldAnnotation(name="reply_to_id", dtype="integer", description="ID of the replied-to message", nullable=True),
                FieldAnnotation(name="forwarded_from", dtype="string", description="Original sender if forwarded", nullable=True),
            ],
            conventions=[
                "Telegram JSON export includes all message types: message, service (system events), etc.",
                "The 'text' field can be a plain string or a list of segments (bold, italic, links) — segments are joined.",
                "Service messages (pinned message, group name changed, etc.) have is_system=true.",
                "Media-only messages may have an empty text field.",
                "Timestamps are in local time of the export — no timezone info is preserved in the file.",
                "Forwarded messages include a 'forwarded_from' field with the original sender.",
                "The export file is typically named 'result.json' or 'messages.json'.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        fname = filename.lower()
        if fname.endswith(".html"):
            return self._parse_html(content, filename)
        return self._parse_json(content, filename)

    def _parse_json(self, content: bytes, filename: str) -> ParseResult:
        try:
            text = content.decode("utf-8-sig", errors="replace")
            data = json.loads(text)
        except json.JSONDecodeError as e:
            return ParseResult(success=False, error=f"JSON parse error: {e}")

        if not isinstance(data, dict) or "messages" not in data:
            return ParseResult(success=False, error="Not a Telegram JSON export: missing 'messages' key")

        raw_messages = data["messages"]
        if not isinstance(raw_messages, list):
            return ParseResult(success=False, error="'messages' is not a list")

        messages = []
        warnings = []

        for i, msg in enumerate(raw_messages):
            if not isinstance(msg, dict):
                warnings.append(f"Message {i}: not a dict, skipped")
                continue

            msg_type = msg.get("type", "message")
            is_system = msg_type in ("service", "system")

            sender = msg.get("from") or msg.get("actor") or ""
            sender_id = str(msg.get("from_id") or msg.get("actor_id") or "")

            text = _extract_text(msg.get("text", ""))

            # Detect media
            media_type = ""
            is_media = False
            for field in ("photo", "file", "video_file", "audio_file", "voice_message",
                          "video_message", "sticker", "animation", "poll"):
                if field in msg:
                    is_media = True
                    media_type = field.replace("_file", "").replace("_message", "")
                    break
            if not is_media and msg.get("media_type"):
                is_media = True
                media_type = msg["media_type"]

            timestamp = _parse_telegram_datetime(msg.get("date", ""))

            reply_to = msg.get("reply_to_message_id")
            forwarded_from = msg.get("forwarded_from") or ""

            messages.append({
                "id": msg.get("id"),
                "timestamp": timestamp,
                "sender": str(sender).strip(),
                "sender_id": sender_id,
                "text": text,
                "is_media": is_media,
                "media_type": media_type,
                "is_system": is_system,
                "reply_to_id": reply_to,
                "forwarded_from": str(forwarded_from).strip() if forwarded_from else "",
            })

        if not messages:
            return ParseResult(success=False, error="No messages could be parsed")

        envelope = ContextEnvelope(schema=self.schema(), data=messages, warnings=warnings)
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _parse_html(self, content: bytes, filename: str) -> ParseResult:
        """Parse Telegram HTML export (basic extraction)."""
        try:
            text = content.decode("utf-8", errors="replace")
        except Exception as e:
            return ParseResult(success=False, error=f"Could not decode HTML: {e}")

        # Telegram HTML structure: divs with class="message" containing date, from, text
        msg_blocks = re.findall(
            r'class="message[^"]*".*?(?=class="message|</body>)',
            text,
            re.DOTALL,
        )

        messages = []
        warnings = []

        for i, block in enumerate(msg_blocks):
            # Extract date
            date_match = re.search(r'title="([^"]+)"', block)
            timestamp = date_match.group(1) if date_match else ""

            # Extract sender
            from_match = re.search(r'class="from_name"[^>]*>(.*?)</span>', block, re.DOTALL)
            sender = _strip_html(from_match.group(1)) if from_match else ""

            # Extract text
            text_match = re.search(r'class="text"[^>]*>(.*?)</div>', block, re.DOTALL)
            body = _strip_html(text_match.group(1)) if text_match else ""

            if not timestamp and not sender and not body:
                continue

            messages.append({
                "id": None,
                "timestamp": timestamp,
                "sender": sender,
                "sender_id": "",
                "text": body,
                "is_media": bool(re.search(r'class="media"', block)),
                "media_type": "",
                "is_system": "service" in block.lower(),
                "reply_to_id": None,
                "forwarded_from": "",
            })

        if not messages:
            return ParseResult(success=False, error="No messages found in Telegram HTML export")

        envelope = ContextEnvelope(schema=self.schema(), data=messages, warnings=warnings)
        return ParseResult(success=True, envelope=envelope, warnings=warnings)


registry.register(TelegramParser())
