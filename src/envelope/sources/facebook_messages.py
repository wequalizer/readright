"""Facebook Messenger export parser — JSON from Download Your Information."""

from __future__ import annotations

import json
from datetime import datetime

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


def _parse_facebook_timestamp(ts_ms) -> str:
    """Convert Facebook millisecond epoch to ISO 8601."""
    if not ts_ms:
        return ""
    try:
        return datetime.utcfromtimestamp(int(ts_ms) / 1000).isoformat()
    except (ValueError, OSError, OverflowError):
        return str(ts_ms)


def _fix_mojibake(text: str) -> str:
    """Facebook encodes UTF-8 strings as latin-1 bytes in JSON.

    e.g. "Ã©" is the mojibake for "é". Round-trip through latin-1 → utf-8.
    """
    if not text:
        return text
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def _extract_content(msg: dict) -> str:
    """Extract text content from a Facebook message dict."""
    content = msg.get("content", "")
    if isinstance(content, str):
        return _fix_mojibake(content)
    return ""


def _detect_media_type(msg: dict) -> tuple[bool, str]:
    """Return (has_media, media_type) from a Facebook message."""
    for field, label in [
        ("photos", "photo"),
        ("videos", "video"),
        ("audio_files", "audio"),
        ("files", "file"),
        ("gifs", "gif"),
        ("sticker", "sticker"),
        ("share", "share"),
    ]:
        if msg.get(field):
            return True, label
    return False, ""


class FacebookMessagesParser(BaseParser):
    def source_type(self) -> str:
        return "facebook_messages_json"

    def source_label(self) -> str:
        return "Facebook Messenger Export (JSON)"

    def detect(self, content: bytes, filename: str) -> float:
        fname = filename.lower()
        if not fname.endswith(".json"):
            return 0.0

        # Filename hints
        if "message" in fname and ("facebook" in fname or fname.startswith("message_")):
            score_bonus = 0.15
        else:
            score_bonus = 0.0

        try:
            text = content[:8192].decode("utf-8", errors="ignore").lstrip("\ufeff")
            # For large files, just check the start
            data = json.loads(text) if len(content) < 100_000 else json.loads(text + '"}]}')
        except Exception:
            try:
                text = content[:8192].decode("utf-8", errors="ignore").lstrip("\ufeff")
                # Try to parse just what we have
                data = json.loads(text.rsplit(",", 1)[0] + "]}")
            except Exception:
                return 0.0

        if not isinstance(data, dict):
            return 0.0

        score = 0.0

        # Core structural signals
        if "messages" in data:
            score += 0.35
        if "participants" in data:
            score += 0.25

        messages = data.get("messages", [])
        if isinstance(messages, list) and messages:
            first = messages[0] if isinstance(messages[0], dict) else {}
            if "sender_name" in first:
                score += 0.25
            if "timestamp_ms" in first:
                score += 0.15

        return min(score + score_bonus, 0.95)

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="timestamp", dtype="datetime", description="Message send time (UTC)", format="ISO 8601"),
                FieldAnnotation(name="sender", dtype="string", description="Sender display name"),
                FieldAnnotation(name="content", dtype="string", description="Message text content"),
                FieldAnnotation(name="is_media", dtype="boolean", description="True if message contains media attachment"),
                FieldAnnotation(name="media_type", dtype="string", description="Type of media: photo, video, audio, file, gif, sticker, share", nullable=True),
                FieldAnnotation(name="is_unsent", dtype="boolean", description="True if the message was unsent (deleted) by the sender"),
                FieldAnnotation(name="reactions", dtype="string", description="Reactions as 'emoji(actor)' list", nullable=True),
                FieldAnnotation(name="share_link", dtype="string", description="URL if the message is a shared link", nullable=True),
            ],
            conventions=[
                "Facebook encodes all text as mojibake: UTF-8 bytes stored as latin-1 codepoints. Parsers must re-encode to recover actual text.",
                "Timestamps are milliseconds since Unix epoch (UTC). Divide by 1000 for seconds.",
                "Unsent messages appear with is_unsent=true and empty content.",
                "Participants list may include people who have left the conversation.",
                "Reactions field: each reaction has an 'actor' (who reacted) and 'reaction' (emoji).",
                "Media files are stored in sub-folders relative to the export root; the JSON only contains filenames.",
                "Group conversations have a 'title' key; direct messages do not.",
                "Export filenames are typically message_1.json, message_2.json (paginated for large conversations).",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        try:
            text = content.decode("utf-8-sig", errors="replace")
            data = json.loads(text)
        except json.JSONDecodeError as e:
            return ParseResult(success=False, error=f"JSON parse error: {e}")

        if not isinstance(data, dict):
            return ParseResult(success=False, error="Not a Facebook messages JSON: expected a dict at root")

        if "messages" not in data:
            return ParseResult(success=False, error="Missing 'messages' key — not a Facebook Messenger export")

        raw_messages = data.get("messages", [])
        if not isinstance(raw_messages, list):
            return ParseResult(success=False, error="'messages' is not a list")

        messages = []
        warnings = []

        for i, msg in enumerate(raw_messages):
            if not isinstance(msg, dict):
                warnings.append(f"Message {i}: not a dict, skipped")
                continue

            try:
                timestamp = _parse_facebook_timestamp(msg.get("timestamp_ms"))
                sender = _fix_mojibake(msg.get("sender_name", ""))
                content_text = _extract_content(msg)
                is_media, media_type = _detect_media_type(msg)
                is_unsent = bool(msg.get("is_unsent"))

                # Reactions: list of {"reaction": "👍", "actor": "Name"}
                raw_reactions = msg.get("reactions", [])
                reactions_parts = []
                if isinstance(raw_reactions, list):
                    for r in raw_reactions:
                        if isinstance(r, dict):
                            emoji = _fix_mojibake(r.get("reaction", ""))
                            actor = _fix_mojibake(r.get("actor", ""))
                            if emoji:
                                reactions_parts.append(f"{emoji}({actor})" if actor else emoji)
                reactions = ", ".join(reactions_parts) if reactions_parts else ""

                # Shared links
                share = msg.get("share", {})
                share_link = share.get("link", "") if isinstance(share, dict) else ""

                messages.append({
                    "timestamp": timestamp,
                    "sender": sender,
                    "content": content_text,
                    "is_media": is_media,
                    "media_type": media_type,
                    "is_unsent": is_unsent,
                    "reactions": reactions,
                    "share_link": share_link,
                })
            except Exception as e:
                warnings.append(f"Message {i}: parse error ({e}), skipped")

        if not messages:
            return ParseResult(success=False, error="No messages could be parsed")

        envelope = ContextEnvelope(schema=self.schema(), data=messages, warnings=warnings)
        return ParseResult(success=True, envelope=envelope, warnings=warnings)


registry.register(FacebookMessagesParser())
