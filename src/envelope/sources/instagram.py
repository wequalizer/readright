"""Instagram messages JSON export parser."""

from __future__ import annotations

import json
from datetime import datetime

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


def _parse_instagram_timestamp(ts_ms) -> str:
    """Convert millisecond epoch to ISO 8601."""
    if not ts_ms:
        return ""
    try:
        return datetime.utcfromtimestamp(int(ts_ms) / 1000).isoformat()
    except (ValueError, OSError, OverflowError):
        return str(ts_ms)


def _fix_mojibake(text: str) -> str:
    """Instagram (like Facebook) encodes UTF-8 as latin-1 in JSON exports."""
    if not text:
        return text
    try:
        return text.encode("latin-1").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        return text


def _detect_media_type(msg: dict) -> tuple[bool, str]:
    """Return (has_media, media_type) from an Instagram message."""
    for field, label in [
        ("photos", "photo"),
        ("videos", "video"),
        ("audio_files", "audio"),
        ("files", "file"),
        ("gifs", "gif"),
        ("animated_media", "animated"),
        ("share", "share"),
        ("reel_share", "reel"),
        ("story_share", "story"),
        ("clips_share", "clip"),
    ]:
        if msg.get(field):
            return True, label
    return False, ""


def _is_instagram_structure(data: dict) -> bool:
    """Check if a parsed dict looks like an Instagram messages export."""
    if "messages" not in data:
        return False
    if "participants" not in data:
        return False
    messages = data.get("messages", [])
    if not isinstance(messages, list) or not messages:
        return False
    first = messages[0] if isinstance(messages[0], dict) else {}
    return "sender_name" in first and "timestamp_ms" in first


class InstagramParser(BaseParser):
    def source_type(self) -> str:
        return "instagram_messages_json"

    def source_label(self) -> str:
        return "Instagram Messages Export (JSON)"

    def detect(self, content: bytes, filename: str) -> float:
        fname = filename.lower()
        if not fname.endswith(".json"):
            return 0.0

        # Filename hints
        score_bonus = 0.0
        if "message" in fname and "instagram" in fname:
            score_bonus = 0.15
        elif fname.startswith("message_") or "inbox" in fname:
            score_bonus = 0.05

        try:
            text = content[:8192].decode("utf-8", errors="ignore").lstrip("\ufeff")
            data = json.loads(text) if len(content) < 100_000 else json.loads(text + '"}]}')
        except Exception:
            return 0.0

        if not isinstance(data, dict):
            return 0.0

        score = 0.0
        if "messages" in data:
            score += 0.30
        if "participants" in data:
            score += 0.20

        # Instagram-specific: "thread_type" and "thread_path" keys
        if "thread_type" in data:
            score += 0.25
        if "thread_path" in data:
            score += 0.10

        messages = data.get("messages", [])
        if isinstance(messages, list) and messages:
            first = messages[0] if isinstance(messages[0], dict) else {}
            if "sender_name" in first:
                score += 0.15
            if "timestamp_ms" in first:
                score += 0.10

        return min(score + score_bonus, 0.95)

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="timestamp", dtype="datetime", description="Message send time (UTC)", format="ISO 8601"),
                FieldAnnotation(name="sender", dtype="string", description="Sender display name"),
                FieldAnnotation(name="content", dtype="string", description="Message text content"),
                FieldAnnotation(name="is_media", dtype="boolean", description="True if message contains media"),
                FieldAnnotation(name="media_type", dtype="string", description="Type of media: photo, video, audio, gif, animated, share, reel, story, clip", nullable=True),
                FieldAnnotation(name="is_unsent", dtype="boolean", description="True if the message was unsent"),
                FieldAnnotation(name="reactions", dtype="string", description="Reactions as 'emoji(actor)' list", nullable=True),
                FieldAnnotation(name="share_link", dtype="string", description="URL if the message is a shared link or post", nullable=True),
            ],
            conventions=[
                "Instagram messages export has the same JSON structure as Facebook Messenger.",
                "Text is mojibake-encoded: UTF-8 stored as latin-1. Parser corrects this automatically.",
                "Timestamps are milliseconds since Unix epoch (UTC).",
                "Instagram-specific: thread_type can be 'Regular' or 'RegularGroup'.",
                "Shared posts (reels, stories, posts) appear as 'share' type with a share link.",
                "Deleted/unsent messages appear as is_unsent=true with no content.",
                "Media files reference relative paths within the export ZIP.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        try:
            text = content.decode("utf-8-sig", errors="replace")
            data = json.loads(text)
        except json.JSONDecodeError as e:
            return ParseResult(success=False, error=f"JSON parse error: {e}")

        if not isinstance(data, dict):
            return ParseResult(success=False, error="Not an Instagram messages JSON: expected a dict at root")

        if "messages" not in data:
            return ParseResult(success=False, error="Missing 'messages' key")

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
                timestamp = _parse_instagram_timestamp(msg.get("timestamp_ms"))
                sender = _fix_mojibake(msg.get("sender_name", ""))

                raw_content = msg.get("content", "")
                content_text = _fix_mojibake(raw_content) if isinstance(raw_content, str) else ""

                is_media, media_type = _detect_media_type(msg)
                is_unsent = bool(msg.get("is_unsent"))

                # Reactions
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

                # Share link
                share = msg.get("share", {})
                if isinstance(share, dict):
                    share_link = share.get("link", "") or share.get("original_content_owner", "")
                else:
                    share_link = ""

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


registry.register(InstagramParser())
