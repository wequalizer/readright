"""Instagram data export parser — posts, stories, messages, profile.

Parses the JSON files from Instagram's 'Download your information' feature.
No external dependencies — uses stdlib json.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


def _ts_to_iso(ts) -> str:
    """Convert Unix timestamp to ISO 8601."""
    if not ts:
        return ""
    try:
        return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
    except (ValueError, TypeError, OSError):
        return str(ts)


class InstagramExportParser(BaseParser):
    """Parser for Instagram data export JSON files.

    Instagram exports contain:
    - Posts (photos, videos, stories, reels) with captions, timestamps, media URIs
    - Comments, likes, saved posts
    - Messages (DMs)
    - Profile information, followers, following
    - Search history, ads interactions

    The export structure varies by file:
    - posts_1.json, stories.json, reels.json (media)
    - messages/inbox/<user>/message_1.json (DMs)
    - liked_posts.json, saved_posts.json
    """

    def source_type(self) -> str:
        return "instagram_export"

    def source_label(self) -> str:
        return "Instagram Data Export"

    def detect(self, content: bytes, filename: str) -> float:
        name_lower = filename.lower()

        # Strong filename matches
        instagram_files = (
            "posts_", "stories.json", "reels.json", "reels_1.json",
            "liked_posts.json", "saved_posts.json",
            "followers_1.json", "following.json",
            "message_1.json", "message_2.json",
            "comments.json", "your_topics.json",
            "account_information.json", "personal_information.json",
        )
        for pattern in instagram_files:
            if pattern in name_lower:
                if name_lower.endswith(".json"):
                    return 0.80

        if "instagram" in name_lower and name_lower.endswith(".json"):
            return 0.85

        if not name_lower.endswith(".json"):
            return 0.0

        # Content sniff
        try:
            head = content[:4096].decode("utf-8", errors="replace")
            # Instagram export markers
            ig_markers = ('"media"', '"creation_timestamp"', '"uri"',
                          '"string_list_data"', '"participants"',
                          '"ig_', '"title"')
            matches = sum(1 for m in ig_markers if m in head)
            if matches >= 3:
                return 0.75
        except Exception:
            pass

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="type", dtype="string",
                                description="Content type",
                                examples=["post", "story", "reel", "message", "comment", "like"]),
                FieldAnnotation(name="date", dtype="date",
                                description="When the item was created/posted",
                                format="ISO8601", nullable=True),
                FieldAnnotation(name="caption", dtype="string",
                                description="Post caption, message text, or comment content",
                                nullable=True),
                FieldAnnotation(name="media_uri", dtype="string",
                                description="Path to the media file within the export",
                                nullable=True),
                FieldAnnotation(name="url", dtype="string",
                                description="External URL (for shared links, liked posts)",
                                nullable=True),
                FieldAnnotation(name="participant", dtype="string",
                                description="Other user involved (message sender, tagged user)",
                                nullable=True),
                FieldAnnotation(name="extra", dtype="string",
                                description="Additional metadata as JSON string",
                                nullable=True),
            ],
            conventions=[
                "Each row is one item (post, message, comment, like, etc.).",
                "Timestamps are converted from Unix epoch to ISO 8601 UTC.",
                "media_uri is a relative path within the export ZIP, not a live URL.",
                "Instagram encodes text in UTF-8 with Latin-1 interpretation — non-ASCII characters may appear garbled in raw exports.",
                "The 'extra' field captures section-specific data that varies by content type.",
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

        rows: list[dict] = []
        warnings: list[str] = []
        name_lower = filename.lower()

        # Determine type from filename and content
        if isinstance(data, list):
            # Some exports are arrays at top level
            for item in data:
                if isinstance(item, dict):
                    rows.extend(self._parse_item(item, name_lower))
        elif isinstance(data, dict):
            # Posts/stories/reels format
            if "ig_" in str(list(data.keys())[:5]) or any(k.startswith("ig_") for k in data.keys()):
                for key, items in data.items():
                    if isinstance(items, list):
                        for item in items:
                            rows.extend(self._parse_item(item, key.lower()))

            # Messages format
            elif "participants" in data and "messages" in data:
                participants = [p.get("name", "") for p in data.get("participants", []) if isinstance(p, dict)]
                for msg in data.get("messages", []):
                    if isinstance(msg, dict):
                        rows.append({
                            "type": "message",
                            "date": _ts_to_iso(msg.get("timestamp_ms", 0) // 1000 if msg.get("timestamp_ms") else None),
                            "caption": msg.get("content"),
                            "media_uri": None,
                            "url": None,
                            "participant": msg.get("sender_name"),
                            "extra": json.dumps({"participants": participants}, ensure_ascii=False) if participants else None,
                        })

            # Generic: try to extract from any structure
            else:
                for key, value in data.items():
                    if isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict):
                                rows.extend(self._parse_item(item, key.lower()))

        if not rows:
            return ParseResult(success=False, error="No Instagram data could be parsed from this file")

        # Count types
        type_counts = {}
        for r in rows:
            t = r.get("type", "unknown")
            type_counts[t] = type_counts.get(t, 0) + 1
        types_str = ", ".join(f"{k}: {v}" for k, v in sorted(type_counts.items()))

        schema = SchemaAnnotation(
            source_type=self.source_type(),
            source_label=f"Instagram Export: {filename}" if filename else self.source_label(),
            fields=self.schema().fields,
            conventions=list(self.schema().conventions) + [f"Content types: {types_str}"],
        )

        envelope = ContextEnvelope(
            schema=schema,
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _parse_item(self, item: dict, context: str) -> list[dict]:
        """Parse a single item from any Instagram export section."""
        rows = []

        # Determine content type from context
        content_type = "post"
        if "stor" in context:
            content_type = "story"
        elif "reel" in context:
            content_type = "reel"
        elif "comment" in context:
            content_type = "comment"
        elif "like" in context:
            content_type = "like"
        elif "saved" in context:
            content_type = "saved"
        elif "message" in context:
            content_type = "message"
        elif "follower" in context or "following" in context:
            content_type = "connection"

        # Extract timestamp
        ts = item.get("creation_timestamp") or item.get("timestamp") or item.get("taken_at")
        date = _ts_to_iso(ts)

        # Extract caption/text
        caption = None
        if "title" in item and item["title"]:
            caption = item["title"]
        elif "string_list_data" in item:
            sld = item["string_list_data"]
            if isinstance(sld, list) and sld:
                caption = sld[0].get("value") or sld[0].get("href")
                if sld[0].get("timestamp"):
                    date = date or _ts_to_iso(sld[0]["timestamp"])

        # Media items (posts can have multiple)
        media_list = item.get("media", [])
        if isinstance(media_list, list) and media_list:
            for media in media_list:
                if isinstance(media, dict):
                    media_caption = media.get("title") or caption
                    media_ts = media.get("creation_timestamp") or ts
                    rows.append({
                        "type": content_type,
                        "date": _ts_to_iso(media_ts) if media_ts else date,
                        "caption": media_caption,
                        "media_uri": media.get("uri"),
                        "url": None,
                        "participant": None,
                        "extra": None,
                    })
        else:
            # Single item (like, comment, connection, etc.)
            url = None
            if "string_list_data" in item:
                sld = item["string_list_data"]
                if isinstance(sld, list) and sld:
                    url = sld[0].get("href")

            rows.append({
                "type": content_type,
                "date": date,
                "caption": caption,
                "media_uri": item.get("uri"),
                "url": url,
                "participant": item.get("value") or item.get("name"),
                "extra": None,
            })

        return rows or [{
            "type": content_type,
            "date": date,
            "caption": caption,
            "media_uri": None,
            "url": None,
            "participant": None,
            "extra": None,
        }]


registry.register(InstagramExportParser())
