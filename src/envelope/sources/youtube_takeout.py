"""YouTube Takeout parser — watch history, search history, subscriptions, playlists, comments.

Extends the generic Google Takeout parser with YouTube-specific structure.
No external dependencies — uses stdlib json.
"""

from __future__ import annotations

import json
import re
from datetime import datetime

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


def _parse_timestamp(raw: str) -> str:
    """Normalize YouTube Takeout timestamp to ISO 8601."""
    if not raw:
        return ""
    raw = raw.strip()
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(raw).isoformat()
    except ValueError:
        return raw


def _extract_video_id(url: str) -> str | None:
    """Extract YouTube video ID from URL."""
    if not url:
        return None
    m = re.search(r"(?:v=|youtu\.be/|/v/|/embed/)([a-zA-Z0-9_-]{11})", url)
    return m.group(1) if m else None


def _extract_channel_id(url: str) -> str | None:
    """Extract YouTube channel ID from URL."""
    if not url:
        return None
    m = re.search(r"/channel/([a-zA-Z0-9_-]+)", url)
    return m.group(1) if m else None


class YouTubeTakeoutParser(BaseParser):
    """Parser for YouTube-specific Google Takeout JSON exports.

    Handles:
    - watch-history.json (videos watched with timestamps)
    - search-history.json (search queries)
    - subscriptions.json (channel subscriptions)
    - playlists/*.json (saved playlists)
    - my-comments/*.json (posted comments)

    Scores higher than the generic Google Takeout parser for YouTube files.
    """

    def source_type(self) -> str:
        return "youtube_takeout"

    def source_label(self) -> str:
        return "YouTube Takeout Export"

    def detect(self, content: bytes, filename: str) -> float:
        name_lower = filename.lower()

        # Strong YouTube filename signals
        youtube_files = (
            "watch-history", "watch_history",
            "search-history", "search_history",
            "subscriptions.json",
            "my-comments", "my_comments",
        )
        for pattern in youtube_files:
            if pattern in name_lower and name_lower.endswith(".json"):
                return 0.88  # Higher than google_takeout_activity (0.85)

        if "youtube" in name_lower and name_lower.endswith(".json"):
            return 0.86

        if not name_lower.endswith(".json"):
            return 0.0

        # Content sniff: YouTube watch history has titleUrl with youtube.com
        try:
            head = content[:8192].decode("utf-8", errors="replace")
            if "youtube.com" in head and '"titleUrl"' in head:
                # Looks like YouTube watch history
                if '"header"' in head and "YouTube" in head:
                    return 0.87
            # Subscriptions format
            if '"snippet"' in head and '"channelId"' in head and '"resourceId"' in head:
                return 0.88
        except Exception:
            pass

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="type", dtype="string",
                                description="Record type",
                                examples=["watch", "search", "subscription", "comment", "playlist_item"]),
                FieldAnnotation(name="date", dtype="date",
                                description="When the action occurred",
                                format="ISO8601"),
                FieldAnnotation(name="title", dtype="string",
                                description="Video title, search query, or channel name"),
                FieldAnnotation(name="url", dtype="string",
                                description="URL to the video, channel, or search",
                                nullable=True),
                FieldAnnotation(name="video_id", dtype="string",
                                description="YouTube video ID (11 characters)",
                                nullable=True),
                FieldAnnotation(name="channel", dtype="string",
                                description="Channel name (for watched videos) or channel title (for subscriptions)",
                                nullable=True),
                FieldAnnotation(name="channel_id", dtype="string",
                                description="YouTube channel ID",
                                nullable=True),
            ],
            conventions=[
                "Watch history: title starts with 'Watched ' — strip prefix for clean video title.",
                "Search history: title starts with 'Searched for ' — strip prefix for clean query.",
                "Timestamps are UTC. Convert to local time for daily/hourly pattern analysis.",
                "video_id is extracted from the URL and can be used to fetch video metadata via YouTube API.",
                "Deleted or private videos appear as 'Watched a video that has been removed'.",
                "Records are typically in reverse-chronological order.",
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

        if isinstance(data, list):
            # Watch history or search history format (Google Takeout array)
            record_type = "watch"
            if "search" in name_lower:
                record_type = "search"

            for item in data:
                if not isinstance(item, dict):
                    continue

                title = item.get("title", "")
                url = item.get("titleUrl", "")
                time = _parse_timestamp(item.get("time", ""))

                # Extract channel from subtitles
                channel = None
                channel_id = None
                subtitles = item.get("subtitles", [])
                if subtitles and isinstance(subtitles, list) and subtitles[0]:
                    channel = subtitles[0].get("name")
                    ch_url = subtitles[0].get("url", "")
                    channel_id = _extract_channel_id(ch_url)

                rows.append({
                    "type": record_type,
                    "date": time,
                    "title": title,
                    "url": url,
                    "video_id": _extract_video_id(url),
                    "channel": channel,
                    "channel_id": channel_id,
                })

        elif isinstance(data, dict):
            # Subscriptions format or other structured export
            if "subscriptions" in name_lower or ("snippet" in str(list(data.keys())[:10])):
                items = data if isinstance(data, list) else data.get("subscriptions", data.get("items", []))
                if isinstance(items, list):
                    for item in items:
                        if not isinstance(item, dict):
                            continue
                        snippet = item.get("snippet", item)
                        rows.append({
                            "type": "subscription",
                            "date": _parse_timestamp(snippet.get("publishedAt", "")),
                            "title": snippet.get("title", ""),
                            "url": None,
                            "video_id": None,
                            "channel": snippet.get("title"),
                            "channel_id": snippet.get("channelId") or (
                                snippet.get("resourceId", {}).get("channelId") if isinstance(snippet.get("resourceId"), dict) else None
                            ),
                        })
            else:
                # Try generic key-value extraction
                for key, value in data.items():
                    if isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict):
                                rows.append({
                                    "type": key.lower().replace(" ", "_"),
                                    "date": _parse_timestamp(item.get("time", "")),
                                    "title": item.get("title", ""),
                                    "url": item.get("titleUrl"),
                                    "video_id": _extract_video_id(item.get("titleUrl", "")),
                                    "channel": None,
                                    "channel_id": None,
                                })

        if not rows:
            return ParseResult(success=False, error="No YouTube data could be parsed")

        # Stats
        type_counts = {}
        for r in rows:
            t = r.get("type", "unknown")
            type_counts[t] = type_counts.get(t, 0) + 1

        conventions = list(self.schema().conventions)
        conventions.append(f"Records: {', '.join(f'{k}: {v}' for k, v in sorted(type_counts.items()))}")

        schema = SchemaAnnotation(
            source_type=self.source_type(),
            source_label=f"YouTube Takeout: {filename}" if filename else self.source_label(),
            fields=self.schema().fields,
            conventions=conventions,
        )

        envelope = ContextEnvelope(
            schema=schema,
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)


registry.register(YouTubeTakeoutParser())
