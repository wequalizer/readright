"""Spotify extended streaming history JSON parser."""

from __future__ import annotations

import json

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


class SpotifyStreamingParser(BaseParser):
    """Parser for Spotify extended streaming history JSON exports.

    Spotify allows users to request their data via Privacy Settings.
    The extended streaming history comes as JSON files named like
    Streaming_History_Audio_*.json, each containing an array of play objects.
    """

    def source_type(self) -> str:
        return "spotify_streaming"

    def source_label(self) -> str:
        return "Spotify Extended Streaming History"

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

        # Must be a list of objects
        if not isinstance(data, list) or not data:
            return 0.0

        first = data[0]
        if not isinstance(first, dict):
            return 0.0

        # High confidence: Spotify-specific keys
        if "master_metadata_track_name" in first or "spotify_track_uri" in first:
            return 0.95

        # Medium confidence: looks like Spotify basic history
        if "ms_played" in first and "ts" in first:
            return 0.80

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="ts", dtype="datetime", description="Timestamp when the stream ended", format="ISO 8601 UTC"),
                FieldAnnotation(name="ms_played", dtype="integer", description="Milliseconds the track was played"),
                FieldAnnotation(name="track_name", dtype="string", description="Track title", nullable=True),
                FieldAnnotation(name="artist_name", dtype="string", description="Artist name", nullable=True),
                FieldAnnotation(name="album_name", dtype="string", description="Album title", nullable=True),
                FieldAnnotation(name="spotify_track_uri", dtype="string", description="Spotify URI for the track (spotify:track:...)", nullable=True),
                FieldAnnotation(name="reason_start", dtype="string", description="Why playback started", nullable=True,
                                examples=["trackdone", "fwdbtn", "clickrow", "appload"]),
                FieldAnnotation(name="reason_end", dtype="string", description="Why playback ended", nullable=True,
                                examples=["trackdone", "fwdbtn", "endplay", "logout"]),
                FieldAnnotation(name="shuffle", dtype="boolean", description="Whether shuffle was enabled", nullable=True),
                FieldAnnotation(name="skipped", dtype="boolean", description="Whether the track was skipped", nullable=True),
                FieldAnnotation(name="offline", dtype="boolean", description="Whether played offline", nullable=True),
                FieldAnnotation(name="platform", dtype="string", description="Platform/device used for playback", nullable=True),
            ],
            conventions=[
                "ms_played = 0 usually means the track was skipped immediately or an ad played.",
                "track_name, artist_name, album_name can be null for podcasts or non-music content.",
                "spotify_track_uri is null for podcasts — check spotify_episode_uri instead.",
                "Timestamps are in UTC. The ts field marks when the stream ended, not started.",
                "Extended history files can be very large (100K+ entries). Basic history has fewer fields.",
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

        if not isinstance(data, list):
            return ParseResult(success=False, error="Expected a JSON array of streaming entries")

        rows = []
        warnings = []

        for i, entry in enumerate(data):
            if not isinstance(entry, dict):
                warnings.append(f"Entry {i}: not a dict, skipped")
                continue

            rows.append({
                "ts": entry.get("ts"),
                "ms_played": entry.get("ms_played"),
                "track_name": entry.get("master_metadata_track_name"),
                "artist_name": entry.get("master_metadata_album_artist_name"),
                "album_name": entry.get("master_metadata_album_album_name"),
                "spotify_track_uri": entry.get("spotify_track_uri"),
                "reason_start": entry.get("reason_start"),
                "reason_end": entry.get("reason_end"),
                "shuffle": entry.get("shuffle"),
                "skipped": entry.get("skipped"),
                "offline": entry.get("offline"),
                "platform": entry.get("platform"),
            })

        if not rows:
            return ParseResult(success=False, error="No streaming entries found")

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


registry.register(SpotifyStreamingParser())
