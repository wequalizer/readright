"""Audio file metadata parser — MP3, FLAC, OGG, M4A, WAV, AIFF, WMA, Opus.

Extracts ID3/Vorbis/MP4 tags and technical metadata.
Uses tinytag for lightweight, pure-Python, zero-dependency extraction.

Dependency: tinytag
    Install: pip install tinytag
"""

from __future__ import annotations

import io

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


# Audio file signatures (magic bytes)
_MAGIC = {
    b"ID3": "mp3",          # MP3 with ID3v2
    b"\xff\xfb": "mp3",     # MP3 frame sync
    b"\xff\xf3": "mp3",     # MP3 frame sync (MPEG2)
    b"\xff\xf2": "mp3",     # MP3 frame sync
    b"fLaC": "flac",        # FLAC
    b"OggS": "ogg",         # OGG/Opus/Vorbis
    b"RIFF": "wav",         # WAV
    b"FORM": "aiff",        # AIFF
}

_AUDIO_EXTENSIONS = {
    ".mp3", ".flac", ".ogg", ".opus", ".m4a", ".mp4", ".aac",
    ".wav", ".aiff", ".aif", ".wma", ".ape", ".mpc", ".wv",
}


class AudioMetadataParser(BaseParser):
    """Extracts metadata tags and technical info from audio files.

    Returns a single row per file with all available metadata.
    Useful for music library cataloging, DJ collection management,
    podcast organization, sample library inventory, and audio forensics.
    """

    def source_type(self) -> str:
        return "audio_metadata"

    def source_label(self) -> str:
        return "Audio File Metadata"

    def detect(self, content: bytes, filename: str) -> float:
        try:
            import tinytag  # noqa: F401
        except ImportError:
            return 0.0

        name_lower = filename.lower()

        # Extension match
        for ext in _AUDIO_EXTENSIONS:
            if name_lower.endswith(ext):
                return 0.90

        # Magic bytes
        head = content[:4]
        for magic in _MAGIC:
            if head[:len(magic)] == magic:
                return 0.85

        # MP4/M4A container: check for ftyp box
        if len(content) > 8 and content[4:8] == b"ftyp":
            ftyp = content[8:12].decode("ascii", errors="replace").lower()
            if ftyp in ("m4a ", "mp41", "mp42", "isom", "aac "):
                return 0.85

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="title", dtype="string", description="Track title", nullable=True),
                FieldAnnotation(name="artist", dtype="string", description="Artist/performer name", nullable=True),
                FieldAnnotation(name="album", dtype="string", description="Album name", nullable=True),
                FieldAnnotation(name="album_artist", dtype="string", description="Album artist (for compilations)", nullable=True),
                FieldAnnotation(name="composer", dtype="string", description="Composer name", nullable=True),
                FieldAnnotation(name="genre", dtype="string", description="Genre tag", nullable=True),
                FieldAnnotation(name="year", dtype="string", description="Release year", nullable=True),
                FieldAnnotation(name="track", dtype="integer", description="Track number", nullable=True),
                FieldAnnotation(name="track_total", dtype="integer", description="Total tracks on album", nullable=True),
                FieldAnnotation(name="disc", dtype="integer", description="Disc number", nullable=True),
                FieldAnnotation(name="disc_total", dtype="integer", description="Total discs", nullable=True),
                FieldAnnotation(name="duration_seconds", dtype="decimal", description="Duration in seconds"),
                FieldAnnotation(name="duration_display", dtype="string", description="Duration as MM:SS or H:MM:SS"),
                FieldAnnotation(name="bitrate", dtype="decimal", description="Bitrate in kbps", unit="kbps"),
                FieldAnnotation(name="samplerate", dtype="integer", description="Sample rate in Hz", unit="Hz"),
                FieldAnnotation(name="channels", dtype="integer", description="Number of audio channels"),
                FieldAnnotation(name="bitdepth", dtype="integer", description="Bit depth (16, 24, 32)", nullable=True),
                FieldAnnotation(name="filesize_bytes", dtype="integer", description="File size in bytes"),
                FieldAnnotation(name="codec", dtype="string", description="Audio codec/format", nullable=True),
                FieldAnnotation(name="comment", dtype="string", description="Comment/notes tag", nullable=True),
                FieldAnnotation(name="has_artwork", dtype="boolean", description="Whether embedded cover art is present"),
            ],
            conventions=[
                "Returns a single row per audio file with all available metadata.",
                "Fields are null when not set in the file's tags.",
                "duration_display is formatted as M:SS for tracks under 1 hour, H:MM:SS otherwise.",
                "bitrate is in kilobits per second (kbps). For VBR files this is the average.",
                "codec is inferred from file format, not from the bitstream.",
                "BPM and musical key are extracted when present in tags (common in DJ software exports).",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        try:
            from tinytag import TinyTag
        except ImportError:
            return ParseResult(
                success=False,
                error="tinytag is not installed. Install it with: pip install tinytag",
            )

        try:
            tag = TinyTag.get(file_obj=io.BytesIO(content), image=True, filename=filename)
        except Exception as e:
            return ParseResult(success=False, error=f"Failed to read audio metadata: {e}")

        warnings: list[str] = []

        # Format duration
        dur = tag.duration or 0
        if dur >= 3600:
            dur_display = f"{int(dur // 3600)}:{int((dur % 3600) // 60):02d}:{int(dur % 60):02d}"
        else:
            dur_display = f"{int(dur // 60)}:{int(dur % 60):02d}"

        # Detect codec from extension/content
        codec = None
        name_lower = filename.lower()
        if name_lower.endswith(".mp3"):
            codec = "MP3"
        elif name_lower.endswith(".flac"):
            codec = "FLAC"
        elif name_lower.endswith((".ogg", ".oga")):
            codec = "OGG Vorbis"
        elif name_lower.endswith(".opus"):
            codec = "Opus"
        elif name_lower.endswith((".m4a", ".aac")):
            codec = "AAC"
        elif name_lower.endswith(".wav"):
            codec = "WAV"
        elif name_lower.endswith((".aiff", ".aif")):
            codec = "AIFF"
        elif name_lower.endswith(".wma"):
            codec = "WMA"

        has_artwork = tag._images is not None and len(tag._images) > 0 if hasattr(tag, "_images") else False
        # Fallback: check via the image attribute
        if not has_artwork:
            try:
                has_artwork = tag.get_image() is not None
            except Exception:
                pass

        # Safe int conversion
        def safe_int(val):
            if val is None:
                return None
            try:
                return int(val)
            except (ValueError, TypeError):
                return None

        row = {
            "title": tag.title or None,
            "artist": tag.artist or None,
            "album": tag.album or None,
            "album_artist": tag.albumartist or None,
            "composer": tag.composer or None,
            "genre": tag.genre or None,
            "year": str(tag.year) if tag.year else None,
            "track": safe_int(tag.track),
            "track_total": safe_int(tag.track_total),
            "disc": safe_int(tag.disc),
            "disc_total": safe_int(tag.disc_total),
            "duration_seconds": round(dur, 2),
            "duration_display": dur_display,
            "bitrate": round(tag.bitrate, 1) if tag.bitrate else None,
            "samplerate": tag.samplerate,
            "channels": tag.channels,
            "bitdepth": tag.bitdepth,
            "filesize_bytes": len(content),
            "codec": codec,
            "comment": tag.comment or None,
            "has_artwork": has_artwork,
        }

        # Extra tags if available (BPM, key — common in DJ files)
        extra = {}
        for attr in ("bpm", "initial_key", "isrc", "publisher", "copyright"):
            val = getattr(tag, attr, None)
            if val:
                extra[attr] = str(val)
        if extra:
            row["extra_tags"] = extra
            # Add extra fields to schema dynamically via conventions
            conventions = list(self.schema().conventions)
            conventions.append(f"Extra tags found: {', '.join(extra.keys())}")
        else:
            conventions = list(self.schema().conventions)

        # Warn if no tags at all
        tag_fields = ["title", "artist", "album", "genre", "year"]
        if not any(row.get(f) for f in tag_fields):
            warnings.append("No metadata tags found. File may be untagged.")

        schema = SchemaAnnotation(
            source_type=self.source_type(),
            source_label=f"Audio: {filename}" if filename else self.source_label(),
            fields=self.schema().fields,
            conventions=conventions,
        )

        envelope = ContextEnvelope(
            schema=schema,
            data=[row],
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)


registry.register(AudioMetadataParser())
