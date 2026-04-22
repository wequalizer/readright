"""Subtitle parser — SRT, VTT, ASS/SSA formats.

Extracts timestamped cues with speaker detection and statistics.
Uses pysubs2 for multi-format support.

Dependency: pysubs2
    Install: pip install pysubs2
"""

from __future__ import annotations

import re

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


# Speaker patterns: "Speaker: text", "<v Speaker>text", "[Speaker] text"
_SPEAKER_PATTERNS = [
    re.compile(r"^<v\s+([^>]+)>(.*)$", re.DOTALL),      # VTT voice tag
    re.compile(r"^([A-Z][A-Za-z ]{1,30}):\s+(.+)$", re.DOTALL),  # "Name: text"
    re.compile(r"^\[([A-Za-z ]{1,30})\]\s*(.+)$", re.DOTALL),    # "[Name] text"
]


def _extract_speaker(text: str) -> tuple[str | None, str]:
    """Extract speaker name from cue text. Returns (speaker, clean_text)."""
    for pat in _SPEAKER_PATTERNS:
        m = pat.match(text.strip())
        if m:
            return m.group(1).strip(), m.group(2).strip()
    return None, text.strip()


def _ms_to_timestamp(ms: int) -> str:
    """Convert milliseconds to HH:MM:SS.mmm format."""
    if ms < 0:
        ms = 0
    h = ms // 3_600_000
    m = (ms % 3_600_000) // 60_000
    s = (ms % 60_000) // 1_000
    remainder = ms % 1_000
    return f"{h:02d}:{m:02d}:{s:02d}.{remainder:03d}"


class SubtitleParser(BaseParser):
    """Parser for subtitle files: SRT, VTT (WebVTT), ASS, SSA.

    Extracts each cue as a row with timing, text, and detected speaker.
    Adds summary statistics: total cues, duration, word count, speaker breakdown.
    """

    def source_type(self) -> str:
        return "subtitle"

    def source_label(self) -> str:
        return "Subtitle File (SRT/VTT/ASS/SSA)"

    def detect(self, content: bytes, filename: str) -> float:
        name_lower = filename.lower()

        # Extension match
        if name_lower.endswith(".srt"):
            return 0.90
        if name_lower.endswith(".vtt"):
            return 0.90
        if name_lower.endswith((".ass", ".ssa")):
            return 0.85

        # Content sniff for SRT: starts with "1\n00:..." or "1\r\n00:..."
        try:
            head = content[:512].decode("utf-8", errors="replace")
            # SRT pattern
            if re.match(r"\s*\d+\s*[\r\n]+\d{2}:\d{2}:\d{2}", head):
                return 0.85
            # VTT pattern
            if head.strip().startswith("WEBVTT"):
                return 0.90
            # ASS pattern
            if "[Script Info]" in head:
                return 0.85
        except Exception:
            pass

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="index", dtype="integer", description="Cue sequence number (1-based)"),
                FieldAnnotation(name="start", dtype="string", description="Cue start time", format="HH:MM:SS.mmm"),
                FieldAnnotation(name="end", dtype="string", description="Cue end time", format="HH:MM:SS.mmm"),
                FieldAnnotation(name="start_ms", dtype="integer", description="Cue start time in milliseconds"),
                FieldAnnotation(name="end_ms", dtype="integer", description="Cue end time in milliseconds"),
                FieldAnnotation(name="duration_ms", dtype="integer", description="Cue duration in milliseconds"),
                FieldAnnotation(name="speaker", dtype="string", description="Detected speaker name", nullable=True),
                FieldAnnotation(name="text", dtype="string", description="Cue text content (formatting tags stripped)"),
                FieldAnnotation(name="word_count", dtype="integer", description="Number of words in cue text"),
            ],
            conventions=[
                "Speaker detection is best-effort: looks for 'Name: text', '<v Name>text', '[Name] text' patterns.",
                "HTML/ASS formatting tags are stripped from text.",
                "Timestamps are in HH:MM:SS.mmm format; start_ms/end_ms are absolute milliseconds for programmatic use.",
                "Empty cues (no text) are included but flagged.",
                "For ASS/SSA files, style information is not preserved — only timing and text.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        try:
            import pysubs2
        except ImportError:
            return ParseResult(
                success=False,
                error="pysubs2 is not installed. Install it with: pip install pysubs2",
            )

        # Detect encoding
        text = self._decode(content)
        if text is None:
            text = content.decode("utf-8", errors="replace")

        # Detect format from extension or content
        name_lower = filename.lower()
        fmt = None
        if name_lower.endswith(".srt"):
            fmt = "srt"
        elif name_lower.endswith(".vtt"):
            fmt = "vtt"
        elif name_lower.endswith(".ass"):
            fmt = "ass"
        elif name_lower.endswith(".ssa"):
            fmt = "ssa"

        try:
            if fmt:
                subs = pysubs2.SSAFile.from_string(text, format_=fmt)
            else:
                subs = pysubs2.SSAFile.from_string(text)
        except Exception as e:
            return ParseResult(success=False, error=f"Failed to parse subtitle file: {e}")

        if not subs:
            return ParseResult(success=False, error="No subtitle cues found in file")

        rows: list[dict] = []
        warnings: list[str] = []
        speakers: dict[str, int] = {}
        total_words = 0
        empty_cues = 0

        for i, event in enumerate(subs):
            if event.is_comment:
                continue

            # Strip formatting tags
            clean_text = event.plaintext if hasattr(event, "plaintext") else event.text
            # Remove ASS override tags like {\b1}
            clean_text = re.sub(r"\{\\[^}]*\}", "", clean_text)
            # Remove HTML tags
            clean_text = re.sub(r"<[^>]+>", "", clean_text)
            clean_text = clean_text.strip()

            speaker, text_body = _extract_speaker(clean_text)
            words = len(text_body.split()) if text_body else 0
            total_words += words

            if not text_body:
                empty_cues += 1

            if speaker:
                speakers[speaker] = speakers.get(speaker, 0) + 1

            rows.append({
                "index": i + 1,
                "start": _ms_to_timestamp(event.start),
                "end": _ms_to_timestamp(event.end),
                "start_ms": event.start,
                "end_ms": event.end,
                "duration_ms": event.end - event.start,
                "speaker": speaker,
                "text": text_body,
                "word_count": words,
            })

        if not rows:
            return ParseResult(success=False, error="No subtitle cues could be parsed")

        # Statistics
        total_duration_ms = max(r["end_ms"] for r in rows) if rows else 0
        conventions = list(self.schema().conventions)
        conventions.append(f"Total cues: {len(rows)}, Total words: {total_words}")
        conventions.append(f"Total duration: {_ms_to_timestamp(total_duration_ms)}")
        if speakers:
            speaker_str = ", ".join(f"{k} ({v} cues)" for k, v in sorted(speakers.items(), key=lambda x: -x[1]))
            conventions.append(f"Speakers detected: {speaker_str}")

        if empty_cues:
            warnings.append(f"{empty_cues} empty cues found (no text content)")

        # Detect format for label
        detected_fmt = fmt or "unknown"
        schema = SchemaAnnotation(
            source_type=self.source_type(),
            source_label=f"Subtitle File ({detected_fmt.upper()}): {filename}" if filename else self.source_label(),
            fields=self.schema().fields,
            conventions=conventions,
        )

        envelope = ContextEnvelope(
            schema=schema,
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)


registry.register(SubtitleParser())
