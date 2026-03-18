"""Discord chat export parser — CSV from DiscordChatExporter."""

from __future__ import annotations

import csv
import io
from datetime import datetime

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Expected columns from DiscordChatExporter CSV format
_REQUIRED_COLS = {"AuthorID", "Author", "Date", "Content"}
_OPTIONAL_COLS = {"Attachments", "Reactions"}

# DiscordChatExporter date format: 2024-01-15 14:30:00 UTC
_DATE_FORMATS = [
    "%Y-%m-%d %H:%M:%S UTC",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%Y %H:%M:%S",
]


def _parse_discord_datetime(dt_str: str) -> str:
    if not dt_str:
        return ""
    dt_str = dt_str.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(dt_str, fmt).isoformat()
        except ValueError:
            continue
    # Try stripping timezone label at end
    trimmed = dt_str.rsplit(" ", 1)[0] if " " in dt_str else dt_str
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(trimmed, fmt).isoformat()
        except ValueError:
            continue
    return dt_str


class DiscordParser(BaseParser):
    def source_type(self) -> str:
        return "discord_csv"

    def source_label(self) -> str:
        return "Discord Chat Export (DiscordChatExporter CSV)"

    def detect(self, content: bytes, filename: str) -> float:
        fname = filename.lower()
        if not fname.endswith(".csv"):
            return 0.0

        try:
            encoding = self.detect_encoding(content)
            text = content.decode(encoding, errors="replace").lstrip("\ufeff")
        except Exception:
            return 0.0

        first_line = text.split("\n")[0].strip()
        if not first_line:
            return 0.0

        # Parse header
        try:
            reader = csv.reader(io.StringIO(first_line))
            headers = {col.strip() for col in next(reader)}
        except Exception:
            return 0.0

        if _REQUIRED_COLS.issubset(headers):
            # AuthorID is very Discord-specific
            if "AuthorID" in headers:
                return 0.95
            return 0.80

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="timestamp", dtype="datetime", description="Message timestamp", format="ISO 8601"),
                FieldAnnotation(name="author_id", dtype="string", description="Discord user ID (snowflake)"),
                FieldAnnotation(name="author", dtype="string", description="Author display name (username#discriminator or display name)"),
                FieldAnnotation(name="content", dtype="string", description="Message text content"),
                FieldAnnotation(name="attachments", dtype="string", description="Comma-separated attachment URLs", nullable=True),
                FieldAnnotation(name="reactions", dtype="string", description="Reactions in format emoji(count), e.g. '👍(3) 😂(1)'", nullable=True),
                FieldAnnotation(name="has_attachment", dtype="boolean", description="True if message has file attachments"),
            ],
            conventions=[
                "Exported by DiscordChatExporter (Tyrrrz/DiscordChatExporter on GitHub).",
                "AuthorID is a Discord snowflake (integer encoded as string) — encodes creation timestamp.",
                "Author field format changed over time: older exports show 'Name#1234', newer show display name only.",
                "Content is empty for media-only messages; check has_attachment.",
                "Reactions format: 'emoji(count)' e.g. '👍(3)'. May be empty even if reactions existed (depends on exporter version).",
                "Export does NOT include: message edits, deletions, read state, threads (unless exported separately).",
                "Timestamps are in UTC.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        encoding = self.detect_encoding(content)
        try:
            text = content.decode(encoding, errors="replace").lstrip("\ufeff")
        except Exception:
            try:
                text = content.decode("utf-8", errors="replace").lstrip("\ufeff")
            except Exception as e:
                return ParseResult(success=False, error=f"Could not decode file: {e}")

        try:
            reader = csv.DictReader(io.StringIO(text))
            rows = list(reader)
        except Exception as e:
            return ParseResult(success=False, error=f"CSV parse error: {e}")

        if not rows:
            return ParseResult(success=False, error="CSV file is empty")

        # Validate columns
        if reader.fieldnames is None:
            return ParseResult(success=False, error="CSV has no headers")

        headers = {h.strip() for h in reader.fieldnames if h}
        missing = _REQUIRED_COLS - headers
        if missing:
            return ParseResult(success=False, error=f"Missing required columns: {missing}")

        messages = []
        warnings = []

        for i, row in enumerate(rows):
            try:
                # Strip whitespace from all values
                row = {k.strip(): (v.strip() if v else "") for k, v in row.items() if k}

                author_id = row.get("AuthorID", "")
                author = row.get("Author", "")
                date_str = row.get("Date", "")
                content_text = row.get("Content", "")
                attachments = row.get("Attachments", "")
                reactions = row.get("Reactions", "")

                timestamp = _parse_discord_datetime(date_str)
                has_attachment = bool(attachments and attachments.strip())

                messages.append({
                    "timestamp": timestamp,
                    "author_id": author_id,
                    "author": author,
                    "content": content_text,
                    "attachments": attachments,
                    "reactions": reactions,
                    "has_attachment": has_attachment,
                })
            except Exception as e:
                warnings.append(f"Row {i + 2}: parse error ({e}), skipped")

        if not messages:
            return ParseResult(success=False, error="No messages could be parsed")

        envelope = ContextEnvelope(schema=self.schema(), data=messages, warnings=warnings)
        return ParseResult(success=True, envelope=envelope, warnings=warnings)


registry.register(DiscordParser())
