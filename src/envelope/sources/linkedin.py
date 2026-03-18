"""LinkedIn data export parser — Connections.csv and Messages.csv."""

from __future__ import annotations

import csv
import io
from datetime import datetime

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# LinkedIn Connections.csv columns (varies slightly by export date)
_CONNECTIONS_REQUIRED = {"First Name", "Last Name", "Connected On"}
_CONNECTIONS_OPTIONAL = {"Email Address", "Company", "Position", "URL"}

# LinkedIn Messages.csv columns
_MESSAGES_REQUIRED = {"FROM", "TO", "DATE", "CONTENT"}
_MESSAGES_OPTIONAL = {"CONVERSATION ID", "CONVERSATION TITLE", "SUBJECT"}

# LinkedIn date formats
_DATE_FORMATS = [
    "%d %b %Y",          # 01 Jan 2024
    "%Y-%m-%d %H:%M:%S UTC",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%Y",
    "%Y-%m-%d",
]


def _parse_linkedin_date(dt_str: str) -> str:
    if not dt_str:
        return ""
    dt_str = dt_str.strip()
    for fmt in _DATE_FORMATS:
        try:
            return datetime.strptime(dt_str, fmt).isoformat()
        except ValueError:
            continue
    # Try stripping trailing timezone label
    for suffix in (" UTC", " GMT"):
        if dt_str.endswith(suffix):
            trimmed = dt_str[: -len(suffix)]
            for fmt in _DATE_FORMATS:
                try:
                    return datetime.strptime(trimmed, fmt).isoformat()
                except ValueError:
                    continue
    return dt_str


def _normalize_headers(headers: list[str]) -> list[str]:
    """Strip BOM, whitespace, and quotes from CSV headers."""
    cleaned = []
    for h in headers:
        h = h.strip().strip('"').strip("\ufeff").strip()
        cleaned.append(h)
    return cleaned


def _detect_linkedin_file_type(headers: set[str]) -> str:
    """Return 'connections', 'messages', or 'unknown'."""
    if _CONNECTIONS_REQUIRED.issubset(headers):
        return "connections"
    if _MESSAGES_REQUIRED.issubset(headers):
        return "messages"
    return "unknown"


class LinkedInConnectionsParser(BaseParser):
    def source_type(self) -> str:
        return "linkedin_connections_csv"

    def source_label(self) -> str:
        return "LinkedIn Connections Export (CSV)"

    def detect(self, content: bytes, filename: str) -> float:
        fname = filename.lower()
        if not fname.endswith(".csv"):
            return 0.0

        # Strong filename signal
        if fname == "connections.csv" or fname.startswith("connections"):
            score_bonus = 0.20
        else:
            score_bonus = 0.0

        try:
            encoding = self.detect_encoding(content)
            text = content.decode(encoding, errors="replace").lstrip("\ufeff")
        except Exception:
            return 0.0

        # LinkedIn Connections.csv has a note block before the CSV header:
        # "Notes:..."
        # We need to find the actual header line
        lines = text.split("\n")
        header_line = None
        for line in lines[:10]:
            stripped = line.strip()
            if "First Name" in stripped and "Last Name" in stripped:
                header_line = stripped
                break

        if header_line is None:
            return 0.0

        try:
            reader = csv.reader(io.StringIO(header_line))
            raw_headers = next(reader)
        except Exception:
            return 0.0

        headers = set(_normalize_headers(raw_headers))
        if _CONNECTIONS_REQUIRED.issubset(headers):
            return min(0.75 + score_bonus, 0.95)

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="first_name", dtype="string", description="Contact's first name"),
                FieldAnnotation(name="last_name", dtype="string", description="Contact's last name"),
                FieldAnnotation(name="email", dtype="string", description="Email address (only visible if shared)", nullable=True),
                FieldAnnotation(name="company", dtype="string", description="Current company", nullable=True),
                FieldAnnotation(name="position", dtype="string", description="Current job title/position", nullable=True),
                FieldAnnotation(name="connected_on", dtype="date", description="Date connection was established", format="ISO 8601"),
                FieldAnnotation(name="url", dtype="string", description="LinkedIn profile URL", nullable=True),
            ],
            conventions=[
                "LinkedIn Connections.csv starts with a notes block (a few lines of text) before the actual CSV header — the parser skips this.",
                "Email is only present if the connection has shared their email with you.",
                "Company and Position reflect what the connection had at the time of export, not necessarily current.",
                "Connected On date format is 'DD Mon YYYY' (e.g. '01 Jan 2024') — normalized to ISO 8601.",
                "LinkedIn does NOT export connections' phone numbers or addresses.",
                "Profile URL may be absent in older exports.",
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

        # Skip LinkedIn's note block before the CSV data
        lines = text.split("\n")
        header_idx = None
        for i, line in enumerate(lines):
            if "First Name" in line and "Last Name" in line:
                header_idx = i
                break

        if header_idx is None:
            return ParseResult(success=False, error="Could not find CSV header row — missing 'First Name' and 'Last Name' columns")

        csv_text = "\n".join(lines[header_idx:])

        try:
            reader = csv.DictReader(io.StringIO(csv_text))
            # Normalize fieldnames
            if reader.fieldnames:
                reader.fieldnames = _normalize_headers(list(reader.fieldnames))
            rows = list(reader)
        except Exception as e:
            return ParseResult(success=False, error=f"CSV parse error: {e}")

        if not rows:
            return ParseResult(success=False, error="No data rows found")

        connections = []
        warnings = []

        for i, row in enumerate(rows):
            try:
                row = {k.strip(): (v.strip() if v else "") for k, v in row.items() if k}

                connected_on = _parse_linkedin_date(
                    row.get("Connected On", "") or row.get("connected_on", "")
                )

                connections.append({
                    "first_name": row.get("First Name", ""),
                    "last_name": row.get("Last Name", ""),
                    "email": row.get("Email Address", "") or row.get("Email", ""),
                    "company": row.get("Company", ""),
                    "position": row.get("Position", ""),
                    "connected_on": connected_on,
                    "url": row.get("URL", "") or row.get("Profile URL", ""),
                })
            except Exception as e:
                warnings.append(f"Row {i + 2}: parse error ({e}), skipped")

        if not connections:
            return ParseResult(success=False, error="No connections could be parsed")

        envelope = ContextEnvelope(schema=self.schema(), data=connections, warnings=warnings)
        return ParseResult(success=True, envelope=envelope, warnings=warnings)


class LinkedInMessagesParser(BaseParser):
    def source_type(self) -> str:
        return "linkedin_messages_csv"

    def source_label(self) -> str:
        return "LinkedIn Messages Export (CSV)"

    def detect(self, content: bytes, filename: str) -> float:
        fname = filename.lower()
        if not fname.endswith(".csv"):
            return 0.0

        # Strong filename signal
        if fname == "messages.csv" or fname.startswith("messages"):
            score_bonus = 0.15
        else:
            score_bonus = 0.0

        try:
            encoding = self.detect_encoding(content)
            text = content.decode(encoding, errors="replace").lstrip("\ufeff")
        except Exception:
            return 0.0

        first_line = text.split("\n")[0].strip()
        if not first_line:
            return 0.0

        try:
            reader = csv.reader(io.StringIO(first_line))
            raw_headers = next(reader)
        except Exception:
            return 0.0

        headers = set(_normalize_headers(raw_headers))

        if _MESSAGES_REQUIRED.issubset(headers):
            # CONVERSATION ID is very LinkedIn-specific
            if "CONVERSATION ID" in headers:
                return min(0.90 + score_bonus, 0.97)
            return min(0.75 + score_bonus, 0.95)

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="conversation_id", dtype="string", description="Unique conversation identifier", nullable=True),
                FieldAnnotation(name="conversation_title", dtype="string", description="Conversation title (group name or participant names)", nullable=True),
                FieldAnnotation(name="from_name", dtype="string", description="Sender name"),
                FieldAnnotation(name="to_names", dtype="string", description="Recipient name(s), comma-separated for group messages"),
                FieldAnnotation(name="timestamp", dtype="datetime", description="Message send time", format="ISO 8601"),
                FieldAnnotation(name="subject", dtype="string", description="Message subject (InMail or email-style messages)", nullable=True),
                FieldAnnotation(name="content", dtype="string", description="Message body text"),
            ],
            conventions=[
                "LinkedIn Messages.csv includes both direct messages and InMail.",
                "CONVERSATION ID groups messages within the same thread.",
                "SUBJECT is populated for InMail and email-style messages; empty for regular DMs.",
                "TO field may contain multiple recipients separated by commas for group conversations.",
                "Timestamps are in UTC.",
                "LinkedIn does NOT export message reactions or read receipts.",
                "Media/attachment content is not exported — only text.",
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
            if reader.fieldnames:
                reader.fieldnames = _normalize_headers(list(reader.fieldnames))
            rows = list(reader)
        except Exception as e:
            return ParseResult(success=False, error=f"CSV parse error: {e}")

        if not rows:
            return ParseResult(success=False, error="No data rows found")

        # Validate headers
        if reader.fieldnames is None:
            return ParseResult(success=False, error="CSV has no headers")

        headers = set(reader.fieldnames)
        missing = _MESSAGES_REQUIRED - headers
        if missing:
            return ParseResult(success=False, error=f"Missing required columns: {missing}")

        messages = []
        warnings = []

        for i, row in enumerate(rows):
            try:
                row = {k.strip(): (v.strip() if v else "") for k, v in row.items() if k}

                timestamp = _parse_linkedin_date(row.get("DATE", ""))

                messages.append({
                    "conversation_id": row.get("CONVERSATION ID", ""),
                    "conversation_title": row.get("CONVERSATION TITLE", ""),
                    "from_name": row.get("FROM", ""),
                    "to_names": row.get("TO", ""),
                    "timestamp": timestamp,
                    "subject": row.get("SUBJECT", ""),
                    "content": row.get("CONTENT", ""),
                })
            except Exception as e:
                warnings.append(f"Row {i + 2}: parse error ({e}), skipped")

        if not messages:
            return ParseResult(success=False, error="No messages could be parsed")

        envelope = ContextEnvelope(schema=self.schema(), data=messages, warnings=warnings)
        return ParseResult(success=True, envelope=envelope, warnings=warnings)


registry.register(LinkedInConnectionsParser())
registry.register(LinkedInMessagesParser())
