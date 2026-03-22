"""MBOX email archive parser."""

from __future__ import annotations

import email
import email.policy
import mailbox
import os
import tempfile

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


class MboxParser(BaseParser):
    """Parser for MBOX email archive files.

    MBOX is a standard format for storing collections of email messages.
    Used by Thunderbird, Apple Mail, Google Takeout, and many other clients.
    Each message starts with a "From " line (RFC 2822 mbox format).
    """

    def source_type(self) -> str:
        return "email_mbox"

    def source_label(self) -> str:
        return "MBOX Email Archive"

    def detect(self, content: bytes, filename: str) -> float:
        # Check extension
        is_mbox_ext = filename.lower().endswith((".mbox", ".mbx"))

        # Check content: mbox files start with "From " (the mbox marker)
        text_start = None
        for enc in ["utf-8", "latin-1"]:
            try:
                text_start = content[:200].decode(enc)
                break
            except (UnicodeDecodeError, LookupError):
                continue

        if text_start is None:
            return 0.0

        starts_with_from = text_start.lstrip("\ufeff").startswith("From ")

        if starts_with_from and is_mbox_ext:
            return 0.95
        if starts_with_from:
            return 0.85
        if is_mbox_ext:
            return 0.60

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="from", dtype="string", description="Sender email address and name"),
                FieldAnnotation(name="to", dtype="string", description="Recipient email address(es)", nullable=True),
                FieldAnnotation(name="subject", dtype="string", description="Email subject line", nullable=True),
                FieldAnnotation(name="date", dtype="datetime", description="Date the email was sent",
                                format="RFC 2822 (as provided in email header)"),
                FieldAnnotation(name="body", dtype="string", description="Plain text body of the email (text/plain part only)",
                                nullable=True),
                FieldAnnotation(name="has_attachments", dtype="boolean",
                                description="Whether the email has non-text attachments"),
            ],
            conventions=[
                "Only the text/plain body part is extracted. HTML-only emails will have a null body.",
                "has_attachments is true when the email contains parts that are not text/plain or text/html.",
                "Date is preserved as-is from the email header (RFC 2822 format).",
                "Large mbox files may contain thousands of messages — processing is sequential.",
                "Email headers may contain encoded words (RFC 2047) which are decoded automatically.",
                "Multipart emails: only the first text/plain part is used for body.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        # mailbox.mbox requires a file path, so write to a temp file
        tmp_fd, tmp_path = tempfile.mkstemp(suffix=".mbox")
        try:
            os.write(tmp_fd, content)
            os.close(tmp_fd)

            mbox = mailbox.mbox(tmp_path)
            rows = []
            warnings = []

            for i, message in enumerate(mbox):
                try:
                    parsed = self._parse_message(message)
                    rows.append(parsed)
                except Exception as e:
                    warnings.append(f"Message {i + 1}: {e}")

            mbox.close()
        finally:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass

        if not rows:
            return ParseResult(success=False, error="No email messages could be parsed")

        envelope = ContextEnvelope(
            schema=self.schema(),
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _parse_message(self, message: mailbox.mboxMessage) -> dict:
        """Parse a single email message into a dict."""
        # Decode headers
        from_addr = self._decode_header(message.get("From", ""))
        to_addr = self._decode_header(message.get("To", ""))
        subject = self._decode_header(message.get("Subject", ""))
        date = message.get("Date", "")

        # Extract plain text body and detect attachments
        body = None
        has_attachments = False

        if message.is_multipart():
            for part in message.walk():
                content_type = part.get_content_type()
                content_disposition = str(part.get("Content-Disposition", ""))

                # Skip attachments for body extraction
                if "attachment" in content_disposition.lower():
                    has_attachments = True
                    continue

                if content_type == "text/plain" and body is None:
                    payload = part.get_payload(decode=True)
                    if payload:
                        body = self._decode_payload(payload)
                elif content_type not in ("text/plain", "text/html", "multipart/mixed",
                                          "multipart/alternative", "multipart/related"):
                    has_attachments = True
        else:
            content_type = message.get_content_type()
            if content_type == "text/plain":
                payload = message.get_payload(decode=True)
                if payload:
                    body = self._decode_payload(payload)

        return {
            "from": from_addr or None,
            "to": to_addr or None,
            "subject": subject or None,
            "date": date or None,
            "body": body,
            "has_attachments": has_attachments,
        }

    def _decode_header(self, header_val: str) -> str:
        """Decode RFC 2047 encoded header values."""
        if not header_val:
            return ""
        try:
            decoded_parts = email.header.decode_header(header_val)
            parts = []
            for part, charset in decoded_parts:
                if isinstance(part, bytes):
                    parts.append(part.decode(charset or "utf-8", errors="replace"))
                else:
                    parts.append(part)
            return " ".join(parts)
        except Exception:
            return str(header_val)

    def _decode_payload(self, payload: bytes) -> str:
        """Decode email body bytes to string."""
        for enc in ["utf-8", "latin-1", "cp1252"]:
            try:
                return payload.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return payload.decode("utf-8", errors="replace")


registry.register(MboxParser())
