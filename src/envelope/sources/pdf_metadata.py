"""PDF metadata parser — extracts document properties, not content.

Extracts: author, title, creation/modification dates, producer, page count,
file size, encryption status, form fields presence.

Dependency: pdfplumber (already used by pdf_generic)
"""

from __future__ import annotations

import io
from datetime import datetime, timezone

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


def _parse_pdf_date(raw: str | None) -> str:
    """Parse PDF date format D:YYYYMMDDHHmmSS+HH'mm' to ISO 8601."""
    if not raw:
        return ""
    raw = raw.strip()
    # Strip the D: prefix
    if raw.startswith("D:"):
        raw = raw[2:]
    # Try common formats
    for fmt in ("%Y%m%d%H%M%S", "%Y%m%d%H%M", "%Y%m%d"):
        try:
            # Take only the date part (before timezone offset)
            clean = raw.split("+")[0].split("-")[0].split("Z")[0]
            # But keep negative sign for year if needed
            if len(clean) >= 8:
                dt = datetime.strptime(clean[:14].ljust(14, "0"), "%Y%m%d%H%M%S")
                return dt.replace(tzinfo=timezone.utc).isoformat()
        except (ValueError, IndexError):
            continue
    return raw  # Return as-is if unparseable


class PDFMetadataParser(BaseParser):
    """Extracts PDF document properties/metadata — not page content.

    Returns a single row with all available metadata fields.
    Useful for document cataloging, legal eDiscovery, compliance auditing,
    digital forensics, and batch PDF inventory.
    """

    def source_type(self) -> str:
        return "pdf_metadata"

    def source_label(self) -> str:
        return "PDF Document Metadata"

    def detect(self, content: bytes, filename: str) -> float:
        try:
            import pdfplumber  # noqa: F401
        except ImportError:
            return 0.0

        # Only activate when pdf_generic would — but score lower so both register
        if content[:5] == b"%PDF-":
            return 0.28
        header = content[:1024]
        if b"%PDF-" in header:
            return 0.23
        if filename.lower().endswith(".pdf"):
            return 0.08
        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="title", dtype="string", description="Document title", nullable=True),
                FieldAnnotation(name="author", dtype="string", description="Document author", nullable=True),
                FieldAnnotation(name="subject", dtype="string", description="Document subject", nullable=True),
                FieldAnnotation(name="keywords", dtype="string", description="Document keywords", nullable=True),
                FieldAnnotation(name="creator", dtype="string", description="Application that created the original document", nullable=True),
                FieldAnnotation(name="producer", dtype="string", description="Application that converted to PDF", nullable=True),
                FieldAnnotation(name="creation_date", dtype="date", description="When the document was created", format="ISO8601", nullable=True),
                FieldAnnotation(name="modification_date", dtype="date", description="When the document was last modified", format="ISO8601", nullable=True),
                FieldAnnotation(name="page_count", dtype="integer", description="Total number of pages"),
                FieldAnnotation(name="file_size_bytes", dtype="integer", description="File size in bytes"),
                FieldAnnotation(name="pdf_version", dtype="string", description="PDF specification version", nullable=True),
                FieldAnnotation(name="is_encrypted", dtype="boolean", description="Whether the PDF is encrypted"),
                FieldAnnotation(name="has_forms", dtype="boolean", description="Whether the PDF contains fillable form fields"),
            ],
            conventions=[
                "Returns a single row with document-level metadata.",
                "This parser extracts properties/metadata only — not page content. Use pdf_generic for text extraction.",
                "creation_date and modification_date are normalized to ISO 8601 UTC from PDF date format.",
                "Fields may be empty if the PDF creator did not set them.",
                "is_encrypted reflects the PDF's encryption flag, not whether content is readable.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        try:
            import pdfplumber
        except ImportError:
            return ParseResult(
                success=False,
                error="pdfplumber is not installed. Install it with: pip install pdfplumber",
            )

        try:
            pdf = pdfplumber.open(io.BytesIO(content))
        except Exception as e:
            error_msg = str(e).lower()
            if "password" in error_msg or "encrypt" in error_msg:
                # Still extract what we can
                row = {
                    "title": None, "author": None, "subject": None,
                    "keywords": None, "creator": None, "producer": None,
                    "creation_date": None, "modification_date": None,
                    "page_count": 0, "file_size_bytes": len(content),
                    "pdf_version": None, "is_encrypted": True, "has_forms": False,
                }
                envelope = ContextEnvelope(
                    schema=self.schema(),
                    data=[row],
                    warnings=["PDF is encrypted/password-protected. Only basic info available."],
                )
                return ParseResult(success=True, envelope=envelope)
            return ParseResult(success=False, error=f"Could not open PDF: {e}")

        warnings: list[str] = []

        try:
            meta = pdf.metadata or {}
            page_count = len(pdf.pages)

            # Extract PDF version from header
            pdf_version = None
            header = content[:20].decode("ascii", errors="replace")
            if "%PDF-" in header:
                idx = header.index("%PDF-") + 5
                pdf_version = header[idx:idx + 3].strip()

            # Check for form fields
            has_forms = False
            try:
                # pdfplumber wraps pdfminer — check for AcroForm
                if hasattr(pdf, "doc") and hasattr(pdf.doc, "catalog"):
                    catalog = pdf.doc.catalog
                    if catalog and "AcroForm" in catalog:
                        has_forms = True
            except Exception:
                pass

            # Check encryption
            is_encrypted = False
            try:
                if hasattr(pdf, "doc") and hasattr(pdf.doc, "is_encrypted"):
                    is_encrypted = bool(pdf.doc.is_encrypted)
            except Exception:
                pass

            row = {
                "title": meta.get("Title") or meta.get("title") or None,
                "author": meta.get("Author") or meta.get("author") or None,
                "subject": meta.get("Subject") or meta.get("subject") or None,
                "keywords": meta.get("Keywords") or meta.get("keywords") or None,
                "creator": meta.get("Creator") or meta.get("creator") or None,
                "producer": meta.get("Producer") or meta.get("producer") or None,
                "creation_date": _parse_pdf_date(meta.get("CreationDate") or meta.get("creationdate")),
                "modification_date": _parse_pdf_date(meta.get("ModDate") or meta.get("moddate")),
                "page_count": page_count,
                "file_size_bytes": len(content),
                "pdf_version": pdf_version,
                "is_encrypted": is_encrypted,
                "has_forms": has_forms,
            }
        finally:
            pdf.close()

        # Count populated fields
        populated = sum(1 for k, v in row.items() if v and v not in (0, False, ""))
        if populated <= 3:  # Only page_count, file_size, pdf_version
            warnings.append("PDF has minimal metadata. The creator did not set document properties.")

        envelope = ContextEnvelope(
            schema=self.schema(),
            data=[row],
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)


registry.register(PDFMetadataParser())
