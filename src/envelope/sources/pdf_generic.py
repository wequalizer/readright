"""Generic PDF text extraction parser using pdfplumber.

Dependency: pdfplumber
    Install: pip install pdfplumber
    pdfplumber is NOT bundled with envelope — it must be installed separately.
    It is intentionally kept as an optional dependency to keep the core lightweight.
"""

from __future__ import annotations

import io

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


class GenericPDFParser(BaseParser):
    """Generic parser for PDF files — extracts text per page.

    Uses pdfplumber for text extraction, which handles most text-based PDFs well.
    Scanned/image-only PDFs will yield empty text (OCR is not included).

    Limitations:
    - Scanned PDFs or image-only PDFs produce empty text — use OCR tools instead.
    - Table detection is best-effort — complex tables may not align perfectly.
    - Encrypted/password-protected PDFs are rejected gracefully.
    - Page layout, fonts, and images are not preserved.

    pdfplumber must be installed: pip install pdfplumber
    """

    def source_type(self) -> str:
        return "pdf_generic"

    def source_label(self) -> str:
        return "Generic PDF Document"

    def detect(self, content: bytes, filename: str) -> float:
        """Detect PDF by magic bytes. Low confidence — specific PDF parsers should win."""
        try:
            import pdfplumber  # noqa: F401
        except ImportError:
            return 0.0

        # PDF magic bytes: %PDF at the start
        if content[:5] == b"%PDF-":
            return 0.30  # Generic fallback — specific parsers (invoice, report) should score higher

        # Some PDFs have leading whitespace or BOM before the magic
        header = content[:1024]
        if b"%PDF-" in header:
            return 0.25

        # Extension-only match (no magic bytes — suspicious but possible)
        if filename.lower().endswith(".pdf"):
            return 0.10

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(
                    name="page_number",
                    dtype="integer",
                    description="1-based page number within the PDF",
                ),
                FieldAnnotation(
                    name="text",
                    dtype="string",
                    description="Extracted text content of the page",
                    nullable=True,
                ),
                FieldAnnotation(
                    name="char_count",
                    dtype="integer",
                    description="Number of characters extracted from the page (0 = possibly scanned/image page)",
                ),
            ],
            conventions=[
                "Text extracted from PDF. Scanned/image PDFs may yield empty text.",
                "Table detection is best-effort.",
                "Page breaks preserved as separate rows.",
                "PDF text extraction is best-effort. For structured data, export from the source application as CSV.",
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
                return ParseResult(
                    success=False,
                    error="PDF is encrypted or password-protected. Decrypt it first before parsing.",
                )
            return ParseResult(success=False, error=f"Could not open PDF: {e}")

        warnings: list[str] = []
        rows: list[dict] = []
        empty_pages: list[int] = []

        try:
            for page in pdf.pages:
                page_num = page.page_number  # 1-based in pdfplumber

                try:
                    text = page.extract_text() or ""
                except Exception as e:
                    warnings.append(f"Page {page_num}: extraction failed ({e})")
                    text = ""

                char_count = len(text)

                if char_count == 0:
                    empty_pages.append(page_num)
                    # Still include the row so page numbering is preserved
                    rows.append({
                        "page_number": page_num,
                        "text": "",
                        "char_count": 0,
                    })
                else:
                    rows.append({
                        "page_number": page_num,
                        "text": text,
                        "char_count": char_count,
                    })
        finally:
            pdf.close()

        if not rows:
            return ParseResult(success=False, error="PDF has no pages")

        if empty_pages:
            if len(empty_pages) == len(rows):
                warnings.append(
                    "All pages are empty — this is likely a scanned/image-only PDF. "
                    "Use an OCR tool to extract text."
                )
            else:
                warnings.append(
                    f"Empty pages (possibly scanned/image): {', '.join(str(p) for p in empty_pages)}"
                )

        total_chars = sum(r["char_count"] for r in rows)

        schema = SchemaAnnotation(
            source_type=self.source_type(),
            source_label=f"PDF: {filename}" if filename else self.source_label(),
            fields=self.schema().fields,
            conventions=[
                "Text extracted from PDF. Scanned/image PDFs may yield empty text.",
                "Table detection is best-effort.",
                "Page breaks preserved as separate rows.",
                "PDF text extraction is best-effort. For structured data, export from the source application as CSV.",
                f"Pages: {len(rows)}, Total characters: {total_chars}",
            ],
        )

        envelope = ContextEnvelope(
            schema=schema,
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)


registry.register(GenericPDFParser())
