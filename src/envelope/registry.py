"""Source type registry — the core of Envelope."""

from __future__ import annotations

import mimetypes
from pathlib import Path
from typing import BinaryIO

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.merchant_patterns import match_merchant
from envelope.parser import BaseParser, ParseResult

# Mapping of keywords (in filenames or hints) to source_type IDs.
# Used to boost confidence when the user or filename gives a clue.
_KEYWORD_MAP: dict[str, list[str]] = {
    "ing": ["ing_csv_nl"],
    "rabobank": ["rabobank_csv_nl"], "rabo": ["rabobank_csv_nl"],
    "abn": ["abn_amro_csv_nl"], "abn amro": ["abn_amro_csv_nl"], "abnamro": ["abn_amro_csv_nl"],
    "triodos": ["triodos_csv_nl"],
    "bunq": ["bunq_csv"],
    "chase": ["chase_csv_us"], "jpmorgan": ["chase_csv_us"],
    "revolut": ["revolut_csv"],
    "n26": ["n26_csv"],
    "wise": ["wise_csv"], "transferwise": ["wise_csv"],
    "monzo": ["monzo_csv_uk"],
    "barclays": ["barclays_csv_uk"],
    "hsbc": ["hsbc_csv"],
    "bofa": ["bofa_csv_us"], "bank of america": ["bofa_csv_us"],
    "wells fargo": ["wellsfargo_csv_us"], "wellsfargo": ["wellsfargo_csv_us"],
    "citi": ["citi_csv_us"], "citibank": ["citi_csv_us"],
    "paypal": ["paypal_csv"],
    "stripe": ["stripe_csv"],
    "square": ["square_csv"],
    "venmo": ["venmo_csv"],
    "cashapp": ["cashapp_csv"], "cash app": ["cashapp_csv"],
    "whatsapp": ["whatsapp_txt"],
    "telegram": ["telegram_json"],
    "signal": ["signal_txt"],
    "discord": ["discord_csv"],
    "facebook": ["facebook_messages_json"],
    "instagram": ["instagram_messages_json"],
    "twitter": ["twitter_archive_js"], "x.com": ["twitter_archive_js"],
    "linkedin": ["linkedin_connections_csv", "linkedin_messages_csv"],
    "spotify": ["spotify_streaming"],
    "netflix": ["netflix_viewing"],
    "amazon": ["amazon_orders_csv"],
    "shopify": ["shopify_orders_csv"],
    "ynab": ["ynab_csv"],
    "apple health": ["apple_health_xml"],
    "google takeout": ["google_takeout_activity_json"],
    "chrome": ["chrome_history"],
    "exact": ["exact_online"], "exact online": ["exact_online"],
    # Media parsers
    "tiktok": ["tiktok_export"],
    "youtube": ["youtube_takeout"], "youtube history": ["youtube_takeout"],
    "instagram": ["instagram_export", "instagram_messages_json"],
    "subtitle": ["subtitle"], "srt": ["subtitle"], "vtt": ["subtitle"],
    "exif": ["image_metadata"], "photo": ["image_metadata"], "jpeg": ["image_metadata"],
    "mp3": ["audio_metadata"], "flac": ["audio_metadata"], "audio": ["audio_metadata"],
    "pdf metadata": ["pdf_metadata"], "pdf properties": ["pdf_metadata"],
}


# Fields to check for merchant matching (order = priority)
_MERCHANT_FIELDS = ("counterparty", "name", "description", "Mededelingen", "Naam / Omschrijving")

# Source types that contain financial transactions
_BANK_PREFIXES = ("bank_", "ing_", "rabobank_", "abn_", "triodos_", "bunq_", "chase_", "citi_",
                   "hsbc_", "monzo_", "n26_", "revolut_", "wise_", "wellsfargo_", "bofa_",
                   "barclays_", "paypal_", "stripe_", "cashapp_", "venmo_", "square_",
                   "exact_", "ynab_")


def _enrich_merchants(envelope) -> None:
    """Add merchant + category columns to bank transaction rows."""
    src = envelope.detected_source or envelope.schema.source_type
    if not any(src.startswith(p) for p in _BANK_PREFIXES):
        return

    # Find which field to match on
    if not envelope.data:
        return
    sample = envelope.data[0]
    match_field = None
    for f in _MERCHANT_FIELDS:
        if f in sample:
            match_field = f
            break
    if not match_field:
        return

    enriched = 0
    cache: dict[str, object] = {}
    for row in envelope.data:
        desc = row.get(match_field, "")
        if desc not in cache:
            cache[desc] = match_merchant(desc)
        result = cache[desc]
        if result:
            row["merchant"] = result.merchant
            row["category"] = result.category
            enriched += 1
        else:
            row["merchant"] = ""
            row["category"] = ""

    if enriched > 0:
        envelope.warnings = envelope.warnings or []
        envelope.warnings.append(
            f"Merchant enrichment: {enriched}/{len(envelope.data)} transactions matched to known merchants."
        )


class SourceRegistry:
    """Registry of source type parsers. Auto-detects and parses any registered format."""

    def __init__(self):
        self._parsers: list[BaseParser] = []

    def register(self, parser: BaseParser) -> None:
        """Register a new source type parser."""
        # Prevent duplicate registrations
        for existing in self._parsers:
            if existing.source_type() == parser.source_type():
                return
        self._parsers.append(parser)

    def _keyword_boost(self, filename: str, hint: str) -> set[str]:
        """Find source_type IDs that match keywords in filename or hint."""
        text = f"{filename} {hint}".lower()
        boosted: set[str] = set()
        for keyword, source_types in _KEYWORD_MAP.items():
            if keyword in text:
                boosted.update(source_types)
        return boosted

    def detect(self, content: bytes, filename: str = "", hint: str = "") -> list[tuple[BaseParser, float]]:
        """Detect which parser(s) can handle this content. Returns [(parser, confidence)].

        If hint or filename contains a known keyword, matching parsers get a
        confidence boost (min 0.50) so they're tried even when headers differ.
        """
        boosted_types = self._keyword_boost(filename, hint)
        matches = []
        for parser in self._parsers:
            try:
                confidence = parser.detect(content, filename)
                # Boost: if keyword matches this parser but detect() returned 0,
                # give it a floor confidence so it still gets tried
                if parser.source_type() in boosted_types:
                    confidence = max(confidence, 0.50)
                if confidence > 0.0:
                    matches.append((parser, confidence))
                    if confidence >= 0.95 and not boosted_types:
                        break
            except Exception:
                continue
        return sorted(matches, key=lambda x: x[1], reverse=True)

    def parse(self, content: bytes, filename: str = "", hint: str = "") -> ParseResult:
        """Auto-detect source type and parse. Returns the best result.

        Args:
            content: Raw file bytes.
            filename: Original filename (used for extension + keyword matching).
            hint: Optional user description of the file (e.g. "ING bank export").
        """
        matches = self.detect(content, filename, hint)

        if not matches:
            # No parser recognized this file — return best-effort fallback
            return self._fallback_parse(content, filename)

        # Try parsers in confidence order
        errors = []
        for parser, confidence in matches:
            try:
                result = parser.parse(content, filename)
            except Exception as exc:
                import logging
                logging.getLogger(__name__).error(
                    "Parser %s crashed: %s", parser.source_type(), exc
                )
                errors.append(f"{parser.source_type()}: crash: {exc}")
                continue
            if result.success and result.envelope:
                result.envelope.detected_source = parser.source_type()
                result.envelope.detection_confidence = confidence
                if hint:
                    result.envelope.warnings = result.envelope.warnings or []
                    result.envelope.warnings.append(f"User hint: {hint}")
                _enrich_merchants(result.envelope)
                # Merge parser-level warnings into the envelope
                if result.warnings:
                    result.envelope.warnings = (result.envelope.warnings or []) + result.warnings
                return result
            if result.error:
                errors.append(f"{parser.source_type()}: {result.error}")

        # All specific parsers failed — return best-effort fallback
        return self._fallback_parse(content, filename)

    def _fallback_parse(self, content: bytes, filename: str) -> ParseResult:
        """Best-effort parse for unrecognized file types. Never fails."""
        ext = Path(filename).suffix.lower() if filename else ""
        mime, _ = mimetypes.guess_type(filename or "file.bin")
        mime = mime or "application/octet-stream"
        size_kb = len(content) / 1024

        fields = [
            FieldAnnotation(name="filename", dtype="string", description="Original filename"),
            FieldAnnotation(name="mime_type", dtype="string", description="Detected MIME type"),
            FieldAnnotation(name="size_bytes", dtype="integer", description="File size in bytes"),
        ]
        data_row = {"filename": filename, "mime_type": mime, "size_bytes": len(content)}

        # Try to extract something useful from text-based files
        rows = []
        is_text = False
        try:
            text = content.decode("utf-8-sig")
            is_text = True
        except (UnicodeDecodeError, ValueError):
            try:
                text = content.decode("latin-1")
                is_text = True
            except Exception:
                pass

        if is_text:
            import csv
            import io
            lines = text.strip().splitlines()
            # Try CSV parse
            try:
                dialect = csv.Sniffer().sniff(lines[0] if lines else "", delimiters=",;\t|")
                reader = csv.DictReader(io.StringIO(text), dialect=dialect)
                csv_rows = []
                for i, row in enumerate(reader):
                    if i >= 500:
                        break
                    csv_rows.append(dict(row))
                if csv_rows and len(csv_rows[0]) > 1:
                    rows = csv_rows
                    fields = [
                        FieldAnnotation(name=col, dtype="string", description=f"Column: {col}")
                        for col in csv_rows[0].keys()
                    ]
            except Exception:
                pass

            # If CSV didn't work, return raw lines
            if not rows and lines:
                rows = [{"line_number": i + 1, "content": line} for i, line in enumerate(lines[:500])]
                fields = [
                    FieldAnnotation(name="line_number", dtype="integer", description="Line number"),
                    FieldAnnotation(name="content", dtype="string", description="Line content"),
                ]
        else:
            rows = [data_row]

        disclaimer = (
            "No source-specific parser matched this file. "
            "This is a best-effort parse — results may not be as precise as our 56 supported formats. "
            "Let us know at hello@readright.ai if you'd like us to add support for this file type."
        )

        envelope = ContextEnvelope(
            schema=SchemaAnnotation(
                source_type="generic_fallback",
                source_label=f"Unrecognized file ({ext or mime})",
                fields=fields,
                conventions=[],
                notes=[disclaimer],
            ),
            data=rows,
            warnings=[disclaimer],
        )
        envelope.detected_source = "generic_fallback"
        envelope.detection_confidence = 0.0

        return ParseResult(
            success=True,
            envelope=envelope,
            warnings=[disclaimer],
        )

    def parse_file(self, path: str | Path) -> ParseResult:
        """Parse a file from disk."""
        path = Path(path)
        if not path.exists():
            return ParseResult(success=False, error=f"File not found: {path}")
        content = path.read_bytes()
        return self.parse(content, filename=path.name)

    def parse_stream(self, stream: BinaryIO, filename: str = "") -> ParseResult:
        """Parse from a file-like object."""
        content = stream.read()
        return self.parse(content, filename=filename)

    @property
    def registered_sources(self) -> list[dict[str, str]]:
        """List all registered source types."""
        return [
            {"type": p.source_type(), "label": p.source_label()}
            for p in self._parsers
        ]

    def get_parser(self, source_type: str) -> BaseParser | None:
        """Get a specific parser by source type ID."""
        for p in self._parsers:
            if p.source_type() == source_type:
                return p
        return None


# Global registry — parsers register themselves on import
registry = SourceRegistry()


def auto_register() -> None:
    """Import all built-in parsers to trigger registration."""
    import importlib
    import pkgutil

    from envelope import sources

    # Auto-discover all parser modules — no manual list needed
    for _importer, modname, _ispkg in pkgutil.iter_modules(sources.__path__):
        if modname.startswith("_"):
            continue
        try:
            importlib.import_module(f"envelope.sources.{modname}")
        except ImportError:
            pass  # Skip parsers with missing optional deps
        except Exception as e:
            import warnings as _warnings
            _warnings.warn(f"Failed to load parser {modname}: {e}")
