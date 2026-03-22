"""Source type registry — the core of Envelope."""

from __future__ import annotations

import re
from pathlib import Path
from typing import BinaryIO

from envelope.envelope import ContextEnvelope
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
    "spotify": ["spotify_streaming_json"],
    "netflix": ["netflix_viewing_csv"],
    "amazon": ["amazon_orders_csv"],
    "shopify": ["shopify_orders_csv"],
    "ynab": ["ynab_csv"],
    "apple health": ["apple_health_xml"],
    "google takeout": ["google_takeout_activity_json"],
    "chrome": ["chrome_history_json"],
    "exact": ["exact_online_csv"], "exact online": ["exact_online_csv"],
}


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
            return ParseResult(
                success=False,
                error=f"No parser recognized this data. Filename: {filename!r}. "
                f"Registered parsers: {[p.source_type() for p in self._parsers]}",
            )

        # Try parsers in confidence order
        errors = []
        for parser, confidence in matches:
            result = parser.parse(content, filename)
            if result.success and result.envelope:
                result.envelope.detected_source = parser.source_type()
                result.envelope.detection_confidence = confidence
                if hint:
                    result.envelope.warnings = result.envelope.warnings or []
                    result.envelope.warnings.append(f"User hint: {hint}")
                return result
            if result.error:
                errors.append(f"{parser.source_type()}: {result.error}")

        return ParseResult(
            success=False,
            error=f"All parsers failed. Errors: {'; '.join(errors)}",
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
        except Exception:
            pass  # Skip parsers with missing optional deps (e.g., openpyxl)
