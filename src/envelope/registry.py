"""Source type registry — the core of Envelope."""

from __future__ import annotations

from pathlib import Path
from typing import BinaryIO

from envelope.envelope import ContextEnvelope
from envelope.parser import BaseParser, ParseResult


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

    def detect(self, content: bytes, filename: str = "") -> list[tuple[BaseParser, float]]:
        """Detect which parser(s) can handle this content. Returns [(parser, confidence)]."""
        matches = []
        for parser in self._parsers:
            try:
                confidence = parser.detect(content, filename)
                if confidence > 0.0:
                    matches.append((parser, confidence))
            except Exception:
                continue
        return sorted(matches, key=lambda x: x[1], reverse=True)

    def parse(self, content: bytes, filename: str = "") -> ParseResult:
        """Auto-detect source type and parse. Returns the best result."""
        matches = self.detect(content, filename)

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
