"""Base parser protocol."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field

from envelope.envelope import ContextEnvelope, SchemaAnnotation


@dataclass
class ParseResult:
    """Result of parsing a file."""

    success: bool
    envelope: ContextEnvelope | None = None
    error: str | None = None
    warnings: list[str] = field(default_factory=list)


class BaseParser(ABC):
    """Every source type implements this."""

    # Default encoding fallback chain — subclasses can override _ENCODINGS
    _ENCODINGS = ("utf-8-sig", "utf-8", "latin-1", "cp1252")

    def _decode(self, content: bytes) -> str | None:
        """Decode bytes trying multiple encodings. Returns None if all fail."""
        for enc in self._ENCODINGS:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None

    @abstractmethod
    def source_type(self) -> str:
        """Unique identifier, e.g. 'ing_csv'."""
        ...

    @abstractmethod
    def source_label(self) -> str:
        """Human label, e.g. 'ING Bank CSV Export (Netherlands)'."""
        ...

    @abstractmethod
    def detect(self, content: bytes, filename: str) -> float:
        """Return confidence 0.0-1.0 that this parser handles this data."""
        ...

    @abstractmethod
    def schema(self) -> SchemaAnnotation:
        """Return the annotated schema for this source type."""
        ...

    @abstractmethod
    def parse(self, content: bytes, filename: str) -> ParseResult:
        """Parse the content into a ContextEnvelope."""
        ...

    def detect_encoding(self, content: bytes) -> str:
        """Detect text encoding. Tries UTF-8 first (handles emoji, most modern files),
        falls back to chardet only if UTF-8 fails."""
        # UTF-8 first — covers 95%+ of modern files and handles emoji correctly.
        # chardet often misdetects UTF-8 with emoji as Windows-125x.
        try:
            content[:4096].decode("utf-8-sig")
            return "utf-8-sig"
        except UnicodeDecodeError:
            pass
        try:
            content[:4096].decode("utf-8")
            return "utf-8"
        except UnicodeDecodeError:
            pass
        # Fall back to chardet for legacy encodings
        try:
            import chardet
            result = chardet.detect(content[:4096])
            return result.get("encoding", "utf-8") or "utf-8"
        except ImportError:
            return "utf-8"
