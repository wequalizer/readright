"""Base parser protocol."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

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
        """Detect text encoding."""
        try:
            import chardet
            result = chardet.detect(content)
            return result.get("encoding", "utf-8") or "utf-8"
        except ImportError:
            return "utf-8"
