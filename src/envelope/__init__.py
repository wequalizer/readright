"""Envelope — wrap data with context for AI."""

from envelope.registry import SourceRegistry, registry
from envelope.envelope import ContextEnvelope
from envelope.parser import BaseParser, ParseResult

__all__ = [
    "SourceRegistry",
    "registry",
    "ContextEnvelope",
    "BaseParser",
    "ParseResult",
]
