"""The ContextEnvelope — data wrapped with meaning."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class FieldAnnotation:
    """What a single field means."""

    name: str
    dtype: str  # "date", "decimal", "string", "integer", "boolean", "enum"
    description: str
    format: str | None = None  # e.g. "DD-MM-YYYY", "comma_decimal"
    unit: str | None = None  # e.g. "EUR", "USD", "kg"
    enum_values: list[str] | None = None
    nullable: bool = False
    examples: list[str] | None = None


@dataclass
class SchemaAnnotation:
    """Full schema with meaning for every field."""

    source_type: str  # e.g. "ing_csv", "whatsapp_txt", "rabobank_mt940"
    source_label: str  # e.g. "ING Bank CSV Export (Netherlands)"
    fields: list[FieldAnnotation]
    conventions: list[str] = field(default_factory=list)  # gotchas, implicit rules
    notes: list[str] = field(default_factory=list)
    version: str = "1.0"


@dataclass
class ContextEnvelope:
    """Data + context. What the agent actually receives."""

    schema: SchemaAnnotation
    data: list[dict[str, Any]]
    row_count: int = 0
    detected_source: str = ""
    detection_confidence: float = 0.0
    parsed_at: str = ""
    warnings: list[str] = field(default_factory=list)

    def __post_init__(self):
        self.row_count = len(self.data)
        if not self.parsed_at:
            self.parsed_at = datetime.now().isoformat()

    def to_llm_context(self, max_rows: int = 50, include_sample: bool = True) -> str:
        """Format this envelope as context for an LLM prompt.

        This is the core value: data + meaning in one block.
        """
        parts = [
            f"## Data Source: {self.schema.source_label}",
            f"Source type: `{self.schema.source_type}` | {self.row_count} rows | "
            f"Detected with {self.detection_confidence:.0%} confidence",
            "",
            "### Schema",
        ]

        for f in self.schema.fields:
            line = f"- **{f.name}** ({f.dtype}): {f.description}"
            if f.format:
                line += f" — format: `{f.format}`"
            if f.unit:
                line += f" — unit: {f.unit}"
            if f.enum_values:
                line += f" — values: {', '.join(f.enum_values)}"
            if f.examples:
                line += f" — e.g. {', '.join(f.examples[:3])}"
            parts.append(line)

        if self.schema.conventions:
            parts.append("")
            parts.append("### Conventions & Gotchas")
            for c in self.schema.conventions:
                parts.append(f"- {c}")

        if self.warnings:
            parts.append("")
            parts.append("### Warnings")
            for w in self.warnings:
                parts.append(f"- ⚠ {w}")

        if include_sample and self.data:
            parts.append("")
            rows = self.data[:max_rows]
            parts.append(f"### Data ({len(rows)}/{self.row_count} rows)")
            parts.append("```json")
            parts.append(json.dumps(rows, indent=2, ensure_ascii=False, default=str))
            parts.append("```")

            if self.row_count > max_rows:
                parts.append(f"*...{self.row_count - max_rows} more rows omitted*")

        return "\n".join(parts)

    def to_dict(self) -> dict[str, Any]:
        """Serialize for API responses."""
        return {
            "source": {
                "type": self.schema.source_type,
                "label": self.schema.source_label,
                "version": self.schema.version,
            },
            "schema": {
                "fields": [
                    {
                        "name": f.name,
                        "dtype": f.dtype,
                        "description": f.description,
                        "format": f.format,
                        "unit": f.unit,
                        "enum_values": f.enum_values,
                        "nullable": f.nullable,
                    }
                    for f in self.schema.fields
                ],
                "conventions": self.schema.conventions,
            },
            "meta": {
                "row_count": self.row_count,
                "detected_source": self.detected_source,
                "detection_confidence": self.detection_confidence,
                "parsed_at": self.parsed_at,
                "warnings": self.warnings,
            },
            "data": self.data,
        }
