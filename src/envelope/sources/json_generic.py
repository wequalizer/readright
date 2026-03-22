"""Generic JSON parser with auto-detection — fallback when no specific parser matches."""

from __future__ import annotations

import json

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry


class GenericJSONParser(BaseParser):
    """Fallback parser for unknown JSON files.

    Handles JSON arrays of objects (most common data export format)
    and single JSON objects (wraps in array).
    """

    def source_type(self) -> str:
        return "json_generic"

    def source_label(self) -> str:
        return "Generic JSON File"

    def detect(self, content: bytes, filename: str) -> float:
        if filename.lower().endswith(".json"):
            try:
                text = content.decode("utf-8-sig")
                stripped = text.strip()
                if stripped.startswith(("{", "[")):
                    json.loads(stripped)
                    return 0.25
            except (UnicodeDecodeError, json.JSONDecodeError):
                return 0.0
        # Also try content-based detection for files without .json extension
        try:
            text = content.decode("utf-8-sig")
            stripped = text.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                data = json.loads(stripped)
                if isinstance(data, list) and len(data) > 0 and isinstance(data[0], dict):
                    return 0.15
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[],
            conventions=[
                "This is a generic JSON parse. Field types are inferred from values.",
                "For accurate schema annotation, register a specific parser for this source type.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        try:
            text = content.decode("utf-8-sig")
        except UnicodeDecodeError:
            return ParseResult(success=False, error="Could not decode JSON file")

        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            return ParseResult(success=False, error=f"Invalid JSON: {e}")

        # Normalize to list of dicts
        if isinstance(data, dict):
            # Check if it's a wrapper with a data key
            for key in ("data", "results", "records", "items", "rows", "entries"):
                if key in data and isinstance(data[key], list):
                    data = data[key]
                    break
            else:
                data = [data]

        if not isinstance(data, list):
            return ParseResult(success=False, error="JSON is not an array or object")

        if not data:
            return ParseResult(success=False, error="JSON array is empty")

        # Flatten to dicts — skip non-dict entries
        rows = [row for row in data if isinstance(row, dict)]
        if not rows:
            return ParseResult(success=False, error="JSON has no object entries")

        # Collect all keys across all rows
        all_keys: list[str] = []
        seen: set[str] = set()
        for row in rows:
            for k in row:
                if k not in seen:
                    all_keys.append(k)
                    seen.add(k)

        # Infer field types
        fields = []
        sample = rows[:100]
        for key in all_keys:
            values = [row.get(key) for row in sample if row.get(key) is not None]
            dtype = self._infer_type(values)
            examples = [str(v) for v in values[:3]]
            fields.append(FieldAnnotation(
                name=key,
                dtype=dtype,
                description=f"Auto-inferred {dtype} field",
                nullable=len(values) < len(sample),
                examples=examples,
            ))

        # Stringify all values for consistency
        str_rows = []
        for row in rows:
            str_row = {}
            for k in all_keys:
                v = row.get(k)
                str_row[k] = str(v) if v is not None else ""
            str_rows.append(str_row)

        schema = SchemaAnnotation(
            source_type=self.source_type(),
            source_label=f"Generic JSON: {filename}" if filename else self.source_label(),
            fields=fields,
            conventions=[
                "Field types auto-inferred from JSON values. Verify before relying on types.",
                f"Keys: {len(all_keys)}, Records: {len(rows)}",
            ],
        )

        envelope = ContextEnvelope(
            schema=schema,
            data=str_rows,
            warnings=["Generic JSON parser used — consider registering a specific parser."],
        )
        return ParseResult(success=True, envelope=envelope)

    def _infer_type(self, values: list) -> str:
        if not values:
            return "string"
        types = set()
        for v in values[:50]:
            if isinstance(v, bool):
                types.add("boolean")
            elif isinstance(v, int):
                types.add("integer")
            elif isinstance(v, float):
                types.add("decimal")
            elif isinstance(v, str):
                types.add("string")
            elif isinstance(v, (list, dict)):
                types.add("object")
            else:
                types.add("string")

        if len(types) == 1:
            return types.pop()
        if types <= {"integer", "decimal"}:
            return "decimal"
        return "string"


registry.register(GenericJSONParser())
