"""ReadRight MCP Server.

Exposes ReadRight's 55+ file parsers as MCP tools so any AI assistant
can parse files into structured ContextEnvelopes with schema + conventions.

Zero storage — files are processed in memory, never written to disk.
"""

from __future__ import annotations

import base64
import json
import traceback

from mcp.server.fastmcp import FastMCP

from envelope.registry import auto_register, registry

# Register all parsers on import
auto_register()

mcp = FastMCP(
    "readright",
    instructions=(
        "ReadRight parses files into structured, AI-ready output. "
        "Send a base64-encoded file to readright_parse to get back a ContextEnvelope "
        "with schema definitions, field conventions, and normalized data. "
        "Use readright_detect to identify file types, readright_sources to list "
        "all supported formats, and readright_schema to inspect a parser's schema."
    ),
)


@mcp.tool()
def readright_parse(file_content: str, filename: str, hint: str = "") -> str:
    """Parse a file and return structured, AI-ready output with schema context.

    Args:
        file_content: Base64-encoded file content.
        filename: Original filename including extension (e.g. "export.csv").
        hint: Optional description to help detection (e.g. "ING bank export").

    Returns:
        JSON string with the full ContextEnvelope: source info, schema with
        field definitions and conventions, metadata, and parsed data rows.
    """
    try:
        content = base64.b64decode(file_content)
    except Exception:
        return json.dumps({"error": "Invalid base64 encoding. Send raw file bytes as base64."})

    result = registry.parse(content, filename=filename, hint=hint)

    if not result.success or not result.envelope:
        return json.dumps({
            "error": result.error or "Parse failed with no error message.",
            "warnings": result.warnings,
        })

    envelope_dict = result.envelope.to_dict()
    if result.warnings:
        envelope_dict["meta"]["warnings"].extend(result.warnings)

    return json.dumps(envelope_dict, ensure_ascii=False, default=str)


@mcp.tool()
def readright_detect(file_content: str, filename: str) -> str:
    """Detect what kind of file this is without full parsing.

    Args:
        file_content: Base64-encoded file content.
        filename: Original filename including extension.

    Returns:
        JSON list of matching parsers with confidence scores, sorted best-first.
    """
    try:
        content = base64.b64decode(file_content)
    except Exception:
        return json.dumps({"error": "Invalid base64 encoding."})

    matches = registry.detect(content, filename=filename)

    return json.dumps([
        {
            "source_type": parser.source_type(),
            "source_label": parser.source_label(),
            "confidence": round(confidence, 3),
        }
        for parser, confidence in matches
    ])


@mcp.tool()
def readright_sources() -> str:
    """List all supported file formats and parsers.

    Returns:
        JSON list of all registered source types with their IDs and labels.
    """
    return json.dumps(registry.registered_sources)


@mcp.tool()
def readright_schema(source_type: str) -> str:
    """Get the schema and conventions for a specific source type.

    Args:
        source_type: Parser ID (e.g. "ing_csv_nl", "whatsapp_txt", "paypal_csv").
            Use readright_sources to see all available IDs.

    Returns:
        JSON with field definitions, conventions, and gotchas for this source type.
    """
    parser = registry.get_parser(source_type)
    if not parser:
        available = [p["type"] for p in registry.registered_sources]
        return json.dumps({
            "error": f"Unknown source type: {source_type!r}",
            "available": available,
        })

    schema = parser.schema()
    return json.dumps({
        "source_type": schema.source_type,
        "source_label": schema.source_label,
        "version": schema.version,
        "fields": [
            {
                "name": f.name,
                "dtype": f.dtype,
                "description": f.description,
                "format": f.format,
                "unit": f.unit,
                "enum_values": f.enum_values,
                "nullable": f.nullable,
                "examples": f.examples,
            }
            for f in schema.fields
        ],
        "conventions": schema.conventions,
        "notes": schema.notes,
    })


def main():
    """Entry point for the ReadRight MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
