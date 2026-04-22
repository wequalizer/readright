# ReadRight MCP Server

MCP (Model Context Protocol) server that gives AI assistants the ability to parse files through ReadRight's 55+ parsers.

## What it does

Any MCP-compatible AI assistant (Claude Desktop, Cursor, etc.) can:

- **Parse files** into structured ContextEnvelopes with schema, conventions, and normalized data
- **Detect file types** -- identify what kind of file something is
- **List supported formats** -- see all 55+ parsers (banks, payments, chat, social media, audio, images, subtitles, PDFs)
- **Get schema info** -- inspect field definitions and conventions for any supported format

## Install

```bash
pip install readright-mcp
```

Or run directly without installing:

```bash
uvx readright-mcp
```

### From source

```bash
git clone https://github.com/waydream-ai/readright.git
cd envelope/mcp-server
pip install -e .
```

## Configure in Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "readright": {
      "command": "readright-mcp"
    }
  }
}
```

If you installed with `uvx`, use:

```json
{
  "mcpServers": {
    "readright": {
      "command": "uvx",
      "args": ["readright-mcp"]
    }
  }
}
```

### Config file location

| OS | Path |
|----|------|
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |
| Linux | `~/.config/Claude/claude_desktop_config.json` |

## Tools

### `readright_parse`

Parse a file and get structured output.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_content` | string | yes | Base64-encoded file bytes |
| `filename` | string | yes | Original filename with extension |
| `hint` | string | no | Description to help detection (e.g. "ING bank export") |

Returns a full ContextEnvelope as JSON: source info, schema with field definitions, conventions, metadata, and data rows.

### `readright_detect`

Identify file type without full parsing.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_content` | string | yes | Base64-encoded file bytes |
| `filename` | string | yes | Original filename with extension |

Returns a list of matching parsers with confidence scores.

### `readright_sources`

List all supported formats. No parameters.

### `readright_schema`

Get schema and conventions for a specific parser.

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_type` | string | yes | Parser ID (e.g. `ing_csv_nl`) |

Returns field definitions, data types, format conventions, and gotchas.

## How it works

- **Local parsing** -- no API calls, uses the [envelope](https://pypi.org/project/envelopeai/) library directly
- **Zero storage** -- files processed in memory, never written to disk
- **Base64 encoding** -- file content transported as base64 since MCP uses JSON

## Requirements

- Python 3.11+
- The `envelopeai` package (installed automatically as a dependency)

## License

MIT -- see [LICENSE](LICENSE).
