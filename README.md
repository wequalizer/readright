# ReadRight

**Data AI won't misread.**

Upload a bank export, chat log, or spreadsheet. Get structured data with schema annotations, format conventions, and gotchas — ready for code or any LLM.

```
pip install envelopeai
```

```python
from envelope.registry import auto_register, registry

auto_register()
result = registry.parse_file("bank-export.csv")
print(result.envelope.to_llm_context(max_rows=5))
```

Output:

```
## Data Source: ING Bank CSV Export (Netherlands)
Source type: `ing_csv_nl` | 142 rows | Detected with 95% confidence

### Schema
- **date** (date): Transaction date — format: `YYYY-MM-DD`
- **amount** (decimal): Signed amount: negative = debit, positive = credit — unit: EUR
- **counterparty** (string): Name of the other party
- **description** (string): Full transaction description/notifications field

### Conventions & Gotchas
- Original ING amounts are ALWAYS positive — 'Af Bij' column determines direction. Normalized to signed.
- Dutch decimal separator (comma) is converted to dot.
- Date format YYYYMMDD without separators. Normalized to YYYY-MM-DD.
- ING exports may use semicolon OR comma as CSV delimiter.

### Data (5/142 rows)
[{"date": "2026-01-15", "amount": "-42.50", "counterparty": "Albert Heijn", ...}, ...]
```

Paste that into ChatGPT, Claude, or any LLM. It won't confuse `01-02-2026` for February 1st when your data means January 2nd. It won't treat `1.234,56` as twelve hundred when it's one thousand two hundred thirty-four euros and fifty-six cents.

---

## What it does

1. **Auto-detects the source** — drop in a file, ReadRight figures out what it is (ING bank export? WhatsApp chat? Stripe payout?) with confidence scoring
2. **Parses with source-specific knowledge** — each source has a dedicated parser that knows the quirks: date formats, decimal conventions, column semantics, encoding issues
3. **Annotates the schema** — every field gets a description, data type, format, unit, and examples
4. **Surfaces conventions and gotchas** — the implicit rules that trip up AI and humans alike ("amounts are always positive, direction is in a separate column")
5. **Outputs a ContextEnvelope** — structured JSON for apps, or a formatted context block for LLMs

---

## Supported sources (40)

### Banking (21)

| Source | Type ID | Region |
|--------|---------|--------|
| ING Bank | `ing_csv_nl` | NL |
| Rabobank | `rabobank_csv_nl` | NL |
| ABN AMRO | `abn_amro_csv_nl` | NL |
| Bunq | `bunq_csv` | NL/EU |
| Triodos | `triodos_csv_nl` | NL |
| Revolut | `revolut_csv` | EU |
| N26 | `n26_csv` | EU |
| Wise | `wise_csv` | Global |
| Monzo | `monzo_csv_uk` | UK |
| Barclays | `barclays_csv_uk` | UK |
| HSBC | `hsbc_csv` | UK |
| Chase | `chase_csv_us` | US |
| Bank of America | `bofa_csv_us` | US |
| Wells Fargo | `wellsfargo_csv_us` | US |
| Citibank | `citi_csv_us` | US |
| PayPal | `paypal_csv` | Global |
| Stripe | `stripe_csv` | Global |
| Venmo | `venmo_csv` | US |
| Cash App | `cashapp_csv` | US |
| OFX/QFX | `bank_ofx` | Universal |
| QIF (Quicken) | `bank_qif` | Universal |

### Chat & Social (8)

| Source | Type ID |
|--------|---------|
| WhatsApp | `whatsapp_txt` |
| Telegram | `telegram_json` |
| Signal | `signal_txt` |
| Discord | `discord_csv` |
| Facebook Messenger | `facebook_messages_json` |
| Instagram Messages | `instagram_messages_json` |
| LinkedIn Messages | `linkedin_messages_csv` |
| Twitter/X Archive | `twitter_archive_js` |

### Commerce & Finance (4)

| Source | Type ID |
|--------|---------|
| Shopify Orders | `shopify_orders_csv` |
| Square POS | `square_csv` |
| Amazon Orders | `amazon_orders_csv` |
| YNAB Budget | `ynab_csv` |

### Contacts & Personal (4)

| Source | Type ID |
|--------|---------|
| Google Contacts | `google_contacts_csv` |
| vCard (.vcf) | `vcf_contacts` |
| LinkedIn Connections | `linkedin_connections_csv` |
| Apple Health | `apple_health_xml` |

### Generic (2)

| Source | Type ID |
|--------|---------|
| Any CSV | `csv_generic` |
| Any Excel (.xlsx) | `excel_generic` |

Generic parsers auto-detect delimiter, encoding, column types, and date/decimal formats as a fallback.

---

## Python API

```python
from envelope.registry import auto_register, registry
auto_register()

# Parse from file, bytes, or stream
result = registry.parse_file("transactions.csv")
result = registry.parse(content=raw_bytes, filename="export.csv")
result = registry.parse_stream(open("chat.txt", "rb"), filename="chat.txt")

# Use the result
if result.success:
    env = result.envelope
    env.detected_source      # "ing_csv_nl"
    env.detection_confidence  # 0.95
    env.row_count             # 142
    env.data                  # [{"date": "2026-01-15", "amount": "-42.50", ...}, ...]
    env.schema.fields         # [FieldAnnotation(name="date", dtype="date", ...), ...]
    env.schema.conventions    # ["Amounts are ALWAYS positive...", ...]
    env.to_dict()             # full dict for APIs/storage
    env.to_llm_context()      # formatted context block for LLM prompts

# Detect source type without parsing
matches = registry.detect(content, filename="mystery.csv")
for parser, confidence in matches:
    print(f"{parser.source_label()}: {confidence:.0%}")

# Get schema for a known source type
schema = registry.get_parser("whatsapp_txt").schema()
```

---

## LLM integration

`to_llm_context()` is designed to paste directly into any AI chat — schema, conventions, and data sample in one block:

```python
result = registry.parse_file("my-transactions.csv")
context = result.envelope.to_llm_context(max_rows=20)

# Paste into ChatGPT/Claude manually
print(context)

# Or use in API calls
messages = [{"role": "user", "content": f"Analyze my spending:\n\n{context}"}]
```

---

## REST API

ReadRight includes a FastAPI server for web and programmatic access.

### Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/parse` | Upload file, get full ContextEnvelope JSON |
| `POST` | `/parse/llm` | Upload file, get LLM-ready context string |
| `POST` | `/detect` | Upload file, detect source type without parsing |
| `GET` | `/sources` | List all registered source types |
| `GET` | `/sources/{type}/schema` | Get annotated schema for a source type |

### Run locally

```bash
pip install envelopeai[api]
uvicorn api.main:app --port 8500

# Parse a file
curl -X POST http://localhost:8500/parse -F "file=@bank-export.csv"
```

Or via Docker: `docker build -t readright . && docker run -p 8500:8500 readright`

---

## Adding a new source parser

Create a single file in `src/envelope/sources/`. Implement 5 methods:

```python
# src/envelope/sources/bank_example.py
from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

class ExampleBankParser(BaseParser):
    def source_type(self) -> str:
        return "example_csv"                       # unique ID

    def source_label(self) -> str:
        return "Example Bank CSV Export"            # human label

    def detect(self, content: bytes, filename: str) -> float:
        # Pattern-match headers/content. Return confidence 0.0-1.0
        if "Example Bank" in content.decode("utf-8", errors="ignore"):
            return 0.95
        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="date", dtype="date",
                    description="Transaction date", format="YYYY-MM-DD"),
                FieldAnnotation(name="amount", dtype="decimal",
                    description="Transaction amount", unit="EUR"),
            ],
            conventions=["Amounts use comma as decimal separator."],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        rows = [...]  # your parsing logic
        return ParseResult(success=True,
            envelope=ContextEnvelope(schema=self.schema(), data=rows))

registry.register(ExampleBankParser())  # auto-discovered on import
```

No config files, no manual imports. Drop the file, run `pytest tests/ -v`.

---

## Privacy

- **Zero storage** — files are parsed in memory and immediately discarded
- **No logging** — file contents are never written to disk or logs
- **No accounts required** — no email, no signup for the free tier
- **Open source** — audit the code yourself
- **Self-hostable** — run it on your own infrastructure

---

## License

MIT
