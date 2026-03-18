"""Test the built-in parsers."""

import json
from envelope.registry import auto_register, registry

auto_register()


# --- ING Bank ---

ING_CSV = b'''"Datum";"Naam / Omschrijving";"Rekening";"Tegenrekening";"Code";"Af Bij";"Bedrag (EUR)";"Mutatiesoort";"Mededelingen";"Saldo na mutatie";"Tag"
"20260315";"Albert Heijn 1234";"NL12INGB0001234567";"";"";"Af";"45,20";"Betaalautomaat";"Pasvolgnr: 001 15-03-2026 14:32 Transactie: ABC123";"1234,56";""
"20260314";"Werkgever BV";"NL12INGB0001234567";"NL98RABO0123456789";"GT";"Bij";"2500,00";"Overschrijving";"Salaris maart 2026";"1279,76";""
"20260313";"Vattenfall";"NL12INGB0001234567";"NL55ABNA0987654321";"IC";"Af";"89,50";"Incasso";"Energiekosten mrt 2026 klantnr 12345";"4779,76";""
'''

def test_ing_detection():
    matches = registry.detect(ING_CSV, "NL12INGB_transactions.csv")
    assert len(matches) > 0
    assert matches[0][0].source_type() == "ing_csv_nl"
    assert matches[0][1] >= 0.90


def test_ing_parse():
    result = registry.parse(ING_CSV, "transactions.csv")
    assert result.success
    assert result.envelope is not None
    assert result.envelope.row_count == 3
    assert result.envelope.detected_source == "ing_csv_nl"

    rows = result.envelope.data
    # First row: Albert Heijn, debit
    assert rows[0]["counterparty"] == "Albert Heijn 1234"
    assert rows[0]["amount"] == "-45.20"  # Negative because Af
    assert rows[0]["direction"] == "debit"
    assert rows[0]["date"] == "2026-03-15"

    # Second row: salary, credit
    assert rows[1]["amount"] == "2500.00"
    assert rows[1]["direction"] == "credit"


def test_ing_llm_context():
    result = registry.parse(ING_CSV, "transactions.csv")
    ctx = result.envelope.to_llm_context()
    assert "ING Bank CSV Export" in ctx
    assert "Conventions" in ctx
    assert "ALWAYS positive" in ctx
    assert "Albert Heijn" in ctx


# --- Rabobank ---

RABO_CSV = b'''"IBAN/BBAN";"Munt";"BIC";"Volgnr";"Datum";"Rentedatum";"Bedrag";"Saldo na trn";"Tegenrekening IBAN/BBAN";"Naam tegenpartij";"Naam uiteindelijke partij";"Naam initierende partij";"BIC tegenpartij";"Code";"Batch ID";"Transactiereferentie";"Machtigingskenmerk";"Incassant ID";"Betalingskenmerk";"Omschrijving-1";"Omschrijving-2";"Omschrijving-3"
"NL12RABO0123456789";"EUR";"RABONL2U";"000";"2026-03-15";"2026-03-15";"-32,50";"5432,10";"NL55ABNA0987654321";"Ziggo";"";"";"ABNANL2A";"id";"";"";"";"";"";"/TRTP/SEPA Incasso/IBAN/NL55ABNA";"Internet maart";"klantnr 99887"
"NL12RABO0123456789";"EUR";"RABONL2U";"001";"2026-03-14";"2026-03-14";"1500,00";"5464,60";"NL98INGB0001234567";"Freelance Klant BV";"";"";"INGBNL2A";"tb";"";"";"";"";"";"/TRTP/SEPA Overboeking";"Factuur 2026-003";""
'''

def test_rabo_detection():
    matches = registry.detect(RABO_CSV, "rabo_export.csv")
    assert len(matches) > 0
    assert matches[0][0].source_type() == "rabobank_csv_nl"


def test_rabo_parse():
    result = registry.parse(RABO_CSV, "rabo_export.csv")
    assert result.success
    assert result.envelope.row_count == 2

    rows = result.envelope.data
    assert rows[0]["counterparty"] == "Ziggo"
    assert rows[0]["direction"] == "debit"
    assert "-32" in rows[0]["amount"]

    assert rows[1]["direction"] == "credit"
    assert "1500" in rows[1]["amount"]


# --- WhatsApp ---

WHATSAPP_TXT = """12-03-2026, 14:32 - Jan: Hoi, hoe gaat het?
12-03-2026, 14:33 - Piet: Goed! Ben je er morgen?
12-03-2026, 14:33 - Jan: Ja, om 10 uur
12-03-2026, 14:34 - Piet: <Media weggelaten>
12-03-2026, 14:35 - Jan: Mooi, dit is een
bericht over meerdere regels
dat gewoon door moet lopen
12-03-2026, 14:36 - Piet: Top!
""".encode("utf-8")


def test_whatsapp_detection():
    matches = registry.detect(WHATSAPP_TXT, "WhatsApp Chat - Jan.txt")
    assert len(matches) > 0
    assert matches[0][0].source_type() == "whatsapp_txt"


def test_whatsapp_parse():
    result = registry.parse(WHATSAPP_TXT, "WhatsApp Chat.txt")
    assert result.success
    assert result.envelope is not None

    rows = result.envelope.data
    assert rows[0]["sender"] == "Jan"
    assert rows[0]["text"] == "Hoi, hoe gaat het?"

    # Media message
    media_msgs = [r for r in rows if r["is_media"]]
    assert len(media_msgs) == 1

    # Multi-line message
    multiline = [r for r in rows if "meerdere regels" in r["text"]]
    assert len(multiline) == 1
    assert "door moet lopen" in multiline[0]["text"]


def test_whatsapp_llm_context():
    result = registry.parse(WHATSAPP_TXT, "chat.txt")
    ctx = result.envelope.to_llm_context()
    assert "WhatsApp" in ctx
    assert "locale-dependent" in ctx


# --- Generic CSV ---

GENERIC_CSV = b"""name,age,city,signup_date
Alice,30,Amsterdam,2026-01-15
Bob,25,Rotterdam,2026-02-20
Charlie,35,Utrecht,2026-03-10
"""


def test_generic_csv_fallback():
    result = registry.parse(GENERIC_CSV, "users.csv")
    assert result.success
    assert result.envelope.row_count == 3
    # Should use generic parser since no specific parser matches
    assert "generic" in result.envelope.detected_source.lower() or result.envelope.detected_source == "csv_generic"


# --- Full envelope serialization ---

def test_envelope_to_dict():
    result = registry.parse(ING_CSV, "transactions.csv")
    d = result.envelope.to_dict()
    assert "schema" in d
    assert "data" in d
    assert "meta" in d
    assert d["source"]["type"] == "ing_csv_nl"
    # Should be valid JSON
    json.dumps(d)


if __name__ == "__main__":
    import sys
    # Quick smoke test
    for name, fn in list(globals().items()):
        if name.startswith("test_") and callable(fn):
            try:
                fn()
                print(f"  PASS: {name}")
            except Exception as e:
                print(f"  FAIL: {name} — {e}")
                sys.exit(1)
    print("\nAll tests passed.")
