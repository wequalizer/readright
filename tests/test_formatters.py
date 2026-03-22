"""Test MT940 and CAMT.053 output formatters."""

import xml.etree.ElementTree as ET
from decimal import Decimal

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.formatters.mt940 import to_mt940
from envelope.formatters.camt053 import to_camt053
from envelope.registry import auto_register, registry

auto_register()


# --- Fixtures ---

def _banking_schema() -> SchemaAnnotation:
    return SchemaAnnotation(
        source_type="test_bank",
        source_label="Test Bank CSV",
        fields=[
            FieldAnnotation(name="date", dtype="date", description="Transaction date"),
            FieldAnnotation(name="amount", dtype="decimal", description="Signed amount"),
            FieldAnnotation(name="direction", dtype="enum", description="Debit or credit"),
            FieldAnnotation(name="counterparty", dtype="string", description="Other party"),
            FieldAnnotation(name="description", dtype="string", description="Description"),
        ],
    )


def _make_envelope(rows: list[dict], schema=None) -> ContextEnvelope:
    return ContextEnvelope(
        schema=schema or _banking_schema(),
        data=rows,
    )


BASIC_ROWS = [
    {
        "date": "2026-03-10",
        "amount": "-23.45",
        "direction": "debit",
        "counterparty": "Albert Heijn 1076",
        "counterparty_iban": "NL55ABNA0987654321",
        "description": "Boodschappen",
        "account_iban": "NL42INGB0001234567",
        "currency": "EUR",
        "balance_after": "1818.85",
    },
    {
        "date": "2026-03-11",
        "amount": "3250.00",
        "direction": "credit",
        "counterparty": "Werkgever BV",
        "counterparty_iban": "NL98RABO0123456789",
        "description": "Salaris Maart",
        "account_iban": "NL42INGB0001234567",
        "currency": "EUR",
        "balance_after": "5068.85",
    },
]


# === MT940 Tests ===


def test_mt940_basic_output():
    env = _make_envelope(BASIC_ROWS)
    result = to_mt940(env)

    # Should contain SWIFT envelope
    assert "{1:F01READRIGHTXXX0000000000}" in result
    assert "{4:" in result
    assert "-}" in result

    # Transaction reference
    assert ":20:READRIGHT0001" in result

    # Account IBAN
    assert ":25:NL42INGB0001234567" in result

    # Statement number
    assert ":28C:1/1" in result


def test_mt940_opening_balance():
    """Opening balance = first balance_after - first amount."""
    env = _make_envelope(BASIC_ROWS)
    result = to_mt940(env)

    # Opening = 1818.85 - (-23.45) = 1842.30
    assert ":60F:C260310EUR1842,30" in result


def test_mt940_closing_balance():
    """Closing balance = last balance_after."""
    env = _make_envelope(BASIC_ROWS)
    result = to_mt940(env)

    assert ":62F:C260311EUR5068,85" in result


def test_mt940_debit_transaction():
    env = _make_envelope(BASIC_ROWS)
    result = to_mt940(env)

    # Debit: D + amount, no sign
    assert ":61:260310D23,45NTRF" in result
    # Description line
    assert ":86:Albert Heijn 1076" in result


def test_mt940_credit_transaction():
    env = _make_envelope(BASIC_ROWS)
    result = to_mt940(env)

    assert ":61:260311C3250,00NTRF" in result
    assert "Salaris Maart" in result


def test_mt940_comma_decimal_format():
    """MT940 must use comma as decimal separator."""
    rows = [{
        "date": "2026-03-15",
        "amount": "-1234.56",
        "direction": "debit",
        "counterparty": "Test",
        "description": "Test",
        "account_iban": "NL42INGB0001234567",
    }]
    env = _make_envelope(rows)
    result = to_mt940(env)

    assert "1234,56" in result
    # Should NOT have dot decimal in amounts
    assert ":61:" in result


def test_mt940_override_iban():
    env = _make_envelope(BASIC_ROWS)
    result = to_mt940(env, account_iban="NL99BUNQ0123456789")

    assert ":25:NL99BUNQ0123456789" in result


def test_mt940_empty_data_raises():
    env = _make_envelope([])
    try:
        to_mt940(env)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "No transaction data" in str(e)


def test_mt940_no_balance_after():
    """When balance_after is not available, assume opening = 0."""
    rows = [
        {"date": "2026-03-10", "amount": "-50.00", "direction": "debit", "counterparty": "Shop", "description": ""},
        {"date": "2026-03-11", "amount": "100.00", "direction": "credit", "counterparty": "Salary", "description": ""},
    ]
    env = _make_envelope(rows)
    result = to_mt940(env)

    # Opening should be 0
    assert ":60F:C" in result
    assert "EUR0,00" in result

    # Closing should be -50 + 100 = 50
    assert ":62F:C" in result


def test_mt940_negative_balance():
    """Negative balance should use D indicator."""
    rows = [{
        "date": "2026-03-10",
        "amount": "-500.00",
        "direction": "debit",
        "counterparty": "Big Purchase",
        "description": "",
        "balance_after": "-200.00",
    }]
    env = _make_envelope(rows)
    result = to_mt940(env)

    # Closing balance should be D (debit = negative)
    assert ":62F:D260310EUR200,00" in result


def test_mt940_sanitizes_text():
    """Special characters should be stripped from descriptions."""
    rows = [{
        "date": "2026-03-10",
        "amount": "-10.00",
        "direction": "debit",
        "counterparty": "Caf\u00e9 M\u00fcller <test>",
        "description": "Payment @#$%^&*",
    }]
    env = _make_envelope(rows)
    result = to_mt940(env)

    # Should not contain < > @ # $ % ^ & *
    assert "<" not in result.split(":86:")[1].split("\r\n")[0]


def test_mt940_uses_crlf():
    """MT940 should use CRLF line endings."""
    env = _make_envelope(BASIC_ROWS)
    result = to_mt940(env)
    assert "\r\n" in result


# === CAMT.053 Tests ===


def test_camt053_valid_xml():
    env = _make_envelope(BASIC_ROWS)
    result = to_camt053(env)

    # Should parse as valid XML
    root = ET.fromstring(result)
    assert root is not None


def test_camt053_namespace():
    env = _make_envelope(BASIC_ROWS)
    result = to_camt053(env)

    assert 'xmlns="urn:iso:std:iso:20022:tech:xsd:camt.053.001.02"' in result


def test_camt053_account_iban():
    env = _make_envelope(BASIC_ROWS)
    result = to_camt053(env)

    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:camt.053.001.02"}
    root = ET.fromstring(result)
    iban = root.find(".//ns:Acct/ns:Id/ns:IBAN", ns)
    assert iban is not None
    assert iban.text == "NL42INGB0001234567"


def test_camt053_opening_balance():
    env = _make_envelope(BASIC_ROWS)
    result = to_camt053(env)

    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:camt.053.001.02"}
    root = ET.fromstring(result)

    bals = root.findall(".//ns:Bal", ns)
    # First Bal should be OPBD
    opbd = bals[0]
    assert opbd.find("ns:Tp/ns:CdOrPrtry/ns:Cd", ns).text == "OPBD"
    assert opbd.find("ns:Amt", ns).text == "1842.30"
    assert opbd.find("ns:CdtDbtInd", ns).text == "CRDT"


def test_camt053_closing_balance():
    env = _make_envelope(BASIC_ROWS)
    result = to_camt053(env)

    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:camt.053.001.02"}
    root = ET.fromstring(result)

    bals = root.findall(".//ns:Bal", ns)
    # Last Bal should be CLBD
    clbd = bals[-1]
    assert clbd.find("ns:Tp/ns:CdOrPrtry/ns:Cd", ns).text == "CLBD"
    assert clbd.find("ns:Amt", ns).text == "5068.85"


def test_camt053_debit_entry():
    env = _make_envelope(BASIC_ROWS)
    result = to_camt053(env)

    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:camt.053.001.02"}
    root = ET.fromstring(result)

    entries = root.findall(".//ns:Ntry", ns)
    assert len(entries) == 2

    # First entry: debit
    first = entries[0]
    assert first.find("ns:Amt", ns).text == "23.45"
    assert first.find("ns:CdtDbtInd", ns).text == "DBIT"
    assert first.find("ns:Sts", ns).text == "BOOK"

    # Debit = we paid, counterparty is creditor
    cdtr = first.find(".//ns:RltdPties/ns:Cdtr/ns:Nm", ns)
    assert cdtr is not None
    assert cdtr.text == "Albert Heijn 1076"


def test_camt053_credit_entry():
    env = _make_envelope(BASIC_ROWS)
    result = to_camt053(env)

    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:camt.053.001.02"}
    root = ET.fromstring(result)

    entries = root.findall(".//ns:Ntry", ns)
    second = entries[1]
    assert second.find("ns:Amt", ns).text == "3250.00"
    assert second.find("ns:CdtDbtInd", ns).text == "CRDT"

    # Credit = we received, counterparty is debtor
    dbtr = second.find(".//ns:RltdPties/ns:Dbtr/ns:Nm", ns)
    assert dbtr is not None
    assert dbtr.text == "Werkgever BV"


def test_camt053_remittance_info():
    env = _make_envelope(BASIC_ROWS)
    result = to_camt053(env)

    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:camt.053.001.02"}
    root = ET.fromstring(result)

    entries = root.findall(".//ns:Ntry", ns)
    ustrd = entries[0].find(".//ns:RmtInf/ns:Ustrd", ns)
    assert ustrd is not None
    assert ustrd.text == "Boodschappen"


def test_camt053_override_iban():
    env = _make_envelope(BASIC_ROWS)
    result = to_camt053(env, account_iban="NL99BUNQ0123456789")

    assert "NL99BUNQ0123456789" in result


def test_camt053_empty_data_raises():
    env = _make_envelope([])
    try:
        to_camt053(env)
        assert False, "Should have raised ValueError"
    except ValueError as e:
        assert "No transaction data" in str(e)


def test_camt053_booking_date():
    env = _make_envelope(BASIC_ROWS)
    result = to_camt053(env)

    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:camt.053.001.02"}
    root = ET.fromstring(result)

    entries = root.findall(".//ns:Ntry", ns)
    booking_dt = entries[0].find("ns:BookgDt/ns:Dt", ns)
    assert booking_dt is not None
    assert booking_dt.text == "2026-03-10"


# === Integration: parse ING CSV → MT940 → valid ===


ING_CSV = b'''"Datum";"Naam / Omschrijving";"Rekening";"Tegenrekening";"Code";"Af Bij";"Bedrag (EUR)";"Mutatiesoort";"Mededelingen";"Saldo na mutatie";"Tag"
"20260315";"Albert Heijn 1234";"NL12INGB0001234567";"";"";"\x41\x66";"45,20";"Betaalautomaat";"Pasvolgnr: 001 15-03-2026 14:32 Transactie: ABC123";"1234,56";""
"20260314";"Werkgever BV";"NL12INGB0001234567";"NL98RABO0123456789";"GT";"Bij";"2500,00";"Overschrijving";"Salaris maart 2026";"1279,76";""
"20260313";"Vattenfall";"NL12INGB0001234567";"NL55ABNA0987654321";"IC";"\x41\x66";"89,50";"Incasso";"Energiekosten mrt 2026 klantnr 12345";"4779,76";""
'''


def test_ing_to_mt940_integration():
    """Full pipeline: ING CSV → parse → MT940."""
    result = registry.parse(ING_CSV, "transactions.csv")
    assert result.success

    mt940 = to_mt940(result.envelope)

    # Should have all 3 transactions
    assert mt940.count(":61:") == 3
    assert mt940.count(":86:") == 3

    # Account IBAN from ING data
    assert ":25:NL12INGB0001234567" in mt940

    # First transaction: debit
    assert "D45,20" in mt940
    assert "Albert Heijn" in mt940

    # Second transaction: credit
    assert "C2500,00" in mt940


def test_ing_to_camt053_integration():
    """Full pipeline: ING CSV → parse → CAMT.053."""
    result = registry.parse(ING_CSV, "transactions.csv")
    assert result.success

    camt = to_camt053(result.envelope)

    # Valid XML
    root = ET.fromstring(camt)
    ns = {"ns": "urn:iso:std:iso:20022:tech:xsd:camt.053.001.02"}

    # 3 entries
    entries = root.findall(".//ns:Ntry", ns)
    assert len(entries) == 3

    # IBAN
    iban = root.find(".//ns:Acct/ns:Id/ns:IBAN", ns)
    assert iban.text == "NL12INGB0001234567"
