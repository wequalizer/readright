"""OFX/QFX (Open Financial Exchange) parser — universal bank format."""

from __future__ import annotations

import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# OFX is a hybrid SGML/XML format. Older versions (1.x) are SGML (no closing tags).
# Newer versions (2.x) are valid XML. We handle both.

_TRNTYPE_MAP = {
    "CREDIT": "credit",
    "DEBIT": "debit",
    "INT": "interest",
    "DIV": "dividend",
    "FEE": "fee",
    "SRVCHG": "service_charge",
    "DEP": "deposit",
    "ATM": "atm",
    "POS": "pos",
    "XFER": "transfer",
    "CHECK": "check",
    "PAYMENT": "payment",
    "CASH": "cash",
    "DIRECTDEP": "direct_deposit",
    "DIRECTDEBIT": "direct_debit",
    "REPEATPMT": "repeat_payment",
    "OTHER": "other",
}


def _parse_ofx_date(raw: str) -> str:
    """Parse OFX date format to ISO 8601.

    OFX dates: YYYYMMDD or YYYYMMDDHHMMSS or YYYYMMDDHHMMSS.mmm[+/-HH:MM]
    """
    if not raw:
        return ""
    raw = raw.strip()
    # Strip timezone suffix like [+1:CET] or [-5:EST]
    raw = re.sub(r"\[.*\]", "", raw)

    formats = [
        ("%Y%m%d%H%M%S", 14),
        ("%Y%m%d", 8),
    ]
    for fmt, length in formats:
        if len(raw) >= length:
            try:
                return datetime.strptime(raw[:length], fmt).date().isoformat()
            except ValueError:
                continue
    return raw


def _extract_tag(text: str, tag: str) -> str:
    """Extract the value of an OFX SGML tag (handles both SGML and XML)."""
    # XML style: <TAG>value</TAG>
    xml_match = re.search(rf"<{tag}>(.*?)</{tag}>", text, re.IGNORECASE | re.DOTALL)
    if xml_match:
        return xml_match.group(1).strip()
    # SGML style: <TAG>value (no closing tag, value ends at next tag or newline)
    sgml_match = re.search(rf"<{tag}>([^<\n\r]+)", text, re.IGNORECASE)
    if sgml_match:
        return sgml_match.group(1).strip()
    return ""


def _extract_all_blocks(text: str, tag: str) -> list[str]:
    """Extract all occurrences of a block tag (e.g. STMTTRN)."""
    # XML-style: <STMTTRN>...</STMTTRN>
    xml_blocks = re.findall(rf"<{tag}>(.*?)</{tag}>", text, re.IGNORECASE | re.DOTALL)
    if xml_blocks:
        return xml_blocks

    # SGML-style: <STMTTRN>...</STMTTRN> OR until next <STMTTRN> or </BANKTRANLIST>
    # Splits on the opening tag
    parts = re.split(rf"<{tag}>", text, flags=re.IGNORECASE)
    if len(parts) <= 1:
        return []
    return parts[1:]  # First element is before any block


class OFXParser(BaseParser):
    """Parser for OFX and QFX bank transaction files.

    OFX (Open Financial Exchange) is used by virtually all major US banks,
    brokerages, and many international banks. QFX is Quicken's variant — it is
    functionally identical to OFX with minor metadata differences.

    OFX 1.x: SGML-like, no closing tags, plain text headers separated from body by blank line
    OFX 2.x: Valid XML, usually starts with <?OFX version="220"?>

    Key structure:
        <OFX>
          <BANKMSGSRSV1>
            <STMTTRNRS>
              <STMTRS>
                <CURDEF>USD</CURDEF>
                <BANKACCTFROM><ACCTID>1234</ACCTID></BANKACCTFROM>
                <BANKTRANLIST>
                  <STMTTRN>
                    <TRNTYPE>DEBIT</TRNTYPE>
                    <DTPOSTED>20240101</DTPOSTED>
                    <TRNAMT>-45.00</TRNAMT>
                    <FITID>unique-id</FITID>
                    <NAME>Store Name</NAME>
                    <MEMO>Purchase description</MEMO>
                  </STMTTRN>
                </BANKTRANLIST>
              </STMTRS>
            </STMTTRNRS>
          </BANKMSGSRSV1>
        </OFX>
    """

    def source_type(self) -> str:
        return "bank_ofx"

    def source_label(self) -> str:
        return "OFX/QFX Bank Export (Open Financial Exchange)"

    def detect(self, content: bytes, filename: str) -> float:
        name_lower = filename.lower()

        if name_lower.endswith((".ofx", ".qfx")):
            if b"<OFX>" in content or b"<ofx>" in content or b"OFXHEADER:" in content:
                return 0.98
            return 0.85  # Extension match alone is fairly definitive

        if name_lower.endswith(".xml"):
            if b"<OFX>" in content[:4096] or b"STMTTRN" in content[:4096]:
                return 0.80

        # Content sniff for files with no recognized extension
        if b"OFXHEADER:" in content[:256] or b"<OFX>" in content[:4096]:
            if b"<STMTTRN>" in content or b"STMTTRN" in content:
                return 0.75

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="date", dtype="date",
                                description="Transaction posting date", format="YYYY-MM-DD"),
                FieldAnnotation(name="amount", dtype="decimal",
                                description="Signed amount: negative = debit (money out), positive = credit (money in)"),
                FieldAnnotation(name="currency", dtype="string",
                                description="Currency code (ISO 4217)", examples=["USD", "EUR", "GBP"]),
                FieldAnnotation(name="type", dtype="enum",
                                description="Transaction type",
                                enum_values=list(set(_TRNTYPE_MAP.values()))),
                FieldAnnotation(name="name", dtype="string",
                                description="Merchant or counterparty name", nullable=True),
                FieldAnnotation(name="memo", dtype="string",
                                description="Transaction memo/description", nullable=True),
                FieldAnnotation(name="fitid", dtype="string",
                                description="Financial institution transaction ID (unique per account)",
                                nullable=True),
                FieldAnnotation(name="check_num", dtype="string",
                                description="Check number (for check transactions)", nullable=True),
                FieldAnnotation(name="account_id", dtype="string",
                                description="Account identifier from the financial institution", nullable=True),
                FieldAnnotation(name="date_user", dtype="date",
                                description="User-initiated transaction date (may differ from posting date)",
                                format="YYYY-MM-DD", nullable=True),
            ],
            conventions=[
                "TRNAMT is signed in OFX: negative = money out (debit), positive = money in (credit).",
                "FITID uniquely identifies transactions within an account — use for deduplication.",
                "NAME is the payee/merchant; MEMO has additional detail. Both may be absent.",
                "OFX 1.x (SGML) and OFX 2.x (XML) are both supported.",
                "QFX (Quicken) files are functionally identical to OFX and parsed the same way.",
                "Currency is extracted from CURDEF at the statement level; applies to all transactions.",
                "Some banks include balance information in LEDGERBAL / AVAILBAL (not extracted to rows).",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        try:
            text = content.decode("utf-8")
        except UnicodeDecodeError:
            text = content.decode("latin-1", errors="replace")

        # Strip OFX 1.x plain-text headers (everything before the blank line before <OFX>)
        ofx_start = text.find("<OFX>")
        if ofx_start == -1:
            ofx_start = text.lower().find("<ofx>")
        if ofx_start == -1:
            return ParseResult(success=False, error="No <OFX> element found")
        body = text[ofx_start:]

        # Extract currency (statement level)
        currency = _extract_tag(body, "CURDEF") or "USD"

        # Extract account ID
        account_id = _extract_tag(body, "ACCTID") or None

        # Extract all transaction blocks
        blocks = _extract_all_blocks(body, "STMTTRN")
        if not blocks:
            return ParseResult(success=False, error="No STMTTRN transaction blocks found")

        rows = []
        warnings = []

        for i, block in enumerate(blocks):
            try:
                row = self._parse_transaction(block, currency, account_id, warnings)
                rows.append(row)
            except Exception as e:
                warnings.append(f"Transaction {i + 1}: {e}")

        if not rows:
            return ParseResult(success=False, error="No transactions could be parsed")

        envelope = ContextEnvelope(
            schema=self.schema(),
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _parse_transaction(self, block: str, currency: str, account_id: str | None, warnings: list | None = None) -> dict:
        raw_amount_orig = _extract_tag(block, "TRNAMT")
        raw_amount = raw_amount_orig.replace(",", ".")
        try:
            amount = Decimal(raw_amount)
        except (InvalidOperation, Exception):
            if warnings is not None:
                warnings.append(f"Could not parse TRNAMT '{raw_amount_orig}', defaulting to 0")
            amount = Decimal("0")

        trntype_raw = _extract_tag(block, "TRNTYPE").upper()
        trntype = _TRNTYPE_MAP.get(trntype_raw, trntype_raw.lower() or "other")

        date_user_raw = _extract_tag(block, "DTUSER")
        date_user = _parse_ofx_date(date_user_raw) if date_user_raw else None

        return {
            "date": _parse_ofx_date(_extract_tag(block, "DTPOSTED")),
            "amount": str(amount),
            "currency": currency,
            "type": trntype,
            "name": _extract_tag(block, "NAME") or None,
            "memo": _extract_tag(block, "MEMO") or None,
            "fitid": _extract_tag(block, "FITID") or None,
            "check_num": _extract_tag(block, "CHECKNUM") or None,
            "account_id": account_id,
            "date_user": date_user,
        }


registry.register(OFXParser())
