"""QIF (Quicken Interchange Format) parser."""

from __future__ import annotations

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# QIF account type headers
_ACCOUNT_TYPES = {
    "!Type:Bank": "bank",
    "!Type:Cash": "cash",
    "!Type:CCard": "credit_card",
    "!Type:Invst": "investment",
    "!Type:Oth A": "asset",
    "!Type:Oth L": "liability",
    "!Type:Invoice": "invoice",
}

# QIF date formats vary by locale and software
# Common formats: M/D/Y, D/M/Y, YYYY-MM-DD, D-M-YYYY, D/M'YY, etc.
_DATE_PATTERNS = [
    # ISO 8601
    (re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})$"), "%Y-%m-%d"),
    # US: M/D/Y or M/D/YYYY or M/D'YY (apostrophe = 1900s/2000s shorthand)
    (re.compile(r"^(\d{1,2})[/\-](\d{1,2})[/\-'](\d{2,4})$"), "mdy"),
    # D/M/Y (European, ambiguous with US — treat as M/D/Y by default)
    (re.compile(r"^(\d{1,2})[./](\d{1,2})[./](\d{4})$"), "mdy"),
]


def _parse_qif_date(raw: str) -> str:
    """Parse QIF date string to ISO YYYY-MM-DD.

    QIF dates are notoriously varied:
    - 1/15/2024 (US: M/D/YYYY)
    - 15/01/2024 (European: D/M/YYYY — ambiguous)
    - 2024-01-15 (ISO)
    - 1/15' 4 (Quicken format for 2004: M/D'YY)
    - 15-1-2024
    """
    if not raw:
        return ""
    raw = raw.strip()

    # Handle Quicken apostrophe year format: "1/15' 4" → Jan 15, 2004
    apostrophe_match = re.match(r"(\d{1,2})/(\d{1,2})'[\s]?(\d{1,2})$", raw)
    if apostrophe_match:
        m, d, yy = apostrophe_match.groups()
        year = 2000 + int(yy) if int(yy) < 50 else 1900 + int(yy)
        try:
            return date(year, int(m), int(d)).isoformat()
        except ValueError:
            pass

    # ISO 8601
    if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
        return raw

    # Try common formats
    for pattern, fmt in [
        (r"^(\d{1,2})/(\d{1,2})/(\d{4})$", "mdy_4"),
        (r"^(\d{1,2})/(\d{1,2})/(\d{2})$", "mdy_2"),
        (r"^(\d{1,2})-(\d{1,2})-(\d{4})$", "mdy_4"),
        (r"^(\d{1,2})\.(\d{1,2})\.(\d{4})$", "mdy_4"),
    ]:
        m = re.match(pattern, raw)
        if m:
            a, b, c = m.groups()
            if fmt.endswith("_2"):
                year = 2000 + int(c) if int(c) < 50 else 1900 + int(c)
            else:
                year = int(c)
            try:
                return date(year, int(a), int(b)).isoformat()
            except ValueError:
                try:
                    # Try D/M/Y interpretation
                    return date(year, int(b), int(a)).isoformat()
                except ValueError:
                    pass

    return raw


def _parse_amount(raw: str) -> Decimal:
    """Parse QIF amount: may have commas as thousands separators."""
    if not raw:
        return Decimal("0")
    cleaned = raw.strip().replace(",", "")
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        return Decimal("0")


class QIFParser(BaseParser):
    """Parser for QIF (Quicken Interchange Format) files.

    QIF is a line-based text format developed by Intuit for Quicken.
    It remains widely supported for exporting from online banking portals.

    Structure:
        !Type:Bank       — declares the account/transaction type
        !Account         — optional account block
        N<account name>  — account name in account block
        ^                — end of record separator

    Transaction fields:
        D<date>          — date
        T<amount>        — total amount (signed: negative = debit)
        U<amount>        — same as T (legacy duplicate)
        P<payee>         — payee / merchant name
        M<memo>          — memo / description
        C<cleared>       — cleared status: *, X, R (cleared, reconciled, etc.)
        N<number>        — check number or reference
        L<category>      — category (may include subcategory after ':')
        A<address>       — address lines (up to 5, repeated A lines)
        S<category>      — split transaction category
        E<memo>          — split transaction memo
        $<amount>        — split transaction amount
        ^                — end of record

    Notes:
        - Amount sign convention: negative = money out, positive = money in
        - Some exporters flip this — always verify with actual data
        - Multiple !Type: headers can appear for different account sections
        - Investment accounts (!Type:Invst) have different fields (N=action, Y=security, etc.)
    """

    def source_type(self) -> str:
        return "bank_qif"

    def source_label(self) -> str:
        return "QIF Bank Export (Quicken Interchange Format)"

    def detect(self, content: bytes, filename: str) -> float:
        name_lower = filename.lower()

        if name_lower.endswith(".qif"):
            # Check for QIF structure markers
            try:
                text_head = content[:2048].decode("utf-8", errors="replace")
            except Exception:
                text_head = ""
            if "!Type:" in text_head or "!type:" in text_head.lower():
                return 0.98
            return 0.85

        # Content sniff for .txt or unknown extension
        if name_lower.endswith((".txt", "")):
            try:
                text_head = content[:2048].decode("utf-8", errors="replace")
            except Exception:
                return 0.0
            if re.search(r"^!Type:(Bank|Cash|CCard|Invst|Oth)", text_head, re.MULTILINE | re.IGNORECASE):
                return 0.80
            if re.search(r"^!Account", text_head, re.MULTILINE):
                if re.search(r"^\^", text_head, re.MULTILINE):
                    return 0.60

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="date", dtype="date",
                                description="Transaction date", format="YYYY-MM-DD"),
                FieldAnnotation(name="amount", dtype="decimal",
                                description="Signed amount: negative = debit (money out), positive = credit (money in)"),
                FieldAnnotation(name="payee", dtype="string",
                                description="Payee or merchant name", nullable=True),
                FieldAnnotation(name="memo", dtype="string",
                                description="Transaction memo or description", nullable=True),
                FieldAnnotation(name="category", dtype="string",
                                description="User-assigned category (may include subcategory as 'Category:Subcategory')",
                                nullable=True),
                FieldAnnotation(name="check_num", dtype="string",
                                description="Check number or transaction reference", nullable=True),
                FieldAnnotation(name="cleared", dtype="enum",
                                description="Cleared/reconciled status",
                                enum_values=["cleared", "reconciled", "uncleared"],
                                nullable=True),
                FieldAnnotation(name="account_type", dtype="enum",
                                description="Account type from !Type: header",
                                enum_values=list(set(_ACCOUNT_TYPES.values()))),
                FieldAnnotation(name="splits", dtype="string",
                                description="Split transaction details as JSON list, if present", nullable=True),
            ],
            conventions=[
                "QIF amounts use accounting sign convention: negative = money out, positive = money in.",
                "Some exporters invert the sign — verify against known transactions.",
                "Date formats vary by locale and exporting software. All normalized to YYYY-MM-DD.",
                "The 'D' date line in some Quicken versions uses an apostrophe for 2-digit years: '1/15' 4' = Jan 15, 2004.",
                "Category field may include subcategory separated by colon: 'Food:Groceries'.",
                "Split transactions (S/E/$ lines) are preserved as a 'splits' JSON array.",
                "Investment transactions (!Type:Invst) are parsed with the same fields; N field is the action (Buy, Sell, etc.).",
                "The ^ character separates records — any malformed record without ^ is skipped.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        text = text.lstrip("\ufeff").replace("\r\n", "\n").replace("\r", "\n")

        lines = text.split("\n")
        rows = []
        warnings = []
        current_record: dict[str, list[str]] = {}
        account_type = "bank"
        in_account_block = False

        for line_num, line in enumerate(lines, start=1):
            if not line:
                continue

            code = line[0].upper()
            value = line[1:].strip()

            # Account type declarations
            if line.startswith("!Type:") or line.startswith("!type:"):
                type_key = "!" + line[1:].strip()
                # Normalize key to match _ACCOUNT_TYPES
                for k, v in _ACCOUNT_TYPES.items():
                    if k.lower() == type_key.lower():
                        account_type = v
                        break
                in_account_block = False
                continue

            if line.startswith("!Account") or line.startswith("!account"):
                in_account_block = True
                continue

            if code == "^":
                if in_account_block:
                    in_account_block = False
                    current_record = {}
                    continue

                if current_record:
                    try:
                        row = self._build_row(current_record, account_type)
                        rows.append(row)
                    except Exception as e:
                        warnings.append(f"Line {line_num}: {e}")
                    current_record = {}
                continue

            if in_account_block:
                continue

            # Accumulate fields
            if code in current_record:
                current_record[code].append(value)
            else:
                current_record[code] = [value]

        # Handle file without trailing ^
        if current_record:
            try:
                row = self._build_row(current_record, account_type)
                rows.append(row)
            except Exception as e:
                warnings.append(f"Final record: {e}")

        if not rows:
            return ParseResult(success=False, error="No QIF transactions could be parsed")

        envelope = ContextEnvelope(
            schema=self.schema(),
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _build_row(self, record: dict, account_type: str) -> dict:
        import json

        def first(key: str) -> str:
            vals = record.get(key, [])
            return vals[0] if vals else ""

        # Cleared status
        cleared_raw = first("C").upper()
        if cleared_raw in ("*", "C"):
            cleared = "cleared"
        elif cleared_raw in ("X", "R"):
            cleared = "reconciled"
        elif cleared_raw:
            cleared = "uncleared"
        else:
            cleared = None

        # Split transactions: S (category), E (memo), $ (amount) — one set per split
        splits = []
        s_cats = record.get("S", [])
        s_memos = record.get("E", [])
        s_amounts = record.get("$", [])
        for idx in range(len(s_cats)):
            split = {"category": s_cats[idx]}
            if idx < len(s_memos):
                split["memo"] = s_memos[idx]
            if idx < len(s_amounts):
                split["amount"] = str(_parse_amount(s_amounts[idx]))
            splits.append(split)

        # Amount: use T field (total); U is a duplicate of T in most software
        amount = _parse_amount(first("T") or first("U"))

        return {
            "date": _parse_qif_date(first("D")),
            "amount": str(amount),
            "payee": first("P") or None,
            "memo": first("M") or None,
            "category": first("L") or None,
            "check_num": first("N") or None,
            "cleared": cleared,
            "account_type": account_type,
            "splits": json.dumps(splits) if splits else None,
        }

    def _decode(self, content: bytes) -> str | None:
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(QIFParser())
