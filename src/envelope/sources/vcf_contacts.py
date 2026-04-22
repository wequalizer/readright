"""vCard (.vcf) contact format parser."""

from __future__ import annotations

import re

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# vCard versions we support
_SUPPORTED_VERSIONS = {"2.1", "3.0", "4.0"}

# Standard property names and their normalized output keys
_PROP_MAP = {
    "FN": "full_name",
    "N": "name_structured",
    "NICKNAME": "nickname",
    "BDAY": "birthday",
    "GENDER": "gender",
    "ORG": "organization",
    "TITLE": "title",
    "NOTE": "notes",
    "UID": "uid",
    "URL": "url",
    "CATEGORIES": "categories",
    "PHOTO": "photo_url",  # Only URL-based photos, not base64 blobs
    "ADR": "address",
    "TEL": "phone",
    "EMAIL": "email",
    "IMPP": "instant_messaging",
    "X-SOCIALPROFILE": "social_profile",
}


def _unfold_lines(text: str) -> str:
    """Unfold vCard line continuations (RFC 6350: folded lines start with whitespace)."""
    return re.sub(r"\r?\n[ \t]", "", text)


def _parse_params(prop_line: str) -> tuple[str, dict[str, str], str]:
    """Split a vCard property line into (name, params, value).

    e.g. 'TEL;TYPE=CELL,VOICE:+31612345678'
    returns ('TEL', {'TYPE': 'CELL,VOICE'}, '+31612345678')
    """
    colon_idx = prop_line.find(":")
    if colon_idx == -1:
        return prop_line.strip(), {}, ""

    lhs = prop_line[:colon_idx]
    value = prop_line[colon_idx + 1:]

    # Split on semicolons to get name + params
    parts = lhs.split(";")
    name = parts[0].strip().upper()
    params: dict[str, str] = {}
    for part in parts[1:]:
        if "=" in part:
            k, v = part.split("=", 1)
            params[k.strip().upper()] = v.strip()
        elif part.strip():
            # vCard 2.1 bare params like ;CELL;VOICE → type value
            params.setdefault("TYPE", part.strip().upper())

    return name, params, value


def _decode_value(value: str, params: dict[str, str]) -> str:
    """Decode encoding if specified (QUOTED-PRINTABLE or BASE64/B)."""
    encoding = params.get("ENCODING", "").upper()
    charset = params.get("CHARSET", "utf-8")

    if encoding == "QUOTED-PRINTABLE":
        import quopri
        try:
            return quopri.decodestring(value.encode("ascii", errors="replace")).decode(charset, errors="replace")
        except Exception:
            return value

    if encoding in ("BASE64", "B"):
        # Don't decode base64 blobs (photos etc.) — just mark as binary
        return "[base64 encoded data]"

    return value


class VCFParser(BaseParser):
    """Parser for vCard (.vcf) contact files.

    Supports vCard versions 2.1, 3.0, and 4.0.
    A single .vcf file may contain multiple vCard records (one per contact).

    vCard structure:
        BEGIN:VCARD
        VERSION:3.0
        FN:John Doe
        N:Doe;John;Middle;Mr.;Jr.
        TEL;TYPE=CELL:+31612345678
        EMAIL;TYPE=WORK:john@example.com
        ADR;TYPE=HOME:;;123 Main St;Amsterdam;;1234AB;Netherlands
        ORG:Acme Corp
        TITLE:Software Engineer
        BDAY:1990-01-15
        NOTE:Met at conference 2023
        END:VCARD

    Multi-value properties (TEL, EMAIL, ADR) produce lists.
    The N (structured name) field: Family;Given;Additional;Prefix;Suffix
    The ADR (address) field: PO Box;Extended;Street;City;Region;Postal;Country
    """

    def source_type(self) -> str:
        return "vcf_contacts"

    def source_label(self) -> str:
        return "vCard Contacts (.vcf)"

    def detect(self, content: bytes, filename: str) -> float:
        name_lower = filename.lower()

        if name_lower.endswith((".vcf", ".vcard")):
            if b"BEGIN:VCARD" in content[:256] or b"begin:vcard" in content[:256].lower():
                return 0.98
            return 0.75

        # Content sniff for .txt or unknown
        snippet = content[:512]
        if b"BEGIN:VCARD" in snippet or b"begin:vcard" in snippet.lower():
            return 0.90

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="full_name", dtype="string",
                                description="Formatted/display name (FN property)"),
                FieldAnnotation(name="given_name", dtype="string",
                                description="First name from structured N field", nullable=True),
                FieldAnnotation(name="family_name", dtype="string",
                                description="Last/family name from structured N field", nullable=True),
                FieldAnnotation(name="additional_name", dtype="string",
                                description="Middle name(s)", nullable=True),
                FieldAnnotation(name="name_prefix", dtype="string",
                                description="Name prefix (Mr., Dr., etc.)", nullable=True),
                FieldAnnotation(name="name_suffix", dtype="string",
                                description="Name suffix (Jr., PhD, etc.)", nullable=True),
                FieldAnnotation(name="nickname", dtype="string",
                                description="Nickname", nullable=True),
                FieldAnnotation(name="birthday", dtype="date",
                                description="Birthday", format="YYYY-MM-DD or YYYYMMDD", nullable=True),
                FieldAnnotation(name="gender", dtype="string",
                                description="Gender", nullable=True),
                FieldAnnotation(name="organization", dtype="string",
                                description="Organization/company name", nullable=True),
                FieldAnnotation(name="title", dtype="string",
                                description="Job title", nullable=True),
                FieldAnnotation(name="phones", dtype="string",
                                description="Phone numbers as list of {type, value}", nullable=True),
                FieldAnnotation(name="emails", dtype="string",
                                description="Email addresses as list of {type, value}", nullable=True),
                FieldAnnotation(name="addresses", dtype="string",
                                description="Addresses as list of {type, street, city, region, postal_code, country}",
                                nullable=True),
                FieldAnnotation(name="url", dtype="string",
                                description="Website URL", nullable=True),
                FieldAnnotation(name="notes", dtype="string",
                                description="Free-text notes", nullable=True),
                FieldAnnotation(name="categories", dtype="string",
                                description="Contact categories/groups, comma-separated", nullable=True),
                FieldAnnotation(name="uid", dtype="string",
                                description="Unique identifier for the contact", nullable=True),
                FieldAnnotation(name="version", dtype="string",
                                description="vCard version (2.1, 3.0, or 4.0)"),
            ],
            conventions=[
                "A single .vcf file may contain multiple contacts — one dict per contact in the output.",
                "Multi-value fields (phones, emails, addresses) are lists of dicts with 'type' and 'value' keys.",
                "The N field stores: Family;Given;Additional;Prefix;Suffix — split accordingly.",
                "The ADR field stores: PO Box;Extended;Street;City;Region;Postal Code;Country.",
                "TYPE parameter values are normalized to lowercase: 'CELL' → 'cell', 'WORK,VOICE' → 'work,voice'.",
                "vCard 2.1 uses QUOTED-PRINTABLE encoding for non-ASCII characters — decoded automatically.",
                "BASE64-encoded photos are replaced with '[base64 encoded data]' to avoid bloating the context.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        text = _unfold_lines(text)
        blocks = re.split(r"BEGIN:VCARD", text, flags=re.IGNORECASE)
        # First element is before any VCARD — skip it
        blocks = [b for b in blocks[1:] if b.strip()]

        if not blocks:
            return ParseResult(success=False, error="No VCARD blocks found")

        rows = []
        warnings = []

        for i, block in enumerate(blocks):
            try:
                contact = self._parse_vcard(block)
                rows.append(contact)
            except Exception as e:
                warnings.append(f"Contact {i + 1}: {e}")

        if not rows:
            return ParseResult(success=False, error="No contacts could be parsed")

        envelope = ContextEnvelope(
            schema=self.schema(),
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _parse_vcard(self, block: str) -> dict:
        """Parse a single vCard block (content after BEGIN:VCARD)."""

        # Strip END:VCARD
        block = re.sub(r"END:VCARD.*", "", block, flags=re.IGNORECASE)

        # Parse all property lines
        props: list[tuple[str, dict, str]] = []
        for line in block.splitlines():
            line = line.strip()
            if not line:
                continue
            name, params, value = _parse_params(line)
            if name:
                value = _decode_value(value, params)
                props.append((name, params, value))

        # Build lookup: name → list of (params, value)
        prop_map: dict[str, list[tuple[dict, str]]] = {}
        for name, params, value in props:
            prop_map.setdefault(name, []).append((params, value))

        def first(key: str) -> str | None:
            entries = prop_map.get(key, [])
            return entries[0][1].strip() if entries else None

        # Structured name: N;Family;Given;Additional;Prefix;Suffix
        name_parts = ["", "", "", "", ""]
        n_raw = first("N")
        if n_raw:
            parts = n_raw.split(";")
            parts = (parts + [""] * 5)[:5]
            name_parts = parts

        # Multi-value: phones, emails, addresses
        phones = self._collect_multi(prop_map, "TEL")
        emails = self._collect_multi(prop_map, "EMAIL")
        addresses = self._collect_addresses(prop_map)

        # Version
        version = first("VERSION") or "3.0"

        return {
            "full_name": first("FN") or "",
            "given_name": name_parts[1] or None,
            "family_name": name_parts[0] or None,
            "additional_name": name_parts[2] or None,
            "name_prefix": name_parts[3] or None,
            "name_suffix": name_parts[4] or None,
            "nickname": first("NICKNAME") or None,
            "birthday": self._normalize_date(first("BDAY")) or None,
            "gender": first("GENDER") or None,
            "organization": first("ORG") or None,
            "title": first("TITLE") or None,
            "phones": phones if phones else None,
            "emails": emails if emails else None,
            "addresses": addresses if addresses else None,
            "url": first("URL") or None,
            "notes": first("NOTE") or None,
            "categories": first("CATEGORIES") or None,
            "uid": first("UID") or None,
            "version": version,
        }

    def _collect_multi(self, prop_map: dict, key: str) -> list[dict]:
        """Collect multi-value properties (TEL, EMAIL) with their type."""
        results = []
        for params, value in prop_map.get(key, []):
            if not value.strip():
                continue
            type_val = params.get("TYPE", "other").lower().strip(",")
            results.append({"type": type_val, "value": value.strip()})
        return results

    def _collect_addresses(self, prop_map: dict) -> list[dict]:
        """Collect ADR properties: PO Box;Extended;Street;City;Region;Postal;Country"""
        results = []
        for params, value in prop_map.get("ADR", []):
            parts = (value.split(";") + [""] * 7)[:7]
            addr = {
                "type": params.get("TYPE", "other").lower(),
                "po_box": parts[0].strip() or None,
                "extended": parts[1].strip() or None,
                "street": parts[2].strip() or None,
                "city": parts[3].strip() or None,
                "region": parts[4].strip() or None,
                "postal_code": parts[5].strip() or None,
                "country": parts[6].strip() or None,
            }
            # Only include if at least one meaningful field is set
            meaningful = {k: v for k, v in addr.items() if k != "type" and v}
            if meaningful:
                results.append(addr)
        return results

    def _normalize_date(self, raw: str | None) -> str | None:
        """Normalize vCard date (YYYYMMDD or YYYY-MM-DD or --MMDD) to YYYY-MM-DD."""
        if not raw:
            return None
        raw = raw.strip()
        # Already ISO
        if re.match(r"^\d{4}-\d{2}-\d{2}$", raw):
            return raw
        # YYYYMMDD
        if re.match(r"^\d{8}$", raw):
            return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
        # --MMDD (no year)
        if re.match(r"^--\d{4}$", raw):
            return f"--{raw[2:4]}-{raw[4:6]}"
        return raw

    def _decode(self, content: bytes) -> str | None:
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(VCFParser())
