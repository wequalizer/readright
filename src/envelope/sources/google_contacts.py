"""Google Contacts CSV export parser."""

from __future__ import annotations

import csv
import io

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Required headers that uniquely identify a Google Contacts export
_REQUIRED_HEADERS = {"Given Name", "Family Name", "Group Membership"}

# Headers that appear in the standard Google Contacts CSV
_GOOGLE_CONTACTS_HEADERS = {
    "Name", "Given Name", "Additional Name", "Family Name",
    "Yomi Name", "Given Name Yomi", "Additional Name Yomi", "Family Name Yomi",
    "Name Prefix", "Name Suffix", "Initials", "Nickname", "Short Name",
    "Maiden Name", "Birthday", "Gender", "Location", "Billing Information",
    "Directory Server", "Mileage", "Occupation", "Hobby", "Sensitivity",
    "Priority", "Subject", "Notes", "Language", "Photo", "Group Membership",
}


class GoogleContactsParser(BaseParser):
    """Parser for Google Contacts CSV exports.

    Google Contacts exports have these characteristics:
    - UTF-8 encoding with BOM
    - Repeated column patterns for multi-value fields: 'E-mail 1 - Type', 'E-mail 1 - Value',
      'E-mail 2 - Type', 'E-mail 2 - Value', etc.
    - Same pattern for Phone, Address, Website, Relation fields
    - Group Membership uses ' ::: ' as separator for multiple groups
    - Birthday format: YYYY-MM-DD or --MM-DD (year unknown)
    - Empty fields are common — nearly every field is nullable
    """

    def source_type(self) -> str:
        return "google_contacts_csv"

    def source_label(self) -> str:
        return "Google Contacts CSV Export"

    def detect(self, content: bytes, filename: str) -> float:
        if not filename.lower().endswith(".csv"):
            return 0.0

        text = self._decode(content)
        if text is None:
            return 0.0

        text = text.lstrip("\ufeff")
        first_line = text.split("\n")[0]
        headers = {h.strip().strip('"') for h in first_line.split(",")}

        # Must have the core Google Contacts-specific set
        if _REQUIRED_HEADERS.issubset(headers):
            # Count how many known Google Contacts headers appear
            overlap = len(headers & _GOOGLE_CONTACTS_HEADERS)
            if overlap >= 10:
                return 0.95
            return 0.80

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="name", dtype="string", description="Full display name"),
                FieldAnnotation(name="given_name", dtype="string", description="First name", nullable=True),
                FieldAnnotation(name="additional_name", dtype="string", description="Middle name(s)", nullable=True),
                FieldAnnotation(name="family_name", dtype="string", description="Last/family name", nullable=True),
                FieldAnnotation(name="name_prefix", dtype="string", description="Title prefix (Mr., Dr., etc.)", nullable=True),
                FieldAnnotation(name="name_suffix", dtype="string", description="Name suffix (Jr., PhD, etc.)", nullable=True),
                FieldAnnotation(name="nickname", dtype="string", description="Nickname", nullable=True),
                FieldAnnotation(name="birthday", dtype="date", description="Birthday", format="YYYY-MM-DD or --MM-DD", nullable=True),
                FieldAnnotation(name="gender", dtype="string", description="Gender", nullable=True),
                FieldAnnotation(name="occupation", dtype="string", description="Job title or occupation", nullable=True),
                FieldAnnotation(name="notes", dtype="string", description="Free-text notes", nullable=True),
                FieldAnnotation(name="groups", dtype="string", description="Group memberships, separated by ' ::: '", nullable=True),
                FieldAnnotation(name="emails", dtype="string", description="Email addresses as JSON list of {type, value}", nullable=True),
                FieldAnnotation(name="phones", dtype="string", description="Phone numbers as JSON list of {type, value}", nullable=True),
                FieldAnnotation(name="addresses", dtype="string", description="Addresses as JSON list of {type, street, city, ...}", nullable=True),
            ],
            conventions=[
                "Multi-value fields (email, phone, address, website) repeat as 'Field N - Type' / 'Field N - Value' columns.",
                "Group Membership uses ' ::: ' (space-colon-colon-colon-space) as separator for multiple groups.",
                "Birthday may be '--MM-DD' (no year) when year is not set.",
                "Almost every field is nullable — contacts commonly have minimal data.",
                "The 'Name' field is the display name; 'Given Name' + 'Family Name' are the structured parts.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        text = self._decode(content)
        if text is None:
            return ParseResult(success=False, error="Could not decode file encoding")

        text = text.lstrip("\ufeff")

        reader = csv.DictReader(io.StringIO(text))
        if not reader.fieldnames:
            return ParseResult(success=False, error="No CSV headers found")

        fieldnames = [f.strip() for f in reader.fieldnames]

        rows = []
        warnings = []

        for i, row in enumerate(reader):
            try:
                parsed = self._parse_row(row, fieldnames)
                rows.append(parsed)
            except Exception as e:
                warnings.append(f"Row {i + 1}: {e}")

        if not rows:
            return ParseResult(success=False, error="No rows could be parsed")

        envelope = ContextEnvelope(
            schema=self.schema(),
            data=rows,
            warnings=warnings,
        )
        return ParseResult(success=True, envelope=envelope, warnings=warnings)

    def _parse_row(self, row: dict, fieldnames: list[str]) -> dict:
        """Parse a single Google Contacts row into normalized format."""
        import json

        def get(key: str) -> str:
            return (row.get(key) or "").strip()

        # Collect multi-value fields by scanning column name patterns
        emails = self._collect_indexed(row, fieldnames, "E-mail")
        phones = self._collect_indexed(row, fieldnames, "Phone")
        addresses = self._collect_addresses(row, fieldnames)

        groups_raw = get("Group Membership")
        groups = [g.strip() for g in groups_raw.split(" ::: ") if g.strip()] if groups_raw else []

        return {
            "name": get("Name"),
            "given_name": get("Given Name") or None,
            "additional_name": get("Additional Name") or None,
            "family_name": get("Family Name") or None,
            "name_prefix": get("Name Prefix") or None,
            "name_suffix": get("Name Suffix") or None,
            "nickname": get("Nickname") or None,
            "birthday": get("Birthday") or None,
            "gender": get("Gender") or None,
            "occupation": get("Occupation") or None,
            "notes": get("Notes") or None,
            "groups": groups if groups else None,
            "emails": emails if emails else None,
            "phones": phones if phones else None,
            "addresses": addresses if addresses else None,
        }

    def _collect_indexed(self, row: dict, fieldnames: list[str], prefix: str) -> list[dict]:
        """Collect repeated 'Prefix N - Type' / 'Prefix N - Value' columns."""
        results = []
        seen_indices = set()

        for col in fieldnames:
            if col.startswith(prefix + " ") and " - Type" in col:
                # Extract index: "E-mail 1 - Type" → 1
                try:
                    idx_part = col[len(prefix) + 1:col.index(" - Type")]
                    idx = int(idx_part)
                except (ValueError, AttributeError):
                    continue

                if idx in seen_indices:
                    continue
                seen_indices.add(idx)

                type_col = f"{prefix} {idx} - Type"
                value_col = f"{prefix} {idx} - Value"

                type_val = (row.get(type_col) or "").strip()
                value_val = (row.get(value_col) or "").strip()

                if value_val:
                    results.append({"type": type_val or "other", "value": value_val})

        return results

    def _collect_addresses(self, row: dict, fieldnames: list[str]) -> list[dict]:
        """Collect repeated address blocks."""
        results = []
        seen_indices = set()
        address_prefix = "Address"

        for col in fieldnames:
            if col.startswith(address_prefix + " ") and " - Type" in col:
                try:
                    idx_part = col[len(address_prefix) + 1:col.index(" - Type")]
                    idx = int(idx_part)
                except (ValueError, AttributeError):
                    continue

                if idx in seen_indices:
                    continue
                seen_indices.add(idx)

                addr = {}
                for sub in ["Type", "Street", "City", "PO Box", "Region", "Postal Code", "Country", "Extended Address"]:
                    val = (row.get(f"{address_prefix} {idx} - {sub}") or "").strip()
                    if val:
                        addr[sub.lower().replace(" ", "_")] = val

                if any(k != "type" for k in addr):
                    results.append(addr)

        return results

    def _decode(self, content: bytes) -> str | None:
        for enc in ["utf-8-sig", "utf-8", "latin-1", "cp1252"]:
            try:
                return content.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return None


registry.register(GoogleContactsParser())
