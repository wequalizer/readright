"""MT940 (SWIFT) output formatter.

Takes a ContextEnvelope with bank transaction data and produces valid MT940 text
that Moneybird (and other accounting tools) will accept as a bank statement import.

MT940 spec (simplified):
  :20:  Transaction reference number
  :25:  Account identification (IBAN)
  :28C: Statement number / sequence
  :60F: Opening balance
  :61:  Statement line (one per transaction)
  :86:  Information to account owner (description)
  :62F: Closing (available) balance
"""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from envelope.envelope import ContextEnvelope


def _parse_date(raw: str) -> date:
    """Parse a date string from envelope data to a date object."""
    if not raw:
        return date.today()
    # Try ISO format first (YYYY-MM-DD)
    if len(raw) >= 10 and raw[4] == "-":
        return date(int(raw[:4]), int(raw[5:7]), int(raw[8:10]))
    # YYYYMMDD
    if len(raw) == 8 and raw.isdigit():
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
    return date.today()


def _mt940_date(d: date) -> str:
    """Format date as YYMMDD for MT940."""
    return d.strftime("%y%m%d")


def _mt940_amount(amount: Decimal) -> str:
    """Format amount for MT940: no sign, comma as decimal separator."""
    # MT940 uses comma as decimal separator, always 2 decimal places
    abs_amount = abs(amount)
    return f"{abs_amount:.2f}".replace(".", ",")


def _sanitize_text(text: str, max_len: int = 65) -> str:
    """Sanitize text for MT940 — only SWIFT-safe characters, max length."""
    # SWIFT allows: a-z A-Z 0-9 / - ? : ( ) . , ' + { } CR LF space
    allowed = set(
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "0123456789/-?:().,'+{} "
    )
    cleaned = "".join(c if c in allowed else " " for c in text)
    # Collapse multiple spaces
    cleaned = " ".join(cleaned.split())
    return cleaned[:max_len]


def _get_account_iban(rows: list[dict[str, Any]]) -> str:
    """Extract the account IBAN from transaction rows."""
    for row in rows:
        iban = row.get("account_iban", "")
        if iban:
            return iban
    return "NL00UNKN0000000000"


def _compute_balances(rows: list[dict[str, Any]]) -> tuple[Decimal, Decimal, date, date]:
    """Compute opening balance, closing balance, first date, last date.

    If balance_after is available on rows, we use it to derive the opening balance.
    Otherwise we assume opening balance 0 and compute from amounts.
    """
    if not rows:
        today = date.today()
        return Decimal("0"), Decimal("0"), today, today

    first_date = _parse_date(rows[0].get("date", ""))
    last_date = _parse_date(rows[-1].get("date", ""))

    # Try to derive from balance_after fields
    first_balance_after = rows[0].get("balance_after")
    first_amount = Decimal(rows[0].get("amount", "0"))

    if first_balance_after is not None:
        closing_balance_raw = rows[-1].get("balance_after")
        if closing_balance_raw is not None:
            closing = Decimal(closing_balance_raw)
        else:
            closing = Decimal(first_balance_after)
            for row in rows[1:]:
                closing += Decimal(row.get("amount", "0"))

        opening = Decimal(first_balance_after) - first_amount
        return opening, closing, first_date, last_date

    # No balance_after — assume opening = 0
    opening = Decimal("0")
    closing = opening
    for row in rows:
        closing += Decimal(row.get("amount", "0"))

    return opening, closing, first_date, last_date


def to_mt940(
    envelope: ContextEnvelope,
    account_iban: str | None = None,
    statement_number: int = 1,
    currency: str = "EUR",
) -> str:
    """Convert a ContextEnvelope with bank transactions to MT940 format.

    Args:
        envelope: ContextEnvelope with bank transaction data (normalized format).
        account_iban: Override account IBAN. Auto-detected from data if not provided.
        statement_number: Statement number for :28C: field.
        currency: Currency code (default EUR).

    Returns:
        Valid MT940 text string ready for import into Moneybird or similar tools.
    """
    rows = envelope.data
    if not rows:
        raise ValueError("No transaction data in envelope")

    # Determine account
    iban = account_iban or _get_account_iban(rows)

    # Detect currency from data if available
    for row in rows:
        if row.get("currency"):
            currency = row["currency"]
            break

    opening_bal, closing_bal, first_date, last_date = _compute_balances(rows)

    lines = []

    # --- Header ---
    # :20: Transaction Reference Number (max 16 chars)
    ref = f"READRIGHT{statement_number:04d}"
    lines.append(f":20:{ref}")

    # :25: Account Identification
    lines.append(f":25:{iban}")

    # :28C: Statement Number / Sequence Number
    lines.append(f":28C:{statement_number}/1")

    # :60F: Opening Balance
    # Format: D/C indicator + date YYMMDD + currency + amount
    bal_dc = "D" if opening_bal < 0 else "C"
    lines.append(
        f":60F:{bal_dc}{_mt940_date(first_date)}{currency}{_mt940_amount(opening_bal)}"
    )

    # --- Transaction lines ---
    for row in rows:
        tx_date = _parse_date(row.get("date", ""))
        amount = Decimal(row.get("amount", "0"))
        direction = row.get("direction", "credit" if amount >= 0 else "debit")

        # :61: Statement Line
        # Format: value date (YYMMDD) + D/C + amount + type code + reference
        dc = "D" if direction == "debit" or amount < 0 else "C"
        # Transaction type identification code: N = non-SWIFT, TRF = transfer
        # Use NTRF for general transfers, NMSC for miscellaneous
        type_code = "NTRF"
        counterparty = _sanitize_text(row.get("counterparty", "") or "", 16)
        ref_text = counterparty or "NONREF"

        lines.append(
            f":61:{_mt940_date(tx_date)}{dc}{_mt940_amount(amount)}{type_code}//{ref_text}"
        )

        # :86: Information to Account Owner
        # Build description from available fields
        desc_parts = []
        cp = row.get("counterparty", "")
        if cp:
            desc_parts.append(cp)
        cp_iban = row.get("counterparty_iban") or row.get("counterparty_account") or ""
        if cp_iban:
            desc_parts.append(cp_iban)
        description = row.get("description", "")
        if description:
            desc_parts.append(description)

        desc_text = _sanitize_text(" ".join(desc_parts), 390)

        # :86: can be multi-line (max 6 lines of 65 chars)
        desc_lines = []
        while desc_text:
            desc_lines.append(desc_text[:65])
            desc_text = desc_text[65:]
        if not desc_lines:
            desc_lines = [""]

        lines.append(f":86:{desc_lines[0]}")
        for extra_line in desc_lines[1:]:
            lines.append(extra_line)

    # --- Trailer ---
    # :62F: Closing Balance
    close_dc = "D" if closing_bal < 0 else "C"
    lines.append(
        f":62F:{close_dc}{_mt940_date(last_date)}{currency}{_mt940_amount(closing_bal)}"
    )

    # :64: Closing Available Balance (same as closing for our purposes)
    lines.append(
        f":64:{close_dc}{_mt940_date(last_date)}{currency}{_mt940_amount(closing_bal)}"
    )

    # Wrap in SWIFT message envelope
    output = "\r\n".join(["{1:F01READRIGHTXXX0000000000}", "{4:", *lines, "-}"])
    return output
