"""CAMT.053 (ISO 20022) output formatter.

Takes a ContextEnvelope with bank transaction data and produces valid CAMT.053 XML
that Moneybird (and other accounting tools) will accept as a bank statement import.

CAMT.053 is the ISO 20022 Bank-to-Customer Statement (BkToCstmrStmt).
Key elements: Document > BkToCstmrStmt > Stmt > Bal (opening/closing) + Ntry (entries)
"""

from __future__ import annotations

import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom.minidom import parseString

from envelope.envelope import ContextEnvelope


NS = "urn:iso:std:iso:20022:tech:xsd:camt.053.001.02"


def _parse_date(raw: str) -> date:
    """Parse a date string from envelope data to a date object."""
    if not raw:
        return date.today()
    if len(raw) >= 10 and raw[4] == "-":
        return date(int(raw[:4]), int(raw[5:7]), int(raw[8:10]))
    if len(raw) == 8 and raw.isdigit():
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
    return date.today()


def _iso_date(d: date) -> str:
    """Format date as ISO 8601 for CAMT.053."""
    return d.isoformat()


def _iso_datetime(d: date) -> str:
    """Format date as ISO 8601 datetime for CAMT.053."""
    return datetime(d.year, d.month, d.day).isoformat()


def _get_account_iban(rows: list[dict[str, Any]]) -> str:
    """Extract the account IBAN from transaction rows."""
    for row in rows:
        iban = row.get("account_iban", "")
        if iban:
            return iban
    return "NL00UNKN0000000000"


def _compute_balances(rows: list[dict[str, Any]]) -> tuple[Decimal, Decimal, date, date]:
    """Compute opening balance, closing balance, first date, last date."""
    if not rows:
        today = date.today()
        return Decimal("0"), Decimal("0"), today, today

    first_date = _parse_date(rows[0].get("date", ""))
    last_date = _parse_date(rows[-1].get("date", ""))

    first_balance_after = rows[0].get("balance_after")
    first_amount = Decimal(rows[0].get("amount", "0"))

    if first_balance_after is not None:
        closing_raw = rows[-1].get("balance_after")
        if closing_raw is not None:
            closing = Decimal(closing_raw)
        else:
            closing = Decimal(first_balance_after)
            for row in rows[1:]:
                closing += Decimal(row.get("amount", "0"))
        opening = Decimal(first_balance_after) - first_amount
        return opening, closing, first_date, last_date

    opening = Decimal("0")
    closing = opening
    for row in rows:
        closing += Decimal(row.get("amount", "0"))
    return opening, closing, first_date, last_date


def _add_balance(parent: Element, bal_type: str, amount: Decimal, dt: date, currency: str):
    """Add a Bal element (opening or closing balance) to the statement."""
    bal = SubElement(parent, "Bal")
    tp = SubElement(bal, "Tp")
    cd_or_prtry = SubElement(tp, "CdOrPrtry")
    SubElement(cd_or_prtry, "Cd").text = bal_type  # OPBD or CLBD
    amt_el = SubElement(bal, "Amt", Ccy=currency)
    amt_el.text = f"{abs(amount):.2f}"
    SubElement(bal, "CdtDbtInd").text = "CRDT" if amount >= 0 else "DBIT"
    dt_el = SubElement(bal, "Dt")
    SubElement(dt_el, "Dt").text = _iso_date(dt)


def _add_entry(parent: Element, row: dict[str, Any], currency: str):
    """Add an Ntry (entry) element for a single transaction."""
    ntry = SubElement(parent, "Ntry")

    amount = Decimal(row.get("amount", "0"))
    direction = row.get("direction", "credit" if amount >= 0 else "debit")
    tx_date = _parse_date(row.get("date", ""))

    # Amount
    amt_el = SubElement(ntry, "Amt", Ccy=currency)
    amt_el.text = f"{abs(amount):.2f}"

    # Credit/Debit indicator
    SubElement(ntry, "CdtDbtInd").text = "DBIT" if direction == "debit" else "CRDT"

    # Status — booked
    SubElement(ntry, "Sts").text = "BOOK"

    # Booking date
    booking_dt = SubElement(ntry, "BookgDt")
    SubElement(booking_dt, "Dt").text = _iso_date(tx_date)

    # Value date
    val_dt = SubElement(ntry, "ValDt")
    SubElement(val_dt, "Dt").text = _iso_date(tx_date)

    # Entry details
    ntry_dtls = SubElement(ntry, "NtryDtls")
    tx_dtls = SubElement(ntry_dtls, "TxDtls")

    # Counterparty info
    counterparty = row.get("counterparty", "")
    cp_iban = row.get("counterparty_iban") or row.get("counterparty_account") or ""

    if counterparty or cp_iban:
        rltd_pties = SubElement(tx_dtls, "RltdPties")
        if direction == "debit":
            # We paid someone — they are the creditor
            cdtr = SubElement(rltd_pties, "Cdtr")
            if counterparty:
                SubElement(cdtr, "Nm").text = counterparty[:70]
            if cp_iban:
                cdtr_acct = SubElement(rltd_pties, "CdtrAcct")
                cdtr_id = SubElement(cdtr_acct, "Id")
                SubElement(cdtr_id, "IBAN").text = cp_iban
        else:
            # We received money — they are the debtor
            dbtr = SubElement(rltd_pties, "Dbtr")
            if counterparty:
                SubElement(dbtr, "Nm").text = counterparty[:70]
            if cp_iban:
                dbtr_acct = SubElement(rltd_pties, "DbtrAcct")
                dbtr_id = SubElement(dbtr_acct, "Id")
                SubElement(dbtr_id, "IBAN").text = cp_iban

    # Remittance information (description)
    description = row.get("description", "")
    if description:
        rmt_inf = SubElement(tx_dtls, "RmtInf")
        SubElement(rmt_inf, "Ustrd").text = description[:140]


def to_camt053(
    envelope: ContextEnvelope,
    account_iban: str | None = None,
    currency: str = "EUR",
) -> str:
    """Convert a ContextEnvelope with bank transactions to CAMT.053 XML.

    Args:
        envelope: ContextEnvelope with bank transaction data (normalized format).
        account_iban: Override account IBAN. Auto-detected from data if not provided.
        currency: Currency code (default EUR).

    Returns:
        Valid CAMT.053 XML string ready for import into Moneybird or similar tools.
    """
    rows = envelope.data
    if not rows:
        raise ValueError("No transaction data in envelope")

    iban = account_iban or _get_account_iban(rows)

    # Detect currency from data
    for row in rows:
        if row.get("currency"):
            currency = row["currency"]
            break

    opening_bal, closing_bal, first_date, last_date = _compute_balances(rows)
    msg_id = f"READRIGHT-{uuid.uuid4().hex[:12].upper()}"
    stmt_id = f"STMT-{first_date.strftime('%Y%m%d')}-{last_date.strftime('%Y%m%d')}"

    # Build XML tree
    root = Element("Document", xmlns=NS)
    bk_to_cstmr = SubElement(root, "BkToCstmrStmt")

    # Group Header
    grp_hdr = SubElement(bk_to_cstmr, "GrpHdr")
    SubElement(grp_hdr, "MsgId").text = msg_id
    SubElement(grp_hdr, "CreDtTm").text = datetime.now().isoformat(timespec="seconds")

    # Statement
    stmt = SubElement(bk_to_cstmr, "Stmt")
    SubElement(stmt, "Id").text = stmt_id

    # Account
    acct = SubElement(stmt, "Acct")
    acct_id = SubElement(acct, "Id")
    SubElement(acct_id, "IBAN").text = iban

    # Opening Balance
    _add_balance(stmt, "OPBD", opening_bal, first_date, currency)

    # Transaction entries
    for row in rows:
        _add_entry(stmt, row, currency)

    # Closing Balance
    _add_balance(stmt, "CLBD", closing_bal, last_date, currency)

    # Serialize to pretty XML
    raw_xml = tostring(root, encoding="unicode", xml_declaration=False)
    dom = parseString(raw_xml)
    pretty = dom.toprettyxml(indent="  ", encoding=None)

    # Remove the extra xml declaration that minidom adds (we'll add our own)
    lines = pretty.split("\n")
    if lines[0].startswith("<?xml"):
        lines = lines[1:]

    return '<?xml version="1.0" encoding="UTF-8"?>\n' + "\n".join(lines)
