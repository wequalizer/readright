"""Banking transaction schemas."""

from datetime import date
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field


class BankTransaction(BaseModel):
    """Normalized bank transaction — the universal output regardless of source bank."""

    date: date = Field(description="Transaction date")
    amount: Decimal = Field(description="Amount in source currency, negative = debit")
    currency: str = Field(default="EUR", description="ISO 4217 currency code")
    direction: Literal["debit", "credit"] = Field(description="Debit (money out) or credit (money in)")
    counterparty: str = Field(default="", description="Name of the other party")
    counterparty_iban: str = Field(default="", description="IBAN of the other party")
    description: str = Field(default="", description="Transaction description/memo")
    balance_after: Decimal | None = Field(default=None, description="Balance after this transaction")
    category: str = Field(default="", description="Auto-detected category")
    raw: dict = Field(default_factory=dict, description="Original fields before normalization")
