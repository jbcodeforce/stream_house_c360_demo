from datetime import date, datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class AccountCreate(BaseModel):
    customer_id: UUID
    account_number: str
    account_type: str
    currency: str = "USD"
    balance: float = 0.0
    credit_limit: float | None = None
    opened_date: date = date.today()
    closed_date: date | None = None
    status: str = "ACTIVE"


class AccountUpdate(BaseModel):
    customer_id: UUID | None = None
    account_number: str | None = None
    account_type: str | None = None
    currency: str | None = None
    balance: float | None = None
    credit_limit: float | None = None
    opened_date: date | None = None
    closed_date: date | None = None
    status: str | None = None


class Account(AccountCreate):
    model_config = ConfigDict(from_attributes=True)

    account_id: UUID
    created_at: datetime
    updated_at: datetime
