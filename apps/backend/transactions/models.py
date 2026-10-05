from datetime import datetime, timezone
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


class TransactionCreate(BaseModel):
    account_id: UUID
    customer_id: UUID
    transaction_type: str
    amount: float
    currency: str = "USD"
    description: str | None = None
    merchant_name: str | None = None
    merchant_category: str | None = None
    channel: str | None = None
    status: str = "COMPLETED"
    reference_id: str | None = None
    transacted_at: datetime = Field(default_factory=_now)
    posted_at: datetime | None = None


class Transaction(TransactionCreate):
    model_config = ConfigDict(from_attributes=True)

    transaction_id: UUID
    created_at: datetime
