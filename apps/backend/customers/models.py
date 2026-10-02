from datetime import date, datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class CustomerCreate(BaseModel):
    first_name: str
    last_name: str
    email: str
    phone: str | None = None
    date_of_birth: datetime | None = None
    gender: str | None = None
    address_line1: str | None = None
    address_line2: str | None = None
    city: str | None = None
    state: str | None = None
    postal_code: str | None = None
    country: str = "US"
    customer_since: date = date.today()
    segment: str | None = None
    status: str = "ACTIVE"


class CustomerUpdate(BaseModel):
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    phone: str | None = None
    date_of_birth: datetime | None = None
    gender: str | None = None
    address_line1: str | None = None
    address_line2: str | None = None
    city: str | None = None
    state: str | None = None
    postal_code: str | None = None
    country: str | None = None
    customer_since: date | None = None
    segment: str | None = None
    status: str | None = None


class Customer(CustomerCreate):
    model_config = ConfigDict(from_attributes=True)

    customer_id: UUID
    created_at: datetime
    updated_at: datetime
