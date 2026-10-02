"""In-memory customer inventory backed by a CSV file."""

import csv
from pathlib import Path
from uuid import UUID

from customers.models import Customer

_CSV_PATH = Path(__file__).parent / "data" / "customers.csv"

_FIELDNAMES = [
    "customer_id",
    "first_name",
    "last_name",
    "email",
    "phone",
    "date_of_birth",
    "gender",
    "address_line1",
    "address_line2",
    "city",
    "state",
    "postal_code",
    "country",
    "customer_since",
    "segment",
    "status",
    "created_at",
    "updated_at",
]

_customers: list[Customer] = []


def _row_to_customer(row: dict) -> Customer:
    """Convert a CSV row dict to a Customer, treating empty strings as None."""
    cleaned = {k: (v if v != "" else None) for k, v in row.items()}
    return Customer.model_validate(cleaned)


def _customer_to_row(customer: Customer) -> dict:
    """Serialise a Customer to a CSV row dict."""
    return {
        "customer_id": str(customer.customer_id),
        "first_name": customer.first_name,
        "last_name": customer.last_name,
        "email": customer.email,
        "phone": customer.phone or "",
        "date_of_birth": customer.date_of_birth.isoformat() if customer.date_of_birth else "",
        "gender": customer.gender or "",
        "address_line1": customer.address_line1 or "",
        "address_line2": customer.address_line2 or "",
        "city": customer.city or "",
        "state": customer.state or "",
        "postal_code": customer.postal_code or "",
        "country": customer.country,
        "customer_since": customer.customer_since.isoformat(),
        "segment": customer.segment or "",
        "status": customer.status,
        "created_at": customer.created_at.isoformat(),
        "updated_at": customer.updated_at.isoformat(),
    }


def load() -> list[Customer]:
    """Read the CSV file into the in-memory cache and return it."""
    global _customers
    with _CSV_PATH.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        _customers = [_row_to_customer(row) for row in reader]
    return _customers


def list_all() -> list[Customer]:
    """Return all customers from the in-memory cache."""
    return _customers


def get_by_id(customer_id: UUID) -> Customer | None:
    """Return the customer with the given ID, or None if not found."""
    for customer in _customers:
        if customer.customer_id == customer_id:
            return customer
    return None


def create(customer: Customer) -> Customer:
    """Append a new customer to the in-memory cache and persist to CSV."""
    _customers.append(customer)
    with _CSV_PATH.open("a", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_FIELDNAMES)
        writer.writerow(_customer_to_row(customer))
    return customer


def update(customer: Customer) -> Customer:
    """Replace an existing customer in the cache and rewrite the CSV."""
    for i, existing in enumerate(_customers):
        if existing.customer_id == customer.customer_id:
            _customers[i] = customer
            break
    _rewrite_csv()
    return customer


def delete_by_id(customer_id: UUID) -> bool:
    """Remove a customer from the cache and rewrite the CSV. Returns True if found."""
    global _customers
    before = len(_customers)
    _customers = [c for c in _customers if c.customer_id != customer_id]
    if len(_customers) == before:
        return False
    _rewrite_csv()
    return True


def _rewrite_csv() -> None:
    """Rewrite the entire CSV from the current in-memory cache."""
    with _CSV_PATH.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_FIELDNAMES)
        writer.writeheader()
        for customer in _customers:
            writer.writerow(_customer_to_row(customer))
