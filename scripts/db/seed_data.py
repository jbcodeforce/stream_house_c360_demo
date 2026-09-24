#!/usr/bin/env python3
"""
seed_data.py
────────────
Generates and inserts realistic synthetic Customer 360 data into the
PostgreSQL RDS instance:

  • customers   — 50 records by default
  • accounts    — 1-3 accounts per customer (CHECKING / SAVINGS / CREDIT)
  • transactions — 5-30 transactions per account

All connections are made with TLS and credentials are sourced from AWS
Secrets Manager by default — no plain-text secrets in code or CLI history.

Usage
-----
    # Full seeding via Secrets Manager
    python seed_data.py --secret-arn <arn> [--region us-west-2] [--customers 100]

    # Truncate existing data before seeding
    python seed_data.py --secret-arn <arn> --truncate

    # Local dev with direct params
    python seed_data.py --host localhost --port 5432 \
        --dbname c360db --username dbadmin --password <pw> \
        --sslmode disable
"""

import argparse
import json
import logging
import os
import random
import sys
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import boto3
import psycopg2
import psycopg2.extras
from botocore.exceptions import ClientError
from faker import Faker

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

fake = Faker("en_US")
Faker.seed(42)
random.seed(42)

# ---------------------------------------------------------------------------
# Constants / lookup tables
# ---------------------------------------------------------------------------

ACCOUNT_TYPES = ["CHECKING", "SAVINGS", "CREDIT", "LOAN"]
ACCOUNT_TYPE_WEIGHTS = [0.45, 0.35, 0.15, 0.05]

CUSTOMER_SEGMENTS = ["RETAIL", "SMB", "ENTERPRISE", "PREMIUM", "STUDENT"]
CUSTOMER_STATUSES = ["ACTIVE", "INACTIVE", "SUSPENDED"]
CUSTOMER_STATUS_WEIGHTS = [0.88, 0.08, 0.04]

GENDERS = ["MALE", "FEMALE", "NON_BINARY", "PREFER_NOT_TO_SAY"]
GENDER_WEIGHTS = [0.47, 0.47, 0.03, 0.03]

TRANSACTION_TYPES = ["DEBIT", "CREDIT", "TRANSFER", "FEE", "INTEREST"]
TRANSACTION_TYPE_WEIGHTS = [0.55, 0.25, 0.10, 0.06, 0.04]

CHANNELS = ["ONLINE", "ATM", "POS", "MOBILE", "BRANCH"]
CHANNEL_WEIGHTS = [0.35, 0.15, 0.25, 0.20, 0.05]

TRANSACTION_STATUSES = ["COMPLETED", "PENDING", "FAILED", "REVERSED"]
TRANSACTION_STATUS_WEIGHTS = [0.88, 0.06, 0.04, 0.02]

MERCHANT_CATEGORIES = [
    "GROCERY", "RESTAURANT", "TRAVEL", "ENTERTAINMENT", "HEALTHCARE",
    "UTILITIES", "RETAIL", "FUEL", "EDUCATION", "SUBSCRIPTION",
    "REAL_ESTATE", "TRANSFER", "ATM_WITHDRAWAL",
]

MERCHANT_NAMES_BY_CATEGORY = {
    "GROCERY": ["Whole Foods", "Trader Joe's", "Safeway", "Kroger", "Costco"],
    "RESTAURANT": ["Chipotle", "McDonald's", "Starbucks", "Subway", "Panera Bread"],
    "TRAVEL": ["Delta Airlines", "United Airlines", "Marriott", "Airbnb", "Uber"],
    "ENTERTAINMENT": ["Netflix", "Spotify", "AMC Theatres", "Steam", "PlayStation Store"],
    "HEALTHCARE": ["CVS Pharmacy", "Walgreens", "Kaiser Permanente", "Quest Diagnostics"],
    "UTILITIES": ["PG&E", "AT&T", "Comcast", "Southern California Edison"],
    "RETAIL": ["Amazon", "Target", "Walmart", "Best Buy", "Apple Store"],
    "FUEL": ["Shell", "Chevron", "BP", "Exxon", "Arco"],
    "EDUCATION": ["Coursera", "Udemy", "Khan Academy", "LinkedIn Learning"],
    "SUBSCRIPTION": ["Adobe Creative Cloud", "Microsoft 365", "iCloud", "Google One"],
    "REAL_ESTATE": ["Zillow", "Redfin", "AvalonBay Communities"],
    "TRANSFER": ["Venmo", "Zelle", "PayPal", "Cash App", "Wire Transfer"],
    "ATM_WITHDRAWAL": ["ATM Withdrawal"],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def fetch_secret(secret_arn: str, region: str) -> dict:
    client = boto3.client("secretsmanager", region_name=region)
    try:
        resp = client.get_secret_value(SecretId=secret_arn)
    except ClientError as exc:
        log.error("Failed to retrieve secret %s: %s", secret_arn, exc)
        sys.exit(1)
    return json.loads(resp["SecretString"])


def build_conn_params(args: argparse.Namespace) -> dict:
    if args.secret_arn:
        log.info("Loading connection details from Secrets Manager: %s", args.secret_arn)
        secret = fetch_secret(args.secret_arn, args.region)
        params = {
            "host": secret["host"],
            "port": int(secret.get("port", 5432)),
            "dbname": secret["database"],
            "user": secret["username"],
            "password": secret["password"],
            "sslmode": "verify-full",
            "sslrootcert": args.ssl_root_cert,
        }
        return params
    params = {
        "host": args.host,
        "port": args.port,
        "dbname": args.dbname,
        "user": args.username,
        "password": args.password,
        "sslmode": args.sslmode,
    }
    if args.sslmode not in ("disable", "allow"):
        params["sslrootcert"] = args.ssl_root_cert
    
    print(params)
    return params


def weighted_choice(choices: list, weights: list):
    return random.choices(choices, weights=weights, k=1)[0]


def random_date_between(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def random_past_datetime(days_back: int = 730) -> datetime:
    now = datetime.now(tz=timezone.utc)
    offset = timedelta(seconds=random.randint(0, days_back * 86400))
    return now - offset


# ---------------------------------------------------------------------------
# Record generators
# ---------------------------------------------------------------------------

def generate_customer() -> dict:
    dob = fake.date_of_birth(minimum_age=18, maximum_age=80)
    since = random_date_between(date(2010, 1, 1), date.today())
    return {
        "customer_id": str(uuid.uuid4()),
        "first_name": fake.first_name(),
        "last_name": fake.last_name(),
        "email": fake.unique.email(),
        "phone": fake.phone_number()[:30],
        "date_of_birth": dob,
        "gender": weighted_choice(GENDERS, GENDER_WEIGHTS),
        "address_line1": fake.street_address(),
        "address_line2": fake.secondary_address() if random.random() < 0.3 else None,
        "city": fake.city(),
        "state": fake.state_abbr(),
        "postal_code": fake.postcode(),
        "country": "US",
        "customer_since": since,
        "segment": random.choice(CUSTOMER_SEGMENTS),
        "status": weighted_choice(CUSTOMER_STATUSES, CUSTOMER_STATUS_WEIGHTS),
        "created_at": datetime.combine(since, datetime.min.time(), tzinfo=timezone.utc),
        "updated_at": datetime.now(tz=timezone.utc),
    }


def generate_account(customer_id: str) -> dict:
    account_type = weighted_choice(ACCOUNT_TYPES, ACCOUNT_TYPE_WEIGHTS)
    opened = random_date_between(date(2012, 1, 1), date.today())

    balance = round(random.uniform(-500, 50_000), 2)
    credit_limit = None
    if account_type == "CREDIT":
        credit_limit = round(random.choice([1_000, 2_500, 5_000, 10_000, 25_000]), 2)
        balance = round(random.uniform(0, float(credit_limit) * 0.8), 2)
    elif account_type == "LOAN":
        credit_limit = round(random.choice([10_000, 25_000, 50_000, 100_000, 250_000]), 2)
        balance = round(random.uniform(float(credit_limit) * 0.2, float(credit_limit)), 2)

    account_number = fake.unique.numerify(text="############").zfill(14)

    return {
        "account_id": str(uuid.uuid4()),
        "customer_id": customer_id,
        "account_number": account_number,
        "account_type": account_type,
        "currency": "USD",
        "balance": Decimal(str(balance)),
        "credit_limit": Decimal(str(credit_limit)) if credit_limit is not None else None,
        "opened_date": opened,
        "closed_date": None,
        "status": weighted_choice(["ACTIVE", "CLOSED", "FROZEN"], [0.90, 0.07, 0.03]),
        "created_at": datetime.combine(opened, datetime.min.time(), tzinfo=timezone.utc),
        "updated_at": datetime.now(tz=timezone.utc),
    }


def generate_transaction(account_id: str, customer_id: str, account_type: str) -> dict:
    tx_type = weighted_choice(TRANSACTION_TYPES, TRANSACTION_TYPE_WEIGHTS)

    # Amount bands based on transaction type
    if tx_type == "FEE":
        amount = round(random.uniform(1, 35), 2)
    elif tx_type == "INTEREST":
        amount = round(random.uniform(0.01, 200), 2)
    elif tx_type == "TRANSFER":
        amount = round(random.uniform(10, 5_000), 2)
    elif account_type == "CREDIT":
        amount = round(random.uniform(1, 500), 2)
    else:
        amount = round(random.uniform(0.50, 3_000), 2)

    merchant_category = random.choice(MERCHANT_CATEGORIES)
    merchant_name = random.choice(MERCHANT_NAMES_BY_CATEGORY[merchant_category])

    transacted_at = random_past_datetime(days_back=730)
    # ~85% of transactions post same day, the rest within 3 days
    post_delay = timedelta(days=random.choice([0, 0, 0, 0, 0, 1, 2, 3]))
    posted_at = transacted_at + post_delay if random.random() < 0.92 else None

    return {
        "transaction_id": str(uuid.uuid4()),
        "account_id": account_id,
        "customer_id": customer_id,
        "transaction_type": tx_type,
        "amount": Decimal(str(amount)),
        "currency": "USD",
        "description": f"{tx_type.title()} — {merchant_name}",
        "merchant_name": merchant_name,
        "merchant_category": merchant_category,
        "channel": weighted_choice(CHANNELS, CHANNEL_WEIGHTS),
        "status": weighted_choice(TRANSACTION_STATUSES, TRANSACTION_STATUS_WEIGHTS),
        "reference_id": fake.uuid4(),
        "transacted_at": transacted_at,
        "posted_at": posted_at,
        "created_at": transacted_at,
    }


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------

INSERT_CUSTOMER = """
INSERT INTO customers (
    customer_id, first_name, last_name, email, phone, date_of_birth,
    gender, address_line1, address_line2, city, state, postal_code,
    country, customer_since, segment, status, created_at, updated_at
) VALUES (
    %(customer_id)s, %(first_name)s, %(last_name)s, %(email)s,
    %(phone)s, %(date_of_birth)s, %(gender)s, %(address_line1)s,
    %(address_line2)s, %(city)s, %(state)s, %(postal_code)s,
    %(country)s, %(customer_since)s, %(segment)s, %(status)s,
    %(created_at)s, %(updated_at)s
)
ON CONFLICT (customer_id) DO NOTHING
"""

INSERT_ACCOUNT = """
INSERT INTO accounts (
    account_id, customer_id, account_number, account_type, currency,
    balance, credit_limit, opened_date, closed_date, status,
    created_at, updated_at
) VALUES (
    %(account_id)s, %(customer_id)s, %(account_number)s, %(account_type)s,
    %(currency)s, %(balance)s, %(credit_limit)s, %(opened_date)s,
    %(closed_date)s, %(status)s, %(created_at)s, %(updated_at)s
)
ON CONFLICT (account_id) DO NOTHING
"""

INSERT_TRANSACTION = """
INSERT INTO transactions (
    transaction_id, account_id, customer_id, transaction_type, amount,
    currency, description, merchant_name, merchant_category, channel,
    status, reference_id, transacted_at, posted_at, created_at
) VALUES (
    %(transaction_id)s, %(account_id)s, %(customer_id)s, %(transaction_type)s,
    %(amount)s, %(currency)s, %(description)s, %(merchant_name)s,
    %(merchant_category)s, %(channel)s, %(status)s, %(reference_id)s,
    %(transacted_at)s, %(posted_at)s, %(created_at)s
)
ON CONFLICT (transaction_id) DO NOTHING
"""


def truncate_tables(cursor) -> None:
    log.warning("Truncating transactions, accounts, customers …")
    cursor.execute("TRUNCATE TABLE transactions, accounts, customers RESTART IDENTITY CASCADE")


def seed(conn, num_customers: int, truncate: bool) -> None:
    customers_inserted = 0
    accounts_inserted = 0
    transactions_inserted = 0

    with conn.cursor() as cur:
        if truncate:
            truncate_tables(cur)
            conn.commit()

        # ── customers ──────────────────────────────────────────────────
        log.info("Generating %d customers …", num_customers)
        customers = [generate_customer() for _ in range(num_customers)]
        psycopg2.extras.execute_batch(cur, INSERT_CUSTOMER, customers, page_size=100)
        customers_inserted = len(customers)

        # ── accounts + transactions ────────────────────────────────────
        log.info("Generating accounts and transactions …")
        all_accounts = []
        all_transactions = []

        for customer in customers:
            num_accounts = random.randint(1, 3)
            for _ in range(num_accounts):
                account = generate_account(customer["customer_id"])
                all_accounts.append(account)

                num_transactions = random.randint(5, 30)
                for _ in range(num_transactions):
                    tx = generate_transaction(
                        account["account_id"],
                        customer["customer_id"],
                        account["account_type"],
                    )
                    all_transactions.append(tx)

        psycopg2.extras.execute_batch(cur, INSERT_ACCOUNT, all_accounts, page_size=200)
        psycopg2.extras.execute_batch(cur, INSERT_TRANSACTION, all_transactions, page_size=500)
        accounts_inserted = len(all_accounts)
        transactions_inserted = len(all_transactions)

        conn.commit()

    log.info(
        "✅ Seed complete — %d customers | %d accounts | %d transactions",
        customers_inserted, accounts_inserted, transactions_inserted,
    )


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Seed C360 PostgreSQL schema with synthetic data")
    # Secrets Manager (preferred)
    parser.add_argument("--secret-arn", help="AWS Secrets Manager secret ARN (preferred)")
    parser.add_argument("--region", default="us-west-2", help="AWS region (default: us-west-2)")
    # Manual overrides
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="c360db")
    parser.add_argument("--username", default="dbadmin")
    parser.add_argument("--password", default=None)
    parser.add_argument("--sslmode", default="require")
    parser.add_argument(
        "--ssl-root-cert",
        default=os.path.expanduser("~/.ssh/global-bundle.pem"),
        help="Path to the CA bundle for SSL verification "
             "(default: ~/.ssh/global-bundle.pem)",
    )
    # Seeding options
    parser.add_argument("--customers", type=int, default=50,
                        help="Number of customer records to generate (default: 50)")
    parser.add_argument("--truncate", action="store_true",
                        help="Truncate existing data before seeding (use with caution)")
    args = parser.parse_args()

    if not args.secret_arn and args.password is None:
        parser.error("Provide --secret-arn (recommended) or --password for manual connection.")

    conn_params = build_conn_params(args)
    log.info("Connecting to %s:%s/%s as %s …",
             conn_params["host"], conn_params["port"],
             conn_params["dbname"], conn_params["user"])

    with psycopg2.connect(**conn_params) as conn:
        seed(conn, num_customers=args.customers, truncate=args.truncate)


if __name__ == "__main__":
    main()
