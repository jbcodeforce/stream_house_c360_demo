#!/usr/bin/env python3
"""
create_tables.py
────────────────
Creates the Customer 360 schema (customers, accounts, transactions) on the
PostgreSQL RDS instance provisioned by the Terraform stack in IaC/AWS/.

Credentials are fetched from AWS Secrets Manager — no plain-text secrets.
Logical replication publication is created so Debezium / Confluent CDC
connectors can capture every change without extra setup.

Usage
-----
    # Fetch connection info from Secrets Manager (default)
    python create_tables.py --secret-arn <arn> [--region us-west-2]

    # Override individual connection params (useful for local testing)
    python create_tables.py --host localhost --port 5432 \
        --dbname c360db --username dbadmin --password <pw>
"""

import argparse
import logging
import sys

import psycopg2

from db_utils import add_db_arguments, build_conn_params, validate_db_args

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

DDL_STATEMENTS = [
    # ------------------------------------------------------------------
    # customers — source-of-truth for a person or organisation
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS customers (
        customer_id     UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
        first_name      VARCHAR(100)    NOT NULL,
        last_name       VARCHAR(100)    NOT NULL,
        email           VARCHAR(255)    NOT NULL UNIQUE,
        phone           VARCHAR(30),
        date_of_birth   TIMESTAMPTZ,
        gender          VARCHAR(20),
        address_line1   VARCHAR(255),
        address_line2   VARCHAR(255),
        city            VARCHAR(100),
        state           VARCHAR(100),
        postal_code     VARCHAR(20),
        country         VARCHAR(60)     NOT NULL DEFAULT 'US',
        customer_since  DATE            NOT NULL DEFAULT CURRENT_DATE,
        segment         VARCHAR(50),    -- e.g. 'RETAIL', 'SMB', 'ENTERPRISE'
        status          VARCHAR(20)     NOT NULL DEFAULT 'ACTIVE',
        created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
    )
    """,

    # ------------------------------------------------------------------
    # accounts — financial accounts owned by a customer
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS accounts (
        account_id      UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
        customer_id     UUID            NOT NULL REFERENCES customers(customer_id),
        account_number  VARCHAR(20)     NOT NULL UNIQUE,
        account_type    VARCHAR(30)     NOT NULL,  -- 'CHECKING', 'SAVINGS', 'CREDIT', 'LOAN'
        currency        CHAR(3)         NOT NULL DEFAULT 'USD',
        balance         NUMERIC(18, 2)  NOT NULL DEFAULT 0.00,
        credit_limit    NUMERIC(18, 2),            -- populated for CREDIT / LOAN accounts
        opened_date     DATE            NOT NULL DEFAULT CURRENT_DATE,
        closed_date     DATE,
        status          VARCHAR(20)     NOT NULL DEFAULT 'ACTIVE',
        created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW()
    )
    """,

    # ------------------------------------------------------------------
    # transactions — every debit / credit event on an account
    # ------------------------------------------------------------------
    """
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id   UUID           PRIMARY KEY DEFAULT gen_random_uuid(),
        account_id       UUID           NOT NULL REFERENCES accounts(account_id),
        customer_id      UUID           NOT NULL REFERENCES customers(customer_id),
        transaction_type VARCHAR(30)    NOT NULL,  -- 'DEBIT', 'CREDIT', 'TRANSFER', 'FEE', 'INTEREST'
        amount           NUMERIC(18, 2) NOT NULL,
        currency         CHAR(3)        NOT NULL DEFAULT 'USD',
        description      VARCHAR(500),
        merchant_name    VARCHAR(200),
        merchant_category VARCHAR(100),
        channel          VARCHAR(50),   -- 'ONLINE', 'ATM', 'POS', 'MOBILE', 'BRANCH'
        status           VARCHAR(20)    NOT NULL DEFAULT 'COMPLETED',
        reference_id     VARCHAR(100),  -- external reference / idempotency key
        transacted_at    TIMESTAMPTZ    NOT NULL DEFAULT NOW(),
        posted_at        TIMESTAMPTZ,
        created_at       TIMESTAMPTZ    NOT NULL DEFAULT NOW()
    )
    """,

    # ------------------------------------------------------------------
    # Indexes — support common query patterns and FK lookups
    # ------------------------------------------------------------------
    "CREATE INDEX IF NOT EXISTS idx_accounts_customer_id ON accounts (customer_id)",
    "CREATE INDEX IF NOT EXISTS idx_transactions_account_id ON transactions (account_id)",
    "CREATE INDEX IF NOT EXISTS idx_transactions_customer_id ON transactions (customer_id)",
    "CREATE INDEX IF NOT EXISTS idx_transactions_transacted_at ON transactions (transacted_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_customers_email ON customers (email)",

    # ------------------------------------------------------------------
    # updated_at auto-update trigger (customers + accounts)
    # ------------------------------------------------------------------
    """
    CREATE OR REPLACE FUNCTION set_updated_at()
    RETURNS TRIGGER LANGUAGE plpgsql AS $$
    BEGIN
        NEW.updated_at = NOW();
        RETURN NEW;
    END;
    $$
    """,
    """
    DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = 'trg_customers_updated_at'
        ) THEN
            CREATE TRIGGER trg_customers_updated_at
            BEFORE UPDATE ON customers
            FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        END IF;
    END $$
    """,
    """
    DO $$ BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger
            WHERE tgname = 'trg_accounts_updated_at'
        ) THEN
            CREATE TRIGGER trg_accounts_updated_at
            BEFORE UPDATE ON accounts
            FOR EACH ROW EXECUTE FUNCTION set_updated_at();
        END IF;
    END $$
    """,
]

# Logical replication publication for CDC (Debezium / Confluent connector)
CDC_PUBLICATION_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'c360_cdc_publication'
    ) THEN
        CREATE PUBLICATION c360_cdc_publication
        FOR TABLE customers, accounts, transactions;
    END IF;
END $$;
"""


# ---------------------------------------------------------------------------
# Schema creation helper
# ---------------------------------------------------------------------------

def create_schema(conn) -> None:
    """Execute DDL statements and create the CDC publication if they do not exist."""
    conn.autocommit = False
    with conn.cursor() as cur:
        for stmt in DDL_STATEMENTS:
            cur.execute(stmt)
            log.info("✓ Executed DDL statement")

        log.info("Creating CDC publication c360_cdc_publication …")
        cur.execute(CDC_PUBLICATION_SQL)

    conn.commit()
    log.info("✅ Schema created successfully (customers, accounts, transactions).")
    log.info("   CDC publication 'c360_cdc_publication' is ready for Debezium / Confluent Connect.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Create C360 schema on PostgreSQL RDS")
    add_db_arguments(parser)
    args = parser.parse_args()

    validate_db_args(parser, args)

    conn_params = build_conn_params(args)
    log.info("Connecting to %s:%s/%s as %s …",
             conn_params["host"], conn_params["port"],
             conn_params["dbname"], conn_params["user"])
    with psycopg2.connect(**conn_params) as conn:
        create_schema(conn)


if __name__ == "__main__":
    main()
