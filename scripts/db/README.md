# C360 Database Scripts

Python scripts to bootstrap the Customer 360 PostgreSQL schema and populate it with synthetic data.
Managed as a [uv](https://docs.astral.sh/uv/) project — no global `pip install` or virtual-env juggling required.

## Scripts

| Script | Purpose |
|---|---|
| [`create_tables.py`](create_tables.py) | Creates `customers`, `accounts`, `transactions` tables, indexes, triggers, and a logical replication publication for CDC |
| [`seed_data.py`](seed_data.py) | Generates and inserts realistic synthetic records using [Faker](https://faker.readthedocs.io/) |

---

## Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/) installed (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- Access to the AWS account where the RDS instance lives (for Secrets Manager lookup)
- Network connectivity to the RDS endpoint (VPC / security group rules already set by Terraform)

---

## Setup

```bash
cd scripts/db
uv sync          # creates .venv and installs all dependencies from uv.lock
```

That's it — no `pip install`, no manual `venv` activation needed.

---

## Step 1 — Create the Schema

Credentials are fetched from AWS Secrets Manager. The secret ARN is emitted by Terraform as `secrets_manager_secret_arn`.

```bash
# Get the ARN from Terraform output
SECRET_ARN=$(terraform -chdir=IaC/AWS output -raw secrets_manager_secret_arn)

uv run python create_tables.py --secret-arn "$SECRET_ARN" --region us-west-2
```

What this creates:

| Object | Notes |
|---|---|
| `customers` | PII fields, segment, status; `updated_at` auto-trigger |
| `accounts` | Linked to `customers`; supports CHECKING / SAVINGS / CREDIT / LOAN |
| `transactions` | Linked to both `accounts` and `customers`; merchant enrichment fields |
| Indexes | FK indexes + `transacted_at DESC` for time-range queries |
| `c360_cdc_publication` | Logical replication publication covering all three tables — ready for Debezium / Confluent CDC connector |

---

## Step 2 — Seed Synthetic Data

```bash
# Default: 50 customers (~100 accounts, ~1 500 transactions)
uv run python seed_data.py --secret-arn "$SECRET_ARN" --region us-west-2

# Scale up
uv run python seed_data.py --secret-arn "$SECRET_ARN" --customers 500

# Wipe and re-seed
uv run python seed_data.py --secret-arn "$SECRET_ARN" --truncate --customers 100
```

### Volume guide

| `--customers` | Accounts (approx.) | Transactions (approx.) |
|---:|---:|---:|
| 50 | 100 | 1 500 |
| 500 | 1 000 | 15 000 |
| 5 000 | 10 000 | 150 000 |

---

## Local Development (no AWS)

Use `--password` to bypass Secrets Manager — useful when running against a local Docker PostgreSQL:

```bash
docker run -d --name c360-pg \
  -e POSTGRES_DB=c360db \
  -e POSTGRES_USER=dbadmin \
  -e POSTGRES_PASSWORD=localdevonly \
  -p 5432:5432 \
  postgres:17

uv run python create_tables.py \
  --host localhost --dbname c360db \
  --username dbadmin --password localdevonly \
  --sslmode disable

uv run python seed_data.py \
  --host localhost --dbname c360db \
  --username dbadmin --password localdevonly \
  --sslmode disable --customers 50
```

> **Security note**: Never use `--password` with production credentials. Use `--secret-arn` for all non-local environments.

---

## Dependency Management

Dependencies are declared in [`pyproject.toml`](pyproject.toml) and pinned in [`uv.lock`](uv.lock).

```bash
# Add a new dependency
uv add <package>

# Upgrade all dependencies to latest compatible versions
uv sync --upgrade
```

Always commit both `pyproject.toml` and `uv.lock` to keep the environment reproducible.

---

## Schema Reference

```
customers
  customer_id     UUID  PK
  first_name      VARCHAR(100)
  last_name       VARCHAR(100)
  email           VARCHAR(255) UNIQUE
  phone           VARCHAR(30)
  date_of_birth   DATE
  gender          VARCHAR(20)
  address_*       VARCHAR
  country         VARCHAR(60)
  customer_since  DATE
  segment         VARCHAR(50)   -- RETAIL | SMB | ENTERPRISE | PREMIUM | STUDENT
  status          VARCHAR(20)   -- ACTIVE | INACTIVE | SUSPENDED
  created_at / updated_at  TIMESTAMPTZ

accounts
  account_id      UUID  PK
  customer_id     UUID  FK → customers
  account_number  VARCHAR(20) UNIQUE
  account_type    VARCHAR(30)  -- CHECKING | SAVINGS | CREDIT | LOAN
  currency        CHAR(3)
  balance         NUMERIC(18,2)
  credit_limit    NUMERIC(18,2)
  opened_date / closed_date  DATE
  status          VARCHAR(20)  -- ACTIVE | CLOSED | FROZEN
  created_at / updated_at  TIMESTAMPTZ

transactions
  transaction_id   UUID  PK
  account_id       UUID  FK → accounts
  customer_id      UUID  FK → customers
  transaction_type VARCHAR(30)  -- DEBIT | CREDIT | TRANSFER | FEE | INTEREST
  amount           NUMERIC(18,2)
  currency         CHAR(3)
  description      VARCHAR(500)
  merchant_name    VARCHAR(200)
  merchant_category VARCHAR(100)
  channel          VARCHAR(50)  -- ONLINE | ATM | POS | MOBILE | BRANCH
  status           VARCHAR(20)  -- COMPLETED | PENDING | FAILED | REVERSED
  reference_id     VARCHAR(100)
  transacted_at / posted_at / created_at  TIMESTAMPTZ
```
