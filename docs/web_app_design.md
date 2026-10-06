# Web App and Backend Design

This note is for explaining the code of the backend and vite application.




## Database schema

The database has three main tables:

* Accounts:
    ```sql
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
    ```

* Customers
    ```sql
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
    ```
    
* Transactions:
    ```sql
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
    ```

