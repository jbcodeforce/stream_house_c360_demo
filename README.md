# Streamhouse Customer 360


The purpose of this repo is to implement a Customer 360 streaming data pipeline integrating operational databases (PostgreSQL, DB2) with event streaming, Change Data Capture (CDC), and modern lakehouse analytical sinks.

## Infrastructure as code

### Pre-requisites

* Get Terraform
* Get aws CLI
* Login to AWS console, search for VPC to use
    ```sh
    aws sso login --profile your-profile-name
    export AWS_PROFILE="your-profile-name"
    ```

* Run: `terraform init` under IaC folder
* Find you public IP address: [https://checkip.amazonaws.com](https://checkip.amazonaws.com)
* Modify terraform environment variables `terraform.tfvars` with:

```sh
db_allowed_cidr_blocks
my_ip_addr
```

### RDS Postgresql

The database is created in a public subnet of an existing VPC but the security group use the user ip address to define a rule 

The database has three main tables:

* Accounts
* Customers
* Transactions

#### Local tests

Under scripts/local-tests there is a script to start postgresql locally so we can test thre python code. Here are the steps

* Start local postgresql server:
    ```sh
    # Be sure container is started
    container system start

    ./start_pg.sh 
    # it should download postgres container image
    ```

* Create the tables
    ```sh
    ./start_pg.sh --seed
    # OR
    uv run python create_tables.py --host localhost --port "${PG_PORT}" \
      --dbname "${PG_DB}" \
      --username "${PG_USER}" \
      --password "${PG_PASSWORD}" \
      --sslmode disable
    ```

* Verify data are in tables. Start a session in psql:
    ```sh
    container exec -it c360-postgres psql -U dbadmin -d c360db
    ```

    ```sql
    -- Row counts for all three tables
    SELECT 'customers'   AS tbl, COUNT(*) FROM customers
    UNION ALL
    SELECT 'accounts'    AS tbl, COUNT(*) FROM accounts
    UNION ALL
    SELECT 'transactions'AS tbl, COUNT(*) FROM transactions;

    -- Sample a customer with their accounts
    SELECT c.first_name, c.last_name, c.email, c.segment,
        a.account_type, a.balance
    FROM   customers c
    JOIN   accounts a USING (customer_id)
    LIMIT  10;

    -- Recent transactions
    SELECT t.transaction_type, t.amount, t.merchant_name,
        t.merchant_category, t.channel, t.status,
        t.transacted_at
    FROM   transactions t
    ORDER  BY t.transacted_at DESC
    LIMIT  10;

    -- Quit
    \q
    ```

* Wipe any existing container, start fresh, and seed
    ```sh
    ./scripts/local-tests/start_pg.sh --reset --seed
    ```

* Stop and remove the container
    ```sh
    ./scripts/local-tests/start_pg.sh --stop
    ```

#### Some info

* AWS RDS requires a DB Subnet Group. RDS manages its own Elastic Network Interfaces (ENIs). An RDS instance requires a DB Subnet Group defining a pool of subnets across at least two different Availability Zones (AZs) in that VPC (even for Single-AZ instances, so AWS can perform maintenance, failover, or migrations).
* The aws_db_instance resource only accepts db_subnet_group_name, not individual subnet IDs

#### Enhancement

* RDS should be in private subnet with VPC private link set to Confluent Cloud

### DB2 Community Edition EC2

### Private network

