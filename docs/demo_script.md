# Demonstration Script

Two approaches: 

1. local database, writing directly to Kafka topics defined within Confluent Cloud Kafka Cluster
2. Run Database on AWS, with Kafka Connector defined to get data from the database to Kafka topics. This approach has cost for the services on AWS.

The web application is able to write to remote database or work with local database and remote Kafka.

## Run with a remote database server

The architecture of the demonstration at the high-level looks like:

![](./diagrams/rds_execution.drawio.png)

Once the Confluent Cloud and AWS resources are created by Terraform, the user starts the local web application that connect to remote database server.

* Set environment variables to access Confluent Cloud resources, like Kafka, schema Key/secret and bootstrap address:
    ```sh
    source ./scripts/set_env_from_tf.sh
    ```

    or use the .env file to create environment variables in your current terminal session:
    ```sh
    set -a
    source apps/backend/.env
    set +a
    ```
* Set environment variables to the .env file
    ```sh
    export ENV_CONFIG_FILE=./apps/backend/.env
    ```

* Start the local webapp with remote database
    ```sh
    ./scripts/run_dev.sh --db rds
    ```

* [Follow with demonstration script](#demonstration-scenarios)

## Run with Local Database Get started locally

This time the database runs locally and the web app writes to kafka in parallel to write to database.

![](./diagrams/local_execution.drawio.png)

* Set environment variables to access Confluent Cloud resources, like Kafka, schema Key/secrets and bootstrap address. This can be done by looking terraform output:
    ```sh
    source ./scripts/set_env_from_tf.sh
    ```

    or using your own scripts to get environment variables preset, to an existing Confluent Cloud environment.
* Set where to find the specidic .env file. You can use different .env file and reference it via environment variable:
    ```sh
    export ENV_CONFIG_FILE=$PWD/apps/backend/.env.j9r
    ```

As an alternate when reusing an existing Confluent Cloud environment, set, via script the following environment variables:
```sh
export ENVIRONMENT_ID=
export CC_REGION=
export CC_PROVIDER=
export KAFKA_CLUSTER_NAME=
export KAFKA_API_KEY=
export KAFKA_API_SECRET=
export KAFKA_BOOTSTRAP_SERVERS=
export FLINK_COMPUTE_POOL_ID=
export SCHEMA_REGISTRY_KEY=
export SCHEMA_REGISTRY_SECRET=
echo "----------------"
export DB_PASSWORD=
export DBNAME=
export DATABASE_URL="postgresql://dbadmin:${DB_PASSWORD}@${RDSHOST}:${RDSPORT}/${DBNAME}"
```

* Start the local webapp
    ```sh
    ./scripts/run_dev.sh 
    ```


## Demonstration Scenarios
### Customers Management

The customers page presents a list of customers in the real database:

![](./images/customers_page.png)

It is possible to add a new customer, or update an existing one.

### Accounts Management

The accounts page presents a list of account per customer in the real database:

![](./images/web_accounts.png)


### Transaction Management

The transactions page presents a list of current transactions in the real database:

![](./images/web_tx.png)

To simulate a week of activity, click **Generate 200 transactions**. The backend creates 200 transactions on existing accounts, timestamped from 7 days ago in 15-minute increments, inserts them in Postgres and (when `KAFKA_PRODUCE_ENABLED=true` and no CDC connector owns Kafka) publishes them to the `cdc.public.transactions` topic. The page then shows the newly created transactions (the list is not kept after a browser refresh).




### Verify data are in tables. 

* Start a session in psql:
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
