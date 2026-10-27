# Demonstration Script

Two approaches: 

1. Local database, writing directly to Kafka topics defined within Confluent Cloud Kafka Cluster
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

* Start the local webapp with remote RDS database
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
    export ENV_CONFIG_FILE=$PWD/apps/backend/.env

    ```

* Start the local webapp
    ```sh
    ./scripts/run_dev.sh 
    ```

* Deploy the Flink statement using dbt
    ```sh
    cd pipelines
    source .venv/bin/activate
    # verify the version, if need to update, change in pyproject.yaml in pipelines folder
    dbt version
    # Deploy the c360 data product from the fact table to the parents
    dbt run --select +c360.facts.fct_c360_profile
    ```

## Demonstration Scenarios

With the following setting in the environment variables, the backend service will create cdc topics, schemas, and send data to kafka directly:

```sh
SINK=postgres
KAFKA_PRODUCE_ENABLED=true
CDC_CONNECTOR_ENABLED=false
KAFKA_SEED_ON_START=true
```

For pure CDC processing, using a CDC connector in Confluent cloud:
```
CDC_CONNECTOR_ENABLED=true
KAFKA_SEED_ON_START=false
KAFKA_PRODUCE_ENABLED=false
```

### Use the Web App

[http://localhost:5173/](http://localhost:5173/)

### Customers Management

The customers page presents a list of existing customers loaded from the database:

![](./images/customers_page.png)

It is possible to add a new customer, with the Add customer button, or view, update, delete an existing customer using the action icon.

For example selecting the eye icon on James Carter brings this view:

![](./images/james_carter.png)

### Accounts Management

The accounts page presents a list of account per customer loaded from the database:

![](./images/web_accounts.png)


### Transaction Management

The transactions page presents a list of current transactions in the database:

![](./images/web_tx.png)

To simulate a week of activity, click **Generate 200 transactions**. The backend creates 200 transactions on existing accounts, timestamped from 7 days ago in 15-minute increments, inserts them in Postgres and (when `KAFKA_PRODUCE_ENABLED=true` and no CDC connector owns Kafka) publishes them to the `cdc.public.transactions` topic. The page then shows the newly created transactions (the list is not kept after a browser refresh).

### Data products view

This page demonstrates the presentation of metrics per data analytics as a product. 

* First select the analytics product
    ![](./images/select-data-product.png)

* See the pipeline graph for one of the fact table:
    ![](./images/statement_graph.png)

* User can drill down to see real-time metrics for on of the statement, and the deployed SQL query:
    ![](./images/statement_metrics.png)

    The message in and out should be greater than 0 when there is record flowing into the topics.

### Analytics Dashboard

The Analytics page is an illustration of what can be done by directly accessing data in kafka

1. Get the aggregation cross customers, by querying, via Flink snapshot query, the `fct_c360_profile` topic
    ![](./images/fct_c360_profile_db.png)

    The backend also returns the elapse time from a client point of view. Arrounf 6.60s to get results, using Flink Snapshot query.

1. Use the snapshot query, which creates a Flink Statement, to get a data for a user:
    ![](./images/snap_query_carter.png)
    Same as above the query now has where condition on the customer name.
    
1. We can do the same query with the new Real-time context engine, lightning query:
    ![]()

### Verify data are in Postgresql tables. 

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
