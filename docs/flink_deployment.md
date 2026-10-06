# Flink Project Management

We want to implement the following pipelines:

![](./diagrams/stream-flink.drawio.png)


1. First create the pipeline fodler to manage the different Flink SQL statement, using the star model. This can be done manually or use tool from [flink-tools-for-agents](https://github.com/jbcodeforce/flink-tools-for-agents/tree/main/tools/dbt)
    ```sh
    uv run sl-dbt init ~/Code/stream_house_c360_demo
    ```

    Here is the expected structure
    ```sh
    stream_house_c360_demo
    ├── docs
    ├── IaC
    ├── local-tests
    ├── pipelines
    ├── sl_dbt.yaml
    └── tools
    ```

1. Add Data product named c360
    ```sh
    uv run sl-dbt add-data-product ~/Code/stream_house_c360_demo c360
    ```

    Here is the expected structure
    ```sh
    pipelines
        ├── dbt_project.yml
        ├── macros
        ├── models
        │   └── c360
        │       ├── dimensions
        │       ├── facts
        │       └── sources
        ├── pyproject.toml
        ├── seeds
        └── tests
    ```

1. Add one table as output of deduplicating customers raw data

    ```sh
     uv run sl-dbt add-table ~/Code/stream_house_c360_demo src_dedup_customers c360 --table-type  src
    ```

    ```sh
    pipelines
        ├── dbt_project.yml
        ├── macros
        ├── models
        │   └── c360
        │       ├── dimensions
        │       ├── facts
        │       ├── schema.yml
        │       └── sources
        │           ├── src_dedup_customers.sql
        │           └── src_dedup_customers.yml
        ├── pyproject.toml
        ├── seeds
        └── tests
    ```

1. Use the Confluent Cloud Workspace to develop query by using an incremental approach. We will illustrate the approach for beginner.

    1. Assess the source table structure. Run the SQL: `show create table cdc.public.customers`
        ![](./images/flink/show_create_table.png)

        We can observe the Debezium envelop is already transformed to the sub schema. We can very the schema definitio in the schema repository has a before and after envelop. Go to schema registry page and select `cdc.public.customers-value` 

        ![](./images/flink/schema_customer.png)

        The reason is the default format for the value and the key are set as:
        ```sh
        value.format' = 'avro-debezium-registry'
        ```

        We can do the same for: `show create table cdc.public.accounts;` and `show create table cdc.public.transactions;`

        We can also discover that by default the changelog mode is set to `retract`. So we could not run snapshot queries but only streaming query.

    1. Look at some data and select only active customer
        ```sql
        SELECT * FROM `j9r-env`.`j9r-kafka`.`cdc.public.customers` where status = 'ACTIVE';
        ```
    1. Because the changelog mode is retract or upsert then the topic, there is no duplicate to the current customers, accounts and transactions, the following query helps to validate that
        ```sql
         with deduped_customer as (
            SELECT 
                    customer_id,
                FROM `cdc.public.customers`
            ),
            numdup as (SELECT
                customer_id,
                count(*) as num_records
            FROM deduped_customer
            group by customer_id)
            select * from numdup where num_records > 1
        ```

        ![](./images/flink/no_duplicate.png)

1. Get the schema from the raw topic to process and automatically generate the source.yaml for dbt
    ```sh
    uv run  sl-dbt get-schema-existing-topic-to-dbt ~/Code/stream_house_c360_demo cdc.public.customers
    ```

1. Continue to create the following flink statements to deduplicate, filter and transform raw CDC topics.

| Source Table(s) | Folder Name | Sink Table |
| ----------------| ------------ |-----------| 
| cdc.public.accounts | sources | src_accounts | 
| cdc.public.customers | sources | src_customers | 
| cdc.public.transactions| sources | src_transactions |
| src_dedup_customers, src_dedup_accounts | dimension |  dim_customers|
| dim_customers, src_transactions | facts | fct_c360_profiles |


1. Undeploy and delete Flink create topics: as dbt does not support undeploying, we need another tool to manage the drop table. The [flink-tools-for-agents](https://github.com/jbcodeforce/flink-tools-for-agents/tree/main/tools/flink) includes tools to define a manifest of metadata and then perform deplooy, undeploy and drop tables. 
    ```sh
    cd flink-tools-for-agents 
    uv run generate-manifest --sql-dir ../stream_house_c360_demo/pipelines/models 
    ```

