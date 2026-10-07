# Flink Project Management
This section addresses the Flink pipeline definition and management using Confluent dbt.

## What is built

We want to implement the following pipeline to process the raw/bronze layer of records coming out of the CDC Debezium source kafka connector:

![](./diagrams/stream-flink.drawio.png)

To a silver layer which includes clean source data, with no duplicate, canonical model and filtered records. The golden layer includes star schema dimensions and facts. For this demonstration the c360-profile define analytics about a customer based on the buying transactions done so far.

The Confluent Cloud Flink statements organization reflects the medaillon and star schema struture.  The Flink statements run in a compute pool, already created via the terraform deployment.

1. First create the pipeline folder to manage the different Flink SQL statements, using the star schema pattern. This can be done manually or using tool from [flink-tools-for-agents](https://github.com/jbcodeforce/flink-tools-for-agents/tree/main/tools/dbt) project, which complement dbt confluent:
    ```sh
    git clone https://github.com/jbcodeforce/flink-tools-for-agents.git
    cd flink-tools-for-agents
    uv run sl-dbt init ~/Code/stream_house_c360_demo
    ```

    Here is the expected structure:
    ```sh
    stream_house_c360_demo
    ├── docs
    ├── IaC
    ├── local-tests
    ├── pipelines
    ├── sl_dbt.yaml
    └── tools
    ```

1. Add a data  analytic product named c360, to keep metrics for customer 360 view.
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
     uv run sl-dbt add-table ~/Code/stream_house_c360_demo src_customers c360 --table-type  src
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
        │           ├── src_customers.sql
        │           └── src_customers.yml
        ├── pyproject.toml
        ├── seeds
        └── tests
    ```

1. Use the Confluent Cloud Workspace to develop query by using an incremental approach: starts by selecting the different sources, to assess data quality. We will illustrate the approach for beginner.

    1. Assess the source table structure. Run the SQL: `show create table cdc.public.customers`
        ![](./images/flink/show_create_table.png)

        We can observe the Debezium envelop is already transformed to the sub schema. We can verify the schema definitiom in the schema registry has a before and after envelop. Go to schema registry page and select `cdc.public.customers-value`. The schema has Debezium fields of `before`, `after`, `op`...

        ![](./images/flink/schema_customer.png)

        The reason is the default format for the value and the key are set as:
        ```sh
        value.format' = 'avro-debezium-registry'
        ```

        We can do the same for: `show create table cdc.public.accounts;` and `show create table cdc.public.transactions;`

        We can also discover that by default the changelog mode is set to `retract`. The because the CDC debezium configuration may have not defined the primary key mapping. So we could not run snapshot queries but only streaming query.

    1. Look at some data and select only active customers:
        ```sql
        SELECT * FROM `j9r-env`.`j9r-kafka`.`cdc.public.customers` where status = 'ACTIVE';
        ```

    1. Because the changelog mode is retract or upsert, within the topic there will be no duplicate for the current customers, accounts and transactions, the following query helps to validate that
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

1. To be able connect to Confluent Cloud, we need to set the environment variables for API keys and secrets
    ```sh
    source ./scripts/set_env_from_tf.sh
    ```

1. `dbt` needs a model, declared within a yaml file, or when the source is already defined. This is the case for CDC topic. To get the schema from the raw topic we. can use a tool to automatically generate the `source.yaml` for dbt
    ```sh
    # under the project flink-tools-for-agents
    uv run  sl-dbt get-schema-existing-topic-to-dbt ~/Code/stream_house_c360_demo/ cdc.public.customers
    ```

    The schema imported looks like:

    ```yaml
     tables:
        - name: cdc.public.customers
            description: Topic 'cdc.public.customers' — registered via sl-dbt get-schema-existing-topic-to-dbt.
            columns:
            - name: customer_id
            data_type: string
            - name: first_name
            data_type: string
            - name: last_name
            data_type: string
            - name: email
            data_type: string
            - name: phone
            data_type: string
            - name: date_of_birth
            data_type: string
            - name: gender
            data_type: string
            - name: address_line1
            data_type: string
            - name: address_line2
            data_type: string
            - name: city
            data_type: string
            - name: state
            data_type: string
            - name: postal_code
            data_type: string
            - name: country
            data_type: string
            - name: customer_since
            data_type: int
            - name: segment
            data_type: string
            - name: status
            data_type: string
            - name: created_at
            data_type: string
            - name: updated_at
            data_type: string
      ````

      Do the same for accounts and transactions:
      ```sh
        # under the project flink-tools-for-agents
        uv run  sl-dbt get-schema-existing-topic-to-dbt ~/Code/stream_house_c360_demo/ cdc.public.accounts


        uv run  sl-dbt get-schema-existing-topic-to-dbt ~/Code/stream_house_c360_demo/ cdc.public.transactions
      ```
1. Be sure to modify your `.dbt/profiles.yml` with the following element.
    ```yaml
    j9r:
        outputs:
            dev:
            cloud_provider: '{{ env_var(''CC_PROVIDER'') }}'
            cloud_region: '{{ env_var('CC_REGION'') }}'
            compute_pool_id: '{{ env_var('FLINK_COMPUTE_POOL_ID'') }}'
            dbname: '{{ env_var('KAFKA_CLUSTER_NAME'') }}'
            environment_id: '{{ env_var('ENVIRONMENT_ID'') }}'
            execution_mode: streaming_query
            flink_api_key: '{{ env_var(''FLINK_API_KEY'') }}'
            flink_api_secret: '{{ env_var(''FLINK_API_SECRET'') }}'
            organization_id: 49c.....
            statement_label: dbt-confluent
            statement_name_prefix: dbt-
            threads: 4
            type: confluent
        target: dev
    ```

    This is a generic profile for Confluent Cloud  that uses environment variables for the default important values. Only the organization_id is set with a string (as of now)

1. Continue to create the following flink statements to filter and transform raw CDC topics, create dimension and fact for the analytics.

| Source Table(s) | Folder Name | Sink Table |
| ----------------| ------------ |-----------| 
| cdc.public.accounts | sources | src_accounts | 
| cdc.public.customers | sources | src_customers | 
| cdc.public.transactions| sources | src_transactions |
| src_dedup_customers, src_dedup_accounts | dimension |  dim_customers|
| dim_customers, src_transactions | facts | fct_c360_profiles |


## Undeploy

1. Undeploy and delete Flink create topics: as dbt does not support undeploying, we need another tool to manage the drop table. The [flink-tools-for-agents](https://github.com/jbcodeforce/flink-tools-for-agents/tree/main/tools/flink) includes tools to define a manifest of metadata and then perform deplooy, undeploy and drop tables. 
    ```sh
    cd flink-tools-for-agents 
    uv run generate-manifest --sql-dir ../stream_house_c360_demo/pipelines/models 
    ```

