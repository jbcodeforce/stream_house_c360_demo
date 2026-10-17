
-- After editing this query, you MUST run `dbt run --full-refresh` to deploy the change.
-- Schema-drift detection only checks columns, types, and WITH options — query logic
-- changes are not detected and will be silently skipped on a normal `dbt run`.
{{ config(
    materialized = 'streaming_table',
    distributed_by={'columns': ['transaction_id'], 'buckets': 1},
    with= {
        'changelog.mode': 'append',
        'kafka.cleanup-policy': 'delete',
        'scan.bounded.mode': 'unbounded',
        'scan.startup.mode': 'earliest-offset',
        'value.format': 'avro-registry'
    }
) }}

SELECT
  transaction_id, 
  account_id, 
  customer_id, 
  transaction_type, 
  amount, 
  currency,
  line_items,
  comments, 
  `status`, 
  CAST(REPLACE(REPLACE(transacted_at, 'T', ' '), 'Z', '') AS TIMESTAMP(6)) AS transacted_at
FROM TO_CHANGELOG(input => TABLE {{ source('cdc', 'transactions') }}, op => DESCRIPTOR(op))
WHERE op = 'INSERT' and status <> 'CANCELLED'
