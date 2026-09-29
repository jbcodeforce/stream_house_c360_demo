
-- After editing this query, you MUST run `dbt run --full-refresh` to deploy the change.
-- Schema-drift detection only checks columns, types, and WITH options — query logic
-- changes are not detected and will be silently skipped on a normal `dbt run`.
{{ config(
    materialized = 'streaming_table',
    with= {
        'changelog.mode': 'append',
        'connector': 'confluent',
        'kafka.cleanup-policy': 'delete',
        'scan.bounded.mode': 'unbounded',
        'scan.startup.mode': 'earliest-offset',
        'value.format': 'avro-registry'
    }
) }}
--- to modify!
SELECT 1;
