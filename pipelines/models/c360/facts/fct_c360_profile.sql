{{ config(
    materialized = 'materialized_table',
    distributed_by={'columns': ['customer_id'], 'buckets': 1},
   
    with= {
        'changelog.mode': 'upsert',
        'connector': 'confluent',
        'kafka.cleanup-policy': 'delete',
        'scan.bounded.mode': 'unbounded',
        'scan.startup.mode': 'earliest-offset',
        'value.format': 'avro-registry'
    }
) }}


SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
FROM {{ ref( 'dim_transactions') }} t
LEFT JOIN {{ref('dim_customers') }} c 
ON t.customer_id = c.customer_id