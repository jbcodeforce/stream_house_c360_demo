
{{ config(
    materialized='materialized_table',
    distributed_by= {'columns': ['customer_id'], 'buckets': 2},
    with={
        'changelog.mode': 'upsert',
        'key.format': 'avro-registry',
        'value.format': 'avro-registry',
        'kafka.retention.time': '0',
        'kafka.producer.compression.type': 'snappy',
        'scan.bounded.mode': 'unbounded',
        'scan.startup.mode': 'earliest-offset',
        'value.fields-include': 'all'
    },
     pre_hook=[
        "ALTER TABLE {{ source('cdc', 'customers') }} SET ( 'changelog.mode' = 'append')", 
        "ALTER TABLE {{ source('cdc', 'customers') }} SET ('value.format' = 'avro-registry')",
    ]
) }}
WITH mapped AS (
  SELECT
    customer_id,
    coalesce(IF(op= 'd', before.first_name, after.first_name), 'NULL') as first_name,
    coalesce(IF(op= 'd', before.last_name, after.last_name), 'NULL') as last_name,
    coalesce(IF(op= 'd', before.email, after.email), 'NULL') as email,
    coalesce(IF(op= 'd', before.phone, after.phone), 'NULL') as phone,
    coalesce(IF(op= 'd', before.date_of_birth, after.date_of_birth), 10) as date_of_birth,
    coalesce(IF(op= 'd', before.gender, after.gender), 'NULL') as gender,
    coalesce(IF(op= 'd', before.created_at, after.created_at), 'NULL') as registration_date,
    coalesce(IF(op= 'd', before.segment, after.segment), 'NULL')   as customer_segment,
    coalesce(IF(op= 'd', before.customer_since, after.customer_since), 10) as customer_since,
    coalesce(IF(op= 'd', before.city, after.city), 'NULL') as city,
    coalesce(IF(op= 'd', before.state, after.state), 'NULL') as state,
    coalesce(IF(op= 'd', before.postal_code, after.postal_code), 'NULL') as postal_code,
    coalesce(IF(op= 'd', before.country, after.country), 'NULL') as country,
    `$rowtime` as event_ts
    FROM {{ source('cdc', 'customers') }}
)
SELECT 
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    date_of_birth,
    gender,
    registration_date,
    customer_segment,
    customer_since,
    city,
    `state`,
    postal_code,
    country,
    event_ts
FROM mapped