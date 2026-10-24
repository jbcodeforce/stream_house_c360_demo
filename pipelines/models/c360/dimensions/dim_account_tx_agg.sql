
{{ config(
    materialized = 'materialized_table',
    start_mode='FROM_BEGINNING', 
    distributed_by={'columns': ['account_id'], 'buckets': 1},
    with= {
        'changelog.mode': 'upsert',
        'kafka.cleanup-policy': 'compact',
        'scan.bounded.mode': 'unbounded',
        'scan.startup.mode': 'earliest-offset',
        'key.format': 'avro-registry',
        'value.format': 'avro-registry',
        'value.fields-include': 'all'
    })
 }}


SELECT
  a.account_id, 
  a.customer_id, 
  a.account_type, 
  a.status AS account_status, 
  a.balance,
  COALESCE(t.txn_count, 0) AS txn_count,
  COALESCE(t.txn_total, 0) AS txn_total,
  t.last_transacted_at
FROM {{ ref('src_accounts') }} a
LEFT JOIN (
  SELECT 
    account_id, 
    COUNT(*) AS txn_count, 
    SUM(amount) AS txn_total,
     MAX(transacted_at) AS last_transacted_at
  FROM {{ ref('dim_transactions') }}
  GROUP BY account_id
  ) t
ON a.account_id = t.account_id
WHERE a.account_type = 'CHECKING'