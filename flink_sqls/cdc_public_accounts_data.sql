-- Flink SQL: CDC seed data for cdc.public.accounts
--
-- Debezium-shaped "create" events (op = 'c', before = NULL), one per row of:
--   apps/backend/accounts/data/accounts.csv
-- Column layout matches the DDL in cdc_public_accounts.sql
-- (account_id, before, after, source, op, ts_ms, transaction).
-- opened_date / closed_date are INT days since 1970-01-01 (Avro date).
--
-- Run the DDL first: cdc_public_accounts.sql
-- ---------------------------------------------------------------------------

INSERT INTO cdc.public.accounts (
    `account_id`, `before`, `after`, `source`, `op`, `ts_ms`, `transaction`
) VALUES
    ('dd68e1d2-2ba4-5166-8fd3-402e7d687498',
     CAST(NULL AS ROW<`account_id` STRING, `customer_id` STRING, `account_number` STRING, `account_type` STRING, `currency` STRING, `balance` DECIMAL(18, 2), `credit_limit` DECIMAL(18, 2), `opened_date` INT, `closed_date` INT, `status` STRING, `created_at` STRING, `updated_at` STRING>),
     ROW(
        'dd68e1d2-2ba4-5166-8fd3-402e7d687498', '00000000-0000-0000-0000-ef1234567890', 'CHK-39958838', 'CHECKING',
        'USD', CAST(472.66 AS DECIMAL(18, 2)), CAST(NULL AS DECIMAL(18, 2)),
        17562, CAST(NULL AS INT), 'ACTIVE',
        '2018-01-31T00:00:00+00:00', '2018-01-31T00:00:00+00:00'
     ),
     ROW(
        '2.7.0.Final', 'postgresql', 'cdc', 1517356800000, 'false', 'c360', CAST(NULL AS STRING), 'public', 'accounts', CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)
     ),
     'c',
     1517356800000,
     CAST(NULL AS ROW<`id` STRING, `total_order` BIGINT, `data_collection_order` BIGINT>)
    ),

    ('27c2cc3a-75fd-5bce-b406-d0d49b5fa78e',
     CAST(NULL AS ROW<`account_id` STRING, `customer_id` STRING, `account_number` STRING, `account_type` STRING, `currency` STRING, `balance` DECIMAL(18, 2), `credit_limit` DECIMAL(18, 2), `opened_date` INT, `closed_date` INT, `status` STRING, `created_at` STRING, `updated_at` STRING>),
     ROW(
        '27c2cc3a-75fd-5bce-b406-d0d49b5fa78e', '00000000-0000-0000-0000-f01234567891', 'CHK-83197857', 'CHECKING',
        'USD', CAST(36955.33 AS DECIMAL(18, 2)), CAST(NULL AS DECIMAL(18, 2)),
        19207, CAST(NULL AS INT), 'ACTIVE',
        '2022-08-03T00:00:00+00:00', '2022-08-03T00:00:00+00:00'
     ),
     ROW(
        '2.7.0.Final', 'postgresql', 'cdc', 1659484800000, 'false', 'c360', CAST(NULL AS STRING), 'public', 'accounts', CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)
     ),
     'c',
     1659484800000,
     CAST(NULL AS ROW<`id` STRING, `total_order` BIGINT, `data_collection_order` BIGINT>)
    ),

    ('4b7603d6-8fb5-5726-a6e0-70dc39db1018',
     CAST(NULL AS ROW<`account_id` STRING, `customer_id` STRING, `account_number` STRING, `account_type` STRING, `currency` STRING, `balance` DECIMAL(18, 2), `credit_limit` DECIMAL(18, 2), `opened_date` INT, `closed_date` INT, `status` STRING, `created_at` STRING, `updated_at` STRING>),
     ROW(
        '4b7603d6-8fb5-5726-a6e0-70dc39db1018', '00000000-0000-0000-0000-012345678902', 'CHK-22575562', 'CHECKING',
        'USD', CAST(8898.34 AS DECIMAL(18, 2)), CAST(NULL AS DECIMAL(18, 2)),
        16566, CAST(NULL AS INT), 'ACTIVE',
        '2015-05-11T00:00:00+00:00', '2015-05-11T00:00:00+00:00'
     ),
     ROW(
        '2.7.0.Final', 'postgresql', 'cdc', 1431302400000, 'false', 'c360', CAST(NULL AS STRING), 'public', 'accounts', CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)
     ),
     'c',
     1431302400000,
     CAST(NULL AS ROW<`id` STRING, `total_order` BIGINT, `data_collection_order` BIGINT>)
    ),

    ('f7af72e5-6d7c-5ddd-ab19-fc51ae2019ba',
     CAST(NULL AS ROW<`account_id` STRING, `customer_id` STRING, `account_number` STRING, `account_type` STRING, `currency` STRING, `balance` DECIMAL(18, 2), `credit_limit` DECIMAL(18, 2), `opened_date` INT, `closed_date` INT, `status` STRING, `created_at` STRING, `updated_at` STRING>),
     ROW(
        'f7af72e5-6d7c-5ddd-ab19-fc51ae2019ba', '00000000-0000-0000-0000-123456789013', 'CHK-85329037', 'CHECKING',
        'USD', CAST(12016.71 AS DECIMAL(18, 2)), CAST(NULL AS DECIMAL(18, 2)),
        18901, CAST(NULL AS INT), 'ACTIVE',
        '2021-10-01T00:00:00+00:00', '2021-10-01T00:00:00+00:00'
     ),
     ROW(
        '2.7.0.Final', 'postgresql', 'cdc', 1633046400000, 'false', 'c360', CAST(NULL AS STRING), 'public', 'accounts', CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)
     ),
     'c',
     1633046400000,
     CAST(NULL AS ROW<`id` STRING, `total_order` BIGINT, `data_collection_order` BIGINT>)
    ),

    ('52bbe3ca-2d46-5760-825a-62c9adab2a3f',
     CAST(NULL AS ROW<`account_id` STRING, `customer_id` STRING, `account_number` STRING, `account_type` STRING, `currency` STRING, `balance` DECIMAL(18, 2), `credit_limit` DECIMAL(18, 2), `opened_date` INT, `closed_date` INT, `status` STRING, `created_at` STRING, `updated_at` STRING>),
     ROW(
        '52bbe3ca-2d46-5760-825a-62c9adab2a3f', '00000000-0000-0000-0000-234567890124', 'CHK-39587039', 'CHECKING',
        'USD', CAST(35942.97 AS DECIMAL(18, 2)), CAST(NULL AS DECIMAL(18, 2)),
        19308, 19852, 'INACTIVE',
        '2022-11-12T00:00:00+00:00', '2024-05-09T00:00:00+00:00'
     ),
     ROW(
        '2.7.0.Final', 'postgresql', 'cdc', 1715212800000, 'false', 'c360', CAST(NULL AS STRING), 'public', 'accounts', CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)
     ),
     'c',
     1715212800000,
     CAST(NULL AS ROW<`id` STRING, `total_order` BIGINT, `data_collection_order` BIGINT>)
    ),

    ('9f368a1a-db07-530c-8e8d-3ef51c409b33',
     CAST(NULL AS ROW<`account_id` STRING, `customer_id` STRING, `account_number` STRING, `account_type` STRING, `currency` STRING, `balance` DECIMAL(18, 2), `credit_limit` DECIMAL(18, 2), `opened_date` INT, `closed_date` INT, `status` STRING, `created_at` STRING, `updated_at` STRING>),
     ROW(
        '9f368a1a-db07-530c-8e8d-3ef51c409b33', '00000000-0000-0000-0000-345678901235', 'CHK-31429110', 'CHECKING',
        'USD', CAST(149370.09 AS DECIMAL(18, 2)), CAST(NULL AS DECIMAL(18, 2)),
        19751, CAST(NULL AS INT), 'ACTIVE',
        '2024-01-29T00:00:00+00:00', '2024-01-29T00:00:00+00:00'
     ),
     ROW(
        '2.7.0.Final', 'postgresql', 'cdc', 1706486400000, 'false', 'c360', CAST(NULL AS STRING), 'public', 'accounts', CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)
     ),
     'c',
     1706486400000,
     CAST(NULL AS ROW<`id` STRING, `total_order` BIGINT, `data_collection_order` BIGINT>)
    ),

    ('5099d512-172a-554c-ab3b-8afae6e10fab',
     CAST(NULL AS ROW<`account_id` STRING, `customer_id` STRING, `account_number` STRING, `account_type` STRING, `currency` STRING, `balance` DECIMAL(18, 2), `credit_limit` DECIMAL(18, 2), `opened_date` INT, `closed_date` INT, `status` STRING, `created_at` STRING, `updated_at` STRING>),
     ROW(
        '5099d512-172a-554c-ab3b-8afae6e10fab', '00000000-0000-0000-0000-456789012346', 'CHK-55176955', 'CHECKING',
        'USD', CAST(88361.38 AS DECIMAL(18, 2)), CAST(NULL AS DECIMAL(18, 2)),
        17072, CAST(NULL AS INT), 'ACTIVE',
        '2016-09-28T00:00:00+00:00', '2016-09-28T00:00:00+00:00'
     ),
     ROW(
        '2.7.0.Final', 'postgresql', 'cdc', 1475020800000, 'false', 'c360', CAST(NULL AS STRING), 'public', 'accounts', CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)
     ),
     'c',
     1475020800000,
     CAST(NULL AS ROW<`id` STRING, `total_order` BIGINT, `data_collection_order` BIGINT>)
    ),

    ('9747db1d-aa37-5c5b-a7b0-d5537a739c97',
     CAST(NULL AS ROW<`account_id` STRING, `customer_id` STRING, `account_number` STRING, `account_type` STRING, `currency` STRING, `balance` DECIMAL(18, 2), `credit_limit` DECIMAL(18, 2), `opened_date` INT, `closed_date` INT, `status` STRING, `created_at` STRING, `updated_at` STRING>),
     ROW(
        '9747db1d-aa37-5c5b-a7b0-d5537a739c97', '00000000-0000-0000-0000-567890123457', 'CHK-56164955', 'CHECKING',
        'USD', CAST(1481.91 AS DECIMAL(18, 2)), CAST(NULL AS DECIMAL(18, 2)),
        16832, CAST(NULL AS INT), 'ACTIVE',
        '2016-02-01T00:00:00+00:00', '2016-02-01T00:00:00+00:00'
     ),
     ROW(
        '2.7.0.Final', 'postgresql', 'cdc', 1454284800000, 'false', 'c360', CAST(NULL AS STRING), 'public', 'accounts', CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)
     ),
     'c',
     1454284800000,
     CAST(NULL AS ROW<`id` STRING, `total_order` BIGINT, `data_collection_order` BIGINT>)
    ),

    ('caaaacc5-6eba-56fd-9e05-dcaab3dad27b',
     CAST(NULL AS ROW<`account_id` STRING, `customer_id` STRING, `account_number` STRING, `account_type` STRING, `currency` STRING, `balance` DECIMAL(18, 2), `credit_limit` DECIMAL(18, 2), `opened_date` INT, `closed_date` INT, `status` STRING, `created_at` STRING, `updated_at` STRING>),
     ROW(
        'caaaacc5-6eba-56fd-9e05-dcaab3dad27b', '00000000-0000-0000-0000-678901234568', 'INV-71662963', 'INVESTMENT',
        'USD', CAST(16034.39 AS DECIMAL(18, 2)), CAST(NULL AS DECIMAL(18, 2)),
        16613, CAST(NULL AS INT), 'SUSPENDED',
        '2015-06-27T00:00:00+00:00', '2015-06-27T00:00:00+00:00'
     ),
     ROW(
        '2.7.0.Final', 'postgresql', 'cdc', 1435363200000, 'false', 'c360', CAST(NULL AS STRING), 'public', 'accounts', CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)
     ),
     'c',
     1435363200000,
     CAST(NULL AS ROW<`id` STRING, `total_order` BIGINT, `data_collection_order` BIGINT>)
    ),

    ('a9266258-06e7-5e89-ab34-c6ce5b2b8d73',
     CAST(NULL AS ROW<`account_id` STRING, `customer_id` STRING, `account_number` STRING, `account_type` STRING, `currency` STRING, `balance` DECIMAL(18, 2), `credit_limit` DECIMAL(18, 2), `opened_date` INT, `closed_date` INT, `status` STRING, `created_at` STRING, `updated_at` STRING>),
     ROW(
        'a9266258-06e7-5e89-ab34-c6ce5b2b8d73', '00000000-0000-0000-0000-789012345679', 'CHK-84093639', 'CHECKING',
        'USD', CAST(10171.77 AS DECIMAL(18, 2)), CAST(NULL AS DECIMAL(18, 2)),
        17986, CAST(NULL AS INT), 'ACTIVE',
        '2019-03-31T00:00:00+00:00', '2019-03-31T00:00:00+00:00'
     ),
     ROW(
        '2.7.0.Final', 'postgresql', 'cdc', 1553990400000, 'false', 'c360', CAST(NULL AS STRING), 'public', 'accounts', CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)
     ),
     'c',
     1553990400000,
     CAST(NULL AS ROW<`id` STRING, `total_order` BIGINT, `data_collection_order` BIGINT>)
    ),

    ('9d154b18-8f40-5e7a-8e59-b9c3e8de2614',
     CAST(NULL AS ROW<`account_id` STRING, `customer_id` STRING, `account_number` STRING, `account_type` STRING, `currency` STRING, `balance` DECIMAL(18, 2), `credit_limit` DECIMAL(18, 2), `opened_date` INT, `closed_date` INT, `status` STRING, `created_at` STRING, `updated_at` STRING>),
     ROW(
        '9d154b18-8f40-5e7a-8e59-b9c3e8de2614', '00000000-0000-0000-0000-789012345679', 'INV-84093640', 'INVESTMENT',
        'USD', CAST(100171.77 AS DECIMAL(18, 2)), CAST(NULL AS DECIMAL(18, 2)),
        17986, CAST(NULL AS INT), 'ACTIVE',
        '2019-03-31T00:00:00+00:00', '2019-03-31T00:00:00+00:00'
     ),
     ROW(
        '2.7.0.Final', 'postgresql', 'cdc', 1553990400000, 'false', 'c360', CAST(NULL AS STRING), 'public', 'accounts', CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT)
     ),
     'c',
     1553990400000,
     CAST(NULL AS ROW<`id` STRING, `total_order` BIGINT, `data_collection_order` BIGINT>)
    );
