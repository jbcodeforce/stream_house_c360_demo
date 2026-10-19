-- Flink SQL: CDC "after" envelope seed data for cdc.public.transactions
--
-- Seed data: "after" envelope (all INSERTs, using transacted_at as __timestamp).
-- These rows mirror the contents of:
--   apps/backend/transactions/data/transactions.csv
--
-- Avro schema: schema-cdc.public.transactions-value-v1.avsc
-- Envelope record structure:
--   before  — ROW with default/null values (no prior state for seed)
--   after   — ROW with actual data from CSV
--   source  — ROW with Debezium metadata (version, connector, name, ts_ms, snapshot, db, sequence, schema, table, txId, lsn, xmin)
--   op      — 'INSERT'
--   ts_ms   — epoch milliseconds derived from transacted_at
--
-- Run the DDL first: cdc_public_transactions.sql
-- ---------------------------------------------------------------------------

INSERT INTO cdc.public.transactions (
    transaction_id,
    before,
    after,
    source,
    op,
    ts_ms
) VALUES
    ('00000000-0000-0000-0000-ef1234567890',
     ROW(
        '00000000-0000-0000-0000-000000000000',
        '', '',
        0,
        'USD',
        NULL, NULL, NULL, NULL,
        'COMPLETED',
        '1970-01-01T00:00:00.000000Z', NULL
     ),
     ROW(
        '00000000-0000-0000-0000-ef1234567890',
        '', 'CHK-39958838',
        'DEBIT', 1600.37,
        'USD',
        'Credit transaction', 'Amazon', 'RETAIL', 'ONLINE',
        'COMPLETED',
        'REF-FBDEF36FA6',
        '2024-01-01T00:00:00+00:00', '2024-01-01T00:00:00+00:00',
        '1970-01-01T00:00:00.000000Z'
     ),
     ROW(
        '20240101',
        'io.debezium.connector.v2.postgresql',
        'cdc.public.transactions.connector',
        1704067200000,
        NULL,
        'Accounts', NULL, NULL,
        'transactions', NULL, NULL, NULL
     ),
     'INSERT',
     1704067200000
    ),

    ('00000000-0000-0000-0000-f01234567891',
     ROW(
        '00000000-0000-0000-0000-000000000000',
        '', '',
        0, 'USD',
        NULL, NULL, NULL, NULL,
        'COMPLETED',
        '1970-01-01T00:00:00.000000Z', NULL
     ),
     ROW(
        '00000000-0000-0000-0000-f01234567891',
        '', 'CHK-83197857',
        'DEBIT', 67.40,
        'USD',
        'Debit transaction', 'Starbucks', 'FOOD', 'ATM',
        'COMPLETED',
        'REF-E972116F11',
        '2024-01-02T01:00:00+00:00', '2024-01-02T01:00:00+00:00',
        '1970-01-01T00:00:00.000000Z'
     ),
     ROW(
        '20240102',
        'io.debezium.connector.v2.postgresql',
        'cdc.public.transactions.connector',
        1704153600000,
        NULL,
        'Accounts', NULL, NULL,
        'transactions', NULL, NULL, NULL
     ),
     'INSERT',
     1704153600000
    ),

    ('00000000-0000-0000-0000-012345678902',
     ROW(
        '00000000-0000-0000-0000-000000000000',
        '', '',
        0, 'USD',
        NULL, NULL, NULL, NULL,
        'COMPLETED',
        '1970-01-01T00:00:00.000000Z', NULL
     ),
     ROW(
        '00000000-0000-0000-0000-012345678902',
        '', 'CHK-22575562',
        'DEBIT', 691.20,
        'USD',
        'Transfer transaction', 'Shell', 'FUEL', 'POS',
        'COMPLETED',
        'REF-BA33F15D15',
        '2024-01-03T02:00:00+00:00', '2024-01-03T02:00:00+00:00',
        '1970-01-01T00:00:00.000000Z'
     ),
     ROW(
        '20240103',
        'io.debezium.connector.v2.postgresql',
        'cdc.public.transactions.connector',
        1704240000000,
        NULL,
        'Accounts', NULL, NULL,
        'transactions', NULL, NULL, NULL
     ),
     'INSERT',
     1704240000000
    ),

    ('00000000-0000-0000-0000-123456789013',
     ROW(
        '00000000-0000-0000-0000-000000000000',
        '', '',
        0, 'USD',
        NULL, NULL, NULL, NULL,
        'COMPLETED',
        '1970-01-01T00:00:00.000000Z', NULL
     ),
     ROW(
        '00000000-0000-0000-0000-123456789013',
        '', 'CHK-85329037',
        'DEBIT', 561.91,
        'USD',
        'Fee transaction', 'Netflix', 'ENTERTAINMENT', 'MOBILE',
        'COMPLETED',
        'REF-C5D93FF4FE',
        '2024-01-04T03:00:00+00:00', '2024-01-04T03:00:00+00:00',
        '1970-01-01T00:00:00.000000Z'
     ),
     ROW(
        '20240104',
        'io.debezium.connector.v2.postgresql',
        'cdc.public.transactions.connector',
        1704326400000,
        NULL,
        'Accounts', NULL, NULL,
        'transactions', NULL, NULL, NULL
     ),
     'INSERT',
     1704326400000
    ),

    ('00000000-0000-0000-0000-234567890124',
     ROW(
        '00000000-0000-0000-0000-000000000000',
        '', '',
        0, 'USD',
        NULL, NULL, NULL, NULL,
        'COMPLETED',
        '1970-01-01T00:00:00.000000Z', NULL
     ),
     ROW(
        '00000000-0000-0000-0000-234567890124',
        '', 'CHK-39587039',
        'DEBIT', 1842.50,
        'USD',
        'Interest transaction', 'Whole Foods', 'GROCERY', 'BRANCH',
        'COMPLETED',
        'REF-C70417F845',
        '2024-01-05T04:00:00+00:00', '2024-01-05T04:00:00+00:00',
        '1970-01-01T00:00:00.000000Z'
     ),
     ROW(
        '20240105',
        'io.debezium.connector.v2.postgresql',
        'cdc.public.transactions.connector',
        1704412800000,
        NULL,
        'Accounts', NULL, NULL,
        'transactions', NULL, NULL, NULL
     ),
     'INSERT',
     1704412800000
    ),

    ('00000000-0000-0000-0000-345678901235',
     ROW(
        '00000000-0000-0000-0000-000000000000',
        '', '',
        0, 'USD',
        NULL, NULL, NULL, NULL,
        'COMPLETED',
        '1970-01-01T00:00:00.000000Z', NULL
     ),
     ROW(
        '00000000-0000-0000-0000-345678901235',
        '', 'CHK-31429110',
        'DEBIT', 1693.37,
        'USD',
        'Credit transaction', NULL, NULL, 'ONLINE',
        'COMPLETED',
        'REF-1CF2C35713',
        '2024-01-06T05:00:00+00:00', '2024-01-06T05:00:00+00:00',
        '1970-01-01T00:00:00.000000Z'
     ),
     ROW(
        '20240106',
        'io.debezium.connector.v2.postgresql',
        'cdc.public.transactions.connector',
        1704501600000,
        NULL,
        'Accounts', NULL, NULL,
        'transactions', NULL, NULL, NULL
     ),
     'INSERT',
     1704501600000
    ),

    ('00000000-0000-0000-0000-456789012346',
     ROW(
        '00000000-0000-0000-0000-000000000000',
        '', '',
        0, 'USD',
        NULL, NULL, NULL, NULL,
        'COMPLETED',
        '1970-01-01T00:00:00.000000Z', NULL
     ),
     ROW(
        '00000000-0000-0000-0000-456789012346',
        '', 'CHK-55176955',
        'DEBIT', 2230.99,
        'USD',
        'Debit transaction', 'Uber', 'TRANSPORT', 'ATM',
        'COMPLETED',
        'REF-8D53DEA086',
        '2024-01-07T06:00:00+00:00', '2024-01-07T06:00:00+00:00',
        '1970-01-01T00:00:00.000000Z'
     ),
     ROW(
        '20240107',
        'io.debezium.connector.v2.postgresql',
        'cdc.public.transactions.connector',
        1704588000000,
        NULL,
        'Accounts', NULL, NULL,
        'transactions', NULL, NULL, NULL
     ),
     'INSERT',
     1704588000000
    ),

    ('00000000-0000-0000-0000-567890123457',
     ROW(
        '00000000-0000-0000-0000-000000000000',
        '', '',
        0, 'USD',
        NULL, NULL, NULL, NULL,
        'COMPLETED',
        '1970-01-01T00:00:00.000000Z', NULL
     ),
     ROW(
        '00000000-0000-0000-0000-567890123457',
        '', 'CHK-56164955',
        'DEBIT', 221.91,
        'USD',
        'Transfer transaction', 'Amazon', 'RETAIL', 'POS',
        'COMPLETED',
        'REF-D9ED1D631B',
        '2024-01-08T07:00:00+00:00', '2024-01-08T07:00:00+00:00',
        '1970-01-01T00:00:00.000000Z'
     ),
     ROW(
        '20240108',
        'io.debezium.connector.v2.postgresql',
        'cdc.public.transactions.connector',
        1704674400000,
        NULL,
        'Accounts', NULL, NULL,
        'transactions', NULL, NULL, NULL
     ),
     'INSERT',
     1704674400000
    ),

    ('00000000-0000-0000-0000-678901234568',
     ROW(
        '00000000-0000-0000-0000-000000000000',
        '', '',
        0, 'USD',
        NULL, NULL, NULL, NULL,
        'SUSPENDED',
        '1970-01-01T00:00:00.000000Z', NULL
     ),
     ROW(
        '00000000-0000-0000-0000-678901234568',
        '', 'INV-71662963',
        'DEBIT', 1057.69,
        'USD',
        'Fee transaction', 'Starbucks', 'FOOD', 'MOBILE',
        'ACTIVE',
        'REF-A7B5BF877A',
        '2024-01-09T08:00:00+00:00', '2024-01-09T08:00:00+00:00',
        '1970-01-01T00:00:00.000000Z'
     ),
     ROW(
        '20240109',
        'io.debezium.connector.v2.postgresql',
        'cdc.public.transactions.connector',
        1704760800000,
        NULL,
        'Accounts', NULL, NULL,
        'transactions', NULL, NULL, NULL
     ),
     'INSERT',
     1704760800000
    ),

    ('00000000-0000-0000-0000-789012345679',
     ROW(
        '00000000-0000-0000-0000-000000000000',
        '', '',
        0, 'USD',
        NULL, NULL, NULL, NULL,
        'COMPLETED',
        '1970-01-01T00:00:00.000000Z', NULL
     ),
     ROW(
        '00000000-0000-0000-0000-789012345679',
        '', 'CHK-84093639',
        'INTEREST', 79.34,
        'USD',
        'Interest transaction', 'Shell', 'FUEL', 'BRANCH',
        'COMPLETED',
        'REF-6B3234051A',
        '2024-01-10T09:00:00+00:00', '2024-01-10T09:00:00+00:00',
        '1970-01-01T00:00:00.000000Z'
     ),
     ROW(
        '20240110',
        'io.debezium.connector.v2.postgresql',
        'cdc.public.transactions.connector',
        1704852000000,
        NULL,
        'Accounts', NULL, NULL,
        'transactions', NULL, NULL, NULL
     ),
     'INSERT',
     1704852000000
    );
