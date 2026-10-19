-- Flink SQL: CDC "after" envelope for cdc.public.accounts
--
-- This file contains the DDL to create the CDC-projection table.
-- Schema matches the Avro Envelope record with nested ROW types.
--
-- Source: apps/backend/accounts/data/accounts.csv
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS cdc.public.accounts (
    -- Envelope fields (Debezium Envelope record)
    `account_id`      STRING,
    `before`: ROW<`account_id` STRING, `customer_id` STRING, `account_number` STRING, `account_type` STRING, `currency` STRING, `balance` DECIMAL(18, 2), `credit_limit` DECIMAL(18, 2), `opened_date` INT, `closed_date` INT, `status` STRING, `created_at` STRING, `updated_at` STRING>, 
    `after`: ROW<`account_id` STRING, `customer_id` STRING, `account_number` STRING, `account_type` STRING, `currency` STRING, `balance` DECIMAL(18, 2), `credit_limit` DECIMAL(18, 2), `opened_date` INT, `closed_date` INT, `status` STRING, `created_at` STRING, `updated_at` STRING>, 
    `source`: ROW<`version` STRING, `connector` STRING, `name` STRING, `ts_ms` BIGINT, `snapshot` STRING, `db` STRING, `sequence` STRING, `schema` STRING, `table` STRING, `txId` BIGINT, `lsn` BIGINT, `xmin` BIGINT>, 
    op: STRING, 
    ts_ms: BIGINT, 
    transaction: ROW<`id` STRING, `total_order` BIGINT, `data_collection_order` BIGINT>
);