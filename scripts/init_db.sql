-- ═══════════════════════════════════════════════════════════════════
-- OmniSight — Database Schema
-- ═══════════════════════════════════════════════════════════════════
-- Run once on a fresh database, or via docker-entrypoint-initdb.d/
-- PostgreSQL 15+
-- ═══════════════════════════════════════════════════════════════════

-- Schema
CREATE SCHEMA IF NOT EXISTS omnisight;

-- ── Master partitioned table ──────────────────────────────────────
CREATE TABLE IF NOT EXISTS omnisight.usdc_transfers (
    id                BIGSERIAL,
    block_number      BIGINT          NOT NULL,
    transaction_hash  VARCHAR(66)     NOT NULL,
    sender_address    VARCHAR(42)     NOT NULL,
    receiver_address  VARCHAR(42)     NOT NULL,
    raw_amount        NUMERIC(30, 0)  NOT NULL,
    adjusted_amount   NUMERIC(20, 6)  NOT NULL,
    amount_usd        NUMERIC(20, 6)  NOT NULL,   -- USDC is 1:1 USD
    ingested_at       TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    PRIMARY KEY (block_number, transaction_hash)
) PARTITION BY RANGE (block_number);

-- ── Partitions (add new ones as chain grows) ──────────────────────
-- Each partition covers 1M blocks (~2 weeks at 2 blocks/sec)
CREATE TABLE IF NOT EXISTS omnisight.usdc_transfers_era_47m
    PARTITION OF omnisight.usdc_transfers
    FOR VALUES FROM (47000000) TO (48000000);

CREATE TABLE IF NOT EXISTS omnisight.usdc_transfers_era_48m
    PARTITION OF omnisight.usdc_transfers
    FOR VALUES FROM (48000000) TO (49000000);

CREATE TABLE IF NOT EXISTS omnisight.usdc_transfers_era_49m
    PARTITION OF omnisight.usdc_transfers
    FOR VALUES FROM (49000000) TO (50000000);

CREATE TABLE IF NOT EXISTS omnisight.usdc_transfers_era_50m
    PARTITION OF omnisight.usdc_transfers
    FOR VALUES FROM (50000000) TO (51000000);

CREATE TABLE IF NOT EXISTS omnisight.usdc_transfers_era_51m
    PARTITION OF omnisight.usdc_transfers
    FOR VALUES FROM (51000000) TO (52000000);

-- Default partition catches any blocks outside the explicit ranges
CREATE TABLE IF NOT EXISTS omnisight.usdc_transfers_default
    PARTITION OF omnisight.usdc_transfers DEFAULT;

-- ── Indexes ───────────────────────────────────────────────────────
-- Whale alerts query: amount_usd DESC LIMIT N
CREATE INDEX IF NOT EXISTS idx_transfers_amount_usd
    ON omnisight.usdc_transfers (amount_usd DESC);

-- Wallet risk query: WHERE sender_address = $1 OR receiver_address = $1
CREATE INDEX IF NOT EXISTS idx_transfers_sender
    ON omnisight.usdc_transfers (sender_address);

CREATE INDEX IF NOT EXISTS idx_transfers_receiver
    ON omnisight.usdc_transfers (receiver_address);

-- Time-range queries for metrics endpoints
CREATE INDEX IF NOT EXISTS idx_transfers_ingested_at
    ON omnisight.usdc_transfers (ingested_at DESC);

-- ── Airflow connection setup reminder (not SQL) ───────────────────
-- After init, create the Airflow Connection via UI or CLI:
--
-- airflow connections add omnisight_postgres \
--   --conn-type postgres \
--   --conn-host localhost \
--   --conn-login omnisight_user \
--   --conn-password "$POSTGRES_PASSWORD" \
--   --conn-schema postgres \
--   --conn-port 5432
--
-- airflow variables set omnisight_node_url "$OMNISIGHT_NODE_URL"
-- airflow variables set omnisight_batch_size "500"
-- airflow connections add slack_webhook \
--   --conn-type http \
--   --conn-host "$SLACK_WEBHOOK_URL"
