CREATE DATABASE IF NOT EXISTS dune_project;

-- Bronze: parser audit/debug stream (short retention)
CREATE TABLE IF NOT EXISTS dune_project.bronze_raw_updates (
    chain LowCardinality(String),
    parser_version String,
    ingested_at_ms UInt64,
    update_type LowCardinality(String),
    slot UInt64,
    signature Nullable(String),
    created_at Nullable(String),
    parsed_ok UInt8,
    parsed_instructions UInt32,
    failed_instructions UInt32,
    dlmm_instruction_count UInt32,
    status Nullable(String),
    status_detail_json Nullable(String),
    payload_json String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(intDiv(ingested_at_ms, 1000)))
ORDER BY (slot, update_type, ingested_at_ms)
TTL toDateTime(intDiv(ingested_at_ms, 1000)) + INTERVAL 14 DAY;

-- Silver: canonical DLMM event fact table for API + CSV export
CREATE TABLE IF NOT EXISTS dune_project.silver_dlmm_events (
    chain LowCardinality(String),
    parser_version String,
    ingested_at_ms UInt64,
    block_time_ms Nullable(UInt64),
    slot UInt64,
    signature String,
    instruction_index UInt16,
    inner_index Int16, -- -1 when not inner
    is_inner UInt8,
    event_name LowCardinality(String),
    program_id String,
    discriminator Array(UInt8),
    parsed UInt8,
    parse_error Nullable(String),
    parse_warning Nullable(String),
    pool Nullable(String),
    user Nullable(String),
    amount_in_raw Nullable(UInt64),
    amount_in_mint Nullable(String),
    token_x_mint Nullable(String),
    token_y_mint Nullable(String),
    swap_for_y Nullable(UInt8),
    event_owner Nullable(String),
    fee_x_raw Nullable(UInt64),
    fee_y_raw Nullable(UInt64),
    args_json Nullable(String),
    idl_accounts_json Nullable(String),
    event_id String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(toDateTime(intDiv(ingested_at_ms, 1000)))
ORDER BY (ifNull(pool, ''), event_name, slot, signature, instruction_index, inner_index, event_id);

-- Gold: pool-level minute aggregates for overview/ranking APIs
CREATE TABLE IF NOT EXISTS dune_project.gold_pool_minute (
    minute_bucket Int64,
    pool String,
    swap_count UInt64,
    volume_raw UInt64,
    unique_users UInt64,
    min_slot UInt64,
    max_slot UInt64,
    last_ingested_unix_ms UInt64
)
ENGINE = ReplacingMergeTree(last_ingested_unix_ms)
PARTITION BY toYYYYMM(toDateTime(minute_bucket * 60))
ORDER BY (pool, minute_bucket);

-- Gold: pool+user hourly aggregates for leaderboard and user analytics
CREATE TABLE IF NOT EXISTS dune_project.gold_pool_user_hour (
    hour_bucket Int64,
    pool String,
    user String,
    swap_count UInt64,
    volume_raw UInt64,
    claim_events UInt64,
    fee_x_raw UInt64,
    fee_y_raw UInt64,
    max_slot UInt64,
    last_ingested_unix_ms UInt64
)
ENGINE = ReplacingMergeTree(last_ingested_unix_ms)
PARTITION BY toYYYYMM(toDateTime(hour_bucket * 3600))
ORDER BY (pool, user, hour_bucket);

-- Gold: parser and ingest quality surface
CREATE TABLE IF NOT EXISTS dune_project.gold_quality_minute (
    minute_bucket Int64,
    total_updates UInt64,
    dlmm_updates UInt64,
    parsed_instructions UInt64,
    failed_instructions UInt64,
    unknown_discriminator_count UInt64,
    last_slot UInt64,
    last_ingested_unix_ms UInt64
)
ENGINE = ReplacingMergeTree(last_ingested_unix_ms)
PARTITION BY toYYYYMM(toDateTime(minute_bucket * 60))
ORDER BY (minute_bucket);
