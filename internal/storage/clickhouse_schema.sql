-- ClickHouse Schema for WebSocket Backend Analytics
-- Optimized for time-series analytics on millions of blockchain transfers
-- Designed for high-performance aggregations and real-time queries

-- =======================
-- 0. DATABASE SETUP
-- =======================

-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS websocket_analytics;

-- =======================
-- 1. MAIN ANALYTICS TABLES
-- =======================

-- Primary analytics table for transfers (optimized for time-series queries)
CREATE TABLE IF NOT EXISTS transfers_analytics (
    id UInt64,
    packet_hash String,
    sort_order String,
    source_chain String,
    dest_chain String,
    source_name String,
    dest_name String,
    sender String,
    receiver String,
    amount Float64,
    token_symbol String,
    canonical_token_symbol String,
    timestamp DateTime('UTC'),
    created_at DateTime('UTC'),
    
    -- Additional computed fields for faster analytics
    route String MATERIALIZED concat(source_chain, '→', dest_chain),
    hour DateTime('UTC') MATERIALIZED toStartOfHour(timestamp),
    day Date MATERIALIZED toDate(timestamp),
    month Date MATERIALIZED toStartOfMonth(timestamp),
    
    -- Hash fields for efficient grouping
    route_hash UInt64 MATERIALIZED cityHash64(concat(source_chain, dest_chain)),
    sender_hash UInt64 MATERIALIZED cityHash64(sender),
    receiver_hash UInt64 MATERIALIZED cityHash64(receiver)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, source_chain, dest_chain)
TTL timestamp + INTERVAL 2 YEAR
SETTINGS index_granularity = 8192;

-- Latency analytics table (optimized for time-series monitoring)
CREATE TABLE IF NOT EXISTS latency_analytics (
    id UInt64,
    source_chain String,
    dest_chain String,
    source_name String,
    dest_name String,
    packet_ack_p5 Float32,
    packet_ack_median Float32,
    packet_ack_p95 Float32,
    packet_recv_p5 Float32,
    packet_recv_median Float32,
    packet_recv_p95 Float32,
    write_ack_p5 Float32,
    write_ack_median Float32,
    write_ack_p95 Float32,
    fetched_at DateTime('UTC'),
    created_at DateTime('UTC'),
    
    -- Computed fields
    route String MATERIALIZED concat(source_chain, '→', dest_chain),
    hour DateTime('UTC') MATERIALIZED toStartOfHour(fetched_at),
    day Date MATERIALIZED toDate(fetched_at)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(fetched_at)
ORDER BY (fetched_at, source_chain, dest_chain)
TTL fetched_at + INTERVAL 1 YEAR
SETTINGS index_granularity = 8192;

-- Node health analytics table (optimized for monitoring)
CREATE TABLE IF NOT EXISTS node_health_analytics (
    id UInt64,
    chain_id String,
    chain_name String,
    rpc_url String,
    rpc_type String,
    status String,
    response_time_ms UInt32,
    latest_block_height UInt64,
    error_message String,
    uptime Float32,
    checked_at DateTime('UTC'),
    created_at DateTime('UTC'),
    
    -- Computed fields
    hour DateTime('UTC') MATERIALIZED toStartOfHour(checked_at),
    day Date MATERIALIZED toDate(checked_at)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(checked_at)
ORDER BY (checked_at, chain_id, rpc_url)
TTL checked_at + INTERVAL 6 MONTH
SETTINGS index_granularity = 8192;

-- =======================
-- 2. TARGET TABLES FOR MATERIALIZED VIEWS
-- =======================

-- Target table for transfer rates
CREATE TABLE IF NOT EXISTS transfer_rates_analytics (
    minute DateTime('UTC'),
    hour DateTime('UTC'),
    day Date,
    transfers UInt64,
    unique_senders UInt64,
    unique_receivers UInt64,
    total_volume Float64,
    avg_amount Float64,
    max_amount Float64,
    calculated_at DateTime('UTC')
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, hour, minute)
TTL day + INTERVAL 1 YEAR;

-- Target table for route analytics
CREATE TABLE IF NOT EXISTS route_analytics (
    source_chain String,
    dest_chain String,
    source_name String,
    dest_name String,
    route String,
    day Date,
    transfer_count UInt64,
    total_volume Float64,
    avg_amount Float64,
    unique_senders UInt64,
    unique_receivers UInt64,
    calculated_at DateTime('UTC')
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, source_chain, dest_chain)
TTL day + INTERVAL 1 YEAR;

-- Target table for token analytics
CREATE TABLE IF NOT EXISTS token_analytics (
    token String,
    day Date,
    transfer_count UInt64,
    total_volume Float64,
    avg_amount Float64,
    max_amount Float64,
    unique_senders UInt64,
    unique_receivers UInt64,
    calculated_at DateTime('UTC')
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(day)
ORDER BY (day, token)
TTL day + INTERVAL 1 YEAR;

-- =======================
-- 3. MATERIALIZED VIEWS FOR REAL-TIME AGGREGATIONS
-- =======================

-- Real-time transfer rates (updates automatically)
CREATE MATERIALIZED VIEW IF NOT EXISTS transfer_rates_mv
TO transfer_rates_analytics
AS SELECT
    toStartOfMinute(timestamp) as minute,
    toStartOfHour(timestamp) as hour,
    toDate(timestamp) as day,
    count() as transfers,
    uniq(sender) as unique_senders,
    uniq(receiver) as unique_receivers,
    sum(amount) as total_volume,
    avg(amount) as avg_amount,
    max(amount) as max_amount,
    now() as calculated_at
FROM transfers_analytics
GROUP BY minute, hour, day;

-- Real-time route analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS route_analytics_mv
TO route_analytics
AS SELECT
    source_chain,
    dest_chain,
    source_name,
    dest_name,
    route,
    toDate(timestamp) as day,
    count() as transfer_count,
    sum(amount) as total_volume,
    avg(amount) as avg_amount,
    uniq(sender) as unique_senders,
    uniq(receiver) as unique_receivers,
    now() as calculated_at
FROM transfers_analytics
GROUP BY source_chain, dest_chain, source_name, dest_name, route, day;

-- Real-time token analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS token_analytics_mv
TO token_analytics
AS SELECT
    coalesce(canonical_token_symbol, token_symbol) as token,
    toDate(timestamp) as day,
    count() as transfer_count,
    sum(amount) as total_volume,
    avg(amount) as avg_amount,
    max(amount) as max_amount,
    uniq(sender) as unique_senders,
    uniq(receiver) as unique_receivers,
    now() as calculated_at
FROM transfers_analytics
WHERE token != ''
GROUP BY token, day;

-- =======================
-- 4. OPTIMIZED INDEXES
-- =======================

-- Secondary indexes for common query patterns
ALTER TABLE transfers_analytics ADD INDEX IF NOT EXISTS idx_source_chain (source_chain) TYPE bloom_filter GRANULARITY 1;
ALTER TABLE transfers_analytics ADD INDEX IF NOT EXISTS idx_dest_chain (dest_chain) TYPE bloom_filter GRANULARITY 1;
ALTER TABLE transfers_analytics ADD INDEX IF NOT EXISTS idx_token (token_symbol) TYPE bloom_filter GRANULARITY 1;
ALTER TABLE transfers_analytics ADD INDEX IF NOT EXISTS idx_canonical_token (canonical_token_symbol) TYPE bloom_filter GRANULARITY 1;
ALTER TABLE transfers_analytics ADD INDEX IF NOT EXISTS idx_sender (sender) TYPE bloom_filter GRANULARITY 1;
ALTER TABLE transfers_analytics ADD INDEX IF NOT EXISTS idx_receiver (receiver) TYPE bloom_filter GRANULARITY 1;

-- =======================
-- 5. HELPER FUNCTIONS (if needed)
-- =======================

-- Note: ClickHouse doesn't support stored procedures like PostgreSQL,
-- but we can create these as prepared statements in the Go application 