-- PostgreSQL Migration Schema for WebSocket Backend
-- Optimized for 5.5M+ records with real-time analytics

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Main transfers table (optimized for analytics)
CREATE TABLE IF NOT EXISTS transfers (
    id BIGSERIAL PRIMARY KEY,
    packet_hash TEXT UNIQUE NOT NULL,
    sort_order TEXT,
    source_chain TEXT NOT NULL,
    dest_chain TEXT NOT NULL,
    source_name TEXT,
    dest_name TEXT,
    sender TEXT NOT NULL,
    receiver TEXT NOT NULL,
    amount NUMERIC,
    token_symbol TEXT,
    canonical_token_symbol TEXT,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Analytical indexes (CRITICAL for performance on 5.5M+ records)
-- BRIN index for timestamp (perfect for time-series data, much smaller than B-tree)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_timestamp_brin ON transfers USING BRIN (timestamp);

-- Route analysis indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_route ON transfers (source_chain, dest_chain);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_route_detail ON transfers (source_chain, dest_chain, source_name, dest_name);

-- Wallet analysis indexes  
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_sender_time ON transfers (sender, timestamp DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_receiver_time ON transfers (receiver, timestamp DESC);

-- Token analysis indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_token_time ON transfers (token_symbol, timestamp DESC) WHERE token_symbol IS NOT NULL;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_canonical_token_time ON transfers (canonical_token_symbol, timestamp DESC) WHERE canonical_token_symbol IS NOT NULL;

-- Partial indexes for recent data (much faster for dashboard queries)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_recent_1h ON transfers (timestamp DESC) WHERE timestamp > NOW() - INTERVAL '1 hour';
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_recent_1d ON transfers (timestamp DESC) WHERE timestamp > NOW() - INTERVAL '1 day';
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_recent_7d ON transfers (timestamp DESC) WHERE timestamp > NOW() - INTERVAL '7 days';
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_recent_30d ON transfers (timestamp DESC) WHERE timestamp > NOW() - INTERVAL '30 days';

-- Composite indexes for common query patterns
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_chain_timestamp ON transfers (source_chain, timestamp DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_transfers_dest_timestamp ON transfers (dest_chain, timestamp DESC);

-- =======================
-- MATERIALIZED VIEWS (GAME CHANGER)
-- =======================

-- 1. Transfer rates for all time periods (replaces expensive real-time counts)
CREATE MATERIALIZED VIEW IF NOT EXISTS transfer_rates AS
WITH rate_periods AS (
  SELECT 
    'all' as period,
    COUNT(*) as count,
    NOW() as calculated_at
  FROM transfers
  UNION ALL
  SELECT 
    '1m' as period,
    COUNT(*) as count,
    NOW() as calculated_at
  FROM transfers 
  WHERE timestamp > NOW() - INTERVAL '1 minute'
  UNION ALL
  SELECT 
    '1h' as period,
    COUNT(*) as count,
    NOW() as calculated_at
  FROM transfers 
  WHERE timestamp > NOW() - INTERVAL '1 hour'
  UNION ALL
  SELECT 
    '1d' as period,
    COUNT(*) as count,
    NOW() as calculated_at
  FROM transfers 
  WHERE timestamp > NOW() - INTERVAL '1 day'
  UNION ALL
  SELECT 
    '7d' as period,
    COUNT(*) as count,
    NOW() as calculated_at
  FROM transfers 
  WHERE timestamp > NOW() - INTERVAL '7 days'
  UNION ALL
  SELECT 
    '14d' as period,
    COUNT(*) as count,
    NOW() as calculated_at
  FROM transfers 
  WHERE timestamp > NOW() - INTERVAL '14 days'
  UNION ALL
  SELECT 
    '30d' as period,
    COUNT(*) as count,
    NOW() as calculated_at
  FROM transfers 
  WHERE timestamp > NOW() - INTERVAL '30 days'
)
SELECT period, count, calculated_at FROM rate_periods;

-- Index for fast lookups
CREATE UNIQUE INDEX IF NOT EXISTS idx_transfer_rates_period ON transfer_rates (period);

-- 2. Unique wallet counts (replaces expensive COUNT DISTINCT operations)
CREATE MATERIALIZED VIEW IF NOT EXISTS wallet_stats AS
WITH wallet_periods AS (
  SELECT 
    '1m' as period,
    COUNT(DISTINCT sender) as unique_senders,
    COUNT(DISTINCT receiver) as unique_receivers,
    NOW() as calculated_at
  FROM transfers 
  WHERE timestamp > NOW() - INTERVAL '1 minute'
  UNION ALL
  SELECT 
    '1h' as period,
    COUNT(DISTINCT sender) as unique_senders,
    COUNT(DISTINCT receiver) as unique_receivers,
    NOW() as calculated_at
  FROM transfers 
  WHERE timestamp > NOW() - INTERVAL '1 hour'
  UNION ALL
  SELECT 
    '1d' as period,
    COUNT(DISTINCT sender) as unique_senders,
    COUNT(DISTINCT receiver) as unique_receivers,
    NOW() as calculated_at
  FROM transfers 
  WHERE timestamp > NOW() - INTERVAL '1 day'
  UNION ALL
  SELECT 
    '7d' as period,
    COUNT(DISTINCT sender) as unique_senders,
    COUNT(DISTINCT receiver) as unique_receivers,
    NOW() as calculated_at
  FROM transfers 
  WHERE timestamp > NOW() - INTERVAL '7 days'
  UNION ALL
  SELECT 
    '14d' as period,
    COUNT(DISTINCT sender) as unique_senders,
    COUNT(DISTINCT receiver) as unique_receivers,
    NOW() as calculated_at
  FROM transfers 
  WHERE timestamp > NOW() - INTERVAL '14 days'
  UNION ALL
  SELECT 
    '30d' as period,
    COUNT(DISTINCT sender) as unique_senders,
    COUNT(DISTINCT receiver) as unique_receivers,
    NOW() as calculated_at
  FROM transfers 
  WHERE timestamp > NOW() - INTERVAL '30 days'
)
SELECT 
  period, 
  unique_senders, 
  unique_receivers,
  -- Calculate unique total with overlap estimation (much faster than UNION)
  GREATEST(unique_senders, unique_receivers) + 
  (LEAST(unique_senders, unique_receivers) * 0.3)::BIGINT as unique_total,
  calculated_at 
FROM wallet_periods;

-- Index for fast lookups
CREATE UNIQUE INDEX IF NOT EXISTS idx_wallet_stats_period ON wallet_stats (period);

-- 3. Popular routes (replaces expensive GROUP BY on millions of records)
CREATE MATERIALIZED VIEW IF NOT EXISTS popular_routes AS
SELECT 
  source_chain,
  dest_chain,
  source_name,
  dest_name,
  source_chain || '→' || dest_chain as route,
  COUNT(*) as transfer_count,
  MAX(timestamp) as last_activity,
  NOW() as calculated_at
FROM transfers 
WHERE timestamp > NOW() - INTERVAL '30 days'
GROUP BY source_chain, dest_chain, source_name, dest_name
ORDER BY transfer_count DESC
LIMIT 50;

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_popular_routes_count ON popular_routes (transfer_count DESC);

-- 4. Chain flows (replaces expensive dual GROUP BY queries)
CREATE MATERIALIZED VIEW IF NOT EXISTS chain_flows AS
WITH outgoing AS (
  SELECT 
    source_chain as chain_id,
    source_name as chain_name,
    COUNT(*) as outgoing_count
  FROM transfers 
  WHERE timestamp > NOW() - INTERVAL '30 days'
  GROUP BY source_chain, source_name
),
incoming AS (
  SELECT 
    dest_chain as chain_id,
    dest_name as chain_name,
    COUNT(*) as incoming_count
  FROM transfers 
  WHERE timestamp > NOW() - INTERVAL '30 days'
  GROUP BY dest_chain, dest_name
)
SELECT 
  COALESCE(o.chain_id, i.chain_id) as universal_chain_id,
  COALESCE(o.chain_name, i.chain_name) as chain_name,
  COALESCE(o.outgoing_count, 0) as outgoing_count,
  COALESCE(i.incoming_count, 0) as incoming_count,
  COALESCE(i.incoming_count, 0) - COALESCE(o.outgoing_count, 0) as net_flow,
  NOW() as last_activity,
  NOW() as calculated_at
FROM outgoing o
FULL OUTER JOIN incoming i ON o.chain_id = i.chain_id
ORDER BY (COALESCE(o.outgoing_count, 0) + COALESCE(i.incoming_count, 0)) DESC;

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_chain_flows_activity ON chain_flows (outgoing_count + incoming_count DESC);

-- 5. Asset volumes (replaces expensive token aggregations)
CREATE MATERIALIZED VIEW IF NOT EXISTS asset_volumes AS
SELECT 
  COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol) as asset_symbol,
  COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol) as asset_name,
  COUNT(*) as transfer_count,
  COALESCE(SUM(amount), 0) as total_volume,
  COALESCE(MAX(amount), 0) as largest_transfer,
  CASE 
    WHEN COUNT(*) > 0 THEN COALESCE(AVG(amount), 0)
    ELSE 0 
  END as average_amount,
  MAX(timestamp) as last_activity,
  NOW() as calculated_at
FROM transfers 
WHERE timestamp > NOW() - INTERVAL '30 days'
  AND COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol) IS NOT NULL
GROUP BY COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol)
ORDER BY transfer_count DESC
LIMIT 50;

-- Index for fast lookups
CREATE INDEX IF NOT EXISTS idx_asset_volumes_count ON asset_volumes (transfer_count DESC);

-- 6. Active senders (replaces expensive wallet GROUP BY)
CREATE MATERIALIZED VIEW IF NOT EXISTS active_senders AS
SELECT 
  sender as address,
  sender as display_address, -- Will be shortened in application
  COUNT(*) as count,
  MAX(timestamp) as last_activity,
  NOW() as calculated_at
FROM transfers 
WHERE timestamp > NOW() - INTERVAL '30 days'
GROUP BY sender
ORDER BY count DESC
LIMIT 50;

-- 7. Active receivers (replaces expensive wallet GROUP BY)
CREATE MATERIALIZED VIEW IF NOT EXISTS active_receivers AS
SELECT 
  receiver as address,
  receiver as display_address, -- Will be shortened in application
  COUNT(*) as count,
  MAX(timestamp) as last_activity,
  NOW() as calculated_at
FROM transfers 
WHERE timestamp > NOW() - INTERVAL '30 days'
GROUP BY receiver
ORDER BY count DESC
LIMIT 50;

-- =======================
-- SUPPORTING TABLES
-- =======================

-- Latency data (unchanged structure, but with PostgreSQL optimizations)
CREATE TABLE IF NOT EXISTS latency_data (
    id BIGSERIAL PRIMARY KEY,
    source_chain TEXT NOT NULL,
    dest_chain TEXT NOT NULL,
    source_name TEXT,
    dest_name TEXT,
    packet_ack_p5 REAL,
    packet_ack_median REAL,
    packet_ack_p95 REAL,
    packet_recv_p5 REAL,
    packet_recv_median REAL,
    packet_recv_p95 REAL,
    write_ack_p5 REAL,
    write_ack_median REAL,
    write_ack_p95 REAL,
    fetched_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(source_chain, dest_chain, fetched_at)
);

-- Indexes for latency data
CREATE INDEX IF NOT EXISTS idx_latency_route ON latency_data (source_chain, dest_chain);
CREATE INDEX IF NOT EXISTS idx_latency_fetched ON latency_data (fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_latency_latest ON latency_data (source_chain, dest_chain, fetched_at DESC);

-- Node health data (unchanged structure)
CREATE TABLE IF NOT EXISTS node_health (
    id BIGSERIAL PRIMARY KEY,
    chain_id TEXT NOT NULL,
    chain_name TEXT NOT NULL,
    rpc_url TEXT NOT NULL,
    rpc_type TEXT NOT NULL,
    status TEXT NOT NULL,
    response_time_ms INTEGER,
    latest_block_height BIGINT,
    error_message TEXT,
    uptime REAL,
    checked_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(rpc_url, checked_at)
);

-- Indexes for node health
CREATE INDEX IF NOT EXISTS idx_node_health_chain ON node_health (chain_id, checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_node_health_status ON node_health (status, checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_node_health_checked ON node_health (checked_at DESC);

-- =======================
-- MATERIALIZED VIEW REFRESH FUNCTIONS
-- =======================

-- Create refresh function for all materialized views
CREATE OR REPLACE FUNCTION refresh_analytics() RETURNS void AS $$
BEGIN
  -- Refresh all materialized views in parallel where possible
  REFRESH MATERIALIZED VIEW transfer_rates;
  REFRESH MATERIALIZED VIEW wallet_stats;
  REFRESH MATERIALIZED VIEW popular_routes;
  REFRESH MATERIALIZED VIEW chain_flows; 
  REFRESH MATERIALIZED VIEW asset_volumes;
  REFRESH MATERIALIZED VIEW active_senders;
  REFRESH MATERIALIZED VIEW active_receivers;
  
  -- Log the refresh
  RAISE NOTICE 'Analytics materialized views refreshed at %', NOW();
END;
$$ LANGUAGE plpgsql;

-- Create function to get refresh status
CREATE OR REPLACE FUNCTION get_analytics_status() RETURNS TABLE(
  view_name TEXT,
  last_updated TIMESTAMP WITH TIME ZONE,
  record_count BIGINT
) AS $$
BEGIN
  RETURN QUERY
  SELECT 'transfer_rates'::TEXT, calculated_at, count FROM transfer_rates WHERE period = 'all'
  UNION ALL
  SELECT 'wallet_stats'::TEXT, calculated_at, unique_total FROM wallet_stats WHERE period = '30d'
  UNION ALL  
  SELECT 'popular_routes'::TEXT, calculated_at, COUNT(*)::BIGINT FROM popular_routes GROUP BY calculated_at
  UNION ALL
  SELECT 'chain_flows'::TEXT, calculated_at, COUNT(*)::BIGINT FROM chain_flows GROUP BY calculated_at
  UNION ALL
  SELECT 'asset_volumes'::TEXT, calculated_at, COUNT(*)::BIGINT FROM asset_volumes GROUP BY calculated_at;
END;
$$ LANGUAGE plpgsql;

-- Schedule automatic refresh every 15 seconds (replaces the Go background job)
-- This requires pg_cron extension
SELECT cron.schedule('refresh-analytics', '*/15 * * * * *', 'SELECT refresh_analytics();');

-- =======================
-- MIGRATION HELPER FUNCTIONS
-- =======================

-- Function to estimate migration progress
CREATE OR REPLACE FUNCTION migration_progress() RETURNS TABLE(
  table_name TEXT,
  estimated_rows BIGINT,
  actual_rows BIGINT,
  progress_pct NUMERIC
) AS $$
BEGIN
  RETURN QUERY
  SELECT 
    'transfers'::TEXT,
    5500000::BIGINT as estimated_rows,
    COUNT(*)::BIGINT as actual_rows,
    (COUNT(*)::NUMERIC / 5500000 * 100)::NUMERIC(5,2) as progress_pct
  FROM transfers;
END;
$$ LANGUAGE plpgsql;

-- Initial refresh of materialized views (run after data migration)
SELECT refresh_analytics();

-- Analyze tables for better query planning
ANALYZE transfers;
ANALYZE latency_data;
ANALYZE node_health; 