-- Main transfers table (already exists)
CREATE TABLE IF NOT EXISTS transfers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    packet_hash TEXT UNIQUE NOT NULL,
    source_chain TEXT NOT NULL,
    dest_chain TEXT NOT NULL,
    source_name TEXT,
    dest_name TEXT,
    sender TEXT NOT NULL,
    receiver TEXT NOT NULL,
    amount REAL,
    token_symbol TEXT,
    timestamp INTEGER NOT NULL,
    created_at INTEGER NOT NULL
);

-- Chart summary tables for pre-computed aggregations
CREATE TABLE IF NOT EXISTS chart_summaries (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chart_type TEXT NOT NULL,      -- 'popular_routes', 'chain_flows', 'asset_volumes', 'active_wallets'
    time_scale TEXT NOT NULL,      -- '1m', '1h', '1d', '7d', '14d', '30d'
    data_json TEXT NOT NULL,       -- Pre-computed JSON data
    updated_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    
    UNIQUE(chart_type, time_scale)
);



-- Latency data table for storing persistent latency statistics
CREATE TABLE IF NOT EXISTS latency_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_chain TEXT NOT NULL,           -- Universal chain ID
    dest_chain TEXT NOT NULL,             -- Universal chain ID  
    source_name TEXT,                     -- Display name
    dest_name TEXT,                       -- Display name
    packet_ack_p5 REAL,                   -- PacketAck P5 percentile (seconds)
    packet_ack_median REAL,               -- PacketAck median (seconds)
    packet_ack_p95 REAL,                  -- PacketAck P95 percentile (seconds)
    packet_recv_p5 REAL,                  -- PacketRecv P5 percentile (seconds)
    packet_recv_median REAL,              -- PacketRecv median (seconds)
    packet_recv_p95 REAL,                 -- PacketRecv P95 percentile (seconds)
    write_ack_p5 REAL,                    -- WriteAck P5 percentile (seconds)
    write_ack_median REAL,                -- WriteAck median (seconds)
    write_ack_p95 REAL,                   -- WriteAck P95 percentile (seconds)
    fetched_at INTEGER NOT NULL,          -- When this data was fetched (Unix timestamp)
    created_at INTEGER NOT NULL,          -- When record was created (Unix timestamp)
    
    UNIQUE(source_chain, dest_chain, fetched_at)  -- Prevent duplicates for same fetch time
);

-- Essential indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_transfers_timestamp ON transfers(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_transfers_route ON transfers(source_chain, dest_chain);
CREATE INDEX IF NOT EXISTS idx_transfers_token ON transfers(token_symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_transfers_sender ON transfers(sender, timestamp);
CREATE INDEX IF NOT EXISTS idx_transfers_receiver ON transfers(receiver, timestamp);
CREATE INDEX IF NOT EXISTS idx_transfers_chain_timestamp ON transfers(source_chain, timestamp);
CREATE INDEX IF NOT EXISTS idx_transfers_dest_timestamp ON transfers(dest_chain, timestamp);

-- Indexes for summary tables
CREATE INDEX IF NOT EXISTS idx_chart_summaries_type_scale ON chart_summaries(chart_type, time_scale);
CREATE INDEX IF NOT EXISTS idx_chart_summaries_updated ON chart_summaries(updated_at DESC);


-- Indexes for latency data
CREATE INDEX IF NOT EXISTS idx_latency_route ON latency_data(source_chain, dest_chain);
CREATE INDEX IF NOT EXISTS idx_latency_fetched ON latency_data(fetched_at DESC);
CREATE INDEX IF NOT EXISTS idx_latency_latest ON latency_data(source_chain, dest_chain, fetched_at DESC); 