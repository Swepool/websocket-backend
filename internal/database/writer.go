package database

import (
	"context"
	"database/sql"
	"fmt"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/internal/utils"
	"websocket-backend-new/models"
	
	_ "github.com/mattn/go-sqlite3"
)

// Config holds database configuration
type Config struct {
	DatabasePath string `json:"databasePath"` // Path to SQLite database file
	BatchSize    int    `json:"batchSize"`    // Batch size for bulk inserts
}

// DefaultConfig returns default database configuration
func DefaultConfig() Config {
	return Config{
		DatabasePath: "./websocket_backend.db",
		BatchSize:    100,
	}
}

// Writer handles writing transfers to SQLite database
type Writer struct {
	config     Config
	channels   *channels.Channels
	db         *sql.DB
	insertStmt *sql.Stmt
}

// NewWriter creates a new database writer
func NewWriter(config Config, channels *channels.Channels) (*Writer, error) {
	// Open SQLite database
	db, err := sql.Open("sqlite3", config.DatabasePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	
	// Configure SQLite for better performance
	if err := configureSQLite(db); err != nil {
		return nil, fmt.Errorf("failed to configure SQLite: %w", err)
	}
	
	// Create writer instance
	writer := &Writer{
		config:   config,
		channels: channels,
		db:       db,
	}
	
	// Initialize enhanced schema with summary tables
	if err := writer.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}
	
	// Prepare insert statement
	insertStmt, err := db.Prepare(`
		INSERT OR IGNORE INTO transfers (
			packet_hash, sort_order, source_chain, dest_chain, source_name, dest_name,
			sender, receiver, amount, token_symbol, canonical_token_symbol, timestamp, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return nil, fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	
	writer.insertStmt = insertStmt
	
	return writer, nil
}

// Start begins the database writer thread
func (w *Writer) Start(ctx context.Context) {
	utils.LogInfo("DB_WRITER", "Starting database writer")
	
	for {
		select {
		case <-ctx.Done():
			utils.LogInfo("DB_WRITER", "Stopping database writer")
			return
			
		case transfers := <-w.channels.DatabaseSaves:
			w.saveTransferBatch(transfers)
		}
	}
}

// saveTransferBatch saves a batch of raw transfers to database
func (w *Writer) saveTransferBatch(transfers []models.Transfer) {
	if len(transfers) == 0 {
		return
	}
	
	successCount := 0
	for _, transfer := range transfers {
		_, err := w.insertStmt.Exec(
			transfer.PacketHash,
			transfer.SortOrder,
			transfer.SourceChain.UniversalChainID,
			transfer.DestinationChain.UniversalChainID,
			transfer.SourceChain.DisplayName,
			transfer.DestinationChain.DisplayName,
			transfer.SenderCanonical,
			transfer.ReceiverCanonical,
			transfer.BaseAmount,
			transfer.BaseTokenSymbol,
			transfer.CanonicalTokenSymbol,
			transfer.TransferSendTimestamp.Unix(),
			transfer.TransferSendTimestamp.Unix(), // created_at
		)
		
		if err != nil {
			utils.LogError("DB_WRITER", "Failed to save transfer %s: %v", transfer.PacketHash, err)
		} else {
			successCount++
		}
	}
	
	utils.LogInfo("DB_WRITER", "Saved %d/%d transfers to database", successCount, len(transfers))
}

// GetDB returns the database connection for chart service
func (w *Writer) GetDB() *sql.DB {
	return w.db
}

// Close closes the database connection
func (w *Writer) Close() error {
	if w.insertStmt != nil {
		w.insertStmt.Close()
	}
	if w.db != nil {
		return w.db.Close()
	}
	return nil
}

// configureSQLite configures SQLite for optimal performance with large datasets
func configureSQLite(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL",        // WAL mode for better concurrency
		"PRAGMA synchronous=NORMAL",      // Faster writes, still safe
		"PRAGMA cache_size=50000",        // 50MB cache (increased for 175M+ records)
		"PRAGMA temp_store=memory",       // Keep temp tables in memory
		"PRAGMA mmap_size=1073741824",    // 1GB memory map (increased for large datasets)
		"PRAGMA page_size=4096",          // Optimize page size for better I/O
		"PRAGMA auto_vacuum=INCREMENTAL", // Prevent file bloat with large datasets
		"PRAGMA wal_autocheckpoint=10000", // Less frequent checkpoints for better write performance
	}
	
	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			return fmt.Errorf("failed to execute %s: %w", pragma, err)
		}
	}
	
	return nil
}

// createTransfersTable creates the transfers table if it doesn't exist
func createTransfersTable(db *sql.DB) error {
	createTableSQL := `
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
	
	-- Essential indexes for chart queries
	CREATE INDEX IF NOT EXISTS idx_timestamp ON transfers(timestamp DESC);
	CREATE INDEX IF NOT EXISTS idx_route ON transfers(source_chain, dest_chain);
	CREATE INDEX IF NOT EXISTS idx_token ON transfers(token_symbol, timestamp);
	CREATE INDEX IF NOT EXISTS idx_sender ON transfers(sender, timestamp);
	CREATE INDEX IF NOT EXISTS idx_receiver ON transfers(receiver, timestamp);
	`
	
	_, err := db.Exec(createTableSQL)
	return err
}

// initSchema creates the database tables if they don't exist
func (w *Writer) initSchema() error {
	schema := `
-- Main transfers table (already exists)
CREATE TABLE IF NOT EXISTS transfers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    packet_hash TEXT UNIQUE NOT NULL,
    sort_order TEXT,
    source_chain TEXT NOT NULL,
    dest_chain TEXT NOT NULL,
    source_name TEXT,
    dest_name TEXT,
    sender TEXT NOT NULL,
    receiver TEXT NOT NULL,
    amount REAL,
    token_symbol TEXT,
    canonical_token_symbol TEXT,
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
CREATE INDEX IF NOT EXISTS idx_transfers_canonical_token ON transfers(canonical_token_symbol, timestamp);
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
`

	_, err := w.db.Exec(schema)
	if err != nil {
		return fmt.Errorf("failed to create schema: %w", err)
	}

	// Run migration for canonical_token_symbol column
	if err := w.migrateCanonicalTokenSymbol(); err != nil {
		return fmt.Errorf("failed to migrate canonical_token_symbol: %w", err)
	}

	utils.LogInfo("DB_WRITER", "Enhanced database schema created successfully")
	return nil
}

// migrateCanonicalTokenSymbol adds the canonical_token_symbol column if it doesn't exist
// and populates it with token_symbol values for existing records
func (w *Writer) migrateCanonicalTokenSymbol() error {
	// Check if column exists
	checkSQL := `PRAGMA table_info(transfers)`
	rows, err := w.db.Query(checkSQL)
	if err != nil {
		return fmt.Errorf("failed to check table info: %w", err)
	}
	defer rows.Close()

	hasCanonicalColumn := false
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull int
		var defaultValue interface{}
		var pk int
		
		err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &pk)
		if err != nil {
			continue
		}
		
		if name == "canonical_token_symbol" {
			hasCanonicalColumn = true
			break
		}
	}

	if !hasCanonicalColumn {
		utils.LogInfo("DB_WRITER", "Adding canonical_token_symbol column to transfers table")
		
		// Add the column
		addColumnSQL := `ALTER TABLE transfers ADD COLUMN canonical_token_symbol TEXT`
		if _, err := w.db.Exec(addColumnSQL); err != nil {
			return fmt.Errorf("failed to add canonical_token_symbol column: %w", err)
		}
		
		// Populate existing records with token_symbol values (as fallback)
		updateSQL := `UPDATE transfers SET canonical_token_symbol = token_symbol WHERE canonical_token_symbol IS NULL`
		if _, err := w.db.Exec(updateSQL); err != nil {
			return fmt.Errorf("failed to populate canonical_token_symbol: %w", err)
		}
		
		// Add index for the new column
		indexSQL := `CREATE INDEX IF NOT EXISTS idx_transfers_canonical_token ON transfers(canonical_token_symbol, timestamp)`
		if _, err := w.db.Exec(indexSQL); err != nil {
			return fmt.Errorf("failed to create canonical_token_symbol index: %w", err)
		}
		
		utils.LogInfo("DB_WRITER", "canonical_token_symbol column migration completed")
	}
	
	return nil
}

// GetLatestSortOrder returns the most recent sort order from the database
// This is used to initialize the fetcher's pagination state on startup
func (w *Writer) GetLatestSortOrder() (string, error) {
	query := `SELECT sort_order FROM transfers WHERE sort_order IS NOT NULL ORDER BY timestamp DESC, id DESC LIMIT 1`
	
	var sortOrder string
	err := w.db.QueryRow(query).Scan(&sortOrder)
	if err != nil {
		if err == sql.ErrNoRows {
			// No transfers in database yet
			return "", nil
		}
		return "", fmt.Errorf("failed to get latest sort order: %w", err)
	}
	
	return sortOrder, nil
}

// GetEarliestSortOrder returns the earliest sort order from the database
// This is used to initialize backward syncing from the oldest known transfer
func (w *Writer) GetEarliestSortOrder() (string, error) {
	query := `SELECT sort_order FROM transfers WHERE sort_order IS NOT NULL ORDER BY timestamp ASC, id ASC LIMIT 1`
	
	var sortOrder string
	err := w.db.QueryRow(query).Scan(&sortOrder)
	if err != nil {
		if err == sql.ErrNoRows {
			// No transfers in database yet
			return "", nil
		}
		return "", fmt.Errorf("failed to get earliest sort order: %w", err)
	}
	
	return sortOrder, nil
}

// GetTransferCount returns the total number of transfers in the database
func (w *Writer) GetTransferCount() (int64, error) {
	query := `SELECT COUNT(*) FROM transfers`
	
	var count int64
	err := w.db.QueryRow(query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get transfer count: %w", err)
	}
	
	return count, nil
}

// GetDatabaseStats returns comprehensive database statistics for monitoring
func (w *Writer) GetDatabaseStats() (map[string]interface{}, error) {
	stats := make(map[string]interface{})
	
	// Get transfer count
	transferCount, _ := w.GetTransferCount()
	stats["transfer_count"] = transferCount
	
	// Get database file size
	var pageCount, pageSize int64
	w.db.QueryRow("PRAGMA page_count").Scan(&pageCount)
	w.db.QueryRow("PRAGMA page_size").Scan(&pageSize)
	dbSizeBytes := pageCount * pageSize
	stats["database_size_mb"] = float64(dbSizeBytes) / (1024 * 1024)
	stats["database_size_gb"] = float64(dbSizeBytes) / (1024 * 1024 * 1024)
	
	// Get WAL file size
	var walPages int64
	w.db.QueryRow("PRAGMA wal_checkpoint(PASSIVE)").Scan(&walPages) // Returns pages checkpointed
	stats["wal_pages"] = walPages
	
	// Get cache statistics
	var cacheSize int64
	w.db.QueryRow("PRAGMA cache_size").Scan(&cacheSize)
	stats["cache_size_pages"] = cacheSize
	stats["cache_size_mb"] = float64(cacheSize * pageSize) / (1024 * 1024)
	
	// Performance estimate
	estimatedSizeGB := float64(transferCount) * 639 / (1024 * 1024 * 1024) // 639 bytes per transfer
	stats["estimated_size_gb"] = estimatedSizeGB
	
	// Storage capacity assessment
	if transferCount > 100_000_000 {
		stats["scale_status"] = "large_scale"
		stats["maintenance_recommended"] = true
	} else if transferCount > 10_000_000 {
		stats["scale_status"] = "medium_scale" 
		stats["maintenance_recommended"] = false
	} else {
		stats["scale_status"] = "small_scale"
		stats["maintenance_recommended"] = false
	}
	
	return stats, nil
}

// PerformMaintenance runs database maintenance operations for large datasets
func (w *Writer) PerformMaintenance() error {
	utils.LogInfo("DB_WRITER", "Starting database maintenance for large dataset")
	
	// Analyze tables to update statistics
	if _, err := w.db.Exec("ANALYZE"); err != nil {
		utils.LogError("DB_WRITER", "Failed to analyze database: %v", err)
	} else {
		utils.LogInfo("DB_WRITER", "Database analysis completed")
	}
	
	// Incremental vacuum to reclaim space
	var pages int64
	if err := w.db.QueryRow("PRAGMA incremental_vacuum").Scan(&pages); err == nil {
		utils.LogInfo("DB_WRITER", "Incremental vacuum reclaimed %d pages", pages)
	}
	
	// WAL checkpoint
	var busy, log, checkpointed int64
	if err := w.db.QueryRow("PRAGMA wal_checkpoint(RESTART)").Scan(&busy, &log, &checkpointed); err == nil {
		utils.LogInfo("DB_WRITER", "WAL checkpoint: %d pages checkpointed, %d pages in log", checkpointed, log)
	}
	
	utils.LogInfo("DB_WRITER", "Database maintenance completed")
	return nil
}

// GetCapacityEstimate estimates storage capacity for target transfer count
func (w *Writer) GetCapacityEstimate(targetTransfers int64) map[string]interface{} {
	estimate := make(map[string]interface{})
	
	// Estimate storage requirements (639 bytes per transfer including indexes)
	estimatedBytes := targetTransfers * 639
	estimatedGB := float64(estimatedBytes) / (1024 * 1024 * 1024)
	
	estimate["target_transfers"] = targetTransfers
	estimate["estimated_size_gb"] = estimatedGB
	estimate["estimated_size_tb"] = estimatedGB / 1024
	
	// SQLite limits
	estimate["sqlite_max_size_tb"] = 281
	estimate["sqlite_max_rows"] = int64(9223372036854775807) // 2^63 - 1 (max int64)
	estimate["can_handle"] = estimatedGB < 281*1024 // Within SQLite limits
	
	// Performance estimates
	if targetTransfers > 1_000_000_000 {
		estimate["performance_tier"] = "very_large"
		estimate["query_performance"] = "slow"
		estimate["maintenance_frequency"] = "weekly"
	} else if targetTransfers > 100_000_000 {
		estimate["performance_tier"] = "large"
		estimate["query_performance"] = "moderate"
		estimate["maintenance_frequency"] = "monthly"
	} else {
		estimate["performance_tier"] = "medium"
		estimate["query_performance"] = "good"
		estimate["maintenance_frequency"] = "quarterly"
	}
	
	return estimate
} 