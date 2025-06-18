package database

import (
	"context"
	"database/sql"
	"fmt"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/internal/utils"
	"websocket-backend-new/models"
	
	_ "github.com/lib/pq"        // PostgreSQL driver
	_ "github.com/mattn/go-sqlite3" // SQLite3 driver (for backward compatibility during migration)
)

// Config holds database configuration
type Config struct {
	// PostgreSQL configuration
	Host     string `json:"host"`
	Port     int    `json:"port"`
	User     string `json:"user"`
	Password string `json:"password"`
	Database string `json:"database"`
	SSLMode  string `json:"sslMode"`
	
	// Legacy SQLite3 configuration (for migration)
	DatabasePath string `json:"databasePath"`
	
	// Common configuration
	BatchSize    int    `json:"batchSize"`
	DatabaseType string `json:"databaseType"` // "postgresql" or "sqlite3"
}

// DefaultConfig returns default database configuration (PostgreSQL)
func DefaultConfig() Config {
	return Config{
		// PostgreSQL defaults
		Host:     "localhost",
		Port:     5432,
		User:     "websocket_backend",
		Password: "websocket_backend_password",
		Database: "websocket_backend",
		SSLMode:  "disable",
		
		// Legacy SQLite3 fallback
		DatabasePath: "./websocket_backend.db",
		
		// Common
		BatchSize:    1000, // Larger batch size for PostgreSQL
		DatabaseType: "postgresql",
	}
}

// Writer handles writing transfers to the database
type Writer struct {
	config     Config
	channels   *channels.Channels
	db         *sql.DB
	insertStmt *sql.Stmt
}

// NewWriter creates a new database writer with PostgreSQL support
func NewWriter(config Config, channels *channels.Channels) (*Writer, error) {
	var db *sql.DB
	var err error
	
	// Connect based on database type
	switch config.DatabaseType {
	case "postgresql":
		db, err = connectPostgreSQL(config)
	case "sqlite3":
		db, err = connectSQLite3(config)
	default:
		return nil, fmt.Errorf("unsupported database type: %s", config.DatabaseType)
	}
	
	if err != nil {
		return nil, fmt.Errorf("failed to connect to %s database: %w", config.DatabaseType, err)
	}
	
	// Configure database connection pool
	if err := configureDatabase(db, config); err != nil {
		return nil, fmt.Errorf("failed to configure database: %w", err)
	}
	
	// Create writer instance
	writer := &Writer{
		config:   config,
		channels: channels,
		db:       db,
	}
	
	// Initialize schema
	if err := writer.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}
	
	// Prepare insert statement
	insertStmt, err := writer.prepareInsertStatement()
	if err != nil {
		return nil, fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	
	writer.insertStmt = insertStmt
	
	utils.LogInfo("DB_WRITER", "Database writer initialized with %s", config.DatabaseType)
	return writer, nil
}

// connectPostgreSQL establishes PostgreSQL connection
func connectPostgreSQL(config Config) (*sql.DB, error) {
	connectionString := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=%s",
		config.Host, config.Port, config.User, config.Password, config.Database, config.SSLMode,
	)
	
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}
	
	// Test connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping PostgreSQL: %w", err)
	}
	
	utils.LogInfo("DB_WRITER", "Connected to PostgreSQL at %s:%d/%s", config.Host, config.Port, config.Database)
	return db, nil
}

// connectSQLite3 establishes SQLite3 connection (legacy support)
func connectSQLite3(config Config) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", config.DatabasePath)
	if err != nil {
		return nil, err
	}
	
	utils.LogInfo("DB_WRITER", "Connected to SQLite3 at %s", config.DatabasePath)
	return db, nil
}

// configureDatabase sets up database-specific optimizations
func configureDatabase(db *sql.DB, config Config) error {
	switch config.DatabaseType {
	case "postgresql":
		return configurePostgreSQL(db)
	case "sqlite3":
		return configureSQLite(db)
	default:
		return fmt.Errorf("unsupported database type: %s", config.DatabaseType)
	}
}

// configurePostgreSQL optimizes PostgreSQL for analytical workload
func configurePostgreSQL(db *sql.DB) error {
	// Connection pool settings for analytics workload
	db.SetMaxOpenConns(25)    // Higher for PostgreSQL
	db.SetMaxIdleConns(10)    // Keep connections alive
	db.SetConnMaxLifetime(0)  // Don't expire connections
	
	// PostgreSQL-specific optimizations
	optimizations := []string{
		"SET work_mem = '256MB'",                    // Large work memory for analytics
		"SET shared_buffers = '1GB'",               // Large shared buffer (if you have control)
		"SET effective_cache_size = '4GB'",         // Assume 4GB available for caching
		"SET random_page_cost = 1.1",               // SSD optimized
		"SET seq_page_cost = 1",                    // Sequential scan cost
		"SET cpu_tuple_cost = 0.01",                // CPU cost for processing tuples
		"SET cpu_index_tuple_cost = 0.005",         // CPU cost for index operations
		"SET enable_hashagg = on",                   // Enable hash aggregation (good for GROUP BY)
		"SET enable_hashjoin = on",                  // Enable hash joins
		"SET maintenance_work_mem = '1GB'",          // Large maintenance memory for index creation
	}
	
	for _, optimization := range optimizations {
		if _, err := db.Exec(optimization); err != nil {
			// Log warning but don't fail - user might not have permissions
			utils.LogWarn("DB_WRITER", "Failed to apply optimization '%s': %v", optimization, err)
		}
	}
	
	utils.LogInfo("DB_WRITER", "PostgreSQL optimized for analytical workload")
	return nil
}

// configureSQLite configures SQLite for optimal performance (legacy)
func configureSQLite(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL",        // WAL mode for better concurrency
		"PRAGMA synchronous=NORMAL",      // Faster writes, still safe
		"PRAGMA cache_size=50000",        // 50MB cache (increased for 175M+ records)
		"PRAGMA temp_store=memory",       // Keep temp tables in memory
		"PRAGMA mmap_size=1073741824",    // 1GB memory map (increased for large datasets)
		"PRAGMA page_size=4096",          // Optimize page size for better I/O
		"PRAGMA auto_vacuum=INCREMENTAL", // Prevent file bloat with large datasets
		"PRAGMA wal_autocheckpoint=1000", // More frequent checkpoints for better read performance (was 10000)
	}
	
	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			return fmt.Errorf("failed to execute %s: %w", pragma, err)
		}
	}
	
	utils.LogInfo("DB_WRITER", "SQLite configured with WAL mode, 1000-page autocheckpoint, 50MB cache")
	return nil
}

// prepareInsertStatement creates the appropriate insert statement for the database type
func (w *Writer) prepareInsertStatement() (*sql.Stmt, error) {
	var query string
	
	switch w.config.DatabaseType {
	case "postgresql":
		// PostgreSQL uses $1, $2, etc. for parameters
		query = `
			INSERT INTO transfers (
				packet_hash, sort_order, source_chain, dest_chain, source_name, dest_name,
				sender, receiver, amount, token_symbol, canonical_token_symbol, timestamp, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT (packet_hash) DO NOTHING`
	case "sqlite3":
		// SQLite3 uses ? for parameters
		query = `
			INSERT OR IGNORE INTO transfers (
				packet_hash, sort_order, source_chain, dest_chain, source_name, dest_name,
				sender, receiver, amount, token_symbol, canonical_token_symbol, timestamp, created_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	default:
		return nil, fmt.Errorf("unsupported database type: %s", w.config.DatabaseType)
	}
	
	return w.db.Prepare(query)
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
			transfer.TransferSendTimestamp,
			transfer.TransferSendTimestamp, // created_at
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

// initSchema creates the database tables if they don't exist
func (w *Writer) initSchema() error {
	var schemaFile string
	
	switch w.config.DatabaseType {
	case "postgresql":
		schemaFile = "internal/database/postgresql_schema.sql"
	case "sqlite3":
		return w.initSQLiteSchema() // Use existing SQLite schema code
	default:
		return fmt.Errorf("unsupported database type: %s", w.config.DatabaseType)
	}
	
	// For PostgreSQL, we'll execute the schema file
	// Note: In production, you'd typically run this manually or via migration tool
	utils.LogInfo("DB_WRITER", "Database schema should be initialized manually using %s", schemaFile)
	utils.LogInfo("DB_WRITER", "Run: psql -d %s -f %s", w.config.Database, schemaFile)
	
	return nil
}

// initSQLiteSchema creates SQLite schema (legacy)
func (w *Writer) initSQLiteSchema() error {
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

-- Essential indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_transfers_timestamp ON transfers(timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_transfers_route ON transfers(source_chain, dest_chain);
CREATE INDEX IF NOT EXISTS idx_transfers_token ON transfers(token_symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_transfers_canonical_token ON transfers(canonical_token_symbol, timestamp);
CREATE INDEX IF NOT EXISTS idx_transfers_sender ON transfers(sender, timestamp);
CREATE INDEX IF NOT EXISTS idx_transfers_receiver ON transfers(receiver, timestamp);
CREATE INDEX IF NOT EXISTS idx_transfers_chain_timestamp ON transfers(source_chain, timestamp);
CREATE INDEX IF NOT EXISTS idx_transfers_dest_timestamp ON transfers(dest_chain, timestamp);
`

	_, err := w.db.Exec(schema)
	if err != nil {
		return fmt.Errorf("failed to create SQLite schema: %w", err)
	}

	utils.LogInfo("DB_WRITER", "SQLite database schema created successfully")
	return nil
}

// GetLatestSortOrder returns the most recent sort order from the database
func (w *Writer) GetLatestSortOrder() (string, error) {
	var query string
	
	switch w.config.DatabaseType {
	case "postgresql":
		query = `SELECT sort_order FROM transfers WHERE sort_order IS NOT NULL ORDER BY timestamp DESC, id DESC LIMIT 1`
	case "sqlite3":
		query = `SELECT sort_order FROM transfers WHERE sort_order IS NOT NULL ORDER BY timestamp DESC, id DESC LIMIT 1`
	}
	
	var sortOrder string
	err := w.db.QueryRow(query).Scan(&sortOrder)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil // No transfers in database yet
		}
		return "", fmt.Errorf("failed to get latest sort order: %w", err)
	}
	
	return sortOrder, nil
}

// GetEarliestSortOrder returns the earliest sort order from the database
func (w *Writer) GetEarliestSortOrder() (string, error) {
	var query string
	
	switch w.config.DatabaseType {
	case "postgresql":
		query = `SELECT sort_order FROM transfers WHERE sort_order IS NOT NULL ORDER BY timestamp ASC, id ASC LIMIT 1`
	case "sqlite3":
		query = `SELECT sort_order FROM transfers WHERE sort_order IS NOT NULL ORDER BY timestamp ASC, id ASC LIMIT 1`
	}
	
	var sortOrder string
	err := w.db.QueryRow(query).Scan(&sortOrder)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil // No transfers in database yet
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
	stats["database_type"] = w.config.DatabaseType
	
	switch w.config.DatabaseType {
	case "postgresql":
		return w.getPostgreSQLStats(stats)
	case "sqlite3":
		return w.getSQLiteStats(stats)
	default:
		return stats, nil
	}
}

// getPostgreSQLStats gets PostgreSQL-specific statistics
func (w *Writer) getPostgreSQLStats(stats map[string]interface{}) (map[string]interface{}, error) {
	// Get database size
	var dbSizeMB float64
	w.db.QueryRow("SELECT pg_size_pretty(pg_database_size(current_database()))").Scan(&dbSizeMB)
	stats["database_size_mb"] = dbSizeMB
	
	// Get materialized view status
	viewStats := make(map[string]interface{})
	rows, err := w.db.Query(`
		SELECT schemaname, matviewname, ispopulated 
		FROM pg_matviews 
		WHERE schemaname = 'public'`)
	
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var schema, viewName string
			var populated bool
			rows.Scan(&schema, &viewName, &populated)
			viewStats[viewName] = populated
		}
	}
	stats["materialized_views"] = viewStats
	
	// Performance estimate
	if transferCount, ok := stats["transfer_count"].(int64); ok {
		if transferCount > 1_000_000 {
			stats["scale_status"] = "large_scale_optimized"
			stats["performance_tier"] = "excellent"
		} else {
			stats["scale_status"] = "medium_scale"
			stats["performance_tier"] = "excellent"
		}
	}
	
	return stats, nil
}

// getSQLiteStats gets SQLite-specific statistics (legacy)
func (w *Writer) getSQLiteStats(stats map[string]interface{}) (map[string]interface{}, error) {
	// Get database file size
	var pageCount, pageSize int64
	w.db.QueryRow("PRAGMA page_count").Scan(&pageCount)
	w.db.QueryRow("PRAGMA page_size").Scan(&pageSize)
	dbSizeBytes := pageCount * pageSize
	stats["database_size_mb"] = float64(dbSizeBytes) / (1024 * 1024)
	stats["database_size_gb"] = float64(dbSizeBytes) / (1024 * 1024 * 1024)
	
	// Get WAL file size
	var walPages int64
	w.db.QueryRow("PRAGMA wal_checkpoint(PASSIVE)").Scan(&walPages)
	stats["wal_pages"] = walPages
	
	// Performance estimate
	if transferCount, ok := stats["transfer_count"].(int64); ok {
		if transferCount > 100_000_000 {
			stats["scale_status"] = "large_scale"
			stats["maintenance_recommended"] = true
			stats["performance_tier"] = "poor"
		} else if transferCount > 10_000_000 {
			stats["scale_status"] = "medium_scale" 
			stats["maintenance_recommended"] = false
			stats["performance_tier"] = "moderate"
		} else {
			stats["scale_status"] = "small_scale"
			stats["maintenance_recommended"] = false
			stats["performance_tier"] = "good"
		}
	}
	
	return stats, nil
} 