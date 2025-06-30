package storage

import (
	"context"
	"fmt"
	"websocket-backend/internal/core"
	"websocket-backend/internal/utils"
	"websocket-backend/models"
)

// Config holds ClickHouse database configuration
type Config struct {
	// ClickHouse configuration
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Database string `json:"database"`
	Username string `json:"username"`
	Password string `json:"password"`
	Debug    bool   `json:"debug"`
	
	// Common configuration
	BatchSize int `json:"batchSize"`
}



// Writer handles writing transfers to ClickHouse
type Writer struct {
	config     Config
	channels   *core.Channels
	clickhouse *ClickHouseService
}

// NewWriter creates a new ClickHouse database writer
func NewWriter(config Config, channels *core.Channels) (*Writer, error) {
	// Create ClickHouse configuration
	chConfig := ClickHouseConfig{
		Host:     config.Host,
		Port:     config.Port,
		Database: config.Database,
		Username: config.Username,
		Password: config.Password,
		Debug:    config.Debug,
	}
	
	// Connect to ClickHouse
	clickhouse, err := NewClickHouseService(chConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	
	// Initialize schema
	if err := clickhouse.InitializeSchema(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize ClickHouse schema: %w", err)
	}
	
	writer := &Writer{
		config:     config,
		channels:   channels,
		clickhouse: clickhouse,
	}
	
	utils.LogInfo("DB_WRITER", "Database writer initialized with ClickHouse")
	return writer, nil
}

// Start begins the database writer thread
func (w *Writer) Start(ctx context.Context) {
	utils.LogInfo("DB_WRITER", "Starting ClickHouse database writer")
	
	for {
		select {
		case <-ctx.Done():
			utils.LogInfo("DB_WRITER", "Stopping database writer")
			return
			
		case transfers := <-w.channels.DatabaseSaves:
			w.saveTransferBatch(ctx, transfers)
		}
	}
}

// saveTransferBatch saves a batch of transfers to ClickHouse
func (w *Writer) saveTransferBatch(ctx context.Context, transfers []models.Transfer) {
	if len(transfers) == 0 {
		return
	}
	
	err := w.clickhouse.InsertTransfers(ctx, transfers)
	if err != nil {
		utils.LogError("DB_WRITER", "Failed to save transfer batch: %v", err)
		return
	}
	
	utils.LogInfo("DB_WRITER", "Saved %d transfers to ClickHouse", len(transfers))
}

// GetClickHouse returns the ClickHouse service
func (w *Writer) GetClickHouse() *ClickHouseService {
	return w.clickhouse
}

// Close closes the ClickHouse connection
func (w *Writer) Close() error {
	if w.clickhouse != nil {
		return w.clickhouse.Close()
	}
	return nil
}

// GetTransferCount returns the total number of transfers in ClickHouse
func (w *Writer) GetTransferCount(ctx context.Context) (int64, error) {
	query := `SELECT count() FROM transfers_analytics`
	
	var count int64
	err := w.clickhouse.conn.QueryRow(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get transfer count: %w", err)
	}
	
	return count, nil
}

// GetDatabaseStats returns comprehensive ClickHouse statistics for monitoring
func (w *Writer) GetDatabaseStats(ctx context.Context) (map[string]interface{}, error) {
	stats := make(map[string]interface{})
	
	// Get transfer count
	transferCount, _ := w.GetTransferCount(ctx)
	stats["transfer_count"] = transferCount
	stats["database_type"] = "clickhouse"
	
	// Get database size
	var dbSizeBytes int64
	err := w.clickhouse.conn.QueryRow(ctx, `
		SELECT sum(bytes_on_disk) 
		FROM system.parts 
		WHERE database = ? AND active = 1
	`, w.config.Database).Scan(&dbSizeBytes)
	
	if err == nil {
		stats["database_size_mb"] = float64(dbSizeBytes) / 1024 / 1024
	}
	
	// Get table information
	tableStats := make(map[string]interface{})
	rows, err := w.clickhouse.conn.Query(ctx, `
		SELECT name, total_rows, total_bytes 
		FROM system.tables 
		WHERE database = ?
	`, w.config.Database)
	
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var tableName string
			var totalRows, totalBytes int64
			if err := rows.Scan(&tableName, &totalRows, &totalBytes); err == nil {
				tableStats[tableName] = map[string]interface{}{
					"rows":  totalRows,
					"bytes": totalBytes,
				}
			}
		}
	}
	stats["tables"] = tableStats
	
	// Performance status
	if transferCount > 1_000_000 {
		stats["scale_status"] = "large_scale_optimized"
		stats["performance_tier"] = "excellent"
	} else {
		stats["scale_status"] = "medium_scale"
		stats["performance_tier"] = "excellent"
	}
	
	return stats, nil
}

// Health checks ClickHouse health
func (w *Writer) Health(ctx context.Context) error {
	return w.clickhouse.Health(ctx)
} 