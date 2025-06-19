package database

import (
	"database/sql"
	"websocket-backend-new/internal/utils"
)

// NewEnhancedChartServiceWithPostgreSQL creates a wrapper that uses PostgreSQL optimizations
// This provides backward compatibility while leveraging PostgreSQL's materialized views
func NewEnhancedChartServiceWithPostgreSQL(db *sql.DB, pgService *PostgreSQLChartService) *EnhancedChartService {
	// Create the standard enhanced chart service
	enhanced := NewEnhancedChartService(db)
	
	// Enable PostgreSQL optimizations
	enhanced.pgService = pgService
	
	utils.LogInfo("CHART_WRAPPER", "PostgreSQL optimizations enabled for chart service")
	return enhanced
}

 