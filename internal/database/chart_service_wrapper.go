package database

import (
	"database/sql"
	"time"
	"websocket-backend-new/internal/utils"
)

// NewEnhancedChartServiceWithPostgreSQL creates a wrapper that uses PostgreSQL optimizations
// This provides backward compatibility while leveraging PostgreSQL's materialized views
func NewEnhancedChartServiceWithPostgreSQL(db *sql.DB, pgService *PostgreSQLChartService) *EnhancedChartService {
	// Create the standard enhanced chart service
	enhanced := NewEnhancedChartService(db)
	
	// Override the expensive methods to use PostgreSQL optimizations
	enhanced.pgService = pgService
	enhanced.usePostgreSQL = true
	
	utils.LogInfo("CHART_WRAPPER", "PostgreSQL optimizations enabled for chart service")
	return enhanced
}

// Add PostgreSQL service fields to EnhancedChartService
// This needs to be added to the EnhancedChartService struct definition
func (c *EnhancedChartService) SetPostgreSQLOptimizations(pgService *PostgreSQLChartService) {
	c.pgService = pgService
	c.usePostgreSQL = true
}

// Override buildChartData to use PostgreSQL optimizations when available
func (c *EnhancedChartService) buildChartDataOptimized(now time.Time) (map[string]interface{}, error) {
	if c.usePostgreSQL && c.pgService != nil {
		// Use ultra-fast PostgreSQL materialized views
		utils.LogDebug("CHART_WRAPPER", "Using PostgreSQL-optimized chart data building")
		return c.pgService.GetChartDataOptimized()
	}
	
	// Fall back to legacy SQLite3 implementation
	utils.LogDebug("CHART_WRAPPER", "Using legacy SQLite3 chart data building")
	return c.buildChartData(now)
}

// Override getTransferRatesWithChanges to use PostgreSQL optimizations
func (c *EnhancedChartService) getTransferRatesOptimized(now time.Time) (FrontendTransferRates, error) {
	if c.usePostgreSQL && c.pgService != nil {
		// Use ultra-fast PostgreSQL materialized views
		utils.LogDebug("CHART_WRAPPER", "Using PostgreSQL-optimized transfer rates")
		return c.pgService.GetTransferRatesOptimized()
	}
	
	// Fall back to legacy SQLite3 implementation
	utils.LogDebug("CHART_WRAPPER", "Using legacy SQLite3 transfer rates")
	return c.getTransferRatesWithChanges(now)
}

// Override buildActiveWalletRates to use PostgreSQL optimizations
func (c *EnhancedChartService) buildActiveWalletRatesOptimized(rates FrontendTransferRates) map[string]interface{} {
	if c.usePostgreSQL && c.pgService != nil {
		// Use ultra-fast PostgreSQL materialized views
		utils.LogDebug("CHART_WRAPPER", "Using PostgreSQL-optimized wallet rates")
		return c.pgService.GetActiveWalletRatesOptimized(rates)
	}
	
	// Fall back to legacy SQLite3 implementation
	utils.LogDebug("CHART_WRAPPER", "Using legacy SQLite3 wallet rates")
	return c.buildActiveWalletRates(rates)
} 