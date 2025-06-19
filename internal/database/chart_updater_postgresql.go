package database

import (
	"database/sql"
	"fmt"
	"time"
	"websocket-backend-new/internal/utils"
)

// PostgreSQLChartUpdater handles chart updates with PostgreSQL-compatible syntax
type PostgreSQLChartUpdater struct {
	db *sql.DB
}

// NewPostgreSQLChartUpdater creates a PostgreSQL-optimized chart updater
func NewPostgreSQLChartUpdater(db *sql.DB) *PostgreSQLChartUpdater {
	return &PostgreSQLChartUpdater{db: db}
}

// UpdateAllChartSummaries updates chart summaries using PostgreSQL syntax
func (u *PostgreSQLChartUpdater) UpdateAllChartSummaries() error {
	// Since we have materialized views, we can simply refresh them
	// instead of manually updating each chart type
	if err := u.refreshMaterializedViews(); err != nil {
		utils.LogError("POSTGRESQL_CHART_UPDATER", "Failed to refresh materialized views: %v", err)
		return err
	}
	
	utils.LogDebug("POSTGRESQL_CHART_UPDATER", "All materialized views refreshed successfully")
	return nil
}

// refreshMaterializedViews refreshes all PostgreSQL materialized views
// Note: Now uses 5-minute periods instead of 1-minute for better performance with 175M records
func (u *PostgreSQLChartUpdater) refreshMaterializedViews() error {
	start := time.Now()
	utils.LogDebug("POSTGRESQL_CHART_UPDATER", "Starting materialized view refresh (5m periods)...")
	
	_, err := u.db.Exec("SELECT refresh_analytics()")
	if err != nil {
		return fmt.Errorf("failed to refresh materialized views: %w", err)
	}
	
	duration := time.Since(start)
	utils.LogInfo("POSTGRESQL_CHART_UPDATER", "✅ Materialized views refreshed in %v", duration)
	
	// Log warning if refresh takes too long (indicates need for optimization)
	if duration > 30*time.Second {
		utils.LogWarn("POSTGRESQL_CHART_UPDATER", "⚠️  Slow refresh detected (%v) - consider increasing refresh interval", duration)
	} else if duration > 10*time.Second {
		utils.LogInfo("POSTGRESQL_CHART_UPDATER", "📊 Moderate refresh time (%v) - monitoring performance", duration)
	}
	
	return nil
}

// Alternative: Manual update methods for compatibility (if needed)

// updatePopularRoutes updates popular routes using PostgreSQL syntax
func (u *PostgreSQLChartUpdater) updatePopularRoutes(timeScale string, since time.Time) error {
	// PostgreSQL uses proper timestamp comparison
	query := `
		SELECT source_chain, dest_chain, source_name, dest_name,
		       COUNT(*) as count, MAX(timestamp) as last_activity
		FROM transfers 
		WHERE timestamp > $1 
		GROUP BY source_chain, dest_chain, source_name, dest_name
		ORDER BY count DESC 
		LIMIT 10`
	
	rows, err := u.db.Query(query, since) // Pass timestamp directly, not Unix()
	if err != nil {
		return fmt.Errorf("failed to query popular routes: %w", err)
	}
	defer rows.Close()
	
	var routes []FrontendRouteData
	for rows.Next() {
		var route FrontendRouteData
		var lastActivity time.Time
		
		err := rows.Scan(&route.FromChain, &route.ToChain, &route.FromName, 
			&route.ToName, &route.Count, &lastActivity)
		if err != nil {
			continue
		}
		
		route.Route = fmt.Sprintf("%s→%s", route.FromName, route.ToName)
		routes = append(routes, route)
	}
	
	// For now, we rely on materialized views instead of manual storage
	// This method is kept for compatibility but not actively used
	return nil
}

// updateWalletStats computes wallet statistics using PostgreSQL syntax
func (u *PostgreSQLChartUpdater) updateWalletStats(timeScale string, since time.Time) error {
	// Get unique senders count
	var uniqueSenders int64
	err := u.db.QueryRow("SELECT COUNT(DISTINCT sender) FROM transfers WHERE timestamp > $1", since).Scan(&uniqueSenders)
	if err != nil {
		return fmt.Errorf("failed to count unique senders: %w", err)
	}
	
	// Get unique receivers count  
	var uniqueReceivers int64
	err = u.db.QueryRow("SELECT COUNT(DISTINCT receiver) FROM transfers WHERE timestamp > $1", since).Scan(&uniqueReceivers)
	if err != nil {
		return fmt.Errorf("failed to count unique receivers: %w", err)
	}
	
	// Estimate unique total (faster than expensive UNION)
	var uniqueTotal int64
	if uniqueSenders > 0 && uniqueReceivers > 0 {
		// Estimate overlap (typically 20-30% for wallet data)
		overlap := int64(float64(min(uniqueSenders, uniqueReceivers)) * 0.25)
		uniqueTotal = uniqueSenders + uniqueReceivers - overlap
	} else {
		uniqueTotal = uniqueSenders + uniqueReceivers
	}
	
	// For now, we rely on materialized views instead of manual storage
	// This method is kept for compatibility but not actively used
	utils.LogDebug("POSTGRESQL_CHART_UPDATER", "Wallet stats: %d senders, %d receivers, %d total", 
		uniqueSenders, uniqueReceivers, uniqueTotal)
	
	return nil
}

 