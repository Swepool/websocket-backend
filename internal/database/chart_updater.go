package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"time"
	"websocket-backend-new/internal/utils"
)

// ChartUpdater handles background computation of chart summaries
type ChartUpdater struct {
	db *sql.DB
}

// NewChartUpdater creates a new chart updater
func NewChartUpdater(db *sql.DB) *ChartUpdater {
	return &ChartUpdater{db: db}
}

// UpdateAllChartSummaries computes and stores all chart summaries
func (u *ChartUpdater) UpdateAllChartSummaries() error {
	timeScales := []string{"1m", "1h", "1d", "7d", "14d", "30d"}
	now := time.Now()
	
	for _, timeScale := range timeScales {
		var since time.Time
		switch timeScale {
		case "1m":
			since = now.Add(-time.Minute)
		case "1h":
			since = now.Add(-time.Hour)
		case "1d":
			since = now.Add(-24 * time.Hour)
		case "7d":
			since = now.Add(-7 * 24 * time.Hour)
		case "14d":
			since = now.Add(-14 * 24 * time.Hour)
		case "30d":
			since = now.Add(-30 * 24 * time.Hour)
		}
		
		// Update all chart types for this time scale
		if err := u.updatePopularRoutes(timeScale, since); err != nil {
			utils.LogError("CHART_UPDATER", "Failed to update popular routes for %s: %v", timeScale, err)
		}
		
		if err := u.updateActiveSenders(timeScale, since); err != nil {
			utils.LogError("CHART_UPDATER", "Failed to update active senders for %s: %v", timeScale, err)
		}
		
		if err := u.updateActiveReceivers(timeScale, since); err != nil {
			utils.LogError("CHART_UPDATER", "Failed to update active receivers for %s: %v", timeScale, err)
		}
		
		if err := u.updateChainFlows(timeScale, since); err != nil {
			utils.LogError("CHART_UPDATER", "Failed to update chain flows for %s: %v", timeScale, err)
		}
		
		if err := u.updateAssetVolumes(timeScale, since); err != nil {
			utils.LogError("CHART_UPDATER", "Failed to update asset volumes for %s: %v", timeScale, err)
		}
		
		// Add wallet statistics update
		if err := u.updateWalletStats(timeScale, since); err != nil {
			utils.LogError("CHART_UPDATER", "Failed to update wallet stats for %s: %v", timeScale, err)
		}
	}
	
	utils.LogDebug("CHART_UPDATER", "All chart summaries updated successfully")
	return nil
}

// Update popular routes for a time scale
func (u *ChartUpdater) updatePopularRoutes(timeScale string, since time.Time) error {
	query := `
		SELECT source_chain, dest_chain, source_name, dest_name,
		       COUNT(*) as count, MAX(timestamp) as last_activity
		FROM transfers 
		WHERE timestamp > ? 
		GROUP BY source_chain, dest_chain, source_name, dest_name
		ORDER BY count DESC 
		LIMIT 10`
	
	rows, err := u.db.Query(query, since.Unix())
	if err != nil {
		return fmt.Errorf("failed to query popular routes: %w", err)
	}
	defer rows.Close()
	
	var routes []FrontendRouteData
	for rows.Next() {
		var route FrontendRouteData
		var lastActivity int64
		
		err := rows.Scan(&route.FromChain, &route.ToChain, &route.FromName, 
			&route.ToName, &route.Count, &lastActivity)
		if err != nil {
			continue
		}
		
		route.Route = fmt.Sprintf("%s→%s", route.FromName, route.ToName)
		routes = append(routes, route)
	}
	
	// Store as JSON summary
	return u.storeChartSummary("popular_routes", timeScale, routes)
}

// Update active senders for a time scale
func (u *ChartUpdater) updateActiveSenders(timeScale string, since time.Time) error {
	query := `
		SELECT sender, COUNT(*) as count, MAX(timestamp) as last_activity
		FROM transfers 
		WHERE timestamp > ? 
		GROUP BY sender 
		ORDER BY count DESC 
		LIMIT 10`
	
	rows, err := u.db.Query(query, since.Unix())
	if err != nil {
		return fmt.Errorf("failed to query active senders: %w", err)
	}
	defer rows.Close()
	
	var wallets []FrontendWalletData
	for rows.Next() {
		var wallet FrontendWalletData
		var lastActivity int64
		
		err := rows.Scan(&wallet.Address, &wallet.Count, &lastActivity)
		if err != nil {
			continue
		}
		
		wallet.DisplayAddress = u.formatAddress(wallet.Address)
		wallet.LastActivity = time.Unix(lastActivity, 0).Format(time.RFC3339)
		wallets = append(wallets, wallet)
	}
	
	return u.storeChartSummary("active_senders", timeScale, wallets)
}

// Update active receivers for a time scale
func (u *ChartUpdater) updateActiveReceivers(timeScale string, since time.Time) error {
	query := `
		SELECT receiver, COUNT(*) as count, MAX(timestamp) as last_activity
		FROM transfers 
		WHERE timestamp > ? 
		GROUP BY receiver 
		ORDER BY count DESC 
		LIMIT 10`
	
	rows, err := u.db.Query(query, since.Unix())
	if err != nil {
		return fmt.Errorf("failed to query active receivers: %w", err)
	}
	defer rows.Close()
	
	var wallets []FrontendWalletData
	for rows.Next() {
		var wallet FrontendWalletData
		var lastActivity int64
		
		err := rows.Scan(&wallet.Address, &wallet.Count, &lastActivity)
		if err != nil {
			continue
		}
		
		wallet.DisplayAddress = u.formatAddress(wallet.Address)
		wallet.LastActivity = time.Unix(lastActivity, 0).Format(time.RFC3339)
		wallets = append(wallets, wallet)
	}
	
	return u.storeChartSummary("active_receivers", timeScale, wallets)
}

// Update chain flows for a time scale
func (u *ChartUpdater) updateChainFlows(timeScale string, since time.Time) error {
	// Get outgoing counts by chain
	outgoingQuery := `
		SELECT source_chain, source_name, COUNT(*) as count
		FROM transfers 
		WHERE timestamp > ? 
		GROUP BY source_chain, source_name`
	
	incomingQuery := `
		SELECT dest_chain, dest_name, COUNT(*) as count
		FROM transfers 
		WHERE timestamp > ? 
		GROUP BY dest_chain, dest_name`
	
	// Build chain flows map
	chainFlows := make(map[string]*FrontendChainFlow)
	
	// Process outgoing
	rows, err := u.db.Query(outgoingQuery, since.Unix())
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var chainID, chainName string
			var count int64
			if err := rows.Scan(&chainID, &chainName, &count); err == nil {
				if chainFlows[chainID] == nil {
					chainFlows[chainID] = &FrontendChainFlow{
						UniversalChainID: chainID,
						ChainName:        chainName,
						LastActivity:     time.Now().Format(time.RFC3339),
					}
				}
				chainFlows[chainID].OutgoingCount = count
			}
		}
	}
	
	// Process incoming
	rows, err = u.db.Query(incomingQuery, since.Unix())
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var chainID, chainName string
			var count int64
			if err := rows.Scan(&chainID, &chainName, &count); err == nil {
				if chainFlows[chainID] == nil {
					chainFlows[chainID] = &FrontendChainFlow{
						UniversalChainID: chainID,
						ChainName:        chainName,
						LastActivity:     time.Now().Format(time.RFC3339),
					}
				}
				chainFlows[chainID].IncomingCount = count
			}
		}
	}
	
	// Calculate net flows and add asset data for each chain
	var flows []FrontendChainFlow
	for chainID, flow := range chainFlows {
		flow.NetFlow = flow.OutgoingCount - flow.IncomingCount
		
		// Get top assets for this chain
		flow.TopAssets = u.getChainTopAssets(chainID, since)
		
		flows = append(flows, *flow)
	}
	
	// Sort chains by total activity (outgoing + incoming) and limit to top 10
	// This ensures we only store the most active chains in chart summaries
	sort.Slice(flows, func(i, j int) bool {
		totalI := flows[i].OutgoingCount + flows[i].IncomingCount
		totalJ := flows[j].OutgoingCount + flows[j].IncomingCount
		return totalI > totalJ
	})
	
	// Limit to top 10 chains to match frontend item count options
	if len(flows) > 10 {
		flows = flows[:10]
	}
	
	return u.storeChartSummary("chain_flows", timeScale, flows)
}

// Get top assets for a specific chain with in/out flow data
func (u *ChartUpdater) getChainTopAssets(chainID string, since time.Time) []FrontendChainAsset {
	// Query assets flowing through this chain (both as source and destination)
	// Use canonical_token_symbol for proper wrapping tracking, fallback to token_symbol
	query := `
		WITH chain_assets AS (
			-- Outgoing assets from this chain
			SELECT 
				COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol) as symbol,
				COUNT(*) as outgoing_count,
				0 as incoming_count,
				COALESCE(SUM(amount), 0) as outgoing_volume,
				0 as incoming_volume,
				MAX(timestamp) as last_activity
			FROM transfers 
			WHERE timestamp > ? AND source_chain = ? AND COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol) IS NOT NULL
			GROUP BY COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol)
			
			UNION ALL
			
			-- Incoming assets to this chain  
			SELECT 
				COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol) as symbol,
				0 as outgoing_count,
				COUNT(*) as incoming_count,
				0 as outgoing_volume,
				COALESCE(SUM(amount), 0) as incoming_volume,
				MAX(timestamp) as last_activity
			FROM transfers 
			WHERE timestamp > ? AND dest_chain = ? AND COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol) IS NOT NULL
			GROUP BY COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol)
		)
		SELECT 
			symbol,
			SUM(outgoing_count) as total_outgoing,
			SUM(incoming_count) as total_incoming,
			SUM(outgoing_volume) as total_outgoing_volume,
			SUM(incoming_volume) as total_incoming_volume,
			MAX(last_activity) as last_activity
		FROM chain_assets
		GROUP BY symbol
		ORDER BY (SUM(outgoing_count) + SUM(incoming_count)) DESC
		LIMIT 10`
	
	rows, err := u.db.Query(query, since.Unix(), chainID, since.Unix(), chainID)
	if err != nil {
		utils.LogError("CHART_UPDATER", "Failed to query chain assets for %s: %v", chainID, err)
		return []FrontendChainAsset{}
	}
	defer rows.Close()
	
	var assets []FrontendChainAsset
	totalVolume := 0.0
	
	// First pass - collect data and calculate total volume
	type assetData struct {
		asset  FrontendChainAsset
		volume float64
	}
	var assetList []assetData
	
	for rows.Next() {
		var asset FrontendChainAsset
		var lastActivity int64
		var outgoingVolume, incomingVolume float64
		
		err := rows.Scan(&asset.AssetSymbol, &asset.OutgoingCount, &asset.IncomingCount,
			&outgoingVolume, &incomingVolume, &lastActivity)
		if err != nil {
			continue
		}
		
		// Calculate derived fields
		asset.AssetName = asset.AssetSymbol
		asset.NetFlow = asset.IncomingCount - asset.OutgoingCount
		asset.TotalVolume = outgoingVolume + incomingVolume
		
		totalCount := asset.OutgoingCount + asset.IncomingCount
		if totalCount > 0 {
			asset.AverageAmount = asset.TotalVolume / float64(totalCount)
		}
		
		asset.LastActivity = time.Unix(lastActivity, 0).Format(time.RFC3339)
		
		assetList = append(assetList, assetData{asset: asset, volume: asset.TotalVolume})
		totalVolume += asset.TotalVolume
	}
	
	// Second pass - calculate percentages
	for _, ad := range assetList {
		asset := ad.asset
		if totalVolume > 0 {
			asset.Percentage = (ad.volume / totalVolume) * 100
		}
		assets = append(assets, asset)
	}
	
	return assets
}

// Update asset volumes for a time scale
func (u *ChartUpdater) updateAssetVolumes(timeScale string, since time.Time) error {
	// Use canonical_token_symbol if available (for proper wrapping tracking), fallback to token_symbol
	query := `
		SELECT COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol) as symbol, COUNT(*) as count, 
		       COALESCE(SUM(amount), 0) as total_volume,
		       COALESCE(MAX(amount), 0) as largest_transfer,
		       MAX(timestamp) as last_activity
		FROM transfers 
		WHERE timestamp > ? AND COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol) IS NOT NULL
		GROUP BY COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol)
		ORDER BY count DESC 
		LIMIT 10`
	
	rows, err := u.db.Query(query, since.Unix())
	if err != nil {
		return fmt.Errorf("failed to query asset volumes: %w", err)
	}
	defer rows.Close()
	
	var assets []FrontendAsset
	for rows.Next() {
		var asset FrontendAsset
		var lastActivity int64
		
		err := rows.Scan(&asset.AssetSymbol, &asset.TransferCount, 
			&asset.TotalVolume, &asset.LargestTransfer, &lastActivity)
		if err != nil {
			continue
		}
		
		asset.AssetName = asset.AssetSymbol
		if asset.TransferCount > 0 {
			asset.AverageAmount = asset.TotalVolume / float64(asset.TransferCount)
		}
		asset.LastActivity = time.Unix(lastActivity, 0).Format(time.RFC3339)
		
		// Get top routes for this asset
		asset.TopRoutes = u.getAssetTopRoutes(asset.AssetSymbol, since)
		
		assets = append(assets, asset)
	}
	
	return u.storeChartSummary("asset_volumes", timeScale, assets)
}

// Get top routes for a specific asset
func (u *ChartUpdater) getAssetTopRoutes(symbol string, since time.Time) []FrontendAssetRoute {
	query := `
		SELECT source_chain, dest_chain, source_name, dest_name,
		       COUNT(*) as count, COALESCE(SUM(amount), 0) as volume,
		       MAX(timestamp) as last_activity
		FROM transfers 
		WHERE timestamp > ? AND COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol) = ?
		GROUP BY source_chain, dest_chain, source_name, dest_name
		ORDER BY count DESC 
		LIMIT 5`
	
	rows, err := u.db.Query(query, since.Unix(), symbol)
	if err != nil {
		return []FrontendAssetRoute{}
	}
	defer rows.Close()
	
	var routes []FrontendAssetRoute
	totalVolume := 0.0
	
	// First pass - get data and calculate total
	type routeData struct {
		route  FrontendAssetRoute
		volume float64
	}
	var routeList []routeData
	
	for rows.Next() {
		var route FrontendAssetRoute
		var lastActivity int64
		var volume float64
		
		err := rows.Scan(&route.FromChain, &route.ToChain, &route.FromName, 
			&route.ToName, &route.Count, &volume, &lastActivity)
		if err != nil {
			continue
		}
		
		route.Route = fmt.Sprintf("%s→%s", route.FromName, route.ToName)
		route.Volume = volume
		route.LastActivity = time.Unix(lastActivity, 0).Format(time.RFC3339)
		
		routeList = append(routeList, routeData{route: route, volume: volume})
		totalVolume += volume
	}
	
	// Second pass - calculate percentages
	for _, rd := range routeList {
		route := rd.route
		if totalVolume > 0 {
			route.Percentage = (rd.volume / totalVolume) * 100
		}
		routes = append(routes, route)
	}
	
	return routes
}

// Store chart summary in database
func (u *ChartUpdater) storeChartSummary(chartType, timeScale string, data interface{}) error {
	dataJSON, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to marshal chart data: %w", err)
	}
	
	now := time.Now().Unix()
	_, err = u.db.Exec(`
		INSERT OR REPLACE INTO chart_summaries 
		(chart_type, time_scale, data_json, updated_at, created_at)
		VALUES (?, ?, ?, ?, ?)`,
		chartType, timeScale, string(dataJSON), now, now)
	
	if err != nil {
		return fmt.Errorf("failed to store chart summary: %w", err)
	}
	
	return nil
}

// Format wallet address for display
func (u *ChartUpdater) formatAddress(address string) string {
	if len(address) <= 12 {
		return address
	}
	return fmt.Sprintf("%s...%s", address[:6], address[len(address)-6:])
}

// updateWalletStats computes and stores wallet statistics for a time scale
func (u *ChartUpdater) updateWalletStats(timeScale string, since time.Time) error {
	// Use simpler, faster queries instead of expensive COUNT(DISTINCT) unions
	var uniqueSenders, uniqueReceivers int64
	
	// Count unique senders (with index optimization)
	err := u.db.QueryRow("SELECT COUNT(DISTINCT sender) FROM transfers WHERE timestamp > ?", since.Unix()).Scan(&uniqueSenders)
	if err != nil {
		return fmt.Errorf("failed to count unique senders: %w", err)
	}
	
	// Count unique receivers (with index optimization)  
	err = u.db.QueryRow("SELECT COUNT(DISTINCT receiver) FROM transfers WHERE timestamp > ?", since.Unix()).Scan(&uniqueReceivers)
	if err != nil {
		return fmt.Errorf("failed to count unique receivers: %w", err)
	}
	
	// For unique total, use a more efficient approach
	// Create a temporary table with UNION to avoid the expensive subquery
	var uniqueTotal int64
	tempTableQuery := `
		CREATE TEMP TABLE IF NOT EXISTS temp_unique_wallets AS
		SELECT DISTINCT sender as wallet FROM transfers WHERE timestamp > ?
		UNION
		SELECT DISTINCT receiver as wallet FROM transfers WHERE timestamp > ?
	`
	
	if _, err := u.db.Exec(tempTableQuery, since.Unix(), since.Unix()); err != nil {
		// Fallback to approximate calculation if temp table fails
		uniqueTotal = uniqueSenders + uniqueReceivers - int64(float64(min(uniqueSenders, uniqueReceivers)) * 0.7) // Estimate overlap
	} else {
		// Count from temp table
		u.db.QueryRow("SELECT COUNT(*) FROM temp_unique_wallets").Scan(&uniqueTotal)
		u.db.Exec("DROP TABLE IF EXISTS temp_unique_wallets")
	}
	
	// Store in chart_summaries table
	walletStats := map[string]interface{}{
		"uniqueSenders":   uniqueSenders,
		"uniqueReceivers": uniqueReceivers,
		"uniqueTotal":     uniqueTotal,
		"timeScale":       timeScale,
		"computedAt":      time.Now().Unix(),
	}
	
		return u.storeChartSummary("wallet_stats", timeScale, walletStats)
} 