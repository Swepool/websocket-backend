package database

import (
	"database/sql"
	"fmt"
	"time"
	"websocket-backend-new/internal/utils"
)

// ChartService provides chart data from database queries
type ChartService struct {
	db *sql.DB
}

// NewChartService creates a new chart service
func NewChartService(db *sql.DB) *ChartService {
	return &ChartService{db: db}
}

// RouteStats represents route statistics
type RouteStats struct {
	SourceChain  string `json:"sourceChain"`
	DestChain    string `json:"destChain"`
	SourceName   string `json:"sourceName"`
	DestName     string `json:"destName"`
	Count        int64  `json:"count"`
	LastActivity string `json:"lastActivity"`
}

// WalletStats represents wallet activity statistics
type WalletStats struct {
	Address      string `json:"address"`
	Count        int64  `json:"count"`
	LastActivity string `json:"lastActivity"`
}

// AssetStats represents asset volume statistics
type AssetStats struct {
	Symbol       string  `json:"symbol"`
	Count        int64   `json:"count"`
	TotalVolume  float64 `json:"totalVolume"`
	LastActivity string  `json:"lastActivity"`
}

// ChainFlowStats represents chain flow statistics
type ChainFlowStats struct {
	ChainID       string `json:"chainId"`
	ChainName     string `json:"chainName"`
	OutgoingCount int64  `json:"outgoingCount"`
	IncomingCount int64  `json:"incomingCount"`
	NetFlow       int64  `json:"netFlow"`
	LastActivity  string `json:"lastActivity"`
}

// TransferRates represents transfer rate statistics
type TransferRates struct {
	LastMinute int64 `json:"lastMinute"`
	LastHour   int64 `json:"lastHour"`
	LastDay    int64 `json:"lastDay"`
	Total      int64 `json:"total"`
}

// GetTopRoutes returns the most popular routes since the given time
func (c *ChartService) GetTopRoutes(since time.Time, limit int) ([]RouteStats, error) {
	query := `
		SELECT source_chain, dest_chain, source_name, dest_name, 
		       COUNT(*) as count,
		       MAX(timestamp) as last_activity
		FROM transfers 
		WHERE timestamp > ? 
		GROUP BY source_chain, dest_chain, source_name, dest_name
		ORDER BY count DESC 
		LIMIT ?`
	
	rows, err := c.db.Query(query, since.Unix(), limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query top routes: %w", err)
	}
	defer rows.Close()
	
	var routes []RouteStats
	for rows.Next() {
		var route RouteStats
		var lastActivity int64
		
		err := rows.Scan(&route.SourceChain, &route.DestChain, &route.SourceName, 
			&route.DestName, &route.Count, &lastActivity)
		if err != nil {
			utils.LogError("CHART_SERVICE", "Failed to scan route: %v", err)
			continue
		}
		
		route.LastActivity = time.Unix(lastActivity, 0).Format(time.RFC3339)
		routes = append(routes, route)
	}
	
	return routes, nil
}

// GetTopSenders returns the most active senders since the given time
func (c *ChartService) GetTopSenders(since time.Time, limit int) ([]WalletStats, error) {
	query := `
		SELECT sender, COUNT(*) as count, MAX(timestamp) as last_activity
		FROM transfers 
		WHERE timestamp > ? 
		GROUP BY sender 
		ORDER BY count DESC 
		LIMIT ?`
	
	return c.getWalletStats(query, since, limit)
}

// GetTopReceivers returns the most active receivers since the given time
func (c *ChartService) GetTopReceivers(since time.Time, limit int) ([]WalletStats, error) {
	query := `
		SELECT receiver, COUNT(*) as count, MAX(timestamp) as last_activity
		FROM transfers 
		WHERE timestamp > ? 
		GROUP BY receiver 
		ORDER BY count DESC 
		LIMIT ?`
	
	return c.getWalletStats(query, since, limit)
}

// GetTopAssets returns the most transferred assets since the given time
func (c *ChartService) GetTopAssets(since time.Time, limit int) ([]AssetStats, error) {
	query := `
		SELECT token_symbol, COUNT(*) as count, 
		       SUM(amount) as total_volume, MAX(timestamp) as last_activity
		FROM transfers 
		WHERE timestamp > ? AND token_symbol IS NOT NULL
		GROUP BY token_symbol 
		ORDER BY count DESC 
		LIMIT ?`
	
	rows, err := c.db.Query(query, since.Unix(), limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query top assets: %w", err)
	}
	defer rows.Close()
	
	var assets []AssetStats
	for rows.Next() {
		var asset AssetStats
		var lastActivity int64
		
		err := rows.Scan(&asset.Symbol, &asset.Count, &asset.TotalVolume, &lastActivity)
		if err != nil {
			utils.LogError("CHART_SERVICE", "Failed to scan asset: %v", err)
			continue
		}
		
		asset.LastActivity = time.Unix(lastActivity, 0).Format(time.RFC3339)
		assets = append(assets, asset)
	}
	
	return assets, nil
}

// GetChainFlows returns chain flow statistics since the given time
func (c *ChartService) GetChainFlows(since time.Time) ([]ChainFlowStats, error) {
	// Get outgoing transfers per chain
	outgoingQuery := `
		SELECT source_chain, source_name, COUNT(*) as count
		FROM transfers 
		WHERE timestamp > ? 
		GROUP BY source_chain, source_name`
	
	// Get incoming transfers per chain
	incomingQuery := `
		SELECT dest_chain, dest_name, COUNT(*) as count
		FROM transfers 
		WHERE timestamp > ? 
		GROUP BY dest_chain, dest_name`
	
	outgoing := make(map[string]map[string]int64) // chainId -> {name, count}
	incoming := make(map[string]map[string]int64)
	
	// Query outgoing
	rows, err := c.db.Query(outgoingQuery, since.Unix())
	if err != nil {
		return nil, fmt.Errorf("failed to query outgoing flows: %w", err)
	}
	
	for rows.Next() {
		var chainId, chainName string
		var count int64
		if err := rows.Scan(&chainId, &chainName, &count); err == nil {
			if outgoing[chainId] == nil {
				outgoing[chainId] = make(map[string]int64)
			}
			outgoing[chainId]["count"] = count
			outgoing[chainId]["name"] = int64(len(chainName)) // Store name length for lookup
		}
	}
	rows.Close()
	
	// Query incoming
	rows, err = c.db.Query(incomingQuery, since.Unix())
	if err != nil {
		return nil, fmt.Errorf("failed to query incoming flows: %w", err)
	}
	
	for rows.Next() {
		var chainId, chainName string
		var count int64
		if err := rows.Scan(&chainId, &chainName, &count); err == nil {
			if incoming[chainId] == nil {
				incoming[chainId] = make(map[string]int64)
			}
			incoming[chainId]["count"] = count
			incoming[chainId]["name"] = int64(len(chainName))
		}
	}
	rows.Close()
	
	// Combine results
	chainSet := make(map[string]string)
	for chainId := range outgoing {
		chainSet[chainId] = chainId
	}
	for chainId := range incoming {
		chainSet[chainId] = chainId
	}
	
	var flows []ChainFlowStats
	for chainId := range chainSet {
		var outCount, inCount int64
		var chainName string = chainId // fallback
		
		if out, exists := outgoing[chainId]; exists {
			outCount = out["count"]
		}
		if in, exists := incoming[chainId]; exists {
			inCount = in["count"]
		}
		
		flows = append(flows, ChainFlowStats{
			ChainID:       chainId,
			ChainName:     chainName,
			OutgoingCount: outCount,
			IncomingCount: inCount,
			NetFlow:       inCount - outCount,
			LastActivity:  time.Now().Format(time.RFC3339), // Simplified
		})
	}
	
	return flows, nil
}

// GetTransferRates returns transfer rates for different time periods
func (c *ChartService) GetTransferRates() (TransferRates, error) {
	now := time.Now()
	
	queries := map[string]time.Time{
		"minute": now.Add(-time.Minute),
		"hour":   now.Add(-time.Hour),
		"day":    now.Add(-24 * time.Hour),
		"total":  time.Unix(0, 0), // All time
	}
	
	rates := TransferRates{}
	
	for period, since := range queries {
		query := "SELECT COUNT(*) FROM transfers WHERE timestamp > ?"
		var count int64
		
		err := c.db.QueryRow(query, since.Unix()).Scan(&count)
		if err != nil {
			utils.LogError("CHART_SERVICE", "Failed to query %s rate: %v", period, err)
			continue
		}
		
		switch period {
		case "minute":
			rates.LastMinute = count
		case "hour":
			rates.LastHour = count
		case "day":
			rates.LastDay = count
		case "total":
			rates.Total = count
		}
	}
	
	return rates, nil
}

// getWalletStats is a helper function for wallet-related queries
func (c *ChartService) getWalletStats(query string, since time.Time, limit int) ([]WalletStats, error) {
	rows, err := c.db.Query(query, since.Unix(), limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query wallet stats: %w", err)
	}
	defer rows.Close()
	
	var wallets []WalletStats
	for rows.Next() {
		var wallet WalletStats
		var lastActivity int64
		
		err := rows.Scan(&wallet.Address, &wallet.Count, &lastActivity)
		if err != nil {
			utils.LogError("CHART_SERVICE", "Failed to scan wallet: %v", err)
			continue
		}
		
		wallet.LastActivity = time.Unix(lastActivity, 0).Format(time.RFC3339)
		wallets = append(wallets, wallet)
	}
	
	return wallets, nil
} 