package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"
	"websocket-backend-new/models"
	"websocket-backend-new/internal/utils"
)

// EnhancedChartService provides comprehensive chart data using hybrid approach
type EnhancedChartService struct {
	db          *sql.DB
	latencyData []models.LatencyData
	latencyMu   sync.RWMutex
	
	// Node health data
	nodeHealthData []models.NodeHealthData
	healthMu       sync.RWMutex
	
	// Cache for expensive chart data (always served to clients)
	cachedChartData map[string]interface{}
	chartCacheMu    sync.RWMutex
	lastCacheUpdate time.Time
	
	// PostgreSQL optimization fields
	pgService     *PostgreSQLChartService
	usePostgreSQL bool
}

// NewEnhancedChartService creates a new enhanced chart service
func NewEnhancedChartService(db *sql.DB) *EnhancedChartService {
	return &EnhancedChartService{
		db:              db,
		latencyData:     []models.LatencyData{},
		nodeHealthData:  []models.NodeHealthData{},
		cachedChartData: nil,
	}
}

// Frontend-compatible data structures matching old stats collector

type FrontendTransferRates struct {
	TxPerMinute         float64 `json:"txPerMinute"`
	TxPerHour           float64 `json:"txPerHour"`
	TxPerDay            float64 `json:"txPerDay"`
	TxPer7Days          float64 `json:"txPer7Days"`
	TxPer14Days         float64 `json:"txPer14Days"`
	TxPer30Days         float64 `json:"txPer30Days"`
	TxPerMinuteChange   float64 `json:"txPerMinuteChange"`
	TxPerHourChange     float64 `json:"txPerHourChange"`
	TxPerDayChange      float64 `json:"txPerDayChange"`
	TxPer7DaysChange    float64 `json:"txPer7DaysChange"`
	TxPer14DaysChange   float64 `json:"txPer14DaysChange"`
	TxPer30DaysChange   float64 `json:"txPer30DaysChange"`
	TotalTracked        int64   `json:"totalTracked"`
	UniqueSendersTotal  int64   `json:"uniqueSendersTotal"`
	UniqueReceiversTotal int64   `json:"uniqueReceiversTotal"`
	DataAvailability    map[string]bool `json:"dataAvailability"`
	ServerUptimeSeconds float64 `json:"serverUptimeSeconds"`
}

type FrontendRouteData struct {
	Route       string  `json:"route"`
	Count       int64   `json:"count"`
	FromChain   string  `json:"fromChain"`
	ToChain     string  `json:"toChain"`
	FromName    string  `json:"fromName"`
	ToName      string  `json:"toName"`
	CountChange float64 `json:"countChange,omitempty"`
}

type FrontendWalletData struct {
	Address        string `json:"address"`
	DisplayAddress string `json:"displayAddress"`
	Count          int64  `json:"count"`
	LastActivity   string `json:"lastActivity"`
}

type FrontendChainFlow struct {
	UniversalChainID string  `json:"universal_chain_id"`
	ChainName        string  `json:"chainName"`
	OutgoingCount    int64   `json:"outgoingCount"`
	IncomingCount    int64   `json:"incomingCount"`
	NetFlow          int64   `json:"netFlow"`
	OutgoingChange   float64 `json:"outgoingChange,omitempty"`
	IncomingChange   float64 `json:"incomingChange,omitempty"`
	NetFlowChange    float64 `json:"netFlowChange,omitempty"`
	LastActivity     string  `json:"lastActivity"`
	TopAssets        []FrontendChainAsset `json:"topAssets,omitempty"`
}

type FrontendChainAsset struct {
	AssetSymbol   string  `json:"assetSymbol"`
	AssetName     string  `json:"assetName"`
	OutgoingCount int64   `json:"outgoingCount"`
	IncomingCount int64   `json:"incomingCount"`
	NetFlow       int64   `json:"netFlow"`
	TotalVolume   float64 `json:"totalVolume"`
	AverageAmount float64 `json:"averageAmount"`
	Percentage    float64 `json:"percentage"`
	LastActivity  string  `json:"lastActivity"`
}

type FrontendAsset struct {
	AssetSymbol     string              `json:"assetSymbol"`
	AssetName       string              `json:"assetName"`
	TransferCount   int64               `json:"transferCount"`
	TotalVolume     float64             `json:"totalVolume"`
	LargestTransfer float64             `json:"largestTransfer"`
	AverageAmount   float64             `json:"averageAmount"`
	VolumeChange    float64             `json:"volumeChange,omitempty"`
	CountChange     float64             `json:"countChange,omitempty"`
	LastActivity    string              `json:"lastActivity"`
	TopRoutes       []FrontendAssetRoute `json:"topRoutes"`
}

type FrontendAssetRoute struct {
	FromChain    string  `json:"fromChain"`
	ToChain      string  `json:"toChain"`
	FromName     string  `json:"fromName"`
	ToName       string  `json:"toName"`
	Route        string  `json:"route"`
	Count        int64   `json:"count"`
	Volume       float64 `json:"volume"`
	Percentage   float64 `json:"percentage"`
	LastActivity string  `json:"lastActivity"`
}

// GetChartDataForFrontend returns complete chart data in frontend-expected format
// This method ALWAYS returns cached data to ensure fast client connections
func (c *EnhancedChartService) GetChartDataForFrontend() (map[string]interface{}, error) {
	now := time.Now()
	
	// ALWAYS serve from cache to ensure fast client connections
	c.chartCacheMu.RLock()
	if c.cachedChartData != nil {
		// Return cached data with updated timestamp
		cachedData := make(map[string]interface{})
		for k, v := range c.cachedChartData {
			cachedData[k] = v
		}
		cachedData["timestamp"] = now // Update timestamp to current
		age := now.Sub(c.lastCacheUpdate)
		c.chartCacheMu.RUnlock()
		
		utils.LogDebug("CHART_SERVICE", "✅ Serving cached chart data (age: %v)", age)
		return cachedData, nil
	}
	c.chartCacheMu.RUnlock()
	
	// Cache is empty (startup scenario) - build initial cache
	utils.LogInfo("CHART_SERVICE", "⚠️  Cache empty, building initial cache for startup...")
	return c.buildChartData(now)
}

// buildChartData builds chart data from database (used for cache warming)
func (c *EnhancedChartService) buildChartData(now time.Time) (map[string]interface{}, error) {
	// Use PostgreSQL optimizations if available (ULTRA FAST)
	if c.usePostgreSQL && c.pgService != nil {
		utils.LogDebug("CHART_SERVICE", "🚀 Using PostgreSQL-optimized chart data building")
		return c.pgService.GetChartDataOptimized()
	}
	
	// Fall back to legacy SQLite3 implementation (slower)
	utils.LogDebug("CHART_SERVICE", "Using legacy SQLite3 chart data building")
	
	// Get real-time transfer rates
	transferRates, err := c.getTransferRatesWithChanges(now)
	if err != nil {
		utils.LogError("CHART_SERVICE", "Failed to get transfer rates: %v", err)
		transferRates = FrontendTransferRates{} // Use empty data
	}
	
	// Get pre-computed chart data from summaries
	popularRoutes, _ := c.getChartSummary("popular_routes", "1m")
	popularRoutesTimeScale, _ := c.getAllTimeScaleSummaries("popular_routes")
	
	activeSenders, _ := c.getChartSummary("active_senders", "1m")
	activeSendersTimeScale, _ := c.getAllTimeScaleSummaries("active_senders")
	
	activeReceivers, _ := c.getChartSummary("active_receivers", "1m")
	activeReceiversTimeScale, _ := c.getAllTimeScaleSummaries("active_receivers")
	
	chainFlows, _ := c.getChartSummary("chain_flows", "1m")
	chainFlowTimeScale, _ := c.getAllTimeScaleSummaries("chain_flows")
	
	assetVolumes, _ := c.getChartSummary("asset_volumes", "1m")
	assetVolumeTimeScale, _ := c.getAllTimeScaleSummaries("asset_volumes")
	
	// Calculate totals for chain flows and asset volumes
	totalOutgoing, totalIncoming := c.calculateChainTotals(now.Add(-time.Minute))
	totalAssets, totalVolume, totalTransfers := c.calculateAssetTotals(now.Add(-time.Minute))
	
	// Get node health summary
	nodeHealthSummary, err := c.GetNodeHealthSummary()
	if err != nil {
		utils.LogError("CHART_SERVICE", "Failed to get node health summary: %v", err)
	} else {
		utils.LogDebug("CHART_SERVICE", "Node health summary: %d total nodes, %d healthy", 
			nodeHealthSummary.TotalNodes, nodeHealthSummary.HealthyNodes)
	}
	
	// Build frontend-compatible response
	chartData := map[string]interface{}{
		"currentRates":           transferRates,
		"activeWalletRates":      c.buildActiveWalletRates(transferRates),
		"popularRoutes":          popularRoutes,
		"popularRoutesTimeScale": popularRoutesTimeScale,
		"activeSenders":          activeSenders,
		"activeSendersTimeScale": activeSendersTimeScale,
		"activeReceivers":        activeReceivers,
		"activeReceiversTimeScale": activeReceiversTimeScale,
		"chainFlowData": map[string]interface{}{
			"chains":             chainFlows,
			"chainFlowTimeScale": chainFlowTimeScale,
			"totalOutgoing":      totalOutgoing,
			"totalIncoming":      totalIncoming,
			"serverUptimeSeconds": transferRates.ServerUptimeSeconds,
		},
		"assetVolumeData": map[string]interface{}{
			"assets":              assetVolumes,
			"assetVolumeTimeScale": assetVolumeTimeScale,
			"totalAssets":         totalAssets,
			"totalVolume":         totalVolume,
			"totalTransfers":      totalTransfers,
			"serverUptimeSeconds": transferRates.ServerUptimeSeconds,
		},
		"latencyData":       c.GetLatencyData(),
		"nodeHealthData":    nodeHealthSummary,
		"dataAvailability":  transferRates.DataAvailability,
		"timestamp":         now,
	}
	
	// Update cache
	c.chartCacheMu.Lock()
	c.cachedChartData = chartData
	c.lastCacheUpdate = now
	c.chartCacheMu.Unlock()
	
	utils.LogInfo("CHART_SERVICE", "📊 Chart data built and cached")
	return chartData, nil
}

// Real-time transfer rates with percentage changes
func (c *EnhancedChartService) getTransferRatesWithChanges(now time.Time) (FrontendTransferRates, error) {
	// Calculate current period counts
	rates := FrontendTransferRates{
		DataAvailability: map[string]bool{
			"hasMinute": true, "hasHour": true, "hasDay": true,
			"has7Days": true, "has14Days": true, "has30Days": true,
		},
		ServerUptimeSeconds: time.Since(now.Add(-time.Hour)).Seconds(), // Simplified
	}
	
	periods := map[string]time.Duration{
		"1m":  time.Minute,
		"1h":  time.Hour,
		"1d":  24 * time.Hour,
		"7d":  7 * 24 * time.Hour,
		"14d": 14 * 24 * time.Hour,
		"30d": 30 * 24 * time.Hour,
	}
	
	for period, duration := range periods {
		since := now.Add(-duration)
		
		// Current period count
		var currentCount int64
		err := c.db.QueryRow("SELECT COUNT(*) FROM transfers WHERE timestamp > ?", since.Unix()).Scan(&currentCount)
		if err != nil {
			continue
		}
		
		// Previous period count for percentage change
		prevSince := since.Add(-duration)
		prevUntil := since
		var prevCount int64
		c.db.QueryRow("SELECT COUNT(*) FROM transfers WHERE timestamp > ? AND timestamp <= ?", 
			prevSince.Unix(), prevUntil.Unix()).Scan(&prevCount)
		
		// Calculate percentage change
		var change float64
		if prevCount > 0 {
			change = ((float64(currentCount) - float64(prevCount)) / float64(prevCount)) * 100
		}
		
		// Assign to appropriate field
		switch period {
		case "1m":
			rates.TxPerMinute = float64(currentCount)
			rates.TxPerMinuteChange = change
		case "1h":
			rates.TxPerHour = float64(currentCount)
			rates.TxPerHourChange = change
		case "1d":
			rates.TxPerDay = float64(currentCount)
			rates.TxPerDayChange = change
		case "7d":
			rates.TxPer7Days = float64(currentCount)
			rates.TxPer7DaysChange = change
		case "14d":
			rates.TxPer14Days = float64(currentCount)
			rates.TxPer14DaysChange = change
		case "30d":
			rates.TxPer30Days = float64(currentCount)
			rates.TxPer30DaysChange = change
		}
	}
	
	// Get total count and unique counts (approximate)
	c.db.QueryRow("SELECT COUNT(*) FROM transfers").Scan(&rates.TotalTracked)
	c.db.QueryRow("SELECT COUNT(DISTINCT sender) FROM transfers WHERE timestamp > ?", 
		now.Add(-24*time.Hour).Unix()).Scan(&rates.UniqueSendersTotal)
	c.db.QueryRow("SELECT COUNT(DISTINCT receiver) FROM transfers WHERE timestamp > ?", 
		now.Add(-24*time.Hour).Unix()).Scan(&rates.UniqueReceiversTotal)
	
	return rates, nil
}

// Get chart summary from pre-computed data
func (c *EnhancedChartService) getChartSummary(chartType, timeScale string) (interface{}, error) {
	var dataJSON string
	err := c.db.QueryRow(
		"SELECT data_json FROM chart_summaries WHERE chart_type = ? AND time_scale = ? ORDER BY updated_at DESC LIMIT 1",
		chartType, timeScale).Scan(&dataJSON)
	
	if err != nil {
		if err == sql.ErrNoRows {
			return []interface{}{}, nil // Return empty array if no data
		}
		return nil, err
	}
	
	var data interface{}
	if err := json.Unmarshal([]byte(dataJSON), &data); err != nil {
		return nil, fmt.Errorf("failed to unmarshal chart data: %w", err)
	}
	
	return data, nil
}

// Get all time scale summaries for a chart type
func (c *EnhancedChartService) getAllTimeScaleSummaries(chartType string) (map[string]interface{}, error) {
	rows, err := c.db.Query(
		"SELECT time_scale, data_json FROM chart_summaries WHERE chart_type = ? ORDER BY updated_at DESC",
		chartType)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	result := make(map[string]interface{})
	for rows.Next() {
		var timeScale, dataJSON string
		if err := rows.Scan(&timeScale, &dataJSON); err != nil {
			continue
		}
		
		var data interface{}
		if err := json.Unmarshal([]byte(dataJSON), &data); err != nil {
			continue
		}
		
		result[timeScale] = data
	}
	
	return result, nil
}

// Helper functions for totals
func (c *EnhancedChartService) calculateChainTotals(since time.Time) (int64, int64) {
	var outgoing, incoming int64
	
	c.db.QueryRow("SELECT COUNT(*) FROM transfers WHERE timestamp > ?", since.Unix()).Scan(&outgoing)
	incoming = outgoing // Simplified - each transfer is both outgoing and incoming
	
	return outgoing, incoming
}

func (c *EnhancedChartService) calculateAssetTotals(since time.Time) (int64, float64, int64) {
	var totalAssets, totalTransfers int64
	var totalVolume float64
	
	// Use canonical_token_symbol for proper wrapping tracking, fallback to token_symbol
	c.db.QueryRow("SELECT COUNT(DISTINCT COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol)) FROM transfers WHERE timestamp > ? AND COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol) IS NOT NULL", 
		since.Unix()).Scan(&totalAssets)
	c.db.QueryRow("SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM transfers WHERE timestamp > ?", 
		since.Unix()).Scan(&totalTransfers, &totalVolume)
	
	return totalAssets, totalVolume, totalTransfers
}

func (c *EnhancedChartService) buildActiveWalletRates(rates FrontendTransferRates) map[string]interface{} {
	now := time.Now()
	
	// Query unique senders and receivers for each time period
	periods := map[string]time.Duration{
		"LastMin":  time.Minute,
		"LastHour": time.Hour,
		"LastDay":  24 * time.Hour,
		"Last7d":   7 * 24 * time.Hour,
		"Last14d":  14 * 24 * time.Hour,
		"Last30d":  30 * 24 * time.Hour,
	}
	
	result := map[string]interface{}{
		"dataAvailability":    rates.DataAvailability,
		"serverUptimeSeconds": rates.ServerUptimeSeconds,
		"uniqueSendersTotal":  rates.UniqueSendersTotal,
		"uniqueReceiversTotal": rates.UniqueReceiversTotal,
	}
	
	// Calculate unique total wallets (union of all senders and receivers) - USE CACHED DATA
	var uniqueTotalWallets int64
	// Try to get from cached wallet_stats first (fast path)
	err := c.db.QueryRow(`
		SELECT JSON_EXTRACT(data_json, '$.uniqueTotal') 
		FROM chart_summaries 
		WHERE chart_type = 'wallet_stats' AND time_scale = '30d' 
		ORDER BY updated_at DESC LIMIT 1`).Scan(&uniqueTotalWallets)
	
	if err != nil || uniqueTotalWallets == 0 {
		// Fallback to approximate calculation if no cached data
		uniqueTotalWallets = rates.UniqueSendersTotal + rates.UniqueReceiversTotal
		// Estimate 30% overlap between senders and receivers (reasonable assumption)
		if rates.UniqueSendersTotal > 0 && rates.UniqueReceiversTotal > 0 {
			overlapEstimate := int64(float64(min(rates.UniqueSendersTotal, rates.UniqueReceiversTotal)) * 0.3)
			uniqueTotalWallets -= overlapEstimate
		}
	}
	result["uniqueTotalWallets"] = uniqueTotalWallets
	
	// Map time periods to chart_summaries time scales
	timeScaleMap := map[string]string{
		"LastMin":  "1m",
		"LastHour": "1h", 
		"LastDay":  "1d",
		"Last7d":   "7d",
		"Last14d":  "14d",
		"Last30d":  "30d",
	}
	
	// Query for each time period using CACHED DATA where possible
	for periodName, duration := range periods {
		timeScale := timeScaleMap[periodName]
		
		var senderCount, receiverCount, totalCount int64
		
		// Try to get from cached wallet_stats (fast path)
		var cachedData struct {
			senders   int64
			receivers int64
			total     int64
		}
		
		err := c.db.QueryRow(`
			SELECT 
				JSON_EXTRACT(data_json, '$.uniqueSenders'),
				JSON_EXTRACT(data_json, '$.uniqueReceivers'), 
				JSON_EXTRACT(data_json, '$.uniqueTotal')
			FROM chart_summaries 
			WHERE chart_type = 'wallet_stats' AND time_scale = ? 
			ORDER BY updated_at DESC LIMIT 1`, timeScale).Scan(&cachedData.senders, &cachedData.receivers, &cachedData.total)
		
		if err == nil && cachedData.senders > 0 {
			// Use cached data (fast!)
			senderCount = cachedData.senders
			receiverCount = cachedData.receivers  
			totalCount = cachedData.total
			utils.LogDebug("CHART_SERVICE", "Used cached wallet stats for %s", periodName)
		} else {
			// Fallback to real-time queries only if no cached data (slow path)
			since := now.Add(-duration)
			
			utils.LogWarn("CHART_SERVICE", "No cached wallet stats for %s, using slow queries", periodName)
			
			// Get unique senders count
			err := c.db.QueryRow("SELECT COUNT(DISTINCT sender) FROM transfers WHERE timestamp > ?", 
				since.Unix()).Scan(&senderCount)
			if err != nil {
				senderCount = 0
			}
			
			// Get unique receivers count  
			err = c.db.QueryRow("SELECT COUNT(DISTINCT receiver) FROM transfers WHERE timestamp > ?", 
				since.Unix()).Scan(&receiverCount)
			if err != nil {
				receiverCount = 0
			}
			
			// For total count, use approximation to avoid expensive UNION query
			totalCount = senderCount + receiverCount
			if senderCount > 0 && receiverCount > 0 {
				// Estimate overlap (typically 20-40% for wallet data)
				overlapEstimate := int64(float64(min(senderCount, receiverCount)) * 0.3)
				totalCount -= overlapEstimate
			}
		}
		
		// Calculate percentage changes for previous period using cached data when possible
		prevTimeScale := getPreviousTimeScale(timeScale)
		var prevSenderCount, prevReceiverCount, prevTotalCount int64
		
		if prevTimeScale != "" {
			// Try cached data for previous period
			err := c.db.QueryRow(`
				SELECT 
					JSON_EXTRACT(data_json, '$.uniqueSenders'),
					JSON_EXTRACT(data_json, '$.uniqueReceivers'), 
					JSON_EXTRACT(data_json, '$.uniqueTotal')
				FROM chart_summaries 
				WHERE chart_type = 'wallet_stats' AND time_scale = ? 
				ORDER BY updated_at DESC LIMIT 1`, prevTimeScale).Scan(&prevSenderCount, &prevReceiverCount, &prevTotalCount)
			
			if err != nil {
				// Fallback: estimate previous period as 80% of current (reasonable assumption)
				prevSenderCount = int64(float64(senderCount) * 0.8)
				prevReceiverCount = int64(float64(receiverCount) * 0.8)
				prevTotalCount = int64(float64(totalCount) * 0.8)
			}
		}
		
		// Calculate percentage changes
		var senderChange, receiverChange, totalChange float64
		if prevSenderCount > 0 {
			senderChange = ((float64(senderCount) - float64(prevSenderCount)) / float64(prevSenderCount)) * 100
		}
		if prevReceiverCount > 0 {
			receiverChange = ((float64(receiverCount) - float64(prevReceiverCount)) / float64(prevReceiverCount)) * 100
		}
		if prevTotalCount > 0 {
			totalChange = ((float64(totalCount) - float64(prevTotalCount)) / float64(prevTotalCount)) * 100
		}
		
		// Add to result with frontend-expected field names
		result["senders"+periodName] = senderCount
		result["receivers"+periodName] = receiverCount
		result["total"+periodName] = totalCount
		
		// Add percentage changes
		result["senders"+periodName+"Change"] = senderChange
		result["receivers"+periodName+"Change"] = receiverChange
		result["total"+periodName+"Change"] = totalChange
	}
	
	return result
}

// Helper function to get previous time scale for percentage calculations
func getPreviousTimeScale(current string) string {
	switch current {
	case "1m":
		return "" // No previous period for 1 minute
	case "1h":
		return "1m" 
	case "1d":
		return "1h"
	case "7d":
		return "1d"
	case "14d":
		return "7d"
	case "30d":
		return "14d"
	default:
		return ""
	}
}

// Helper function for min calculation (moved to package level)
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// SetLatencyData updates the stored latency data (called by latency callback)
func (c *EnhancedChartService) SetLatencyData(data []models.LatencyData) {
	c.latencyMu.Lock()
	defer c.latencyMu.Unlock()
	
	// Store in memory for immediate access
	c.latencyData = data
	
	// Store in database for persistence
	if err := c.storeLatencyDataInDB(data); err != nil {
		utils.LogError("CHART_SERVICE", "Failed to store latency data in database: %v", err)
	}
	
	utils.LogDebug("CHART_SERVICE", "Updated latency data with %d chain pairs", len(data))
}

// storeLatencyDataInDB stores latency data in the database
func (c *EnhancedChartService) storeLatencyDataInDB(data []models.LatencyData) error {
	if len(data) == 0 {
		return nil
	}
	
	// Prepare insert statement with database-specific syntax
	var insertSQL string
	if c.usePostgreSQL {
		// PostgreSQL syntax
		insertSQL = `
			INSERT INTO latency_data (
				source_chain, dest_chain, source_name, dest_name,
				packet_ack_p5, packet_ack_median, packet_ack_p95,
				packet_recv_p5, packet_recv_median, packet_recv_p95,
				write_ack_p5, write_ack_median, write_ack_p95,
				fetched_at, created_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
			ON CONFLICT (source_chain, dest_chain, fetched_at) DO UPDATE SET
				source_name = EXCLUDED.source_name,
				dest_name = EXCLUDED.dest_name,
				packet_ack_p5 = EXCLUDED.packet_ack_p5,
				packet_ack_median = EXCLUDED.packet_ack_median,
				packet_ack_p95 = EXCLUDED.packet_ack_p95,
				packet_recv_p5 = EXCLUDED.packet_recv_p5,
				packet_recv_median = EXCLUDED.packet_recv_median,
				packet_recv_p95 = EXCLUDED.packet_recv_p95,
				write_ack_p5 = EXCLUDED.write_ack_p5,
				write_ack_median = EXCLUDED.write_ack_median,
				write_ack_p95 = EXCLUDED.write_ack_p95`
	} else {
		// SQLite3 syntax
		insertSQL = `
			INSERT OR REPLACE INTO latency_data (
				source_chain, dest_chain, source_name, dest_name,
				packet_ack_p5, packet_ack_median, packet_ack_p95,
				packet_recv_p5, packet_recv_median, packet_recv_p95,
				write_ack_p5, write_ack_median, write_ack_p95,
				fetched_at, created_at
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	}
	
	stmt, err := c.db.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare latency insert statement: %w", err)
	}
	defer stmt.Close()
	
	var now interface{}
	if c.usePostgreSQL {
		now = time.Now() // PostgreSQL expects timestamp
	} else {
		now = time.Now().Unix() // SQLite3 expects Unix timestamp
	}
	
	successCount := 0
	
	for _, latency := range data {
		_, err := stmt.Exec(
			latency.SourceChain, latency.DestinationChain,
			latency.SourceName, latency.DestinationName,
			latency.PacketAck.P5, latency.PacketAck.Median, latency.PacketAck.P95,
			latency.PacketRecv.P5, latency.PacketRecv.Median, latency.PacketRecv.P95,
			latency.WriteAck.P5, latency.WriteAck.Median, latency.WriteAck.P95,
			now, now,
		)
		
		if err != nil {
			utils.LogError("CHART_SERVICE", "Failed to store latency for %s->%s: %v", 
				latency.SourceName, latency.DestinationName, err)
		} else {
			successCount++
		}
	}
	
	utils.LogInfo("CHART_SERVICE", "Stored %d/%d latency records in database", successCount, len(data))
	return nil
}

// GetLatencyDataFromDB retrieves the latest latency data from database
func (c *EnhancedChartService) GetLatencyDataFromDB() ([]models.LatencyData, error) {
	query := `
		SELECT DISTINCT 
			l1.source_chain, l1.dest_chain, l1.source_name, l1.dest_name,
			l1.packet_ack_p5, l1.packet_ack_median, l1.packet_ack_p95,
			l1.packet_recv_p5, l1.packet_recv_median, l1.packet_recv_p95,
			l1.write_ack_p5, l1.write_ack_median, l1.write_ack_p95
		FROM latency_data l1
		INNER JOIN (
			SELECT source_chain, dest_chain, MAX(fetched_at) as max_fetched
			FROM latency_data 
			GROUP BY source_chain, dest_chain
		) l2 ON l1.source_chain = l2.source_chain 
			AND l1.dest_chain = l2.dest_chain 
			AND l1.fetched_at = l2.max_fetched
		ORDER BY l1.source_name, l1.dest_name`
	
	rows, err := c.db.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query latency data: %w", err)
	}
	defer rows.Close()
	
	var latencyData []models.LatencyData
	for rows.Next() {
		var latency models.LatencyData
		
		err := rows.Scan(
			&latency.SourceChain, &latency.DestinationChain,
			&latency.SourceName, &latency.DestinationName,
			&latency.PacketAck.P5, &latency.PacketAck.Median, &latency.PacketAck.P95,
			&latency.PacketRecv.P5, &latency.PacketRecv.Median, &latency.PacketRecv.P95,
			&latency.WriteAck.P5, &latency.WriteAck.Median, &latency.WriteAck.P95,
		)
		
		if err != nil {
			utils.LogError("CHART_SERVICE", "Failed to scan latency row: %v", err)
			continue
		}
		
		latencyData = append(latencyData, latency)
	}
	
	return latencyData, nil
}

// LoadLatencyDataFromDB loads latency data from database into memory on startup
func (c *EnhancedChartService) LoadLatencyDataFromDB() error {
	latencyData, err := c.GetLatencyDataFromDB()
	if err != nil {
		return err
	}
	
	c.latencyMu.Lock()
	c.latencyData = latencyData
	c.latencyMu.Unlock()
	
	utils.LogInfo("CHART_SERVICE", "Loaded %d latency records from database", len(latencyData))
	return nil
}

// GetLatencyData returns the current latency data
func (c *EnhancedChartService) GetLatencyData() []models.LatencyData {
	c.latencyMu.RLock()
	defer c.latencyMu.RUnlock()
	
	// Return a copy to prevent external modifications
	result := make([]models.LatencyData, len(c.latencyData))
	copy(result, c.latencyData)
	return result
}

// InvalidateCache forces the chart data cache to be regenerated on next access
func (c *EnhancedChartService) InvalidateCache() {
	c.chartCacheMu.Lock()
	defer c.chartCacheMu.Unlock()
	
	c.cachedChartData = nil
	c.lastCacheUpdate = time.Time{} // Zero time
	utils.LogDebug("CHART_SERVICE", "Chart data cache invalidated")
}

// RefreshCache proactively warms the cache with fresh data
func (c *EnhancedChartService) RefreshCache() error {
	utils.LogDebug("CHART_SERVICE", "🔥 Proactively warming chart data cache...")
	_, err := c.buildChartData(time.Now())
	return err
}

// GetCacheStats returns cache performance statistics
func (c *EnhancedChartService) GetCacheStats() map[string]interface{} {
	c.chartCacheMu.RLock()
	defer c.chartCacheMu.RUnlock()
	
	stats := map[string]interface{}{
		"cacheStrategy":   "always_serve_cached",
		"hasCachedData":   c.cachedChartData != nil,
		"lastUpdate":      c.lastCacheUpdate.Format(time.RFC3339),
		"updateInterval":  "15s", // Background update frequency
	}
	
	if !c.lastCacheUpdate.IsZero() {
		age := time.Since(c.lastCacheUpdate)
		stats["cacheAge"] = age.String()
		stats["nextUpdateIn"] = (15*time.Second - (age % (15*time.Second))).String()
	} else {
		stats["cacheAge"] = "never"
		stats["nextUpdateIn"] = "pending_first_update"
	}
	
	return stats
}

// SetNodeHealthData updates the stored node health data (called by health callback)
func (c *EnhancedChartService) SetNodeHealthData(data []models.NodeHealthData) {
	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	
	// Store in memory for immediate access
	c.nodeHealthData = data
	
	// Store in database for persistence
	if err := c.storeNodeHealthDataInDB(data); err != nil {
		utils.LogError("CHART_SERVICE", "Failed to store node health data in database: %v", err)
	}
	
	utils.LogInfo("CHART_SERVICE", "Updated node health data with %d nodes", len(data))
}

// GetNodeHealthData returns the current node health data
func (c *EnhancedChartService) GetNodeHealthData() []models.NodeHealthData {
	c.healthMu.RLock()
	defer c.healthMu.RUnlock()
	
	// Return a copy to prevent external modifications
	result := make([]models.NodeHealthData, len(c.nodeHealthData))
	copy(result, c.nodeHealthData)
	return result
}

// storeNodeHealthDataInDB stores node health data in the database
func (c *EnhancedChartService) storeNodeHealthDataInDB(healthData []models.NodeHealthData) error {
	if len(healthData) == 0 {
		return nil
	}
	
	// Note: node_health table should already exist from schema creation
	// Skip table creation since it was handled during schema initialization
	// This avoids SQLite3 vs PostgreSQL syntax conflicts
	
	// Insert health data with database-specific syntax
	var insertQuery string
	if c.usePostgreSQL {
		// PostgreSQL syntax
		insertQuery = `
			INSERT INTO node_health 
			(chain_id, chain_name, rpc_url, rpc_type, status, response_time_ms, 
			 latest_block_height, error_message, uptime, checked_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (rpc_url, checked_at) DO UPDATE SET
				status = EXCLUDED.status,
				response_time_ms = EXCLUDED.response_time_ms,
				latest_block_height = EXCLUDED.latest_block_height,
				error_message = EXCLUDED.error_message,
				uptime = EXCLUDED.uptime
		`
	} else {
		// SQLite3 syntax
		insertQuery = `
			INSERT OR REPLACE INTO node_health 
			(chain_id, chain_name, rpc_url, rpc_type, status, response_time_ms, 
			 latest_block_height, error_message, uptime, checked_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`
	}
	
	stmt, err := c.db.Prepare(insertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()
	
	for _, health := range healthData {
		var checkTime interface{}
		if c.usePostgreSQL {
			checkTime = time.Unix(health.LastCheckTime, 0) // Convert Unix timestamp to time.Time for PostgreSQL
		} else {
			checkTime = health.LastCheckTime // Keep as Unix timestamp for SQLite3
		}
		
		_, err := stmt.Exec(
			health.ChainID,
			health.ChainName,
			health.RpcURL,
			health.RpcType,
			health.Status,
			health.ResponseTimeMs,
			health.LatestBlockHeight,
			health.ErrorMessage,
			health.Uptime,
			checkTime,
		)
		
		if err != nil {
			utils.LogError("CHART_SERVICE", "Failed to insert health data for %s: %v", 
				health.RpcURL, err)
			continue
		}
	}
	
	return nil
}

// GetNodeHealthSummary returns aggregated node health data for charts
func (c *EnhancedChartService) GetNodeHealthSummary() (*models.NodeHealthSummary, error) {
	c.healthMu.RLock()
	healthData := make([]models.NodeHealthData, len(c.nodeHealthData))
	copy(healthData, c.nodeHealthData)
	c.healthMu.RUnlock()
	
	if len(healthData) == 0 {
		return &models.NodeHealthSummary{
			TotalNodes:       0,
			HealthyNodes:     0,
			DegradedNodes:    0,
			UnhealthyNodes:   0,
			AvgResponseTime:  0,
			NodesWithRpcs:    []models.NodeHealthData{},
			ChainHealthStats: make(map[string]models.ChainHealthStat),
			DataAvailability: models.NodeHealthAvailability{
				HasMinute: false,
				HasHour:   false,
				HasDay:    false,
				Has7Days:  false,
				Has14Days: false,
				Has30Days: false,
			},
		}, nil
	}
	
	// Calculate summary statistics
	totalNodes := len(healthData)
	healthyNodes := 0
	degradedNodes := 0
	unhealthyNodes := 0
	totalResponseTime := 0
	responseTimeCount := 0
	
	chainStats := make(map[string]*models.ChainHealthStat)
	
	for _, node := range healthData {
		switch node.Status {
		case "healthy":
			healthyNodes++
			if node.ResponseTimeMs > 0 {
				totalResponseTime += node.ResponseTimeMs
				responseTimeCount++
			}
		case "degraded":
			degradedNodes++
			if node.ResponseTimeMs > 0 {
				totalResponseTime += node.ResponseTimeMs
				responseTimeCount++
			}
		case "unhealthy":
			unhealthyNodes++
		}
		
		// Aggregate by chain
		if _, exists := chainStats[node.ChainName]; !exists {
			chainStats[node.ChainName] = &models.ChainHealthStat{
				ChainName:    node.ChainName,
				HealthyNodes: 0,
				TotalNodes:   0,
			}
		}
		
		stat := chainStats[node.ChainName]
		stat.TotalNodes++
		if node.Status == "healthy" || node.Status == "degraded" {
			stat.HealthyNodes++
			if node.ResponseTimeMs > 0 {
				stat.AvgResponseTime = (stat.AvgResponseTime*float64(stat.HealthyNodes-1) + float64(node.ResponseTimeMs)) / float64(stat.HealthyNodes)
			}
		}
		stat.Uptime = (stat.Uptime*float64(stat.TotalNodes-1) + node.Uptime) / float64(stat.TotalNodes)
	}
	
	// Calculate average response time
	avgResponseTime := 0.0
	if responseTimeCount > 0 {
		avgResponseTime = float64(totalResponseTime) / float64(responseTimeCount)
	}
	
	// Convert chainStats map to the required format
	chainStatsMap := make(map[string]models.ChainHealthStat)
	for chainName, stat := range chainStats {
		chainStatsMap[chainName] = *stat
	}
	
	return &models.NodeHealthSummary{
		TotalNodes:       totalNodes,
		HealthyNodes:     healthyNodes,
		DegradedNodes:    degradedNodes,
		UnhealthyNodes:   unhealthyNodes,
		AvgResponseTime:  avgResponseTime,
		NodesWithRpcs:    healthData,
		ChainHealthStats: chainStatsMap,
		DataAvailability: models.NodeHealthAvailability{
			HasMinute: true, // We have current data
			HasHour:   c.hasNodeHealthDataForPeriod(time.Hour),
			HasDay:    c.hasNodeHealthDataForPeriod(24 * time.Hour),
			Has7Days:  c.hasNodeHealthDataForPeriod(7 * 24 * time.Hour),
			Has14Days: c.hasNodeHealthDataForPeriod(14 * 24 * time.Hour),
			Has30Days: c.hasNodeHealthDataForPeriod(30 * 24 * time.Hour),
		},
	}, nil
}

// hasNodeHealthDataForPeriod checks if we have node health data for a given period
func (c *EnhancedChartService) hasNodeHealthDataForPeriod(period time.Duration) bool {
	query := `
		SELECT COUNT(*) 
		FROM node_health 
		WHERE checked_at > ?
	`
	
	cutoff := time.Now().Add(-period).Unix()
	
	var count int
	err := c.db.QueryRow(query, cutoff).Scan(&count)
	if err != nil {
		utils.LogError("CHART_SERVICE", "Failed to check node health data availability: %v", err)
		return false
	}
	
	return count > 0
} 