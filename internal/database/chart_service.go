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

// EnhancedChartService provides real-time chart data with caching and persistence
type EnhancedChartService struct {
	db                  *sql.DB
	cache               map[string]interface{}
	lastCacheUpdate     time.Time
	cacheDuration       time.Duration
	latencyData         []models.LatencyData
	nodeHealthData      []models.NodeHealthData
	pgService          *PostgreSQLChartService
	latencyMu           sync.RWMutex
	healthMu            sync.RWMutex
	cachedChartData     map[string]interface{}
	chartCacheMu        sync.RWMutex
}

// NewEnhancedChartService creates a new enhanced chart service
func NewEnhancedChartService(db *sql.DB) *EnhancedChartService {
	return &EnhancedChartService{
		db:            db,
		cache:         make(map[string]interface{}),
		cacheDuration: 30 * time.Second, // 30 second cache
		latencyData:   make([]models.LatencyData, 0),
		nodeHealthData: make([]models.NodeHealthData, 0),
	}
}

// Frontend-compatible data structures matching original SQLite3 stats collector

type DataAvailability struct {
	HasMinute bool `json:"hasMinute"`
	HasHour   bool `json:"hasHour"`
	HasDay    bool `json:"hasDay"`
	Has7Days  bool `json:"has7Days"`
	Has14Days bool `json:"has14Days"`
	Has30Days bool `json:"has30Days"`
}

type FrontendTransferRates struct {
	TxPerMinute           int64            `json:"txPerMinute"`
	TxPerHour             int64            `json:"txPerHour"`
	TxPerDay              int64            `json:"txPerDay"`
	TxPer7Days            int64            `json:"txPer7Days"`
	TxPer14Days           int64            `json:"txPer14Days"`
	TxPer30Days           int64            `json:"txPer30Days"`
	TxPerMinuteChange     *float64         `json:"txPerMinuteChange,omitempty"`
	TxPerHourChange       *float64         `json:"txPerHourChange,omitempty"`
	TxPerDayChange        *float64         `json:"txPerDayChange,omitempty"`
	TxPer7DaysChange      *float64         `json:"txPer7DaysChange,omitempty"`
	TxPer14DaysChange     *float64         `json:"txPer14DaysChange,omitempty"`
	TxPer30DaysChange     *float64         `json:"txPer30DaysChange,omitempty"`
	PercentageChangeMin   float64          `json:"percentageChangeMin"`
	PercentageChangeHour  float64          `json:"percentageChangeHour"`
	PercentageChangeDay   float64          `json:"percentageChangeDay"`
	PercentageChange7Day  float64          `json:"percentageChange7Day"`
	PercentageChange14Day float64          `json:"percentageChange14Day"`
	PercentageChange30Day float64          `json:"percentageChange30Day"`
	TotalTracked          int64            `json:"totalTracked"`
	DataAvailability      DataAvailability `json:"dataAvailability"`
	ServerUptimeSeconds   float64          `json:"serverUptimeSeconds"`
	UniqueReceiversTotal  int64            `json:"uniqueReceiversTotal"`
	UniqueSendersTotal    int64            `json:"uniqueSendersTotal"`
	LastUpdateTime        time.Time        `json:"lastUpdateTime"`
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

// GetChartDataForFrontend returns cached chart data
func (c *EnhancedChartService) GetChartDataForFrontend() (map[string]interface{}, error) {
	// Check if cache needs refresh
	if time.Since(c.lastCacheUpdate) > c.cacheDuration || len(c.cache) == 0 {
		if err := c.RefreshCache(); err != nil {
			return nil, err
		}
	}
	
	return c.cache, nil
}

// RefreshCache updates the chart data cache
func (c *EnhancedChartService) RefreshCache() error {
	now := time.Now()
	
	chartData, err := c.buildChartData(now)
	if err != nil {
		return fmt.Errorf("failed to build chart data: %w", err)
	}
	
	c.cache = chartData
	c.lastCacheUpdate = now
	
	return nil
}

// buildChartData builds comprehensive chart data (restored original SQLite3 format)
func (c *EnhancedChartService) buildChartData(now time.Time) (map[string]interface{}, error) {
	// Use PostgreSQL optimizations if available (ULTRA FAST)
	if c.pgService != nil {
		utils.LogDebug("CHART_SERVICE", "🚀 Using PostgreSQL-optimized chart data building")
		return c.pgService.GetChartDataOptimized()
	}
	
	// Standard PostgreSQL implementation with original SQLite3 format
	utils.LogDebug("CHART_SERVICE", "Using standard PostgreSQL chart data building (original format)")
	
	// Get real-time transfer rates in original format
	transferRates, err := c.getTransferRatesWithChanges(now)
	if err != nil {
		utils.LogError("CHART_SERVICE", "Failed to get transfer rates: %v", err)
		transferRates = FrontendTransferRates{
			DataAvailability: DataAvailability{
				HasMinute: false,
				HasHour:   false,
				HasDay:    false,
				Has7Days:  false,
				Has14Days: false,
				Has30Days: false,
			},
		} // Use empty data
	}
	
	// Get pre-computed chart data from summaries for all timeframes
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
	
	// Get active wallet rates in original format
	activeWalletRates := c.buildActiveWalletRates(transferRates)
	
	// Return in original SQLite3 API format with proper timeframe structure
	return map[string]interface{}{
		"timestamp":    now,
		"currentRates": transferRates,
		"activeWalletRates": activeWalletRates,
		"activeSenders": activeSenders,
		"activeSendersTimeScale": activeSendersTimeScale, // Already proper format from getAllTimeScaleSummaries
		"activeReceivers": activeReceivers,
		"activeReceiversTimeScale": activeReceiversTimeScale, // Already proper format
		"popularRoutes": popularRoutes,
		"popularRoutesTimeScale": popularRoutesTimeScale, // Already proper format
		"chainFlowData": map[string]interface{}{
			"chains":             chainFlows,
			"chainFlowTimeScale": chainFlowTimeScale, // Already proper format
			"totalOutgoing":      totalOutgoing,
			"totalIncoming":      totalIncoming,
			"serverUptimeSeconds": time.Since(now.Add(-time.Hour)).Seconds(),
		},
		"assetVolumeData": map[string]interface{}{
			"assets":              assetVolumes,
			"assetVolumeTimeScale": assetVolumeTimeScale, // Already proper format
			"totalAssets":         totalAssets,
			"totalVolume":         totalVolume,
			"totalTransfers":      totalTransfers,
			"serverUptimeSeconds": time.Since(now.Add(-time.Hour)).Seconds(),
		},
		"latencyData": c.getLatencyDataFromDatabase(),
		"nodeHealthData": c.getNodeHealthDataFromDatabase(),
		// Add dataAvailability at the root level for charts to use
		"dataAvailability": map[string]interface{}{
			"hasMinute": c.hasDataForPeriod("1m"),
			"hasHour":   c.hasDataForPeriod("1h"),
			"hasDay":    c.hasDataForPeriod("1d"),
			"has7Days":  c.hasDataForPeriod("7d"),
			"has14Days": c.hasDataForPeriod("14d"),
			"has30Days": c.hasDataForPeriod("30d"),
		},
	}, nil
}

// Real-time transfer rates with percentage changes (restored original SQLite3 format)
func (c *EnhancedChartService) getTransferRatesWithChanges(now time.Time) (FrontendTransferRates, error) {
	// Get the actual data timestamp range (not import time!)
	var totalRecords int64
	var minTimestamp, maxTimestamp int64
	c.db.QueryRow("SELECT COUNT(*), MIN(timestamp), MAX(timestamp) FROM transfers").Scan(&totalRecords, &minTimestamp, &maxTimestamp)
	
	minTime := time.Unix(minTimestamp, 0)
	maxTime := time.Unix(maxTimestamp, 0)
	dataSpan := maxTime.Sub(minTime)
	
	utils.LogInfo("CHART_SERVICE", "📊 DATA ANALYSIS: %d total records | Span: %v | From: %s | To: %s", 
		totalRecords, dataSpan, minTime.Format("2006-01-02 15:04:05"), maxTime.Format("2006-01-02 15:04:05"))
	
	// Use the most recent transfer timestamp as our reference point (not current time!)
	dataEndTime := maxTime
	
	// Check distribution across periods relative to actual data timeline
	var count1h, count1d, count7d int64
	c.db.QueryRow("SELECT COUNT(*) FROM transfers WHERE timestamp > $1", dataEndTime.Add(-time.Hour).Unix()).Scan(&count1h)
	c.db.QueryRow("SELECT COUNT(*) FROM transfers WHERE timestamp > $1", dataEndTime.Add(-24*time.Hour).Unix()).Scan(&count1d)
	c.db.QueryRow("SELECT COUNT(*) FROM transfers WHERE timestamp > $1", dataEndTime.Add(-7*24*time.Hour).Unix()).Scan(&count7d)
	
	utils.LogInfo("CHART_SERVICE", "📈 DISTRIBUTION FROM DATA END (%s): Last 1h=%d, Last 1d=%d, Last 7d=%d", 
		dataEndTime.Format("2006-01-02 15:04:05"), count1h, count1d, count7d)
	
	// Calculate current period counts
	rates := FrontendTransferRates{
		LastUpdateTime: now,
		DataAvailability: DataAvailability{
			HasMinute: c.hasDataForPeriod("1m"),
			HasHour:   c.hasDataForPeriod("1h"),
			HasDay:    c.hasDataForPeriod("1d"),
			Has7Days:  c.hasDataForPeriod("7d"),
			Has14Days: c.hasDataForPeriod("14d"),
			Has30Days: c.hasDataForPeriod("30d"),
		},
		ServerUptimeSeconds: time.Since(now.Add(-time.Hour)).Seconds(), // Simplified uptime calculation
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
		// Use actual data timeline, not current time!
		since := dataEndTime.Add(-duration)
		
		// Current period count
		var currentCount int64
		err := c.db.QueryRow("SELECT COUNT(*) FROM transfers WHERE timestamp > $1", since.Unix()).Scan(&currentCount)
		if err != nil {
			continue
		}
		
		// Previous period count for percentage change - use longer lookback for stable comparison
		var prevCount int64
		var percentageChange float64
		
		// For short periods (1m, 1h), use a more stable comparison window
		// For longer periods, use the standard previous period comparison
		if period == "1m" {
			// Compare current minute vs average of last 5 minutes (excluding current)
			err = c.db.QueryRow("SELECT COUNT(*) FROM transfers WHERE timestamp > $1 AND timestamp <= $2", 
				dataEndTime.Add(-6*time.Minute).Unix(), dataEndTime.Add(-time.Minute).Unix()).Scan(&prevCount)
			if err == nil && prevCount > 0 {
				avgPrevCount := float64(prevCount) / 5.0 // Average over 5 minutes
				if avgPrevCount > 0 {
					percentageChange = ((float64(currentCount) - avgPrevCount) / avgPrevCount) * 100
				}
			}
		} else if period == "1h" {
			// Compare current hour vs previous hour
			err = c.db.QueryRow("SELECT COUNT(*) FROM transfers WHERE timestamp > $1 AND timestamp <= $2", 
				dataEndTime.Add(-2*time.Hour).Unix(), dataEndTime.Add(-time.Hour).Unix()).Scan(&prevCount)
			if err == nil && prevCount > 0 {
				percentageChange = ((float64(currentCount) - float64(prevCount)) / float64(prevCount)) * 100
			}
		} else {
			// For daily and longer periods, use standard previous period comparison
			prevSince := dataEndTime.Add(-2 * duration)
			prevUntil := dataEndTime.Add(-duration)
			err = c.db.QueryRow("SELECT COUNT(*) FROM transfers WHERE timestamp > $1 AND timestamp <= $2", 
				prevSince.Unix(), prevUntil.Unix()).Scan(&prevCount)
			if err == nil && prevCount > 0 {
				percentageChange = ((float64(currentCount) - float64(prevCount)) / float64(prevCount)) * 100
			}
		}
		
		// Handle case where no previous period data exists
		if prevCount == 0 && currentCount > 0 {
			// If we have no previous period data, try alternative comparison
			if period == "1d" || period == "7d" || period == "30d" {
				// For longer periods with no historical data, compare against shorter period trend
				var recentHourCount int64
				c.db.QueryRow("SELECT COUNT(*) FROM transfers WHERE timestamp > $1", 
					dataEndTime.Add(-time.Hour).Unix()).Scan(&recentHourCount)
				
				if recentHourCount > 0 {
					// Extrapolate hourly rate to period and compare
					periodHours := duration.Hours()
					expectedCount := float64(recentHourCount) * periodHours
					if expectedCount > 0 {
						percentageChange = ((float64(currentCount) - expectedCount) / expectedCount) * 100
						utils.LogInfo("CHART_SERVICE", "🔄 Period %s: Using trend-based calculation (recent hourly rate: %d)", period, recentHourCount)
					}
				}
			}
		}
		
		// Only default to 0% if we truly have no way to calculate
		if prevCount == 0 && percentageChange == 0 {
			percentageChange = 0.0 // Show 0% when no comparison is possible
		}
		
		utils.LogInfo("CHART_SERVICE", "🔍 Period %s: current=%d, prev=%d, change=%.1f%% | Current: %s to %s", 
			period, currentCount, prevCount, percentageChange,
			since.Format("2006-01-02 15:04:05"), dataEndTime.Format("2006-01-02 15:04:05"))
		
		// Assign to appropriate field
		switch period {
		case "1m":
			rates.TxPerMinute = currentCount
			rates.PercentageChangeMin = percentageChange
			rates.TxPerMinuteChange = &percentageChange
		case "1h":
			rates.TxPerHour = currentCount
			rates.PercentageChangeHour = percentageChange
			rates.TxPerHourChange = &percentageChange
		case "1d":
			rates.TxPerDay = currentCount
			rates.PercentageChangeDay = percentageChange
			rates.TxPerDayChange = &percentageChange
		case "7d":
			rates.TxPer7Days = currentCount
			rates.PercentageChange7Day = percentageChange
			rates.TxPer7DaysChange = &percentageChange
		case "14d":
			rates.TxPer14Days = currentCount
			rates.PercentageChange14Day = percentageChange
			rates.TxPer14DaysChange = &percentageChange
		case "30d":
			rates.TxPer30Days = currentCount
			rates.PercentageChange30Day = percentageChange
			rates.TxPer30DaysChange = &percentageChange
		}
	}
	
	// Get unique senders and receivers totals (use data timeline!)
	c.db.QueryRow("SELECT COUNT(DISTINCT sender) FROM transfers WHERE timestamp > $1", 
		dataEndTime.Add(-30*24*time.Hour).Unix()).Scan(&rates.UniqueSendersTotal)
	c.db.QueryRow("SELECT COUNT(DISTINCT receiver) FROM transfers WHERE timestamp > $1", 
		dataEndTime.Add(-30*24*time.Hour).Unix()).Scan(&rates.UniqueReceiversTotal)
	
	// Set total tracked
	rates.TotalTracked = rates.UniqueSendersTotal + rates.UniqueReceiversTotal
	
	return rates, nil
}

// Get chart summary from pre-computed data
func (c *EnhancedChartService) getChartSummary(chartType, timeScale string) (interface{}, error) {
	var dataJSON string
	err := c.db.QueryRow(
		"SELECT data_json FROM chart_summaries WHERE chart_type = $1 AND time_scale = $2 ORDER BY updated_at DESC LIMIT 1",
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
		"SELECT time_scale, data_json FROM chart_summaries WHERE chart_type = $1 ORDER BY updated_at DESC",
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
	
	c.db.QueryRow("SELECT COUNT(*) FROM transfers WHERE timestamp > $1", since.Unix()).Scan(&outgoing)
	incoming = outgoing // Simplified - each transfer is both outgoing and incoming
	
	return outgoing, incoming
}

func (c *EnhancedChartService) calculateAssetTotals(since time.Time) (int64, float64, int64) {
	var totalAssets, totalTransfers int64
	var totalVolume float64
	
	// Use canonical_token_symbol for proper wrapping tracking, fallback to token_symbol
	c.db.QueryRow("SELECT COUNT(DISTINCT COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol)) FROM transfers WHERE timestamp > $1 AND COALESCE(NULLIF(canonical_token_symbol, ''), token_symbol) IS NOT NULL", 
		since.Unix()).Scan(&totalAssets)
	c.db.QueryRow("SELECT COUNT(*), COALESCE(SUM(amount), 0) FROM transfers WHERE timestamp > $1", 
		since.Unix()).Scan(&totalTransfers, &totalVolume)
	
	return totalAssets, totalVolume, totalTransfers
}

func (c *EnhancedChartService) buildActiveWalletRates(rates FrontendTransferRates) map[string]interface{} {
	// Get the actual data timeline (same as in transfer rates)
	var maxTimestamp int64
	c.db.QueryRow("SELECT MAX(timestamp) FROM transfers").Scan(&maxTimestamp)
	dataEndTime := time.Unix(maxTimestamp, 0)
	
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
		"dataAvailable":     true,
		"dataAvailability": DataAvailability{
			HasMinute: rates.DataAvailability.HasMinute,
			HasHour:   rates.DataAvailability.HasHour,
			HasDay:    rates.DataAvailability.HasDay,
			Has7Days:  rates.DataAvailability.Has7Days,
			Has14Days: rates.DataAvailability.Has14Days,
			Has30Days: rates.DataAvailability.Has30Days,
		},
		"serverUptime":         time.Since(dataEndTime.Add(-time.Hour)).Seconds(),
		"serverUptimeSeconds":  time.Since(dataEndTime.Add(-time.Hour)).Seconds(),
		"lastUpdateTime":       rates.LastUpdateTime,
		"uniqueSendersTotal":   rates.UniqueSendersTotal,
		"uniqueReceiversTotal": rates.UniqueReceiversTotal,
	}
	
	// Calculate unique total wallets (union of all senders and receivers) - USE CACHED DATA
	var uniqueTotalWallets int64
	// Try to get from cached wallet_stats first (fast path)
	err := c.db.QueryRow(`
		SELECT (data_json->'uniqueTotal')::int 
		FROM chart_summaries 
		WHERE chart_type = 'wallet_stats' AND time_scale = '30d' 
		ORDER BY updated_at DESC LIMIT 1`).Scan(&uniqueTotalWallets)
	
	if err != nil || uniqueTotalWallets == 0 {
		// Fallback to approximate calculation if no cached data (use data timeline!)
		var senders, receivers int64
		c.db.QueryRow("SELECT COUNT(DISTINCT sender) FROM transfers WHERE timestamp > $1", 
			dataEndTime.Add(-30*24*time.Hour).Unix()).Scan(&senders)
		c.db.QueryRow("SELECT COUNT(DISTINCT receiver) FROM transfers WHERE timestamp > $1", 
			dataEndTime.Add(-30*24*time.Hour).Unix()).Scan(&receivers)
		
		uniqueTotalWallets = senders + receivers
		// Estimate 30% overlap between senders and receivers (reasonable assumption)
		if senders > 0 && receivers > 0 {
			overlapEstimate := int64(float64(min(senders, receivers)) * 0.3)
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
				(data_json->'uniqueSenders')::int,
				(data_json->'uniqueReceivers')::int, 
				(data_json->'uniqueTotal')::int
			FROM chart_summaries 
			WHERE chart_type = 'wallet_stats' AND time_scale = $1 
			ORDER BY updated_at DESC LIMIT 1`, timeScale).Scan(&cachedData.senders, &cachedData.receivers, &cachedData.total)
		
		if err == nil && cachedData.senders > 0 {
			// Use cached data (fast!)
			senderCount = cachedData.senders
			receiverCount = cachedData.receivers  
			totalCount = cachedData.total
			utils.LogDebug("CHART_SERVICE", "Used cached wallet stats for %s", periodName)
		} else {
			// Fallback to real-time queries only if no cached data (slow path, use data timeline!)
			since := dataEndTime.Add(-duration)
			
			utils.LogWarn("CHART_SERVICE", "No cached wallet stats for %s, using slow queries with data timeline", periodName)
			
			// Get unique senders count
			err := c.db.QueryRow("SELECT COUNT(DISTINCT sender) FROM transfers WHERE timestamp > $1", 
				since.Unix()).Scan(&senderCount)
			if err != nil {
				senderCount = 0
			}
			
			// Get unique receivers count  
			err = c.db.QueryRow("SELECT COUNT(DISTINCT receiver) FROM transfers WHERE timestamp > $1", 
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
		
		// Calculate percentage changes using improved time-based comparison
		var prevSenderCount, prevReceiverCount, prevTotalCount int64
		var senderChange, receiverChange, totalChange float64
		
		// Use same improved logic as transfer rates (with data timeline!)
		if periodName == "LastMin" {
			// For minute data, compare against 5-minute average (more stable)
			err := c.db.QueryRow("SELECT COUNT(DISTINCT sender) FROM transfers WHERE timestamp > $1 AND timestamp <= $2", 
				dataEndTime.Add(-6*time.Minute).Unix(), dataEndTime.Add(-time.Minute).Unix()).Scan(&prevSenderCount)
			if err == nil && prevSenderCount > 0 {
				avgPrevSenders := float64(prevSenderCount) / 5.0
				if avgPrevSenders > 0 {
					senderChange = ((float64(senderCount) - avgPrevSenders) / avgPrevSenders) * 100
				}
			}
			
			err = c.db.QueryRow("SELECT COUNT(DISTINCT receiver) FROM transfers WHERE timestamp > $1 AND timestamp <= $2", 
				dataEndTime.Add(-6*time.Minute).Unix(), dataEndTime.Add(-time.Minute).Unix()).Scan(&prevReceiverCount)
			if err == nil && prevReceiverCount > 0 {
				avgPrevReceivers := float64(prevReceiverCount) / 5.0
				if avgPrevReceivers > 0 {
					receiverChange = ((float64(receiverCount) - avgPrevReceivers) / avgPrevReceivers) * 100
				}
			}
			
			// Calculate total change based on combined average
			if prevSenderCount > 0 && prevReceiverCount > 0 {
				avgPrevTotal := (float64(prevSenderCount + prevReceiverCount) / 5.0) * 0.7 // Account for overlap
				if avgPrevTotal > 0 {
					totalChange = ((float64(totalCount) - avgPrevTotal) / avgPrevTotal) * 100
				}
			}
		} else {
			// For longer periods, use standard previous period comparison (with data timeline!)
			prevSince := dataEndTime.Add(-2 * duration)
			prevUntil := dataEndTime.Add(-duration)
			
			// Get previous period counts
			err := c.db.QueryRow("SELECT COUNT(DISTINCT sender) FROM transfers WHERE timestamp > $1 AND timestamp <= $2", 
				prevSince.Unix(), prevUntil.Unix()).Scan(&prevSenderCount)
			if err != nil {
				prevSenderCount = 0
			}
			
			err = c.db.QueryRow("SELECT COUNT(DISTINCT receiver) FROM transfers WHERE timestamp > $1 AND timestamp <= $2", 
				prevSince.Unix(), prevUntil.Unix()).Scan(&prevReceiverCount)
			if err != nil {
				prevReceiverCount = 0
			}
			
			// Calculate previous total (with overlap estimation)
			prevTotalCount = prevSenderCount + prevReceiverCount
			if prevSenderCount > 0 && prevReceiverCount > 0 {
				overlapEstimate := int64(float64(min(prevSenderCount, prevReceiverCount)) * 0.3)
				prevTotalCount -= overlapEstimate
			}
			
			// Calculate percentage changes only if we have previous data
			if prevSenderCount > 0 {
				senderChange = ((float64(senderCount) - float64(prevSenderCount)) / float64(prevSenderCount)) * 100
			}
			
			if prevReceiverCount > 0 {
				receiverChange = ((float64(receiverCount) - float64(prevReceiverCount)) / float64(prevReceiverCount)) * 100
			}
			
			if prevTotalCount > 0 {
				totalChange = ((float64(totalCount) - float64(prevTotalCount)) / float64(prevTotalCount)) * 100
			}
		}
		
		// Default to 0% change when no previous data (instead of 100%)
		if prevSenderCount == 0 {
			senderChange = 0.0
		}
		if prevReceiverCount == 0 {
			receiverChange = 0.0
		}
		if prevTotalCount == 0 && (periodName != "LastMin" || (prevSenderCount == 0 && prevReceiverCount == 0)) {
			totalChange = 0.0
		}
		
		utils.LogInfo("CHART_SERVICE", "📊 Wallet %s: senders=%d->%d (%.1f%%), receivers=%d->%d (%.1f%%), total=%d->%d (%.1f%%) [Data timeline: %s]", 
			periodName, prevSenderCount, senderCount, senderChange, 
			prevReceiverCount, receiverCount, receiverChange,
			prevTotalCount, totalCount, totalChange, dataEndTime.Format("2006-01-02 15:04:05"))
		
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

// Helper function to get previous time scale for percentage calculations (deprecated - now using time-based comparison)

// Helper function for min calculation
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// hasDataForPeriod checks if data is available for a given time period
func (c *EnhancedChartService) hasDataForPeriod(period string) bool {
	// Check if we have any chart summaries for this time period
	var count int
	err := c.db.QueryRow("SELECT COUNT(*) FROM chart_summaries WHERE time_scale = $1", period).Scan(&count)
	if err != nil {
		return false
	}
	return count > 0
}

// countHealthyNodes counts nodes with healthy status
func (c *EnhancedChartService) countHealthyNodes() int {
	c.healthMu.RLock()
	defer c.healthMu.RUnlock()
	
	count := 0
	for _, node := range c.nodeHealthData {
		if node.Status == "healthy" || node.Status == "active" {
			count++
		}
	}
	return count
}

// countDegradedNodes counts nodes with degraded status
func (c *EnhancedChartService) countDegradedNodes() int {
	c.healthMu.RLock()
	defer c.healthMu.RUnlock()
	
	count := 0
	for _, node := range c.nodeHealthData {
		if node.Status == "degraded" || node.Status == "warning" {
			count++
		}
	}
	return count
}

// countUnhealthyNodes counts nodes with unhealthy status
func (c *EnhancedChartService) countUnhealthyNodes() int {
	c.healthMu.RLock()
	defer c.healthMu.RUnlock()
	
	count := 0
	for _, node := range c.nodeHealthData {
		if node.Status == "unhealthy" || node.Status == "error" || node.Status == "down" {
			count++
		}
	}
	return count
}

// calculateAvgResponseTime calculates average response time across all nodes
func (c *EnhancedChartService) calculateAvgResponseTime() float64 {
	c.healthMu.RLock()
	defer c.healthMu.RUnlock()
	
	if len(c.nodeHealthData) == 0 {
		return 0.0
	}
	
	total := 0.0
	validCount := 0
	for _, node := range c.nodeHealthData {
		if node.ResponseTimeMs > 0 {
			total += float64(node.ResponseTimeMs)
			validCount++
		}
	}
	
	if validCount == 0 {
		return 0.0
	}
	
	return total / float64(validCount)
}

// getNodesWithRpcs returns a list of nodes with their RPC information
func (c *EnhancedChartService) getNodesWithRpcs() []interface{} {
	c.healthMu.RLock()
	defer c.healthMu.RUnlock()
	
	nodes := make([]interface{}, 0, len(c.nodeHealthData))
	for _, node := range c.nodeHealthData {
		nodeInfo := map[string]interface{}{
			"chainId":    node.ChainID,
			"chainName":  node.ChainName,
			"rpcUrl":     node.RpcURL,
			"rpcType":    node.RpcType,
			"status":     node.Status,
			"responseTime": node.ResponseTimeMs,
			"blockHeight": node.LatestBlockHeight,
			"uptime":     node.Uptime,
			"lastCheck":  node.LastCheckTime,
		}
		if node.ErrorMessage != "" {
			nodeInfo["error"] = node.ErrorMessage
		}
		nodes = append(nodes, nodeInfo)
	}
	
	return nodes
}

// getChainHealthStats returns health statistics grouped by chain
func (c *EnhancedChartService) getChainHealthStats() map[string]interface{} {
	c.healthMu.RLock()
	defer c.healthMu.RUnlock()
	
	chainStats := make(map[string]interface{})
	chainCounts := make(map[string]map[string]int)
	
	// Initialize chain counts
	for _, node := range c.nodeHealthData {
		if _, exists := chainCounts[node.ChainID]; !exists {
			chainCounts[node.ChainID] = map[string]int{
				"healthy":   0,
				"degraded":  0,
				"unhealthy": 0,
				"total":     0,
			}
		}
		
		chainCounts[node.ChainID]["total"]++
		
		switch node.Status {
		case "healthy", "active":
			chainCounts[node.ChainID]["healthy"]++
		case "degraded", "warning":
			chainCounts[node.ChainID]["degraded"]++
		case "unhealthy", "error", "down":
			chainCounts[node.ChainID]["unhealthy"]++
		}
	}
	
	// Build chain stats
	for chainID, counts := range chainCounts {
		// Find chain name
		chainName := chainID
		for _, node := range c.nodeHealthData {
			if node.ChainID == chainID && node.ChainName != "" {
				chainName = node.ChainName
				break
			}
		}
		
		chainStats[chainID] = map[string]interface{}{
			"chainName":      chainName,
			"totalNodes":     counts["total"],
			"healthyNodes":   counts["healthy"],
			"degradedNodes":  counts["degraded"],
			"unhealthyNodes": counts["unhealthy"],
			"healthScore":    float64(counts["healthy"]) / float64(counts["total"]) * 100,
		}
	}
	
	return chainStats
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
	
	// PostgreSQL insert statement
	insertSQL := `
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
	
	stmt, err := c.db.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to prepare latency insert statement: %w", err)
	}
	defer stmt.Close()
	
	now := time.Now() // PostgreSQL expects timestamp
	
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

// GetCacheStats returns cache performance statistics
func (c *EnhancedChartService) GetCacheStats() map[string]interface{} {
	stats := map[string]interface{}{
		"cacheStrategy":   "always_serve_cached",
		"hasCachedData":   len(c.cache) > 0,
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
	
	// PostgreSQL insert statement
	insertQuery := `
		INSERT INTO node_health 
		(chain_id, chain_name, rpc_url, rpc_type, status, response_time_ms, 
		 latest_block_height, error_message, uptime, checked_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (rpc_url, checked_at) DO UPDATE SET
			status = EXCLUDED.status,
			response_time_ms = EXCLUDED.response_time_ms,
			latest_block_height = EXCLUDED.latest_block_height,
			error_message = EXCLUDED.error_message,
			uptime = EXCLUDED.uptime`
	
	stmt, err := c.db.Prepare(insertQuery)
	if err != nil {
		return fmt.Errorf("failed to prepare insert statement: %w", err)
	}
	defer stmt.Close()
	
	for _, health := range healthData {
		checkTime := time.Unix(health.LastCheckTime, 0) // Convert Unix timestamp to time.Time for PostgreSQL
		
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

// hasNodeHealthDataForPeriod checks if we have node health data for a given period (for in-memory data)
func (c *EnhancedChartService) hasNodeHealthDataForPeriod(period time.Duration) bool {
	c.healthMu.RLock()
	defer c.healthMu.RUnlock()
	
	cutoff := time.Now().Add(-period)
	for _, node := range c.nodeHealthData {
		nodeCheckTime := time.Unix(node.LastCheckTime, 0)
		if nodeCheckTime.After(cutoff) {
			return true
		}
	}
	return false
}

// getLatencyDataFromDatabase retrieves the latest latency data from database
func (c *EnhancedChartService) getLatencyDataFromDatabase() []interface{} {
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
		utils.LogError("CHART_SERVICE", "Failed to query latency data: %v", err)
		return []interface{}{}
	}
	defer rows.Close()
	
	var latencyData []interface{}
	for rows.Next() {
		var sourceChain, destChain, sourceName, destName string
		var packetAckP5, packetAckMedian, packetAckP95 float64
		var packetRecvP5, packetRecvMedian, packetRecvP95 float64
		var writeAckP5, writeAckMedian, writeAckP95 float64
		
		err := rows.Scan(
			&sourceChain, &destChain, &sourceName, &destName,
			&packetAckP5, &packetAckMedian, &packetAckP95,
			&packetRecvP5, &packetRecvMedian, &packetRecvP95,
			&writeAckP5, &writeAckMedian, &writeAckP95,
		)
		
		if err != nil {
			utils.LogError("CHART_SERVICE", "Failed to scan latency row: %v", err)
			continue
		}
		
		latency := map[string]interface{}{
			"sourceChain":      sourceChain,
			"destinationChain": destChain,
			"sourceName":       sourceName,
			"destinationName":  destName,
			"packetAck": map[string]interface{}{
				"p5":     packetAckP5,
				"median": packetAckMedian,
				"p95":    packetAckP95,
			},
			"packetRecv": map[string]interface{}{
				"p5":     packetRecvP5,
				"median": packetRecvMedian,
				"p95":    packetRecvP95,
			},
			"writeAck": map[string]interface{}{
				"p5":     writeAckP5,
				"median": writeAckMedian,
				"p95":    writeAckP95,
			},
		}
		
		latencyData = append(latencyData, latency)
	}
	
	utils.LogDebug("CHART_SERVICE", "Retrieved %d latency records from database", len(latencyData))
	return latencyData
}

// getNodeHealthDataFromDatabase retrieves node health data from database
func (c *EnhancedChartService) getNodeHealthDataFromDatabase() map[string]interface{} {
	// Get latest health data
	query := `
		SELECT 
			nh1.chain_id, nh1.chain_name, nh1.rpc_url, nh1.rpc_type, 
			nh1.status, nh1.response_time_ms, nh1.latest_block_height, 
			nh1.error_message, nh1.uptime, nh1.checked_at
		FROM node_health nh1
		INNER JOIN (
			SELECT rpc_url, MAX(checked_at) as max_checked
			FROM node_health 
			WHERE checked_at > NOW() - INTERVAL '1 hour'
			GROUP BY rpc_url
		) nh2 ON nh1.rpc_url = nh2.rpc_url 
			AND nh1.checked_at = nh2.max_checked
		ORDER BY nh1.chain_name, nh1.rpc_url`
	
	rows, err := c.db.Query(query)
	if err != nil {
		utils.LogError("CHART_SERVICE", "Failed to query node health data: %v", err)
		return map[string]interface{}{
			"totalNodes":       0,
			"healthyNodes":     0,
			"degradedNodes":    0,
			"unhealthyNodes":   0,
			"avgResponseTime":  0.0,
			"nodesWithRpcs":    []interface{}{},
			"chainHealthStats": map[string]interface{}{},
			"dataAvailability": map[string]interface{}{
				"hasMinute": false,
				"hasHour":   false,
				"hasDay":    false,
				"has7Days":  false,
				"has14Days": false,
				"has30Days": false,
			},
		}
	}
	defer rows.Close()
	
	var nodes []interface{}
	totalNodes := 0
	healthyNodes := 0
	degradedNodes := 0
	unhealthyNodes := 0
	totalResponseTime := 0
	responseTimeCount := 0
	chainStats := make(map[string]map[string]interface{})
	
	for rows.Next() {
		var chainID, chainName, rpcURL, rpcType, status, errorMessage string
		var responseTimeMs int
		var blockHeight *int64
		var uptime float64
		var checkedAt time.Time
		
		err := rows.Scan(&chainID, &chainName, &rpcURL, &rpcType, &status, 
			&responseTimeMs, &blockHeight, &errorMessage, &uptime, &checkedAt)
		if err != nil {
			utils.LogError("CHART_SERVICE", "Failed to scan node health row: %v", err)
			continue
		}
		
		totalNodes++
		
		// Count by status
		switch status {
		case "healthy":
			healthyNodes++
			if responseTimeMs > 0 {
				totalResponseTime += responseTimeMs
				responseTimeCount++
			}
		case "degraded":
			degradedNodes++
			if responseTimeMs > 0 {
				totalResponseTime += responseTimeMs
				responseTimeCount++
			}
		case "unhealthy":
			unhealthyNodes++
		}
		
		// Build node info
		nodeInfo := map[string]interface{}{
			"chainId":      chainID,
			"chainName":    chainName,
			"rpcUrl":       rpcURL,
			"rpcType":      rpcType,
			"status":       status,
			"responseTime": responseTimeMs,
			"blockHeight":  blockHeight,
			"uptime":       uptime,
			"lastCheck":    checkedAt.Unix(),
		}
		if errorMessage != "" {
			nodeInfo["error"] = errorMessage
		}
		nodes = append(nodes, nodeInfo)
		
		// Aggregate chain stats
		if _, exists := chainStats[chainID]; !exists {
			chainStats[chainID] = map[string]interface{}{
				"chainName":      chainName,
				"totalNodes":     0,
				"healthyNodes":   0,
				"degradedNodes":  0,
				"unhealthyNodes": 0,
				"healthScore":    0.0,
			}
		}
		
		stat := chainStats[chainID]
		stat["totalNodes"] = stat["totalNodes"].(int) + 1
		
		switch status {
		case "healthy":
			stat["healthyNodes"] = stat["healthyNodes"].(int) + 1
		case "degraded":
			stat["degradedNodes"] = stat["degradedNodes"].(int) + 1
		case "unhealthy":
			stat["unhealthyNodes"] = stat["unhealthyNodes"].(int) + 1
		}
		
		// Update health score
		total := stat["totalNodes"].(int)
		healthy := stat["healthyNodes"].(int)
		if total > 0 {
			stat["healthScore"] = float64(healthy) / float64(total) * 100
		}
	}
	
	// Calculate average response time
	avgResponseTime := 0.0
	if responseTimeCount > 0 {
		avgResponseTime = float64(totalResponseTime) / float64(responseTimeCount)
	}
	
	utils.LogDebug("CHART_SERVICE", "Retrieved node health summary: %d nodes (%d healthy, %d degraded, %d unhealthy)", 
		totalNodes, healthyNodes, degradedNodes, unhealthyNodes)
	
	return map[string]interface{}{
		"totalNodes":       totalNodes,
		"healthyNodes":     healthyNodes,
		"degradedNodes":    degradedNodes,
		"unhealthyNodes":   unhealthyNodes,
		"avgResponseTime":  avgResponseTime,
		"nodesWithRpcs":    nodes,
		"chainHealthStats": chainStats,
		"dataAvailability": map[string]interface{}{
			"hasMinute": totalNodes > 0,
			"hasHour":   totalNodes > 0,
			"hasDay":    c.hasNodeHealthDataForDatabasePeriod(24 * time.Hour),
			"has7Days":  c.hasNodeHealthDataForDatabasePeriod(7 * 24 * time.Hour),
			"has14Days": c.hasNodeHealthDataForDatabasePeriod(14 * 24 * time.Hour),
			"has30Days": c.hasNodeHealthDataForDatabasePeriod(30 * 24 * time.Hour),
		},
	}
}

// hasNodeHealthDataForDatabasePeriod checks if we have node health data for a given period in database
func (c *EnhancedChartService) hasNodeHealthDataForDatabasePeriod(period time.Duration) bool {
	query := `SELECT COUNT(*) FROM node_health WHERE checked_at > $1`
	cutoff := time.Now().Add(-period)
	
	var count int
	err := c.db.QueryRow(query, cutoff).Scan(&count)
	if err != nil {
		return false
	}
	return count > 0
} 