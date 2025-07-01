package storage

import (
	"context"
	"fmt"
	"time"
	"websocket-backend/internal/utils"
)

// BroadcasterInterface defines the interface for broadcasting chart data
type BroadcasterInterface interface {
	BroadcastChartData(data interface{})
}

// LatencyServiceInterface defines the interface for latency data access
type LatencyServiceInterface interface {
	GetLatencyDataInterface() []interface{} // Returns []models.LatencyData but we use interface{} to avoid import cycle
}

// NodeHealthServiceInterface defines the interface for node health data access
type NodeHealthServiceInterface interface {
	GetHealthDataInterface() []interface{} // Returns []models.NodeHealthData but we use interface{} to avoid import cycle
}

// ChartBroadcaster handles broadcasting chart data updates to WebSocket clients
type ChartBroadcaster struct {
	clickhouseService     *ClickHouseService
	broadcasterInstance   BroadcasterInterface
	latencyService        LatencyServiceInterface
	nodeHealthService     NodeHealthServiceInterface
}

// NewChartBroadcaster creates a new chart broadcaster with ClickHouse
func NewChartBroadcaster(clickhouseService *ClickHouseService, broadcasterInstance BroadcasterInterface, latencyService LatencyServiceInterface, nodeHealthService NodeHealthServiceInterface) *ChartBroadcaster {
	return &ChartBroadcaster{
		clickhouseService:     clickhouseService,
		broadcasterInstance:   broadcasterInstance,
		latencyService:        latencyService,
		nodeHealthService:     nodeHealthService,
	}
}

// BroadcastChartUpdate broadcasts chart data to all connected clients (cache-first)
// This method now primarily serves from cache (6-minute TTL) populated by staggered updates
func (cb *ChartBroadcaster) BroadcastChartUpdate(ctx context.Context) {
	utils.LogDebug("CHART_BROADCASTER", "Broadcasting chart data update (cache-first) via ClickHouse")
	
	// Get optimized chart data from ClickHouse
	transferRates, err := cb.clickhouseService.GetTransferRates(ctx)
	if err != nil {
		utils.LogError("CHART_BROADCASTER", "Failed to get transfer rates from ClickHouse: %v", err)
		return
	}
	
	activeWalletRates, err := cb.clickhouseService.GetActiveWalletRates(ctx)
	if err != nil {
		utils.LogError("CHART_BROADCASTER", "Failed to get active wallet rates from ClickHouse: %v", err)
		return
	}
	
	popularRoutes, err := cb.clickhouseService.GetPopularRoutes(ctx, 20, "7d")
	if err != nil {
		utils.LogError("CHART_BROADCASTER", "Failed to get popular routes from ClickHouse: %v", err)
		popularRoutes = []FrontendRouteData{} // Default to empty
	}
	
	// Get popular routes for all timeframes (for timeScale data)
	timeframes := []string{"5m", "1h", "1d", "7d", "14d", "30d"}
	popularRoutesTimeScale := make(map[string][]FrontendRouteData)
	for _, tf := range timeframes {
		routes, err := cb.clickhouseService.GetPopularRoutes(ctx, 20, tf)
		if err != nil {
			utils.LogDebug("CHART_BROADCASTER", "Failed to get routes for timeframe %s: %v", tf, err)
			routes = []FrontendRouteData{}
		}
		popularRoutesTimeScale[tf] = routes
	}
	
	// Get chain flow data
	chainFlowData, err := cb.clickhouseService.GetChainFlowData(ctx, "1d")
	if err != nil {
		utils.LogError("CHART_BROADCASTER", "Failed to get chain flow data from ClickHouse: %v", err)
		// Create empty chain flow data
		chainFlowData = &FrontendChainFlowData{
			Chains:              []FrontendChainData{},
			ChainFlowTimeScale:  make(map[string][]FrontendChainData),
			TotalOutgoing:       0,
			TotalIncoming:       0,
			ServerUptimeSeconds: 0,
		}
	}
	
	// Get chain flow data for all timeframes (for timeScale data)
	chainFlowTimeScale := make(map[string][]FrontendChainData)
	for _, tf := range timeframes {
		chainData, err := cb.clickhouseService.GetChainFlowData(ctx, tf)
		if err != nil {
			utils.LogDebug("CHART_BROADCASTER", "Failed to get chain flow for timeframe %s: %v", tf, err)
			chainFlowTimeScale[tf] = []FrontendChainData{}
		} else {
			chainFlowTimeScale[tf] = chainData.Chains
		}
	}
	
	// Update chain flow data with timeScale
	chainFlowData.ChainFlowTimeScale = chainFlowTimeScale
	
	// Get wallet activity for default timeframe (last hour)
	activeSenders, err := cb.clickhouseService.GetTopSenders(ctx, 10, "1h")
	if err != nil {
		utils.LogError("CHART_BROADCASTER", "Failed to get active senders: %v", err)
		activeSenders = []FrontendWalletData{} // Default to empty
	}
	
	activeReceivers, err := cb.clickhouseService.GetTopReceivers(ctx, 10, "1h")
	if err != nil {
		utils.LogError("CHART_BROADCASTER", "Failed to get active receivers: %v", err)
		activeReceivers = []FrontendWalletData{} // Default to empty
	}
	
	// Get wallet activity for all timeframes (for timeScale data)
	activeSendersTimeScale := make(map[string][]FrontendWalletData)
	activeReceiversTimeScale := make(map[string][]FrontendWalletData)
	
	for _, tf := range timeframes {
		senders, err := cb.clickhouseService.GetTopSenders(ctx, 10, tf)
		if err != nil {
			utils.LogDebug("CHART_BROADCASTER", "Failed to get senders for timeframe %s: %v", tf, err)
			senders = []FrontendWalletData{}
		}
		activeSendersTimeScale[tf] = senders
		
		receivers, err := cb.clickhouseService.GetTopReceivers(ctx, 10, tf)
		if err != nil {
			utils.LogDebug("CHART_BROADCASTER", "Failed to get receivers for timeframe %s: %v", tf, err)
			receivers = []FrontendWalletData{}
		}
		activeReceiversTimeScale[tf] = receivers
	}
	
	// Get asset volume data
	utils.LogInfo("CHART_BROADCASTER", "🔍 ATTEMPTING to fetch asset volume data...")
	assetVolumeData, err := cb.clickhouseService.GetAssetVolumes(ctx, "1h")
	if err != nil {
		utils.LogError("CHART_BROADCASTER", "❌ FAILED to get asset volumes from ClickHouse: %v", err)
		// Don't return - continue with empty asset data
		assetVolumeData = &FrontendAssetVolumeData{
			Assets:               []FrontendAsset{},
			AssetVolumeTimeScale: make(map[string][]FrontendAsset),
			TotalAssets:          0,
			TotalVolume:          0,
			TotalTransfers:       0,
			ServerUptimeSeconds:  0,
		}
		utils.LogError("CHART_BROADCASTER", "❌ Using empty asset data due to error")
	} else {
		utils.LogInfo("CHART_BROADCASTER", "✅ Successfully fetched asset volume data: %d assets, total volume: %.2f", 
			len(assetVolumeData.Assets), assetVolumeData.TotalVolume)
	}
	
	// Get asset volume data for all timeframes (for timeScale data)
	assetVolumeTimeScale := make(map[string][]FrontendAsset)
	for _, tf := range timeframes {
		utils.LogInfo("CHART_BROADCASTER", "🔍 FETCHING asset volumes for timeframe: %s", tf)
		assetData, err := cb.clickhouseService.GetAssetVolumes(ctx, tf)
		if err != nil {
			utils.LogError("CHART_BROADCASTER", "❌ FAILED to get asset volumes for timeframe %s: %v", tf, err)
			assetVolumeTimeScale[tf] = []FrontendAsset{}
		} else {
			assetVolumeTimeScale[tf] = assetData.Assets
			utils.LogInfo("CHART_BROADCASTER", "✅ TIMEFRAME DEBUG: %s has %d assets", tf, len(assetData.Assets))
			if len(assetData.Assets) == 0 {
				utils.LogWarn("CHART_BROADCASTER", "⚠️  EMPTY RESULT for timeframe %s - no assets found", tf)
			} else {
				// Debug: Show top 3 assets for this timeframe
				for i, asset := range assetData.Assets {
					if i < 3 {
						utils.LogInfo("CHART_BROADCASTER", "🔍 [%s] Top asset #%d: %s (count=%d, vol=%.2f)", 
							tf, i+1, asset.AssetSymbol, asset.TransferCount, asset.TotalVolume)
					}
				}
			}
		}
	}
	
	// Check for potential duplicate data across timeframes
	timeframeDataHashes := make(map[string]string)
	for tf, assets := range assetVolumeTimeScale {
		if len(assets) > 0 {
			// Create a simple hash of the first 3 assets to detect duplicates
			hashData := ""
			for i, asset := range assets {
				if i < 3 {
					hashData += fmt.Sprintf("%s:%d:", asset.AssetSymbol, asset.TransferCount)
				}
			}
			timeframeDataHashes[tf] = hashData
		}
	}
	
	// Check for identical data patterns between longer timeframes (this can be normal)
	utils.LogDebug("CHART_BROADCASTER", "🔍 CHECKING for identical data across timeframes...")
	duplicateCount := 0
	for tf1, hash1 := range timeframeDataHashes {
		for tf2, hash2 := range timeframeDataHashes {
			if tf1 != tf2 && hash1 == hash2 && hash1 != "" {
				// Only warn for unexpected duplicates (shorter timeframe = longer timeframe)
				timeframeOrder := map[string]int{"5m": 1, "1h": 2, "1d": 3, "7d": 4, "14d": 5, "30d": 6}
				if timeframeOrder[tf1] < timeframeOrder[tf2] {
					utils.LogDebug("CHART_BROADCASTER", "📊 DATA INSIGHT: Timeframe %s and %s have identical top assets (likely due to recent activity concentration)", tf1, tf2)
					duplicateCount++
				}
			}
		}
	}
	
	if duplicateCount > 2 {
		utils.LogInfo("CHART_BROADCASTER", "📈 ACTIVITY PATTERN: %d timeframe pairs show identical data - indicates recent activity concentration", duplicateCount)
	}
	
	// Update asset volume data with timeScale
	assetVolumeData.AssetVolumeTimeScale = assetVolumeTimeScale
	
	// Log the final timeScale structure for debugging
	utils.LogInfo("CHART_BROADCASTER", "🔍 FINAL TIMESCALE DEBUG: Asset volume timeScale structure:")
	for tf, assets := range assetVolumeData.AssetVolumeTimeScale {
		utils.LogInfo("CHART_BROADCASTER", "  → %s: %d assets", tf, len(assets))
		if len(assets) > 0 {
			utils.LogInfo("CHART_BROADCASTER", "    First asset: %s (%d transfers)", assets[0].AssetSymbol, assets[0].TransferCount)
		}
	}
	
	// Get latency data
	var latencyData []interface{}
	if cb.latencyService != nil {
		latencyData = cb.latencyService.GetLatencyDataInterface()
		utils.LogInfo("CHART_BROADCASTER", "🔍 LATENCY DEBUG: Retrieved %d latency data points", len(latencyData))
	} else {
		utils.LogWarn("CHART_BROADCASTER", "⚠️  No latency service available")
		latencyData = []interface{}{}
	}
	
	// Get node health data
	var nodeHealthData interface{}
	if cb.nodeHealthService != nil {
		// Get the summary object directly, not as an array
		if healthDataArray := cb.nodeHealthService.GetHealthDataInterface(); len(healthDataArray) > 0 {
			nodeHealthData = healthDataArray[0] // Extract the first (and only) summary object
			utils.LogInfo("CHART_BROADCASTER", "🔍 NODE HEALTH DEBUG: Retrieved node health summary data")
		} else {
			nodeHealthData = nil
			utils.LogWarn("CHART_BROADCASTER", "⚠️  No node health data available")
		}
	} else {
		utils.LogWarn("CHART_BROADCASTER", "⚠️  No node health service available")
		nodeHealthData = nil
	}
	
	// Build comprehensive chart data payload
	chartData := map[string]interface{}{
		"currentRates":             transferRates,             // Frontend expects "currentRates"
		"activeWalletRates":        activeWalletRates,        // Frontend expects "activeWalletRates"
		"popularRoutes":            popularRoutes,
		"popularRoutesTimeScale":   popularRoutesTimeScale,   // Popular routes by timeframe
		"activeSenders":            activeSenders,            // Individual wallet data
		"activeReceivers":          activeReceivers,          // Individual wallet data
		"activeSendersTimeScale":   activeSendersTimeScale,   // Wallet data by timeframe
		"activeReceiversTimeScale": activeReceiversTimeScale, // Wallet data by timeframe
		"chainFlowData":            chainFlowData,            // Chain flow data with timeScale
		"assetVolumeData":          assetVolumeData,          // Asset volume data with timeScale
		"latencyData":              latencyData,              // Latency data for cross-chain monitoring
		"nodeHealthData":           nodeHealthData,           // Node health data for monitoring
		"lastUpdated":              time.Now().Format("2006-01-02 15:04:05"),
		"dataSource":               "clickhouse",
		"cached":                   false, // Indicate this was freshly built
	}
	
	utils.LogInfo("CHART_BROADCASTER", "🔍 DEBUG: Chart data payload includes assetVolumeData with %d assets", 
		len(assetVolumeData.Assets))
	
	// Broadcast to all WebSocket clients via the broadcaster interface
	if cb.broadcasterInstance != nil {
		cb.broadcasterInstance.BroadcastChartData(chartData)
	}
	
	utils.LogDebug("CHART_BROADCASTER", "Chart data broadcast completed (cache-first) via ClickHouse")
}

// UpdateChartGroupA updates the first group of charts (rates, routes, chains)
func (cb *ChartBroadcaster) UpdateChartGroupA(ctx context.Context) {
	utils.LogInfo("CHART_BROADCASTER", "Updating chart group A: rates, routes, chains")
	
	// Group A: Transfer rates, wallet rates, popular routes, chain flows
	timeframes := []string{"5m", "1h", "1d", "7d", "14d", "30d"}
	
	// Update transfer rates (cache TTL: 6 minutes)
	if _, err := cb.clickhouseService.GetTransferRates(ctx); err != nil {
		utils.LogError("CHART_BROADCASTER", "Failed to update transfer rates: %v", err)
	}
	
	// Update active wallet rates (cache TTL: 6 minutes)
	if _, err := cb.clickhouseService.GetActiveWalletRates(ctx); err != nil {
		utils.LogError("CHART_BROADCASTER", "Failed to update active wallet rates: %v", err)
	}
	
	// Update popular routes for all timeframes (cache TTL: 6 minutes)
	for _, tf := range timeframes {
		if _, err := cb.clickhouseService.GetPopularRoutes(ctx, 20, tf); err != nil {
			utils.LogDebug("CHART_BROADCASTER", "Failed to update popular routes for %s: %v", tf, err)
		}
	}
	
	// Update chain flow data for all timeframes (cache TTL: 6 minutes)
	for _, tf := range timeframes {
		if _, err := cb.clickhouseService.GetChainFlowData(ctx, tf); err != nil {
			utils.LogDebug("CHART_BROADCASTER", "Failed to update chain flows for %s: %v", tf, err)
		}
	}
	
	utils.LogInfo("CHART_BROADCASTER", "Chart group A update completed")
}

// UpdateChartGroupB updates the second group of charts (assets, senders, receivers)
func (cb *ChartBroadcaster) UpdateChartGroupB(ctx context.Context) {
	utils.LogInfo("CHART_BROADCASTER", "Updating chart group B: assets, senders, receivers")
	
	// Group B: Asset volumes, active senders, active receivers
	timeframes := []string{"5m", "1h", "1d", "7d", "14d", "30d"}
	
	// Update asset volumes for all timeframes (cache TTL: 6 minutes)
	for _, tf := range timeframes {
		if _, err := cb.clickhouseService.GetAssetVolumes(ctx, tf); err != nil {
			utils.LogError("CHART_BROADCASTER", "Failed to update asset volumes for %s: %v", tf, err)
		}
	}
	
	// Update active senders for all timeframes (cache TTL: 6 minutes)
	for _, tf := range timeframes {
		if _, err := cb.clickhouseService.GetTopSenders(ctx, 10, tf); err != nil {
			utils.LogDebug("CHART_BROADCASTER", "Failed to update active senders for %s: %v", tf, err)
		}
	}
	
	// Update active receivers for all timeframes (cache TTL: 6 minutes)
	for _, tf := range timeframes {
		if _, err := cb.clickhouseService.GetTopReceivers(ctx, 10, tf); err != nil {
			utils.LogDebug("CHART_BROADCASTER", "Failed to update active receivers for %s: %v", tf, err)
		}
	}
	
	utils.LogInfo("CHART_BROADCASTER", "Chart group B update completed")
} 