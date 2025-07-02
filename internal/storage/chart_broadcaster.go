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

// BroadcastChartUpdate broadcasts chart data to all connected clients (PURE cache-first)
// This method reads ONLY from cache populated by staggered updates - never hits database
func (cb *ChartBroadcaster) BroadcastChartUpdate(ctx context.Context) {
	utils.LogDebug("CHART_BROADCASTER", "Broadcasting chart data update (PURE cache-first)")
	
	// Get cache instance from ClickHouse service
	cache := cb.clickhouseService.GetCache()
	
	// Read transfer rates from cache ONLY
	var transferRates *FrontendTransferRates
	if data, hit := cache.Get("transfer_rates"); hit {
		transferRates = data.(*FrontendTransferRates)
		utils.LogDebug("CHART_BROADCASTER", "✅ Transfer rates cache HIT")
	} else {
		utils.LogWarn("CHART_BROADCASTER", "❌ Transfer rates cache MISS - using empty data")
		transferRates = &FrontendTransferRates{} // Empty fallback
	}
	
	// Read active wallet rates from cache ONLY
	var activeWalletRates *ActiveWalletRates
	if data, hit := cache.Get("active_wallet_rates"); hit {
		activeWalletRates = data.(*ActiveWalletRates)
		utils.LogDebug("CHART_BROADCASTER", "✅ Active wallet rates cache HIT")
	} else {
		utils.LogWarn("CHART_BROADCASTER", "❌ Active wallet rates cache MISS - using empty data")
		activeWalletRates = &ActiveWalletRates{} // Empty fallback
	}
	
	// Read popular routes from cache ONLY
	var popularRoutes []FrontendRouteData
	if data, hit := cache.Get("popular_routes_7d_20"); hit {
		popularRoutes = data.([]FrontendRouteData)
		utils.LogDebug("CHART_BROADCASTER", "✅ Popular routes cache HIT")
	} else {
		utils.LogWarn("CHART_BROADCASTER", "❌ Popular routes cache MISS - using empty data")
		popularRoutes = []FrontendRouteData{} // Empty fallback
	}
	
	// Read popular routes for all timeframes from cache ONLY
	timeframes := []string{"5m", "1h", "1d", "7d", "14d", "30d"}
	popularRoutesTimeScale := make(map[string][]FrontendRouteData)
	for _, tf := range timeframes {
		cacheKey := fmt.Sprintf("popular_routes_%s_20", tf)
		if data, hit := cache.Get(cacheKey); hit {
			popularRoutesTimeScale[tf] = data.([]FrontendRouteData)
			utils.LogDebug("CHART_BROADCASTER", "✅ Popular routes %s cache HIT", tf)
		} else {
			utils.LogDebug("CHART_BROADCASTER", "❌ Popular routes %s cache MISS", tf)
			popularRoutesTimeScale[tf] = []FrontendRouteData{}
		}
	}
	
	// Read chain flow data from cache ONLY
	var chainFlowData *FrontendChainFlowData
	if data, hit := cache.Get("chain_flow_data_1d"); hit {
		chainFlowData = data.(*FrontendChainFlowData)
		utils.LogDebug("CHART_BROADCASTER", "✅ Chain flow data cache HIT")
	} else {
		utils.LogWarn("CHART_BROADCASTER", "❌ Chain flow data cache MISS - using empty data")
		chainFlowData = &FrontendChainFlowData{
			Chains:              []FrontendChainData{},
			ChainFlowTimeScale:  make(map[string][]FrontendChainData),
			TotalOutgoing:       0,
			TotalIncoming:       0,
			ServerUptimeSeconds: 0,
		}
	}
	
	// Read chain flow data for all timeframes from cache ONLY
	chainFlowTimeScale := make(map[string][]FrontendChainData)
	for _, tf := range timeframes {
		cacheKey := fmt.Sprintf("chain_flow_data_%s", tf)
		if data, hit := cache.Get(cacheKey); hit {
			if chainData, ok := data.(*FrontendChainFlowData); ok {
				chainFlowTimeScale[tf] = chainData.Chains
				utils.LogDebug("CHART_BROADCASTER", "✅ Chain flow %s cache HIT", tf)
			} else {
				chainFlowTimeScale[tf] = []FrontendChainData{}
			}
		} else {
			utils.LogDebug("CHART_BROADCASTER", "❌ Chain flow %s cache MISS", tf)
			chainFlowTimeScale[tf] = []FrontendChainData{}
		}
	}
	
	// Update chain flow data with timeScale
	chainFlowData.ChainFlowTimeScale = chainFlowTimeScale
	
	// Read wallet activity for default timeframe from cache ONLY
	var activeSenders []FrontendWalletData
	if data, hit := cache.Get("top_senders_1h_10"); hit {
		activeSenders = data.([]FrontendWalletData)
		utils.LogDebug("CHART_BROADCASTER", "✅ Active senders cache HIT")
	} else {
		utils.LogWarn("CHART_BROADCASTER", "❌ Active senders cache MISS - using empty data")
		activeSenders = []FrontendWalletData{}
	}
	
	var activeReceivers []FrontendWalletData
	if data, hit := cache.Get("top_receivers_1h_10"); hit {
		activeReceivers = data.([]FrontendWalletData)
		utils.LogDebug("CHART_BROADCASTER", "✅ Active receivers cache HIT")
	} else {
		utils.LogWarn("CHART_BROADCASTER", "❌ Active receivers cache MISS - using empty data")
		activeReceivers = []FrontendWalletData{}
	}
	
	// Read wallet activity for all timeframes from cache ONLY
	activeSendersTimeScale := make(map[string][]FrontendWalletData)
	activeReceiversTimeScale := make(map[string][]FrontendWalletData)
	
	for _, tf := range timeframes {
		// Read senders from cache
		sendersCacheKey := fmt.Sprintf("top_senders_%s_10", tf)
		if data, hit := cache.Get(sendersCacheKey); hit {
			activeSendersTimeScale[tf] = data.([]FrontendWalletData)
			utils.LogDebug("CHART_BROADCASTER", "✅ Senders %s cache HIT", tf)
		} else {
			utils.LogDebug("CHART_BROADCASTER", "❌ Senders %s cache MISS", tf)
			activeSendersTimeScale[tf] = []FrontendWalletData{}
		}
		
		// Read receivers from cache
		receiversCacheKey := fmt.Sprintf("top_receivers_%s_10", tf)
		if data, hit := cache.Get(receiversCacheKey); hit {
			activeReceiversTimeScale[tf] = data.([]FrontendWalletData)
			utils.LogDebug("CHART_BROADCASTER", "✅ Receivers %s cache HIT", tf)
		} else {
			utils.LogDebug("CHART_BROADCASTER", "❌ Receivers %s cache MISS", tf)
			activeReceiversTimeScale[tf] = []FrontendWalletData{}
		}
	}
	
	// Read asset volume data from cache ONLY
	var assetVolumeData *FrontendAssetVolumeData
	if data, hit := cache.Get("asset_volumes_1h"); hit {
		assetVolumeData = data.(*FrontendAssetVolumeData)
		utils.LogInfo("CHART_BROADCASTER", "✅ Asset volume data cache HIT: %d assets, total volume: %.2f", 
			len(assetVolumeData.Assets), assetVolumeData.TotalVolume)
	} else {
		utils.LogWarn("CHART_BROADCASTER", "❌ Asset volume data cache MISS - using empty data")
		assetVolumeData = &FrontendAssetVolumeData{
			Assets:               []FrontendAsset{},
			AssetVolumeTimeScale: make(map[string][]FrontendAsset),
			TotalAssets:          0,
			TotalVolume:          0,
			TotalTransfers:       0,
			ServerUptimeSeconds:  0,
		}
	}
	
	// Read asset volume data for all timeframes from cache ONLY
	assetVolumeTimeScale := make(map[string][]FrontendAsset)
	for _, tf := range timeframes {
		cacheKey := fmt.Sprintf("asset_volumes_%s", tf)
		if data, hit := cache.Get(cacheKey); hit {
			if assetData, ok := data.(*FrontendAssetVolumeData); ok {
				assetVolumeTimeScale[tf] = assetData.Assets
				utils.LogInfo("CHART_BROADCASTER", "✅ Asset volumes %s cache HIT: %d assets", tf, len(assetData.Assets))
				if len(assetData.Assets) > 0 {
					// Debug: Show top 3 assets for this timeframe
					for i, asset := range assetData.Assets {
						if i < 3 {
							utils.LogDebug("CHART_BROADCASTER", "[%s] Top asset #%d: %s (count=%d, vol=%.2f)", 
								tf, i+1, asset.AssetSymbol, asset.TransferCount, asset.TotalVolume)
						}
					}
				}
			} else {
				assetVolumeTimeScale[tf] = []FrontendAsset{}
			}
		} else {
			utils.LogDebug("CHART_BROADCASTER", "❌ Asset volumes %s cache MISS", tf)
			assetVolumeTimeScale[tf] = []FrontendAsset{}
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
	
	// Get latency data from cache ONLY (no direct service calls)
	var latencyData []interface{}
	if data, hit := cache.Get("latency_data"); hit {
		latencyData = data.([]interface{})
		utils.LogInfo("CHART_BROADCASTER", "✅ Latency data cache HIT: %d data points", len(latencyData))
	} else {
		utils.LogWarn("CHART_BROADCASTER", "❌ Latency data cache MISS - using empty data")
		latencyData = []interface{}{} // Safe fallback
	}
	
	// Get node health data from cache ONLY (no direct service calls)
	var nodeHealthData interface{}
	if data, hit := cache.Get("node_health_data"); hit {
		nodeHealthData = data
		utils.LogInfo("CHART_BROADCASTER", "✅ Node health data cache HIT")
	} else {
		utils.LogWarn("CHART_BROADCASTER", "❌ Node health data cache MISS - using empty data")
		nodeHealthData = nil // Safe fallback
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
		"cached":                   true,  // Indicate this was built from cache
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
	successfulAssetUpdates := 0
	for _, tf := range timeframes {
		if _, err := cb.clickhouseService.GetAssetVolumes(ctx, tf); err != nil {
			utils.LogError("CHART_BROADCASTER", "❌ Failed to update asset volumes for %s: %v", tf, err)
		} else {
			successfulAssetUpdates++
		}
	}
	utils.LogInfo("CHART_BROADCASTER", "✅ Asset volumes: %d/%d timeframes updated successfully", successfulAssetUpdates, len(timeframes))
	
	// Update active senders for all timeframes (cache TTL: 6 minutes)
	successfulSenderUpdates := 0
	for _, tf := range timeframes {
		if _, err := cb.clickhouseService.GetTopSenders(ctx, 10, tf); err != nil {
			utils.LogError("CHART_BROADCASTER", "❌ Failed to update active senders for %s: %v", tf, err)
		} else {
			successfulSenderUpdates++
		}
	}
	utils.LogInfo("CHART_BROADCASTER", "✅ Active senders: %d/%d timeframes updated successfully", successfulSenderUpdates, len(timeframes))
	
	// Update active receivers for all timeframes (cache TTL: 6 minutes)
	successfulReceiverUpdates := 0
	for _, tf := range timeframes {
		if _, err := cb.clickhouseService.GetTopReceivers(ctx, 10, tf); err != nil {
			utils.LogError("CHART_BROADCASTER", "❌ Failed to update active receivers for %s: %v", tf, err)
		} else {
			successfulReceiverUpdates++
		}
	}
	utils.LogInfo("CHART_BROADCASTER", "✅ Active receivers: %d/%d timeframes updated successfully", successfulReceiverUpdates, len(timeframes))
	
	utils.LogInfo("CHART_BROADCASTER", "Chart group B update completed: %d assets, %d senders, %d receivers updated", 
		successfulAssetUpdates, successfulSenderUpdates, successfulReceiverUpdates)
} 