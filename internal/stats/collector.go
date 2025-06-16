package stats

import (
	"time"
	"github.com/axiomhq/hyperloglog"
)

// StatsConfig holds configuration for stats collection and broadcasting
type StatsConfig struct {
	TopItemsLimit        int // Number of top items to return for routes, senders, receivers (chart data)
	TopItemsTimeScale    int // Number of top items to return for time scale data
	MaxWalletsInBroadcast int // Maximum number of wallets to include in broadcast (0 = unlimited)
	MaxAssetsInBroadcast  int // Maximum number of assets to include in broadcast (0 = unlimited)
}

// NewEnhancedCollector creates a new enhanced statistics collector
func NewEnhancedCollector(config StatsConfig) *EnhancedCollector {
	now := time.Now()
	return &EnhancedCollector{
		config:           config,
		routeStats:       make(map[string]*RouteStats),
		senderStats:      make(map[string]*WalletStats),
		receiverStats:    make(map[string]*WalletStats),
		chainFlowStats:   make(map[string]*ChainFlowStats),
		timeBuckets:      make(map[time.Time]*TimeBucket),
		walletActivityBuckets: make(map[time.Time]*WalletActivityBucket),
		routeActivityBuckets: make(map[time.Time]*RouteActivityBucket),
		chainFlowBuckets:   make(map[time.Time]*ChainFlowBucket),
		assetVolumeBuckets10s: make(map[time.Time]*AssetVolumeBucket),
		assetVolumeBuckets1m:  make(map[time.Time]*AssetVolumeBucket),
		assetVolumeBuckets1h:  make(map[time.Time]*AssetVolumeBucket),
		denomToSymbol:    make(map[string]string),
		globalSendersHLL:   hyperloglog.New16(),
		globalReceiversHLL: hyperloglog.New16(),
		globalWalletsHLL:   hyperloglog.New16(),
		hllPool:            NewHLLPool(),
		startTime:        now,
		lastUpdateTime:   now,
		lastMinuteCheck:  now,
		lastHourCheck:    now,
		lastDayCheck:     now,
		last7DaysCheck:   now,
		last14DaysCheck:  now,
		last30DaysCheck:  now,
	}
}

// GetChartData returns basic chart data
func (ec *EnhancedCollector) GetChartData() ChartData {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	return ChartData{
		CurrentRates:    ec.calculateTransferRates(),
		PopularRoutes:   ec.getTopRoutes(ec.config.TopItemsLimit),
		ActiveSenders:   ec.getTopSenders(ec.config.TopItemsLimit),
		ActiveReceivers: ec.getTopReceivers(ec.config.TopItemsLimit),
	}
}

// GetEnhancedChartData returns enhanced chart data with additional fields
func (ec *EnhancedCollector) GetEnhancedChartData() EnhancedChartData {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	return EnhancedChartData{
		CurrentRates:             ec.calculateTransferRates(),
		ActiveWalletRates:        ec.calculateActiveWalletRates(),
		PopularRoutes:            ec.getTopRoutes(ec.config.TopItemsLimit),
		PopularRoutesTimeScale:   ec.getPopularRoutesTimeScale(),
		ActiveSenders:            ec.getTopSenders(ec.config.TopItemsLimit),
		ActiveReceivers:          ec.getTopReceivers(ec.config.TopItemsLimit),
		ActiveSendersTimeScale:   ec.getActiveSendersTimeScale(),
		ActiveReceiversTimeScale: ec.getActiveReceiversTimeScale(),
		ChainFlowData:            ec.calculateChainFlowData(),
		DataAvailability:         ec.calculateDataAvailability(),
	}
}

// GetBroadcastData returns data for broadcasting to clients
func (ec *EnhancedCollector) GetBroadcastData() interface{} {
	ec.mu.RLock()
	defer ec.mu.RUnlock()

	// Embed enhanced chart data directly
	enhancedData := ec.GetEnhancedChartData()
	
	// Create broadcast data with additional fields
	broadcastData := struct {
		EnhancedChartData
		WalletData      *WalletData      `json:"walletData"`
		AssetVolumeData *AssetVolumeData `json:"assetVolumeData"`
		ServerUptime    float64          `json:"serverUptime"`
		Timestamp       time.Time        `json:"timestamp"`
	}{
		EnhancedChartData: enhancedData,
		WalletData:        ec.calculateWalletDataForBroadcast(),
		AssetVolumeData:   ec.calculateAssetVolumeDataForBroadcast(),
		ServerUptime:      time.Since(ec.startTime).Seconds(),
		Timestamp:         time.Now(),
	}

	return broadcastData
}

// calculateDataAvailability determines what time periods have sufficient data
func (ec *EnhancedCollector) calculateDataAvailability() DataAvailability {
	now := time.Now()
	uptime := now.Sub(ec.startTime)

	return DataAvailability{
		HasMinute: uptime >= time.Minute,
		HasHour:   uptime >= time.Hour,
		HasDay:    uptime >= 24*time.Hour,
		Has7Days:  uptime >= 7*24*time.Hour,
		Has14Days: uptime >= 14*24*time.Hour,
		Has30Days: uptime >= 30*24*time.Hour,
	}
}

// cleanupOldTimeData removes old time-based data to prevent memory leaks
func (ec *EnhancedCollector) cleanupOldTimeData(cutoff time.Time) {
	// Clean up time buckets
	for timestamp := range ec.timeBuckets {
		if timestamp.Before(cutoff) {
			delete(ec.timeBuckets, timestamp)
		}
	}

	// Clean up wallet activity buckets
	for timestamp := range ec.walletActivityBuckets {
		if timestamp.Before(cutoff) {
			delete(ec.walletActivityBuckets, timestamp)
		}
	}

	// Clean up route activity buckets
	for timestamp := range ec.routeActivityBuckets {
		if timestamp.Before(cutoff) {
			delete(ec.routeActivityBuckets, timestamp)
		}
	}

	// Clean up chain flow buckets
	for timestamp := range ec.chainFlowBuckets {
		if timestamp.Before(cutoff) {
			delete(ec.chainFlowBuckets, timestamp)
		}
	}

	// Clean up asset volume buckets
	for timestamp := range ec.assetVolumeBuckets10s {
		if timestamp.Before(cutoff) {
			delete(ec.assetVolumeBuckets10s, timestamp)
		}
	}
} 