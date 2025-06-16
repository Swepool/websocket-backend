package stats

import (
	"time"
	"github.com/axiomhq/hyperloglog"
)

// aggregateOldBuckets aggregates old 10-second buckets into 1-minute and 1-hour buckets to save memory
func (ec *EnhancedCollector) aggregateOldBuckets() {
	now := time.Now()
	
	// Aggregate 10-second buckets older than 1 hour into 1-minute buckets
	oneHourAgo := now.Add(-time.Hour)
	ec.aggregateBucketsToMinute(oneHourAgo)
	
	// Aggregate 1-minute buckets older than 1 day into 1-hour buckets
	oneDayAgo := now.Add(-24 * time.Hour)
	ec.aggregateBucketsToHour(oneDayAgo)
}

// aggregateBucketsToMinute aggregates 10-second asset volume buckets into 1-minute buckets
func (ec *EnhancedCollector) aggregateBucketsToMinute(cutoff time.Time) {
	// Group 10-second buckets by minute
	minuteBuckets := make(map[time.Time][]*AssetVolumeBucket)
	
	for timestamp, bucket := range ec.assetVolumeBuckets10s {
		if timestamp.Before(cutoff) {
			minuteTimestamp := timestamp.Truncate(time.Minute)
			minuteBuckets[minuteTimestamp] = append(minuteBuckets[minuteTimestamp], bucket)
		}
	}
	
	// Aggregate each minute's worth of 10-second buckets
	for minuteTimestamp, buckets := range minuteBuckets {
		aggregatedBucket := ec.aggregateAssetVolumeBuckets(buckets, "1m")
		aggregatedBucket.Timestamp = minuteTimestamp
		
		// Store in 1-minute bucket map
		ec.assetVolumeBuckets1m[minuteTimestamp] = aggregatedBucket
		
		// Remove the original 10-second buckets
		for _, bucket := range buckets {
			delete(ec.assetVolumeBuckets10s, bucket.Timestamp)
		}
	}
}

// aggregateBucketsToHour aggregates 1-minute asset volume buckets into 1-hour buckets
func (ec *EnhancedCollector) aggregateBucketsToHour(cutoff time.Time) {
	// Group 1-minute buckets by hour
	hourBuckets := make(map[time.Time][]*AssetVolumeBucket)
	
	for timestamp, bucket := range ec.assetVolumeBuckets1m {
		if timestamp.Before(cutoff) {
			hourTimestamp := timestamp.Truncate(time.Hour)
			hourBuckets[hourTimestamp] = append(hourBuckets[hourTimestamp], bucket)
		}
	}
	
	// Aggregate each hour's worth of 1-minute buckets
	for hourTimestamp, buckets := range hourBuckets {
		aggregatedBucket := ec.aggregateAssetVolumeBuckets(buckets, "1h")
		aggregatedBucket.Timestamp = hourTimestamp
		
		// Store in 1-hour bucket map
		ec.assetVolumeBuckets1h[hourTimestamp] = aggregatedBucket
		
		// Remove the original 1-minute buckets
		for _, bucket := range buckets {
			delete(ec.assetVolumeBuckets1m, bucket.Timestamp)
		}
	}
}

// aggregateAssetVolumeBuckets aggregates multiple asset volume buckets into one
func (ec *EnhancedCollector) aggregateAssetVolumeBuckets(buckets []*AssetVolumeBucket, granularity string) *AssetVolumeBucket {
	if len(buckets) == 0 {
		return &AssetVolumeBucket{
			AssetVolumes: make(map[string]*AssetBucketData),
			Granularity:  granularity,
		}
	}
	
	aggregatedAssetVolumes := make(map[string]*AssetBucketData)
	
	// Aggregate all buckets
	for _, bucket := range buckets {
		for assetKey, assetData := range bucket.AssetVolumes {
			if existing, exists := aggregatedAssetVolumes[assetKey]; exists {
				// Merge with existing data
				existing.TransferCount += assetData.TransferCount
				existing.TotalVolume += assetData.TotalVolume
				if assetData.LargestTransfer > existing.LargestTransfer {
					existing.LargestTransfer = assetData.LargestTransfer
				}
				
				// Merge HyperLogLog for unique holder counting
				if assetData.UniqueHoldersHLL != nil {
					if existing.UniqueHoldersHLL == nil {
						existing.UniqueHoldersHLL = hyperloglog.New16()
					}
					existing.UniqueHoldersHLL.Merge(assetData.UniqueHoldersHLL)
					existing.UniqueHolderCount = int64(existing.UniqueHoldersHLL.Estimate())
				} else {
					existing.UniqueHolderCount += assetData.UniqueHolderCount
				}
				
				// Merge chain flows
				for chainFlowKey, chainFlowData := range assetData.ChainFlows {
					if existingChainFlow, chainExists := existing.ChainFlows[chainFlowKey]; chainExists {
						existingChainFlow.TransferCount += chainFlowData.TransferCount
						existingChainFlow.TotalVolume += chainFlowData.TotalVolume
						if chainFlowData.LargestTransfer > existingChainFlow.LargestTransfer {
							existingChainFlow.LargestTransfer = chainFlowData.LargestTransfer
						}
						if chainFlowData.LastActivity.After(existingChainFlow.LastActivity) {
							existingChainFlow.LastActivity = chainFlowData.LastActivity
						}
					} else {
						existing.ChainFlows[chainFlowKey] = &AssetChainFlowData{
							TransferCount:   chainFlowData.TransferCount,
							TotalVolume:     chainFlowData.TotalVolume,
							LargestTransfer: chainFlowData.LargestTransfer,
							LastActivity:    chainFlowData.LastActivity,
						}
					}
				}
			} else {
				// Create new entry
				uniqueHoldersHLL := hyperloglog.New16()
				if assetData.UniqueHoldersHLL != nil {
					uniqueHoldersHLL.Merge(assetData.UniqueHoldersHLL)
				}
				
				// Copy chain flows
				chainFlows := make(map[string]*AssetChainFlowData)
				for chainFlowKey, chainFlowData := range assetData.ChainFlows {
					chainFlows[chainFlowKey] = &AssetChainFlowData{
						TransferCount:   chainFlowData.TransferCount,
						TotalVolume:     chainFlowData.TotalVolume,
						LargestTransfer: chainFlowData.LargestTransfer,
						LastActivity:    chainFlowData.LastActivity,
					}
				}
				
				aggregatedAssetVolumes[assetKey] = &AssetBucketData{
					TransferCount:     assetData.TransferCount,
					TotalVolume:       assetData.TotalVolume,
					LargestTransfer:   assetData.LargestTransfer,
					UniqueHolderCount: assetData.UniqueHolderCount,
					UniqueHoldersHLL:  uniqueHoldersHLL,
					ChainFlows:        chainFlows,
				}
			}
		}
	}
	
	return &AssetVolumeBucket{
		AssetVolumes: aggregatedAssetVolumes,
		Granularity:  granularity,
	}
}

// cleanupOldAggregatedBuckets removes very old aggregated buckets to prevent unbounded memory growth
func (ec *EnhancedCollector) cleanupOldAggregatedBuckets() {
	now := time.Now()
	
	// Keep 1-minute buckets for 30 days
	thirtyDaysAgo := now.AddDate(0, 0, -30)
	for timestamp := range ec.assetVolumeBuckets1m {
		if timestamp.Before(thirtyDaysAgo) {
			delete(ec.assetVolumeBuckets1m, timestamp)
		}
	}
	
	// Keep 1-hour buckets for 1 year
	oneYearAgo := now.AddDate(-1, 0, 0)
	for timestamp := range ec.assetVolumeBuckets1h {
		if timestamp.Before(oneYearAgo) {
			delete(ec.assetVolumeBuckets1h, timestamp)
		}
	}
} 