package stats

import (
	"sort"
	"strings"
	"time"
)

// calculateAssetVolumeData calculates comprehensive asset volume data
func (ec *EnhancedCollector) calculateAssetVolumeData() *AssetVolumeData {
	uptime := time.Since(ec.startTime).Seconds()

	// Aggregate asset data from all buckets
	assetAggregates := make(map[string]*AssetVolumeStats)
	
	// Process 10-second buckets (most recent data)
	for _, bucket := range ec.assetVolumeBuckets10s {
		ec.processAssetVolumeBucket(bucket, assetAggregates)
	}
	
	// Process 1-minute buckets (medium-term data)
	for _, bucket := range ec.assetVolumeBuckets1m {
		ec.processAssetVolumeBucket(bucket, assetAggregates)
	}
	
	// Process 1-hour buckets (long-term data)
	for _, bucket := range ec.assetVolumeBuckets1h {
		ec.processAssetVolumeBucket(bucket, assetAggregates)
	}

	// Convert to slice and calculate derived metrics
	assets := make([]*AssetVolumeStats, 0, len(assetAggregates))
	var totalVolume float64
	var totalTransfers int64

	for denom, assetStats := range assetAggregates {
		// Set display symbol
		if symbol, exists := ec.denomToSymbol[denom]; exists {
			assetStats.AssetSymbol = symbol
		} else {
			assetStats.AssetSymbol = denom
		}
		assetStats.AssetName = assetStats.AssetSymbol // Use symbol as name for now

		// Calculate average amount
		if assetStats.TransferCount > 0 {
			assetStats.AverageAmount = assetStats.TotalVolume / float64(assetStats.TransferCount)
		}

		// Calculate percentage changes if we have previous data
		ec.calculateAssetVolumePercentageChanges(assetStats, denom)

		// Process chain flows for this asset
		ec.processAssetChainFlows(assetStats)

		assets = append(assets, assetStats)
		totalVolume += assetStats.TotalVolume
		totalTransfers += assetStats.TransferCount
	}

	// Sort by transfer count (descending) for popularity ranking
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].TransferCount > assets[j].TransferCount
	})

	// Assign popularity ranks
	for i, asset := range assets {
		asset.PopularityRank = i + 1
	}

	// Sort by volume (descending) for volume ranking
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].TotalVolume > assets[j].TotalVolume
	})

	// Assign volume ranks
	for i, asset := range assets {
		asset.VolumeRank = i + 1
	}

	// Sort by popularity rank for final output (most popular first)
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].PopularityRank < assets[j].PopularityRank
	})

	return &AssetVolumeData{
		Assets:              assets,
		AssetVolumeTimeScale: ec.getAssetVolumeTimeScale(),
		TotalAssets:         len(assets),
		TotalVolume:         totalVolume,
		TotalTransfers:      totalTransfers,
		ServerUptimeSeconds: uptime,
	}
}

// processAssetVolumeBucket processes a single asset volume bucket and updates aggregates
func (ec *EnhancedCollector) processAssetVolumeBucket(bucket *AssetVolumeBucket, aggregates map[string]*AssetVolumeStats) {
	for denom, bucketData := range bucket.AssetVolumes {
		if existing, exists := aggregates[denom]; exists {
			// Merge with existing data
			existing.TransferCount += bucketData.TransferCount
			existing.TotalVolume += bucketData.TotalVolume
			if bucketData.LargestTransfer > existing.LargestTransfer {
				existing.LargestTransfer = bucketData.LargestTransfer
			}
			
			// Use HyperLogLog for accurate unique holder counting
			if bucketData.UniqueHoldersHLL != nil {
				existing.UniqueHolders = int(bucketData.UniqueHoldersHLL.Estimate())
			} else {
				existing.UniqueHolders += int(bucketData.UniqueHolderCount)
			}
			
			if bucket.Timestamp.After(existing.LastActivity) {
				existing.LastActivity = bucket.Timestamp
			}

			// Merge chain flows
			for chainFlowKey, chainFlowData := range bucketData.ChainFlows {
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
			// Create new aggregate entry
			uniqueHolders := int(bucketData.UniqueHolderCount)
			if bucketData.UniqueHoldersHLL != nil {
				uniqueHolders = int(bucketData.UniqueHoldersHLL.Estimate())
			}

			// Copy chain flows
			chainFlows := make(map[string]*AssetChainFlowData)
			for chainFlowKey, chainFlowData := range bucketData.ChainFlows {
				chainFlows[chainFlowKey] = &AssetChainFlowData{
					TransferCount:   chainFlowData.TransferCount,
					TotalVolume:     chainFlowData.TotalVolume,
					LargestTransfer: chainFlowData.LargestTransfer,
					LastActivity:    chainFlowData.LastActivity,
				}
			}

			aggregates[denom] = &AssetVolumeStats{
				TransferCount:   bucketData.TransferCount,
				TotalVolume:     bucketData.TotalVolume,
				LargestTransfer: bucketData.LargestTransfer,
				UniqueHolders:   uniqueHolders,
				LastActivity:    bucket.Timestamp,
				ChainFlows:      chainFlows,
			}
		}
	}
}

// calculateAssetVolumePercentageChanges calculates percentage changes for asset volume stats
func (ec *EnhancedCollector) calculateAssetVolumePercentageChanges(assetStats *AssetVolumeStats, denom string) {
	// Try to find previous data for this asset
	if ec.previousDay != nil && ec.previousDay.AssetVolumeData != nil {
		for _, prevAsset := range ec.previousDay.AssetVolumeData.Assets {
			// Match by symbol or denom
			if prevAsset.AssetSymbol == assetStats.AssetSymbol || 
			   (ec.denomToSymbol[denom] != "" && prevAsset.AssetSymbol == ec.denomToSymbol[denom]) {
				assetStats.VolumeChange = ec.calculatePercentageChange(assetStats.TotalVolume, prevAsset.TotalVolume)
				assetStats.CountChange = ec.calculatePercentageChange(float64(assetStats.TransferCount), float64(prevAsset.TransferCount))
				break
			}
		}
	}
}

// processAssetChainFlows processes chain flows for an asset and creates structured data
func (ec *EnhancedCollector) processAssetChainFlows(assetStats *AssetVolumeStats) {
	// Convert chain flow map to structured chain flow data
	chainFlows := make([]*AssetChainFlow, 0, len(assetStats.ChainFlows))
	sourceChainStats := make(map[string]*AssetChainStats)
	destChainStats := make(map[string]*AssetChainStats)

	for chainFlowKey, chainFlowData := range assetStats.ChainFlows {
		// Parse chain flow key (format: "sourceChain->destChain")
		parts := strings.Split(chainFlowKey, "->")
		if len(parts) >= 2 {
			sourceChain := parts[0]
			destChain := parts[1]
			
			// Get chain names from global chain flow stats
			sourceChainName := sourceChain
			destChainName := destChain
			if globalChain, exists := ec.chainFlowStats[sourceChain]; exists {
				sourceChainName = globalChain.ChainName
			}
			if globalChain, exists := ec.chainFlowStats[destChain]; exists {
				destChainName = globalChain.ChainName
			}

			// Calculate average amount for this route
			averageAmount := float64(0)
			if chainFlowData.TransferCount > 0 {
				averageAmount = chainFlowData.TotalVolume / float64(chainFlowData.TransferCount)
			}

			// Calculate percentages
			volumePercentage := float64(0)
			countPercentage := float64(0)
			if assetStats.TotalVolume > 0 {
				volumePercentage = (chainFlowData.TotalVolume / assetStats.TotalVolume) * 100
			}
			if assetStats.TransferCount > 0 {
				countPercentage = (float64(chainFlowData.TransferCount) / float64(assetStats.TransferCount)) * 100
			}

			chainFlow := &AssetChainFlow{
				SourceChain:      sourceChain,
				SourceChainName:  sourceChainName,
				DestChain:        destChain,
				DestChainName:    destChainName,
				TransferCount:    chainFlowData.TransferCount,
				TotalVolume:      chainFlowData.TotalVolume,
				AverageAmount:    averageAmount,
				LargestTransfer:  chainFlowData.LargestTransfer,
				VolumePercentage: volumePercentage,
				CountPercentage:  countPercentage,
				LastActivity:     chainFlowData.LastActivity,
			}
			chainFlows = append(chainFlows, chainFlow)

			// Aggregate source chain stats
			if sourceStats, exists := sourceChainStats[sourceChain]; exists {
				sourceStats.TransferCount += chainFlowData.TransferCount
				sourceStats.TotalVolume += chainFlowData.TotalVolume
				if chainFlowData.LastActivity.After(sourceStats.LastActivity) {
					sourceStats.LastActivity = chainFlowData.LastActivity
				}
			} else {
				sourceChainStats[sourceChain] = &AssetChainStats{
					ChainID:       sourceChain,
					ChainName:     sourceChainName,
					TransferCount: chainFlowData.TransferCount,
					TotalVolume:   chainFlowData.TotalVolume,
					LastActivity:  chainFlowData.LastActivity,
				}
			}

			// Aggregate destination chain stats
			if destStats, exists := destChainStats[destChain]; exists {
				destStats.TransferCount += chainFlowData.TransferCount
				destStats.TotalVolume += chainFlowData.TotalVolume
				if chainFlowData.LastActivity.After(destStats.LastActivity) {
					destStats.LastActivity = chainFlowData.LastActivity
				}
			} else {
				destChainStats[destChain] = &AssetChainStats{
					ChainID:       destChain,
					ChainName:     destChainName,
					TransferCount: chainFlowData.TransferCount,
					TotalVolume:   chainFlowData.TotalVolume,
					LastActivity:  chainFlowData.LastActivity,
				}
			}
		}
	}

	// Calculate percentages for chain stats
	for _, sourceStats := range sourceChainStats {
		if assetStats.TotalVolume > 0 {
			sourceStats.VolumePercentage = (sourceStats.TotalVolume / assetStats.TotalVolume) * 100
		}
		if assetStats.TransferCount > 0 {
			sourceStats.CountPercentage = (float64(sourceStats.TransferCount) / float64(assetStats.TransferCount)) * 100
		}
	}
	for _, destStats := range destChainStats {
		if assetStats.TotalVolume > 0 {
			destStats.VolumePercentage = (destStats.TotalVolume / assetStats.TotalVolume) * 100
		}
		if assetStats.TransferCount > 0 {
			destStats.CountPercentage = (float64(destStats.TransferCount) / float64(assetStats.TransferCount)) * 100
		}
	}

	// Convert to slices and sort
	topSourceChains := make([]*AssetChainStats, 0, len(sourceChainStats))
	for _, stats := range sourceChainStats {
		topSourceChains = append(topSourceChains, stats)
	}
	sort.Slice(topSourceChains, func(i, j int) bool {
		return topSourceChains[i].TotalVolume > topSourceChains[j].TotalVolume
	})

	topDestChains := make([]*AssetChainStats, 0, len(destChainStats))
	for _, stats := range destChainStats {
		topDestChains = append(topDestChains, stats)
	}
	sort.Slice(topDestChains, func(i, j int) bool {
		return topDestChains[i].TotalVolume > topDestChains[j].TotalVolume
	})

	// Sort chain flows by volume
	sort.Slice(chainFlows, func(i, j int) bool {
		return chainFlows[i].TotalVolume > chainFlows[j].TotalVolume
	})

	// Update asset stats (don't modify ChainFlows as it contains raw data)
	assetStats.TopSourceChains = topSourceChains
	assetStats.TopDestChains = topDestChains
}

// getAssetVolumeTimeScale returns asset volume data for different time scales
func (ec *EnhancedCollector) getAssetVolumeTimeScale() map[string][]*AssetVolumeStats {
	timeScales := getTimePeriodsFromNow()
	result := make(map[string][]*AssetVolumeStats)

	for timeframe, since := range timeScales {
		result[timeframe] = ec.getAssetVolumeInPeriod(since)
	}

	return result
}

// getAssetVolumeInPeriod returns asset volume stats for a specific time period
func (ec *EnhancedCollector) getAssetVolumeInPeriod(since time.Time) []*AssetVolumeStats {
	assetAggregates := make(map[string]*AssetVolumeStats)

	// Process buckets in the specified period
	for _, bucket := range ec.assetVolumeBuckets10s {
		if bucket.Timestamp.After(since) {
			ec.processAssetVolumeBucket(bucket, assetAggregates)
		}
	}
	for _, bucket := range ec.assetVolumeBuckets1m {
		if bucket.Timestamp.After(since) {
			ec.processAssetVolumeBucket(bucket, assetAggregates)
		}
	}
	for _, bucket := range ec.assetVolumeBuckets1h {
		if bucket.Timestamp.After(since) {
			ec.processAssetVolumeBucket(bucket, assetAggregates)
		}
	}

	// Convert to slice
	assets := make([]*AssetVolumeStats, 0, len(assetAggregates))
	for denom, assetStats := range assetAggregates {
		// Set display symbol
		if symbol, exists := ec.denomToSymbol[denom]; exists {
			assetStats.AssetSymbol = symbol
		} else {
			assetStats.AssetSymbol = denom
		}
		assetStats.AssetName = assetStats.AssetSymbol

		// Calculate average amount
		if assetStats.TransferCount > 0 {
			assetStats.AverageAmount = assetStats.TotalVolume / float64(assetStats.TransferCount)
		}

		assets = append(assets, assetStats)
	}

	// Sort by transfer count (descending)
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].TransferCount > assets[j].TransferCount
	})

	return assets
}

// calculateAssetVolumeDataForBroadcast calculates asset volume data with broadcast limits
func (ec *EnhancedCollector) calculateAssetVolumeDataForBroadcast() *AssetVolumeData {
	uptime := time.Since(ec.startTime).Seconds()

	// Aggregate asset data from all buckets
	assetAggregates := make(map[string]*AssetVolumeStats)
	
	// Process 10-second buckets (most recent data)
	for _, bucket := range ec.assetVolumeBuckets10s {
		ec.processAssetVolumeBucket(bucket, assetAggregates)
	}
	
	// Process 1-minute buckets (medium-term data)
	for _, bucket := range ec.assetVolumeBuckets1m {
		ec.processAssetVolumeBucket(bucket, assetAggregates)
	}
	
	// Process 1-hour buckets (long-term data)
	for _, bucket := range ec.assetVolumeBuckets1h {
		ec.processAssetVolumeBucket(bucket, assetAggregates)
	}

	// Convert to slice and calculate derived metrics
	assets := make([]*AssetVolumeStats, 0, len(assetAggregates))
	var totalVolume float64
	var totalTransfers int64

	for denom, assetStats := range assetAggregates {
		// Set display symbol
		if symbol, exists := ec.denomToSymbol[denom]; exists {
			assetStats.AssetSymbol = symbol
		} else {
			assetStats.AssetSymbol = denom
		}
		assetStats.AssetName = assetStats.AssetSymbol // Use symbol as name for now

		// Calculate average amount
		if assetStats.TransferCount > 0 {
			assetStats.AverageAmount = assetStats.TotalVolume / float64(assetStats.TransferCount)
		}

		// Calculate percentage changes if we have previous data
		ec.calculateAssetVolumePercentageChanges(assetStats, denom)

		// Process chain flows for this asset
		ec.processAssetChainFlows(assetStats)

		assets = append(assets, assetStats)
		totalVolume += assetStats.TotalVolume
		totalTransfers += assetStats.TransferCount
	}

	// Sort by transfer count (descending) for popularity ranking
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].TransferCount > assets[j].TransferCount
	})

	// Assign popularity ranks
	for i, asset := range assets {
		asset.PopularityRank = i + 1
	}

	// Sort by volume (descending) for volume ranking
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].TotalVolume > assets[j].TotalVolume
	})

	// Assign volume ranks
	for i, asset := range assets {
		asset.VolumeRank = i + 1
	}

	// Sort by popularity rank for final output (most popular first)
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].PopularityRank < assets[j].PopularityRank
	})

	// Apply broadcast limit if configured
	broadcastAssets := assets
	if ec.config.MaxAssetsInBroadcast > 0 && len(assets) > ec.config.MaxAssetsInBroadcast {
		broadcastAssets = assets[:ec.config.MaxAssetsInBroadcast]
	}

	return &AssetVolumeData{
		Assets:              broadcastAssets,
		AssetVolumeTimeScale: ec.getAssetVolumeTimeScale(),
		TotalAssets:         len(assets), // Keep total count even if we limit broadcast
		TotalVolume:         totalVolume,
		TotalTransfers:      totalTransfers,
		ServerUptimeSeconds: uptime,
	}
} 