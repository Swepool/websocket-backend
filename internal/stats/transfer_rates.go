package stats

import (
	"time"
	"fmt"
	"websocket-backend/internal/models"
	"github.com/axiomhq/hyperloglog"
)

// UpdateTransferStats updates statistics with new transfer data
func (ec *EnhancedCollector) UpdateTransferStats(transfers []models.Transfer) {
	if len(transfers) == 0 {
		return
	}

	ec.mu.Lock()
	defer ec.mu.Unlock()

	now := time.Now()
	ec.lastUpdateTime = now

	// Process each transfer
	for _, transfer := range transfers {
		// Update route statistics
		routeKey := transfer.SourceChain.DisplayName + " → " + transfer.DestinationChain.DisplayName
		if ec.routeStats[routeKey] == nil {
			ec.routeStats[routeKey] = &RouteStats{
				Route:        routeKey,
				Count:        0,
				FromChain:    transfer.SourceChain.UniversalChainID,
				ToChain:      transfer.DestinationChain.UniversalChainID,
				FromName:     transfer.SourceChain.DisplayName,
				ToName:       transfer.DestinationChain.DisplayName,
				LastActivity: transfer.TransferSendTimestamp,
			}
		}
		ec.routeStats[routeKey].Count++
		if transfer.TransferSendTimestamp.After(ec.routeStats[routeKey].LastActivity) {
			ec.routeStats[routeKey].LastActivity = transfer.TransferSendTimestamp
		}

		// Update sender statistics
		senderAddr := transfer.SenderCanonical
		if ec.senderStats[senderAddr] == nil {
			ec.senderStats[senderAddr] = &WalletStats{
				Address:      senderAddr,
				Count:        0,
				LastActivity: transfer.TransferSendTimestamp,
			}
		}
		ec.senderStats[senderAddr].Count++
		if transfer.TransferSendTimestamp.After(ec.senderStats[senderAddr].LastActivity) {
			ec.senderStats[senderAddr].LastActivity = transfer.TransferSendTimestamp
		}

		// Update receiver statistics
		receiverAddr := transfer.ReceiverCanonical
		if ec.receiverStats[receiverAddr] == nil {
			ec.receiverStats[receiverAddr] = &WalletStats{
				Address:      receiverAddr,
				Count:        0,
				LastActivity: transfer.TransferSendTimestamp,
			}
		}
		ec.receiverStats[receiverAddr].Count++
		if transfer.TransferSendTimestamp.After(ec.receiverStats[receiverAddr].LastActivity) {
			ec.receiverStats[receiverAddr].LastActivity = transfer.TransferSendTimestamp
		}

		// Update chain flow statistics
		sourceChainID := transfer.SourceChain.ChainID
		destChainID := transfer.DestinationChain.ChainID

		// Update source chain (outgoing)
		if ec.chainFlowStats[sourceChainID] == nil {
			ec.chainFlowStats[sourceChainID] = &ChainFlowStats{
				UniversalChainID: sourceChainID,
				ChainName:        transfer.SourceChain.DisplayName,
				OutgoingCount:    0,
				IncomingCount:    0,
				NetFlow:          0,
				LastActivity:     transfer.TransferSendTimestamp,
			}
		}
		ec.chainFlowStats[sourceChainID].OutgoingCount++
		ec.chainFlowStats[sourceChainID].NetFlow = ec.chainFlowStats[sourceChainID].OutgoingCount - ec.chainFlowStats[sourceChainID].IncomingCount
		if transfer.TransferSendTimestamp.After(ec.chainFlowStats[sourceChainID].LastActivity) {
			ec.chainFlowStats[sourceChainID].LastActivity = transfer.TransferSendTimestamp
		}

		// Update destination chain (incoming)
		if ec.chainFlowStats[destChainID] == nil {
			ec.chainFlowStats[destChainID] = &ChainFlowStats{
				UniversalChainID: destChainID,
				ChainName:        transfer.DestinationChain.DisplayName,
				OutgoingCount:    0,
				IncomingCount:    0,
				NetFlow:          0,
				LastActivity:     transfer.TransferSendTimestamp,
			}
		}
		ec.chainFlowStats[destChainID].IncomingCount++
		ec.chainFlowStats[destChainID].NetFlow = ec.chainFlowStats[destChainID].OutgoingCount - ec.chainFlowStats[destChainID].IncomingCount
		if transfer.TransferSendTimestamp.After(ec.chainFlowStats[destChainID].LastActivity) {
			ec.chainFlowStats[destChainID].LastActivity = transfer.TransferSendTimestamp
		}

		// Add to HyperLogLog for unique counting
		ec.globalSendersHLL.Insert([]byte(senderAddr))
		ec.globalReceiversHLL.Insert([]byte(receiverAddr))
		ec.globalWalletsHLL.Insert([]byte(senderAddr))
		ec.globalWalletsHLL.Insert([]byte(receiverAddr))

		// Store symbol mapping for display
		if transfer.BaseTokenSymbol != "" {
			ec.denomToSymbol[transfer.BaseToken] = transfer.BaseTokenSymbol
		}

		// Increment total transfer count
		ec.totalTransfers++

		// Process transfer into buckets for time-based analytics
		ec.processTransferIntoBuckets(transfer, now)
	}

	// Aggregate old buckets periodically
	ec.aggregateOldBuckets()
}

// calculateTransferRates calculates transfer rates for different time periods
func (ec *EnhancedCollector) calculateTransferRates() TransferRates {
	now := time.Now()
	uptime := now.Sub(ec.startTime).Seconds()

	// Calculate rates for different periods
	txPerMinute := float64(ec.countTransfersInPeriod(now.Add(-time.Minute)))
	txPerHour := float64(ec.countTransfersInPeriod(now.Add(-time.Hour)))
	txPerDay := float64(ec.countTransfersInPeriod(now.Add(-24 * time.Hour)))
	txPer7Days := float64(ec.countTransfersInPeriod(now.AddDate(0, 0, -7)))
	txPer14Days := float64(ec.countTransfersInPeriod(now.AddDate(0, 0, -14)))
	txPer30Days := float64(ec.countTransfersInPeriod(now.AddDate(0, 0, -30)))

	// Calculate percentage changes from previous periods
	var txPerMinuteChange, txPerHourChange, txPerDayChange float64
	var txPer7DaysChange, txPer14DaysChange, txPer30DaysChange float64

	if ec.previousMinute != nil {
		txPerMinuteChange = ec.calculatePercentageChange(txPerMinute, ec.previousMinute.TransferRates.TxPerMinute)
	}
	if ec.previousHour != nil {
		txPerHourChange = ec.calculatePercentageChange(txPerHour, ec.previousHour.TransferRates.TxPerHour)
	}
	if ec.previousDay != nil {
		txPerDayChange = ec.calculatePercentageChange(txPerDay, ec.previousDay.TransferRates.TxPerDay)
	}
	if ec.previous7Days != nil {
		txPer7DaysChange = ec.calculatePercentageChange(txPer7Days, ec.previous7Days.TransferRates.TxPer7Days)
	}
	if ec.previous14Days != nil {
		txPer14DaysChange = ec.calculatePercentageChange(txPer14Days, ec.previous14Days.TransferRates.TxPer14Days)
	}
	if ec.previous30Days != nil {
		txPer30DaysChange = ec.calculatePercentageChange(txPer30Days, ec.previous30Days.TransferRates.TxPer30Days)
	}

	return TransferRates{
		TxPerMinute:         txPerMinute,
		TxPerHour:           txPerHour,
		TxPerDay:            txPerDay,
		TxPer7Days:          txPer7Days,
		TxPer14Days:         txPer14Days,
		TxPer30Days:         txPer30Days,
		TxPerMinuteChange:   txPerMinuteChange,
		TxPerHourChange:     txPerHourChange,
		TxPerDayChange:      txPerDayChange,
		TxPer7DaysChange:    txPer7DaysChange,
		TxPer14DaysChange:   txPer14DaysChange,
		TxPer30DaysChange:   txPer30DaysChange,
		TotalTracked:        ec.totalTransfers,
		DataAvailability:    ec.calculateDataAvailability(),
		ServerUptimeSeconds: uptime,
	}
}

// countAllInPeriod counts all metrics since the given time in a single pass
func (ec *EnhancedCollector) countAllInPeriod(since time.Time) PeriodCounts {
	var transferCount int64
	
	// Reset and reuse HLL instances from pool to reduce allocations
	ec.hllPool.Reset()
	sendersHLL := ec.hllPool.GetSenders()
	receiversHLL := ec.hllPool.GetReceivers()
	combinedHLL := ec.hllPool.GetCombined()
	
	// Get filtered buckets for the time period
	timeBucketsInPeriod := ec.getTimeBucketsInPeriod(since)
	walletBucketsInPeriod := ec.getWalletActivityBucketsInPeriod(since)
	
	// Single pass through filtered time buckets for transfer counts
	for _, bucket := range timeBucketsInPeriod {
		transferCount += bucket.TransferCount
	}
	
	// Single pass through filtered wallet activity buckets for unique counts
	for _, activity := range walletBucketsInPeriod {
		if activity.SenderHLL != nil {
			sendersHLL.Merge(activity.SenderHLL)
		}
		if activity.ReceiverHLL != nil {
			receiversHLL.Merge(activity.ReceiverHLL)
		}
		if activity.CombinedHLL != nil {
			combinedHLL.Merge(activity.CombinedHLL)
		}
	}
	
	return PeriodCounts{
		Transfers: transferCount,
		Senders:   int(sendersHLL.Estimate()),
		Receivers: int(receiversHLL.Estimate()),
		Total:     int(combinedHLL.Estimate()),
	}
}

// Bucket filtering helper functions to optimize repeated time period checks
func (ec *EnhancedCollector) getTimeBucketsInPeriod(since time.Time) []*TimeBucket {
	var result []*TimeBucket
	for _, bucket := range ec.timeBuckets {
		if bucket.Timestamp.After(since) {
			result = append(result, bucket)
		}
	}
	return result
}

func (ec *EnhancedCollector) getWalletActivityBucketsInPeriod(since time.Time) []*WalletActivityBucket {
	var result []*WalletActivityBucket
	for _, bucket := range ec.walletActivityBuckets {
		if bucket.Timestamp.After(since) {
			result = append(result, bucket)
		}
	}
	return result
}

// Legacy functions for backward compatibility - now use consolidated function
func (ec *EnhancedCollector) countTransfersInPeriod(since time.Time) int64 {
	return ec.countAllInPeriod(since).Transfers
}

func (ec *EnhancedCollector) countActiveWalletsInPeriod(since time.Time) int {
	return ec.countAllInPeriod(since).Total
}

func (ec *EnhancedCollector) countActiveSendersInPeriod(since time.Time) int {
	return ec.countAllInPeriod(since).Senders
}

func (ec *EnhancedCollector) countActiveReceiversInPeriod(since time.Time) int {
	return ec.countAllInPeriod(since).Receivers
}

// checkAndRotatePeriods checks if we need to rotate period data and does so
func (ec *EnhancedCollector) checkAndRotatePeriods(now time.Time) {
	// Check minute rotation (every minute)
	if now.Sub(ec.lastMinuteCheck) >= time.Minute {
		ec.rotatePeriod("minute", now)
		ec.lastMinuteCheck = now
	}
	
	// Check hour rotation (every hour)
	if now.Sub(ec.lastHourCheck) >= time.Hour {
		ec.rotatePeriod("hour", now)
		ec.lastHourCheck = now
	}
	
	// Check day rotation (every day)
	if now.Sub(ec.lastDayCheck) >= 24*time.Hour {
		ec.rotatePeriod("day", now)
		ec.lastDayCheck = now
	}
	
	// Check 7-day rotation (every 7 days)
	if now.Sub(ec.last7DaysCheck) >= 7*24*time.Hour {
		ec.rotatePeriod("7days", now)
		ec.last7DaysCheck = now
	}
	
	// Check 14-day rotation (every 14 days)
	if now.Sub(ec.last14DaysCheck) >= 14*24*time.Hour {
		ec.rotatePeriod("14days", now)
		ec.last14DaysCheck = now
	}
	
	// Check 30-day rotation (every 30 days)
	if now.Sub(ec.last30DaysCheck) >= 30*24*time.Hour {
		ec.rotatePeriod("30days", now)
		ec.last30DaysCheck = now
	}
}

// rotatePeriod saves current data as previous data for the specified period
func (ec *EnhancedCollector) rotatePeriod(period string, timestamp time.Time) {
	// Calculate current rates and wallet stats
	currentTransferRates := ec.calculateTransferRates()
	currentWalletRates := ec.calculateActiveWalletRates()
	currentPopularRoutes := ec.getPopularRoutesTimeScale()
	currentChainFlowData := ec.calculateChainFlowData()
	
	// Debug logging
	fmt.Printf("[DEBUG] Rotating period: %s at %s\n", period, timestamp.Format("15:04:05"))
	fmt.Printf("[DEBUG] Current chain flow data has %d chains\n", len(currentChainFlowData.Chains))
	if len(currentChainFlowData.Chains) > 0 {
		fmt.Printf("[DEBUG] First chain: %s, outgoing: %d, incoming: %d\n", 
			currentChainFlowData.Chains[0].UniversalChainID,
			currentChainFlowData.Chains[0].OutgoingCount,
			currentChainFlowData.Chains[0].IncomingCount)
	}
	
	// Create snapshot of current data
	snapshot := &PreviousPeriodData{
		TransferRates:     currentTransferRates,
		ActiveWalletRates: currentWalletRates,
		PopularRoutes:     currentPopularRoutes,
		ChainFlowData:     currentChainFlowData,
		Timestamp:         timestamp,
		WalletData:        ec.calculateWalletData(),
		AssetVolumeData:   ec.calculateAssetVolumeData(),
	}
	
	// Store snapshot based on period type
	switch period {
	case "minute":
		ec.previousMinute = snapshot
	case "hour":
		ec.previousHour = snapshot
	case "day":
		ec.previousDay = snapshot
	case "7days":
		ec.previous7Days = snapshot
	case "14days":
		ec.previous14Days = snapshot
	case "30days":
		ec.previous30Days = snapshot
	}
}

// processTransferIntoBuckets processes a transfer into various bucket types for time-based analytics
func (ec *EnhancedCollector) processTransferIntoBuckets(transfer models.Transfer, now time.Time) {
	// Round timestamp to 10-second intervals for bucketing
	bucketTime := transfer.TransferSendTimestamp.Truncate(10 * time.Second)
	
	// Process into time buckets
	if ec.timeBuckets[bucketTime] == nil {
		ec.timeBuckets[bucketTime] = &TimeBucket{
			Timestamp:     bucketTime,
			TransferCount: 0,
			SenderCount:   0,
			ReceiverCount: 0,
			TotalCount:    0,
			Granularity:   "10s",
		}
	}
	ec.timeBuckets[bucketTime].TransferCount++
	
	// Process into wallet activity buckets
	if ec.walletActivityBuckets[bucketTime] == nil {
		ec.walletActivityBuckets[bucketTime] = &WalletActivityBucket{
			Timestamp:     bucketTime,
			SenderCounts:  make(map[string]int64),
			ReceiverCounts: make(map[string]int64),
			Granularity:   "10s",
			SenderHLL:     hyperloglog.New16(),
			ReceiverHLL:   hyperloglog.New16(),
			CombinedHLL:   hyperloglog.New16(),
		}
	}
	
	bucket := ec.walletActivityBuckets[bucketTime]
	bucket.SenderCounts[transfer.SenderCanonical]++
	bucket.ReceiverCounts[transfer.ReceiverCanonical]++
	bucket.SenderHLL.Insert([]byte(transfer.SenderCanonical))
	bucket.ReceiverHLL.Insert([]byte(transfer.ReceiverCanonical))
	bucket.CombinedHLL.Insert([]byte(transfer.SenderCanonical))
	bucket.CombinedHLL.Insert([]byte(transfer.ReceiverCanonical))
	
	// Process into route activity buckets
	routeKey := transfer.SourceChain.DisplayName + " → " + transfer.DestinationChain.DisplayName
	if ec.routeActivityBuckets[bucketTime] == nil {
		ec.routeActivityBuckets[bucketTime] = &RouteActivityBucket{
			Timestamp:   bucketTime,
			RouteCounts: make(map[string]int64),
			Granularity: "10s",
		}
	}
	ec.routeActivityBuckets[bucketTime].RouteCounts[routeKey]++
	
	// Process into chain flow buckets
	if ec.chainFlowBuckets[bucketTime] == nil {
		ec.chainFlowBuckets[bucketTime] = &ChainFlowBucket{
			Timestamp:      bucketTime,
			OutgoingCounts: make(map[string]int64),
			IncomingCounts: make(map[string]int64),
			Granularity:    "10s",
		}
	}
	
	chainBucket := ec.chainFlowBuckets[bucketTime]
	chainBucket.OutgoingCounts[transfer.SourceChain.ChainID]++
	chainBucket.IncomingCounts[transfer.DestinationChain.ChainID]++
	
	// Process into asset volume buckets
	if ec.assetVolumeBuckets10s[bucketTime] == nil {
		ec.assetVolumeBuckets10s[bucketTime] = &AssetVolumeBucket{
			Timestamp:    bucketTime,
			AssetVolumes: make(map[string]*AssetBucketData),
			Granularity:  "10s",
		}
	}
	
	assetBucket := ec.assetVolumeBuckets10s[bucketTime]
	denom := transfer.BaseToken
	
	if assetBucket.AssetVolumes[denom] == nil {
		assetBucket.AssetVolumes[denom] = &AssetBucketData{
			TransferCount:     0,
			TotalVolume:       0,
			LargestTransfer:   0,
			UniqueHolderCount: 0,
			ChainFlows:        make(map[string]*AssetChainFlowData),
			UniqueHoldersHLL:  hyperloglog.New16(),
		}
	}
	
	assetData := assetBucket.AssetVolumes[denom]
	assetData.TransferCount++
	
	// Parse transfer amount (simplified - in a real implementation you'd handle decimals properly)
	// For now, we'll use a placeholder volume calculation
	volume := 1.0 // This should be parsed from transfer.BaseAmount with proper decimal handling
	assetData.TotalVolume += volume
	if volume > assetData.LargestTransfer {
		assetData.LargestTransfer = volume
	}
	
	// Track unique holders
	assetData.UniqueHoldersHLL.Insert([]byte(transfer.SenderCanonical))
	assetData.UniqueHoldersHLL.Insert([]byte(transfer.ReceiverCanonical))
	
	// Track chain flows for this asset
	chainFlowKey := transfer.SourceChain.ChainID + "->" + transfer.DestinationChain.ChainID
	if assetData.ChainFlows[chainFlowKey] == nil {
		assetData.ChainFlows[chainFlowKey] = &AssetChainFlowData{
			TransferCount:   0,
			TotalVolume:     0,
			LargestTransfer: 0,
			LastActivity:    transfer.TransferSendTimestamp,
		}
	}
	
	chainFlow := assetData.ChainFlows[chainFlowKey]
	chainFlow.TransferCount++
	chainFlow.TotalVolume += volume
	if volume > chainFlow.LargestTransfer {
		chainFlow.LargestTransfer = volume
	}
	if transfer.TransferSendTimestamp.After(chainFlow.LastActivity) {
		chainFlow.LastActivity = transfer.TransferSendTimestamp
	}
} 