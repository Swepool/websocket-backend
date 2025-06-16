package stats

import (
	"sort"
	"time"
)

// calculateActiveWalletRates calculates active wallet rates for different time periods
func (ec *EnhancedCollector) calculateActiveWalletRates() ActiveWalletRates {
	now := time.Now()
	uptime := now.Sub(ec.startTime).Seconds()

	// Calculate counts for different periods using consolidated function
	countsLastMin := ec.countAllInPeriod(now.Add(-time.Minute))
	countsLastHour := ec.countAllInPeriod(now.Add(-time.Hour))
	countsLastDay := ec.countAllInPeriod(now.Add(-24 * time.Hour))
	countsLast7d := ec.countAllInPeriod(now.AddDate(0, 0, -7))
	countsLast14d := ec.countAllInPeriod(now.AddDate(0, 0, -14))
	countsLast30d := ec.countAllInPeriod(now.AddDate(0, 0, -30))

	// Calculate percentage changes from previous periods
	var sendersLastMinChange, sendersLastHourChange, sendersLastDayChange float64
	var sendersLast7dChange, sendersLast14dChange, sendersLast30dChange float64
	var receiversLastMinChange, receiversLastHourChange, receiversLastDayChange float64
	var receiversLast7dChange, receiversLast14dChange, receiversLast30dChange float64
	var totalLastMinChange, totalLastHourChange, totalLastDayChange float64
	var totalLast7dChange, totalLast14dChange, totalLast30dChange float64

	if ec.previousMinute != nil {
		sendersLastMinChange = ec.calculatePercentageChange(float64(countsLastMin.Senders), float64(ec.previousMinute.ActiveWalletRates.SendersLastMin))
		receiversLastMinChange = ec.calculatePercentageChange(float64(countsLastMin.Receivers), float64(ec.previousMinute.ActiveWalletRates.ReceiversLastMin))
		totalLastMinChange = ec.calculatePercentageChange(float64(countsLastMin.Total), float64(ec.previousMinute.ActiveWalletRates.TotalLastMin))
	}
	if ec.previousHour != nil {
		sendersLastHourChange = ec.calculatePercentageChange(float64(countsLastHour.Senders), float64(ec.previousHour.ActiveWalletRates.SendersLastHour))
		receiversLastHourChange = ec.calculatePercentageChange(float64(countsLastHour.Receivers), float64(ec.previousHour.ActiveWalletRates.ReceiversLastHour))
		totalLastHourChange = ec.calculatePercentageChange(float64(countsLastHour.Total), float64(ec.previousHour.ActiveWalletRates.TotalLastHour))
	}
	if ec.previousDay != nil {
		sendersLastDayChange = ec.calculatePercentageChange(float64(countsLastDay.Senders), float64(ec.previousDay.ActiveWalletRates.SendersLastDay))
		receiversLastDayChange = ec.calculatePercentageChange(float64(countsLastDay.Receivers), float64(ec.previousDay.ActiveWalletRates.ReceiversLastDay))
		totalLastDayChange = ec.calculatePercentageChange(float64(countsLastDay.Total), float64(ec.previousDay.ActiveWalletRates.TotalLastDay))
	}
	if ec.previous7Days != nil {
		sendersLast7dChange = ec.calculatePercentageChange(float64(countsLast7d.Senders), float64(ec.previous7Days.ActiveWalletRates.SendersLast7d))
		receiversLast7dChange = ec.calculatePercentageChange(float64(countsLast7d.Receivers), float64(ec.previous7Days.ActiveWalletRates.ReceiversLast7d))
		totalLast7dChange = ec.calculatePercentageChange(float64(countsLast7d.Total), float64(ec.previous7Days.ActiveWalletRates.TotalLast7d))
	}
	if ec.previous14Days != nil {
		sendersLast14dChange = ec.calculatePercentageChange(float64(countsLast14d.Senders), float64(ec.previous14Days.ActiveWalletRates.SendersLast14d))
		receiversLast14dChange = ec.calculatePercentageChange(float64(countsLast14d.Receivers), float64(ec.previous14Days.ActiveWalletRates.ReceiversLast14d))
		totalLast14dChange = ec.calculatePercentageChange(float64(countsLast14d.Total), float64(ec.previous14Days.ActiveWalletRates.TotalLast14d))
	}
	if ec.previous30Days != nil {
		sendersLast30dChange = ec.calculatePercentageChange(float64(countsLast30d.Senders), float64(ec.previous30Days.ActiveWalletRates.SendersLast30d))
		receiversLast30dChange = ec.calculatePercentageChange(float64(countsLast30d.Receivers), float64(ec.previous30Days.ActiveWalletRates.ReceiversLast30d))
		totalLast30dChange = ec.calculatePercentageChange(float64(countsLast30d.Total), float64(ec.previous30Days.ActiveWalletRates.TotalLast30d))
	}

	return ActiveWalletRates{
		SendersLastMin:      countsLastMin.Senders,
		SendersLastHour:     countsLastHour.Senders,
		SendersLastDay:      countsLastDay.Senders,
		SendersLast7d:       countsLast7d.Senders,
		SendersLast14d:      countsLast14d.Senders,
		SendersLast30d:      countsLast30d.Senders,
		SendersLastMinChange:    sendersLastMinChange,
		SendersLastHourChange:   sendersLastHourChange,
		SendersLastDayChange:    sendersLastDayChange,
		SendersLast7dChange:     sendersLast7dChange,
		SendersLast14dChange:    sendersLast14dChange,
		SendersLast30dChange:    sendersLast30dChange,
		ReceiversLastMin:    countsLastMin.Receivers,
		ReceiversLastHour:   countsLastHour.Receivers,
		ReceiversLastDay:    countsLastDay.Receivers,
		ReceiversLast7d:     countsLast7d.Receivers,
		ReceiversLast14d:    countsLast14d.Receivers,
		ReceiversLast30d:    countsLast30d.Receivers,
		ReceiversLastMinChange:  receiversLastMinChange,
		ReceiversLastHourChange: receiversLastHourChange,
		ReceiversLastDayChange:  receiversLastDayChange,
		ReceiversLast7dChange:   receiversLast7dChange,
		ReceiversLast14dChange:  receiversLast14dChange,
		ReceiversLast30dChange:  receiversLast30dChange,
		TotalLastMin:        countsLastMin.Total,
		TotalLastHour:       countsLastHour.Total,
		TotalLastDay:        countsLastDay.Total,
		TotalLast7d:         countsLast7d.Total,
		TotalLast14d:        countsLast14d.Total,
		TotalLast30d:        countsLast30d.Total,
		TotalLastMinChange:      totalLastMinChange,
		TotalLastHourChange:     totalLastHourChange,
		TotalLastDayChange:      totalLastDayChange,
		TotalLast7dChange:       totalLast7dChange,
		TotalLast14dChange:      totalLast14dChange,
		TotalLast30dChange:      totalLast30dChange,
		UniqueSendersTotal:  int(ec.globalSendersHLL.Estimate()),
		UniqueReceiversTotal: int(ec.globalReceiversHLL.Estimate()),
		UniqueTotalWallets:  int(ec.globalWalletsHLL.Estimate()),
		ServerUptimeSeconds: uptime,
	}
}

// calculateWalletData calculates comprehensive wallet data
func (ec *EnhancedCollector) calculateWalletData() *WalletData {
	uptime := time.Since(ec.startTime).Seconds()

	// Get all unique wallets (combine senders and receivers)
	allWallets := make(map[string]*WalletStats)
	
	// Add all senders
	for address, stats := range ec.senderStats {
		allWallets[address] = &WalletStats{
			Count:        stats.Count,
			Address:      stats.Address,
			LastActivity: stats.LastActivity,
		}
	}
	
	// Add receivers (merge if already exists as sender)
	for address, stats := range ec.receiverStats {
		if existing, exists := allWallets[address]; exists {
			existing.Count += stats.Count
			if stats.LastActivity.After(existing.LastActivity) {
				existing.LastActivity = stats.LastActivity
			}
		} else {
			allWallets[address] = &WalletStats{
				Count:        stats.Count,
				Address:      stats.Address,
				LastActivity: stats.LastActivity,
			}
		}
	}

	// Convert to slice for sorting
	wallets := make([]*WalletStats, 0, len(allWallets))
	for _, wallet := range allWallets {
		wallets = append(wallets, wallet)
	}

	// Sort by activity count (descending)
	sort.Slice(wallets, func(i, j int) bool {
		return wallets[i].Count > wallets[j].Count
	})

	return &WalletData{
		Wallets:             wallets,
		WalletActivityTimeScale: ec.getWalletActivityTimeScale(),
		TotalWallets:        len(wallets),
		ServerUptimeSeconds: uptime,
	}
}

// calculateWalletDataForBroadcast calculates wallet data with broadcast limits
func (ec *EnhancedCollector) calculateWalletDataForBroadcast() *WalletData {
	uptime := time.Since(ec.startTime).Seconds()

	// Get all unique wallets (combine senders and receivers)
	allWallets := make(map[string]*WalletStats)
	
	// Add all senders
	for address, stats := range ec.senderStats {
		allWallets[address] = &WalletStats{
			Count:        stats.Count,
			Address:      stats.Address,
			LastActivity: stats.LastActivity,
		}
	}
	
	// Add receivers (merge if already exists as sender)
	for address, stats := range ec.receiverStats {
		if existing, exists := allWallets[address]; exists {
			existing.Count += stats.Count
			if stats.LastActivity.After(existing.LastActivity) {
				existing.LastActivity = stats.LastActivity
			}
		} else {
			allWallets[address] = &WalletStats{
				Count:        stats.Count,
				Address:      stats.Address,
				LastActivity: stats.LastActivity,
			}
		}
	}

	// Convert to slice for sorting
	wallets := make([]*WalletStats, 0, len(allWallets))
	for _, wallet := range allWallets {
		wallets = append(wallets, wallet)
	}

	// Sort by activity count (descending)
	sort.Slice(wallets, func(i, j int) bool {
		return wallets[i].Count > wallets[j].Count
	})

	// Apply broadcast limit if configured
	if ec.config.MaxWalletsInBroadcast > 0 && len(wallets) > ec.config.MaxWalletsInBroadcast {
		wallets = wallets[:ec.config.MaxWalletsInBroadcast]
	}

	return &WalletData{
		Wallets:             wallets,
		WalletActivityTimeScale: ec.getWalletActivityTimeScale(),
		TotalWallets:        len(allWallets), // Keep total count even if we limit broadcast
		ServerUptimeSeconds: uptime,
	}
}

// getWalletActivityTimeScale returns wallet activity data for different time scales
func (ec *EnhancedCollector) getWalletActivityTimeScale() map[string][]*WalletActivityBucket {
	timeScales := getTimePeriodsFromNow()
	result := make(map[string][]*WalletActivityBucket)

	for timeframe, since := range timeScales {
		var buckets []*WalletActivityBucket
		for _, bucket := range ec.walletActivityBuckets {
			if bucket.Timestamp.After(since) {
				buckets = append(buckets, bucket)
			}
		}
		
		// Sort by timestamp
		sort.Slice(buckets, func(i, j int) bool {
			return buckets[i].Timestamp.Before(buckets[j].Timestamp)
		})
		
		result[timeframe] = buckets
	}

	return result
}

// getTopWalletsInPeriod returns top wallets for a specific activity type and time period
func (ec *EnhancedCollector) getTopWalletsInPeriod(activityType WalletActivityType, since time.Time, limit int) []*WalletStats {
	walletCounts := make(map[string]int64)
	walletLastActivity := make(map[string]time.Time)

	// Aggregate wallet activity from buckets in the specified period
	for _, bucket := range ec.walletActivityBuckets {
		if bucket.Timestamp.After(since) {
			var countsMap map[string]int64
			switch activityType {
			case SenderActivity:
				countsMap = bucket.SenderCounts
			case ReceiverActivity:
				countsMap = bucket.ReceiverCounts
			}

			for wallet, count := range countsMap {
				walletCounts[wallet] += count
				if bucket.Timestamp.After(walletLastActivity[wallet]) {
					walletLastActivity[wallet] = bucket.Timestamp
				}
			}
		}
	}

	// Convert to slice for sorting
	wallets := make([]*WalletStats, 0, len(walletCounts))
	for wallet, count := range walletCounts {
		wallets = append(wallets, &WalletStats{
			Count:        count,
			Address:      wallet,
			LastActivity: walletLastActivity[wallet],
		})
	}

	// Sort by count (descending)
	sort.Slice(wallets, func(i, j int) bool {
		return wallets[i].Count > wallets[j].Count
	})

	// Return top N
	if len(wallets) > limit {
		wallets = wallets[:limit]
	}

	return wallets
}

// getActiveSendersTimeScale returns active senders for different time scales
func (ec *EnhancedCollector) getActiveSendersTimeScale() map[string][]*WalletStats {
	timeScales := getTimePeriodsFromNow()
	result := make(map[string][]*WalletStats)

	for timeframe, since := range timeScales {
		result[timeframe] = ec.getTopWalletsInPeriod(SenderActivity, since, ec.config.TopItemsTimeScale)
	}

	return result
}

// getActiveReceiversTimeScale returns active receivers for different time scales
func (ec *EnhancedCollector) getActiveReceiversTimeScale() map[string][]*WalletStats {
	timeScales := getTimePeriodsFromNow()
	result := make(map[string][]*WalletStats)

	for timeframe, since := range timeScales {
		result[timeframe] = ec.getTopWalletsInPeriod(ReceiverActivity, since, ec.config.TopItemsTimeScale)
	}

	return result
}

// Legacy functions for backward compatibility
func (ec *EnhancedCollector) getTopSenders(limit int) []*WalletStats {
	// Convert map to slice for sorting
	senders := make([]*WalletStats, 0, len(ec.senderStats))
	for _, sender := range ec.senderStats {
		senders = append(senders, sender)
	}

	// Sort by count (descending)
	sort.Slice(senders, func(i, j int) bool {
		return senders[i].Count > senders[j].Count
	})

	// Return top N
	if len(senders) > limit {
		senders = senders[:limit]
	}

	return senders
}

func (ec *EnhancedCollector) getTopReceivers(limit int) []*WalletStats {
	// Convert map to slice for sorting
	receivers := make([]*WalletStats, 0, len(ec.receiverStats))
	for _, receiver := range ec.receiverStats {
		receivers = append(receivers, receiver)
	}

	// Sort by count (descending)
	sort.Slice(receivers, func(i, j int) bool {
		return receivers[i].Count > receivers[j].Count
	})

	// Return top N
	if len(receivers) > limit {
		receivers = receivers[:limit]
	}

	return receivers
}

func (ec *EnhancedCollector) getTopSendersInPeriod(since time.Time, limit int) []*WalletStats {
	return ec.getTopWalletsInPeriod(SenderActivity, since, limit)
}

func (ec *EnhancedCollector) getTopReceiversInPeriod(since time.Time, limit int) []*WalletStats {
	return ec.getTopWalletsInPeriod(ReceiverActivity, since, limit)
} 