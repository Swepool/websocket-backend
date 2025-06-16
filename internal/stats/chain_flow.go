package stats

import (
	"sort"
	"time"
)

// calculateChainFlowData calculates comprehensive chain flow data
func (ec *EnhancedCollector) calculateChainFlowData() ChainFlowData {
	uptime := time.Since(ec.startTime).Seconds()

	// Convert chain flow stats to slice for sorting
	chains := make([]*ChainFlowStats, 0, len(ec.chainFlowStats))
	var totalOutgoing, totalIncoming int64

	for _, chainFlow := range ec.chainFlowStats {
		// Calculate percentage changes if we have previous data
		ec.calculateChainFlowPercentageChanges(chainFlow)
		
		chains = append(chains, chainFlow)
		totalOutgoing += chainFlow.OutgoingCount
		totalIncoming += chainFlow.IncomingCount
	}

	// Sort by net flow (descending - most positive net flow first)
	sort.Slice(chains, func(i, j int) bool {
		return chains[i].NetFlow > chains[j].NetFlow
	})

	return ChainFlowData{
		Chains:              chains,
		ChainFlowTimeScale:  ec.getChainFlowTimeScale(),
		TotalOutgoing:       totalOutgoing,
		TotalIncoming:       totalIncoming,
		ServerUptimeSeconds: uptime,
	}
}

// calculateChainFlowPercentageChanges calculates percentage changes for chain flow stats
func (ec *EnhancedCollector) calculateChainFlowPercentageChanges(chainFlow *ChainFlowStats) {
	// Try to find previous data for this chain
	var previousChainFlow *ChainFlowStats
	
	// Check different time periods for previous data
	if ec.previousDay != nil {
		for _, prevChain := range ec.previousDay.ChainFlowData.Chains {
			if prevChain.UniversalChainID == chainFlow.UniversalChainID {
				previousChainFlow = prevChain
				break
			}
		}
	}
	
	if previousChainFlow != nil {
		chainFlow.OutgoingChange = ec.calculatePercentageChange(float64(chainFlow.OutgoingCount), float64(previousChainFlow.OutgoingCount))
		chainFlow.IncomingChange = ec.calculatePercentageChange(float64(chainFlow.IncomingCount), float64(previousChainFlow.IncomingCount))
		chainFlow.NetFlowChange = ec.calculatePercentageChange(float64(chainFlow.NetFlow), float64(previousChainFlow.NetFlow))
	}
}

// getChainFlowTimeScale returns chain flow data for different time scales
func (ec *EnhancedCollector) getChainFlowTimeScale() map[string][]*ChainFlowStats {
	timeScales := getTimePeriodsFromNow()
	result := make(map[string][]*ChainFlowStats)

	for timeframe, since := range timeScales {
		result[timeframe] = ec.getChainFlowInPeriod(since)
	}

	return result
}

// getChainFlowInPeriod returns chain flow stats for a specific time period
func (ec *EnhancedCollector) getChainFlowInPeriod(since time.Time) []*ChainFlowStats {
	chainOutgoing := make(map[string]int64)
	chainIncoming := make(map[string]int64)
	chainNames := make(map[string]string)
	chainLastActivity := make(map[string]time.Time)

	// Aggregate chain flow from buckets in the specified period
	for _, bucket := range ec.chainFlowBuckets {
		if bucket.Timestamp.After(since) {
			// Aggregate outgoing counts
			for chainID, count := range bucket.OutgoingCounts {
				chainOutgoing[chainID] += count
				if bucket.Timestamp.After(chainLastActivity[chainID]) {
					chainLastActivity[chainID] = bucket.Timestamp
				}
				
				// Get chain name from global stats if available
				if globalChain, exists := ec.chainFlowStats[chainID]; exists {
					chainNames[chainID] = globalChain.ChainName
				}
			}
			
			// Aggregate incoming counts
			for chainID, count := range bucket.IncomingCounts {
				chainIncoming[chainID] += count
				if bucket.Timestamp.After(chainLastActivity[chainID]) {
					chainLastActivity[chainID] = bucket.Timestamp
				}
				
				// Get chain name from global stats if available
				if globalChain, exists := ec.chainFlowStats[chainID]; exists {
					chainNames[chainID] = globalChain.ChainName
				}
			}
		}
	}

	// Create chain flow stats for all chains that had activity
	allChains := make(map[string]bool)
	for chainID := range chainOutgoing {
		allChains[chainID] = true
	}
	for chainID := range chainIncoming {
		allChains[chainID] = true
	}

	chains := make([]*ChainFlowStats, 0, len(allChains))
	for chainID := range allChains {
		outgoing := chainOutgoing[chainID]
		incoming := chainIncoming[chainID]
		netFlow := incoming - outgoing
		
		chainName := chainNames[chainID]
		if chainName == "" {
			chainName = chainID // Fallback to ID if name not available
		}

		chains = append(chains, &ChainFlowStats{
			UniversalChainID: chainID,
			ChainName:        chainName,
			OutgoingCount:    outgoing,
			IncomingCount:    incoming,
			NetFlow:          netFlow,
			LastActivity:     chainLastActivity[chainID],
		})
	}

	// Sort by net flow (descending)
	sort.Slice(chains, func(i, j int) bool {
		return chains[i].NetFlow > chains[j].NetFlow
	})

	return chains
} 