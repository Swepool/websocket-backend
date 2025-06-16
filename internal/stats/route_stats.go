package stats

import (
	"sort"
	"time"
)

// getTopRoutes returns the top N routes by transfer count
func (ec *EnhancedCollector) getTopRoutes(limit int) []*RouteStats {
	// Convert map to slice for sorting
	routes := make([]*RouteStats, 0, len(ec.routeStats))
	for _, route := range ec.routeStats {
		routes = append(routes, route)
	}

	// Sort by count (descending)
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].Count > routes[j].Count
	})

	// Return top N
	if len(routes) > limit {
		routes = routes[:limit]
	}

	return routes
}

// getTopRoutesInPeriod returns top routes for a specific time period
func (ec *EnhancedCollector) getTopRoutesInPeriod(since time.Time, limit int) []*RouteStats {
	routeCounts := make(map[string]int64)
	routeInfo := make(map[string]*RouteStats)

	// Aggregate route activity from buckets in the specified period
	for _, bucket := range ec.routeActivityBuckets {
		if bucket.Timestamp.After(since) {
			for routeKey, count := range bucket.RouteCounts {
				routeCounts[routeKey] += count
				
				// Store route info if we haven't seen it yet
				if _, exists := routeInfo[routeKey]; !exists {
					if globalRoute, globalExists := ec.routeStats[routeKey]; globalExists {
						routeInfo[routeKey] = &RouteStats{
							Count:     0, // Will be set below
							FromChain: globalRoute.FromChain,
							ToChain:   globalRoute.ToChain,
							FromName:  globalRoute.FromName,
							ToName:    globalRoute.ToName,
						}
					}
				}
			}
		}
	}

	// Convert to slice for sorting
	routes := make([]*RouteStats, 0, len(routeCounts))
	for routeKey, count := range routeCounts {
		if info, exists := routeInfo[routeKey]; exists {
			info.Count = count
			routes = append(routes, info)
		}
	}

	// Sort by count (descending)
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].Count > routes[j].Count
	})

	// Return top N
	if len(routes) > limit {
		routes = routes[:limit]
	}

	return routes
}

// getPopularRoutesTimeScale returns popular routes for different time scales
func (ec *EnhancedCollector) getPopularRoutesTimeScale() map[string][]*RouteStats {
	timeScales := getTimePeriodsFromNow()
	result := make(map[string][]*RouteStats)

	for timeframe, since := range timeScales {
		result[timeframe] = ec.getTopRoutesInPeriod(since, ec.config.TopItemsTimeScale)
	}

	return result
}

// calculateRoutePercentageChanges calculates percentage changes for routes
func (ec *EnhancedCollector) calculateRoutePercentageChanges(routes []*RouteStats, previousRoutes map[string][]*RouteStats, timeframe string) {
	if previousRoutes == nil {
		return
	}

	previousRoutesForTimeframe, exists := previousRoutes[timeframe]
	if !exists {
		return
	}

	// Create a map for quick lookup of previous route counts
	previousCounts := make(map[string]int64)
	for _, prevRoute := range previousRoutesForTimeframe {
		routeKey := prevRoute.FromChain + "->" + prevRoute.ToChain
		previousCounts[routeKey] = prevRoute.Count
	}

	// Calculate percentage changes
	for _, route := range routes {
		routeKey := route.FromChain + "->" + route.ToChain
		if prevCount, exists := previousCounts[routeKey]; exists {
			route.CountChange = ec.calculatePercentageChange(float64(route.Count), float64(prevCount))
		}
	}
} 