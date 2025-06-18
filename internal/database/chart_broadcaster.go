package database

import (
	"websocket-backend-new/internal/utils"
	"websocket-backend-new/internal/broadcaster"
)

// ChartBroadcaster handles broadcasting chart data updates via WebSocket
type ChartBroadcaster struct {
	chartService *EnhancedChartService
	broadcaster  broadcaster.BroadcasterInterface
	stopCh       chan struct{}
}

// NewChartBroadcaster creates a new chart broadcaster
func NewChartBroadcaster(chartService *EnhancedChartService, broadcasterInstance broadcaster.BroadcasterInterface) *ChartBroadcaster {
	return &ChartBroadcaster{
		chartService: chartService,
		broadcaster:  broadcasterInstance,
		stopCh:       make(chan struct{}),
	}
}

// Start begins broadcasting chart updates on-demand (when cache is refreshed)
func (cb *ChartBroadcaster) Start() {
	utils.LogInfo("CHART_BROADCASTER", "Starting chart broadcaster (event-driven mode)")
	
	// Send initial chart data immediately
	go cb.broadcastChartData()
	
	// No timer - broadcasts are now triggered by cache updates
}

// Stop stops the chart broadcaster
func (cb *ChartBroadcaster) Stop() {
	close(cb.stopCh)
	utils.LogInfo("CHART_BROADCASTER", "Chart broadcaster stopped")
}

// TriggerBroadcast triggers an immediate chart data broadcast (called when cache is updated)
func (cb *ChartBroadcaster) TriggerBroadcast() {
	go cb.broadcastChartData()
}

// BroadcastChartData sends current chart data to all WebSocket clients
func (cb *ChartBroadcaster) broadcastChartData() {
	chartData, err := cb.chartService.GetChartDataForFrontend()
	if err != nil {
		utils.LogError("CHART_BROADCASTER", "Failed to get chart data: %v", err)
		return
	}
	
	// Use broadcaster's built-in chart broadcasting method (handles JSON marshaling internally)
	if shardedBroadcaster, ok := cb.broadcaster.(*broadcaster.Broadcaster); ok {
		shardedBroadcaster.BroadcastChartData(chartData)
	}
	
	clientCount := cb.broadcaster.GetClientCount()
	utils.LogDebug("CHART_BROADCASTER", "Chart data broadcasted to %d clients", clientCount)
}

// BroadcastNow forces an immediate chart data broadcast
func (cb *ChartBroadcaster) BroadcastNow() {
	go cb.broadcastChartData()
} 