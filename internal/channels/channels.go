package channels

import (
	"websocket-backend-new/internal/utils"
	"websocket-backend-new/models"
)

// Channels holds all communication channels for the pipeline
type Channels struct {
	// Pipeline channels
	RawTransfers      chan []models.Transfer // Fetcher → Enhancer
	EnhancedTransfers chan []models.Transfer // Enhancer → Scheduler
	TransferBroadcasts chan models.Transfer  // Scheduler → Broadcaster (one-by-one)
	
	// Stats channel (separate from main flow)
	StatsUpdates      chan models.Transfer  // Stats collector listens here
	
	// Optional: Chart updates for WebSocket broadcasts
	ChartUpdates      chan interface{}      // Chart data updates
}

// NewChannels creates and initializes all channels with optimized buffer sizes
func NewChannels() *Channels {
	return &Channels{
		// Use optimized defaults with environment overrides
		RawTransfers: make(chan []models.Transfer, 
			utils.DefaultGetChannelBufferSize("TransferUpdates", 2000)),
		
		EnhancedTransfers: make(chan []models.Transfer,
			utils.DefaultGetChannelBufferSize("TransferEnhanced", 2000)),
		
		TransferBroadcasts: make(chan models.Transfer,
			utils.DefaultGetChannelBufferSize("TransferBroadcasts", 200)),
		
		StatsUpdates: make(chan models.Transfer,
			utils.DefaultGetChannelBufferSize("TransferToSchedule", 2000)),
		
		// Critical optimization: 50x larger buffer for chart updates
		ChartUpdates: make(chan interface{},
			utils.DefaultGetChannelBufferSize("ChartUpdates", 200)),
	}
}

// Close closes all channels
func (c *Channels) Close() {
	close(c.RawTransfers)
	close(c.EnhancedTransfers)
	close(c.TransferBroadcasts)
	close(c.StatsUpdates)
	close(c.ChartUpdates)
} 