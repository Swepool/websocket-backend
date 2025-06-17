package channels

import "websocket-backend-new/models"

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

// NewChannels creates and initializes all channels
func NewChannels() *Channels {
	return &Channels{
		RawTransfers:       make(chan []models.Transfer, 2000),   // Handle 2000 batches (even 500-transfer batches)
		EnhancedTransfers:  make(chan []models.Transfer, 2000),   // Same capacity for enhanced batches
		TransferBroadcasts: make(chan models.Transfer, 20000),    // Handle 20k individual transfers (40s at 500 TPS)
		StatsUpdates:       make(chan models.Transfer, 20000),    // Same for stats processing
		ChartUpdates:       make(chan interface{}, 200),          // More chart update capacity
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