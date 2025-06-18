package channels

import (
	"websocket-backend-new/internal/utils"
	"websocket-backend-new/models"
)

// Channels holds all communication channels for the pipeline
type Channels struct {
	// Main pipeline flow
	RawTransfers       chan []models.Transfer // Fetcher → Processor
	ProcessedTransfers chan []models.Transfer // Processor → Scheduler
	TransferBroadcasts chan models.Transfer   // Scheduler → Broadcaster (one-by-one)
	
	// Database save channel (parallel from fetcher, saves raw data)
	DatabaseSaves      chan []models.Transfer // Fetcher → Database Writer (raw transfers)
	
	// Optional: Chart updates for WebSocket broadcasts
	ChartUpdates       chan interface{}       // Chart data updates (periodic)
}

// NewChannels creates and initializes all channels with optimized buffer sizes
func NewChannels() *Channels {
	return &Channels{
		// Raw transfers from fetcher to processor
		RawTransfers: make(chan []models.Transfer, 
			utils.DefaultGetChannelBufferSize("RawTransfers", 2000)),
		
		// Processed transfers from processor to scheduler
		ProcessedTransfers: make(chan []models.Transfer,
			utils.DefaultGetChannelBufferSize("ProcessedTransfers", 2000)),
		
		// Individual transfers from scheduler to broadcaster
		TransferBroadcasts: make(chan models.Transfer,
			utils.DefaultGetChannelBufferSize("TransferBroadcasts", 200)),
		
		// Raw transfer batches from fetcher to database (parallel path)
		DatabaseSaves: make(chan []models.Transfer,
			utils.DefaultGetChannelBufferSize("DatabaseSaves", 1000)), // Batch saves, smaller buffer needed
		
		// Chart updates (optional - periodic chart data)
		ChartUpdates: make(chan interface{},
			utils.DefaultGetChannelBufferSize("ChartUpdates", 100)),
	}
}

// Close closes all channels
func (c *Channels) Close() {
	close(c.RawTransfers)
	close(c.ProcessedTransfers)
	close(c.TransferBroadcasts)
	close(c.DatabaseSaves)
	close(c.ChartUpdates)
} 