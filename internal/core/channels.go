package core

import (
	"websocket-backend/models"
)

// Channels holds all communication channels for the clean modular pipeline
type Channels struct {
	// Clean pipeline flow: Fetcher → Processor → {Broadcaster, Batcher}
	RawTransfers        chan []models.Transfer // Fetcher → Processor (batches of 100)
	BatchedTransfers    chan models.Transfer   // Processor → Batcher (individual transfers for real-time DB)
	WebSocketBroadcasts chan models.Transfer   // Processor → Broadcaster (individual transfers with natural timing)
	DatabaseSaves       chan []models.Transfer // BackwardFetcher → Database (batches of 100 for historical data)
}

// NewChannels creates and initializes all channels with optimized buffer sizes for dual sync mode
func NewChannels() *Channels {
	return &Channels{
		// Raw transfers from fetcher to processor (batched)
		RawTransfers: make(chan []models.Transfer, 2000),  
		
		// Individual transfers from processor to batcher (for efficient DB writes)
		BatchedTransfers: make(chan models.Transfer, 50000),
		
		// Individual transfers from processor to broadcaster (WebSocket with natural timing)
		WebSocketBroadcasts: make(chan models.Transfer, 25000),
		
		// Batched historical transfers from backward fetcher to database
		DatabaseSaves: make(chan []models.Transfer, 1000),
	}
}

// Close closes all channels
func (c *Channels) Close() {
	close(c.RawTransfers)
	close(c.BatchedTransfers)
	close(c.WebSocketBroadcasts)
	close(c.DatabaseSaves)
}

// GetChannelStats returns current channel utilization for monitoring
func (c *Channels) GetChannelStats() map[string]interface{} {
	return map[string]interface{}{
		"raw_transfers": map[string]interface{}{
			"length":  len(c.RawTransfers),
			"capacity": cap(c.RawTransfers),
			"utilization": float64(len(c.RawTransfers)) / float64(cap(c.RawTransfers)) * 100,
		},
		"batched_transfers": map[string]interface{}{
			"length":  len(c.BatchedTransfers),
			"capacity": cap(c.BatchedTransfers),
			"utilization": float64(len(c.BatchedTransfers)) / float64(cap(c.BatchedTransfers)) * 100,
		},
		"websocket_broadcasts": map[string]interface{}{
			"length":  len(c.WebSocketBroadcasts),
			"capacity": cap(c.WebSocketBroadcasts),
			"utilization": float64(len(c.WebSocketBroadcasts)) / float64(cap(c.WebSocketBroadcasts)) * 100,
		},
		"database_saves": map[string]interface{}{
			"length":  len(c.DatabaseSaves),
			"capacity": cap(c.DatabaseSaves),
			"utilization": float64(len(c.DatabaseSaves)) / float64(cap(c.DatabaseSaves)) * 100,
		},
	}
} 