package scheduler

import (
	"context"
	"sync"
	"time"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/internal/utils"
	"websocket-backend-new/models"
)

// Config holds scheduler configuration for timestamp-based natural flow
type Config struct {
	LiveOffset       time.Duration `json:"liveOffset"`       // How far ahead of "now" to broadcast (default: 2s)
	MaxBacklog       time.Duration `json:"maxBacklog"`       // Max time to wait for old transfers (default: 10s)
	MinInterval      time.Duration `json:"minInterval"`      // Minimum interval between broadcasts (default: 50ms)
}

// DefaultConfig returns default scheduler configuration for timestamp-based flow
func DefaultConfig() Config {
	return Config{
		LiveOffset:  2 * time.Second,   // Broadcast 2 seconds after "live" time
		MaxBacklog:  10 * time.Second,  // Don't wait more than 10s for old transfers
		MinInterval: 50 * time.Millisecond, // Minimum 50ms between broadcasts
	}
}

// Scheduler handles timestamp-based realistic timing and streaming of enhanced transfers
type Scheduler struct {
	config    Config
	channels  *channels.Channels
	totalSent int64
	mu        sync.RWMutex
}

// NewScheduler creates a new scheduler
func NewScheduler(config Config, channels *channels.Channels) *Scheduler {
	return &Scheduler{
		config:   config,
		channels: channels,
	}
}

// Start begins the scheduler thread
func (s *Scheduler) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
			
		case transfers := <-s.channels.ProcessedTransfers:
			utils.LogInfo("SCHEDULER", "Received %d processed transfers from processor", len(transfers))
			s.processTransferBatch(transfers)
		}
	}
}

// processTransferBatch processes a batch of enhanced transfers with timestamp-based timing
func (s *Scheduler) processTransferBatch(transfers []models.Transfer) {
	if len(transfers) == 0 {
		return
	}
	
	// Process transfers sequentially to maintain chronological order with micro-spacing
	go func() {
		for i, transfer := range transfers {
			s.sendTransferWithTiming(transfer, i)
		}
	}()
}

// sendTransferWithTiming sends a transfer with realistic timestamp-based timing and micro-spacing
func (s *Scheduler) sendTransferWithTiming(transfer models.Transfer, batchIndex int) {
	now := time.Now()
	
	// Calculate target broadcast time: transfer timestamp + live offset
	targetTime := transfer.TransferSendTimestamp.Add(s.config.LiveOffset)
	
	// Calculate delay needed to reach target time
	delay := targetTime.Sub(now)
	
	// Add micro-spacing to prevent batch feel (25ms per transfer in batch)
	microSpacing := time.Duration(batchIndex) * (s.config.MinInterval / 2) // 25ms per transfer
	
	// Handle different timing scenarios
	var actualDelay time.Duration
	var reason string
	
	if delay <= 0 {
		// Transfer is from the past (older than live offset)
		actualDelay = s.config.MinInterval + microSpacing
		reason = "past_transfer_spaced"
	} else if delay > s.config.MaxBacklog {
		// Transfer is too far in the future, don't wait too long
		actualDelay = s.config.MaxBacklog + microSpacing
		reason = "future_capped_spaced"
	} else {
		// Transfer is within reasonable timing window
		actualDelay = delay + microSpacing
		reason = "realistic_timing_spaced"
	}
	
	// Apply the calculated delay
	if actualDelay > 0 {
		time.Sleep(actualDelay)
	}
	
	// Send to broadcaster with reliable delivery (no drops!)
	config := utils.DefaultBackpressureConfig()
	config.TimeoutMs = 1000    // Longer timeout for reliable delivery
	config.DropOnOverflow = false  // Never drop transfers - they're valuable for smooth flow!
	
	if utils.SendWithBackpressure(s.channels.TransferBroadcasts, transfer, config, nil) {
		utils.LogDebug("SCHEDULER", "Sent transfer %s after %v delay (%s) - target: %v, spacing: %v", 
			transfer.PacketHash, actualDelay, reason, targetTime.Format("15:04:05.000"), microSpacing)
	} else {
		utils.LogWarn("SCHEDULER", "Failed to send transfer %s to broadcaster (channel busy)", transfer.PacketHash)
	}
	
	s.mu.Lock()
	s.totalSent++
	s.mu.Unlock()
}

// GetStats returns scheduler statistics
func (s *Scheduler) GetStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	return map[string]interface{}{
		"totalSent":         s.totalSent,
		"broadcastingMode":  "timestamp_based",
		"liveOffset":        s.config.LiveOffset.String(),
		"maxBacklog":        s.config.MaxBacklog.String(),
		"minInterval":       s.config.MinInterval.String(),
		"microSpacing":      (s.config.MinInterval / 2).String(),
		"realisticTiming":   true,
		"smoothDelivery":    true,
		"timingSource":      "transfer_send_timestamp",
	}
} 