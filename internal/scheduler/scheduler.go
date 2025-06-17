package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/models"
)

// Config holds scheduler configuration
type Config struct {
	MinDelay    time.Duration `json:"minDelay"`    // Minimum delay between transfers (default: 200ms)
	MaxDelay    time.Duration `json:"maxDelay"`    // Maximum delay between transfers (default: 2s)
	JitterRange time.Duration `json:"jitterRange"` // Random jitter range (default: 100ms)
}

// DefaultConfig returns default scheduler configuration
func DefaultConfig() Config {
	return Config{
		MinDelay:    200 * time.Millisecond,
		MaxDelay:    2 * time.Second,
		JitterRange: 100 * time.Millisecond,
	}
}

// Scheduler handles timing and streaming of enhanced transfers
type Scheduler struct {
	config    Config
	channels  *channels.Channels
	rand      *rand.Rand
	totalSent int64
	mu        sync.RWMutex
}

// NewScheduler creates a new scheduler
func NewScheduler(config Config, channels *channels.Channels) *Scheduler {
	return &Scheduler{
		config:   config,
		channels: channels,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Start begins the scheduler thread
func (s *Scheduler) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
			
		case transfers := <-s.channels.EnhancedTransfers:
			fmt.Printf("[SCHEDULER] Received %d enhanced transfers from enhancer\n", len(transfers))
			s.processTransferBatch(transfers)
		}
	}
}

// processTransferBatch processes a batch of enhanced transfers
func (s *Scheduler) processTransferBatch(transfers []models.Transfer) {
	if len(transfers) == 0 {
		return
	}
	
	// Process each transfer sequentially to maintain proper timing order
	go func() {
		for _, transfer := range transfers {
			s.sendTransferWithTiming(transfer)
		}
	}()
}

// sendTransferWithTiming sends a transfer with natural timing
func (s *Scheduler) sendTransferWithTiming(transfer models.Transfer) {
	// Calculate natural timing (200ms to 2s with jitter)
	baseDelayMs := int(s.config.MinDelay.Milliseconds()) + 
		s.rand.Intn(int(s.config.MaxDelay.Milliseconds()-s.config.MinDelay.Milliseconds()))
	jitterMs := s.rand.Intn(int(s.config.JitterRange.Milliseconds()))
	
	totalDelay := time.Duration(baseDelayMs+jitterMs) * time.Millisecond
	
	// Apply timing
	time.Sleep(totalDelay)
	
	// Send to broadcaster (stats are handled by enhancer at true fetch rate)
	select {
	case s.channels.TransferBroadcasts <- transfer:
		fmt.Printf("[SCHEDULER] Sent transfer %s to broadcaster\n", transfer.PacketHash)
	default:
		fmt.Printf("[SCHEDULER] Warning: broadcaster channel full, dropping transfer %s\n", transfer.PacketHash)
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
		"totalSent":    s.totalSent,
		"minDelay":     s.config.MinDelay.String(),
		"maxDelay":     s.config.MaxDelay.String(),
		"jitterRange":  s.config.JitterRange.String(),
	}
} 