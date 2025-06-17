package scheduler

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/internal/models"
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
	fmt.Printf("[SCHEDULER] Starting with natural timing (%v - %v + %v jitter)\n", 
		s.config.MinDelay, s.config.MaxDelay, s.config.JitterRange)
	
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[SCHEDULER] Shutting down\n")
			return
			
		case transfers := <-s.channels.EnhancedTransfers:
			s.processTransferBatch(transfers)
		}
	}
}

// processTransferBatch processes a batch of enhanced transfers
func (s *Scheduler) processTransferBatch(transfers []models.Transfer) {
	if len(transfers) == 0 {
		return
	}
	
	fmt.Printf("[SCHEDULER] Processing batch of %d transfers with natural streaming\n", len(transfers))
	
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
		// Successfully sent to broadcaster
	default:
		fmt.Printf("[SCHEDULER] Warning: Broadcaster channel full, dropping transfer %s\n", transfer.PacketHash)
	}
	
	s.mu.Lock()
	s.totalSent++
	currentTotal := s.totalSent
	s.mu.Unlock()
	
	if currentTotal%100 == 0 {
		fmt.Printf("[SCHEDULER] Sent %d transfers with natural timing to broadcaster\n", currentTotal)
	}
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