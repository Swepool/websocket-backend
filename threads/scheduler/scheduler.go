package scheduler

import (
	"context"
	"log"
	"math/rand"
	"sort"
	"time"
	"websocket-backend-new/models"
	"websocket-backend-new/storage"
)

// ScheduledTransfer represents a transfer scheduled for streaming delivery
type ScheduledTransfer struct {
	Transfer      models.Transfer
	ScheduledTime int64
}

// Scheduler handles scheduling transfers for natural streaming delivery
type Scheduler struct {
	memory         *storage.Memory
	channels       *storage.Channels
	scheduledQueue []ScheduledTransfer
}

// NewScheduler creates a new scheduler
func NewScheduler(memory *storage.Memory, channels *storage.Channels) *Scheduler {
	return &Scheduler{
		memory:         memory,
		channels:       channels,
		scheduledQueue: make([]ScheduledTransfer, 0),
	}
}

// Start begins the scheduling loop (Thread 3: Schedule)
func (s *Scheduler) Start(ctx context.Context) {
	log.Printf("[THREAD-3] 🚀 Starting scheduler (Natural streaming)")
	
	// Start the streaming processor
	go s.streamingProcessor(ctx)
	
	// Main scheduling loop
	for {
		select {
		case <-ctx.Done():
			log.Printf("[THREAD-3] 🛑 Context cancelled, stopping scheduler")
			return
			
		case <-s.channels.Shutdown:
			log.Printf("[THREAD-3] 🛑 Shutdown signal received, stopping")
			return
			
		case enhancedTransfers := <-s.channels.EnhancedTransfers:
			s.scheduleTransfers(enhancedTransfers)
		}
	}
}

// scheduleTransfers schedules transfers for natural streaming delivery
func (s *Scheduler) scheduleTransfers(transfers []models.Transfer) {
	if len(transfers) == 0 {
		return
	}
	
	log.Printf("[THREAD-3] ⏰ Scheduling %d transfers for streaming", len(transfers))
	
	now := time.Now().UnixMilli()
	
	// Sort transfers by timestamp for chronological order
	sort.Slice(transfers, func(i, j int) bool {
		return transfers[i].TransferSendTimestamp.Before(transfers[j].TransferSendTimestamp)
	})
	
	// Schedule with natural streaming delays (200ms to 2s spread)
	baseDelay := int64(200)  // Start 200ms from now
	maxSpread := int64(1800) // Spread over 1.8 seconds max
	
	for i, transfer := range transfers {
		// Calculate delay with natural spacing
		var delay int64
		if len(transfers) > 1 {
			// Linear distribution across the spread time
			delay = baseDelay + int64(float64(i)/float64(len(transfers)-1)*float64(maxSpread))
		} else {
			delay = baseDelay
		}
		
		// Add small jitter for naturalness (±100ms)
		jitter := int64((rand.Float64() - 0.5) * 200)
		scheduledTime := now + delay + jitter
		
		// Add to queue
		s.scheduledQueue = append(s.scheduledQueue, ScheduledTransfer{
			Transfer:      transfer,
			ScheduledTime: scheduledTime,
		})
	}
	
	// Sort queue by scheduled time
	sort.Slice(s.scheduledQueue, func(i, j int) bool {
		return s.scheduledQueue[i].ScheduledTime < s.scheduledQueue[j].ScheduledTime
	})
	
	log.Printf("[THREAD-3] ✅ Scheduled %d transfers (queue: %d)", len(transfers), len(s.scheduledQueue))
}

// streamingProcessor processes scheduled transfers and sends them one by one
func (s *Scheduler) streamingProcessor(ctx context.Context) {
	log.Printf("[THREAD-3] 🎬 Starting streaming processor")
	
	ticker := time.NewTicker(50 * time.Millisecond) // Check every 50ms for smooth streaming
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.channels.Shutdown:
			return
		case <-ticker.C:
			s.processReadyTransfers()
		}
	}
}

// processReadyTransfers processes transfers that are ready to be sent
func (s *Scheduler) processReadyTransfers() {
	if len(s.scheduledQueue) == 0 {
		return
	}
	
	now := time.Now().UnixMilli()
	ready := make([]models.Transfer, 0)
	remaining := make([]ScheduledTransfer, 0)
	
	// Split queue into ready and remaining
	for _, scheduled := range s.scheduledQueue {
		if scheduled.ScheduledTime <= now {
			ready = append(ready, scheduled.Transfer)
		} else {
			remaining = append(remaining, scheduled)
		}
	}
	
	// Update queue
	s.scheduledQueue = remaining
	
	// Send ready transfers ONE BY ONE for natural feel
	for _, transfer := range ready {
		s.sendSingleTransfer(transfer)
		
		// Small delay between sends for ultra-smooth streaming
		time.Sleep(10 * time.Millisecond)
	}
}

// sendSingleTransfer sends a single transfer to the broadcaster
func (s *Scheduler) sendSingleTransfer(transfer models.Transfer) {
	// Convert to broadcast format
	broadcastTransfer := transfer.ToBroadcastTransfer()
	
	log.Printf("[THREAD-3] 🚀 Streaming: %s → %s (%s %s)", 
		transfer.SourceDisplayName,
		transfer.DestinationDisplayName,
		transfer.BaseAmount,
		transfer.BaseTokenSymbol,
	)
	
	// Send to broadcaster as single-item array for frontend compatibility
	select {
	case s.channels.TransferBroadcasts <- []models.BroadcastTransfer{broadcastTransfer}:
		// Also update stats
		select {
		case s.channels.TransferStats <- []models.Transfer{transfer}:
		default:
			// Don't block if stats channel is full
		}
	default:
		log.Printf("[THREAD-3] ⚠️ Broadcast channel full, dropping transfer")
	}
}

// GetQueueStatus returns the current queue status
func (s *Scheduler) GetQueueStatus() map[string]interface{} {
	return map[string]interface{}{
		"queuedTransfers": len(s.scheduledQueue),
		"thread":          "scheduler",
		"mode":            "natural-streaming",
	}
} 