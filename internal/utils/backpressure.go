package utils

import (
	"sync/atomic"
	"time"
)

// BackpressureMetrics tracks channel overflow statistics
type BackpressureMetrics struct {
	channelOverflows int64
	channelTimeouts  int64
	droppedMessages  int64
}

// Inc increments the overflow counter
func (bm *BackpressureMetrics) IncOverflows() {
	atomic.AddInt64(&bm.channelOverflows, 1)
}

// IncTimeouts increments the timeout counter
func (bm *BackpressureMetrics) IncTimeouts() {
	atomic.AddInt64(&bm.channelTimeouts, 1)
}

// IncDropped increments the dropped messages counter
func (bm *BackpressureMetrics) IncDropped() {
	atomic.AddInt64(&bm.droppedMessages, 1)
}

// GetStats returns current metrics
func (bm *BackpressureMetrics) GetStats() (overflows, timeouts, dropped int64) {
	return atomic.LoadInt64(&bm.channelOverflows),
		atomic.LoadInt64(&bm.channelTimeouts),
		atomic.LoadInt64(&bm.droppedMessages)
}

// BackpressureConfig holds configuration for overflow handling
type BackpressureConfig struct {
	DropOnOverflow bool
	TimeoutMs      int
}

// DefaultBackpressureConfig returns sensible defaults
func DefaultBackpressureConfig() BackpressureConfig {
	return BackpressureConfig{
		DropOnOverflow: false, // Conservative default
		TimeoutMs:      100,   // 100ms timeout
	}
}

// SendWithBackpressure sends data to a channel with overflow protection
func SendWithBackpressure[T any](ch chan<- T, data T, config BackpressureConfig, metrics *BackpressureMetrics) bool {
	select {
	case ch <- data:
		return true // Success
	default:
		// Channel full - apply backpressure
		if metrics != nil {
			metrics.IncOverflows()
		}
		
		if config.DropOnOverflow {
			if metrics != nil {
				metrics.IncDropped()
			}
			return false // Drop message
		}
		
		// Block with timeout
		timeout := time.Duration(config.TimeoutMs) * time.Millisecond
		select {
		case ch <- data:
			return true // Success after wait
		case <-time.After(timeout):
			if metrics != nil {
				metrics.IncTimeouts()
			}
			return false // Timeout
		}
	}
}

// TrySend attempts to send without blocking, returns success status
func TrySend[T any](ch chan<- T, data T, metrics *BackpressureMetrics) bool {
	select {
	case ch <- data:
		return true
	default:
		if metrics != nil {
			metrics.IncOverflows()
		}
		return false
	}
}

// GetChannelUtilization returns channel utilization as percentage (0-100)
func GetChannelUtilization(used, capacity int) float64 {
	if capacity <= 0 {
		return 0.0
	}
	return float64(used) / float64(capacity) * 100.0
} 