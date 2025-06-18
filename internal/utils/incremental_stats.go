package utils

import (
	"sync/atomic"
	"time"
	"websocket-backend-new/models"
	
	"github.com/axiomhq/hyperloglog"
)

// IncrementalStats maintains running statistics without full recomputation
type IncrementalStats struct {
	// Atomic counters for thread-safe updates
	transferCount   int64
	totalVolume     int64  // Store as int64 (scaled by 1000 for precision)
	errorCount      int64
	
	// HLL sketches for unique counting
	uniqueSenders   *hyperloglog.Sketch
	uniqueReceivers *hyperloglog.Sketch
	uniqueWallets   *hyperloglog.Sketch
	
	// Time tracking
	firstTransfer   int64 // Unix timestamp
	lastTransfer    int64 // Unix timestamp
	lastUpdate      int64 // Unix timestamp
	
	// Moving averages (scaled by 1000 for precision)
	avgTransferSize int64
	
	// Chain flow tracking (map access needs external synchronization)
	chainOutgoing map[string]*int64 // Chain ID -> count pointer
	chainIncoming map[string]*int64 // Chain ID -> count pointer
	
	// Asset tracking
	assetCounts  map[string]*int64 // Asset symbol -> count pointer
	assetVolumes map[string]*int64 // Asset symbol -> volume pointer (scaled)
}

// WindowedStats provides time-windowed statistics
type WindowedStats struct {
	stats      []*IncrementalStats
	windowSize time.Duration
	maxWindows int
	current    int
	startTime  time.Time
}

// NewIncrementalStats creates a new incremental stats tracker
func NewIncrementalStats() *IncrementalStats {
	return &IncrementalStats{
		uniqueSenders:   hyperloglog.New14(), // Use lower precision for performance
		uniqueReceivers: hyperloglog.New14(),
		uniqueWallets:   hyperloglog.New14(),
		chainOutgoing:   make(map[string]*int64),
		chainIncoming:   make(map[string]*int64),
		assetCounts:     make(map[string]*int64),
		assetVolumes:    make(map[string]*int64),
	}
}

// NewWindowedStats creates a new windowed stats tracker
func NewWindowedStats(windowSize time.Duration, maxWindows int) *WindowedStats {
	stats := make([]*IncrementalStats, maxWindows)
	for i := 0; i < maxWindows; i++ {
		stats[i] = NewIncrementalStats()
	}
	
	return &WindowedStats{
		stats:      stats,
		windowSize: windowSize,
		maxWindows: maxWindows,
		startTime:  time.Now(),
	}
}

// AddTransfer adds a transfer to the incremental stats
func (is *IncrementalStats) AddTransfer(transfer models.Transfer) {
	now := time.Now().Unix()
	
	// Update atomic counters
	atomic.AddInt64(&is.transferCount, 1)
	atomic.StoreInt64(&is.lastTransfer, now)
	atomic.StoreInt64(&is.lastUpdate, now)
	
	// Set first transfer time if not set
	if atomic.LoadInt64(&is.firstTransfer) == 0 {
		atomic.CompareAndSwapInt64(&is.firstTransfer, 0, now)
	}
	
	// Parse and add volume
	if volume := parseAmountScaled(transfer.BaseAmount); volume > 0 {
		atomic.AddInt64(&is.totalVolume, volume)
		
		// Update moving average (simple implementation)
		count := atomic.LoadInt64(&is.transferCount)
		if count > 0 {
			newAvg := atomic.LoadInt64(&is.totalVolume) / count
			atomic.StoreInt64(&is.avgTransferSize, newAvg)
		}
	}
	
	// Update HLL sketches for unique counting
	if transfer.SenderCanonical != "" {
		senderBytes := []byte(transfer.SenderCanonical)
		is.uniqueSenders.Insert(senderBytes)
		is.uniqueWallets.Insert(senderBytes)
	}
	
	if transfer.ReceiverCanonical != "" {
		receiverBytes := []byte(transfer.ReceiverCanonical)
		is.uniqueReceivers.Insert(receiverBytes)
		is.uniqueWallets.Insert(receiverBytes)
	}
}

// GetSnapshot returns a point-in-time snapshot of the stats
func (is *IncrementalStats) GetSnapshot() IncrementalStatsSnapshot {
	return IncrementalStatsSnapshot{
		TransferCount:    atomic.LoadInt64(&is.transferCount),
		TotalVolume:      float64(atomic.LoadInt64(&is.totalVolume)) / 1000.0,
		ErrorCount:       atomic.LoadInt64(&is.errorCount),
		UniqueSenders:    is.uniqueSenders.Estimate(),
		UniqueReceivers:  is.uniqueReceivers.Estimate(),
		UniqueWallets:    is.uniqueWallets.Estimate(),
		FirstTransfer:    time.Unix(atomic.LoadInt64(&is.firstTransfer), 0),
		LastTransfer:     time.Unix(atomic.LoadInt64(&is.lastTransfer), 0),
		LastUpdate:       time.Unix(atomic.LoadInt64(&is.lastUpdate), 0),
		AvgTransferSize:  float64(atomic.LoadInt64(&is.avgTransferSize)) / 1000.0,
	}
}

// IncrementalStatsSnapshot represents a point-in-time snapshot
type IncrementalStatsSnapshot struct {
	TransferCount   int64     `json:"transferCount"`
	TotalVolume     float64   `json:"totalVolume"`
	ErrorCount      int64     `json:"errorCount"`
	UniqueSenders   uint64    `json:"uniqueSenders"`
	UniqueReceivers uint64    `json:"uniqueReceivers"`
	UniqueWallets   uint64    `json:"uniqueWallets"`
	FirstTransfer   time.Time `json:"firstTransfer"`
	LastTransfer    time.Time `json:"lastTransfer"`
	LastUpdate      time.Time `json:"lastUpdate"`
	AvgTransferSize float64   `json:"avgTransferSize"`
}

// parseAmountScaled parses amount string and returns scaled integer (x1000 for precision)
func parseAmountScaled(amountStr string) int64 {
	if amountStr == "" {
		return 0
	}
	
	// Simple parsing - in production you'd use a proper decimal parser
	// This is a simplified version for demonstration
	amount := int64(0)
	
	// Basic string to int conversion with decimal support
	// This would be replaced with proper big.Float parsing in production
	for i, ch := range amountStr {
		if ch >= '0' && ch <= '9' {
			amount = amount*10 + int64(ch-'0')
		} else if ch == '.' {
			// Handle decimal places (simplified)
			remaining := amountStr[i+1:]
			if len(remaining) > 3 {
				remaining = remaining[:3] // Take only 3 decimal places
			}
			for _, decCh := range remaining {
				if decCh >= '0' && decCh <= '9' {
					amount = amount*10 + int64(decCh-'0')
				}
			}
			// Scale to maintain precision
			for len(remaining) < 3 {
				amount *= 10
				remaining += "0"
			}
			break
		}
	}
	
	return amount
}

// IncrementError increments the error counter
func (is *IncrementalStats) IncrementError() {
	atomic.AddInt64(&is.errorCount, 1)
} 