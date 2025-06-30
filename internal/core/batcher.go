package core

import (
	"context"
	"time"
	"websocket-backend/config"
	"websocket-backend/internal/utils"
	"websocket-backend/models"
)

// DatabaseWriter interface defines what the batcher needs from the database
type DatabaseWriter interface {
	InsertTransfers(ctx context.Context, transfers []models.Transfer) error
}

// Batcher accumulates transfers and flushes them to ClickHouse efficiently
type Batcher struct {
	config         config.BatcherConfig
	channels       *Channels
	dbWriter       DatabaseWriter
	buffer         []models.Transfer
	stats          BatcherStats
	flushTimer     *time.Timer
}

// BatcherStats tracks batcher performance
type BatcherStats struct {
	TotalBatched     int64     `json:"totalBatched"`
	FlushCount       int64     `json:"flushCount"`
	LastFlushTime    time.Time `json:"lastFlushTime"`
	FlushErrors      int64     `json:"flushErrors"`
	CurrentBuffer    int       `json:"currentBuffer"`
	AverageFlushTime float64   `json:"averageFlushTime"`
	IsRunning        bool      `json:"isRunning"`
	FlushRate        float64   `json:"flushRate"`
}

// NewBatcher creates a new batcher component
func NewBatcher(cfg config.BatcherConfig, channels *Channels, dbWriter DatabaseWriter) *Batcher {
	return &Batcher{
		config:     cfg,
		channels:   channels,
		dbWriter:   dbWriter,
		buffer:     make([]models.Transfer, 0, cfg.BatchSize),
		stats:      BatcherStats{},
		flushTimer: time.NewTimer(cfg.FlushInterval),
	}
}

// Start begins the batcher component
func (b *Batcher) Start(ctx context.Context) {
	utils.LogInfo("BATCHER", "Starting batcher (flush: %v or %d transfers)", 
		b.config.FlushInterval, b.config.BatchSize)
	b.stats.IsRunning = true
	
	// Reset timer
	b.flushTimer.Reset(b.config.FlushInterval)
	
	for {
		select {
		case <-ctx.Done():
			utils.LogInfo("BATCHER", "Stopping batcher")
			b.stats.IsRunning = false
			// Flush remaining transfers before stopping
			if len(b.buffer) > 0 {
				b.flushBuffer(ctx)
			}
			return
			
		case transfer := <-b.channels.BatchedTransfers:
			b.addToBuffer(ctx, transfer)
			
		case historicalBatch := <-b.channels.DatabaseSaves:
			// Handle historical transfers from BackwardFetcher
			b.flushHistoricalBatch(ctx, historicalBatch)
			
		case <-b.flushTimer.C:
			// Time-based flush
			if len(b.buffer) > 0 {
				utils.LogDebug("BATCHER", "Timer-triggered flush (%d transfers)", len(b.buffer))
				b.flushBuffer(ctx)
			}
			b.flushTimer.Reset(b.config.FlushInterval)
		}
	}
}

// addToBuffer adds a transfer to the buffer and flushes if needed
func (b *Batcher) addToBuffer(ctx context.Context, transfer models.Transfer) {
	b.buffer = append(b.buffer, transfer)
	b.stats.CurrentBuffer = len(b.buffer)
	
	// Log buffer status periodically  
	if len(b.buffer)%20 == 0 {
		utils.LogInfo("BATCHER", "📦 Buffer: %d transfers", len(b.buffer))
	}
	
	// Size-based flush
	if len(b.buffer) >= b.config.BatchSize {
		utils.LogDebug("BATCHER", "Size-triggered flush (%d transfers)", len(b.buffer))
		b.flushBuffer(ctx)
		b.flushTimer.Reset(b.config.FlushInterval) // Reset timer after manual flush
	}
}

// flushBuffer flushes the current buffer to ClickHouse
func (b *Batcher) flushBuffer(ctx context.Context) {
	if len(b.buffer) == 0 {
		return
	}
	
	startTime := time.Now()
	transferCount := len(b.buffer)
	
	// Attempt to flush with retries
	var err error
	for attempt := 0; attempt <= b.config.MaxRetries; attempt++ {
		err = b.dbWriter.InsertTransfers(ctx, b.buffer)
		if err == nil {
			// Success
			break
		}
		
		if attempt < b.config.MaxRetries {
			retryDelay := time.Duration(attempt+1) * time.Second
			utils.LogWarn("BATCHER", "Flush attempt %d failed, retrying in %v: %v", 
				attempt+1, retryDelay, err)
			
			select {
			case <-time.After(retryDelay):
				continue
			case <-ctx.Done():
				return
			}
		}
	}
	
	if err != nil {
		b.stats.FlushErrors++
		utils.LogError("BATCHER", "Failed to flush %d transfers after %d attempts: %v", 
			transferCount, b.config.MaxRetries+1, err)
		// TODO: Could implement dead letter queue here
	} else {
		// Success - update stats
		b.stats.TotalBatched += int64(transferCount)
		b.stats.FlushCount++
		b.stats.LastFlushTime = time.Now()
		
		flushDuration := time.Since(startTime)
		
		// Update average flush time
		if b.stats.AverageFlushTime == 0 {
			b.stats.AverageFlushTime = flushDuration.Seconds()
		} else {
			b.stats.AverageFlushTime = 0.9*b.stats.AverageFlushTime + 0.1*flushDuration.Seconds()
		}
		
		// Update flush rate (transfers per second)
		if flushDuration.Seconds() > 0 {
			currentRate := float64(transferCount) / flushDuration.Seconds()
			if b.stats.FlushRate == 0 {
				b.stats.FlushRate = currentRate
			} else {
				b.stats.FlushRate = 0.9*b.stats.FlushRate + 0.1*currentRate
			}
		}
		
		utils.LogInfo("BATCHER", "🔴 Saved %d real-time transfers to ClickHouse", transferCount)
	}
	
	// Clear buffer
	b.buffer = b.buffer[:0]
	b.stats.CurrentBuffer = 0
}

// GetStats returns current batcher statistics
func (b *Batcher) GetStats() BatcherStats {
	// Update current buffer size in stats
	b.stats.CurrentBuffer = len(b.buffer)
	return b.stats
}

// flushHistoricalBatch flushes historical transfers immediately (no buffering)
func (b *Batcher) flushHistoricalBatch(ctx context.Context, historicalTransfers []models.Transfer) {
	if len(historicalTransfers) == 0 {
		return
	}
	
	startTime := time.Now()
	transferCount := len(historicalTransfers)
	
	// Flush historical transfers immediately with retries
	var err error
	for attempt := 0; attempt <= b.config.MaxRetries; attempt++ {
		err = b.dbWriter.InsertTransfers(ctx, historicalTransfers)
		if err == nil {
			// Success
			break
		}
		
		if attempt < b.config.MaxRetries {
			retryDelay := time.Duration(attempt+1) * time.Second
			utils.LogWarn("BATCHER", "Historical flush attempt %d failed, retrying in %v: %v", 
				attempt+1, retryDelay, err)
			
			select {
			case <-time.After(retryDelay):
				continue
			case <-ctx.Done():
				return
			}
		}
	}
	
	if err != nil {
		b.stats.FlushErrors++
		utils.LogError("BATCHER", "Failed to flush %d historical transfers after %d attempts: %v", 
			transferCount, b.config.MaxRetries+1, err)
	} else {
		// Success - update stats
		b.stats.TotalBatched += int64(transferCount)
		b.stats.FlushCount++
		b.stats.LastFlushTime = time.Now()
		
		flushDuration := time.Since(startTime)
		
		// Update average flush time (same logic as regular flushes)
		if b.stats.AverageFlushTime == 0 {
			b.stats.AverageFlushTime = flushDuration.Seconds()
		} else {
			b.stats.AverageFlushTime = 0.9*b.stats.AverageFlushTime + 0.1*flushDuration.Seconds()
		}
		
		// Update flush rate (transfers per second)
		if flushDuration.Seconds() > 0 {
			currentRate := float64(transferCount) / flushDuration.Seconds()
			if b.stats.FlushRate == 0 {
				b.stats.FlushRate = currentRate
			} else {
				b.stats.FlushRate = 0.9*b.stats.FlushRate + 0.1*currentRate
			}
		}
		
		utils.LogInfo("BATCHER", "🔵 Saved %d historical transfers to ClickHouse (%.2fs)", transferCount, flushDuration.Seconds())
	}
}

// ForceFlush forces an immediate flush (useful for testing or shutdown)
func (b *Batcher) ForceFlush(ctx context.Context) error {
	if len(b.buffer) == 0 {
		return nil
	}
	
	utils.LogInfo("BATCHER", "Force flushing %d transfers", len(b.buffer))
	b.flushBuffer(ctx)
	return nil
} 