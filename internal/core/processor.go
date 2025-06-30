package core

import (
	"context"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"
	"websocket-backend/config"
	"websocket-backend/internal/utils"
	"websocket-backend/models"
)

// Processor receives Transfer objects, normalizes them, and fans out to Batcher and Broadcaster
type Processor struct {
	config   config.ProcessorConfig
	channels *Channels
	stats    ProcessorStats
}

// ProcessorStats tracks processor performance
type ProcessorStats struct {
	TotalProcessed       int64     `json:"totalProcessed"`
	BatchesProcessed     int64     `json:"batchesProcessed"`
	LastProcessTime      time.Time `json:"lastProcessTime"`
	ProcessErrors        int64     `json:"processErrors"`
	AverageLatency       float64   `json:"averageLatency"`
	IsRunning            bool      `json:"isRunning"`
	ProcessingRate       float64   `json:"processingRate"`
	// New counters for broadcast vs save tracking
	TotalSentToBatcher   int64     `json:"totalSentToBatcher"`      // Transfers sent to database
	TotalSentToBroadcast int64     `json:"totalSentToBroadcast"`    // Transfers sent to WebSocket
	BatcherDropped       int64     `json:"batcherDropped"`          // Transfers dropped from batcher
	BroadcastDropped     int64     `json:"broadcastDropped"`        // Transfers dropped from broadcaster
}

// NewProcessor creates a new clean processor component
func NewProcessor(cfg config.ProcessorConfig, channels *Channels) *Processor {
	return &Processor{
		config:   cfg,
		channels: channels,
		stats:    ProcessorStats{},
	}
}

// Start begins the processor component
func (p *Processor) Start(ctx context.Context) {
	utils.LogInfo("PROCESSOR", "Starting processor")
	p.stats.IsRunning = true
	
	// Single processing loop
	for {
		select {
		case <-ctx.Done():
			utils.LogInfo("PROCESSOR", "Stopping processor")
			p.stats.IsRunning = false
			return
			
		case transferBatch := <-p.channels.RawTransfers:
			p.processBatch(ctx, transferBatch)
		}
	}
}

// processBatch normalizes transfers and fans them out with natural timing
func (p *Processor) processBatch(ctx context.Context, transfers []models.Transfer) {
	startTime := time.Now()
	
	utils.LogInfo("PROCESSOR", "📥 Processing %d transfers", len(transfers))
	
	// Sort transfers by timestamp for natural ordering
	sortedTransfers := make([]models.Transfer, len(transfers))
	copy(sortedTransfers, transfers)
	sort.Slice(sortedTransfers, func(i, j int) bool {
		return sortedTransfers[i].TransferSendTimestamp.Before(sortedTransfers[j].TransferSendTimestamp)
	})
	
	// Send to batcher immediately (database storage)
	batcherSent := 0
	batcherDropped := 0
	for _, transfer := range sortedTransfers {
		normalizedTransfer := p.normalizeTransfer(transfer)
		
		select {
		case p.channels.BatchedTransfers <- normalizedTransfer:
			batcherSent++
			atomic.AddInt64(&p.stats.TotalSentToBatcher, 1)
		default:
			batcherDropped++
			atomic.AddInt64(&p.stats.BatcherDropped, 1)
			utils.LogError("PROCESSOR", "❌ Database channel full, dropped transfer")
			p.stats.ProcessErrors++
		}
	}
	
	if batcherSent > 0 {
		utils.LogInfo("PROCESSOR", "💾 Sent %d to database", batcherSent)
	}
	
	// Send to broadcaster with natural flow timing (immediate + staggered delivery)
	p.sendToBroadcaster(ctx, sortedTransfers)
	
	// Update statistics
	p.updateStats(len(sortedTransfers), time.Since(startTime), batcherSent, batcherDropped)
}

// normalizeTransfer ensures transfer data is properly formatted and enhanced
func (p *Processor) normalizeTransfer(transfer models.Transfer) models.Transfer {
	// Create a copy to avoid modifying the original
	normalized := transfer
	
	// Enhance with computed fields
	normalized.IsTestnetTransfer = transfer.SourceChain.Testnet || transfer.DestinationChain.Testnet
	normalized.FormattedTimestamp = transfer.TransferSendTimestamp.Format("2006-01-02 15:04:05")
	normalized.RouteKey = transfer.SourceChain.UniversalChainID + "_" + transfer.DestinationChain.UniversalChainID
	normalized.SourceDisplayName = transfer.SourceChain.DisplayName
	normalized.DestinationDisplayName = transfer.DestinationChain.DisplayName
	
	// Ensure display addresses are set
	if normalized.SenderDisplay == "" {
		normalized.SenderDisplay = transfer.SenderCanonical
	}
	if normalized.ReceiverDisplay == "" {
		normalized.ReceiverDisplay = transfer.ReceiverCanonical
	}
	
	// Ensure canonical token symbol is set
	if normalized.CanonicalTokenSymbol == "" {
		normalized.CanonicalTokenSymbol = transfer.BaseTokenSymbol
	}
	
	return normalized
}

// updateStats updates processor statistics
func (p *Processor) updateStats(transferCount int, latency time.Duration, batcherSent, batcherDropped int) {
	p.stats.TotalProcessed += int64(transferCount)
	p.stats.BatchesProcessed++
	p.stats.LastProcessTime = time.Now()
	
	// Update average latency using exponential moving average
	if p.stats.AverageLatency == 0 {
		p.stats.AverageLatency = latency.Seconds()
	} else {
		p.stats.AverageLatency = 0.9*p.stats.AverageLatency + 0.1*latency.Seconds()
	}
	
	// Calculate processing rate (transfers per second)
	if latency.Seconds() > 0 {
		currentRate := float64(transferCount) / latency.Seconds()
		if p.stats.ProcessingRate == 0 {
			p.stats.ProcessingRate = currentRate
		} else {
			p.stats.ProcessingRate = 0.9*p.stats.ProcessingRate + 0.1*currentRate
		}
	}
	
	// Log summary every batch to monitor flow distribution
	totalBatcher := atomic.LoadInt64(&p.stats.TotalSentToBatcher)
	totalBroadcast := atomic.LoadInt64(&p.stats.TotalSentToBroadcast)
	batcherDrops := atomic.LoadInt64(&p.stats.BatcherDropped)
	broadcastDrops := atomic.LoadInt64(&p.stats.BroadcastDropped)
	
	// Log comprehensive flow summary every batch for debugging
	utils.LogInfo("PROCESSOR", "🔥📊 FLOW SUMMARY: 💾 Database: %d sent, %d dropped | 📡 WebSocket: %d sent, %d dropped", 
		totalBatcher, batcherDrops, totalBroadcast, broadcastDrops)
}

// sendToBroadcaster sends transfers to broadcaster with natural flow timing
func (p *Processor) sendToBroadcaster(ctx context.Context, sortedTransfers []models.Transfer) {
	if len(sortedTransfers) == 0 {
		return
	}
	
	sent := 0
	dropped := 0
	
	// Check if natural flow is enabled
	if !p.config.NaturalFlow || len(sortedTransfers) <= p.config.MaxBurstSize {
		// Send all transfers immediately (original behavior)
		for _, transfer := range sortedTransfers {
			select {
			case p.channels.WebSocketBroadcasts <- transfer:
				sent++
				atomic.AddInt64(&p.stats.TotalSentToBroadcast, 1)
			case <-ctx.Done():
				return
			default:
				dropped++
				atomic.AddInt64(&p.stats.BroadcastDropped, 1)
				utils.LogError("PROCESSOR", "❌ WebSocket channel full, dropped transfer")
				p.stats.ProcessErrors++
			}
		}
		
		if sent > 0 {
			utils.LogInfo("PROCESSOR", "📡 Sent %d to WebSocket (immediate)", sent)
		}
		return
	}
	
	// Natural flow: Send first MaxBurstSize transfers immediately
	burstCount := min(p.config.MaxBurstSize, len(sortedTransfers))
	for i := 0; i < burstCount; i++ {
		select {
		case p.channels.WebSocketBroadcasts <- sortedTransfers[i]:
			sent++
			atomic.AddInt64(&p.stats.TotalSentToBroadcast, 1)
		case <-ctx.Done():
			return
		default:
			dropped++
			atomic.AddInt64(&p.stats.BroadcastDropped, 1)
			utils.LogError("PROCESSOR", "❌ WebSocket channel full, dropped transfer")
			p.stats.ProcessErrors++
		}
	}
	
	if sent > 0 {
		utils.LogInfo("PROCESSOR", "📡 Sent %d to WebSocket (immediate burst)", sent)
	}
	
	// Send remaining transfers with natural timing in background
	if len(sortedTransfers) > burstCount {
		remaining := sortedTransfers[burstCount:]
		go p.sendWithNaturalFlow(ctx, remaining)
		utils.LogInfo("PROCESSOR", "🌊 Queued %d transfers for natural flow", len(remaining))
	}
}

// sendWithNaturalFlow sends transfers with natural delays
func (p *Processor) sendWithNaturalFlow(ctx context.Context, transfers []models.Transfer) {
	for i, transfer := range transfers {
		// Calculate natural delay between transfers
		minDelay := p.config.FlowMinDelay
		maxDelay := p.config.FlowMaxDelay
		delayRange := maxDelay - minDelay
		
		var delay time.Duration
		if delayRange > 0 {
			delay = minDelay + time.Duration(rand.Int63n(int64(delayRange)))
		} else {
			delay = minDelay
		}
		
		// Wait for the natural delay
		select {
		case <-time.After(delay):
			// Send the transfer
			select {
			case p.channels.WebSocketBroadcasts <- transfer:
				atomic.AddInt64(&p.stats.TotalSentToBroadcast, 1)
				
				// Log progress for debugging (every 5th transfer)
				if (i+1)%5 == 0 {
					utils.LogDebug("PROCESSOR", "🌊 Natural flow: sent %d/%d transfers", i+1, len(transfers))
				}
			case <-ctx.Done():
				utils.LogInfo("PROCESSOR", "🌊 Natural flow stopped due to context cancellation")
				return
			default:
				atomic.AddInt64(&p.stats.BroadcastDropped, 1)
				utils.LogError("PROCESSOR", "❌ WebSocket channel full in natural flow, dropped transfer")
			}
		case <-ctx.Done():
			utils.LogInfo("PROCESSOR", "🌊 Natural flow stopped due to context cancellation")
			return
		}
	}
	
	utils.LogInfo("PROCESSOR", "🌊 Natural flow completed: %d transfers sent with delays", len(transfers))
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// GetStats returns current processor statistics
func (p *Processor) GetStats() ProcessorStats {
	// Create a copy of the stats with atomic reads for thread safety
	stats := p.stats
	stats.TotalSentToBatcher = atomic.LoadInt64(&p.stats.TotalSentToBatcher)
	stats.TotalSentToBroadcast = atomic.LoadInt64(&p.stats.TotalSentToBroadcast)
	stats.BatcherDropped = atomic.LoadInt64(&p.stats.BatcherDropped)
	stats.BroadcastDropped = atomic.LoadInt64(&p.stats.BroadcastDropped)
	return stats
} 