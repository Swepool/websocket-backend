package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/internal/fetcher"
	"websocket-backend-new/internal/enhancer"
	"websocket-backend-new/internal/scheduler"
	"websocket-backend-new/internal/broadcaster"
	"websocket-backend-new/internal/stats"
	"websocket-backend-new/internal/utils"
	"websocket-backend-new/models"
)

// Coordinator manages the entire pipeline
type Coordinator struct {
	fetcher     *fetcher.Fetcher
	enhancer    *enhancer.Enhancer
	scheduler   *scheduler.Scheduler
	broadcaster broadcaster.BroadcasterInterface
	statsCollector *stats.Collector
	
	channels    *channels.Channels
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewCoordinator creates a new pipeline coordinator
func NewCoordinator(config Config, chainProvider fetcher.ChainProvider) (*Coordinator, error) {
	// Initialize channels
	ch := channels.NewChannels()
	
	// Initialize stats collector
	statsConfig := stats.DefaultConfig()
	if config.Stats.RetentionHours > 0 {
		statsConfig.RetentionHours = config.Stats.RetentionHours
	}
	if config.Stats.TopItemsLimit > 0 {
		statsConfig.TopItemsLimit = config.Stats.TopItemsLimit
	}
	statsCollector := stats.NewCollector(statsConfig)
	
	// Initialize components
	f, err := fetcher.NewFetcher(config.Fetcher, ch, chainProvider)
	if err != nil {
		return nil, fmt.Errorf("failed to create fetcher: %w", err)
	}
	
	e := enhancer.NewEnhancer(config.Enhancer, ch)
	s := scheduler.NewScheduler(config.Scheduler, ch)
	
	// Create broadcaster with built-in optimizations
	b := broadcaster.CreateBroadcaster(config.Broadcaster, ch, statsCollector)
	
	return &Coordinator{
		fetcher:     f,
		enhancer:    e,
		scheduler:   s,
		broadcaster: b,
		statsCollector: statsCollector,
		channels:    ch,
	}, nil
}

// Start begins all pipeline threads
func (c *Coordinator) Start(ctx context.Context) error {
	utils.LogInfo("COORDINATOR", "Starting pipeline coordinator")
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	
	// Start all threads concurrently
	c.wg.Add(5)
	utils.LogInfo("COORDINATOR", "Starting 5 pipeline threads")
	
	// Thread 1: Fetcher
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "Fetcher panic recovered: %v", r)
			}
		}()
		c.fetcher.Start(ctx)
	}()
	
	// Thread 2: Enhancer  
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "Enhancer panic recovered: %v", r)
			}
		}()
		c.enhancer.Start(ctx)
	}()
	
	// Thread 3: Scheduler
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "Scheduler panic recovered: %v", r)
			}
		}()
		c.scheduler.Start(ctx)
	}()
	
	// Thread 4: Stats Collector
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "Stats panic recovered: %v", r)
			}
		}()
		c.runStatsCollector(ctx)
	}()
	
	// Thread 5: Broadcaster
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "Broadcaster panic recovered: %v", r)
			}
		}()
		c.broadcaster.Start(ctx)
	}()
	
	utils.LogInfo("COORDINATOR", "All pipeline threads started successfully")
	return nil
}

// runStatsCollector runs the stats collection thread
func (c *Coordinator) runStatsCollector(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
			
		case transfer := <-c.channels.StatsUpdates:
			// Process transfer for stats (non-blocking)
			c.processTransferForStats(transfer)
			
		case <-time.After(30 * time.Second):
			// Periodic health check
			c.logStatsHealth()
		}
	}
}

// processTransferForStats processes a single transfer for statistics
func (c *Coordinator) processTransferForStats(transfer models.Transfer) {
	defer func() {
		if r := recover(); r != nil {
			utils.LogError("COORDINATOR", "Error processing transfer %s: %v", transfer.PacketHash, r)
		}
	}()
	
	// Add to stats collector (should be fast)
	c.statsCollector.ProcessTransfer(transfer)
}

// logStatsHealth logs periodic stats health information
func (c *Coordinator) logStatsHealth() {
	defer func() {
		if r := recover(); r != nil {
			utils.LogError("COORDINATOR", "Error getting health info: %v", r)
		}
	}()
	
	// Health check runs silently
	_ = c.statsCollector.GetChartData()
}

// GetStatsCollector returns the stats collector for API endpoints
func (c *Coordinator) GetStatsCollector() *stats.Collector {
	return c.statsCollector
}

// GetBroadcaster returns the broadcaster for WebSocket client management
func (c *Coordinator) GetBroadcaster() broadcaster.BroadcasterInterface {
	return c.broadcaster
}

// Stop gracefully shuts down the coordinator
func (c *Coordinator) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	
	// Wait for all threads to complete
	c.wg.Wait()
} 