package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/internal/fetcher"
	"websocket-backend-new/internal/processor"
	"websocket-backend-new/internal/scheduler"
	"websocket-backend-new/internal/broadcaster"
	"websocket-backend-new/internal/database"
	"websocket-backend-new/internal/utils"
)

// Coordinator manages the entire simplified pipeline with enhanced chart system and bidirectional sync
type Coordinator struct {
	syncManager     *fetcher.SyncManager  // Replaced fetcher with SyncManager
	processor       *processor.Processor
	scheduler       *scheduler.Scheduler
	broadcaster     broadcaster.BroadcasterInterface
	dbWriter        *database.Writer
	chartService    *database.EnhancedChartService
	chartUpdater    *database.ChartUpdater
	chartBroadcaster *database.ChartBroadcaster
	
	channels    *channels.Channels
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

// NewCoordinator creates a new pipeline coordinator with bidirectional sync and enhanced chart system
func NewCoordinator(config Config, chainProvider fetcher.ChainProvider) (*Coordinator, error) {
	// Initialize channels
	ch := channels.NewChannels()
	
	// Initialize database writer first (needed for sync manager)
	dbWriter, err := database.NewWriter(config.Database, ch)
	if err != nil {
		return nil, fmt.Errorf("failed to create database writer: %w", err)
	}
	
	// Initialize SyncManager instead of direct fetcher
	syncManager := fetcher.NewSyncManager(config.Fetcher, ch, chainProvider, dbWriter)
	
	// Configure bidirectional sync settings
	syncManager.SetBackwardSyncConfig(
		true,                 // Enable backward sync
		30,                   // Sync back 30 days for full UI coverage
		6*time.Hour,         // Start backward sync if gap > 6 hours
	)
	
	utils.LogInfo("COORDINATOR", "Initialized SyncManager with bidirectional sync (30 days depth, 6h gap threshold)")
	
	// Initialize other components
	p := processor.NewProcessor(config.Processor, ch)
	s := scheduler.NewScheduler(config.Scheduler, ch)
	
	// Create enhanced chart service using the same database connection
	chartService := database.NewEnhancedChartService(dbWriter.GetDB())
	
	// Load existing latency data from database on startup
	if err := chartService.LoadLatencyDataFromDB(); err != nil {
		utils.LogWarn("COORDINATOR", "Failed to load latency data from database: %v", err)
	}
	
	// Pre-warm chart data cache on startup for fast initial client connections
	utils.LogInfo("COORDINATOR", "Pre-warming chart data cache on startup...")
	if err := chartService.RefreshCache(); err != nil {
		utils.LogWarn("COORDINATOR", "Failed to pre-warm chart cache on startup: %v", err)
	} else {
		utils.LogInfo("COORDINATOR", "✅ Chart data cache pre-warmed for fast initial connections")
	}
	
	// Create chart updater for background processing
	chartUpdater := database.NewChartUpdater(dbWriter.GetDB())
	
	// Create broadcaster
	b := broadcaster.CreateBroadcaster(config.Broadcaster, ch)
	
	// Set chart service on broadcaster for initial chart data sending
	b.SetChartService(chartService)
	
	// Create chart broadcaster with enhanced chart service
	chartBroadcaster := database.NewChartBroadcaster(chartService, b)
	
	return &Coordinator{
		syncManager:     syncManager,   // Using SyncManager instead of fetcher
		processor:       p,
		scheduler:       s,
		broadcaster:     b,
		dbWriter:        dbWriter,
		chartService:    chartService,
		chartUpdater:    chartUpdater,
		chartBroadcaster: chartBroadcaster,
		channels:        ch,
	}, nil
}

// Start begins all pipeline threads with enhanced chart system and bidirectional sync
func (c *Coordinator) Start(ctx context.Context) error {
	utils.LogInfo("COORDINATOR", "Starting enhanced pipeline coordinator with bidirectional sync system")
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	
	// Start all threads concurrently
	c.wg.Add(6) // Now 6 threads: sync manager + 4 core + chart updater (broadcaster is event-driven)
	utils.LogInfo("COORDINATOR", "Starting 6 pipeline threads with bidirectional sync + event-driven chart broadcasting")
	
	// Thread 1: SyncManager (replaces Fetcher - handles both forward and backward sync)
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "SyncManager panic recovered: %v", r)
			}
		}()
		c.syncManager.Start(ctx)
	}()
	
	// Thread 2: Processor  
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "Processor panic recovered: %v", r)
			}
		}()
		c.processor.Start(ctx)
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
	
	// Thread 4: Database Writer
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "Database Writer panic recovered: %v", r)
			}
		}()
		c.dbWriter.Start(ctx)
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
	
	// Thread 6: Chart Updater (triggers event-driven chart broadcasts)
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "Chart Updater panic recovered: %v", r)
			}
		}()
		c.startChartUpdater(ctx)
	}()
	
	// Initialize chart broadcaster (event-driven, no separate thread needed)
	c.chartBroadcaster.Start()
	
	utils.LogInfo("COORDINATOR", "All pipeline threads started successfully with event-driven chart broadcasting")
	
	// Log initial sync status
	c.logSyncStatus()
	
	return nil
}

// logSyncStatus logs the current bidirectional sync status
func (c *Coordinator) logSyncStatus() {
	status := c.syncManager.GetStatus()
	utils.LogInfo("COORDINATOR", "Bidirectional Sync Status:")
	utils.LogInfo("COORDINATOR", "  Total transfers: %v", status["total_transfers"])
	utils.LogInfo("COORDINATOR", "  Forward sync: active")
	utils.LogInfo("COORDINATOR", "  Backward sync: %v", map[bool]string{true: "active", false: "inactive"}[status["backward_active"].(bool)])
	utils.LogInfo("COORDINATOR", "  Sync depth: %v days", status["sync_depth_days"])
	utils.LogInfo("COORDINATOR", "  Gap threshold: %.1f hours", status["gap_threshold_hours"])
}

// startChartUpdater runs the chart updater every 15 seconds
func (c *Coordinator) startChartUpdater(ctx context.Context) {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	
	utils.LogInfo("COORDINATOR", "Starting chart updater with 15-second intervals (event-driven broadcasts)")
	
	// Run initial update and warm cache
	if err := c.chartUpdater.UpdateAllChartSummaries(); err != nil {
		utils.LogError("COORDINATOR", "Initial chart update failed: %v", err)
	} else {
		// Proactively warm cache after successful update
		if err := c.chartService.RefreshCache(); err != nil {
			utils.LogError("COORDINATOR", "Failed to warm initial cache: %v", err)
		} else {
			utils.LogInfo("COORDINATOR", "🔥 Initial chart summaries updated and cache warmed")
			// Trigger broadcast since cache was updated
			c.chartBroadcaster.TriggerBroadcast()
		}
	}
	
	for {
		select {
		case <-ctx.Done():
			utils.LogInfo("COORDINATOR", "Chart updater stopping")
			return
		case <-ticker.C:
			if err := c.chartUpdater.UpdateAllChartSummaries(); err != nil {
				utils.LogError("COORDINATOR", "Chart update failed: %v", err)
			} else {
				// Proactively warm cache after successful update (no invalidation!)
				if err := c.chartService.RefreshCache(); err != nil {
					utils.LogError("COORDINATOR", "Failed to warm cache after update: %v", err)
				} else {
					utils.LogDebug("COORDINATOR", "🔥 Chart summaries updated and cache warmed")
					// Trigger broadcast only when cache is actually updated
					c.chartBroadcaster.TriggerBroadcast()
				}
			}
		}
	}
}

// GetChartService returns the enhanced chart service for API endpoints
func (c *Coordinator) GetChartService() *database.EnhancedChartService {
	return c.chartService
}

// GetBroadcaster returns the broadcaster for WebSocket client management
func (c *Coordinator) GetBroadcaster() broadcaster.BroadcasterInterface {
	return c.broadcaster
}

// GetScheduler returns the scheduler for stats endpoints
func (c *Coordinator) GetScheduler() *scheduler.Scheduler {
	return c.scheduler
}

// GetSyncManager returns the sync manager for stats endpoints
func (c *Coordinator) GetSyncManager() *fetcher.SyncManager {
	return c.syncManager
}

// GetProcessor returns the processor for stats endpoints
func (c *Coordinator) GetProcessor() *processor.Processor {
	return c.processor
}

// GetDatabaseWriter returns the database writer for stats endpoints
func (c *Coordinator) GetDatabaseWriter() *database.Writer {
	return c.dbWriter
}

// GetPipelineStats returns comprehensive pipeline statistics
func (c *Coordinator) GetPipelineStats() map[string]interface{} {
	// Get database stats
	dbStats, err := c.dbWriter.GetDatabaseStats()
	if err != nil {
		dbStats = map[string]interface{}{"error": err.Error()}
	}
	
	stats := map[string]interface{}{
		"timestamp": time.Now(),
		"components": map[string]interface{}{
			"syncManager": c.syncManager.GetStatus(),
			"processor":   c.processor.GetStats(),
			"scheduler":   c.scheduler.GetStats(),
			"broadcaster": c.broadcaster.GetShardStats(),
			"database":    dbStats,
		},
		"channels": map[string]interface{}{
			"rawTransfers":       len(c.channels.RawTransfers),
			"processedTransfers": len(c.channels.ProcessedTransfers), 
			"transferBroadcasts": len(c.channels.TransferBroadcasts),
			"databaseSaves":     len(c.channels.DatabaseSaves),
			"chartUpdates":      len(c.channels.ChartUpdates),
		},
		"channelCapacities": map[string]interface{}{
			"rawTransfers":       cap(c.channels.RawTransfers),
			"processedTransfers": cap(c.channels.ProcessedTransfers),
			"transferBroadcasts": cap(c.channels.TransferBroadcasts),
			"databaseSaves":     cap(c.channels.DatabaseSaves),
			"chartUpdates":      cap(c.channels.ChartUpdates),
		},
		"channelUtilization": map[string]interface{}{
			"rawTransfers":       float64(len(c.channels.RawTransfers)) / float64(cap(c.channels.RawTransfers)) * 100,
			"processedTransfers": float64(len(c.channels.ProcessedTransfers)) / float64(cap(c.channels.ProcessedTransfers)) * 100,
			"transferBroadcasts": float64(len(c.channels.TransferBroadcasts)) / float64(cap(c.channels.TransferBroadcasts)) * 100,
			"databaseSaves":     float64(len(c.channels.DatabaseSaves)) / float64(cap(c.channels.DatabaseSaves)) * 100,
			"chartUpdates":      float64(len(c.channels.ChartUpdates)) / float64(cap(c.channels.ChartUpdates)) * 100,
		},
		"architecture": map[string]interface{}{
			"mode":               "timestamp_based_scheduling",
			"bidirectionalSync":  true,
			"realisticTiming":    true,
			"microSpacing":       true,
			"chartUpdateInterval": "15s",
			"chartBroadcastMode": "event_driven",
			"dataFlow": []string{
				"SyncManager → [RawTransfers] → Processor → [ProcessedTransfers] → Scheduler → [TransferBroadcasts] → Broadcaster",
				"SyncManager → [DatabaseSaves] → Database Writer",
				"Database → Chart Updater (15s) → Chart Cache Refresh → Event-Driven Broadcast → WebSocket",
			},
		},
	}
	
	return stats
}

// Stop gracefully shuts down the coordinator
func (c *Coordinator) Stop() {
	if c.cancel != nil {
		utils.LogInfo("COORDINATOR", "Stopping enhanced pipeline coordinator with bidirectional sync")
		
		// Stop sync manager first
		c.syncManager.Stop()
		utils.LogInfo("COORDINATOR", "SyncManager stopped")
		
		// Stop chart broadcaster
		c.chartBroadcaster.Stop()
		utils.LogInfo("COORDINATOR", "Chart broadcaster stopped")
		
		// Cancel context to signal shutdown to all components
		c.cancel()
	}
}

// Wait waits for all threads to complete
func (c *Coordinator) Wait() {
	c.wg.Wait()
	utils.LogInfo("COORDINATOR", "All pipeline threads stopped")
} 