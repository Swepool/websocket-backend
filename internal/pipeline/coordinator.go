package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"
	"websocket-backend/config"
	"websocket-backend/internal/core"
	"websocket-backend/internal/fetcher"
	"websocket-backend/internal/services"
	"websocket-backend/internal/services/nodehealth"
	"websocket-backend/internal/storage"
	"websocket-backend/internal/transport"
	"websocket-backend/internal/utils"
)

// Coordinator manages the clean modular pipeline with clear responsibilities
type Coordinator struct {
	// Core components
	syncManager *fetcher.SyncManager
	processor   *core.Processor
	batcher     *core.Batcher
	broadcaster transport.BroadcasterInterface
	
	// Supporting services
	clickhouseService *storage.ClickHouseService
	chartBroadcaster  *storage.ChartBroadcaster
	chainsService     *services.ChainsService
	latencyService    *services.LatencyService
	nodeHealthService *nodehealth.Service
	channels          *core.Channels
	
	// Control
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewCoordinator creates a new clean modular pipeline coordinator
func NewCoordinator(cfg config.Config) (*Coordinator, error) {
	// Initialize channels for clean communication
	ch := core.NewChannels()
	
	// Initialize ClickHouse service
	clickhouseService, err := storage.NewClickHouseService(storage.ClickHouseConfig{
		Host:     cfg.Database.Host,
		Port:     cfg.Database.Port,
		Database: cfg.Database.Database,
		Username: cfg.Database.Username,
		Password: cfg.Database.Password,
		Debug:    cfg.Database.Debug,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse service: %w", err)
	}
	
	// Initialize ClickHouse schema
	if err := clickhouseService.InitializeSchema(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to initialize ClickHouse schema: %w", err)
	}
	
	// Initialize components with clear responsibilities
	syncManager := fetcher.NewSyncManager(cfg.Fetcher, cfg.BackwardFetcher, ch, nil, clickhouseService)
	processorComponent := core.NewProcessor(cfg.Processor, ch)
	batcherComponent := core.NewBatcher(cfg.Batcher, ch, clickhouseService)
	broadcasterComponent := transport.NewBroadcaster(cfg.Broadcaster, ch)
	
	// Set up chart service for initial data on WebSocket connect
	broadcasterComponent.SetChartService(clickhouseService)
	
	// Initialize services
	chainsService := services.NewChainsService(services.DefaultChainsConfig())
	latencyService := services.NewLatencyService(services.DefaultLatencyConfig(), chainsService)
	nodeHealthService := nodehealth.NewService(nodehealth.DefaultNodeHealthConfig())
	
	// Set latency service for initial client data
	clickhouseService.SetLatencyService(latencyService)
	
	// Set node health service for initial client data
	clickhouseService.SetNodeHealthService(nodeHealthService)
	
	// Create chart broadcaster for periodic chart data updates
	chartBroadcaster := storage.NewChartBroadcaster(clickhouseService, broadcasterComponent, latencyService, nodeHealthService)
	
	utils.LogInfo("COORDINATOR", "Created clean modular pipeline:")
	utils.LogInfo("COORDINATOR", "  → SyncManager: coordinates forward/backward sync")
	utils.LogInfo("COORDINATOR", "  → Processor: normalizes and routes to WebSocket & database")
	utils.LogInfo("COORDINATOR", "  → Batcher: accumulates for ClickHouse")
	utils.LogInfo("COORDINATOR", "  → Broadcaster: WebSocket server")
	utils.LogInfo("COORDINATOR", "  → ChartBroadcaster: periodic chart data with asset volumes + latency")
	utils.LogInfo("COORDINATOR", "  → ChainsService: chain metadata monitoring")
	utils.LogInfo("COORDINATOR", "  → LatencyService: cross-chain latency monitoring")
	utils.LogInfo("COORDINATOR", "  → NodeHealthService: RPC node health monitoring")
	
	return &Coordinator{
		syncManager:       syncManager,
		processor:         processorComponent,
		batcher:           batcherComponent,
		broadcaster:       broadcasterComponent,
		clickhouseService: clickhouseService,
		chartBroadcaster:  chartBroadcaster,
		chainsService:     chainsService,
		latencyService:    latencyService,
		nodeHealthService: nodeHealthService,
		channels:          ch,
	}, nil
}

// Start begins all components in the clean modular pipeline
func (c *Coordinator) Start(ctx context.Context) error {
	utils.LogInfo("COORDINATOR", "Starting clean modular pipeline")
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	
	// Start all components concurrently + chart updates + services
	c.wg.Add(8) // 4 main components + chart updates + chains service + latency service + node health service
	
	// Component 1: SyncManager (coordinates forward + backward sync)
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "SyncManager panic recovered: %v", r)
			}
		}()
		c.syncManager.Start(ctx)
	}()
	
	// Component 2: Processor (normalizes and routes to WebSocket & database)
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "Processor panic recovered: %v", r)
			}
		}()
		c.processor.Start(ctx)
	}()
	
	// Component 3: Batcher (accumulates for ClickHouse)
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "Batcher panic recovered: %v", r)
			}
		}()
		c.batcher.Start(ctx)
	}()
	
	// Component 4: Broadcaster (WebSocket server)
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "Broadcaster panic recovered: %v", r)
			}
		}()
		c.broadcaster.Start(ctx)
	}()
	
	// Component 5: Chart Data Updates (periodic stats broadcasting)
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "Chart update panic recovered: %v", r)
			}
		}()
		c.startChartUpdates(ctx)
	}()
	
	// Component 6: Chains Service (chain metadata monitoring)
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "ChainsService panic recovered: %v", r)
			}
		}()
		c.chainsService.Start(ctx)
	}()
	
	// Component 7: Latency Service (cross-chain latency monitoring)
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "LatencyService panic recovered: %v", r)
			}
		}()
		c.latencyService.Start(ctx)
	}()
	
	// Component 8: NodeHealthService (RPC node health monitoring)
	go func() {
		defer c.wg.Done()
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("COORDINATOR", "NodeHealthService panic recovered: %v", r)
			}
		}()
		c.nodeHealthService.Start(ctx)
	}()
	
	utils.LogInfo("COORDINATOR", "All components started successfully")
	
	// Log initial status
	c.logPipelineStatus()
	
	return nil
}

// logPipelineStatus logs the current pipeline status and flow
func (c *Coordinator) logPipelineStatus() {
	utils.LogInfo("COORDINATOR", "Clean Modular Pipeline Status:")
	utils.LogInfo("COORDINATOR", "  Data Flow:")
	utils.LogInfo("COORDINATOR", "    🔴 Forward Fetcher → [RawTransfers] → Processor → {WebSocket + Database}")
	utils.LogInfo("COORDINATOR", "    🔵 Backward Fetcher → [DatabaseSaves] → Database (historical gaps)")
	utils.LogInfo("COORDINATOR", "    Processor → [WebSocketBroadcasts] → Broadcaster (real-time)")
	utils.LogInfo("COORDINATOR", "    Processor → [BatchedTransfers] → Batcher → ClickHouse (10s/2000tx)")
	utils.LogInfo("COORDINATOR", "  Components: All running")
	utils.LogInfo("COORDINATOR", "  Database: ClickHouse analytics")
}

// GetClickHouseService returns the ClickHouse service for analytics
func (c *Coordinator) GetClickHouseService() *storage.ClickHouseService {
	return c.clickhouseService
}

// GetBroadcaster returns the broadcaster for WebSocket client management
func (c *Coordinator) GetBroadcaster() transport.BroadcasterInterface {
	return c.broadcaster
}

// GetChannels returns the channels for monitoring
func (c *Coordinator) GetChannels() *core.Channels {
	return c.channels
}

// GetStats returns comprehensive pipeline statistics
func (c *Coordinator) GetStats() map[string]interface{} {
	stats := map[string]interface{}{
		"timestamp": time.Now(),
		"architecture": map[string]interface{}{
			"type":        "clean_modular_with_sync",
			"components":  4,
			"database":    "clickhouse",
			"data_flow": []string{
				"🔴 Forward Fetcher → Processor → {WebSocket + Database}",
				"🔵 Backward Fetcher → Database (historical gaps)",
			},
		},
		"components": map[string]interface{}{
			"syncManager": c.syncManager.GetStatus(),
			"processor":   c.processor.GetStats(),
			"batcher":     c.batcher.GetStats(),
			"broadcaster": c.broadcaster.GetShardStats(),
		},
		"channels": c.channels.GetChannelStats(),
	}
	
	return stats
}

// Stop gracefully shuts down all components
func (c *Coordinator) Stop() {
	if c.cancel != nil {
		utils.LogInfo("COORDINATOR", "Stopping clean modular pipeline")
		c.cancel()
	}
}

// Wait waits for all components to complete
func (c *Coordinator) Wait() {
	c.wg.Wait()
	utils.LogInfo("COORDINATOR", "All components stopped")
}

// Health checks the health of all components
func (c *Coordinator) Health(ctx context.Context) map[string]interface{} {
	health := map[string]interface{}{
		"timestamp": time.Now(),
		"overall":   "healthy",
	}
	
	// Check ClickHouse health
	if err := c.clickhouseService.Health(ctx); err != nil {
		health["clickhouse"] = "unhealthy: " + err.Error()
		health["overall"] = "degraded"
	} else {
		health["clickhouse"] = "healthy"
	}
	
	// Check component status
	syncStatus := c.syncManager.GetStatus()
	processorStats := c.processor.GetStats()
	batcherStats := c.batcher.GetStats()
	
	health["components"] = map[string]interface{}{
		"syncManager": fmt.Sprintf("forward: %v, backward: %v", syncStatus["forward_active"], syncStatus["backward_active"]),
		"processor": map[bool]string{true: "running", false: "stopped"}[processorStats.IsRunning],
		"batcher":   map[bool]string{true: "running", false: "stopped"}[batcherStats.IsRunning],
		"broadcaster": "running", // Broadcaster doesn't expose IsRunning yet
	}
	
	return health
}

// startChartUpdates handles periodic chart data broadcasts to WebSocket clients
func (c *Coordinator) startChartUpdates(ctx context.Context) {
	utils.LogInfo("COORDINATOR", "Starting chart data updates (1 minute intervals)")
	
	// Send initial chart data immediately
	c.broadcastChartUpdate(ctx)
	
	// Set up periodic updates every 1 minute
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			utils.LogInfo("COORDINATOR", "Chart updates stopping")
			return
		case <-ticker.C:
			c.broadcastChartUpdate(ctx)
		}
	}
}

// broadcastChartUpdate broadcasts chart data to all connected WebSocket clients
func (c *Coordinator) broadcastChartUpdate(ctx context.Context) {
	defer func() {
		if r := recover(); r != nil {
			utils.LogError("COORDINATOR", "Chart update broadcast panic recovered: %v", r)
		}
	}()
	
	utils.LogInfo("COORDINATOR", "Starting chart data broadcast via ChartBroadcaster...")
	
	// Use ChartBroadcaster for comprehensive chart data including asset volumes
	c.chartBroadcaster.BroadcastChartUpdate(ctx)
	
	utils.LogInfo("COORDINATOR", "Chart data broadcast completed via ChartBroadcaster")
} 