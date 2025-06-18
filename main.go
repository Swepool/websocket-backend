package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"websocket-backend-new/config"
	"websocket-backend-new/internal/pipeline"
	"websocket-backend-new/internal/server"
	"websocket-backend-new/internal/chains"
	"websocket-backend-new/internal/utils"
	"websocket-backend-new/models"
)

func main() {
	utils.LogInfo("MAIN", "Starting WebSocket Backend")
	
	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Load application configuration
	appConfig := config.LoadConfig()
	utils.LogInfo("MAIN", "Configuration loaded")
	
	// Initialize all performance optimizations
	utils.InitializeOptimizations()
	utils.LogInfo("MAIN", "Performance optimizations initialized")
	
	// Print optimization status
	utils.PrintOptimizationStatus()
	
	// Print broadcaster configuration
	utils.LogInfo("MAIN", "Broadcaster configuration:")
	utils.LogInfo("MAIN", "  Sharding enabled: %v", appConfig.Broadcaster.UseSharding)
	if appConfig.Broadcaster.UseSharding {
		utils.LogInfo("MAIN", "  Number of shards: %d", appConfig.Broadcaster.NumShards)
		utils.LogInfo("MAIN", "  Workers per shard: %d", appConfig.Broadcaster.WorkersPerShard)
		utils.LogInfo("MAIN", "  Max clients per shard: %d", appConfig.Broadcaster.MaxClients)
		utils.LogInfo("MAIN", "  Total capacity: %d clients", 
			appConfig.Broadcaster.NumShards*appConfig.Broadcaster.MaxClients)
	}
	
	// Create chains service first
	chainsService := chains.NewService(appConfig.Chains)
	
	// Create pipeline with configuration and chains service
	coordinator, err := pipeline.NewCoordinator(appConfig.Pipeline, chainsService)
	if err != nil {
		utils.LogError("MAIN", "Failed to create coordinator: %v", err)
		os.Exit(1)
	}
	utils.LogInfo("MAIN", "Pipeline coordinator created")
	
	// Set up latency callback to update stats collector
	chainsService.SetLatencyCallback(func(latencyData []models.LatencyData) {
		coordinator.GetStatsCollector().UpdateLatencyData(latencyData)
		utils.LogInfo("MAIN", "Updated latency data with %d chain pairs", len(latencyData))
	})
	
	// Create server with coordinator and chains service
	srv := server.NewServer(coordinator, chainsService)
	
	var wg sync.WaitGroup
	
	// Start pipeline
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := coordinator.Start(ctx); err != nil {
			utils.LogError("MAIN", "Pipeline error: %v", err)
		}
	}()
	
	// Start HTTP server
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Start(ctx, appConfig.Server.Port); err != nil {
			utils.LogError("MAIN", "Server error: %v", err)
		}
	}()
	
	utils.LogInfo("MAIN", "WebSocket Backend started successfully on %s", appConfig.Server.Port)
	utils.LogInfo("MAIN", "Press Ctrl+C to stop...")
	
	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	<-sigChan
	utils.LogInfo("MAIN", "Shutdown signal received")
	
	// Cancel context to signal shutdown
	cancel()
	
	// Wait for all components to shut down
	done := make(chan struct{})
	go func() {
		wg.Wait()
		coordinator.Stop()
		close(done)
	}()
	
	// Wait for shutdown with timeout
	select {
	case <-done:
		utils.LogInfo("MAIN", "Graceful shutdown completed")
	case <-time.After(10 * time.Second):
		utils.LogWarn("MAIN", "Shutdown timeout reached")
	}
}

 