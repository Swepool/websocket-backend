package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"websocket-backend/config"
	"websocket-backend/internal/pipeline"
	"websocket-backend/internal/transport"
	"websocket-backend/internal/utils"
)

func main() {
	fmt.Println("DEBUG: main() function started")
	utils.LogInfo("MAIN", "🚀 Starting Clean Modular WebSocket Backend with ClickHouse")
	
	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Load application configuration
	utils.LogInfo("MAIN", "📋 Loading configuration...")
	appConfig := config.LoadConfig()
	fmt.Printf("DEBUG: appConfig loaded: %+v\n", appConfig)
	utils.LogInfo("MAIN", "⚙️  Configuration loaded successfully")
	
	// Initialize performance optimizations
	utils.InitializeOptimizations()
	utils.LogInfo("MAIN", "⚡ Performance optimizations initialized")
	
	// Print optimization status
	utils.PrintOptimizationStatus()
	
	fmt.Println("DEBUG: After PrintOptimizationStatus()")
	
	fmt.Println("DEBUG: About to log coordinator creation...")
	utils.LogInfo("MAIN", "🔍 Starting coordinator creation...")
	
	fmt.Println("DEBUG: About to print config...")
	// Print clean architecture configuration
	utils.LogInfo("MAIN", "🏗️  Clean Modular Architecture Configuration:")
	
	fmt.Println("DEBUG: About to access fetcher config...")
	utils.LogInfo("MAIN", "  📥 Fetcher: poll=%v, batch=%d, mock=%t", 
		appConfig.Fetcher.PollInterval,
		appConfig.Fetcher.BatchSize,
		appConfig.Fetcher.MockMode)
	
	fmt.Println("DEBUG: About to access processor config...")
	utils.LogInfo("MAIN", "  ⚙️  Processor: single worker mode")
	
	fmt.Println("DEBUG: About to access batcher config...")
	utils.LogInfo("MAIN", "  📦 Batcher: flush=%v, size=%d", 
		appConfig.Batcher.FlushInterval,
		appConfig.Batcher.BatchSize)
	
	fmt.Println("DEBUG: About to access broadcaster config...")
	utils.LogInfo("MAIN", "  📡 Broadcaster: shards=%d, workers=%d", 
		appConfig.Broadcaster.NumShards,
		appConfig.Broadcaster.WorkersPerShard)
	
	fmt.Println("DEBUG: About to access database config...")
	fmt.Printf("DEBUG: Database config - Host: %s, Port: %d\n", appConfig.Database.Host, appConfig.Database.Port)
	utils.LogInfo("MAIN", "  Database: ClickHouse at %s:%d", 
		appConfig.Database.Host,
		appConfig.Database.Port)
	
	fmt.Println("DEBUG: About to create coordinator...")
	fmt.Println("DEBUG: About to create pipeline coordinator...")
	fmt.Println("DEBUG: Calling pipeline.NewCoordinator now...")
	
	// Add panic recovery to see if there's a panic in NewCoordinator
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("PANIC in NewCoordinator: %v\n", r)
			os.Exit(1)
		}
	}()
	
	coordinator, err := pipeline.NewCoordinator(appConfig)
	if err != nil {
		fmt.Printf("ERROR from NewCoordinator: %v\n", err)
		utils.LogError("MAIN", "❌ Failed to create clean coordinator: %v", err)
		os.Exit(1)
	}
	fmt.Println("DEBUG: Coordinator created successfully!")
	utils.LogInfo("MAIN", "✅ Pipeline coordinator created")
	
	srv := transport.NewServer(coordinator)
	
	var wg sync.WaitGroup
	
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
	
	utils.LogInfo("MAIN", "🎉 Clean Modular WebSocket Backend started successfully on %s", appConfig.Server.Port)
	utils.LogInfo("MAIN", "📊 ClickHouse analytics enabled")
	utils.LogInfo("MAIN", "🔄 Data Flow: Fetcher → Processor → {Broadcaster (with natural timing), Batcher → ClickHouse}")
	utils.LogInfo("MAIN", "Press Ctrl+C to stop...")
	
	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	<-sigChan
	utils.LogInfo("MAIN", "🛑 Shutdown signal received")
	
	// Cancel context to signal shutdown
	cancel()
	
	// Wait for all components to shut down
	done := make(chan struct{})
	go func() {
		wg.Wait()
		coordinator.Stop()
		coordinator.Wait()
		close(done)
	}()
	
	// Wait for shutdown with timeout
	select {
	case <-done:
		utils.LogInfo("MAIN", "✅ Graceful shutdown completed")
	case <-time.After(15 * time.Second):
		utils.LogWarn("MAIN", "⚠️  Shutdown timeout reached")
	}
	
	utils.LogInfo("MAIN", "👋 Clean Modular WebSocket Backend stopped")
}

 