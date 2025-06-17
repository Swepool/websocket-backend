package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"websocket-backend-new/config"
	"websocket-backend-new/internal/pipeline"
	"websocket-backend-new/internal/server"
	"websocket-backend-new/internal/chains"
)

func main() {
	fmt.Printf("=== WebSocket Backend (New Architecture) ===\n")
	fmt.Printf("Starting with clean pipeline + HLL stats system\n\n")
	
	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Load application configuration
	appConfig := config.DefaultConfig()
	
	// Create chains service first
	chainsService := chains.NewService(appConfig.Chains)
	
	// Create pipeline with configuration and chains service
	coordinator, err := pipeline.NewCoordinator(appConfig.Pipeline, chainsService)
	if err != nil {
		fmt.Printf("Failed to create coordinator: %v\n", err)
		os.Exit(1)
	}
	
	// Create server with coordinator and chains service
	srv := server.NewServer(coordinator, chainsService)
	
	var wg sync.WaitGroup
	
	// Start pipeline
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := coordinator.Start(ctx); err != nil {
			fmt.Printf("Pipeline error: %v\n", err)
		}
	}()
	
	// Start HTTP server (which also starts chains service)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Start(ctx, appConfig.Server.Port); err != nil {
			fmt.Printf("Server error: %v\n", err)
		}
	}()
	
	// Log initial stats
	time.Sleep(2 * time.Second)
	fmt.Printf("\n[MAIN] System started successfully:\n")
	fmt.Printf("  - HTTP server: http://localhost%s\n", appConfig.Server.Port)
	fmt.Printf("  - WebSocket: ws://localhost%s/ws\n", appConfig.Server.Port)
	fmt.Printf("  - Chains API: http://localhost%s/api/chains\n", appConfig.Server.Port)
	fmt.Printf("  - Stats API: http://localhost%s/api/stats\n", appConfig.Server.Port)
	fmt.Printf("  - Health API: http://localhost%s/api/health\n", appConfig.Server.Port)
	fmt.Printf("  - Pipeline: 5 threads (fetch→enhance→schedule→stats→broadcast)\n")
	fmt.Printf("  - Stats: HyperLogLog buckets with automatic aggregation\n")
	fmt.Printf("  - Fetcher: %v polling, %d batch size, GraphQL: %s\n", appConfig.Fetcher.PollInterval, appConfig.Fetcher.BatchSize, appConfig.Fetcher.GraphQLURL)
	fmt.Printf("  - Chains: %v refresh from GraphQL\n\n", appConfig.Chains.RefreshInterval)
	
	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	<-sigChan
	fmt.Printf("\n[MAIN] Shutdown signal received\n")
	
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
		fmt.Printf("[MAIN] Graceful shutdown completed\n")
	case <-time.After(10 * time.Second):
		fmt.Printf("[MAIN] Shutdown timeout reached\n")
	}
} 