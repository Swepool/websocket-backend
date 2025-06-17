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
	fmt.Printf("Starting WebSocket Backend...\n")
	
	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Load application configuration
	appConfig := config.DefaultConfig()
	fmt.Printf("Configuration loaded\n")
	
	// Create chains service first
	chainsService := chains.NewService(appConfig.Chains)
	
	// Create pipeline with configuration and chains service
	coordinator, err := pipeline.NewCoordinator(appConfig.Pipeline, chainsService)
	if err != nil {
		fmt.Printf("Failed to create coordinator: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("Pipeline coordinator created\n")
	
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
	
	// Start HTTP server
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Start(ctx, appConfig.Server.Port); err != nil {
			fmt.Printf("Server error: %v\n", err)
		}
	}()
	
	fmt.Printf("WebSocket Backend started successfully on %s\n", appConfig.Server.Port)
	fmt.Printf("Press Ctrl+C to stop...\n")
	
	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	<-sigChan
	fmt.Printf("Shutdown signal received...\n")
	
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
		fmt.Printf("Graceful shutdown completed\n")
	case <-time.After(10 * time.Second):
		fmt.Printf("Shutdown timeout reached\n")
	}
} 