package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"websocket-backend-new/config"
	"websocket-backend-new/internal/pipeline"
	"websocket-backend-new/internal/server"
	"websocket-backend-new/internal/chains"
	"websocket-backend-new/models"
)

func main() {
	fmt.Println("🚀 Testing Sharded Broadcaster Implementation")
	
	// Get expected client count from environment or use default
	expectedClients := 5000
	fmt.Printf("📊 Configuring for %d expected clients\n", expectedClients)
	
	// Create configuration with sharding (use LoadConfig to respect env vars)
	appConfig := config.ConfigWithSharding(expectedClients)
	
	fmt.Printf("🔧 Configuration:\n")
	fmt.Printf("   Sharding Enabled: %v\n", appConfig.Broadcaster.UseSharding)
	if appConfig.Broadcaster.UseSharding {
		fmt.Printf("   Number of Shards: %d\n", appConfig.Broadcaster.NumShards)
		fmt.Printf("   Workers per Shard: %d\n", appConfig.Broadcaster.WorkersPerShard)
		fmt.Printf("   Max Clients per Shard: %d\n", appConfig.Broadcaster.MaxClients)
		fmt.Printf("   Total Capacity: %d clients\n", 
			appConfig.Broadcaster.NumShards*appConfig.Broadcaster.MaxClients)
	}
	
	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	// Create chains service
	chainsService := chains.NewService(appConfig.Chains)
	
	// Create pipeline coordinator
	coordinator, err := pipeline.NewCoordinator(appConfig.Pipeline, chainsService)
	if err != nil {
		log.Fatalf("Failed to create coordinator: %v", err)
	}
	
	// Create server
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
	
	// Wait a moment for services to start
	time.Sleep(2 * time.Second)
	
	// Print status
	printStatus(coordinator, appConfig.Server.Port)
	
	// Start monitoring goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		monitorBroadcaster(ctx, coordinator)
	}()
	
	// Start demo transfer simulation
	wg.Add(1)
	go func() {
		defer wg.Done()
		simulateTransfers(ctx, coordinator)
	}()
	
	fmt.Printf("✅ Sharded broadcaster test started\n")
	fmt.Printf("🌐 Access endpoints:\n")
	fmt.Printf("   Health: http://localhost%s/health\n", appConfig.Server.Port)
	fmt.Printf("   Broadcaster Stats: http://localhost%s/api/broadcaster\n", appConfig.Server.Port)
	fmt.Printf("   WebSocket: ws://localhost%s/ws\n", appConfig.Server.Port)
	fmt.Printf("\n🔄 Press Ctrl+C to stop...\n")
	
	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	<-sigChan
	fmt.Printf("\n🛑 Shutdown signal received...\n")
	
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
		fmt.Printf("✅ Graceful shutdown completed\n")
	case <-time.After(30 * time.Second):
		fmt.Printf("⚠️ Shutdown timeout reached\n")
	}
}

func printStatus(coordinator *pipeline.Coordinator, port string) {
	broadcaster := coordinator.GetBroadcaster()
	
	fmt.Printf("\n📈 Broadcaster Status:\n")
	fmt.Printf("   Type: %s\n", getBroadcasterType(broadcaster))
	fmt.Printf("   Current Clients: %d\n", broadcaster.GetClientCount())
	
	// Test the API endpoint
	go func() {
		time.Sleep(1 * time.Second)
		testAPIEndpoint(port)
	}()
}

func getBroadcasterType(b interface{}) string {
	switch b.(type) {
	case *interface{}:
		return "Interface"
	default:
		return fmt.Sprintf("%T", b)
	}
}

func monitorBroadcaster(ctx context.Context, coordinator *pipeline.Coordinator) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			broadcaster := coordinator.GetBroadcaster()
			clientCount := broadcaster.GetClientCount()
			
			if clientCount > 0 {
				fmt.Printf("📊 [MONITOR] Active clients: %d\n", clientCount)
			}
		}
	}
}

func simulateTransfers(ctx context.Context, coordinator *pipeline.Coordinator) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	
	transferCount := 0
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Simulate a transfer broadcast (this would normally come from the pipeline)
			transferCount++
			fmt.Printf("🔄 [DEMO] Simulating transfer broadcast #%d\n", transferCount)
			
			// Create a mock transfer
			transfer := models.Transfer{
				PacketHash: fmt.Sprintf("demo_transfer_%d_%d", transferCount, time.Now().Unix()),
				SourceChain: models.Chain{
					DisplayName:       "Demo Source",
					UniversalChainID:  "demo-source",
					ChainID:           "demo-1",
				},
				DestinationChain: models.Chain{
					DisplayName:       "Demo Destination", 
					UniversalChainID:  "demo-dest",
					ChainID:           "demo-2",
				},
				SourceDisplayName:      "Demo Source",
				DestinationDisplayName: "Demo Destination",
				FormattedTimestamp:     time.Now().Format("2006-01-02 15:04:05"),
				RouteKey:               "demo-source_demo-dest",
				SenderDisplay:          "demo_sender",
				ReceiverDisplay:        "demo_receiver",
			}
			
			// This would normally be sent through channels, but for demo we'll just log
			_ = transfer
		}
	}
}

func testAPIEndpoint(port string) {
	url := fmt.Sprintf("http://localhost%s/api/broadcaster", port)
	
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("⚠️ Failed to test API endpoint: %v\n", err)
		return
	}
	defer resp.Body.Close()
	
	if resp.StatusCode == 200 {
		fmt.Printf("✅ API endpoint responding correctly\n")
	} else {
		fmt.Printf("⚠️ API endpoint returned status: %d\n", resp.StatusCode)
	}
} 