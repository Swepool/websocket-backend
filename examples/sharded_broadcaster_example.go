package main

import (
	"context"
	"fmt"
	"log"
	"time"
	"websocket-backend-new/internal/broadcaster"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/internal/stats"
)

func main() {
	// Example 1: Auto-configuration based on expected load
	fmt.Println("=== Auto-Configuration Example ===")
	
	expectedClients := 8000
	config := broadcaster.GetRecommendedConfig(expectedClients)
	
	fmt.Printf("Expected clients: %d\n", expectedClients)
	fmt.Printf("Recommended config:\n")
	fmt.Printf("  UseSharding: %v\n", config.UseSharding)
	fmt.Printf("  NumShards: %d\n", config.NumShards)
	fmt.Printf("  WorkersPerShard: %d\n", config.WorkersPerShard)
	fmt.Printf("  MaxClients: %d\n", config.MaxClients)
	fmt.Printf("  Total Capacity: %d\n", config.NumShards*config.MaxClients)
	
	// Example 2: Manual high-performance configuration
	fmt.Println("\n=== Manual High-Performance Configuration ===")
	
	highPerfConfig := broadcaster.Config{
		MaxClients:      2000, // 2K clients per shard
		BufferSize:      256,  // Larger buffers for high throughput
		DropSlowClients: true,
		UseSharding:     true,
		NumShards:       8,  // 8 shards
		WorkersPerShard: 6,  // 6 workers per shard
	}
	
	fmt.Printf("High-performance config:\n")
	fmt.Printf("  Total Capacity: %d clients\n", highPerfConfig.NumShards*highPerfConfig.MaxClients)
	fmt.Printf("  Total Workers: %d\n", highPerfConfig.NumShards*highPerfConfig.WorkersPerShard)
	fmt.Printf("  Memory Overhead: ~%dMB\n", highPerfConfig.NumShards*4)
	
	// Example 3: Practical integration
	fmt.Println("\n=== Practical Integration Example ===")
	demonstrateIntegration()
}

func demonstrateIntegration() {
	// Initialize dependencies
	channels := channels.NewChannels()
	statsCollector := stats.NewCollector(stats.DefaultConfig())
	
	// Get configuration for medium load (3000 clients)
	config := broadcaster.GetRecommendedConfig(3000)
	
	// Create broadcaster using factory
	b := broadcaster.CreateBroadcaster(config, channels, statsCollector)
	
	// Start broadcaster in background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	go b.Start(ctx)
	
	fmt.Printf("Started broadcaster with configuration:\n")
	fmt.Printf("  Type: %s\n", getBroadcasterType(b))
	fmt.Printf("  Sharding: %v\n", config.UseSharding)
	if config.UseSharding {
		fmt.Printf("  Shards: %d\n", config.NumShards)
		fmt.Printf("  Workers per shard: %d\n", config.WorkersPerShard)
	}
	
	// Simulate some activity
	time.Sleep(100 * time.Millisecond)
	
	fmt.Printf("  Current clients: %d\n", b.GetClientCount())
	
	// If it's a sharded broadcaster, show shard statistics
	if shardedBroadcaster, ok := b.(*broadcaster.ShardedBroadcaster); ok {
		stats := shardedBroadcaster.GetShardStats()
		fmt.Printf("  Shard statistics: %+v\n", stats)
	}
	
	fmt.Println("Broadcaster integration example completed successfully!")
}

func getBroadcasterType(b broadcaster.BroadcasterInterface) string {
	switch b.(type) {
	case *broadcaster.ShardedBroadcaster:
		return "ShardedBroadcaster"
	case *broadcaster.Broadcaster:
		return "StandardBroadcaster"
	default:
		return "Unknown"
	}
}

// Example configuration for different deployment scenarios
func getConfigExamples() {
	fmt.Println("\n=== Configuration Examples for Different Scenarios ===")
	
	scenarios := []struct {
		name            string
		expectedClients int
		description     string
	}{
		{"Development", 50, "Local development with few test clients"},
		{"Small Production", 500, "Small production deployment"},
		{"Medium Production", 2000, "Medium-scale production"},
		{"Large Production", 8000, "Large-scale production"},
		{"Enterprise", 20000, "Enterprise-scale deployment"},
	}
	
	for _, scenario := range scenarios {
		config := broadcaster.GetRecommendedConfig(scenario.expectedClients)
		
		fmt.Printf("\n%s (%d clients): %s\n", scenario.name, scenario.expectedClients, scenario.description)
		fmt.Printf("  Sharding: %v", config.UseSharding)
		if config.UseSharding {
			fmt.Printf(" | Shards: %d | Workers/Shard: %d", config.NumShards, config.WorkersPerShard)
			fmt.Printf(" | Total Capacity: %d", config.NumShards*config.MaxClients)
		}
		fmt.Println()
	}
}

// Load testing helper
func loadTestExample(clients int, duration time.Duration) {
	fmt.Printf("\n=== Load Test Example: %d clients for %v ===\n", clients, duration)
	
	config := broadcaster.GetRecommendedConfig(clients)
	channels := channels.NewChannels()
	statsCollector := stats.NewCollector(stats.DefaultConfig())
	
	b := broadcaster.CreateBroadcaster(config, channels, statsCollector)
	
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()
	
	go b.Start(ctx)
	
	startTime := time.Now()
	
	// Simulate client connections (in real scenario, these would be actual WebSocket connections)
	go func() {
		for i := 0; i < clients; i++ {
			// In real implementation, this would be actual WebSocket connections
			// For demo purposes, we just track time and report
			if i%1000 == 0 {
				elapsed := time.Since(startTime)
				fmt.Printf("Simulated %d clients in %v\n", i, elapsed)
			}
			time.Sleep(time.Millisecond) // Simulate connection establishment time
		}
	}()
	
	// Wait for test duration
	<-ctx.Done()
	
	fmt.Printf("Load test completed. Final client count: %d\n", b.GetClientCount())
	if shardedBroadcaster, ok := b.(*broadcaster.ShardedBroadcaster); ok {
		stats := shardedBroadcaster.GetShardStats()
		fmt.Printf("Final shard statistics: %+v\n", stats)
	}
}

func init() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
} 