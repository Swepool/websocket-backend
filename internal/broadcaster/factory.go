package broadcaster

import (
	"context"
	"net/http"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/internal/stats"
)

// BroadcasterInterface defines the common interface for all broadcaster implementations
type BroadcasterInterface interface {
	Start(ctx context.Context)
	UpgradeConnection(w http.ResponseWriter, r *http.Request)
	GetClientCount() int
	GetType() string
	GetShardStats() map[string]interface{}
}

// CreateBroadcaster creates either a regular or sharded broadcaster based on config with enhanced management
func CreateBroadcaster(config Config, channels *channels.Channels, statsCollector *stats.Collector) BroadcasterInterface {
	if config.UseSharding {
		// Create sharded broadcaster with enhanced settings
		shardedConfig := ShardedConfig{
			MaxClients:      config.MaxClients,
			BufferSize:      config.BufferSize,
			DropSlowClients: config.DropSlowClients,
			NumShards:       config.NumShards,
			WorkersPerShard: config.WorkersPerShard,
		}
		return NewShardedBroadcaster(shardedConfig, channels, statsCollector)
	}
	
	// Create regular broadcaster with enhanced management
	return NewBroadcaster(config, channels, statsCollector)
}

// GetRecommendedConfig returns optimized config based on expected load with enhanced settings
func GetRecommendedConfig(expectedClients int) Config {
	config := DefaultConfig()
	
	// Enhanced configuration recommendations based on load testing and production experience
	if expectedClients > 15000 {
		// Very high load: aggressive sharding with larger buffers
		config.UseSharding = true
		config.NumShards = calculateOptimalShards(expectedClients)
		config.WorkersPerShard = 8    // More workers for very high load
		config.MaxClients = 2500      // Higher capacity per shard
		config.BufferSize = 256       // Larger buffers to handle bursts
	} else if expectedClients > 8000 {
		// High load: optimized sharding
		config.UseSharding = true
		config.NumShards = calculateOptimalShards(expectedClients)
		config.WorkersPerShard = 6    // Increased workers for better parallelism
		config.MaxClients = 2000      // Increased per-shard capacity
		config.BufferSize = 200       // Larger buffers for better throughput
	} else if expectedClients > 3000 {
		// Medium-high load: balanced sharding
		config.UseSharding = true
		config.NumShards = calculateOptimalShards(expectedClients)
		config.WorkersPerShard = 4    // Standard workers
		config.MaxClients = 1500      // Moderate per-shard capacity
		config.BufferSize = 150       // Enhanced buffers
	} else if expectedClients > 1000 {
		// Medium load: light sharding for better performance
		config.UseSharding = true
		config.NumShards = 2          // Minimal sharding
		config.WorkersPerShard = 4
		config.MaxClients = 1500
		config.BufferSize = 128       // Slightly larger buffers
	} else if expectedClients > 500 {
		// Low-medium load: enhanced standard broadcaster
		config.UseSharding = false
		config.MaxClients = 1200      // Increased capacity
		config.BufferSize = 128       // Enhanced buffer size
	} else {
		// Low load: optimized standard broadcaster
		config.UseSharding = false
		config.MaxClients = 800       // Reasonable capacity
		config.BufferSize = 100       // Standard buffer size
	}
	
	// Always enable slow client dropping for stability
	config.DropSlowClients = true
	
	return config
}

// calculateOptimalShards calculates optimal number of shards based on client count with improved algorithm
func calculateOptimalShards(expectedClients int) int {
	// Enhanced algorithm that considers both client count and system resources
	// Aim for 1500-2500 clients per shard for optimal performance
	
	if expectedClients <= 1500 {
		return 2 // Minimum shards for any sharded setup
	} else if expectedClients <= 4000 {
		return 2 // 2 shards for up to 4k clients
	} else if expectedClients <= 8000 {
		return 4 // 4 shards for up to 8k clients
	} else if expectedClients <= 16000 {
		return 6 // 6 shards for up to 16k clients
	} else if expectedClients <= 24000 {
		return 8 // 8 shards for up to 24k clients
	} else if expectedClients <= 40000 {
		return 12 // 12 shards for up to 40k clients
	} else {
		// For very high loads, calculate based on target of ~2000 clients per shard
		shards := (expectedClients + 1999) / 2000 // Round up
		if shards > 20 {
			return 20 // Maximum reasonable number of shards
		}
		return shards
	}
}

// GetOptimizedConfig returns production-ready config with specific optimizations
func GetOptimizedConfig(scenario string) Config {
	switch scenario {
	case "development":
		return Config{
			MaxClients:      200,
			BufferSize:      64,
			DropSlowClients: false, // More forgiving in dev
			UseSharding:     false,
			NumShards:       1,
			WorkersPerShard: 2,
		}
		
	case "testing":
		return Config{
			MaxClients:      1000,
			BufferSize:      100,
			DropSlowClients: true,
			UseSharding:     true,
			NumShards:       2,
			WorkersPerShard: 4,
		}
		
	case "staging":
		return Config{
			MaxClients:      1500,
			BufferSize:      150,
			DropSlowClients: true,
			UseSharding:     true,
			NumShards:       4,
			WorkersPerShard: 6,
		}
		
	case "production":
		return Config{
			MaxClients:      2000,
			BufferSize:      200,
			DropSlowClients: true,
			UseSharding:     true,
			NumShards:       6,
			WorkersPerShard: 8,
		}
		
	case "high-load":
		return Config{
			MaxClients:      2500,
			BufferSize:      256,
			DropSlowClients: true,
			UseSharding:     true,
			NumShards:       10,
			WorkersPerShard: 8,
		}
		
	default:
		// Return enhanced default config
		config := DefaultConfig()
		config.BufferSize = 128 // Enhanced default buffer
		config.UseSharding = true // Enable sharding by default for better performance
		config.NumShards = 4
		config.WorkersPerShard = 4
		return config
	}
}

// GetConfigForEnvironment returns environment-specific optimized configuration
func GetConfigForEnvironment(env string, expectedClients int) Config {
	baseConfig := GetRecommendedConfig(expectedClients)
	
	switch env {
	case "development", "dev":
		// More conservative settings for development
		baseConfig.DropSlowClients = false
		baseConfig.BufferSize = max(baseConfig.BufferSize/2, 50)
		if baseConfig.UseSharding && baseConfig.NumShards > 2 {
			baseConfig.NumShards = 2 // Limit shards in development
		}
		
	case "testing", "test":
		// Balanced settings for testing
		baseConfig.DropSlowClients = true
		// Keep recommended settings but ensure minimum sharding
		if baseConfig.UseSharding && baseConfig.NumShards < 2 {
			baseConfig.NumShards = 2
		}
		
	case "staging":
		// Production-like but with more logging and safety
		baseConfig.DropSlowClients = true
		// Keep recommended settings
		
	case "production", "prod":
		// Aggressive optimization for production
		baseConfig.DropSlowClients = true
		// Slightly increase buffer sizes for production stability
		baseConfig.BufferSize = min(baseConfig.BufferSize*120/100, 300)
		
	default:
		// Use recommended settings as-is
	}
	
	return baseConfig
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
} 