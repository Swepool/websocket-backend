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



// CreateBroadcaster creates a sharded broadcaster with enhanced management
func CreateBroadcaster(config Config, channels *channels.Channels, statsCollector *stats.Collector) BroadcasterInterface {
	// Always create sharded broadcaster
	return NewBroadcaster(config, channels, statsCollector)
}

// GetRecommendedConfig returns optimized config based on expected load
func GetRecommendedConfig(expectedClients int) Config {
	config := DefaultConfig()
	
	// Enhanced configuration recommendations based on load testing and production experience
	if expectedClients > 15000 {
		// Very high load: aggressive sharding with larger buffers
		config.NumShards = calculateOptimalShards(expectedClients)
		config.WorkersPerShard = 8    // More workers for very high load
		config.MaxClients = 2500      // Higher capacity per shard
		config.BufferSize = 256       // Larger buffers to handle bursts
	} else if expectedClients > 8000 {
		// High load: optimized sharding
		config.NumShards = calculateOptimalShards(expectedClients)
		config.WorkersPerShard = 6    // Increased workers for better parallelism
		config.MaxClients = 2000      // Increased per-shard capacity
		config.BufferSize = 200       // Larger buffers for better throughput
	} else if expectedClients > 3000 {
		// Medium-high load: balanced sharding
		config.NumShards = calculateOptimalShards(expectedClients)
		config.WorkersPerShard = 4    // Standard workers
		config.MaxClients = 1500      // Moderate per-shard capacity
		config.BufferSize = 150       // Enhanced buffers
	} else if expectedClients > 1000 {
		// Medium load: light sharding for better performance
		config.NumShards = 2          // Minimal sharding
		config.WorkersPerShard = 4
		config.MaxClients = 1500
		config.BufferSize = 128       // Slightly larger buffers
	} else if expectedClients > 500 {
		// Low-medium load: enhanced standard sharding
		config.NumShards = 2          // Light sharding
		config.MaxClients = 1200      // Increased capacity
		config.BufferSize = 128       // Enhanced buffer size
	} else {
		// Low load: minimal sharding
		config.NumShards = 1          // Single shard for very low load
		config.MaxClients = 800       // Reasonable capacity
		config.BufferSize = 100       // Standard buffer size
	}
	
	config.DropSlowClients = true // Always drop slow clients in production
	
	return config
}

// calculateOptimalShards calculates the optimal number of shards for given client count
func calculateOptimalShards(expectedClients int) int {
	// Target ~1500 clients per shard for optimal performance
	shards := (expectedClients + 1499) / 1500
	
	// Ensure minimum of 1 shard, maximum of 12 for practical limits
	if shards < 1 {
		shards = 1
	} else if shards > 12 {
		shards = 12
	}
	
	return shards
}

// GetOptimizedConfig returns environment-specific configurations
func GetOptimizedConfig(scenario string) Config {
	switch scenario {
	case "development":
		return Config{
			MaxClients:      200,
			BufferSize:      64,
			DropSlowClients: false, // More forgiving in dev
			NumShards:       1,     // Single shard for development
			WorkersPerShard: 2,
		}
		
	case "testing":
		return Config{
			MaxClients:      1000,
			BufferSize:      100,
			DropSlowClients: true,
			NumShards:       2,
			WorkersPerShard: 4,
		}
		
	case "staging":
		return Config{
			MaxClients:      1500,
			BufferSize:      150,
			DropSlowClients: true,
			NumShards:       4,
			WorkersPerShard: 6,
		}
		
	case "production":
		return Config{
			MaxClients:      2000,
			BufferSize:      200,
			DropSlowClients: true,
			NumShards:       6,
			WorkersPerShard: 8,
		}
		
	case "high-load":
		return Config{
			MaxClients:      2500,
			BufferSize:      256,
			DropSlowClients: true,
			NumShards:       10,
			WorkersPerShard: 8,
		}
		
	default:
		// Return enhanced default config
		config := DefaultConfig()
		config.BufferSize = 128 // Enhanced default buffer
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
		if baseConfig.NumShards > 2 {
			baseConfig.NumShards = 2 // Limit shards in development
		}
		
	case "testing", "test":
		// Balanced settings for testing
		baseConfig.DropSlowClients = true
		// Ensure minimum sharding
		if baseConfig.NumShards < 2 {
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