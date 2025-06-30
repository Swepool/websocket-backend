package transport

import (
	"context"
	"net/http"
	"websocket-backend/config"
	"websocket-backend/internal/core"
)

// BroadcasterInterface defines the common interface for all broadcaster implementations
type BroadcasterInterface interface {
	Start(ctx context.Context)
	UpgradeConnection(w http.ResponseWriter, r *http.Request)
	GetClientCount() int
	GetType() string
	GetShardStats() map[string]interface{}
	BroadcastChartData(data interface{})
	SetChartService(chartService interface{})
}



// CreateBroadcaster creates a sharded broadcaster with enhanced management
func CreateBroadcaster(cfg config.BroadcasterConfig, channels *core.Channels) BroadcasterInterface {
	// Always create sharded broadcaster
	return NewBroadcaster(cfg, channels)
}

// getDefaultBroadcasterConfig returns default broadcaster configuration
func getDefaultBroadcasterConfig() config.BroadcasterConfig {
	return config.BroadcasterConfig{
		MaxClients:      1000,
		BufferSize:      100,
		DropSlowClients: true,
		NumShards:       4,
		WorkersPerShard: 4,
	}
}

// GetRecommendedConfig returns optimized config based on expected load
func GetRecommendedConfig(expectedClients int) config.BroadcasterConfig {
	cfg := getDefaultBroadcasterConfig()
	
	// Enhanced configuration recommendations based on load testing and production experience
	if expectedClients > 15000 {
		// Very high load: aggressive sharding with larger buffers
		cfg.NumShards = calculateOptimalShards(expectedClients)
		cfg.WorkersPerShard = 8    // More workers for very high load
		cfg.MaxClients = 2500      // Higher capacity per shard
		cfg.BufferSize = 256       // Larger buffers to handle bursts
	} else if expectedClients > 8000 {
		// High load: optimized sharding
		cfg.NumShards = calculateOptimalShards(expectedClients)
		cfg.WorkersPerShard = 6    // Increased workers for better parallelism
		cfg.MaxClients = 2000      // Increased per-shard capacity
		cfg.BufferSize = 200       // Larger buffers for better throughput
	} else if expectedClients > 3000 {
		// Medium-high load: balanced sharding
		cfg.NumShards = calculateOptimalShards(expectedClients)
		cfg.WorkersPerShard = 4    // Standard workers
		cfg.MaxClients = 1500      // Moderate per-shard capacity
		cfg.BufferSize = 150       // Enhanced buffers
	} else if expectedClients > 1000 {
		// Medium load: light sharding for better performance
		cfg.NumShards = 2          // Minimal sharding
		cfg.WorkersPerShard = 4
		cfg.MaxClients = 1500
		cfg.BufferSize = 128       // Slightly larger buffers
	} else if expectedClients > 500 {
		// Low-medium load: enhanced standard sharding
		cfg.NumShards = 2          // Light sharding
		cfg.MaxClients = 1200      // Increased capacity
		cfg.BufferSize = 128       // Enhanced buffer size
	} else {
		// Low load: minimal sharding
		cfg.NumShards = 1          // Single shard for very low load
		cfg.MaxClients = 800       // Reasonable capacity
		cfg.BufferSize = 100       // Standard buffer size
	}
	
	cfg.DropSlowClients = true // Always drop slow clients in production
	
	return cfg
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
func GetOptimizedConfig(scenario string) config.BroadcasterConfig {
	switch scenario {
	case "development":
		return config.BroadcasterConfig{
			MaxClients:      200,
			BufferSize:      64,
			DropSlowClients: false, // More forgiving in dev
			NumShards:       1,     // Single shard for development
			WorkersPerShard: 2,
		}
		
	case "testing":
		return config.BroadcasterConfig{
			MaxClients:      1000,
			BufferSize:      100,
			DropSlowClients: true,
			NumShards:       2,
			WorkersPerShard: 4,
		}
		
	case "staging":
		return config.BroadcasterConfig{
			MaxClients:      1500,
			BufferSize:      150,
			DropSlowClients: true,
			NumShards:       4,
			WorkersPerShard: 6,
		}
		
	case "production":
		return config.BroadcasterConfig{
			MaxClients:      2000,
			BufferSize:      200,
			DropSlowClients: true,
			NumShards:       6,
			WorkersPerShard: 8,
		}
		
	case "high-load":
		return config.BroadcasterConfig{
			MaxClients:      2500,
			BufferSize:      256,
			DropSlowClients: true,
			NumShards:       10,
			WorkersPerShard: 8,
		}
		
	default:
		// Return enhanced default config
		cfg := getDefaultBroadcasterConfig()
		cfg.BufferSize = 128 // Enhanced default buffer
		cfg.NumShards = 4
		cfg.WorkersPerShard = 4
		return cfg
	}
}

// GetConfigForEnvironment returns environment-specific optimized configuration
func GetConfigForEnvironment(env string, expectedClients int) config.BroadcasterConfig {
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