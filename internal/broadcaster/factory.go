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

// CreateBroadcaster creates either a regular or sharded broadcaster based on config
func CreateBroadcaster(config Config, channels *channels.Channels, statsCollector *stats.Collector) BroadcasterInterface {
	if config.UseSharding {
		// Create sharded broadcaster
		shardedConfig := ShardedConfig{
			MaxClients:      config.MaxClients,
			BufferSize:      config.BufferSize,
			DropSlowClients: config.DropSlowClients,
			NumShards:       config.NumShards,
			WorkersPerShard: config.WorkersPerShard,
		}
		return NewShardedBroadcaster(shardedConfig, channels, statsCollector)
	}
	
	// Create regular broadcaster
	return NewBroadcaster(config, channels, statsCollector)
}

// GetRecommendedConfig returns optimized config based on expected load
func GetRecommendedConfig(expectedClients int) Config {
	config := DefaultConfig()
	
	if expectedClients > 5000 {
		// High load: use sharding
		config.UseSharding = true
		config.NumShards = calculateOptimalShards(expectedClients)
		config.WorkersPerShard = 8 // More workers for high load
		config.MaxClients = 2000   // Increase per-shard capacity
	} else if expectedClients > 1000 {
		// Medium load: use sharding with fewer shards
		config.UseSharding = true
		config.NumShards = 2
		config.WorkersPerShard = 4
		config.MaxClients = 1500
	}
	// Low load: keep default (no sharding)
	
	return config
}

// calculateOptimalShards calculates optimal number of shards based on client count
func calculateOptimalShards(expectedClients int) int {
	// Rule of thumb: ~1000-2000 clients per shard for optimal performance
	shards := expectedClients / 1500
	
	// Minimum 2 shards, maximum 16 shards
	if shards < 2 {
		return 2
	}
	if shards > 16 {
		return 16
	}
	
	return shards
} 