package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
	"websocket-backend-new/internal/pipeline"
	"websocket-backend-new/internal/chains"
	"websocket-backend-new/internal/fetcher"
	"websocket-backend-new/internal/processor"
	"websocket-backend-new/internal/scheduler"
	"websocket-backend-new/internal/database"
	"websocket-backend-new/internal/broadcaster"
)

// Config holds all application configuration
type Config struct {
	Server      ServerConfig        `json:"server"`
	Chains      chains.Config       `json:"chains"`
	Pipeline    pipeline.Config     `json:"pipeline"`
	Fetcher     fetcher.Config      `json:"fetcher"`
	Processor   processor.Config    `json:"processor"`
	Scheduler   scheduler.Config    `json:"scheduler"`
	Database    database.Config     `json:"database"`
	Broadcaster broadcaster.Config  `json:"broadcaster"`
}

// ServerConfig holds server-specific configuration
type ServerConfig struct {
	Port    string        `json:"port"`
	Timeout time.Duration `json:"timeout"`
}

// DefaultConfig returns default configuration for the entire application
func DefaultConfig() Config {
	fetcherConfig := DefaultFetcherConfig()
	pipelineConfig := pipeline.DefaultConfig()
	pipelineConfig.Fetcher = fetcherConfig
	
	return Config{
		Server: ServerConfig{
			Port:    ":8080",
			Timeout: 30 * time.Second,
		},
		Chains:      chains.DefaultConfig(),
		Pipeline:    pipelineConfig,
		Fetcher:     fetcherConfig,
		Processor:   processor.DefaultConfig(),
		Scheduler:   scheduler.DefaultConfig(),
		Database:    database.DefaultConfig(),
		Broadcaster: getDefaultBroadcasterConfig(),
	}
}

// DefaultFetcherConfig returns optimized fetcher configuration for real transfers
func DefaultFetcherConfig() fetcher.Config {
	return fetcher.Config{
		PollInterval: 500 * time.Millisecond, // Optimal 500ms polling for real-time transfers
		BatchSize:    100,                    // Larger batches for better throughput
		MockMode:     false,                  // Use real data
		GraphQLURL:   "https://staging.graphql.union.build/v1/graphql",
	}
}

// ConfigWithSharding returns configuration optimized for sharded broadcasting
func ConfigWithSharding(expectedClients int) Config {
	config := DefaultConfig()
	
	// Use recommended sharding configuration
	config.Broadcaster = broadcaster.GetRecommendedConfig(expectedClients)
	
	return config
}

// HighPerformanceConfig returns configuration for high-load scenarios
func HighPerformanceConfig() Config {
	config := DefaultConfig()
	
	// High-performance broadcaster settings (always sharded)
	config.Broadcaster = broadcaster.Config{
		MaxClients:      2000,
		BufferSize:      256,
		DropSlowClients: true,
		NumShards:       8,
		WorkersPerShard: 8,
	}
	
	// Optimized fetcher settings
	config.Fetcher.PollInterval = 500 * time.Millisecond
	config.Fetcher.BatchSize = 200
	
	// Optimized scheduler settings for timestamp-based timing
	config.Pipeline.Scheduler.LiveOffset = 1 * time.Second     // Faster live offset for high performance
	config.Pipeline.Scheduler.MaxBacklog = 5 * time.Second     // Shorter backlog for responsiveness
	config.Pipeline.Scheduler.MinInterval = 25 * time.Millisecond // Tighter micro-spacing
	
	return config
}

// getDefaultBroadcasterConfig returns broadcaster config with optimal sharding
func getDefaultBroadcasterConfig() broadcaster.Config {
	return broadcaster.Config{
		NumShards:       10,
		WorkersPerShard: 6,
		MaxClients:      2000,
		BufferSize:      256,
		DropSlowClients: true,
	}
}

// DevelopmentConfig returns configuration optimized for development
func DevelopmentConfig() Config {
	config := DefaultConfig()
	
	// Use single shard for development (simpler debugging)
	config.Broadcaster.NumShards = 1
	config.Broadcaster.WorkersPerShard = 2
	
	// Keep optimal 500ms polling even in development
	config.Fetcher.PollInterval = 500 * time.Millisecond
	config.Fetcher.BatchSize = 50
	
	return config
}

// LoadConfig loads configuration based on environment variables and modes
func LoadConfig() Config {
	// Check for configuration mode
	configMode := "high-performance"
	
	switch configMode {
	case "development":
		fmt.Printf("Using development configuration\n")
		return DevelopmentConfig()
		
	case "high-performance":
		fmt.Printf("Using high-performance configuration\n")
		return HighPerformanceConfig()
		
	case "sharded":
		// Get expected clients from environment
		expectedClients := 5000 // default
		if clientsStr := os.Getenv("EXPECTED_CLIENTS"); clientsStr != "" {
			if parsed, err := strconv.Atoi(clientsStr); err == nil {
				expectedClients = parsed
			}
		}
		fmt.Printf("Using sharded configuration for %d expected clients\n", expectedClients)
		return ConfigWithSharding(expectedClients)
		
	default:
		// Use default configuration with environment variable overrides
		appConfig := DefaultConfig()
		
		// Optional: configure shard count
		if shardsStr := os.Getenv("NUM_SHARDS"); shardsStr != "" {
			if shards, err := strconv.Atoi(shardsStr); err == nil && shards > 0 {
				appConfig.Broadcaster.NumShards = shards
			}
		}
		
		// Optional: configure workers per shard
		if workersStr := os.Getenv("WORKERS_PER_SHARD"); workersStr != "" {
			if workers, err := strconv.Atoi(workersStr); err == nil && workers > 0 {
				appConfig.Broadcaster.WorkersPerShard = workers
			}
		}
		
		// Optional: configure max clients per shard
		if clientsStr := os.Getenv("MAX_CLIENTS_PER_SHARD"); clientsStr != "" {
			if clients, err := strconv.Atoi(clientsStr); err == nil && clients > 0 {
				appConfig.Broadcaster.MaxClients = clients
			}
		}
		
		fmt.Printf("Using default configuration with environment overrides\n")
		return appConfig
	}
} 