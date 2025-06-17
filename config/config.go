package config

import (
	"time"
	"websocket-backend-new/internal/pipeline"
	"websocket-backend-new/internal/chains"
	"websocket-backend-new/internal/fetcher"
	"websocket-backend-new/internal/enhancer"
	"websocket-backend-new/internal/scheduler"
	"websocket-backend-new/internal/stats"
	"websocket-backend-new/internal/broadcaster"
)

// Config holds all application configuration
type Config struct {
	Server      ServerConfig        `json:"server"`
	Chains      chains.Config       `json:"chains"`
	Pipeline    pipeline.Config     `json:"pipeline"`
	Fetcher     fetcher.Config      `json:"fetcher"`
	Enhancer    enhancer.Config     `json:"enhancer"`
	Scheduler   scheduler.Config    `json:"scheduler"`
	Stats       stats.Config        `json:"stats"`
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
		Enhancer:    enhancer.DefaultConfig(),
		Scheduler:   scheduler.DefaultConfig(),
		Stats:       stats.DefaultConfig(),
		Broadcaster: broadcaster.DefaultConfig(),
	}
}

// DefaultFetcherConfig returns optimized fetcher configuration for real transfers
func DefaultFetcherConfig() fetcher.Config {
	return fetcher.Config{
		PollInterval: 500 * time.Millisecond, // Fast polling for real-time transfers
		BatchSize:    100,                    // Larger batches for better throughput
		MockMode:     false,                  // Always use real data
		GraphQLURL:   "https://staging.graphql.union.build/v1/graphql",
	}
} 