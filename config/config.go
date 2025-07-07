package config

import (
	"os"
	"strconv"
	"time"
)

// Config holds all application configuration
type Config struct {
	Server          ServerConfig          `json:"server"`
	Fetcher         FetcherConfig         `json:"fetcher"`
	BackwardFetcher BackwardFetcherConfig `json:"backwardFetcher"`
	Processor       ProcessorConfig       `json:"processor"`
	Batcher         BatcherConfig         `json:"batcher"`
	Broadcaster     BroadcasterConfig     `json:"broadcaster"`
	Database        DatabaseConfig        `json:"database"`
	NodeHealth      NodeHealthConfig      `json:"nodeHealth"`
}

// ServerConfig holds server-specific configuration
type ServerConfig struct {
	Port    string        `json:"port"`
	Timeout time.Duration `json:"timeout"`
}

// FetcherConfig holds fetcher configuration
type FetcherConfig struct {
	PollInterval time.Duration `json:"pollInterval"`
	BatchSize    int           `json:"batchSize"`
	MockMode     bool          `json:"mockMode"`
	GraphQLURL   string        `json:"graphqlUrl"`
}

// BackwardFetcherConfig holds backward fetcher configuration
type BackwardFetcherConfig struct {
	Enabled                bool          `json:"enabled"`                // Whether backward sync is enabled
	MaxDepthDays           int           `json:"maxDepthDays"`           // Maximum number of days to sync backwards
	BatchSize              int           `json:"batchSize"`              // Batch size for GraphQL queries
	GraphQLURL             string        `json:"graphqlUrl"`             // GraphQL endpoint URL
	HTTPTimeout            time.Duration `json:"httpTimeout"`            // HTTP client timeout
	RetryDelay             time.Duration `json:"retryDelay"`             // Delay between failed request retries
	DatabaseRetryDelay     time.Duration `json:"databaseRetryDelay"`     // Delay between failed database saves
	RateLimitDelay         time.Duration `json:"rateLimitDelay"`         // Delay between successful requests (rate limiting)
	BackpressureTimeoutMs  int           `json:"backpressureTimeoutMs"`  // Timeout for database backpressure in milliseconds
}

// ProcessorConfig holds processor configuration
type ProcessorConfig struct {
	NaturalFlow      bool          `json:"naturalFlow"`      // Enable natural flow timing for WebSocket broadcasts
	FlowMinDelay     time.Duration `json:"flowMinDelay"`     // Minimum delay between transfers
	FlowMaxDelay     time.Duration `json:"flowMaxDelay"`     // Maximum delay between transfers  
	MaxBurstSize     int           `json:"maxBurstSize"`     // Max transfers to send immediately (rest get natural timing)
}

// BatcherConfig holds batcher configuration
type BatcherConfig struct {
	FlushInterval time.Duration `json:"flushInterval"`
	BatchSize     int           `json:"batchSize"`
	MaxRetries    int           `json:"maxRetries"`
}

// BroadcasterConfig holds broadcaster configuration
type BroadcasterConfig struct {
	MaxClients      int  `json:"maxClients"`
	BufferSize      int  `json:"bufferSize"`
	DropSlowClients bool `json:"dropSlowClients"`
	NumShards       int  `json:"numShards"`
	WorkersPerShard int  `json:"workersPerShard"`
}

// DatabaseConfig holds ClickHouse database configuration
type DatabaseConfig struct {
	Host      string `json:"host"`
	Port      int    `json:"port"`
	Database  string `json:"database"`
	Username  string `json:"username"`
	Password  string `json:"password"`
	Debug     bool   `json:"debug"`
	BatchSize int    `json:"batchSize"`
}

// NodeHealthConfig holds node health checker configuration
type NodeHealthConfig struct {
	GraphQLURL      string        `json:"graphqlUrl"`
	CheckInterval   time.Duration `json:"checkInterval"`
	RequestTimeout  time.Duration `json:"requestTimeout"`
	MaxConcurrency  int           `json:"maxConcurrency"`
}

// LoadConfig returns the single configuration for the application
func LoadConfig() Config {
	return Config{
		Server: ServerConfig{
			Port:    getEnvString("SERVER_PORT", ":8080"),
			Timeout: getEnvDuration("SERVER_TIMEOUT", 30*time.Second),
		},
		Fetcher: FetcherConfig{
			PollInterval: getEnvDuration("FETCHER_POLL_INTERVAL", 500*time.Millisecond),
			BatchSize:    getEnvInt("FETCHER_BATCH_SIZE", 100),
			MockMode:     getEnvBool("FETCHER_MOCK_MODE", false),
			GraphQLURL:   getEnvString("FETCHER_GRAPHQL_URL", "https://staging.graphql.union.build/v1/graphql"),
		},
		BackwardFetcher: BackwardFetcherConfig{
			Enabled:               getEnvBool("BACKWARD_FETCHER_ENABLED", true),              // Enabled by default
			MaxDepthDays:          getEnvInt("BACKWARD_FETCHER_MAX_DEPTH_DAYS", 30),         // 30 days default
			BatchSize:             getEnvInt("BACKWARD_FETCHER_BATCH_SIZE", 100),            // Same as forward fetcher
			GraphQLURL:            getEnvString("BACKWARD_FETCHER_GRAPHQL_URL", "https://staging.graphql.union.build/v1/graphql"),
			HTTPTimeout:           getEnvDuration("BACKWARD_FETCHER_HTTP_TIMEOUT", 30*time.Second),
			RetryDelay:            getEnvDuration("BACKWARD_FETCHER_RETRY_DELAY", 1*time.Second),
			DatabaseRetryDelay:    getEnvDuration("BACKWARD_FETCHER_DB_RETRY_DELAY", 500*time.Millisecond),
			RateLimitDelay:        getEnvDuration("BACKWARD_FETCHER_RATE_LIMIT_DELAY", 150*time.Millisecond),
			BackpressureTimeoutMs: getEnvInt("BACKWARD_FETCHER_BACKPRESSURE_TIMEOUT_MS", 1000),
		},
		Processor: ProcessorConfig{
			NaturalFlow:  getEnvBool("PROCESSOR_NATURAL_FLOW", true),
			FlowMinDelay: getEnvDuration("PROCESSOR_FLOW_MIN_DELAY", 0*time.Millisecond),
			FlowMaxDelay: getEnvDuration("PROCESSOR_FLOW_MAX_DELAY", 100*time.Millisecond),
			MaxBurstSize: getEnvInt("PROCESSOR_MAX_BURST_SIZE", 1),
		},
		Batcher: BatcherConfig{
			FlushInterval: getEnvDuration("BATCHER_FLUSH_INTERVAL", 10*time.Second),
			BatchSize:     getEnvInt("BATCHER_BATCH_SIZE", 2000),
			MaxRetries:    getEnvInt("BATCHER_MAX_RETRIES", 3),
		},
		Broadcaster: BroadcasterConfig{
			MaxClients:      getEnvInt("BROADCASTER_MAX_CLIENTS", 1000),
			BufferSize:      getEnvInt("BROADCASTER_BUFFER_SIZE", 1000),
			DropSlowClients: getEnvBool("BROADCASTER_DROP_SLOW_CLIENTS", true),
			NumShards:       getEnvInt("BROADCASTER_NUM_SHARDS", 4),
			WorkersPerShard: getEnvInt("BROADCASTER_WORKERS_PER_SHARD", 4),
		},
		Database: DatabaseConfig{
			Host:      getEnvString("CLICKHOUSE_HOST", "localhost"),
			Port:      getEnvInt("CLICKHOUSE_PORT", 9000),
			Database:  getEnvString("CLICKHOUSE_DATABASE", "websocket_analytics"),
			Username:  getEnvString("CLICKHOUSE_USER", "websocket_user"),
			Password:  getEnvString("CLICKHOUSE_PASSWORD", ""),
			Debug:     getEnvBool("CLICKHOUSE_DEBUG", false),
			BatchSize: getEnvInt("CLICKHOUSE_BATCH_SIZE", 10000),
		},
		NodeHealth: NodeHealthConfig{
			GraphQLURL:     getEnvString("NODE_HEALTH_GRAPHQL_URL", "https://staging.graphql.union.build/v1/graphql"),
			CheckInterval:  getEnvDuration("NODE_HEALTH_CHECK_INTERVAL", 10*time.Second),
			RequestTimeout: getEnvDuration("NODE_HEALTH_REQUEST_TIMEOUT", 15*time.Second),
			MaxConcurrency: getEnvInt("NODE_HEALTH_MAX_CONCURRENCY", 15),
		},
	}
}

// Helper functions for clean environment variable handling

func getEnvString(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

func getEnvBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		return value == "true"
	}
	return defaultValue
}

func getEnvDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if parsed, err := time.ParseDuration(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

 