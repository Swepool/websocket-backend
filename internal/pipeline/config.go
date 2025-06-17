package pipeline

import (
	"websocket-backend-new/internal/fetcher"
	"websocket-backend-new/internal/enhancer"
	"websocket-backend-new/internal/scheduler"
	"websocket-backend-new/internal/broadcaster"
)

// Config holds configuration for the entire pipeline
type Config struct {
	Fetcher     fetcher.Config     `json:"fetcher"`
	Enhancer    enhancer.Config    `json:"enhancer"`
	Scheduler   scheduler.Config   `json:"scheduler"`
	Broadcaster broadcaster.Config `json:"broadcaster"`
	Stats       StatsConfig        `json:"stats"`
}

// StatsConfig holds configuration for statistics collection
type StatsConfig struct {
	RetentionHours  int `json:"retentionHours"`  // How long to keep detailed stats (default: 24)
	TopItemsLimit   int `json:"topItemsLimit"`   // Max items for charts (default: 20)
}

// DefaultConfig returns default pipeline configuration
func DefaultConfig() Config {
	return Config{
		Fetcher:     fetcher.DefaultConfig(),
		Enhancer:    enhancer.DefaultConfig(),
		Scheduler:   scheduler.DefaultConfig(),
		Broadcaster: broadcaster.DefaultConfig(),
		Stats: StatsConfig{
			RetentionHours: 24,
			TopItemsLimit:  20,
		},
	}
} 