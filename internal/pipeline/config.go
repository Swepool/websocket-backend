package pipeline

import (
	"websocket-backend-new/internal/fetcher"
	"websocket-backend-new/internal/processor"
	"websocket-backend-new/internal/scheduler"
	"websocket-backend-new/internal/broadcaster"
	"websocket-backend-new/internal/database"
)

// Config holds configuration for the entire simplified pipeline
type Config struct {
	Fetcher     fetcher.Config     `json:"fetcher"`
	Processor   processor.Config   `json:"processor"`
	Scheduler   scheduler.Config   `json:"scheduler"`
	Broadcaster broadcaster.Config `json:"broadcaster"`
	Database    database.Config    `json:"database"`
}

// DefaultConfig returns default pipeline configuration
func DefaultConfig() Config {
	return Config{
		Fetcher:     fetcher.DefaultConfig(),
		Processor:   processor.DefaultConfig(),
		Scheduler:   scheduler.DefaultConfig(),
		Broadcaster: broadcaster.DefaultConfig(),
		Database:    database.DefaultConfig(),
	}
} 