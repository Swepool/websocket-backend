package transport

import (
	"context"
	"fmt"
	"net/http"
	"websocket-backend/internal/core"
	"websocket-backend/internal/storage"
)

// CoordinatorInterface defines what the server needs from the coordinator
type CoordinatorInterface interface {
	GetBroadcaster() BroadcasterInterface
	GetClickHouseService() *storage.ClickHouseService
	GetChannels() *core.Channels
	GetStats() map[string]interface{}
	Health(ctx context.Context) map[string]interface{}
}

// Server represents the HTTP server for the clean modular architecture
type Server struct {
	coordinator CoordinatorInterface
}

// NewServer creates a new clean server
func NewServer(coordinator CoordinatorInterface) *Server {
	return &Server{
		coordinator: coordinator,
	}
}

// Start starts the clean HTTP server
func (s *Server) Start(ctx context.Context, addr string) error {
	
	mux := http.NewServeMux()
	
	// WebSocket endpoint
	mux.HandleFunc("/ws", s.handleWebSocket)
	
	// API endpoints for clean architecture
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/health", s.handleHealth)
	mux.HandleFunc("/api/pipeline", s.handlePipelineStats)
	mux.HandleFunc("/api/channels", s.handleChannelStats)
	mux.HandleFunc("/api/clickhouse", s.handleClickHouseStats)
	
	// Component-specific endpoints
	mux.HandleFunc("/api/fetcher", s.handleFetcherStats)
	mux.HandleFunc("/api/processor", s.handleProcessorStats)
	mux.HandleFunc("/api/scheduler", s.handleSchedulerStats)
	mux.HandleFunc("/api/batcher", s.handleBatcherStats)
	mux.HandleFunc("/api/broadcaster", s.handleBroadcasterStats)
	mux.HandleFunc("/api/cache", s.handleCacheStats)
	
	// Health check endpoint (for compatibility)
	mux.HandleFunc("/health", s.handleHealth)
	
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	
	fmt.Printf("Clean HTTP server listening on %s\n", addr)
	
	// Start server in a goroutine
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP server error: %v\n", err)
		}
	}()
	
	// Wait for context cancellation
	<-ctx.Done()
	fmt.Printf("Shutting down clean HTTP server...\n")
	return server.Shutdown(context.Background())
}

 