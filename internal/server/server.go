package server

import (
	"context"
	"fmt"
	"net/http"
	"websocket-backend-new/internal/pipeline"
	"websocket-backend-new/internal/chains"
)

// Server represents the HTTP server
type Server struct {
	coordinator    *pipeline.Coordinator
	chainsService  *chains.Service
}

// NewServer creates a new server with the given coordinator
func NewServer(coordinator *pipeline.Coordinator, chainsService *chains.Service) *Server {
	return &Server{
		coordinator:   coordinator,
		chainsService: chainsService,
	}
}

// Start starts the HTTP server and chains service
func (s *Server) Start(ctx context.Context, addr string) error {
	// Start chains service
	go s.chainsService.Start(ctx)
	
	mux := http.NewServeMux()
	
	// WebSocket endpoint
	mux.HandleFunc("/ws", s.handleWebSocket)
	
	// API endpoints
	mux.HandleFunc("/api/chains", s.handleChains)
	mux.HandleFunc("/api/stats", s.handleStats)
	mux.HandleFunc("/api/health", s.handleHealth)
	mux.HandleFunc("/api/broadcaster", s.handleBroadcasterStats)
	mux.HandleFunc("/api/database", s.handleDatabaseStats) // Database storage monitoring
	
	// Pipeline component stats endpoints
	mux.HandleFunc("/api/scheduler", s.handleSchedulerStats)   // Scheduler statistics
	mux.HandleFunc("/api/processor", s.handleProcessorStats)   // Processor statistics  
	mux.HandleFunc("/api/sync", s.handleSyncStats)            // Sync manager statistics
	mux.HandleFunc("/api/pipeline", s.handlePipelineStats)    // Comprehensive pipeline stats
	mux.HandleFunc("/api/cache", s.handleCacheStats)          // Chart data cache stats
	
	// Health check endpoint (for compatibility)
	mux.HandleFunc("/health", s.handleHealth)
	
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	
	fmt.Printf("HTTP server listening on %s\n", addr)
	
	// Start server in a goroutine
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("HTTP server error: %v\n", err)
		}
	}()
	
	// Wait for context cancellation
	<-ctx.Done()
	fmt.Printf("Shutting down HTTP server...\n")
	return server.Shutdown(context.Background())
}

// GetCoordinator returns the pipeline coordinator
func (s *Server) GetCoordinator() *pipeline.Coordinator {
	return s.coordinator
} 