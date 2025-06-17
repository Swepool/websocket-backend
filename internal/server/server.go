package server

import (
	"context"
	"fmt"
	"net/http"
	"websocket-backend-new/internal/pipeline"
	"websocket-backend-new/internal/chains"
	"github.com/gorilla/websocket"
)

// Server represents the HTTP server
type Server struct {
	coordinator    *pipeline.Coordinator
	chainsService  *chains.Service
	upgrader       websocket.Upgrader
}

// NewServer creates a new server with the given coordinator
func NewServer(coordinator *pipeline.Coordinator, chainsService *chains.Service) *Server {
	return &Server{
		coordinator:   coordinator,
		chainsService: chainsService,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins in development
			},
		},
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
	
	// Health check endpoint (for compatibility)
	mux.HandleFunc("/health", s.handleHealth)
	
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	
	fmt.Printf("[SERVER] Starting HTTP server on %s\n", addr)
	
	// Start server in a goroutine
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("[SERVER] HTTP server error: %v\n", err)
		}
	}()
	
	// Wait for context cancellation
	<-ctx.Done()
	
	fmt.Printf("[SERVER] Shutting down HTTP server\n")
	return server.Shutdown(context.Background())
}

// GetCoordinator returns the pipeline coordinator
func (s *Server) GetCoordinator() *pipeline.Coordinator {
	return s.coordinator
} 