package core

import (
	"context"
	"log"
	"sync"
	"time"
	"websocket-backend-new/api/graphql"
	"websocket-backend-new/api/http"
	"websocket-backend-new/config"
	"websocket-backend-new/storage"
	"websocket-backend-new/threads/broadcaster"
	"websocket-backend-new/threads/fetcher"
)

// App represents the main application
type App struct {
	config      *config.Config
	memory      *storage.Memory
	channels    *storage.Channels
	
	// Components
	graphqlClient  *graphql.Client
	coordinator    *fetcher.Coordinator
	broadcaster    *broadcaster.Broadcaster
	httpServer     *http.Server
	
	// Lifecycle
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// NewApp creates a new application instance
func NewApp() *App {
	cfg := config.Default()
	memory := storage.NewMemory()
	channels := storage.NewChannels()
	
	// Create GraphQL client
	graphqlClient := graphql.NewClient(cfg.GraphQLEndpoint)
	
	// Create main coordinator (Thread 1: Fetch → Enhance → Broadcast)
	coordinator := fetcher.NewCoordinator(cfg, memory, channels, graphqlClient)
	
	// Create broadcaster (Thread 5: Broadcasting)
	broadcasterThread := broadcaster.NewBroadcaster(memory, channels)
	
	// Create HTTP server
	httpServer := http.NewServer(memory, channels)
	
	ctx, cancel := context.WithCancel(context.Background())
	
	return &App{
		config:        cfg,
		memory:        memory,
		channels:      channels,
		graphqlClient: graphqlClient,
		coordinator:   coordinator,
		broadcaster:   broadcasterThread,
		httpServer:    httpServer,
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Start starts all application threads
func (a *App) Start() error {
	log.Printf("🚀 Starting WebSocket Backend (Clean 5-Thread Architecture)")
	log.Printf("📡 GraphQL Endpoint: %s", a.config.GraphQLEndpoint)
	log.Printf("⏱️  Poll Interval: %s", a.config.PollInterval)
	
	// Initialize chains first
	if err := a.initializeChains(); err != nil {
		log.Printf("⚠️ Warning: Failed to fetch chains: %v", err)
		log.Printf("📝 Continuing without chain data - will retry during operation")
	}
	
	// Start Thread 1: Main processing (Fetch → Enhance → Broadcast)
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.coordinator.Start(a.ctx)
	}()
	
	// Start Thread 5: Broadcasting
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		a.broadcaster.Start()
	}()
	
	// Start HTTP Server (with WebSocket support)
	a.wg.Add(1)
	go func() {
		defer a.wg.Done()
		if err := a.httpServer.Start(a.config.Port); err != nil {
			log.Printf("[HTTP] ❌ HTTP server error: %v", err)
		}
	}()
	
	// TODO: Start Thread 2 (Stats), Thread 3 (Charts), Thread 4 (Clients)
	// For now, we have a minimal working version with just the main flow + HTTP
	
	log.Printf("✅ All threads started successfully")
	log.Printf("👀 Watch the logs to see live transfers being processed!")
	log.Printf("🔄 Fetching new transfers every %s", a.config.PollInterval)
	log.Printf("🌐 Web interface: http://localhost%s", a.config.Port)
	log.Printf("📡 WebSocket: ws://localhost%s/ws", a.config.Port)
	
	return nil
}

// initializeChains fetches chain data on startup
func (a *App) initializeChains() error {
	log.Printf("🔗 Fetching chain information...")
	
	ctx, cancel := context.WithTimeout(a.ctx, 30*time.Second)
	defer cancel()
	
	chains, err := a.graphqlClient.FetchChains(ctx)
	if err != nil {
		return err
	}
	
	a.memory.UpdateChains(chains)
	log.Printf("✅ Loaded %d chains", len(chains))
	
	return nil
}

// Stop gracefully stops all application threads
func (a *App) Stop() {
	log.Printf("🛑 Stopping application...")
	
	// Signal shutdown to all threads
	a.cancel()
	a.channels.Close()
	
	// Wait for all threads to finish
	done := make(chan struct{})
	go func() {
		a.wg.Wait()
		close(done)
	}()
	
	// Wait with timeout
	select {
	case <-done:
		log.Printf("✅ All threads stopped gracefully")
	case <-time.After(10 * time.Second):
		log.Printf("⚠️ Shutdown timeout, forcing exit")
	}
}

// GetStatus returns application status
func (a *App) GetStatus() map[string]interface{} {
	status := map[string]interface{}{
		"config": map[string]interface{}{
			"pollInterval":     a.config.PollInterval.String(),
			"graphqlEndpoint":  a.config.GraphQLEndpoint,
			"port":             a.config.Port,
		},
		"chains": map[string]interface{}{
			"count": len(a.memory.GetChains()),
		},
		"clients": map[string]interface{}{
			"count": a.memory.GetClientCount(),
		},
	}
	
	// Add coordinator status
	if a.coordinator != nil {
		status["coordinator"] = a.coordinator.GetStatus()
	}
	
	return status
} 