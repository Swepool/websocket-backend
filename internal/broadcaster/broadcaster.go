package broadcaster

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/internal/utils"
	"websocket-backend-new/models"
	"websocket-backend-new/internal/stats"
	"github.com/gorilla/websocket"
)

// Config holds configuration for the broadcaster
type Config struct {
	MaxClients      int  `json:"maxClients"`      // Maximum clients per shard (default: 1000)
	BufferSize      int  `json:"bufferSize"`      // Buffer size for each client (default: 100)
	DropSlowClients bool `json:"dropSlowClients"` // Whether to drop slow clients (default: true)
	NumShards       int  `json:"numShards"`       // Number of shards (default: 4)
	WorkersPerShard int  `json:"workersPerShard"` // Number of workers per shard (default: 4)
}

// DefaultConfig returns the default broadcaster configuration
func DefaultConfig() Config {
	return Config{
		MaxClients:      1000,
		BufferSize:      100,
		DropSlowClients: true,
		NumShards:       4,
		WorkersPerShard: 4,
	}
}

// Client represents a WebSocket client with shard information and enhanced management
type Client struct {
	id       string
	conn     *websocket.Conn
	send     chan []byte
	shardID  int
	lastPong time.Time
	isClosing bool
	closeMu   sync.Mutex
}

// Shard represents a shard of clients with its own worker pool and enhanced management
type Shard struct {
	id              int
	clients         map[*Client]bool
	register        chan *Client
	unregister      chan *Client
	broadcast       chan []byte
	mu              sync.RWMutex
	config          Config
	workerPool      chan chan []byte
	workers         []*ShardWorker
	ctx             context.Context
	cancel          context.CancelFunc
	clientCount     int64
	healthTicker    *time.Ticker
	shutdownOnce    sync.Once
	// Callback for when new clients connect
	onClientConnect func(*Client)
}

// ShardWorker represents a worker that handles broadcasts for a shard with improved efficiency
type ShardWorker struct {
	id         int
	shardID    int
	work       chan []byte
	workerPool chan chan []byte
	quit       chan bool
	shard      *Shard
	isActive   int32 // atomic flag for worker state
}

// Broadcaster manages WebSocket connections using a sharded approach
type Broadcaster struct {
	config         Config
	channels       *channels.Channels
	statsCollector *stats.Collector
	shards         []*Shard
	upgrader       websocket.Upgrader
	totalClients   int64
	mu             sync.RWMutex
	shutdownOnce   sync.Once
	chartFetching  int32 // atomic flag to prevent concurrent chart fetches
}

// NewBroadcaster creates a new sharded broadcaster
func NewBroadcaster(config Config, channels *channels.Channels, statsCollector *stats.Collector) *Broadcaster {
	sb := &Broadcaster{
		config:         config,
		channels:       channels,
		statsCollector: statsCollector,
		shards:         make([]*Shard, config.NumShards),
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow connections from any origin
			},
			// Add connection timeout settings
			HandshakeTimeout: 45 * time.Second,
		},
	}

	// Initialize shards with enhanced settings
	for i := 0; i < config.NumShards; i++ {
		sb.shards[i] = newShard(i, config)
		// Set callback for new client connections
		sb.shards[i].onClientConnect = sb.sendInitialChartData
	}

	return sb
}

// newShard creates a new shard with worker pool and enhanced management
func newShard(id int, config Config) *Shard {
	ctx, cancel := context.WithCancel(context.Background())
	
	shard := &Shard{
		id:         id,
		clients:    make(map[*Client]bool),
		register:   make(chan *Client, 200), // Larger buffer for better throughput
		unregister: make(chan *Client, 200), // Larger buffer for cleanup bursts
		broadcast:  make(chan []byte, 2000), // Larger buffer for high load
		config:     config,
		workerPool: make(chan chan []byte, config.WorkersPerShard),
		workers:    make([]*ShardWorker, config.WorkersPerShard),
		ctx:        ctx,
		cancel:     cancel,
		healthTicker: time.NewTicker(45 * time.Second), // Health check every 45 seconds
	}

	// Initialize workers for this shard with better settings
	for i := 0; i < config.WorkersPerShard; i++ {
		worker := &ShardWorker{
			id:         i,
			shardID:    id,
			work:       make(chan []byte, 200), // Larger worker buffer
			workerPool: shard.workerPool,
			quit:       make(chan bool, 1), // Buffered for non-blocking shutdown
			shard:      shard,
			isActive:   1,
		}
		shard.workers[i] = worker
	}

	return shard
}

// Start begins all shards and their worker pools with enhanced error handling
func (sb *Broadcaster) Start(ctx context.Context) {
	utils.LogInfo("SHARDED_BROADCASTER", "Starting with %d shards, %d workers per shard", 
		sb.config.NumShards, sb.config.WorkersPerShard)

	// Start all shards
	for i, shard := range sb.shards {
		go shard.start()
		utils.LogInfo("SHARDED_BROADCASTER", "Started shard %d with %d workers", i, sb.config.WorkersPerShard)
	}

	// Create a ticker for periodic chart updates
	chartTicker := time.NewTicker(15 * time.Second)
	
	defer func() {
		chartTicker.Stop()
		sb.shutdown()
	}()

	for {
		select {
		case <-ctx.Done():
			utils.LogInfo("SHARDED_BROADCASTER", "Context cancelled, shutting down")
			return

		case transfer := <-sb.channels.TransferBroadcasts:
			sb.broadcastTransfer(transfer)

		case chartData := <-sb.channels.ChartUpdates:
			sb.broadcastChartData(chartData)

		case <-chartTicker.C:
			sb.sendPeriodicChartUpdate()
		}
	}
}

// start begins a shard's main loop and worker pool with enhanced management
func (s *Shard) start() {
	utils.LogInfo("SHARDED_BROADCASTER", "Shard %d starting with enhanced management", s.id)
	
	// Start all workers
	for _, worker := range s.workers {
		go worker.start()
	}

	// Start health monitoring
	go s.healthMonitor()

	defer func() {
		s.shutdownOnce.Do(func() {
			utils.LogInfo("SHARDED_BROADCASTER", "Shard %d shutting down", s.id)
			s.healthTicker.Stop()
			
			// Shutdown workers
			for _, worker := range s.workers {
				worker.stop()
			}
			
			// Close all clients
			s.mu.Lock()
			for client := range s.clients {
				client.safeClose()
			}
			s.clients = make(map[*Client]bool)
			s.mu.Unlock()
		})
	}()

	for {
		select {
		case <-s.ctx.Done():
			return

		case client := <-s.register:
			s.handleClientRegistration(client)

		case client := <-s.unregister:
			s.handleClientUnregistration(client)

		case data := <-s.broadcast:
			s.dispatchToWorkers(data)
		}
	}
}

// healthMonitor performs periodic health checks on shard clients
func (s *Shard) healthMonitor() {
	defer s.healthTicker.Stop()
	
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-s.healthTicker.C:
			s.performHealthCheck()
		}
	}
}

// performHealthCheck checks client health and removes stale connections
func (s *Shard) performHealthCheck() {
	s.mu.RLock()
	clients := make([]*Client, 0, len(s.clients))
	for client := range s.clients {
		clients = append(clients, client)
	}
	s.mu.RUnlock()
	
	now := time.Now()
	staleClients := 0
	
	for _, client := range clients {
		// Check if client hasn't responded to pings for too long (2 minutes)
		if now.Sub(client.lastPong) > 2*time.Minute {
					utils.LogInfo("SHARDED_BROADCASTER", "Shard %d client %s appears stale (last pong: %v ago), removing", 
			s.id, client.id, now.Sub(client.lastPong))
			
			// Non-blocking unregister
			select {
			case s.unregister <- client:
			default:
				// If channel is full, force close the client
				client.safeClose()
			}
			staleClients++
		}
	}
	
	if staleClients > 0 {
		utils.LogInfo("SHARDED_BROADCASTER", "Shard %d health check removed %d stale clients", s.id, staleClients)
	}
}

// start begins a worker's processing loop with improved efficiency
func (w *ShardWorker) start() {
	atomic.StoreInt32(&w.isActive, 1)
	
	defer func() {
		atomic.StoreInt32(&w.isActive, 0)
		if r := recover(); r != nil {
			utils.LogError("SHARDED_BROADCASTER", "Worker %d-%d panic recovered: %v", w.shardID, w.id, r)
		}
	}()
	
	for {
		// Make worker available in pool
		select {
		case w.workerPool <- w.work:
		case <-w.quit:
			return
		}

		select {
		case data := <-w.work:
			w.handleBroadcast(data)
		case <-w.quit:
			return
		}
	}
}

// stop stops a worker gracefully
func (w *ShardWorker) stop() {
	if atomic.LoadInt32(&w.isActive) == 1 {
		select {
		case w.quit <- true:
		default:
		}
	}
}

// handleBroadcast processes a broadcast for this worker's clients with improved efficiency
func (w *ShardWorker) handleBroadcast(data []byte) {
	w.shard.mu.RLock()
	clients := make([]*Client, 0, len(w.shard.clients))
	for client := range w.shard.clients {
		clients = append(clients, client)
	}
	w.shard.mu.RUnlock()

	if len(clients) == 0 {
		return
	}

	// Distribute work across workers by client hash for better load balancing
	workersCount := len(w.shard.workers)
	successCount := 0
	failedCount := 0
	
	for _, client := range clients {
		// Check if this worker should handle this client
		clientWorkerID := int(hash(client.id)) % workersCount
		if clientWorkerID != w.id {
			continue
		}

		// Skip closing clients
		if client.isClosing {
			continue
		}

		select {
		case client.send <- data:
			successCount++
		default:
			// Client's send channel is full
			failedCount++
			if w.shard.config.DropSlowClients {
				// Non-blocking unregister
				select {
				case w.shard.unregister <- client:
				default:
					// If unregister channel is full, force close
					client.safeClose()
				}
			}
		}
	}

	if successCount > 0 || failedCount > 0 {
		utils.LogDebug("SHARDED_BROADCASTER", "Worker %d-%d sent data to %d clients, failed %d", 
			w.shardID, w.id, successCount, failedCount)
	}
}

// dispatchToWorkers sends broadcast data to available workers with improved fallback
func (s *Shard) dispatchToWorkers(data []byte) {
	clientCount := atomic.LoadInt64(&s.clientCount)
	if clientCount == 0 {
		return
	}

	// Try to dispatch to available worker with timeout
	select {
	case work := <-s.workerPool:
		select {
		case work <- data:
			// Successfully dispatched to worker
		case <-time.After(10 * time.Millisecond):
			// Worker is slow, return to pool and use fallback
			go func() { 
				select {
				case s.workerPool <- work:
				default:
				}
			}()
			s.directBroadcast(data)
		}
	case <-time.After(5 * time.Millisecond):
		// No workers available quickly, use direct broadcast
		s.directBroadcast(data)
	}
}

// directBroadcast sends data directly to all clients in shard (fallback) with improved handling
func (s *Shard) directBroadcast(data []byte) {
	s.mu.RLock()
	clients := make([]*Client, 0, len(s.clients))
	for client := range s.clients {
		clients = append(clients, client)
	}
	s.mu.RUnlock()

	successCount := 0
	failedCount := 0
	
	for _, client := range clients {
		if client.isClosing {
			continue
		}
		
		select {
		case client.send <- data:
			successCount++
		default:
			failedCount++
			if s.config.DropSlowClients {
				// Non-blocking unregister
				select {
				case s.unregister <- client:
				default:
					client.safeClose()
				}
			}
		}
	}

	if successCount > 0 || failedCount > 0 {
		utils.LogDebug("SHARDED_BROADCASTER", "Shard %d direct broadcast to %d clients, failed %d", 
			s.id, successCount, failedCount)
	}
}

// handleClientRegistration handles new client registration in shard with enhanced management
func (s *Shard) handleClientRegistration(client *Client) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check client limits for this shard
	if len(s.clients) >= s.config.MaxClients {
		utils.LogWarn("SHARDED_BROADCASTER", "Shard %d max clients reached (%d), rejecting client %s", 
			s.id, s.config.MaxClients, client.id)
		client.safeClose()
		return
	}

	s.clients[client] = true
	atomic.AddInt64(&s.clientCount, 1)
	client.lastPong = time.Now()

	utils.LogInfo("SHARDED_BROADCASTER", "Shard %d client %s registered, total: %d", s.id, client.id, len(s.clients))

	// Start client's goroutines
	go client.writePump(s.unregister)
	go client.readPump(s.unregister)
	
	// Call callback for new client connection (send initial chart data)
	if s.onClientConnect != nil {
		go s.onClientConnect(client)
	}
}

// handleClientUnregistration handles client disconnection in shard with proper cleanup
func (s *Shard) handleClientUnregistration(client *Client) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.clients[client]; ok {
		delete(s.clients, client)
		client.safeClose()
		atomic.AddInt64(&s.clientCount, -1)
			utils.LogInfo("SHARDED_BROADCASTER", "Shard %d client %s unregistered, remaining: %d", 
		s.id, client.id, len(s.clients))
	}
}

// getShardForClient determines which shard a client should belong to
func (sb *Broadcaster) getShardForClient(clientID string) int {
	return int(hash(clientID)) % sb.config.NumShards
}

// hash function for consistent client distribution
func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// broadcastTransfer broadcasts a single transfer to all shards with optimized performance
func (sb *Broadcaster) broadcastTransfer(transfer models.Transfer) {
	// Use optimized marshaling with built-in pooling
	data, err := utils.DefaultMarshalTransfer(transfer)
	if err != nil {
		utils.LogError("SHARDED_BROADCASTER", "Failed to marshal transfer %s: %v", transfer.PacketHash, err)
		return
	}

	totalClients := sb.GetClientCount()
	
	// Always log when we receive a transfer, even if no clients
	if totalClients == 0 {
		utils.LogDebug("SHARDED_BROADCASTER", "Received transfer %s but no clients connected, skipping broadcast", transfer.PacketHash)
		return
	}
	
	utils.LogInfo("SHARDED_BROADCASTER", "Broadcasting transfer %s to %d shards (%d total clients)", 
		transfer.PacketHash, len(sb.shards), totalClients)

	// Broadcast to all shards with improved error handling
	successfulShards := 0
	for _, shard := range sb.shards {
		select {
		case shard.broadcast <- data:
			successfulShards++
		default:
			utils.LogWarn("SHARDED_BROADCASTER", "Shard %d broadcast channel full", shard.id)
		}
	}

	if successfulShards > 0 {
		utils.LogInfo("SHARDED_BROADCASTER", "Successfully sent transfer %s to %d/%d shards", 
			transfer.PacketHash, successfulShards, len(sb.shards))
	}
}

// broadcastChartData broadcasts chart data to all shards with improved error handling
func (sb *Broadcaster) broadcastChartData(rawData interface{}) {
	if rawData == nil {
		utils.LogWarn("SHARDED_BROADCASTER", "Attempted to broadcast nil chart data")
		return
	}
	
	data, err := utils.DefaultMarshalChart(rawData)
	if err != nil {
		utils.LogError("SHARDED_BROADCASTER", "Failed to marshal chart data: %v", err)
		return
	}

	totalClients := sb.GetClientCount()
	if totalClients == 0 {
		utils.LogDebug("SHARDED_BROADCASTER", "No clients connected for chart data broadcast")
		return
	}
	
	utils.LogInfo("SHARDED_BROADCASTER", "Broadcasting chart data to %d shards (%d total clients, size: %d bytes)", 
		len(sb.shards), totalClients, len(data))

	successfulShards := 0
	for _, shard := range sb.shards {
		select {
		case shard.broadcast <- data:
			successfulShards++
		default:
			utils.LogWarn("SHARDED_BROADCASTER", "Shard %d chart broadcast channel full", shard.id)
		}
	}
	
	if successfulShards > 0 {
		utils.LogInfo("SHARDED_BROADCASTER", "Successfully sent chart data to %d/%d shards", 
			successfulShards, len(sb.shards))
	} else {
		utils.LogWarn("SHARDED_BROADCASTER", "Failed to send chart data to any shards")
	}
}

// sendPeriodicChartUpdate sends periodic chart updates ONLY when meaningful data exists
func (sb *Broadcaster) sendPeriodicChartUpdate() {
	// Always fetch chart data to keep stats fresh, even with no clients
	// Check if we're already fetching chart data to prevent concurrent fetches
	if !atomic.CompareAndSwapInt32(&sb.chartFetching, 0, 1) {
		utils.LogDebug("SHARDED_BROADCASTER", "Chart data fetch already in progress, skipping")
		return
	}
	
	// Run chart data fetching in a separate goroutine to avoid blocking the main loop
	go func() {
		defer func() {
			atomic.StoreInt32(&sb.chartFetching, 0) // Reset the flag
			if r := recover(); r != nil {
				utils.LogError("SHARDED_BROADCASTER", "Panic in sendPeriodicChartUpdate: %v", r)
			}
		}()
		
		clientCount := sb.GetClientCount()
		utils.LogDebug("SHARDED_BROADCASTER", "Fetching periodic chart data for %d clients", clientCount)
		
		// Fetch chart data with timeout
		chartDataChan := make(chan interface{}, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					utils.LogError("SHARDED_BROADCASTER", "Panic in periodic chart data fetch: %v", r)
				}
			}()
			chartData := sb.statsCollector.GetChartDataForFrontend()
			select {
			case chartDataChan <- chartData:
			default:
			}
		}()
		
		var chartData interface{}
		select {
		case chartData = <-chartDataChan:
			if clientCount > 0 {
				utils.LogDebug("SHARDED_BROADCASTER", "Broadcasting chart data to all clients")
				sb.broadcastChartData(chartData)
			} else {
				utils.LogDebug("SHARDED_BROADCASTER", "Chart data refreshed but no clients connected")
			}
		case <-time.After(3 * time.Second):
			utils.LogWarn("SHARDED_BROADCASTER", "Timeout fetching periodic chart data")
		}
	}()
}

// UpgradeConnection upgrades HTTP connection to WebSocket and assigns to appropriate shard with enhanced settings
func (sb *Broadcaster) UpgradeConnection(w http.ResponseWriter, r *http.Request) {
	conn, err := sb.upgrader.Upgrade(w, r, nil)
	if err != nil {
		utils.LogError("SHARDED_BROADCASTER", "Failed to upgrade connection: %v", err)
		return
	}

	// Configure connection settings
	conn.SetReadLimit(512)

	clientID := generateClientID()
	shardID := sb.getShardForClient(clientID)

	client := &Client{
		id:        clientID,
		conn:      conn,
		send:      make(chan []byte, sb.config.BufferSize),
		shardID:   shardID,
		lastPong:  time.Now(),
		isClosing: false,
	}

	utils.LogInfo("SHARDED_BROADCASTER", "New connection from %s, client %s assigned to shard %d", 
		r.RemoteAddr, clientID, shardID)

	// Register client with appropriate shard (non-blocking)
	shard := sb.shards[shardID]
	select {
	case shard.register <- client:
		atomic.AddInt64(&sb.totalClients, 1)
	case <-time.After(100 * time.Millisecond):
			utils.LogWarn("SHARDED_BROADCASTER", "Failed to register client %s (shard %d register channel full)", 
		clientID, shardID)
		client.safeClose()
	}
}

// GetClientCount returns the total number of connected clients across all shards
func (sb *Broadcaster) GetClientCount() int {
	total := int64(0)
	for _, shard := range sb.shards {
		total += atomic.LoadInt64(&shard.clientCount)
	}
	return int(total)
}

// GetType returns the type of broadcaster
func (sb *Broadcaster) GetType() string {
	return "sharded"
}

// GetShardStats returns statistics for all shards with enhanced information
func (sb *Broadcaster) GetShardStats() map[string]interface{} {
	stats := make(map[string]interface{})
	
	totalActiveWorkers := 0
	for i, shard := range sb.shards {
		activeWorkers := 0
		for _, worker := range shard.workers {
			if atomic.LoadInt32(&worker.isActive) == 1 {
				activeWorkers++
			}
		}
		totalActiveWorkers += activeWorkers
		
		shardStats := map[string]interface{}{
			"clients":       atomic.LoadInt64(&shard.clientCount),
			"workers":       len(shard.workers),
			"active_workers": activeWorkers,
		}
		stats[fmt.Sprintf("shard_%d", i)] = shardStats
	}
	
	stats["sharding_enabled"] = true
	stats["type"] = "sharded"
	stats["total_clients"] = sb.GetClientCount()
	stats["total_shards"] = len(sb.shards)
	stats["total_workers"] = len(sb.shards) * sb.config.WorkersPerShard
	stats["total_active_workers"] = totalActiveWorkers
	
	return stats
}

// shutdown gracefully shuts down all shards
func (sb *Broadcaster) shutdown() {
	sb.shutdownOnce.Do(func() {
		utils.LogInfo("SHARDED_BROADCASTER", "Shutting down %d shards", len(sb.shards))
		for _, shard := range sb.shards {
			shard.cancel()
		}
	})
}

// safeClose safely closes a client connection
func (c *Client) safeClose() {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	
	if !c.isClosing {
		c.isClosing = true
		close(c.send)
		c.conn.Close()
	}
}

// writePump pumps messages from the hub to the websocket connection with improved error handling
func (c *Client) writePump(unregister chan<- *Client) {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		ticker.Stop()
		c.safeClose()
	}()

	for {
		select {
		case message, ok := <-c.send:
			// Set write deadline for every message
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			
			if !ok {
				// Channel was closed
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				if !websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					utils.LogDebug("SHARDED_BROADCASTER", "Write error for client %s: %v", c.id, err)
				}
				return
			}

		case <-ticker.C:
			// Send periodic ping
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// readPump pumps messages from the websocket connection with improved error handling
func (c *Client) readPump(unregister chan<- *Client) {
	defer func() {
		// Ensure client gets unregistered
		select {
		case unregister <- c:
		case <-time.After(100 * time.Millisecond):
			// If unregister channel is full, force close
			c.safeClose()
		}
	}()

	c.conn.SetReadLimit(512)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))

	// Set pong handler to update last pong time
	c.conn.SetPongHandler(func(string) error {
		c.lastPong = time.Now()
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			if !websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				utils.LogDebug("SHARDED_BROADCASTER", "Read error for client %s: %v", c.id, err)
			}
			break
		}
		// Update read deadline on any message
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	}
}

// GetID returns the client's ID
func (c *Client) GetID() string {
	return c.id
}

// generateClientID generates a unique client ID
func generateClientID() string {
	bytes := make([]byte, 8)
	rand.Read(bytes)
	return hex.EncodeToString(bytes)
}

// sendInitialChartData sends immediate chart data to a newly connected client
func (sb *Broadcaster) sendInitialChartData(client *Client) {
	defer func() {
		if r := recover(); r != nil {
			utils.LogError("SHARDED_BROADCASTER", "Panic sending initial chart data to client %s: %v", client.id, r)
		}
	}()
	
	utils.LogDebug("SHARDED_BROADCASTER", "Sending initial chart data to client %s", client.id)
	
	// Get chart data with timeout
	chartDataChan := make(chan interface{}, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("SHARDED_BROADCASTER", "Panic fetching initial chart data: %v", r)
			}
		}()
		chartData := sb.statsCollector.GetChartDataForFrontend()
		select {
		case chartDataChan <- chartData:
		default:
		}
	}()
	
	// Send chart data to the specific client
	select {
	case chartData := <-chartDataChan:
		data, err := utils.DefaultMarshalChart(chartData)
		if err != nil {
			utils.LogError("SHARDED_BROADCASTER", "Failed to marshal initial chart data for client %s: %v", client.id, err)
			return
		}
		
		// Send to the specific client with timeout
		select {
		case client.send <- data:
			utils.LogDebug("SHARDED_BROADCASTER", "Sent initial chart data to client %s", client.id)
		case <-time.After(1 * time.Second):
			utils.LogWarn("SHARDED_BROADCASTER", "Timeout sending initial chart data to client %s", client.id)
		}
		
	case <-time.After(2 * time.Second):
		utils.LogWarn("SHARDED_BROADCASTER", "Timeout fetching initial chart data for client %s", client.id)
	}
} 