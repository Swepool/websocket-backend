package broadcaster

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/models"
	"websocket-backend-new/internal/stats"
	"github.com/gorilla/websocket"
)

// ShardedConfig holds sharded broadcaster configuration
type ShardedConfig struct {
	MaxClients      int  `json:"maxClients"`      // Maximum clients per shard (default: 1000)
	BufferSize      int  `json:"bufferSize"`      // Buffer size per client (default: 100)
	DropSlowClients bool `json:"dropSlowClients"` // Drop slow clients (default: true)
	NumShards       int  `json:"numShards"`       // Number of shards (default: 4)
	WorkersPerShard int  `json:"workersPerShard"` // Workers per shard (default: 4)
}

// DefaultShardedConfig returns default sharded broadcaster configuration
func DefaultShardedConfig() ShardedConfig {
	return ShardedConfig{
		MaxClients:      1000,
		BufferSize:      100,
		DropSlowClients: true,
		NumShards:       4,
		WorkersPerShard: 4,
	}
}

// ShardedClient represents a WebSocket client with shard information
type ShardedClient struct {
	id      string
	conn    *websocket.Conn
	send    chan []byte
	shardID int
}

// Shard represents a shard of clients with its own worker pool
type Shard struct {
	id              int
	clients         map[*ShardedClient]bool
	register        chan *ShardedClient
	unregister      chan *ShardedClient
	broadcast       chan []byte
	mu              sync.RWMutex
	config          ShardedConfig
	workerPool      chan chan []byte
	workers         []*ShardWorker
	ctx             context.Context
	cancel          context.CancelFunc
	clientCount     int64
}

// ShardWorker represents a worker that handles broadcasts for a shard
type ShardWorker struct {
	id        int
	shardID   int
	work      chan []byte
	workerPool chan chan []byte
	quit      chan bool
	shard     *Shard
}

// ShardedBroadcaster manages multiple shards and distributes clients across them
type ShardedBroadcaster struct {
	shards          []*Shard
	upgrader        websocket.Upgrader
	config          ShardedConfig
	channels        *channels.Channels
	statsCollector  *stats.Collector
	totalClients    int64
	mu              sync.RWMutex
}

// NewShardedBroadcaster creates a new sharded broadcaster
func NewShardedBroadcaster(config ShardedConfig, channels *channels.Channels, statsCollector *stats.Collector) *ShardedBroadcaster {
	sb := &ShardedBroadcaster{
		shards:         make([]*Shard, config.NumShards),
		config:         config,
		channels:       channels,
		statsCollector: statsCollector,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow connections from any origin
			},
		},
	}

	// Initialize shards
	for i := 0; i < config.NumShards; i++ {
		sb.shards[i] = newShard(i, config)
	}

	return sb
}

// newShard creates a new shard with worker pool
func newShard(id int, config ShardedConfig) *Shard {
	ctx, cancel := context.WithCancel(context.Background())
	
	shard := &Shard{
		id:         id,
		clients:    make(map[*ShardedClient]bool),
		register:   make(chan *ShardedClient, 100),
		unregister: make(chan *ShardedClient, 100),
		broadcast:  make(chan []byte, 1000),
		config:     config,
		workerPool: make(chan chan []byte, config.WorkersPerShard),
		workers:    make([]*ShardWorker, config.WorkersPerShard),
		ctx:        ctx,
		cancel:     cancel,
	}

	// Initialize workers for this shard
	for i := 0; i < config.WorkersPerShard; i++ {
		worker := &ShardWorker{
			id:         i,
			shardID:    id,
			work:       make(chan []byte, 100),
			workerPool: shard.workerPool,
			quit:       make(chan bool),
			shard:      shard,
		}
		shard.workers[i] = worker
	}

	return shard
}

// Start begins all shards and their worker pools
func (sb *ShardedBroadcaster) Start(ctx context.Context) {
	fmt.Printf("[SHARDED_BROADCASTER] Starting with %d shards, %d workers per shard\n", 
		sb.config.NumShards, sb.config.WorkersPerShard)

	// Start all shards
	for i, shard := range sb.shards {
		go shard.start()
		fmt.Printf("[SHARDED_BROADCASTER] Started shard %d with %d workers\n", i, sb.config.WorkersPerShard)
	}

	// Create a ticker for periodic chart updates
	chartTicker := time.NewTicker(5 * time.Second)
	defer chartTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			sb.shutdown()
			return

		case transfer := <-sb.channels.TransferBroadcasts:
			fmt.Printf("[SHARDED_BROADCASTER] Broadcasting transfer %s to %d shards\n", 
				transfer.PacketHash, len(sb.shards))
			sb.broadcastTransfer(transfer)

		case chartData := <-sb.channels.ChartUpdates:
			sb.broadcastChartData(chartData)

		case <-chartTicker.C:
			// Periodically send updated chart data to all clients
			if sb.GetClientCount() > 0 {
				chartData := sb.statsCollector.GetChartDataForFrontend()
				sb.broadcastChartData(chartData)
				fmt.Printf("[SHARDED_BROADCASTER] Sent periodic chart update to %d total clients\n", 
					sb.GetClientCount())
			}
		}
	}
}

// start begins a shard's main loop and worker pool
func (s *Shard) start() {
	// Start all workers
	for _, worker := range s.workers {
		go worker.start()
	}

	for {
		select {
		case <-s.ctx.Done():
			// Shutdown workers
			for _, worker := range s.workers {
				worker.stop()
			}
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

// start begins a worker's processing loop
func (w *ShardWorker) start() {
	go func() {
		for {
			// Add worker to pool
			w.workerPool <- w.work

			select {
			case data := <-w.work:
				w.handleBroadcast(data)
			case <-w.quit:
				return
			}
		}
	}()
}

// stop stops a worker
func (w *ShardWorker) stop() {
	w.quit <- true
}

// handleBroadcast processes a broadcast for this worker's clients
func (w *ShardWorker) handleBroadcast(data []byte) {
	w.shard.mu.RLock()
	clients := make([]*ShardedClient, 0, len(w.shard.clients))
	for client := range w.shard.clients {
		clients = append(clients, client)
	}
	w.shard.mu.RUnlock()

	// Distribute work across workers by client hash
	workersCount := len(w.shard.workers)
	successCount := 0
	
	for _, client := range clients {
		// Check if this worker should handle this client
		clientWorkerID := int(hash(client.id)) % workersCount
		if clientWorkerID != w.id {
			continue
		}

		select {
		case client.send <- data:
			successCount++
		default:
			// Client's send channel is full, unregister
			select {
			case w.shard.unregister <- client:
			default:
			}
		}
	}

	if successCount > 0 {
		fmt.Printf("[WORKER %d-%d] Sent data to %d clients\n", w.shardID, w.id, successCount)
	}
}

// dispatchToWorkers sends broadcast data to available workers
func (s *Shard) dispatchToWorkers(data []byte) {
	clientCount := atomic.LoadInt64(&s.clientCount)
	if clientCount == 0 {
		return
	}

	// Try to get an available worker
	select {
	case work := <-s.workerPool:
		select {
		case work <- data:
		default:
			// Worker is busy, put it back and try another approach
			go func() { s.workerPool <- work }()
			// Fallback: broadcast directly from shard
			s.directBroadcast(data)
		}
	default:
		// No workers available, broadcast directly from shard
		s.directBroadcast(data)
	}
}

// directBroadcast sends data directly to all clients in shard (fallback)
func (s *Shard) directBroadcast(data []byte) {
	s.mu.RLock()
	clients := make([]*ShardedClient, 0, len(s.clients))
	for client := range s.clients {
		clients = append(clients, client)
	}
	s.mu.RUnlock()

	successCount := 0
	for _, client := range clients {
		select {
		case client.send <- data:
			successCount++
		default:
			// Client's send channel is full, unregister
			select {
			case s.unregister <- client:
			default:
			}
		}
	}

	if successCount > 0 {
		fmt.Printf("[SHARD %d] Direct broadcast to %d clients\n", s.id, successCount)
	}
}

// handleClientRegistration handles new client registration in shard
func (s *Shard) handleClientRegistration(client *ShardedClient) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check client limits for this shard
	if len(s.clients) >= s.config.MaxClients {
		close(client.send)
		return
	}

	s.clients[client] = true
	atomic.AddInt64(&s.clientCount, 1)

	// Start client's send goroutine
	go client.writePump()

	fmt.Printf("[SHARD %d] Client registered, total: %d\n", s.id, len(s.clients))
}

// handleClientUnregistration handles client disconnection in shard
func (s *Shard) handleClientUnregistration(client *ShardedClient) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.clients[client]; ok {
		delete(s.clients, client)
		close(client.send)
		atomic.AddInt64(&s.clientCount, -1)
		fmt.Printf("[SHARD %d] Client unregistered, total: %d\n", s.id, len(s.clients))
	}
}

// getShardForClient determines which shard a client should belong to
func (sb *ShardedBroadcaster) getShardForClient(clientID string) int {
	return int(hash(clientID)) % sb.config.NumShards
}

// hash function for consistent client distribution
func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// broadcastTransfer broadcasts a single transfer to all shards
func (sb *ShardedBroadcaster) broadcastTransfer(transfer models.Transfer) {
	// Convert to frontend format (same as original)
	enhancedTransfer := map[string]interface{}{
		"source_chain": map[string]interface{}{
			"universal_chain_id": transfer.SourceChain.UniversalChainID,
			"display_name":       transfer.SourceChain.DisplayName,
			"chain_id":           transfer.SourceChain.ChainID,
			"testnet":            transfer.SourceChain.Testnet,
			"rpc_type":           transfer.SourceChain.RpcType,
			"addr_prefix":        transfer.SourceChain.AddrPrefix,
		},
		"destination_chain": map[string]interface{}{
			"universal_chain_id": transfer.DestinationChain.UniversalChainID,
			"display_name":       transfer.DestinationChain.DisplayName,
			"chain_id":           transfer.DestinationChain.ChainID,
			"testnet":            transfer.DestinationChain.Testnet,
			"rpc_type":           transfer.DestinationChain.RpcType,
			"addr_prefix":        transfer.DestinationChain.AddrPrefix,
		},
		"packet_hash":            transfer.PacketHash,
		"isTestnetTransfer":      transfer.IsTestnetTransfer,
		"sourceDisplayName":      transfer.SourceDisplayName,
		"destinationDisplayName": transfer.DestinationDisplayName,
		"formattedTimestamp":     transfer.FormattedTimestamp,
		"routeKey":               transfer.RouteKey,
		"senderDisplay":          transfer.SenderDisplay,
		"receiverDisplay":        transfer.ReceiverDisplay,
	}

	data, err := json.Marshal(map[string]interface{}{
		"type": "transfers",
		"data": []map[string]interface{}{enhancedTransfer},
	})
	if err != nil {
		return
	}

	// Broadcast to all shards
	for _, shard := range sb.shards {
		select {
		case shard.broadcast <- data:
		default:
			fmt.Printf("[SHARDED_BROADCASTER] Warning: shard %d broadcast channel full\n", shard.id)
		}
	}

	totalClients := sb.GetClientCount()
	if totalClients > 0 {
		fmt.Printf("[SHARDED_BROADCASTER] Sent transfer %s to %d total clients across %d shards\n", 
			transfer.PacketHash, totalClients, len(sb.shards))
	}
}

// broadcastChartData broadcasts chart data to all shards
func (sb *ShardedBroadcaster) broadcastChartData(rawData interface{}) {
	data, err := json.Marshal(map[string]interface{}{
		"type": "chartData",
		"data": rawData,
	})
	if err != nil {
		return
	}

	// Broadcast to all shards
	for _, shard := range sb.shards {
		select {
		case shard.broadcast <- data:
		default:
			fmt.Printf("[SHARDED_BROADCASTER] Warning: shard %d chart broadcast channel full\n", shard.id)
		}
	}
}

// UpgradeConnection upgrades HTTP connection to WebSocket and assigns to appropriate shard
func (sb *ShardedBroadcaster) UpgradeConnection(w http.ResponseWriter, r *http.Request) {
	conn, err := sb.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}

	clientID := generateClientID()
	shardID := sb.getShardForClient(clientID)

	client := &ShardedClient{
		id:      clientID,
		conn:    conn,
		send:    make(chan []byte, sb.config.BufferSize),
		shardID: shardID,
	}

	// Register client with appropriate shard
	shard := sb.shards[shardID]
	shard.register <- client

	// Start read pump for this client
	go client.readPump(shard.unregister)

	atomic.AddInt64(&sb.totalClients, 1)
	fmt.Printf("[SHARDED_BROADCASTER] New client %s assigned to shard %d, total clients: %d\n", 
		clientID, shardID, atomic.LoadInt64(&sb.totalClients))
}

// GetClientCount returns the total number of connected clients across all shards
func (sb *ShardedBroadcaster) GetClientCount() int {
	total := int64(0)
	for _, shard := range sb.shards {
		total += atomic.LoadInt64(&shard.clientCount)
	}
	return int(total)
}

// GetType returns the type of broadcaster
func (sb *ShardedBroadcaster) GetType() string {
	return "sharded"
}

// GetShardStats returns statistics for all shards
func (sb *ShardedBroadcaster) GetShardStats() map[string]interface{} {
	stats := make(map[string]interface{})
	
	for i, shard := range sb.shards {
		shardStats := map[string]interface{}{
			"clients": atomic.LoadInt64(&shard.clientCount),
			"workers": len(shard.workers),
		}
		stats[fmt.Sprintf("shard_%d", i)] = shardStats
	}
	
	stats["sharding_enabled"] = true
	stats["type"] = "sharded"
	stats["total_clients"] = sb.GetClientCount()
	stats["total_shards"] = len(sb.shards)
	
	return stats
}

// shutdown gracefully shuts down all shards
func (sb *ShardedBroadcaster) shutdown() {
	fmt.Printf("[SHARDED_BROADCASTER] Shutting down %d shards\n", len(sb.shards))
	for _, shard := range sb.shards {
		shard.cancel()
	}
}

// writePump pumps messages from the hub to the websocket connection
func (c *ShardedClient) writePump() {
	ticker := time.NewTicker(54 * time.Second)
	defer func() {
		ticker.Stop()
		c.conn.Close()
	}()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				return
			}

		case <-ticker.C:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// readPump pumps messages from the websocket connection
func (c *ShardedClient) readPump(unregister chan<- *ShardedClient) {
	defer func() {
		unregister <- c
		c.conn.Close()
	}()

	c.conn.SetReadLimit(512)
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, _, err := c.conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

// GetID returns the client's ID
func (c *ShardedClient) GetID() string {
	return c.id
} 