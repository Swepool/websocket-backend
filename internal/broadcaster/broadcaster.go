package broadcaster

import (
	"context"
	"net/http"
	"sync"
	"time"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/internal/utils"
	"websocket-backend-new/models"
	"websocket-backend-new/internal/stats"
	"github.com/gorilla/websocket"
	"sync/atomic"
)

// Config holds broadcaster configuration
type Config struct {
	MaxClients      int  `json:"maxClients"`      // Maximum clients (default: 1000)
	BufferSize      int  `json:"bufferSize"`      // Buffer size per client (default: 100)
	DropSlowClients bool `json:"dropSlowClients"` // Drop slow clients (default: true)
	UseSharding     bool `json:"useSharding"`     // Enable sharded broadcaster (default: false)
	NumShards       int  `json:"numShards"`       // Number of shards (default: 4)
	WorkersPerShard int  `json:"workersPerShard"` // Workers per shard (default: 4)
}

// DefaultConfig returns default broadcaster configuration
func DefaultConfig() Config {
	return Config{
		MaxClients:      1000,
		BufferSize:      100,
		DropSlowClients: true,
		UseSharding:     false, // Default to original implementation
		NumShards:       4,
		WorkersPerShard: 4,
	}
}

// Client represents a WebSocket client with enhanced connection management
type Client struct {
	id           string
	conn         *websocket.Conn
	send         chan []byte
	lastPong     time.Time
	isClosing    bool
	closeMu      sync.Mutex
}

// StatsCollector interface for getting chart data
type StatsCollector interface {
	GetChartDataForFrontend() interface{}
}

// Broadcaster manages WebSocket clients and broadcasts data with improved reliability
type Broadcaster struct {
	clients         map[*Client]bool
	register        chan *Client
	unregister      chan *Client
	mu              sync.RWMutex
	upgrader        websocket.Upgrader
	config          Config
	channels        *channels.Channels
	statsCollector  *stats.Collector
	healthTicker    *time.Ticker
	shutdownOnce    sync.Once
	chartFetching   int32  // atomic flag to prevent concurrent chart fetches
}

// NewBroadcaster creates a new broadcaster with enhanced client management
func NewBroadcaster(config Config, channels *channels.Channels, statsCollector *stats.Collector) *Broadcaster {
	return &Broadcaster{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client, 100), // Buffered to prevent blocking
		unregister: make(chan *Client, 100), // Buffered for better cleanup
		config:     config,
		channels:   channels,
		statsCollector: statsCollector,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow connections from any origin
			},
			// Add connection timeout settings
			HandshakeTimeout: 45 * time.Second,
		},
	}
}

// Start begins the broadcaster's main loop with optimized performance
func (b *Broadcaster) Start(ctx context.Context) {
	utils.LogInfo("BROADCASTER", "Starting optimized broadcaster with enhanced client management")
	
	// Create tickers for periodic updates and health checks
	chartTicker := time.NewTicker(5 * time.Second)
	b.healthTicker = time.NewTicker(30 * time.Second)
	
	// Add a debug ticker to monitor main loop health
	debugTicker := time.NewTicker(60 * time.Second)
	lastTransferTime := time.Now()
	
	defer func() {
		chartTicker.Stop()
		b.healthTicker.Stop()
		debugTicker.Stop()
		b.cleanup()
	}()
	
	for {
		select {
		case <-ctx.Done():
			utils.LogInfo("BROADCASTER", "Context cancelled, shutting down")
			return
			
		case client := <-b.register:
			b.handleClientRegistration(client)
			
		case client := <-b.unregister:
			b.handleClientUnregistration(client)
			
		case transfer := <-b.channels.TransferBroadcasts:
			lastTransferTime = time.Now()
			b.broadcastTransfer(transfer)
			
		case chartData := <-b.channels.ChartUpdates:
			b.broadcastChartData(chartData)
			
		case <-chartTicker.C:
			b.sendPeriodicChartUpdate()
			
		case <-b.healthTicker.C:
			b.performHealthCheck()
			
		case <-debugTicker.C:
			// Debug: Log broadcaster health with batched logging
			b.mu.RLock()
			clientCount := len(b.clients)
			b.mu.RUnlock()
			
			timeSinceLastTransfer := time.Since(lastTransferTime)
			utils.LogDebug("BROADCASTER", "Debug - Clients: %d, Last transfer: %v ago, Main loop healthy", 
				clientCount, timeSinceLastTransfer)
		}
	}
}

// handleClientRegistration handles new client registration with proper limits and initialization
func (b *Broadcaster) handleClientRegistration(client *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	// Check client limits
	if len(b.clients) >= b.config.MaxClients {
		utils.LogWarn("BROADCASTER", "Max clients reached (%d), rejecting new client %s", b.config.MaxClients, client.id)
		client.safeClose()
		return
	}
	
	b.clients[client] = true
	client.lastPong = time.Now()
	
	utils.LogInfo("BROADCASTER", "Client %s registered, total clients: %d", client.id, len(b.clients))
	
	// Start client's goroutines
	go client.writePump(b.unregister)
	go client.readPump(b.unregister)
	
	// Send initial chart data to new client
	b.sendInitialDataToClient(client)
}

// sendInitialDataToClient sends welcome data to newly connected client with optimized performance
func (b *Broadcaster) sendInitialDataToClient(client *Client) {
	// Run chart data fetching in a separate goroutine to avoid blocking the main loop
	go func() {
		defer func() {
			if r := recover(); r != nil {
				utils.LogError("BROADCASTER", "Panic in sendInitialDataToClient for client %s: %v", client.id, r)
			}
		}()
		
		// Fetch chart data with timeout
		chartDataChan := make(chan interface{}, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					utils.LogError("BROADCASTER", "Panic in chart data fetch for client %s: %v", client.id, r)
				}
			}()
			chartData := b.statsCollector.GetChartDataForFrontend()
			select {
			case chartDataChan <- chartData:
			default:
			}
		}()
		
		var chartData interface{}
		select {
		case chartData = <-chartDataChan:
		case <-time.After(2 * time.Second):
			utils.LogWarn("BROADCASTER", "Timeout fetching chart data for client %s", client.id)
			return
		}
		
		// Use optimized JSON marshaling
		data, err := utils.DefaultMarshalChart(chartData)
		if err != nil {
			utils.LogError("BROADCASTER", "Failed to marshal initial chart data for client %s: %v", client.id, err)
			return
		}
		
		// Use optimized backpressure-protected send
		config := utils.DefaultBackpressureConfig()
		config.TimeoutMs = 100
		config.DropOnOverflow = true
		
		success := utils.SendWithBackpressure(client.send, data, config, nil)
		if success {
			utils.LogDebug("BROADCASTER", "Sent initial chart data to client %s", client.id)
		} else {
			utils.LogWarn("BROADCASTER", "Failed to send initial data to slow client %s, will unregister", client.id)
			// Non-blocking unregister
			select {
			case b.unregister <- client:
			default:
				// If unregister channel is full, force close
				client.safeClose()
			}
		}
	}()
}

// handleClientUnregistration handles client disconnection with proper cleanup
func (b *Broadcaster) handleClientUnregistration(client *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	if _, exists := b.clients[client]; exists {
		delete(b.clients, client)
		client.safeClose()
		utils.LogInfo("BROADCASTER", "Client %s unregistered, remaining clients: %d", client.id, len(b.clients))
	}
}

// getClientSnapshot creates a snapshot of clients with minimal lock time (2.2 optimization)
func (b *Broadcaster) getClientSnapshot() []*Client {
	b.mu.RLock()
	clients := make([]*Client, 0, len(b.clients))
	for client := range b.clients {
		clients = append(clients, client)
	}
	b.mu.RUnlock()
	return clients
}

// buildTransferData creates the transfer data structure without holding locks
func (b *Broadcaster) buildTransferData(transfer models.Transfer) map[string]interface{} {
	return map[string]interface{}{
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
}

// broadcastTransfer broadcasts a single transfer to all connected clients with optimized performance
func (b *Broadcaster) broadcastTransfer(transfer models.Transfer) {
	// Marshal using optimized JSON functions (includes pooling and pre-marshaled content)
	data, err := utils.DefaultMarshalTransfer(transfer)
	if err != nil {
		utils.LogError("BROADCASTER", "Failed to marshal transfer %s: %v", transfer.PacketHash, err)
		return
	}
	
	// Get client snapshot with minimal lock time
	clients := b.getClientSnapshot()
	clientCount := len(clients)
	
	// Always log when we receive a transfer, even if no clients
	if clientCount == 0 {
		utils.LogDebug("BROADCASTER", "Received transfer %s but no clients connected, skipping broadcast", transfer.PacketHash)
		return
	}
	
	utils.LogInfo("BROADCASTER", "Broadcasting transfer %s to %d clients", transfer.PacketHash, clientCount)
	
	// Send to all clients with backpressure protection
	successCount := 0
	for _, client := range clients {
		if b.sendToClientOptimized(client, data) {
			successCount++
		}
	}
	
	if successCount > 0 {
		utils.LogInfo("BROADCASTER", "Successfully sent transfer %s to %d/%d clients", transfer.PacketHash, successCount, clientCount)
	}
}

// sendToClientOptimized sends data to a client with backpressure protection
func (b *Broadcaster) sendToClientOptimized(client *Client, data []byte) bool {
	if client.isClosing {
		return false
	}
	
	// Use optimized backpressure protection
	config := utils.DefaultBackpressureConfig()
	config.DropOnOverflow = b.config.DropSlowClients
	config.TimeoutMs = 50 // Quick timeout for client sends
	
	success := utils.SendWithBackpressure(client.send, data, config, nil)
	
	// If failed and we drop slow clients, schedule disconnect
	if !success && b.config.DropSlowClients {
		utils.LogDebug("BROADCASTER", "Client %s send buffer full, scheduling disconnect", client.id)
		go func() { 
			select {
			case b.unregister <- client:
			default:
				// If unregister channel is full, force close
				client.safeClose()
			}
		}()
	}
	
	return success
}

// sendToClient is kept for backward compatibility but uses optimized version
func (b *Broadcaster) sendToClient(client *Client, data []byte) bool {
	return b.sendToClientOptimized(client, data)
}

// broadcastChartData broadcasts chart data to all connected clients with optimized performance
func (b *Broadcaster) broadcastChartData(rawData interface{}) {
	if rawData == nil {
		utils.LogWarn("BROADCASTER", "Attempted to broadcast nil chart data")
		return
	}
	
	// Marshal using optimized JSON functions
	data, err := utils.DefaultMarshalChart(rawData)
	if err != nil {
		utils.LogError("BROADCASTER", "Failed to marshal chart data: %v", err)
		return
	}
	
	// Get client snapshot with minimal lock time
	clients := b.getClientSnapshot()
	clientCount := len(clients)
	
	if clientCount == 0 {
		utils.LogDebug("BROADCASTER", "No clients connected for chart data broadcast")
		return
	}
	
	utils.LogInfo("BROADCASTER", "Broadcasting chart data to %d clients (size: %d bytes)", clientCount, len(data))
	
	// Send to all clients with backpressure protection
	successCount := 0
	for _, client := range clients {
		if b.sendToClientOptimized(client, data) {
			successCount++
		}
	}
	
	if successCount > 0 {
		utils.LogInfo("BROADCASTER", "Successfully sent chart data to %d/%d clients", successCount, clientCount)
	} else {
		utils.LogWarn("BROADCASTER", "Failed to send chart data to any clients")
	}
}

// sendPeriodicChartUpdate sends periodic chart updates
func (b *Broadcaster) sendPeriodicChartUpdate() {
	b.mu.RLock()
	clientCount := len(b.clients)
	b.mu.RUnlock()
	
	if clientCount > 0 {
		// Check if we're already fetching chart data to prevent concurrent fetches
		if !atomic.CompareAndSwapInt32(&b.chartFetching, 0, 1) {
			utils.LogDebug("BROADCASTER", "Chart data fetch already in progress, skipping")
			return
		}
		
		// Run chart data fetching in a separate goroutine to avoid blocking the main loop
		go func() {
			defer func() {
				atomic.StoreInt32(&b.chartFetching, 0) // Reset the flag
				if r := recover(); r != nil {
					utils.LogError("BROADCASTER", "Panic in sendPeriodicChartUpdate: %v", r)
				}
			}()
			
			utils.LogDebug("BROADCASTER", "Fetching periodic chart data for %d clients", clientCount)
			
			// Fetch chart data with timeout
			chartDataChan := make(chan interface{}, 1)
			go func() {
				defer func() {
					if r := recover(); r != nil {
						utils.LogError("BROADCASTER", "Panic in periodic chart data fetch: %v", r)
					}
				}()
				chartData := b.statsCollector.GetChartDataForFrontend()
				select {
				case chartDataChan <- chartData:
				default:
				}
			}()
			
			var chartData interface{}
			select {
			case chartData = <-chartDataChan:
				utils.LogDebug("BROADCASTER", "Successfully fetched chart data")
				b.broadcastChartData(chartData)
			case <-time.After(3 * time.Second):
				utils.LogWarn("BROADCASTER", "Timeout fetching periodic chart data")
			}
		}()
	}
}

// performHealthCheck checks client health and removes stale connections
func (b *Broadcaster) performHealthCheck() {
	b.mu.RLock()
	clients := make([]*Client, 0, len(b.clients))
	for client := range b.clients {
		clients = append(clients, client)
	}
	b.mu.RUnlock()
	
	now := time.Now()
	staleClients := 0
	
	for _, client := range clients {
		// Check if client hasn't responded to pings for too long (2 minutes)
		if now.Sub(client.lastPong) > 2*time.Minute {
			utils.LogInfo("BROADCASTER", "Client %s appears stale (last pong: %v ago), removing", 
				client.id, now.Sub(client.lastPong))
			go func(c *Client) { b.unregister <- c }(client)
			staleClients++
		}
	}
	
	if staleClients > 0 {
		utils.LogInfo("BROADCASTER", "Health check removed %d stale clients", staleClients)
	}
}

// UpgradeConnection upgrades HTTP connection to WebSocket with optimized settings
func (b *Broadcaster) UpgradeConnection(w http.ResponseWriter, r *http.Request) {
	conn, err := b.upgrader.Upgrade(w, r, nil)
	if err != nil {
		utils.LogError("BROADCASTER", "Failed to upgrade connection: %v", err)
		return
	}
	
	// Configure connection settings
	conn.SetReadLimit(512)
	
	// Use optimized buffer size from configuration or defaults
	bufferSize := utils.DefaultGetChannelBufferSize("ClientSend", b.config.BufferSize)
	
	client := &Client{
		id:       generateClientID(),
		conn:     conn,
		send:     make(chan []byte, bufferSize),
		lastPong: time.Now(),
		isClosing: false,
	}
	
	utils.LogInfo("BROADCASTER", "New WebSocket connection from %s, assigned ID: %s", 
		r.RemoteAddr, client.id)
	
	// Register client with backpressure protection
	config := utils.DefaultBackpressureConfig()
	config.TimeoutMs = 100
	config.DropOnOverflow = true
	
	success := utils.SendWithBackpressure(b.register, client, config, nil)
	if !success {
		utils.LogWarn("BROADCASTER", "Failed to register client %s (register channel full)", client.id)
		client.safeClose()
	}
}

// GetClientCount returns the current number of connected clients
func (b *Broadcaster) GetClientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}

// GetType returns the type of broadcaster
func (b *Broadcaster) GetType() string {
	return "standard"
}

// GetShardStats returns shard statistics (empty for standard broadcaster)
func (b *Broadcaster) GetShardStats() map[string]interface{} {
	return map[string]interface{}{
		"sharding_enabled": false,
		"type": "standard",
		"total_clients": b.GetClientCount(),
	}
}

// cleanup performs final cleanup when broadcaster shuts down
func (b *Broadcaster) cleanup() {
	b.shutdownOnce.Do(func() {
		utils.LogInfo("BROADCASTER", "Performing cleanup")
		
		b.mu.Lock()
		for client := range b.clients {
			client.safeClose()
		}
		b.clients = make(map[*Client]bool)
		b.mu.Unlock()
		
		// Drain channels
		go func() {
			for {
				select {
				case <-b.register:
				case <-b.unregister:
				default:
					return
				}
			}
		}()
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
					utils.LogDebug("BROADCASTER", "Write error for client %s: %v", c.id, err)
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
				utils.LogDebug("BROADCASTER", "Read error for client %s: %v", c.id, err)
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
	return utils.DefaultGenerateID("client")
} 