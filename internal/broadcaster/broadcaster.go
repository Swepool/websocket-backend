package broadcaster

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/models"
	"websocket-backend-new/internal/stats"
	"github.com/gorilla/websocket"
)

// Config holds broadcaster configuration
type Config struct {
	MaxClients    int  `json:"maxClients"`    // Maximum clients (default: 1000)
	BufferSize    int  `json:"bufferSize"`    // Buffer size per client (default: 100)
	DropSlowClients bool `json:"dropSlowClients"` // Drop slow clients (default: true)
}

// DefaultConfig returns default broadcaster configuration
func DefaultConfig() Config {
	return Config{
		MaxClients:      1000,
		BufferSize:      100,
		DropSlowClients: true,
	}
}

// Client represents a WebSocket client
type Client struct {
	id   string
	conn *websocket.Conn
	send chan []byte
}

// StatsCollector interface for getting chart data
type StatsCollector interface {
	GetChartDataForFrontend() interface{}
}

// Broadcaster manages WebSocket clients and broadcasts data
type Broadcaster struct {
	clients         map[*Client]bool
	register        chan *Client
	unregister      chan *Client
	mu              sync.RWMutex
	upgrader        websocket.Upgrader
	config          Config
	channels        *channels.Channels
	statsCollector  *stats.Collector
}

// NewBroadcaster creates a new broadcaster
func NewBroadcaster(config Config, channels *channels.Channels, statsCollector *stats.Collector) *Broadcaster {
	return &Broadcaster{
		clients:    make(map[*Client]bool),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		config:     config,
		channels:   channels,
		statsCollector: statsCollector,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow connections from any origin
			},
		},
	}
}

// Start begins the broadcaster's main loop
func (b *Broadcaster) Start(ctx context.Context) {
	// Create a ticker for periodic chart updates
	chartTicker := time.NewTicker(5 * time.Second)
	defer chartTicker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
			
		case client := <-b.register:
			b.handleClientRegistration(client)
			
		case client := <-b.unregister:
			b.handleClientUnregistration(client)
			
		case transfer := <-b.channels.TransferBroadcasts:
			fmt.Printf("[BROADCASTER] Received transfer %s, broadcasting immediately to %d clients\n", transfer.PacketHash, len(b.clients))
			b.broadcastTransfer(transfer)
			
		case chartData := <-b.channels.ChartUpdates:
			b.broadcastChartData(chartData)
			
		case <-chartTicker.C:
			// Periodically send updated chart data to all clients
			if len(b.clients) > 0 {
				chartData := b.statsCollector.GetChartDataForFrontend()
				b.broadcastChartData(chartData)
				fmt.Printf("[BROADCASTER] Sent periodic chart update to %d clients\n", len(b.clients))
			}
		}
	}
}

// handleClientRegistration handles new client registration
func (b *Broadcaster) handleClientRegistration(client *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	// Check client limits
	if len(b.clients) >= b.config.MaxClients {
		close(client.send)
		return
	}
	
	b.clients[client] = true
	
	// Start client's send goroutine
	go client.writePump()
	
	// Send initial chart data to new client
	chartData := b.statsCollector.GetChartDataForFrontend()
	data, err := json.Marshal(map[string]interface{}{
		"type": "chartData",
		"data": chartData,
	})
	if err != nil {
		return
	}
	
	select {
	case client.send <- data:
	default:
		close(client.send)
		delete(b.clients, client)
	}
}

// handleClientUnregistration handles client disconnection
func (b *Broadcaster) handleClientUnregistration(client *Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	if _, ok := b.clients[client]; ok {
		delete(b.clients, client)
		close(client.send)
	}
}

// broadcastTransfer broadcasts a single transfer to all connected clients (in array format)
func (b *Broadcaster) broadcastTransfer(transfer models.Transfer) {
	// Convert to frontend-expected TransferListItem + enhanced format
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
		"packet_hash": transfer.PacketHash,
		// Enhanced fields that frontend expects
		"isTestnetTransfer":      transfer.IsTestnetTransfer,
		"sourceDisplayName":      transfer.SourceDisplayName,
		"destinationDisplayName": transfer.DestinationDisplayName,
		"formattedTimestamp":     transfer.FormattedTimestamp,
		"routeKey":               transfer.RouteKey,
		"senderDisplay":          transfer.SenderDisplay,
		"receiverDisplay":        transfer.ReceiverDisplay,
	}
	
	// Send as array with single transfer (frontend expects arrays)
	data, err := json.Marshal(map[string]interface{}{
		"type": "transfers",
		"data": []map[string]interface{}{enhancedTransfer},
	})
	if err != nil {
		return
	}
	
	b.mu.RLock()
	clients := make([]*Client, 0, len(b.clients))
	for client := range b.clients {
		clients = append(clients, client)
	}
	b.mu.RUnlock()
	
	// Send to all clients asynchronously
	for _, client := range clients {
		go func(c *Client) {
			select {
			case c.send <- data:
			default:
				// Client's send channel is full
				b.unregister <- c
			}
		}(client)
	}
	
	if len(clients) > 0 {
		fmt.Printf("[BROADCASTER] Sent transfer %s to %d clients\n", transfer.PacketHash, len(clients))
	}
}

// broadcastChartData broadcasts chart data to all connected clients
func (b *Broadcaster) broadcastChartData(rawData interface{}) {
	data, err := json.Marshal(map[string]interface{}{
		"type": "chartData",
		"data": rawData,
	})
	if err != nil {
		return
	}
	
	b.mu.RLock()
	clients := make([]*Client, 0, len(b.clients))
	for client := range b.clients {
		clients = append(clients, client)
	}
	b.mu.RUnlock()
	
	// Send to all clients asynchronously
	for _, client := range clients {
		go func(c *Client) {
			select {
			case c.send <- data:
			default:
				// Client's send channel is full
				b.unregister <- c
			}
		}(client)
	}
}

// broadcastStats broadcasts stats data to all connected clients
func (b *Broadcaster) broadcastStats(stats interface{}) {
	data, err := json.Marshal(map[string]interface{}{
		"type": "stats_update",
		"data": stats,
	})
	if err != nil {
		return
	}
	
	b.mu.RLock()
	clients := make([]*Client, 0, len(b.clients))
	for client := range b.clients {
		clients = append(clients, client)
	}
	b.mu.RUnlock()
	
	// Send to all clients
	for _, client := range clients {
		select {
		case client.send <- data:
		default:
			close(client.send)
			delete(b.clients, client)
		}
	}
}

// UpgradeConnection upgrades HTTP connection to WebSocket
func (b *Broadcaster) UpgradeConnection(w http.ResponseWriter, r *http.Request) {
	conn, err := b.upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	
	client := &Client{
		id:   generateClientID(),
		conn: conn,
		send: make(chan []byte, 256),
	}
	
	b.register <- client
	
	// Start read pump for this client
	go client.readPump(b.unregister)
}

// GetClientCount returns the current number of connected clients
func (b *Broadcaster) GetClientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}

// writePump pumps messages from the hub to the websocket connection
func (c *Client) writePump() {
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

// readPump pumps messages from the websocket connection to the hub
func (c *Client) readPump(unregister chan<- *Client) {
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
func (c *Client) GetID() string {
	return c.id
}

// generateClientID generates a unique client ID
func generateClientID() string {
	return fmt.Sprintf("client_%d", time.Now().UnixNano())
} 