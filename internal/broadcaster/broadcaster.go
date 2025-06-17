package broadcaster

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/internal/models"
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

// Client interface for WebSocket clients
type Client interface {
	Send(data []byte) error
	GetID() string
	Close() error
}

// StatsCollector interface for getting chart data
type StatsCollector interface {
	GetChartDataForFrontend() interface{}
}

// Broadcaster handles WebSocket broadcasting
type Broadcaster struct {
	config         Config
	channels       *channels.Channels
	clients        map[Client]bool
	mu             sync.RWMutex
	statsCollector StatsCollector
}

// NewBroadcaster creates a new broadcaster
func NewBroadcaster(config Config, channels *channels.Channels, statsCollector StatsCollector) *Broadcaster {
	return &Broadcaster{
		config:         config,
		channels:       channels,
		clients:        make(map[Client]bool),
		statsCollector: statsCollector,
	}
}

// Start begins the broadcaster thread
func (b *Broadcaster) Start(ctx context.Context) {
	fmt.Printf("[BROADCASTER] Starting with max clients: %d, buffer size: %d\n", 
		b.config.MaxClients, b.config.BufferSize)
	
	// Start chart data broadcasting every 15 seconds
	chartTicker := time.NewTicker(15 * time.Second)
	defer chartTicker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[BROADCASTER] Shutting down\n")
			return
			
		case transfer := <-b.channels.TransferBroadcasts:
			b.broadcastTransfer(transfer)
			
		case <-chartTicker.C:
			b.broadcastChartData()
		}
	}
}

// AddClient adds a new WebSocket client
func (b *Broadcaster) AddClient(client Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	if len(b.clients) >= b.config.MaxClients {
		fmt.Printf("[BROADCASTER] Max clients reached, rejecting new client\n")
		client.Close()
		return
	}
	
	b.clients[client] = true
	fmt.Printf("[BROADCASTER] Added client %s (total: %d)\n", client.GetID(), len(b.clients))
	
	// Send initial chart data to new client
	go b.sendInitialDataToClient(client)
}

// sendInitialDataToClient sends initial chart data to a newly connected client
func (b *Broadcaster) sendInitialDataToClient(client Client) {
	if b.statsCollector == nil {
		return
	}
	
	// Send initial chart data
	frontendData := b.statsCollector.GetChartDataForFrontend()
	
	// Wrap chart data in "data" field to match frontend expectations
	message := map[string]interface{}{
		"type": "chartData",
		"data": frontendData,
	}
	
	jsonData, err := json.Marshal(message)
	if err != nil {
		fmt.Printf("[BROADCASTER] Error marshaling initial chart data: %v\n", err)
		return
	}
	
	if err := client.Send(jsonData); err != nil {
		fmt.Printf("[BROADCASTER] Error sending initial data to client %s: %v\n", client.GetID(), err)
	} else {
		fmt.Printf("[BROADCASTER] Sent initial chart data to client %s\n", client.GetID())
	}
}

// RemoveClient removes a WebSocket client
func (b *Broadcaster) RemoveClient(client Client) {
	b.mu.Lock()
	defer b.mu.Unlock()
	
	if _, exists := b.clients[client]; exists {
		delete(b.clients, client)
		fmt.Printf("[BROADCASTER] Removed client %s (total: %d)\n", client.GetID(), len(b.clients))
	}
}

// broadcastTransfer broadcasts a single transfer to all clients
func (b *Broadcaster) broadcastTransfer(transfer models.Transfer) {
	b.mu.RLock()
	clientCount := len(b.clients)
	b.mu.RUnlock()

	if clientCount == 0 {
		return // No clients to broadcast to
	}

	// Convert to BroadcastTransfer format (includes isTestnetTransfer field)
	broadcastTransfer := convertToBroadcastTransfer(transfer)

	// Create message matching frontend expectations (with timestamp like old system)
	message := map[string]interface{}{
		"type":      "transfers",
		"data":      []interface{}{broadcastTransfer}, // Single transfer in array as expected
		"timestamp": time.Now().Unix(),
	}
	
	data, err := json.Marshal(message)
	if err != nil {
		fmt.Printf("[BROADCASTER] Error marshaling transfer: %v\n", err)
		return
	}
	
	// Broadcast to all clients
	b.mu.RLock()
	clients := make([]Client, 0, len(b.clients))
	for client := range b.clients {
		clients = append(clients, client)
	}
	b.mu.RUnlock()
	
	// Send asynchronously to all clients (fire-and-forget)
	for _, client := range clients {
		go func(c Client) {
			if err := c.Send(data); err != nil {
				if b.config.DropSlowClients {
					b.RemoveClient(c)
				}
			}
		}(client)
	}
	
	fmt.Printf("[BROADCASTER] Broadcasting transfer %s to %d clients (async)\n", 
		transfer.PacketHash, clientCount)
}

// GetClientCount returns the current number of connected clients
func (b *Broadcaster) GetClientCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.clients)
}

// broadcastChartData broadcasts chart data to all clients every 15 seconds
func (b *Broadcaster) broadcastChartData() {
	b.mu.RLock()
	clientCount := len(b.clients)
	b.mu.RUnlock()
	
	if clientCount == 0 {
		return
	}
	
	if b.statsCollector == nil {
		return
	}
	
	// Get chart data formatted for frontend
	frontendData := b.statsCollector.GetChartDataForFrontend()
	
	// Wrap chart data in "data" field to match frontend expectations
	message := map[string]interface{}{
		"type": "chartData",
		"data": frontendData,
	}
	
	jsonData, err := json.Marshal(message)
	if err != nil {
		fmt.Printf("[BROADCASTER] Error marshaling chart data: %v\n", err)
		return
	}
	
	// Broadcast to all clients asynchronously
	b.mu.RLock()
	clients := make([]Client, 0, len(b.clients))
	for client := range b.clients {
		clients = append(clients, client)
	}
	b.mu.RUnlock()
	
	// Send asynchronously to all clients (fire-and-forget)
	for _, client := range clients {
		go func(c Client) {
			if err := c.Send(jsonData); err != nil {
				if b.config.DropSlowClients {
					b.RemoveClient(c)
				}
			}
		}(client)
	}
	
	fmt.Printf("[BROADCASTER] Broadcasting chart data to %d clients (async)\n", len(clients))
}

// transformChartDataForFrontend converts our chart data to match frontend expectations
func (b *Broadcaster) transformChartDataForFrontend(data interface{}) interface{} {
	// For now, return as-is, but we can transform here to match frontend structure
	// The frontend expects currentRates, activeWalletRates, etc.
	return data
}

// BroadcastStats broadcasts stats data to all clients
func (b *Broadcaster) BroadcastStats(data interface{}) {
	b.mu.RLock()
	clientCount := len(b.clients)
	b.mu.RUnlock()
	
	if clientCount == 0 {
		return
	}
	
	message := map[string]interface{}{
		"type": "stats",
		"data": data,
	}
	
	jsonData, err := json.Marshal(message)
	if err != nil {
		fmt.Printf("[BROADCASTER] Error marshaling stats: %v\n", err)
		return
	}
	
	// Broadcast to all clients (simplified)
	b.mu.RLock()
	for client := range b.clients {
		go func(c Client) {
			if err := c.Send(jsonData); err != nil {
				if b.config.DropSlowClients {
					b.RemoveClient(c)
				}
			}
		}(client)
	}
	b.mu.RUnlock()
}

// convertToBroadcastTransfer converts internal models.Transfer to broadcast format
func convertToBroadcastTransfer(transfer models.Transfer) map[string]interface{} {
	// Determine if it's a testnet transfer
	isTestnet := transfer.SourceChain.Testnet || transfer.DestinationChain.Testnet
	
	// Format timestamp as string
	formattedTimestamp := transfer.TransferSendTimestamp.Format("2006-01-02 15:04:05")
	
	// Create route key
	routeKey := transfer.SourceChain.UniversalChainID + "_" + transfer.DestinationChain.UniversalChainID
	
	// Use SenderDisplay and ReceiverDisplay if available, otherwise use canonical
	senderDisplay := transfer.SenderDisplay
	if senderDisplay == "" {
		senderDisplay = transfer.SenderCanonical
	}
	
	receiverDisplay := transfer.ReceiverDisplay
	if receiverDisplay == "" {
		receiverDisplay = transfer.ReceiverCanonical
	}
	
	return map[string]interface{}{
		"source_chain": map[string]interface{}{
			"universal_chain_id": transfer.SourceChain.UniversalChainID,
			"display_name":       transfer.SourceChain.DisplayName,
		},
		"destination_chain": map[string]interface{}{
			"universal_chain_id": transfer.DestinationChain.UniversalChainID,
			"display_name":       transfer.DestinationChain.DisplayName,
		},
		"packet_hash":                transfer.PacketHash,
		"sort_order":                 transfer.SortOrder,
		"isTestnetTransfer":          isTestnet,
		"formattedTimestamp":         formattedTimestamp,
		"routeKey":                   routeKey,
		"senderDisplay":              senderDisplay,
		"receiverDisplay":            receiverDisplay,
		"baseAmount":                 transfer.BaseTokenAmountDisplay,
		"baseTokenSymbol":            transfer.BaseTokenSymbol,
		"transferSendTimestamp":      transfer.TransferSendTimestamp,
	}
} 