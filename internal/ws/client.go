package ws

import (
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"websocket-backend/internal/models"
	"websocket-backend/internal/utils"
)

// Slow client detection constants
const (
	slowClientThreshold     = 10        // Number of buffer full events to consider slow
	slowClientTimeWindow    = 60        // Time window in seconds to track buffer full events
	slowClientAutoDisconnect = true     // Whether to auto-disconnect persistently slow clients
)

// Errors
var (
	ErrClientBufferFull = errors.New("client buffer is full")
)

// SlowReadStats tracks slow read detection metrics
type SlowReadStats struct {
	BufferFullCount    int64     // Total buffer full events
	LastBufferFull     time.Time // Last time buffer was full
	RecentBufferFulls  []time.Time // Recent buffer full timestamps for windowed detection
	IsMarkedSlow       bool      // Whether client is marked as slow
	SlowMarkedAt       time.Time // When client was marked as slow
}

// Client represents a WebSocket client optimized for high concurrency
type Client struct {
	conn        *websocket.Conn
	send        chan []byte
	filter      *models.ChainFilter
	shardIndex  int // Shard index for optimized removal
	
	// Slow-read detection
	slowReadStats SlowReadStats
	
	mu          sync.RWMutex
	cleanup     func(*Client)
	closed      bool
	closeMu     sync.Mutex
}

// NewClient creates a new WebSocket client with cleanup callback
func NewClient(conn *websocket.Conn, cleanup func(*Client)) *Client {
	return &Client{
		conn:       conn,
		send:       make(chan []byte, maxMessageQueue), // Use configurable buffer size
		cleanup:    cleanup,
		shardIndex: -1, // Initialize to -1 to indicate not yet assigned
	}
}

// WritePump pumps messages from the client's send channel to the WebSocket connection
func (c *Client) WritePump() {
	defer c.close()

	// Set write deadline for each message
	ticker := time.NewTicker(54 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case message, ok := <-c.send:
			c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				c.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			if err := c.conn.WriteMessage(websocket.TextMessage, message); err != nil {
				utils.WSLogger.Error("Error writing message", map[string]interface{}{
					"error": err.Error(),
				})
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

// ReadPump pumps messages from the WebSocket connection to the server with improved close handling
func (c *Client) ReadPump(handler func(*Client, []byte)) {
	defer c.close()

	// Set read deadline and pong handler
	c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	c.conn.SetPongHandler(func(string) error {
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := c.conn.ReadMessage()
		if err != nil {
			// Handle different types of WebSocket close errors
			if websocket.IsCloseError(err, 
				websocket.CloseNormalClosure,
				websocket.CloseGoingAway,
				websocket.CloseAbnormalClosure,
				websocket.CloseNoStatusReceived) {
				
				utils.WSLogger.Debug("WebSocket closed", map[string]interface{}{
					"error": err.Error(),
					"type":  "normal_close",
				})
			} else if websocket.IsUnexpectedCloseError(err, 
				websocket.CloseGoingAway, 
				websocket.CloseAbnormalClosure,
				websocket.CloseNormalClosure) {
				
				utils.WSLogger.Warn("Unexpected WebSocket close error", map[string]interface{}{
					"error": err.Error(),
					"type":  "unexpected_close",
				})
			} else {
				// Other network or protocol errors
				utils.WSLogger.Error("WebSocket read error", map[string]interface{}{
					"error": err.Error(),
					"type":  "read_error",
				})
			}
			break
		}

		// Reset read deadline on each message
		c.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		handler(c, message)
	}
}

// close handles client cleanup
func (c *Client) close() {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()

	if c.closed {
		return
	}
	c.closed = true

	// Close the connection
	c.conn.Close()

	// Close the send channel
	close(c.send)

	// Call cleanup callback
	if c.cleanup != nil {
		c.cleanup(c)
	}
}

// SetFilter sets the client's chain filter
func (c *Client) SetFilter(filter *models.ChainFilter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.filter = filter
}

// GetFilter gets the client's chain filter
func (c *Client) GetFilter() *models.ChainFilter {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.filter
}

// Send sends a message to the client with non-blocking behavior
func (c *Client) Send(message interface{}) error {
	// Critical nil checks first
	if c == nil {
		return errors.New("client is nil")
	}
	
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return errors.New("client is closed")
	}
	
	// Additional safety check
	if c.send == nil {
		c.closeMu.Unlock()
		return errors.New("client send channel is nil")
	}
	c.closeMu.Unlock()

	data, err := json.Marshal(message)
	if err != nil {
		return err
	}

	select {
	case c.send <- data:
		return nil
	default:
		// Channel is full, track for slow client detection
		c.trackBufferFull()
		utils.WSLogger.Warn("Client buffer full, dropping message", map[string]interface{}{
			"queueUtilization": c.GetQueueUtilization(),
			"isSlowClient":     c.IsSlowClient(),
		})
		return ErrClientBufferFull
	}
}

// SendPreEncoded sends pre-encoded JSON bytes to the client (zero-copy broadcast optimization)
func (c *Client) SendPreEncoded(data []byte) error {
	// Critical nil checks first
	if c == nil {
		return errors.New("client is nil")
	}
	
	if len(data) == 0 {
		return errors.New("empty data")
	}
	
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return errors.New("client is closed")
	}
	
	// Additional safety check
	if c.send == nil {
		c.closeMu.Unlock()
		return errors.New("client send channel is nil")
	}
	c.closeMu.Unlock()

	select {
	case c.send <- data:
		return nil
	default:
		// Channel is full, track for slow client detection
		c.trackBufferFull()
		utils.WSLogger.Warn("Client buffer full, dropping message", map[string]interface{}{
			"queueUtilization": c.GetQueueUtilization(),
			"isSlowClient":     c.IsSlowClient(),
		})
		return ErrClientBufferFull
	}
}

// SendTransfers sends transfers to the client (optimized version)
func (c *Client) SendTransfers(transfers []models.Transfer) bool {
	if len(transfers) == 0 {
		return false
	}
	
	err := c.Send(struct {
		Type      string           `json:"type"`
		Data      []models.Transfer `json:"data"`
		Timestamp int64            `json:"timestamp"`
	}{
		Type:      "transfers",
		Data:      transfers,
		Timestamp: time.Now().UnixMilli(),
	})
	return err == nil
}

// SendBroadcastTransfers sends broadcast transfers to the client (reduced payload version)
func (c *Client) SendBroadcastTransfers(transfers []models.BroadcastTransfer) bool {
	if len(transfers) == 0 {
		return false
	}
	
	err := c.Send(struct {
		Type      string                    `json:"type"`
		Data      []models.BroadcastTransfer `json:"data"`
		Timestamp int64                     `json:"timestamp"`
	}{
		Type:      "transfers",
		Data:      transfers,
		Timestamp: time.Now().UnixMilli(),
	})
	return err == nil
}

// SendConnected sends a connected message to the client
func (c *Client) SendConnected() error {
	return c.Send(struct {
		Type      string `json:"type"`
		Message   string `json:"message"`
		Timestamp int64  `json:"timestamp"`
	}{
		Type:      "connected",
		Message:   "Connected to Union transfer stream (optimized for 100k connections)",
		Timestamp: time.Now().UnixMilli(),
	})
}

// SendChains sends chain data to the client
func (c *Client) SendChains(chains []models.Chain) error {
	if len(chains) == 0 {
		return errors.New("no chains to send")
	}
	
	return c.Send(struct {
		Type      string        `json:"type"`
		Data      []models.Chain `json:"data"`
		Timestamp int64         `json:"timestamp"`
	}{
		Type:      "chains",
		Data:      chains,
		Timestamp: time.Now().UnixMilli(),
	})
}

// SendServerInfo sends server info to the client
func (c *Client) SendServerInfo() error {
	return c.Send(struct {
		Type      string `json:"type"`
		Data      struct {
			ServerPrecomputation string   `json:"serverPrecomputation"`
			Features            []string `json:"features"`
			Architecture        string   `json:"architecture"`
			MaxConnections      int      `json:"maxConnections"`
		} `json:"data"`
		Timestamp int64 `json:"timestamp"`
	}{
		Type: "serverInfo",
		Data: struct {
			ServerPrecomputation string   `json:"serverPrecomputation"`
			Features            []string `json:"features"`
			Architecture        string   `json:"architecture"`
			MaxConnections      int      `json:"maxConnections"`
		}{
			ServerPrecomputation: "enabled",
			Features: []string{
				"testnet_flags_precomputed",
				"display_names_precomputed",
				"full_addresses_provided",
				"timestamps_formatted",
				"route_keys_generated",
				"chains_data_provided",
				"high_concurrency_optimized",
				"sharded_client_management",
				"parallel_broadcasting",
			},
			Architecture:   "sharded_workers",
			MaxConnections: 100000,
		},
		Timestamp: time.Now().UnixMilli(),
	})
}

// SendFilterSet sends filter set confirmation to the client
func (c *Client) SendFilterSet(fromChain, toChain string) error {
	return c.Send(struct {
		Type string `json:"type"`
		Data struct {
			FromChain string `json:"fromChain"`
			ToChain   string `json:"toChain"`
		} `json:"data"`
	}{
		Type: "filterSet",
		Data: struct {
			FromChain string `json:"fromChain"`
			ToChain   string `json:"toChain"`
		}{
			FromChain: fromChain,
			ToChain:   toChain,
		},
	})
}

// SendChartData sends chart data to the client
func (c *Client) SendChartData(chartData interface{}) error {
	if chartData == nil {
		return errors.New("chart data is nil")
	}
	
	return c.Send(struct {
		Type      string      `json:"type"`
		Data      interface{} `json:"data"`
		Timestamp int64       `json:"timestamp"`
	}{
		Type:      "chartData",
		Data:      chartData,
		Timestamp: time.Now().UnixMilli(),
	})
}

// IsClosed returns whether the client is closed
func (c *Client) IsClosed() bool {
	c.closeMu.Lock()
	defer c.closeMu.Unlock()
	return c.closed
}

// SetShardIndex sets the shard index for this client
func (c *Client) SetShardIndex(index int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.shardIndex = index
}

// GetShardIndex gets the shard index for this client
func (c *Client) GetShardIndex() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.shardIndex
}

// trackBufferFull records a buffer full event for slow client detection
func (c *Client) trackBufferFull() {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	now := time.Now()
	c.slowReadStats.BufferFullCount++
	c.slowReadStats.LastBufferFull = now
	
	// Add to recent buffer fulls (windowed tracking)
	c.slowReadStats.RecentBufferFulls = append(c.slowReadStats.RecentBufferFulls, now)
	
	// Clean old entries outside the time window
	cutoff := now.Add(-time.Duration(slowClientTimeWindow) * time.Second)
	filtered := c.slowReadStats.RecentBufferFulls[:0]
	for _, timestamp := range c.slowReadStats.RecentBufferFulls {
		if timestamp.After(cutoff) {
			filtered = append(filtered, timestamp)
		}
	}
	c.slowReadStats.RecentBufferFulls = filtered
	
	// Check if client should be marked as slow
	if len(c.slowReadStats.RecentBufferFulls) >= slowClientThreshold {
		if !c.slowReadStats.IsMarkedSlow {
			c.slowReadStats.IsMarkedSlow = true
			c.slowReadStats.SlowMarkedAt = now
			
			// Log slow client detection
			clientAddr := "unknown"
			if c.conn != nil && c.conn.RemoteAddr() != nil {
				clientAddr = c.conn.RemoteAddr().String()
			}
			
			utils.WSLogger.Warn("Client marked as slow", map[string]interface{}{
				"clientAddr":       clientAddr,
				"bufferFullCount":  c.slowReadStats.BufferFullCount,
				"recentFulls":      len(c.slowReadStats.RecentBufferFulls),
				"timeWindow":       slowClientTimeWindow,
				"autoDisconnect":   slowClientAutoDisconnect,
			})
			
			// Auto-disconnect if enabled
			if slowClientAutoDisconnect {
				utils.WSLogger.Info("Auto-disconnecting slow client", map[string]interface{}{
					"clientAddr": clientAddr,
				})
				go c.close() // Close in goroutine to avoid deadlock
			}
		}
	}
}

// IsSlowClient returns whether the client is marked as slow
func (c *Client) IsSlowClient() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.slowReadStats.IsMarkedSlow
}

// GetSlowReadStats returns a copy of the slow read statistics
func (c *Client) GetSlowReadStats() SlowReadStats {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	// Return a copy to avoid data races
	return SlowReadStats{
		BufferFullCount:   c.slowReadStats.BufferFullCount,
		LastBufferFull:    c.slowReadStats.LastBufferFull,
		RecentBufferFulls: append([]time.Time(nil), c.slowReadStats.RecentBufferFulls...),
		IsMarkedSlow:      c.slowReadStats.IsMarkedSlow,
		SlowMarkedAt:      c.slowReadStats.SlowMarkedAt,
	}
}

// GetQueueUtilization returns the current send queue utilization (0.0 to 1.0)
func (c *Client) GetQueueUtilization() float64 {
	if c.send == nil {
		return 0.0
	}
	return float64(len(c.send)) / float64(cap(c.send))
}

// GetClientMetrics returns comprehensive client metrics for monitoring
func (c *Client) GetClientMetrics() map[string]interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	clientAddr := "unknown"
	if c.conn != nil && c.conn.RemoteAddr() != nil {
		clientAddr = c.conn.RemoteAddr().String()
	}
	
	return map[string]interface{}{
		"clientAddr":        clientAddr,
		"shardIndex":        c.shardIndex,
		"queueUtilization":  c.GetQueueUtilization(),
		"bufferFullCount":   c.slowReadStats.BufferFullCount,
		"lastBufferFull":    c.slowReadStats.LastBufferFull,
		"recentBufferFulls": len(c.slowReadStats.RecentBufferFulls),
		"isMarkedSlow":      c.slowReadStats.IsMarkedSlow,
		"slowMarkedAt":      c.slowReadStats.SlowMarkedAt,
		"isClosed":          c.closed,
	}
}