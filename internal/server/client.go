package server

import (
	"fmt"
	"sync"
	"time"
	"github.com/gorilla/websocket"
)

// Client represents a WebSocket client
type Client struct {
	conn   *websocket.Conn
	sendMu sync.Mutex // Protect concurrent writes to the same connection
}

// NewClient creates a new WebSocket client
func NewClient(conn *websocket.Conn) *Client {
	return &Client{
		conn: conn,
	}
}

// Send sends data to the client (non-blocking with timeout)
func (c *Client) Send(data []byte) error {
	// Lock to prevent concurrent writes to the same connection
	c.sendMu.Lock()
	defer c.sendMu.Unlock()
	
	// Set write deadline to prevent blocking
	c.conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	return c.conn.WriteMessage(websocket.TextMessage, data)
}

// Close closes the client connection
func (c *Client) Close() error {
	return c.conn.Close()
}

// GetID returns a unique identifier for the client
func (c *Client) GetID() string {
	return fmt.Sprintf("%p", c.conn)
} 