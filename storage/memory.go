package storage

import (
	"sync"
	"time"
	"websocket-backend-new/models"
)

// Memory represents the in-memory storage for the application
type Memory struct {
	// Chain data
	chains   []models.Chain
	chainsMu sync.RWMutex
	
	// Last polling state
	lastSortOrder  string
	isInitialFetch bool
	pollingMu      sync.RWMutex
	
	// Current chart data (cached)
	currentChartData models.ChartData
	chartDataMu      sync.RWMutex
	
	// Connected clients (for broadcasting)
	clients   map[string]*Client
	clientsMu sync.RWMutex
}

// Client represents a WebSocket client connection
type Client struct {
	ID     string
	Conn   interface{} // WebSocket connection
	Send   chan []byte
	Active bool
}

// NewMemory creates a new memory storage instance
func NewMemory() *Memory {
	return &Memory{
		chains:           make([]models.Chain, 0),
		isInitialFetch:   true,
		currentChartData: models.ChartData{LastUpdated: time.Now()},
		clients:          make(map[string]*Client),
	}
}

// === Chain Management ===

// UpdateChains updates the stored chain data
func (m *Memory) UpdateChains(chains []models.Chain) {
	m.chainsMu.Lock()
	defer m.chainsMu.Unlock()
	m.chains = chains
}

// GetChains returns a copy of current chain data
func (m *Memory) GetChains() []models.Chain {
	m.chainsMu.RLock()
	defer m.chainsMu.RUnlock()
	
	// Return a copy to prevent data races
	chains := make([]models.Chain, len(m.chains))
	copy(chains, m.chains)
	return chains
}

// === Polling State Management ===

// UpdatePollingState updates the last sort order and initial fetch state
func (m *Memory) UpdatePollingState(sortOrder string) {
	m.pollingMu.Lock()
	defer m.pollingMu.Unlock()
	m.lastSortOrder = sortOrder
	m.isInitialFetch = false
}

// GetPollingState returns current polling state
func (m *Memory) GetPollingState() (string, bool) {
	m.pollingMu.RLock()
	defer m.pollingMu.RUnlock()
	return m.lastSortOrder, m.isInitialFetch
}

// === Chart Data Management ===

// UpdateChartData updates the current chart data
func (m *Memory) UpdateChartData(data models.ChartData) {
	m.chartDataMu.Lock()
	defer m.chartDataMu.Unlock()
	m.currentChartData = data
}

// GetChartData returns a copy of current chart data
func (m *Memory) GetChartData() models.ChartData {
	m.chartDataMu.RLock()
	defer m.chartDataMu.RUnlock()
	return m.currentChartData
}

// === Client Management ===

// AddClient adds a new WebSocket client
func (m *Memory) AddClient(client *Client) {
	m.clientsMu.Lock()
	defer m.clientsMu.Unlock()
	m.clients[client.ID] = client
}

// RemoveClient removes a WebSocket client
func (m *Memory) RemoveClient(clientID string) {
	m.clientsMu.Lock()
	defer m.clientsMu.Unlock()
	if client, exists := m.clients[clientID]; exists {
		client.Active = false
		close(client.Send)
		delete(m.clients, clientID)
	}
}

// GetActiveClients returns a list of active clients
func (m *Memory) GetActiveClients() []*Client {
	m.clientsMu.RLock()
	defer m.clientsMu.RUnlock()
	
	clients := make([]*Client, 0, len(m.clients))
	for _, client := range m.clients {
		if client.Active {
			clients = append(clients, client)
		}
	}
	return clients
}

// GetClientCount returns the number of active clients
func (m *Memory) GetClientCount() int {
	m.clientsMu.RLock()
	defer m.clientsMu.RUnlock()
	
	count := 0
	for _, client := range m.clients {
		if client.Active {
			count++
		}
	}
	return count
} 