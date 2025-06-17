package http

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"
	"websocket-backend-new/storage"
	
	"github.com/gorilla/websocket"
)

// Server represents the HTTP server
type Server struct {
	memory   *storage.Memory
	channels *storage.Channels
	upgrader websocket.Upgrader
}

// NewServer creates a new HTTP server
func NewServer(memory *storage.Memory, channels *storage.Channels) *Server {
	return &Server{
		memory:   memory,
		channels: channels,
		upgrader: websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true // Allow all origins for now
			},
		},
	}
}

// Start starts the HTTP server
func (s *Server) Start(port string) error {
	mux := http.NewServeMux()
	
	// WebSocket endpoint
	mux.HandleFunc("/ws", s.handleWebSocket)
	
	// REST endpoints
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/stats", s.handleStats)
	
	// Static file serving (optional)
	mux.HandleFunc("/", s.handleRoot)
	
	log.Printf("[HTTP] 🌐 Starting HTTP server on port %s", port)
	log.Printf("[HTTP] 📡 WebSocket endpoint: ws://localhost%s/ws", port)
	log.Printf("[HTTP] 🔍 Health endpoint: http://localhost%s/health", port)
	log.Printf("[HTTP] 📊 Status endpoint: http://localhost%s/status", port)
	
	return http.ListenAndServe(port, mux)
}

// handleWebSocket handles WebSocket connections
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[HTTP] ❌ WebSocket upgrade failed: %v", err)
		return
	}
	
	clientID := fmt.Sprintf("client_%d", time.Now().UnixNano())
	log.Printf("[HTTP] 📱 New WebSocket client connected: %s", clientID)
	
	// Create client
	client := &storage.Client{
		ID:     clientID,
		Conn:   conn,
		Send:   make(chan []byte, 256),
		Active: true,
	}
	
	// Add to memory
	s.memory.AddClient(client)
	
	// Start client goroutines
	go s.clientWriter(client)
	go s.clientReader(client)
}

// clientWriter handles sending messages to the client
func (s *Server) clientWriter(client *storage.Client) {
	defer func() {
		if conn, ok := client.Conn.(*websocket.Conn); ok {
			conn.Close()
		}
		s.memory.RemoveClient(client.ID)
		log.Printf("[HTTP] 📱 Client disconnected: %s", client.ID)
	}()
	
	conn := client.Conn.(*websocket.Conn)
	
	for {
		select {
		case message, ok := <-client.Send:
			if !ok {
				conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := conn.WriteMessage(websocket.TextMessage, message); err != nil {
				log.Printf("[HTTP] ❌ Write error for client %s: %v", client.ID, err)
				return
			}
		}
	}
}

// clientReader handles receiving messages from the client
func (s *Server) clientReader(client *storage.Client) {
	defer func() {
		if conn, ok := client.Conn.(*websocket.Conn); ok {
			conn.Close()
		}
		s.memory.RemoveClient(client.ID)
	}()
	
	conn := client.Conn.(*websocket.Conn)
	conn.SetReadLimit(512)
	conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})
	
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("[HTTP] ❌ Read error for client %s: %v", client.ID, err)
			}
			break
		}
	}
}

// handleHealth returns health status
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	response := map[string]interface{}{
		"status":    "healthy",
		"timestamp": time.Now(),
		"clients":   s.memory.GetClientCount(),
		"chains":    len(s.memory.GetChains()),
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleStatus returns detailed status
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	lastSortOrder, isInitial := s.memory.GetPollingState()
	
	response := map[string]interface{}{
		"status": "running",
		"polling": map[string]interface{}{
			"lastSortOrder":  lastSortOrder,
			"isInitialFetch": isInitial,
		},
		"clients": map[string]interface{}{
			"count":  s.memory.GetClientCount(),
			"active": s.memory.GetClientCount(),
		},
		"chains": map[string]interface{}{
			"count": len(s.memory.GetChains()),
		},
		"timestamp": time.Now(),
	}
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// handleStats returns current stats/chart data
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	chartData := s.memory.GetChartData()
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(chartData)
}

// handleRoot serves a simple HTML page for testing
func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	
	html := `<!DOCTYPE html>
<html>
<head>
    <title>WebSocket Backend - Clean Architecture</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        .container { max-width: 800px; }
        .status { padding: 10px; background: #f0f0f0; border-radius: 5px; margin: 10px 0; }
        .transfer { padding: 8px; background: #e8f5e8; border-radius: 3px; margin: 5px 0; }
        .error { background: #ffe8e8; }
        pre { background: #f5f5f5; padding: 10px; border-radius: 3px; overflow-x: auto; }
    </style>
</head>
<body>
    <div class="container">
        <h1>🌟 WebSocket Backend - Clean Architecture</h1>
        
        <div class="status">
            <h3>📡 Connection Status</h3>
            <p id="status">Connecting...</p>
        </div>
        
        <div class="status">
            <h3>📊 Live Transfers</h3>
            <div id="transfers">Waiting for transfers...</div>
        </div>
        
        <div class="status">
            <h3>📈 Chart Data</h3>
            <div id="charts">Waiting for chart data...</div>
        </div>
    </div>
    
    <script>
        const ws = new WebSocket('ws://localhost:8080/ws');
        const statusEl = document.getElementById('status');
        const transfersEl = document.getElementById('transfers');
        const chartsEl = document.getElementById('charts');
        
        ws.onopen = function() {
            statusEl.innerHTML = '✅ Connected to WebSocket';
            statusEl.className = 'status';
        };
        
        ws.onmessage = function(event) {
            try {
                const data = JSON.parse(event.data);
                
                if (data.type === 'transfers') {
                    transfersEl.innerHTML = '<h4>New Transfers:</h4>';
                    data.transfers.forEach((transfer, i) => {
                        transfersEl.innerHTML += '<div class="transfer">' +
                            '<strong>' + transfer.source_chain.display_name + ' → ' + transfer.destination_chain.display_name + '</strong><br>' +
                            '<small>Route: ' + transfer.routeKey + '</small><br>' +
                            '<small>' + transfer.senderDisplay + ' → ' + transfer.receiverDisplay + '</small><br>' +
                            '<small>Hash: ' + transfer.transfer_send_transaction_hash.slice(0, 10) + '...</small><br>' +
                            '<small>Time: ' + transfer.formattedTimestamp + '</small>' +
                            (transfer.isTestnetTransfer ? '<br><small style="color: orange;">🧪 Testnet</small>' : '') +
                        '</div>';
                    });
                } else if (data.type === 'chart_data') {
                    chartsEl.innerHTML = '<h4>Chart Data Updated:</h4>' +
                        '<p>Transfer Rates: ' + data.data.transferRates.length + '</p>' +
                        '<p>Popular Routes: ' + data.data.popularRoutes.length + '</p>' +
                        '<p>Active Senders: ' + data.data.activeSenders.length + '</p>';
                }
            } catch (e) {
                console.error('Error parsing message:', e);
            }
        };
        
        ws.onclose = function() {
            statusEl.innerHTML = '❌ Disconnected';
            statusEl.className = 'status error';
        };
        
        ws.onerror = function(error) {
            statusEl.innerHTML = '❌ Connection Error';
            statusEl.className = 'status error';
            console.error('WebSocket error:', error);
        };
    </script>
</body>
</html>`
	
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(html))
} 