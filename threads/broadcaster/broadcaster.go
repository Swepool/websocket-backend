package broadcaster

import (
	"encoding/json"
	"log"
	"time"
	"websocket-backend-new/models"
	"websocket-backend-new/storage"
)

// Broadcaster handles broadcasting transfers to clients (Thread 5)
type Broadcaster struct {
	memory   *storage.Memory
	channels *storage.Channels
}

// NewBroadcaster creates a new broadcaster
func NewBroadcaster(memory *storage.Memory, channels *storage.Channels) *Broadcaster {
	return &Broadcaster{
		memory:   memory,
		channels: channels,
	}
}

// Start begins the broadcasting loop (Thread 5)
func (b *Broadcaster) Start() {
	log.Printf("[THREAD-5] 🚀 Starting broadcaster thread")
	
	for {
		select {
		case <-b.channels.Shutdown:
			log.Printf("[THREAD-5] 🛑 Shutdown signal received, stopping")
			return
			
		case transfers := <-b.channels.TransferBroadcasts:
			b.broadcastTransfers(transfers)
			
		case chartData := <-b.channels.ChartUpdates:
			b.broadcastChartData(chartData)
		}
	}
}

// broadcastTransfers broadcasts live transfers to all clients (expects single transfer arrays for streaming)
func (b *Broadcaster) broadcastTransfers(transfers []models.BroadcastTransfer) {
	clients := b.memory.GetActiveClients()
	clientCount := len(clients)
	
	// Process transfers individually for natural streaming feel
	for i, transfer := range transfers {
		log.Printf("[THREAD-5] 📡 Streaming transfer %d/%d: %s → %s (%s %s)", 
			i+1, len(transfers),
			transfer.SourceChain.DisplayName,
			transfer.DestinationChain.DisplayName,
			transfer.BaseAmount,
			transfer.BaseTokenSymbol,
		)
		
		// Send each transfer individually to match frontend expectations
		if clientCount > 0 {
			// Frontend expects: message.type === "transfers" && Array.isArray(message.data)
			message := map[string]interface{}{
				"type": "transfers",
				"data": []models.BroadcastTransfer{transfer}, // Single transfer in array
			}
			
			messageJSON, err := json.Marshal(message)
			if err != nil {
				log.Printf("[THREAD-5] ❌ Failed to marshal transfer: %v", err)
				continue
			}
			
			// Send to all active clients
			successCount := 0
			for _, client := range clients {
				select {
				case client.Send <- messageJSON:
					successCount++
				default:
					log.Printf("[THREAD-5] ⚠️ Client %s channel full, dropping transfer", client.ID)
				}
			}
			
			log.Printf("[THREAD-5] ✅ Streamed transfer to %d/%d clients", successCount, clientCount)
		} else {
			log.Printf("[THREAD-5] ℹ️ No clients connected, transfer logged only")
		}
		
		// Small delay between individual transfers for ultra-smooth streaming
		if i < len(transfers)-1 {
			time.Sleep(25 * time.Millisecond)
		}
	}
}

// broadcastChartData broadcasts chart data to all clients
func (b *Broadcaster) broadcastChartData(chartData models.ChartData) {
	clients := b.memory.GetActiveClients()
	clientCount := len(clients)
	
	log.Printf("[THREAD-5] 📊 Broadcasting chart data to %d clients", clientCount)
	log.Printf("[THREAD-5] 📈 Chart data - Transfer rates: %d, Popular routes: %d, Active senders: %d", 
		len(chartData.TransferRates),
		len(chartData.PopularRoutes),
		len(chartData.ActiveSenders),
	)
	
	// Send to WebSocket clients
	if clientCount > 0 {
		message := map[string]interface{}{
			"type": "chartData",
			"data": chartData,
		}
		
		messageJSON, err := json.Marshal(message)
		if err != nil {
			log.Printf("[THREAD-5] ❌ Failed to marshal chart data: %v", err)
			return
		}
		
		// Send to all active clients
		successCount := 0
		for _, client := range clients {
			select {
			case client.Send <- messageJSON:
				successCount++
			default:
				log.Printf("[THREAD-5] ⚠️ Client %s channel full, dropping chart data", client.ID)
			}
		}
		
		log.Printf("[THREAD-5] ✅ Successfully sent chart data to %d/%d clients", successCount, clientCount)
	} else {
		log.Printf("[THREAD-5] ℹ️ No clients connected for chart data")
	}
} 