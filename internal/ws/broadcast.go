package ws

import (
	"encoding/json"
	"log"
	"sync/atomic"
	"time"

	"websocket-backend/internal/models"
	"runtime/debug"
)

// BroadcastMessage represents a message to broadcast
type BroadcastMessage struct {
	transfers []models.Transfer
	timestamp int64
}

// PreEncodedBroadcastMessage represents a pre-encoded message for zero-copy broadcasting
type PreEncodedBroadcastMessage struct {
	Type      string                     `json:"type"`
	Data      []*models.BroadcastTransfer `json:"data"`
	Timestamp int64                      `json:"timestamp"`
}

// MessageCache stores pre-encoded JSON for different filter keys
type MessageCache struct {
	encodedMessages map[string][]byte // filterKey -> encoded JSON
	timestamp       int64
}

// broadcastWorker handles broadcasting messages to clients in assigned shards
func (s *Server) broadcastWorker(workerID int, messages chan BroadcastMessage) {
	defer func() {
		s.wg.Done()
		if err := recover(); err != nil {
			log.Printf("[BROADCAST] Worker %d panic recovered: %v", workerID, err)
			// Log stack trace for debugging
			log.Printf("[BROADCAST] Worker %d stack trace: %s", workerID, debug.Stack())
		}
	}()
	
	// Worker started - no log needed
	
	// Calculate which shards this worker handles
	shardsPerWorker := clientShards / maxWorkers
	startShard := workerID * shardsPerWorker
	endShard := startShard + shardsPerWorker
	if workerID == maxWorkers-1 {
		endShard = clientShards
	}
	
	// Validate shard range
	if startShard < 0 || endShard > clientShards || startShard >= endShard {
		log.Printf("[BROADCAST] Worker %d has invalid shard range [%d, %d), maxShards: %d", 
			workerID, startShard, endShard, clientShards)
		return
	}
	
	for {
		select {
		case <-s.shutdown:
			return
		case msg, ok := <-messages:
			if !ok {
				return
			}
			
			func() {
				defer func() {
					if err := recover(); err != nil {
						log.Printf("[BROADCAST] Worker %d message processing panic recovered: %v", workerID, err)
					}
				}()
				
				if len(msg.transfers) == 0 {
					return
				}
				
				s.deliverToClientsOptimized(workerID, startShard, endShard, msg.transfers)
			}()
		}
	}
}

// deliverToClientsOptimized groups clients by filter and applies filtering once per unique filter
// with zero-copy JSON broadcast optimization
func (s *Server) deliverToClientsOptimized(workerID, startShard, endShard int, transfers []models.Transfer) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("[BROADCAST] deliverToClientsOptimized panic recovered: %v", err)
		}
	}()
	
	// Group clients by filter key
	clientsByFilter := make(map[string][]*Client)
	
	// Collect all clients from assigned shards and group by filter
	for i := startShard; i < endShard; i++ {
		// Critical bounds check
		if i >= len(s.clientShards) || i < 0 {
			log.Printf("[BROADCAST] Worker %d: CRITICAL shard index %d out of range (max: %d)", workerID, i, len(s.clientShards))
			break
		}
		
		shard := s.clientShards[i]
		if shard == nil {
			log.Printf("[BROADCAST] Worker %d: CRITICAL shard %d is nil", workerID, i)
			continue
		}
		
		func() {
			defer func() {
				if err := recover(); err != nil {
					log.Printf("[BROADCAST] Worker %d shard %d grouping panic recovered: %v", workerID, i, err)
				}
			}()
			
			shard.mu.RLock()
			defer shard.mu.RUnlock()
			
			if shard.clients == nil {
				log.Printf("[BROADCAST] Worker %d: shard %d clients map is nil", workerID, i)
				return
			}
			
			for client := range shard.clients {
				if client == nil || client.IsClosed() {
					continue
				}
				
				filter := client.GetFilter()
				filterKey := models.GetFilterKeyFromFilter(filter)
				clientsByFilter[filterKey] = append(clientsByFilter[filterKey], client)
			}
		}()
	}
	
	// Pre-convert all transfers to broadcast transfers once
	broadcastTransfers := make([]*models.BroadcastTransfer, 0, len(transfers))
	for _, transfer := range transfers {
		if transfer.SourceChain.UniversalChainID == "" || transfer.DestinationChain.UniversalChainID == "" {
			log.Printf("[BROADCAST] Skipping transfer with empty chain IDs")
			continue
		}
		broadcastTransfers = append(broadcastTransfers, transfer.ToBroadcastTransfer())
	}
	
	if len(broadcastTransfers) == 0 {
		return
	}
	
	// Zero-copy optimization: Cache to store pre-encoded JSON for each filter key
	messageCache := &MessageCache{
		encodedMessages: make(map[string][]byte),
		timestamp:       time.Now().UnixMilli(),
	}
	
	// Process each unique filter once and pre-encode the message
	for filterKey, clients := range clientsByFilter {
		if len(clients) == 0 {
			continue
		}
		
		// Get the filter from the first client (all clients in this group have the same filter)
		filter := clients[0].GetFilter()
		
		// Apply filtering once for this filter key
		filteredTransfers := make([]*models.BroadcastTransfer, 0, len(broadcastTransfers))
		for _, broadcastTransfer := range broadcastTransfers {
			if filter == nil || broadcastTransfer.MatchesFilter(filter) {
				filteredTransfers = append(filteredTransfers, broadcastTransfer)
			}
		}
		
		// If no transfers match this filter, skip
		if len(filteredTransfers) == 0 {
			continue
		}
		
		// Pre-encode the message once for this filter key (zero-copy optimization)
		message := PreEncodedBroadcastMessage{
			Type:      "transfers",
			Data:      filteredTransfers,
			Timestamp: messageCache.timestamp,
		}
		
		encodedData, err := json.Marshal(message)
		if err != nil {
			log.Printf("[BROADCAST] Failed to encode message for filter %s: %v", filterKey, err)
			continue
		}
		
		// Store the pre-encoded message
		messageCache.encodedMessages[filterKey] = encodedData
		
		// Send the same pre-encoded bytes to all clients with this filter (zero-copy)
		for _, client := range clients {
			if client == nil || client.IsClosed() {
				continue
			}
			
			func() {
				defer func() {
					if err := recover(); err != nil {
						log.Printf("[BROADCAST] Client SendPreEncoded panic recovered: %v", err)
					}
				}()
				
				// Use zero-copy SendPreEncoded instead of re-encoding
				client.SendPreEncoded(encodedData)
			}()
		}
	}
}

// broadcastTransfers distributes transfers across broadcast workers
func (s *Server) broadcastTransfers(transfers []models.Transfer) {
	if len(transfers) == 0 {
		return
	}
	
	msg := BroadcastMessage{
		transfers: transfers,
		timestamp: time.Now().UnixMilli(),
	}
	
	// Distribute to all workers
	for i, worker := range s.broadcastWorkers {
		if worker == nil {
			log.Printf("[BROADCAST] Worker %d channel is nil", i)
			continue
		}
		
		select {
		case worker <- msg:
			// Message sent successfully
		default:
			log.Printf("[BROADCAST] Worker %d channel full, dropping message", i)
		}
	}
	
	// Distribution completed - no log needed
}

// broadcastChartData broadcasts chart data to all clients with zero-copy optimization
func (s *Server) broadcastChartData() {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("[CHART] Chart data broadcast panic recovered: %v", err)
		}
	}()
	
	chartData := s.getChartData()
	if chartData == nil {
		return
	}
	
	// Zero-copy optimization: Pre-encode chart data once
	message := struct {
		Type      string      `json:"type"`
		Data      interface{} `json:"data"`
		Timestamp int64       `json:"timestamp"`
	}{
		Type:      "chartData",
		Data:      chartData,
		Timestamp: time.Now().UnixMilli(),
	}
	
	encodedData, err := json.Marshal(message)
	if err != nil {
		log.Printf("[CHART] Failed to encode chart data: %v", err)
		return
	}
	
	// Send pre-encoded data to all workers
	for i := 0; i < maxWorkers; i++ {
		s.sendPreEncodedChartDataToWorker(i, encodedData)
	}
}

// sendPreEncodedChartDataToWorker sends pre-encoded chart data to a specific worker's clients
func (s *Server) sendPreEncodedChartDataToWorker(workerID int, encodedData []byte) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("[CHART] Worker %d chart data send panic recovered: %v", workerID, err)
		}
	}()
	
	if len(encodedData) == 0 {
		return
	}
	
	// Calculate which shards this worker handles
	shardsPerWorker := clientShards / maxWorkers
	startShard := workerID * shardsPerWorker
	endShard := startShard + shardsPerWorker
	if workerID == maxWorkers-1 {
		endShard = clientShards
	}
	
	// Send to all clients in assigned shards using pre-encoded data
	for i := startShard; i < endShard; i++ {
		if i >= len(s.clientShards) {
			continue
		}
		
		shard := s.clientShards[i]
		if shard == nil {
			continue
		}
		
		func() {
			defer func() {
				if err := recover(); err != nil {
					log.Printf("[CHART] Shard %d chart data send panic recovered: %v", i, err)
				}
			}()
			
			shard.mu.RLock()
			defer shard.mu.RUnlock()
			
			for client := range shard.clients {
				if client == nil || client.IsClosed() {
					continue
				}
				
				// Use zero-copy SendPreEncoded instead of re-encoding
				func() {
					defer func() {
						if err := recover(); err != nil {
							log.Printf("[CHART] Client chart data send panic recovered: %v", err)
						}
					}()
					client.SendPreEncoded(encodedData)
				}()
			}
		}()
	}
}

// chartDataBroadcaster periodically broadcasts chart data
func (s *Server) chartDataBroadcaster() {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("[CHART] Chart data broadcaster panic recovered: %v", err)
			// Restart the broadcaster after a panic
			time.Sleep(5 * time.Second) // Wait before restarting
			go s.chartDataBroadcaster() // Restart the broadcaster
		}
	}()
	
	ticker := time.NewTicker(1 * time.Second) // Broadcast every 1 second for real-time updates
	defer ticker.Stop()
	
	for {
		select {
		case <-s.shutdown:
			return
		case <-ticker.C:
			func() {
				defer func() {
					if err := recover(); err != nil {
						log.Printf("[CHART] Chart data broadcast cycle panic recovered: %v", err)
					}
				}()
				
				clientCount := atomic.LoadInt64(&s.clientCount)
				if clientCount > 0 {
					s.broadcastChartData()
				}
			}()
		}
	}
} 