package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// handleWebSocket handles WebSocket connections
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Printf("[WEBSOCKET] Failed to upgrade connection: %v\n", err)
		return
	}
	defer conn.Close()
	
	client := NewClient(conn)
	
	fmt.Printf("[WEBSOCKET] New client connected\n")
	
	// Register client with broadcaster
	s.coordinator.GetBroadcaster().AddClient(client)
	defer s.coordinator.GetBroadcaster().RemoveClient(client)
	
	// Keep connection alive
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			fmt.Printf("[WEBSOCKET] Client disconnected: %v\n", err)
			break
		}
	}
}

// handleStats provides stats API endpoint
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	// Get chart data from stats collector in frontend format (same as WebSocket)
	chartData := s.coordinator.GetStatsCollector().GetChartDataForFrontend()
	
	// Create API response
	response := map[string]interface{}{
		"success": true,
		"data":    chartData,
		"meta": map[string]interface{}{
			"timestamp": time.Now().Unix(),
			"source":    "HLL-based stats collector",
			"note":      "Real-time statistics using HyperLogLog for unique counting",
		},
	}
	
	if err := json.NewEncoder(w).Encode(response); err != nil {
		fmt.Printf("[API] Error encoding stats response: %v\n", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// handleChains provides chains metadata API endpoint
func (s *Server) handleChains(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	// Get all chains from chains service
	chains := s.chainsService.GetAllChains()
	
	// Create API response
	response := map[string]interface{}{
		"success": true,
		"data":    chains,
		"meta": map[string]interface{}{
			"timestamp": time.Now().Unix(),
			"count":     len(chains),
			"note":      "Available chains for cross-chain transfers",
		},
	}
	
	if err := json.NewEncoder(w).Encode(response); err != nil {
		fmt.Printf("[API] Error encoding chains response: %v\n", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// handleHealth provides health check endpoint
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	chartData := s.coordinator.GetStatsCollector().GetChartDataForFrontend()
	
	// Extract values safely from frontend format
	var uptime float64
	var totalTransfers int64
	var uniqueWallets int64
	var topRoutes, topAssets int
	
	if data, ok := chartData.(map[string]interface{}); ok {
		if currentRates, ok := data["currentRates"].(map[string]interface{}); ok {
			if val, ok := currentRates["serverUptimeSeconds"].(float64); ok {
				uptime = val
			}
			if val, ok := currentRates["totalTracked"].(int64); ok {
				totalTransfers = val
			}
		}
		if walletRates, ok := data["activeWalletRates"].(map[string]interface{}); ok {
			if val, ok := walletRates["uniqueTotalWallets"].(int64); ok {
				uniqueWallets = val
			}
		}
		if routes, ok := data["popularRoutes"].([]interface{}); ok {
			topRoutes = len(routes)
		}
		if assets, ok := data["assetVolumeData"].(map[string]interface{}); ok {
			if assetList, ok := assets["assets"].([]interface{}); ok {
				topAssets = len(assetList)
			}
		}
	}
	
	response := map[string]interface{}{
		"status": "healthy",
		"uptime": uptime,
		"stats": map[string]interface{}{
			"totalTransfers": totalTransfers,
			"uniqueWallets":  uniqueWallets,
			"topRoutes":      topRoutes,
			"topAssets":      topAssets,
		},
		"timestamp": time.Now().Unix(),
	}
	
	json.NewEncoder(w).Encode(response)
} 