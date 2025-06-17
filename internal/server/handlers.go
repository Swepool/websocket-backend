package server

import (
	"encoding/json"
	"net/http"
)

// handleWebSocket handles WebSocket connections
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	broadcaster := s.coordinator.GetBroadcaster()
	broadcaster.UpgradeConnection(w, r)
}

// handleStats returns current stats data
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	statsCollector := s.coordinator.GetStatsCollector()
	data := statsCollector.GetChartDataForFrontend()
	
	if err := json.NewEncoder(w).Encode(data); err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// handleChains returns current chains data
func (s *Server) handleChains(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	chains := s.chainsService.GetAllChains()
	
	response := map[string]interface{}{
		"chains": chains,
		"count":  len(chains),
	}
	
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// handleHealth returns health status
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	broadcaster := s.coordinator.GetBroadcaster()
	statsCollector := s.coordinator.GetStatsCollector()
	
	response := map[string]interface{}{
		"status":      "ok",
		"clients":     broadcaster.GetClientCount(),
		"stats":       statsCollector.GetChartData(),
		"chains":      len(s.chainsService.GetAllChains()),
	}
	
	json.NewEncoder(w).Encode(response)
}

 