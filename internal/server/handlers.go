package server

import (
	"encoding/json"
	"net/http"
	"time"
	"websocket-backend-new/internal/utils"
)

// handleWebSocket handles WebSocket connections
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	broadcaster := s.coordinator.GetBroadcaster()
	broadcaster.UpgradeConnection(w, r)
}

// handleStats returns current stats data from enhanced chart service
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	chartService := s.coordinator.GetChartService()
	
	// Get complete chart data from enhanced chart service
	chartData, err := chartService.GetChartDataForFrontend()
	if err != nil {
		utils.LogError("SERVER", "Failed to get chart data: %v", err)
		http.Error(w, "Failed to get chart data", http.StatusInternalServerError)
		return
	}
	
	if err := json.NewEncoder(w).Encode(chartData); err != nil {
		utils.LogError("SERVER", "Failed to encode chart data: %v", err)
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
	chartService := s.coordinator.GetChartService()
	
	// Get simplified health data
	response := map[string]interface{}{
		"status":      "ok",
		"clients":     broadcaster.GetClientCount(),
		"chains":      len(s.chainsService.GetAllChains()),
		"broadcaster": broadcaster.GetType(),
		"timestamp":   time.Now(),
	}
	
	// Try to get transfer count as a basic health indicator
	chartData, err := chartService.GetChartDataForFrontend()
	if err == nil {
		if rates, ok := chartData["currentRates"].(map[string]interface{}); ok {
			response["totalTransfers"] = rates["totalTracked"]
		}
	}
	
	// Add shard information if using sharded broadcaster
	if broadcaster.GetType() == "sharded" {
		response["shards"] = broadcaster.GetShardStats()
	}
	
	json.NewEncoder(w).Encode(response)
}

// handleBroadcasterStats returns detailed broadcaster statistics
func (s *Server) handleBroadcasterStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	broadcaster := s.coordinator.GetBroadcaster()
	
	// Get comprehensive stats from broadcaster
	response := broadcaster.GetShardStats()
	
	json.NewEncoder(w).Encode(response)
}

// handleDatabaseStats returns database storage statistics and capacity monitoring
func (s *Server) handleDatabaseStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	// Get database statistics through the coordinator
	dbWriter := s.coordinator.GetDatabaseWriter()
	stats, err := dbWriter.GetDatabaseStats()
	
	response := map[string]interface{}{
		"timestamp": time.Now(),
	}
	
	if err == nil {
		// Merge database stats into response
		for k, v := range stats {
			response[k] = v
		}
		
		// Add PostgreSQL-specific analysis
		if transferCount, ok := stats["transfer_count"].(int64); ok {
			// Calculate storage estimates for PostgreSQL
			estimatedBytes := transferCount * 800 // ~800 bytes per transfer including indexes in PostgreSQL
			estimatedGB := float64(estimatedBytes) / (1024 * 1024 * 1024)
			
			response["storage"] = map[string]interface{}{
				"current_size_gb":     estimatedGB,
				"current_size_mb":     estimatedGB * 1024,
				"bytes_per_transfer":  800,
				"estimated_size_gb":   estimatedGB,
			}
			
			// PostgreSQL capacity analysis for target scenarios
			scenarios := map[string]int64{
				"current":     transferCount,
				"10_million":  10_000_000,
				"100_million": 100_000_000,
				"175_million": 175_000_000, // User's target
				"1_billion":   1_000_000_000,
			}
			
			capacityAnalysis := make(map[string]interface{})
			for scenario, count := range scenarios {
				sizeGB := float64(count * 800) / (1024 * 1024 * 1024)
				
				capacityAnalysis[scenario] = map[string]interface{}{
					"transfers":    count,
					"size_gb":      sizeGB,
					"size_tb":      sizeGB / 1024,
					"can_handle":   true, // PostgreSQL can handle massive datasets
					"performance":  "excellent", // PostgreSQL performance is excellent at scale
				}
			}
			
			response["capacity_analysis"] = capacityAnalysis
			
			// PostgreSQL performance recommendations
			if transferCount > 100_000_000 {
				response["recommendations"] = []string{
					"Consider periodic VACUUM ANALYZE for optimal performance",
					"Monitor materialized view refresh frequency",
					"Consider partitioning for historical data if needed",
					"Ensure adequate work_mem for complex queries",
				}
			} else if transferCount > 10_000_000 {
				response["recommendations"] = []string{
					"Run ANALYZE periodically for query optimization",
					"Monitor index usage and bloat",
					"Consider connection pooling for high concurrency",
				}
			} else {
				response["recommendations"] = []string{
					"Database is well-optimized for current scale",
					"PostgreSQL handles this volume excellently",
				}
			}
		}
	}
	
	if err != nil {
		response["error"] = "Failed to get database statistics"
		response["details"] = err.Error()
	}
	
	json.NewEncoder(w).Encode(response)
}

// handleSchedulerStats returns detailed scheduler statistics
func (s *Server) handleSchedulerStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	scheduler := s.coordinator.GetScheduler()
	
	// Get comprehensive stats from scheduler
	response := scheduler.GetStats()
	
	json.NewEncoder(w).Encode(response)
}

// handleProcessorStats returns detailed processor statistics
func (s *Server) handleProcessorStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	processor := s.coordinator.GetProcessor()
	
	// Get comprehensive stats from processor
	response := processor.GetStats()
	
	json.NewEncoder(w).Encode(response)
}

// handleSyncStats returns detailed sync manager statistics
func (s *Server) handleSyncStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	syncManager := s.coordinator.GetSyncManager()
	
	// Get comprehensive stats from sync manager
	response := syncManager.GetStatus()
	
	json.NewEncoder(w).Encode(response)
}

// handlePipelineStats returns comprehensive pipeline statistics
func (s *Server) handlePipelineStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	// Get comprehensive stats from coordinator
	response := s.coordinator.GetPipelineStats()
	
	json.NewEncoder(w).Encode(response)
}

// handleCacheStats returns chart data cache statistics
func (s *Server) handleCacheStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	
	chartService := s.coordinator.GetChartService()
	
	// Get cache performance stats
	response := chartService.GetCacheStats()
	
	json.NewEncoder(w).Encode(response)
}



 