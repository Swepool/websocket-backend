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
	
	// We need to access the database writer through the coordinator
	// For now, we'll get basic stats from the chart service and add capacity estimates
	chartService := s.coordinator.GetChartService()
	
	response := map[string]interface{}{
		"timestamp": time.Now(),
	}
	
	// Get current transfer count from chart data
	chartData, err := chartService.GetChartDataForFrontend()
	if err == nil {
		if rates, ok := chartData["currentRates"].(map[string]interface{}); ok {
			if totalTracked, ok := rates["totalTracked"].(int64); ok {
				response["current_transfers"] = totalTracked
				
				// Calculate storage estimates
				estimatedBytes := totalTracked * 639 // 639 bytes per transfer including indexes
				estimatedGB := float64(estimatedBytes) / (1024 * 1024 * 1024)
				
				response["storage"] = map[string]interface{}{
					"current_size_gb":     estimatedGB,
					"current_size_mb":     estimatedGB * 1024,
					"bytes_per_transfer":  639,
					"estimated_size_gb":   estimatedGB,
				}
				
				// Capacity analysis for target scenarios
				scenarios := map[string]int64{
					"current":     totalTracked,
					"10_million":  10_000_000,
					"100_million": 100_000_000,
					"175_million": 175_000_000, // User's target
					"1_billion":   1_000_000_000,
				}
				
				capacityAnalysis := make(map[string]interface{})
				for scenario, count := range scenarios {
					sizeGB := float64(count * 639) / (1024 * 1024 * 1024)
					
					capacityAnalysis[scenario] = map[string]interface{}{
						"transfers":    count,
						"size_gb":      sizeGB,
						"size_tb":      sizeGB / 1024,
						"can_handle":   sizeGB < 281*1024, // SQLite max 281TB
						"performance":  getPerformanceTier(count),
					}
				}
				
				response["capacity_analysis"] = capacityAnalysis
				
				// SQLite limits
				response["sqlite_limits"] = map[string]interface{}{
					"max_size_tb":    281,
					"max_rows":       int64(9223372036854775807), // 2^63 - 1 (max int64)
					"current_usage":  (estimatedGB / 1024) / 281 * 100, // Percentage of max
				}
				
				// Performance recommendations
				if totalTracked > 100_000_000 {
					response["recommendations"] = []string{
						"Consider monthly maintenance (ANALYZE, VACUUM)",
						"Monitor query performance on complex aggregations",
						"Consider archiving old transfers if performance degrades",
						"Increase cache_size if available memory allows",
					}
				} else if totalTracked > 10_000_000 {
					response["recommendations"] = []string{
						"Run quarterly maintenance (ANALYZE)",
						"Monitor database file size growth",
						"Consider WAL checkpoint frequency tuning",
					}
				} else {
					response["recommendations"] = []string{
						"Database size is optimal",
						"No special maintenance required",
					}
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

// getPerformanceTier returns performance assessment for transfer count
func getPerformanceTier(transfers int64) string {
	if transfers > 1_000_000_000 {
		return "very_large_may_be_slow"
	} else if transfers > 100_000_000 {
		return "large_moderate_performance"
	} else if transfers > 10_000_000 {
		return "medium_good_performance"
	} else {
		return "small_excellent_performance"
	}
}

 