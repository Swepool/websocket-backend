package services

import (
	"context"
	"sync"
	"time"
	"websocket-backend/api/graphql"
	"websocket-backend/internal/utils"
	"websocket-backend/models"
)

// LatencyConfig holds latency monitoring configuration
type LatencyConfig struct {
	GraphQLURL       string        `json:"graphqlUrl"`       // GraphQL endpoint for latency data
	CheckInterval    time.Duration `json:"checkInterval"`    // How often to check latency (default: 2 minutes)
	RequestTimeout   time.Duration `json:"requestTimeout"`   // Timeout for individual latency checks
	MaxConcurrency   int           `json:"maxConcurrency"`   // Max concurrent latency checks
	HistoryRetention time.Duration `json:"historyRetention"` // How long to keep latency history
}

// DefaultLatencyConfig returns default latency monitoring configuration
func DefaultLatencyConfig() LatencyConfig {
	return LatencyConfig{
		GraphQLURL:       "https://staging.graphql.union.build/v1/graphql",
		CheckInterval:    2 * time.Minute, // Same as chains for latency data
		RequestTimeout:   5 * time.Second,
		MaxConcurrency:   20,
		HistoryRetention: 24 * time.Hour,
	}
}

// LatencyUpdateCallback is called when latency data is updated
type LatencyUpdateCallback func([]models.LatencyData)

// LatencyService manages cross-chain latency monitoring
type LatencyService struct {
	config           LatencyConfig
	graphql          *graphql.Client
	chainsService    *ChainsService // Reference to chains service for chain data
	latencyData      map[string]models.LatencyData // Key: source_chain-dest_chain
	mu               sync.RWMutex
	latencyCallback  LatencyUpdateCallback
	semaphore        chan struct{} // For limiting concurrent checks
}

// NewLatencyService creates a new latency monitoring service
func NewLatencyService(config LatencyConfig, chainsService *ChainsService) *LatencyService {
	return &LatencyService{
		config:        config,
		graphql:       graphql.NewClient(config.GraphQLURL),
		chainsService: chainsService,
		latencyData:   make(map[string]models.LatencyData),
		semaphore:     make(chan struct{}, config.MaxConcurrency),
	}
}

// SetLatencyCallback sets the callback function for latency updates
func (s *LatencyService) SetLatencyCallback(callback LatencyUpdateCallback) {
	s.latencyCallback = callback
}

// Start begins the latency monitoring service
func (s *LatencyService) Start(ctx context.Context) {
	utils.LogInfo("LATENCY_SERVICE", "Starting latency monitoring service")

	// Wait for chains to be loaded before starting latency monitoring
	go func() {
		// Wait 10 seconds for chains service to load chains
		timer := time.NewTimer(10 * time.Second)
		defer timer.Stop()
		
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			// Initial latency check after delay
			s.refreshLatencyData(ctx)
		}
	}()

	ticker := time.NewTicker(s.config.CheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			utils.LogInfo("LATENCY_SERVICE", "Stopping latency monitoring service")
			return
		case <-ticker.C:
			s.refreshLatencyData(ctx)
		}
	}
}

// FetchLatencyData fetches latency statistics for all chain pairs
func (s *LatencyService) FetchLatencyData(ctx context.Context) ([]models.LatencyData, error) {
	if s.chainsService == nil {
		utils.LogWarn("LATENCY_SERVICE", "No chains service available for latency data")
		return []models.LatencyData{}, nil
	}

	chains := s.chainsService.GetAllChains()
	if len(chains) < 2 {
		utils.LogWarn("LATENCY_SERVICE", "Not enough chains for latency data (%d chains)", len(chains))
		return []models.LatencyData{}, nil // Need at least 2 chains for latency data
	}

	var latencyData []models.LatencyData
	successCount := 0
	errorCount := 0

	// Use semaphore to limit concurrent requests
	var wg sync.WaitGroup
	var latencyMu sync.Mutex

	// Fetch latency for each chain pair
	for i, sourceChain := range chains {
		for j, destChain := range chains {
			if i == j {
				continue // Skip same chain
			}

			wg.Add(1)
			go func(source, dest models.Chain) {
				defer wg.Done()

				// Acquire semaphore to limit concurrency
				s.semaphore <- struct{}{}
				defer func() { <-s.semaphore }()

				// Check context cancellation before each request
				select {
				case <-ctx.Done():
					utils.LogInfo("LATENCY_SERVICE", "Context cancelled, stopping latency fetch")
					return
				default:
				}

				// Use shorter timeout for individual requests to staging endpoint
				latencyCtx, latencyCancel := context.WithTimeout(ctx, s.config.RequestTimeout)
				latency, err := s.graphql.FetchLatency(latencyCtx, source.UniversalChainID, dest.UniversalChainID)
				latencyCancel()

				latencyMu.Lock()
				defer latencyMu.Unlock()

				if err != nil {
					errorCount++
					// Log error but continue with other pairs
					utils.LogWarn("LATENCY_SERVICE", "Failed to fetch latency for %s -> %s: %v", source.DisplayName, dest.DisplayName, err)
					return
				}

				if latency != nil {
					// Enhance with chain display names
					latency.SourceName = source.DisplayName
					latency.DestinationName = dest.DisplayName
					latencyData = append(latencyData, *latency)
					successCount++
				}
			}(sourceChain, destChain)
		}
	}

	wg.Wait()

	if successCount > 0 {
		utils.LogInfo("LATENCY_SERVICE", "Successfully fetched latency data for %d chain pairs (%d failed)", successCount, errorCount)
	} else if errorCount > 0 {
		utils.LogWarn("LATENCY_SERVICE", "Failed to fetch latency data for all %d chain pairs attempted", errorCount)
	}

	return latencyData, nil
}

// refreshLatencyData fetches and updates latency data
func (s *LatencyService) refreshLatencyData(ctx context.Context) {
	// Run latency fetching in background so it doesn't block shutdown
	go func() {
		// Add timeout for staging endpoint but don't block main thread
		latencyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		latencyData, err := s.FetchLatencyData(latencyCtx)
		if err != nil {
			utils.LogError("LATENCY_SERVICE", "Failed to fetch latency data: %v", err)
			return
		}

		// Update stored latency data
		s.updateLatencyData(latencyData)

		// Only call callback if we have latency data or if callback is set
		if s.latencyCallback != nil && len(latencyData) > 0 {
			s.latencyCallback(latencyData)
			utils.LogInfo("LATENCY_SERVICE", "Updated latency data for %d chain pairs", len(latencyData))
		} else if len(latencyData) == 0 {
			utils.LogWarn("LATENCY_SERVICE", "No latency data fetched - chains may not be loaded yet")
		}
	}()
}

// updateLatencyData updates the stored latency data atomically
func (s *LatencyService) updateLatencyData(latencyData []models.LatencyData) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update latency data (key: source-dest pair)
	for _, data := range latencyData {
		key := data.SourceChain + "-" + data.DestinationChain
		s.latencyData[key] = data
	}
}

// GetLatencyData returns current latency data
func (s *LatencyService) GetLatencyData() []models.LatencyData {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]models.LatencyData, 0, len(s.latencyData))
	for _, data := range s.latencyData {
		result = append(result, data)
	}

	return result
}

// GetLatencyDataInterface returns current latency data as interface{} for the chart broadcaster
func (s *LatencyService) GetLatencyDataInterface() []interface{} {
	latencyData := s.GetLatencyData()
	result := make([]interface{}, len(latencyData))
	for i, data := range latencyData {
		result[i] = data
	}
	return result
}

// GetLatencyForPair returns latency data for a specific chain pair
func (s *LatencyService) GetLatencyForPair(sourceChain, destChain string) (models.LatencyData, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := sourceChain + "-" + destChain
	data, exists := s.latencyData[key]
	return data, exists
}

// GetLatencyStats returns statistics about latency monitoring
func (s *LatencyService) GetLatencyStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	totalPairs := len(s.latencyData)
	totalLatency := 0.0
	maxLatency := 0.0
	minLatency := -1.0

	for _, data := range s.latencyData {
		// Use PacketAck median as the primary latency metric
		latency := data.PacketAck.Median
		if latency > 0 {
			totalLatency += latency
			if latency > maxLatency {
				maxLatency = latency
			}
			if minLatency == -1 || latency < minLatency {
				minLatency = latency
			}
		}
	}

	avgLatency := 0.0
	if totalPairs > 0 {
		avgLatency = totalLatency / float64(totalPairs)
	}

	return map[string]interface{}{
		"total_pairs":     totalPairs,
		"successful":      totalPairs, // All pairs with data are considered successful
		"failed":          0,          // We don't track failures in this version
		"average_latency": avgLatency,
		"max_latency":     maxLatency,
		"min_latency":     minLatency,
	}
} 