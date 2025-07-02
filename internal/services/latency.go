package services

import (
	"context"
	"sync"
	"time"
	"websocket-backend/api/graphql"
	"websocket-backend/internal/utils"
	"websocket-backend/models"
)

// Constants for latency service
const (
	ChainsLoadDelay       = 5 * time.Second  // Wait time for chains service to load (reduced for faster cache population)
	LatencyFetchTimeout   = 30 * time.Second // Timeout for latency data fetching
	MinChainsForLatency   = 2                // Minimum chains needed for latency monitoring
)

// LatencyConfig holds latency monitoring configuration
type LatencyConfig struct {
	GraphQLURL     string        `json:"graphqlUrl"`     // GraphQL endpoint for latency data
	CheckInterval  time.Duration `json:"checkInterval"`  // How often to check latency (default: 2 minutes)
	RequestTimeout time.Duration `json:"requestTimeout"` // Timeout for individual latency checks
	MaxConcurrency int           `json:"maxConcurrency"` // Max concurrent latency checks
}

// DefaultLatencyConfig returns default latency monitoring configuration  
func DefaultLatencyConfig() LatencyConfig {
	return LatencyConfig{
		GraphQLURL:     "https://staging.graphql.union.build/v1/graphql",
		CheckInterval:  3 * time.Minute, // Every 3 minutes (well within 6min TTL, less aggressive than 2min)
		RequestTimeout: 5 * time.Second, // Back to original
		MaxConcurrency: 20, // Back to original - partial success is better than none
	}
}

// LatencyUpdateCallback is called when latency data is updated
type LatencyUpdateCallback func([]models.LatencyData)

// LatencyService manages cross-chain latency monitoring
type LatencyService struct {
	config          LatencyConfig
	graphql         *graphql.Client
	chainsService   *ChainsService // Reference to chains service for chain data
	latencyData     map[string]models.LatencyData // Key: source_chain-dest_chain
	mu              sync.RWMutex
	latencyCallback LatencyUpdateCallback
	semaphore       chan struct{} // For limiting concurrent checks
	cache           CacheInterface // Cache interface for storing latency data
}

// CacheInterface defines the interface for caching latency data
type CacheInterface interface {
	SetWithTTL(key string, data interface{}, ttl time.Duration)
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

// SetCache sets the cache interface for storing latency data
func (s *LatencyService) SetCache(cache CacheInterface) {
	s.cache = cache
}

// Start begins the latency monitoring service
func (s *LatencyService) Start(ctx context.Context) {
	utils.LogInfo("LATENCY_SERVICE", "Starting latency monitoring service")

	// Wait for chains to be loaded before starting latency monitoring
	go func() {
		timer := time.NewTimer(ChainsLoadDelay)
		defer timer.Stop()
		
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
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

// generateLatencyKey creates a consistent key for latency data storage
func (s *LatencyService) generateLatencyKey(sourceChain, destChain string) string {
	return sourceChain + "-" + destChain
}

// fetchLatencyForPair fetches latency data for a single chain pair
func (s *LatencyService) fetchLatencyForPair(ctx context.Context, source, dest models.Chain) (*models.LatencyData, error) {
	latencyCtx, cancel := context.WithTimeout(ctx, s.config.RequestTimeout)
	defer cancel()

	latency, err := s.graphql.FetchLatency(latencyCtx, source.UniversalChainID, dest.UniversalChainID)
	if err != nil {
		return nil, err
	}

	if latency != nil {
		// Enhance with chain display names
		latency.SourceName = source.DisplayName
		latency.DestinationName = dest.DisplayName
	}

	return latency, nil
}

// validateChainsForLatency checks if we have enough chains for latency monitoring
func (s *LatencyService) validateChainsForLatency() ([]models.Chain, error) {
	if s.chainsService == nil {
		utils.LogWarn("LATENCY_SERVICE", "No chains service available for latency data")
		return []models.Chain{}, nil
	}

	chains := s.chainsService.GetAllChains()
	if len(chains) < MinChainsForLatency {
		utils.LogWarn("LATENCY_SERVICE", "Not enough chains for latency data (%d chains)", len(chains))
		return []models.Chain{}, nil
	}

	return chains, nil
}

// FetchLatencyData fetches latency statistics for all chain pairs
func (s *LatencyService) FetchLatencyData(ctx context.Context) ([]models.LatencyData, error) {
	chains, err := s.validateChainsForLatency()
	if err != nil || len(chains) == 0 {
		return []models.LatencyData{}, err
	}

	var latencyData []models.LatencyData
	var successCount, errorCount int
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

				// No artificial delays - let it run as it did before

				// Check context cancellation before each request
				select {
				case <-ctx.Done():
					utils.LogInfo("LATENCY_SERVICE", "Context cancelled, stopping latency fetch")
					return
				default:
				}

				latency, err := s.fetchLatencyForPair(ctx, source, dest)

				latencyMu.Lock()
				defer latencyMu.Unlock()

				if err != nil {
					errorCount++
					utils.LogWarn("LATENCY_SERVICE", "Failed to fetch latency for %s -> %s: %v", source.DisplayName, dest.DisplayName, err)
					return
				}

				if latency != nil {
					latencyData = append(latencyData, *latency)
					successCount++
				}
			}(sourceChain, destChain)
		}
	}

	wg.Wait()

	s.logFetchResults(successCount, errorCount)
	return latencyData, nil
}

// logFetchResults logs the results of latency data fetching
func (s *LatencyService) logFetchResults(successCount, errorCount int) {
	if successCount > 0 {
		utils.LogInfo("LATENCY_SERVICE", "Successfully fetched latency data for %d chain pairs (%d failed)", successCount, errorCount)
	} else if errorCount > 0 {
		utils.LogWarn("LATENCY_SERVICE", "Failed to fetch latency data for all %d chain pairs attempted", errorCount)
	}
}

// refreshLatencyData fetches and updates latency data
func (s *LatencyService) refreshLatencyData(ctx context.Context) {
	go func() {
		latencyCtx, cancel := context.WithTimeout(ctx, LatencyFetchTimeout)
		defer cancel()

		latencyData, err := s.FetchLatencyData(latencyCtx)
		if err != nil {
			utils.LogError("LATENCY_SERVICE", "Failed to fetch latency data: %v", err)
			return
		}

		s.updateLatencyData(latencyData)

		if s.latencyCallback != nil && len(latencyData) > 0 {
			s.latencyCallback(latencyData)
			utils.LogInfo("LATENCY_SERVICE", "Updated latency data for %d chain pairs", len(latencyData))
		} else if len(latencyData) == 0 {
			utils.LogWarn("LATENCY_SERVICE", "No latency data fetched - chains may not be loaded yet")
		}
	}()
}

// updateLatencyData updates the stored latency data atomically and caches it
func (s *LatencyService) updateLatencyData(latencyData []models.LatencyData) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Update latency data (key: source-dest pair)
	for _, data := range latencyData {
		key := s.generateLatencyKey(data.SourceChain, data.DestinationChain)
		s.latencyData[key] = data
	}
	
	// Cache the latency data for chart broadcaster (6-minute TTL to match other chart data)
	if s.cache != nil && len(latencyData) > 0 {
		// Convert to interface{} slice for cache storage
		interfaceData := make([]interface{}, len(latencyData))
		for i, data := range latencyData {
			interfaceData[i] = data
		}
		s.cache.SetWithTTL("latency_data", interfaceData, 6*time.Minute)
		utils.LogInfo("LATENCY_SERVICE", "✅ Cached %d latency data points with 6-minute TTL", len(latencyData))
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



// GetLatencyForPair returns latency data for a specific chain pair
func (s *LatencyService) GetLatencyForPair(sourceChain, destChain string) (models.LatencyData, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	key := s.generateLatencyKey(sourceChain, destChain)
	data, exists := s.latencyData[key]
	return data, exists
}

// calculateLatencyStats computes statistical metrics from latency data
func (s *LatencyService) calculateLatencyStats() (totalPairs int, avgLatency, maxLatency, minLatency float64) {
	totalLatency := 0.0
	minLatency = -1.0

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

	totalPairs = len(s.latencyData)
	if totalPairs > 0 {
		avgLatency = totalLatency / float64(totalPairs)
	}

	return
}

// GetLatencyStats returns statistics about latency monitoring
func (s *LatencyService) GetLatencyStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	totalPairs, avgLatency, maxLatency, minLatency := s.calculateLatencyStats()

	return map[string]interface{}{
		"total_pairs":     totalPairs,
		"successful":      totalPairs, // All pairs with data are considered successful
		"failed":          0,          // We don't track failures in this version
		"average_latency": avgLatency,
		"max_latency":     maxLatency,
		"min_latency":     minLatency,
	}
} 