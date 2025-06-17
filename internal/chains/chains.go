package chains

import (
	"context"
	"fmt"
	"sync"
	"time"
	"websocket-backend-new/api/graphql"
	"websocket-backend-new/models"
)

// LatencyUpdateCallback is called when latency data is updated
type LatencyUpdateCallback func([]models.LatencyData)

// Config holds chains service configuration
type Config struct {
	GraphQLURL      string        `json:"graphqlUrl"`      // GraphQL endpoint URL
	RefreshInterval time.Duration `json:"refreshInterval"` // How often to refresh chains (default: 10 minutes)
}

// DefaultConfig returns default chains configuration
func DefaultConfig() Config {
	return Config{
		GraphQLURL:      "https://staging.graphql.union.build/v1/graphql",
		RefreshInterval: 10 * time.Minute,
	}
}

// Service manages chain information
type Service struct {
	config            Config
	graphql           *graphql.Client
	chains            []models.Chain
	mu                sync.RWMutex
	refreshing        bool
	latencyCallback   LatencyUpdateCallback
}

// NewService creates a new chains service
func NewService(config Config) *Service {
	return &Service{
		config:  config,
		graphql: graphql.NewClient(config.GraphQLURL),
		chains:  []models.Chain{},
	}
}

// SetLatencyCallback sets the callback function for latency updates
func (s *Service) SetLatencyCallback(callback LatencyUpdateCallback) {
	s.latencyCallback = callback
}

// Start begins the chains service
func (s *Service) Start(ctx context.Context) {
	fmt.Printf("[CHAINS] Starting chains service...\n")
	// Initial fetch
	if err := s.refreshChains(ctx); err != nil {
		fmt.Printf("[CHAINS] Failed to fetch initial chains: %v\n", err)
		return
	}
	fmt.Printf("[CHAINS] Initial chains fetch completed\n")
	
	// Initial latency fetch
	s.refreshLatencyData(ctx)
	
	ticker := time.NewTicker(s.config.RefreshInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.refreshChains(ctx); err != nil {
				continue
			}
			// Also refresh latency data
			s.refreshLatencyData(ctx)
		}
	}
}

// GetAllChains returns all chains
func (s *Service) GetAllChains() []models.Chain {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	// Return a copy to prevent external modifications
	result := make([]models.Chain, len(s.chains))
	copy(result, s.chains)
	return result
}

// GetChainByID returns a chain by its universal chain ID
func (s *Service) GetChainByID(universalChainID string) (models.Chain, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	for _, chain := range s.chains {
		if chain.UniversalChainID == universalChainID {
			return chain, true
		}
	}
	
	return models.Chain{}, false
}

// FetchLatencyData fetches latency statistics for all chain pairs
func (s *Service) FetchLatencyData(ctx context.Context) ([]models.LatencyData, error) {
	s.mu.RLock()
	chains := make([]models.Chain, len(s.chains))
	copy(chains, s.chains)
	s.mu.RUnlock()
	
	if len(chains) < 2 {
		fmt.Printf("[CHAINS] Not enough chains for latency data (%d chains)\n", len(chains))
		return []models.LatencyData{}, nil // Need at least 2 chains for latency data
	}
	
	var latencyData []models.LatencyData
	successCount := 0
	errorCount := 0
	
	// Fetch latency for each chain pair
	for i, sourceChain := range chains {
		for j, destChain := range chains {
			if i == j {
				continue // Skip same chain
			}
			
			// Check context cancellation before each request
			select {
			case <-ctx.Done():
				fmt.Printf("[CHAINS] Context cancelled, stopping latency fetch\n")
				return latencyData, nil
			default:
			}
			
			// Use shorter timeout for individual requests to staging endpoint
			latencyCtx, latencyCancel := context.WithTimeout(ctx, 5*time.Second)
			latency, err := s.graphql.FetchLatency(latencyCtx, sourceChain.UniversalChainID, destChain.UniversalChainID)
			latencyCancel()
			
			if err != nil {
				errorCount++
				// Log error but continue with other pairs
				fmt.Printf("[CHAINS] Failed to fetch latency for %s -> %s: %v\n", sourceChain.DisplayName, destChain.DisplayName, err)
				continue
			}
			
			if latency != nil {
				// Enhance with chain display names
				latency.SourceName = sourceChain.DisplayName
				latency.DestinationName = destChain.DisplayName
				latencyData = append(latencyData, *latency)
				successCount++
			}
		}
	}
	
	if successCount > 0 {
		fmt.Printf("[CHAINS] Successfully fetched latency data for %d chain pairs (%d failed)\n", successCount, errorCount)
	} else if errorCount > 0 {
		fmt.Printf("[CHAINS] Failed to fetch latency data for all %d chain pairs attempted\n", errorCount)
	}
	
	return latencyData, nil
}

// refreshChains fetches the latest chains from GraphQL
func (s *Service) refreshChains(ctx context.Context) error {
	s.mu.Lock()
	if s.refreshing {
		s.mu.Unlock()
		return nil // Already refreshing
	}
	s.refreshing = true
	s.mu.Unlock()
	
	defer func() {
		s.mu.Lock()
		s.refreshing = false
		s.mu.Unlock()
	}()
	
	// Fetch chains from GraphQL API
	graphqlChains, err := s.graphql.FetchChains(ctx)
	if err != nil {
		return err
	}
	
	// Convert to Chain format
	chains := make([]models.Chain, len(graphqlChains))
	for i, chain := range graphqlChains {
		chains[i] = models.Chain{
			UniversalChainID: chain.UniversalChainID,
			ChainID:          chain.ChainID,
			DisplayName:      chain.DisplayName,
			Testnet:          chain.Testnet,
			RpcType:          chain.RpcType,
			AddrPrefix:       chain.AddrPrefix,
		}
	}
	
	// Update chains atomically
	s.mu.Lock()
	s.chains = chains
	s.mu.Unlock()
	
	fmt.Printf("[CHAINS] Successfully fetched and stored %d chains\n", len(chains))
	return nil
}

// refreshLatencyData fetches and updates latency data
func (s *Service) refreshLatencyData(ctx context.Context) {
	if s.latencyCallback == nil {
		return // No callback set, skip latency fetch
	}
	
	// Run latency fetching in background so it doesn't block shutdown
	go func() {
		// Add timeout for staging endpoint but don't block main thread
		latencyCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		
		latencyData, err := s.FetchLatencyData(latencyCtx)
		if err != nil {
			fmt.Printf("[CHAINS] Failed to fetch latency data: %v\n", err)
			return
		}
		
		// Call the callback to update latency data
		s.latencyCallback(latencyData)
	}()
} 