package services

import (
	"context"
	"sync"
	"time"
	"websocket-backend/api/graphql"
	"websocket-backend/internal/utils"
	"websocket-backend/models"
)

// ChainsConfig holds chains service configuration
type ChainsConfig struct {
	GraphQLURL      string        `json:"graphqlUrl"`      // GraphQL endpoint URL
	RefreshInterval time.Duration `json:"refreshInterval"` // How often to refresh chains (default: 10 minutes)
}

// DefaultChainsConfig returns default chains configuration
func DefaultChainsConfig() ChainsConfig {
	return ChainsConfig{
		GraphQLURL:      "https://staging.graphql.union.build/v1/graphql",
		RefreshInterval: 10 * time.Minute, // Standard refresh interval for chains
	}
}

// ChainsService manages blockchain chain information
type ChainsService struct {
	config     ChainsConfig
	graphql    *graphql.Client
	chains     []models.Chain
	mu         sync.RWMutex
	refreshing bool
}

// NewChainsService creates a new chains service
func NewChainsService(config ChainsConfig) *ChainsService {
	return &ChainsService{
		config:  config,
		graphql: graphql.NewClient(config.GraphQLURL),
		chains:  []models.Chain{},
	}
}

// Start begins the chains service
func (s *ChainsService) Start(ctx context.Context) {
	utils.LogInfo("CHAINS_SERVICE", "Starting chains service")
	
	// Initial fetch
	if err := s.refreshChains(ctx); err != nil {
		utils.LogError("CHAINS_SERVICE", "Failed to fetch initial chains: %v", err)
		return
	}
	utils.LogInfo("CHAINS_SERVICE", "Initial chains fetch completed")
	
	ticker := time.NewTicker(s.config.RefreshInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			utils.LogInfo("CHAINS_SERVICE", "Stopping chains service")
			return
		case <-ticker.C:
			if err := s.refreshChains(ctx); err != nil {
				utils.LogWarn("CHAINS_SERVICE", "Failed to refresh chains: %v", err)
			}
		}
	}
}

// GetAllChains returns all chains
func (s *ChainsService) GetAllChains() []models.Chain {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	// Return a copy to prevent external modifications
	result := make([]models.Chain, len(s.chains))
	copy(result, s.chains)
	return result
}

// GetChainByID returns a chain by its universal chain ID
func (s *ChainsService) GetChainByID(universalChainID string) (models.Chain, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	for _, chain := range s.chains {
		if chain.UniversalChainID == universalChainID {
			return chain, true
		}
	}
	
	return models.Chain{}, false
}

// GetChainPairs returns all possible chain pairs for cross-chain operations
func (s *ChainsService) GetChainPairs() []ChainPair {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	var pairs []ChainPair
	for i, sourceChain := range s.chains {
		for j, destChain := range s.chains {
			if i != j { // Don't include same chain pairs
				pairs = append(pairs, ChainPair{
					Source:      sourceChain.UniversalChainID,
					Destination: destChain.UniversalChainID,
				})
			}
		}
	}
	
	return pairs
}

// ChainPair represents a source-destination chain pair
type ChainPair struct {
	Source      string
	Destination string
}

// GetChainCount returns the number of chains
func (s *ChainsService) GetChainCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.chains)
}

// refreshChains fetches the latest chains from GraphQL
func (s *ChainsService) refreshChains(ctx context.Context) error {
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
	
	utils.LogInfo("CHAINS_SERVICE", "🔍 Attempting to fetch chains from GraphQL API...")
	
	// Fetch chains from GraphQL API
	graphqlChains, err := s.graphql.FetchChains(ctx)
	if err != nil {
		utils.LogError("CHAINS_SERVICE", "❌ GraphQL fetch failed: %v", err)
		return err
	}
	
	utils.LogInfo("CHAINS_SERVICE", "✅ GraphQL fetch successful: got %d chains", len(graphqlChains))
	
	// Log some chain details for debugging
	if len(graphqlChains) > 0 {
		utils.LogInfo("CHAINS_SERVICE", "🔍 Sample chains: %s, %s", 
			graphqlChains[0].DisplayName, 
			func() string {
				if len(graphqlChains) > 1 {
					return graphqlChains[1].DisplayName
				}
				return "only 1 chain"
			}())
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
	
	utils.LogInfo("CHAINS_SERVICE", "Successfully fetched and stored %d chains", len(chains))
	return nil
} 