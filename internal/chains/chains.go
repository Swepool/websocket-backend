package chains

import (
	"context"
	"fmt"
	"sync"
	"time"
	"websocket-backend-new/api/graphql"
	"websocket-backend-new/models"
)

// Config holds chains service configuration
type Config struct {
	GraphQLURL      string        `json:"graphqlUrl"`      // GraphQL endpoint URL
	RefreshInterval time.Duration `json:"refreshInterval"` // How often to refresh chains (default: 1 hour)
}

// DefaultConfig returns default chains configuration
func DefaultConfig() Config {
	return Config{
		GraphQLURL:      "https://staging.graphql.union.build/v1/graphql",
		RefreshInterval: time.Hour,
	}
}

// Service manages chain information
type Service struct {
	config     Config
	graphql    *graphql.Client
	chains     []models.Chain
	mu         sync.RWMutex
	refreshing bool
}

// NewService creates a new chains service
func NewService(config Config) *Service {
	return &Service{
		config:  config,
		graphql: graphql.NewClient(config.GraphQLURL),
		chains:  []models.Chain{},
	}
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