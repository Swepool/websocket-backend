package chains

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
	"websocket-backend-new/internal/models"
)

// Config holds chains service configuration
type Config struct {
	GraphQLURL      string        `json:"graphqlUrl"`      // GraphQL endpoint URL
	RefreshInterval time.Duration `json:"refreshInterval"` // How often to refresh chains (default: 5 minutes)
}

// DefaultConfig returns default chains service configuration
func DefaultConfig() Config {
	return Config{
		GraphQLURL:      "https://staging.graphql.union.build/v1/graphql",
		RefreshInterval: 5 * time.Minute,
	}
}

// Service provides chain metadata
type Service struct {
	config Config
	mu     sync.RWMutex
	chains []models.ChainInfo
	client *http.Client
}

// NewService creates a new chains service
func NewService(config Config) *Service {
	return &Service{
		config: config,
		chains: []models.ChainInfo{},
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

// Start begins the chains service with periodic refresh
func (s *Service) Start(ctx context.Context) {
	fmt.Printf("[CHAINS] Starting chains service with GraphQL URL: %s\n", s.config.GraphQLURL)
	
	// Initial fetch
	if err := s.fetchChains(); err != nil {
		fmt.Printf("[CHAINS] Initial fetch failed: %v\n", err)
	}
	
	// Periodic refresh
	ticker := time.NewTicker(s.config.RefreshInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[CHAINS] Shutting down\n")
			return
		case <-ticker.C:
			if err := s.fetchChains(); err != nil {
				fmt.Printf("[CHAINS] Refresh failed: %v\n", err)
			}
		}
	}
}

// GetAllChains returns all available chains
func (s *Service) GetAllChains() []models.ChainInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	// Return a copy to avoid race conditions
	chains := make([]models.ChainInfo, len(s.chains))
	copy(chains, s.chains)
	return chains
}

// GetChainByID returns a chain by its universal chain ID
func (s *Service) GetChainByID(universalChainID string) *models.ChainInfo {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	for _, chain := range s.chains {
		if chain.UniversalChainID == universalChainID {
			chainCopy := chain
			return &chainCopy
		}
	}
	return nil
}

// GraphQL types for chains query
type chainsGraphQLResponse struct {
	Data struct {
		V2Chains []struct {
			UniversalChainID string `json:"universal_chain_id"`
			ChainID          string `json:"chain_id"`
			DisplayName      string `json:"display_name"`
			Testnet          bool   `json:"testnet"`
			RpcType          string `json:"rpc_type"`
			AddrPrefix       string `json:"addr_prefix"`
		} `json:"v2_chains"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// fetchChains fetches chains from GraphQL
func (s *Service) fetchChains() error {
	query := `
		query Chains {
			v2_chains {
				universal_chain_id
				chain_id
				display_name
				testnet
				rpc_type
				addr_prefix
			}
		}
	`
	
	requestBody := map[string]interface{}{
		"query": query,
	}
	
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return fmt.Errorf("failed to marshal GraphQL request: %w", err)
	}
	
	req, err := http.NewRequest("POST", s.config.GraphQLURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("Content-Type", "application/json")
	
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("GraphQL request failed: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GraphQL request returned status %d", resp.StatusCode)
	}
	
	var graphqlResp chainsGraphQLResponse
	if err := json.NewDecoder(resp.Body).Decode(&graphqlResp); err != nil {
		return fmt.Errorf("failed to decode GraphQL response: %w", err)
	}
	
	if len(graphqlResp.Errors) > 0 {
		return fmt.Errorf("GraphQL errors: %v", graphqlResp.Errors)
	}
	
	// Convert GraphQL response to our models
	chains := make([]models.ChainInfo, len(graphqlResp.Data.V2Chains))
	for i, chain := range graphqlResp.Data.V2Chains {
		chains[i] = models.ChainInfo{
			UniversalChainID: chain.UniversalChainID,
			ChainID:          chain.ChainID,
			DisplayName:      chain.DisplayName,
			Testnet:          chain.Testnet,
			RpcType:          chain.RpcType,
			AddrPrefix:       chain.AddrPrefix,
			LogoURL:          "", // Not available in v2_chains
			Enabled:          true, // Assume enabled if returned by API
		}
	}
	
	// Update chains with lock
	s.mu.Lock()
	s.chains = chains
	s.mu.Unlock()
	
	fmt.Printf("[CHAINS] Fetched %d chains from GraphQL\n", len(chains))
	return nil
} 