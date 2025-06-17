package fetcher

import (
	"context"
	"fmt"
	"log"
	"time"
	"websocket-backend-new/config"
	"websocket-backend-new/models"
	"websocket-backend-new/storage"
)

// GraphQLClient interface for GraphQL operations
type GraphQLClient interface {
	FetchLatestTransfers(ctx context.Context, limit int, networkFilter []string) ([]models.Transfer, error)
	FetchNewTransfers(ctx context.Context, lastSortOrder string, limit int, networkFilter []string) ([]models.Transfer, error)
	FetchChains(ctx context.Context) ([]models.Chain, error)
}

// Fetcher handles fetching transfers from GraphQL API
type Fetcher struct {
	config         *config.Config
	memory         *storage.Memory
	channels       *storage.Channels
	graphqlClient  GraphQLClient
}

// NewFetcher creates a new fetcher
func NewFetcher(config *config.Config, memory *storage.Memory, channels *storage.Channels, graphqlClient GraphQLClient) *Fetcher {
	return &Fetcher{
		config:         config,
		memory:         memory,
		channels:       channels,
		graphqlClient:  graphqlClient,
	}
}

// Start begins the main fetcher loop (Thread 1: Just Fetch)
func (f *Fetcher) Start(ctx context.Context) {
	log.Printf("[THREAD-1] 🚀 Starting fetcher (Fetch only)")
	
	// Main fetch loop
	ticker := time.NewTicker(f.config.PollInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			log.Printf("[THREAD-1] 🛑 Context cancelled, stopping fetcher")
			return
			
		case <-f.channels.Shutdown:
			log.Printf("[THREAD-1] 🛑 Shutdown signal received, stopping")
			return
			
		case <-ticker.C:
			f.fetchAndSend(ctx)
		}
	}
}

// fetchAndSend fetches new transfers and sends them for enhancement
func (f *Fetcher) fetchAndSend(ctx context.Context) {
	// Fetch raw transfers
	transfers, err := f.FetchNewTransfers(ctx)
	if err != nil {
		log.Printf("[THREAD-1] ❌ Failed to fetch transfers: %v", err)
		return
	}
	
	if len(transfers) == 0 {
		return
	}
	
	log.Printf("[THREAD-1] 📥 Fetched %d new transfers, sending for enhancement", len(transfers))
	
	// Send to enhancer (non-blocking)
	select {
	case f.channels.RawTransfers <- transfers:
		log.Printf("[THREAD-1] ✅ Sent %d transfers for enhancement", len(transfers))
	default:
		log.Printf("[THREAD-1] ⚠️ Enhancement channel full, dropping %d transfers", len(transfers))
	}
}

// FetchNewTransfers fetches new transfers from GraphQL API
func (f *Fetcher) FetchNewTransfers(ctx context.Context) ([]models.Transfer, error) {
	lastSortOrder, isInitial := f.memory.GetPollingState()
	
	if isInitial || lastSortOrder == "" {
		return f.establishBaseline(ctx)
	}
	
	return f.fetchNewTransfers(ctx, lastSortOrder)
}

// establishBaseline gets the initial baseline for polling
func (f *Fetcher) establishBaseline(ctx context.Context) ([]models.Transfer, error) {
	// Convert network filter from pointer to slice
	var networkFilter []string
	if netPtr := f.config.GetNetworkFilter(); netPtr != nil {
		networkFilter = []string{*netPtr}
	}
	
	transfers, err := f.graphqlClient.FetchLatestTransfers(ctx, 1, networkFilter)
	if err != nil {
		return nil, fmt.Errorf("FetchLatestTransfers error: %w", err)
	}

	if len(transfers) > 0 {
		f.memory.UpdatePollingState(transfers[0].SortOrder)
	}
	
	// Return empty slice for baseline - we don't want to process old transfers
	return []models.Transfer{}, nil
}

// fetchNewTransfers fetches and returns new transfers
func (f *Fetcher) fetchNewTransfers(ctx context.Context, lastSortOrder string) ([]models.Transfer, error) {
	// Convert network filter from pointer to slice
	var networkFilter []string
	if netPtr := f.config.GetNetworkFilter(); netPtr != nil {
		networkFilter = []string{*netPtr}
	}
	
	transfers, err := f.graphqlClient.FetchNewTransfers(ctx, lastSortOrder, f.config.PollLimit, networkFilter)
	if err != nil {
		return nil, fmt.Errorf("FetchNewTransfers error: %w", err)
	}

	if len(transfers) > 0 {
		// Update sort order to the latest
		latestSortOrder := transfers[len(transfers)-1].SortOrder
		f.memory.UpdatePollingState(latestSortOrder)
	}

	return transfers, nil
}

// FetchChains fetches chain information
func (f *Fetcher) FetchChains(ctx context.Context) error {
	chains, err := f.graphqlClient.FetchChains(ctx)
	if err != nil {
		return fmt.Errorf("FetchChains error: %w", err)
	}

	f.memory.UpdateChains(chains)
	return nil
} 