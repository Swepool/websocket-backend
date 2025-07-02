package fetcher

import (
	"context"
	"time"
	"websocket-backend/api/graphql"
	"websocket-backend/config"
	"websocket-backend/internal/core"
	"websocket-backend/internal/utils"
	"websocket-backend/models"
)



// Fetcher pulls real-time blockchain transfers and outputs normalized Transfer objects
type Fetcher struct {
	config     config.FetcherConfig
	channels   *core.Channels
	stats      Stats
	graphqlClient *graphql.Client
	dbWriter   DatabaseWriter
	lastSortOrder string
}

// Stats tracks fetcher performance
type Stats struct {
	TotalFetched    int64     `json:"totalFetched"`
	BatchesFetched  int64     `json:"batchesFetched"`
	LastFetchTime   time.Time `json:"lastFetchTime"`
	FetchErrors     int64     `json:"fetchErrors"`
	AverageLatency  float64   `json:"averageLatency"`
	IsRunning       bool      `json:"isRunning"`
}

// NewFetcher creates a new clean fetcher component
func NewFetcher(cfg config.FetcherConfig, channels *core.Channels, dbWriter DatabaseWriter) *Fetcher {
	return &Fetcher{
		config:        cfg,
		channels:      channels,
		stats:         Stats{},
		graphqlClient: graphql.NewClient(cfg.GraphQLURL),
		dbWriter:      dbWriter,
		lastSortOrder: "",
	}
}

// Start begins the fetcher component
func (f *Fetcher) Start(ctx context.Context) {
	utils.LogInfo("FETCHER", "Starting clean fetcher component (poll interval: %v)", f.config.PollInterval)
	f.stats.IsRunning = true
	
	// Initialize lastSortOrder from database for proper resumption
	if f.dbWriter != nil {
		if latestSortOrder, err := f.dbWriter.GetLatestSortOrder(); err == nil && latestSortOrder != "" {
			f.lastSortOrder = latestSortOrder
			utils.LogInfo("FETCHER", "🔄 RESUMING from last processed sort order: %s", latestSortOrder)
		} else {
			utils.LogInfo("FETCHER", "🚀 STARTING fresh - no previous transfers found")
		}
	}
	
	ticker := time.NewTicker(f.config.PollInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			utils.LogInfo("FETCHER", "Stopping fetcher component")
			f.stats.IsRunning = false
			return
			
		case <-ticker.C:
			f.fetchBatch(ctx)
		}
	}
}

// fetchBatch fetches a batch of transfers and sends to processor
func (f *Fetcher) fetchBatch(ctx context.Context) {
	startTime := time.Now()
	
	var transfers []models.Transfer
	var err error
	
	if f.config.MockMode {
		transfers = f.generateMockTransfers()
	} else {
		transfers, err = f.fetchRealTransfers(ctx)
	}
	
	if err != nil {
		f.stats.FetchErrors++
		utils.LogError("FETCHER", "Failed to fetch transfers: %v", err)
		return
	}
	
	if len(transfers) == 0 {
		return // No transfers to process
	}
	
	// Send normalized transfers to processor
	select {
	case f.channels.RawTransfers <- transfers:
		f.updateStats(len(transfers), time.Since(startTime))
		utils.LogInfo("FETCHER", "📤 Fetched %d transfers", len(transfers))
	case <-ctx.Done():
		return
	default:
		utils.LogWarn("FETCHER", "❌ Channel full, dropped %d transfers", len(transfers))
	}
}

// generateMockTransfers creates mock transfer data for testing
func (f *Fetcher) generateMockTransfers() []models.Transfer {
	numTransfers := f.config.BatchSize / 10 // Generate fewer mock transfers
	if numTransfers == 0 {
		numTransfers = 1
	}
	
	transfers := make([]models.Transfer, numTransfers)
	now := time.Now()
	
	for i := 0; i < numTransfers; i++ {
		transfers[i] = models.Transfer{
			TransferSendTxHash: generateMockHash(),
			SortOrder:         generateMockSortOrder(now, i),
			TransferSendTimestamp: now.Add(time.Duration(-i) * time.Second),
			BaseAmount:        "1000000",
			BaseTokenSymbol:   "USDC",
			CanonicalTokenSymbol: "USDC",
			SenderCanonical:   generateMockAddress(),
			ReceiverCanonical: generateMockAddress(),
			SourceChain: models.Chain{
				UniversalChainID: "cosmos-hub",
				DisplayName:      "Cosmos Hub",
				Testnet:         false,
			},
			DestinationChain: models.Chain{
				UniversalChainID: "osmosis",
				DisplayName:      "Osmosis",
				Testnet:         false,
			},
			PacketHash: generateMockHash(),
		}
	}
	
	return transfers
}

// fetchRealTransfers fetches actual transfers from GraphQL API
func (f *Fetcher) fetchRealTransfers(ctx context.Context) ([]models.Transfer, error) {
	var transfers []models.Transfer
	var err error
	
	// If we don't have a last sort order, fetch the latest transfers to establish baseline
	if f.lastSortOrder == "" {
		transfers, err = f.graphqlClient.FetchLatestTransfers(ctx, 1, nil)
		utils.LogInfo("FETCHER", "🚀 Fetching latest transfer to establish baseline")
	} else {
		// Use the safe version that implements 10-second buffer to avoid processing delays
		transfers, err = f.graphqlClient.FetchNewTransfersSafe(ctx, f.lastSortOrder, f.config.BatchSize, nil)
		utils.LogDebug("FETCHER", "🔄 Fetching new transfers since sort order: %s (with 10s safety buffer)", f.lastSortOrder)
	}
	
	if err != nil {
		utils.LogError("FETCHER", "❌ GraphQL failed: %v", err)
		return nil, err
	}
	
	// Update the last sort order if we got transfers
	if len(transfers) > 0 {
		f.lastSortOrder = transfers[len(transfers)-1].SortOrder
		utils.LogDebug("FETCHER", "📍 Updated last sort order to: %s", f.lastSortOrder)
	}
	
	return transfers, nil
}

// updateStats updates fetcher statistics
func (f *Fetcher) updateStats(transferCount int, latency time.Duration) {
	f.stats.TotalFetched += int64(transferCount)
	f.stats.BatchesFetched++
	f.stats.LastFetchTime = time.Now()
	
	// Update average latency using exponential moving average
	if f.stats.AverageLatency == 0 {
		f.stats.AverageLatency = latency.Seconds()
	} else {
		f.stats.AverageLatency = 0.9*f.stats.AverageLatency + 0.1*latency.Seconds()
	}
}

// GetStats returns current fetcher statistics
func (f *Fetcher) GetStats() Stats {
	return f.stats
}

// Helper functions for mock data generation
func generateMockHash() string {
	// Generate a simple mock hash
	return "0x" + time.Now().Format("20060102150405") + "abcdef"
}

func generateMockSortOrder(baseTime time.Time, offset int) string {
	// Generate mock sort order based on timestamp
	return baseTime.Add(time.Duration(-offset)*time.Second).Format("20060102150405")
}

func generateMockAddress() string {
	// Generate a simple mock address
	return "cosmos1" + time.Now().Format("150405") + "mock"
} 