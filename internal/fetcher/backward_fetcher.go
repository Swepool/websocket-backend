package fetcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"time"
	"websocket-backend/config"
	"websocket-backend/internal/core"
	"websocket-backend/internal/utils"
	"websocket-backend/models"
)

// DatabaseWriter interface defines what the fetcher needs from the database
type DatabaseWriter interface {
	InsertTransfers(ctx context.Context, transfers []models.Transfer) error
	GetEarliestSortOrder() (string, error)
	GetLatestSortOrder() (string, error)
	GetTransferCount() (int64, error)
}

// transfersGraphQLResponse represents the GraphQL response structure
type transfersGraphQLResponse struct {
	Data struct {
		V2Transfers []struct {
			SourceChain struct {
				UniversalChainID string `json:"universal_chain_id"`
				ChainID          string `json:"chain_id"`
				DisplayName      string `json:"display_name"`
				Testnet          bool   `json:"testnet"`
				RpcType          string `json:"rpc_type"`
				AddrPrefix       string `json:"addr_prefix"`
			} `json:"source_chain"`
			DestinationChain struct {
				UniversalChainID string `json:"universal_chain_id"`
				ChainID          string `json:"chain_id"`
				DisplayName      string `json:"display_name"`
				Testnet          bool   `json:"testnet"`
				RpcType          string `json:"rpc_type"`
				AddrPrefix       string `json:"addr_prefix"`
			} `json:"destination_chain"`
			SenderCanonical         string    `json:"sender_canonical"`
			SenderDisplay           string    `json:"sender_display"`
			ReceiverCanonical       string    `json:"receiver_canonical"`
			ReceiverDisplay         string    `json:"receiver_display"`
			TransferSendTimestamp   time.Time `json:"transfer_send_timestamp"`
			BaseToken               string    `json:"base_token"`
			BaseAmount              string    `json:"base_amount"`
			BaseTokenSymbol         string    `json:"base_token_symbol"`
			BaseTokenDecimals       int       `json:"base_token_decimals"`
			SortOrder               string    `json:"sort_order"`
			PacketHash              string    `json:"packet_hash"`
		} `json:"v2_transfers"`
	} `json:"data"`
	Errors []interface{} `json:"errors"`
}

// BackwardFetcher handles fetching historical transfers going backwards in time
type BackwardFetcher struct {
	config            config.BackwardFetcherConfig
	channels          *core.Channels
	httpClient        *http.Client
	earliestSortOrder string
	dbWriter          DatabaseWriter
	isRunning         bool
}

// NewBackwardFetcher creates a new backward fetcher
func NewBackwardFetcher(cfg config.BackwardFetcherConfig, channels *core.Channels, dbWriter DatabaseWriter) *BackwardFetcher {
	return &BackwardFetcher{
		config:     cfg,
		channels:   channels,
		httpClient: &http.Client{Timeout: cfg.HTTPTimeout},
		dbWriter:   dbWriter,
	}
}

// Start begins backward syncing from the earliest transfer in database
func (bf *BackwardFetcher) Start(ctx context.Context) {
	// Check if backward sync is enabled
	if !bf.config.Enabled {
		utils.LogInfo("BACKWARD_FETCHER", "Backward sync is disabled in configuration")
		return
	}
	
	utils.LogInfo("BACKWARD_FETCHER", "Starting backward sync (max depth: %d days)", bf.config.MaxDepthDays)
	
	// Get the earliest sort order from database
	if err := bf.initializeEarliestSortOrder(); err != nil {
		utils.LogError("BACKWARD_FETCHER", "Failed to initialize earliest sort order: %v", err)
		return
	}
	
	if bf.earliestSortOrder == "" {
		utils.LogInfo("BACKWARD_FETCHER", "No transfers in database - skipping backward sync")
		return
	}
	
	bf.isRunning = true
	defer func() { bf.isRunning = false }()
	
	utils.LogInfo("BACKWARD_FETCHER", "Starting backward sync from sort order: %s", bf.earliestSortOrder)
	
	// Calculate cutoff time for maximum depth
	cutoffTime := time.Now().AddDate(0, 0, -bf.config.MaxDepthDays)
	
	batchCount := 0
	totalFetched := 0
	
	for bf.isRunning {
		select {
		case <-ctx.Done():
			utils.LogInfo("BACKWARD_FETCHER", "Stopping backward sync (context cancelled)")
			return
		default:
			// Fetch a batch going backwards
			transfers, err := bf.fetchBackwardBatch()
			if err != nil {
				utils.LogError("BACKWARD_FETCHER", "Failed to fetch backward batch: %v", err)
				time.Sleep(bf.config.RetryDelay) // Configured retry delay
				continue
			}
			
			if len(transfers) == 0 {
				utils.LogInfo("BACKWARD_FETCHER", "No more historical transfers available - backward sync complete")
				return
			}
			
			// Check if we've reached our depth limit
			oldestTransfer := transfers[len(transfers)-1]
			if oldestTransfer.TransferSendTimestamp.Before(cutoffTime) {
				utils.LogInfo("BACKWARD_FETCHER", "Reached maximum depth (%d days) - stopping backward sync", bf.config.MaxDepthDays)
				return
			}
			
			// Store transfers in database (no broadcasting)
			backpressureConfig := utils.DefaultBackpressureConfig()
			backpressureConfig.DropOnOverflow = false
			backpressureConfig.TimeoutMs = bf.config.BackpressureTimeoutMs // Configured timeout for historical data
			
			if utils.SendWithBackpressure(bf.channels.DatabaseSaves, transfers, backpressureConfig, nil) {
				batchCount++
				totalFetched += len(transfers)
				utils.LogInfo("BACKWARD_FETCHER", "🔵 BACKWARD: Batch %d → Stored %d HISTORICAL transfers (total: %d)", 
					batchCount, len(transfers), totalFetched)
			} else {
				utils.LogError("BACKWARD_FETCHER", "❌ BACKWARD: Failed to store batch %d (%d historical transfers)", batchCount+1, len(transfers))
				time.Sleep(bf.config.DatabaseRetryDelay) // Configured database retry delay
				continue
			}
			
			// Update earliest sort order for next iteration
			bf.earliestSortOrder = transfers[len(transfers)-1].SortOrder
			
			// Rate limiting delay (respecting GraphQL limits)
			time.Sleep(bf.config.RateLimitDelay)
		}
	}
}

// Stop stops the backward fetcher
func (bf *BackwardFetcher) Stop() {
	bf.isRunning = false
}

// IsRunning returns whether the backward fetcher is currently running
func (bf *BackwardFetcher) IsRunning() bool {
	return bf.isRunning
}

// initializeEarliestSortOrder gets the earliest sort order from database
func (bf *BackwardFetcher) initializeEarliestSortOrder() error {
	earliestSortOrder, err := bf.dbWriter.GetEarliestSortOrder()
	if err != nil {
		return fmt.Errorf("failed to get earliest sort order from database: %w", err)
	}
	
	bf.earliestSortOrder = earliestSortOrder
	return nil
}

// fetchBackwardBatch fetches a batch of transfers going backwards from current position
func (bf *BackwardFetcher) fetchBackwardBatch() ([]models.Transfer, error) {
	query := `
		query TransferListBackward($page: String!, $limit: Int!) {
			v2_transfers(args: {
				p_limit: $limit,
				p_sort_order: $page,
				p_comparison: "lt"
			}) {
				source_chain {
					universal_chain_id
					chain_id
					display_name
					testnet
					rpc_type
					addr_prefix
				}
				destination_chain {
					universal_chain_id
					chain_id
					display_name
					testnet
					rpc_type
					addr_prefix
				}
				sender_canonical
				sender_display
				receiver_canonical
				receiver_display
				transfer_send_timestamp
				base_token
				base_amount
				base_token_symbol
				base_token_decimals
				sort_order
				packet_hash
			}
		}
	`
	
	variables := map[string]interface{}{
		"page":  bf.earliestSortOrder,
		"limit": bf.config.BatchSize, // Keep at 100 due to GraphQL limit
	}
	
	requestBody := map[string]interface{}{
		"query":     query,
		"variables": variables,
	}
	
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GraphQL request: %w", err)
	}
	
	req, err := http.NewRequest("POST", bf.config.GraphQLURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("Content-Type", "application/json")
	
	resp, err := bf.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GraphQL request failed: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GraphQL request returned status %d", resp.StatusCode)
	}
	
	var graphqlResp transfersGraphQLResponse
	if err := json.NewDecoder(resp.Body).Decode(&graphqlResp); err != nil {
		return nil, fmt.Errorf("failed to decode GraphQL response: %w", err)
	}
	
	if len(graphqlResp.Errors) > 0 {
		return nil, fmt.Errorf("GraphQL errors: %v", graphqlResp.Errors)
	}
	
	// Convert GraphQL response to our models
	transfers := make([]models.Transfer, len(graphqlResp.Data.V2Transfers))
	for i, raw := range graphqlResp.Data.V2Transfers {
		// Calculate display amount from base amount and decimals
		displayAmount := formatTokenAmount(raw.BaseAmount, raw.BaseTokenDecimals)
		
		transfers[i] = models.Transfer{
			PacketHash:            raw.PacketHash,
			SortOrder:             raw.SortOrder,
			TransferSendTimestamp: raw.TransferSendTimestamp,
			SenderCanonical:       raw.SenderCanonical,
			SenderDisplay:         raw.SenderDisplay,
			ReceiverCanonical:     raw.ReceiverCanonical,
			ReceiverDisplay:       raw.ReceiverDisplay,
			SourceChain: models.Chain{
				UniversalChainID: raw.SourceChain.UniversalChainID,
				ChainID:          raw.SourceChain.ChainID,
				DisplayName:      raw.SourceChain.DisplayName,
				Testnet:          raw.SourceChain.Testnet,
				RpcType:          raw.SourceChain.RpcType,
				AddrPrefix:       raw.SourceChain.AddrPrefix,
			},
			DestinationChain: models.Chain{
				UniversalChainID: raw.DestinationChain.UniversalChainID,
				ChainID:          raw.DestinationChain.ChainID,
				DisplayName:      raw.DestinationChain.DisplayName,
				Testnet:          raw.DestinationChain.Testnet,
				RpcType:          raw.DestinationChain.RpcType,
				AddrPrefix:       raw.DestinationChain.AddrPrefix,
			},
			BaseAmount:      displayAmount,
			BaseTokenSymbol: raw.BaseTokenSymbol,
		}
	}
	
	return transfers, nil
}

// Helper function to share token amount formatting logic
// Uses big.Rat for precise decimal arithmetic, preserves FULL precision (no truncation)
func formatTokenAmount(baseAmount string, decimals int) string {
	if baseAmount == "" || baseAmount == "0" {
		return "0"
	}
	
	// Parse the base amount as a big integer
	baseAmountBig := new(big.Int)
	_, ok := baseAmountBig.SetString(baseAmount, 10)
	if !ok {
		utils.LogWarn("BACKWARD_FETCHER", "Failed to parse amount '%s', using original", baseAmount)
		return baseAmount
	}
	
	// If no decimals, return as string
	if decimals <= 0 {
		return baseAmountBig.String()
	}
	
	// Create divisor as 10^decimals using big.Int
	divisor := new(big.Int)
	divisor.Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	
	// Use big.Rat for exact decimal division
	rat := new(big.Rat)
	rat.SetFrac(baseAmountBig, divisor)
	
	// Convert to decimal string with FULL precision (no truncation)
	// Send complete precision to frontend, let it decide how to display
	formatted := rat.FloatString(decimals)
	
	return formatted
} 