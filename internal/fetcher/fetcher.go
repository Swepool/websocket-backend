package fetcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"net/http"
	"strings"
	"time"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/internal/models"
)

// Config holds fetcher configuration
type Config struct {
	PollInterval time.Duration `json:"pollInterval"` // How often to fetch (default: 5s)
	BatchSize    int           `json:"batchSize"`    // Max transfers per batch (default: 50)
	MockMode     bool          `json:"mockMode"`     // Use mock data (default: false)
	GraphQLURL   string        `json:"graphqlUrl"`   // GraphQL endpoint URL
}

// DefaultConfig returns default fetcher configuration
func DefaultConfig() Config {
	return Config{
		PollInterval: 500 * time.Millisecond, // Fast polling for real-time transfers
		BatchSize:    100,
		MockMode:     false, // Always use real data
		GraphQLURL:   "https://staging.graphql.union.build/v1/graphql",
	}
}

// ChainProvider interface for getting chain data
type ChainProvider interface {
	GetAllChains() []models.ChainInfo
}

// Fetcher handles fetching transfers from the GraphQL API
type Fetcher struct {
	config        Config
	channels      *channels.Channels
	chainProvider ChainProvider
	rand          *rand.Rand
	httpClient    *http.Client
	lastSortOrder string
}

// NewFetcher creates a new fetcher
func NewFetcher(config Config, channels *channels.Channels, chainProvider ChainProvider) (*Fetcher, error) {
	return &Fetcher{
		config:        config,
		channels:      channels,
		chainProvider: chainProvider,
		rand:          rand.New(rand.NewSource(time.Now().UnixNano())),
		httpClient:    &http.Client{Timeout: 30 * time.Second},
		lastSortOrder: "",
	}, nil
}

// Start begins the fetcher thread
func (f *Fetcher) Start(ctx context.Context) {
	fmt.Printf("[FETCHER] Starting with %v poll interval (mock mode: %v)\n", 
		f.config.PollInterval, f.config.MockMode)
	
	ticker := time.NewTicker(f.config.PollInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[FETCHER] Shutting down\n")
			return
			
		case <-ticker.C:
			f.fetchAndSend()
		}
	}
}

// fetchAndSend fetches transfers and sends them directly to scheduler and stats
func (f *Fetcher) fetchAndSend() {
	var transfers []models.Transfer
	var err error
	
	if f.config.MockMode {
		transfers = f.generateMockTransfers()
	} else {
		transfers, err = f.fetchRealTransfers()
		if err != nil {
			fmt.Printf("[FETCHER] Error fetching real transfers: %v\n", err)
			return
		}
	}
	
	if len(transfers) > 0 {
		// Send to scheduler for natural timing and broadcasting
		select {
		case f.channels.EnhancedTransfers <- transfers:
			fmt.Printf("[FETCHER] Sent batch of %d transfers to scheduler\n", len(transfers))
		default:
			fmt.Printf("[FETCHER] Warning: Scheduler channel full, dropping batch of %d transfers\n", len(transfers))
		}
		
		// ALSO send each transfer to stats collector (at true fetch rate)
		for _, transfer := range transfers {
			select {
			case f.channels.StatsUpdates <- transfer:
				// Successfully sent to stats
			default:
				fmt.Printf("[FETCHER] Warning: Stats channel full, dropping stats for transfer %s\n", transfer.PacketHash)
			}
		}
		
		fmt.Printf("[FETCHER] Sent %d transfers to stats collector at true fetch rate\n", len(transfers))
	}
}

// generateMockTransfers creates mock transfer data using real chains from GraphQL
func (f *Fetcher) generateMockTransfers() []models.Transfer {
	// Get real chains from the chain provider
	availableChains := f.chainProvider.GetAllChains()
	if len(availableChains) < 2 {
		fmt.Printf("[FETCHER] Not enough chains available (%d), skipping mock generation\n", len(availableChains))
		return []models.Transfer{}
	}
	
	now := time.Now()
	
	// Generate 1-3 random transfers using real chains
	numTransfers := 1 + f.rand.Intn(3)
	transfers := make([]models.Transfer, numTransfers)
	
	for i := 0; i < numTransfers; i++ {
		// Pick two different random chains
		sourceIdx := f.rand.Intn(len(availableChains))
		destIdx := f.rand.Intn(len(availableChains))
		for destIdx == sourceIdx && len(availableChains) > 1 {
			destIdx = f.rand.Intn(len(availableChains))
		}
		
		sourceChain := availableChains[sourceIdx]
		destChain := availableChains[destIdx]
		
		// Generate mock addresses based on chain prefixes
		senderAddr := f.generateMockAddress(sourceChain.AddrPrefix)
		receiverAddr := f.generateMockAddress(destChain.AddrPrefix)
		
		// Pick a random token
		tokens := []string{"ATOM", "OSMO", "JUNO", "STARS", "USDC", "ETH"}
		tokenSymbol := tokens[f.rand.Intn(len(tokens))]
		
		// Generate random amount
		amount := fmt.Sprintf("%.2f", 1.0+f.rand.Float64()*100.0)
		
		transfers[i] = models.Transfer{
			PacketHash:            fmt.Sprintf("0x%d%d", now.Unix(), i),
			TransferSendTimestamp: now.Add(-time.Duration(f.rand.Intn(60)) * time.Second),
			SenderCanonical:       senderAddr,
			ReceiverCanonical:     receiverAddr,
			SourceChain: models.Chain{
				UniversalChainID: sourceChain.UniversalChainID,
				ChainID:          sourceChain.ChainID,
				DisplayName:      sourceChain.DisplayName,
				Testnet:          sourceChain.Testnet,
				RpcType:          sourceChain.RpcType,
				AddrPrefix:       sourceChain.AddrPrefix,
			},
			DestinationChain: models.Chain{
				UniversalChainID: destChain.UniversalChainID,
				ChainID:          destChain.ChainID,
				DisplayName:      destChain.DisplayName,
				Testnet:          destChain.Testnet,
				RpcType:          destChain.RpcType,
				AddrPrefix:       destChain.AddrPrefix,
			},
			BaseToken:              fmt.Sprintf("u%s", strings.ToLower(tokenSymbol)),
			BaseTokenSymbol:        tokenSymbol,
			BaseTokenAmountDisplay: amount,
		}
	}
	
	return transfers
}

// generateMockAddress generates a mock address for the given prefix
func (f *Fetcher) generateMockAddress(prefix string) string {
	if prefix == "0x" {
		// Ethereum-style address
		return fmt.Sprintf("0x%040x", f.rand.Uint64())
	}
	
	// Cosmos-style address
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	suffix := make([]byte, 39) // Standard bech32 length
	for i := range suffix {
		suffix[i] = charset[f.rand.Intn(len(charset))]
	}
	return fmt.Sprintf("%s1%s", prefix, string(suffix))
}

// GraphQL types for transfers query
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
			SenderCanonical       string    `json:"sender_canonical"`
			SenderDisplay         string    `json:"sender_display"`
			ReceiverCanonical     string    `json:"receiver_canonical"`
			ReceiverDisplay       string    `json:"receiver_display"`
			TransferSendTimestamp time.Time `json:"transfer_send_timestamp"`
			BaseToken             string    `json:"base_token"`
			BaseAmount            string    `json:"base_amount"`
			BaseTokenSymbol       string    `json:"base_token_symbol"`
			BaseTokenDecimals     int       `json:"base_token_decimals"`
			SortOrder             string    `json:"sort_order"`
			PacketHash            string    `json:"packet_hash"`
		} `json:"v2_transfers"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// fetchRealTransfers fetches real transfers from Union GraphQL API
func (f *Fetcher) fetchRealTransfers() ([]models.Transfer, error) {
	var query string
	var variables map[string]interface{}
	
	if f.lastSortOrder == "" {
		// Initial fetch - get latest transfers
		query = `
			query TransferListLatest($limit: Int!) {
				v2_transfers(args: {
					p_limit: $limit
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
		variables = map[string]interface{}{
			"limit": f.config.BatchSize,
		}
	} else {
		// Subsequent fetches - get new transfers since last sort order
		query = `
			query TransferListPage($page: String!, $limit: Int!) {
				v2_transfers(args: {
					p_limit: $limit,
					p_sort_order: $page,
					p_comparison: "gt"
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
		variables = map[string]interface{}{
			"page":  f.lastSortOrder,
			"limit": f.config.BatchSize,
		}
	}
	
	requestBody := map[string]interface{}{
		"query":     query,
		"variables": variables,
	}
	
	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal GraphQL request: %w", err)
	}
	
	req, err := http.NewRequest("POST", f.config.GraphQLURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("Content-Type", "application/json")
	
	resp, err := f.httpClient.Do(req)
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
		displayAmount := f.formatTokenAmount(raw.BaseAmount, raw.BaseTokenDecimals)
		
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
			BaseToken:              raw.BaseToken,
			BaseTokenSymbol:        raw.BaseTokenSymbol,
			BaseTokenAmountDisplay: displayAmount,
		}
	}
	
	// Update last sort order for next fetch
	if len(transfers) > 0 {
		f.lastSortOrder = graphqlResp.Data.V2Transfers[len(graphqlResp.Data.V2Transfers)-1].SortOrder
	}
	
	return transfers, nil
}

// formatTokenAmount formats token amount from base units to display units
func (f *Fetcher) formatTokenAmount(baseAmount string, decimals int) string {
	if baseAmount == "" || baseAmount == "0" {
		return "0"
	}
	
	// Parse the base amount as a big integer
	amount := new(big.Int)
	amount, ok := amount.SetString(baseAmount, 10)
	if !ok {
		return "0"
	}
	
	// Create divisor (10^decimals)
	divisor := new(big.Int)
	divisor.Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	
	// Divide amount by divisor to get the display amount
	quotient := new(big.Int)
	remainder := new(big.Int)
	quotient.DivMod(amount, divisor, remainder)
	
	// Format with up to 6 decimal places
	if remainder.Cmp(big.NewInt(0)) == 0 {
		return quotient.String()
	}
	
	// Calculate fractional part
	fractional := new(big.Float)
	fractional.SetInt(remainder)
	divisorFloat := new(big.Float)
	divisorFloat.SetInt(divisor)
	fractional.Quo(fractional, divisorFloat)
	
	// Format the result
	wholeFloat := new(big.Float)
	wholeFloat.SetInt(quotient)
	result := new(big.Float)
	result.Add(wholeFloat, fractional)
	
	return result.Text('f', 6)
} 