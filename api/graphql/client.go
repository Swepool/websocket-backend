package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
	"websocket-backend-new/models"
)

// Client represents a GraphQL client
type Client struct {
	endpoint   string
	httpClient *http.Client
}

// NewClient creates a new GraphQL client
func NewClient(endpoint string) *Client {
	return &Client{
		endpoint: endpoint,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				MaxConnsPerHost:     50,
				IdleConnTimeout:     90 * time.Second,
				DisableKeepAlives:   false,
			},
		},
	}
}

// GraphQL queries matching the old system
const (
	latestTransfersQuery = `
		query TransferListLatest($limit: Int!, $network: String) {
			v2_transfers(args: {
				p_limit: $limit,
				p_network: $network
			}) {
				source_chain {
					universal_chain_id
					display_name
					chain_id
					testnet
					rpc_type
					addr_prefix
				}
				destination_chain {
					universal_chain_id
					display_name
					chain_id
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
				transfer_send_transaction_hash
				transfer_recv_timestamp
				packet_hash
			}
		}
	`

	newTransfersQuery = `
		query TransferListPage($page: String!, $limit: Int!, $network: String) {
			v2_transfers(args: {
				p_limit: $limit,
				p_sort_order: $page,
				p_comparison: "gt",
				p_network: $network
			}) {
				source_chain {
					universal_chain_id
					display_name
					chain_id
					testnet
					rpc_type
					addr_prefix
				}
				destination_chain {
					universal_chain_id
					display_name
					chain_id
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
				transfer_send_transaction_hash
				transfer_recv_timestamp
				packet_hash
			}
		}
	`

	chainsQuery = `
		query Chains {
			v2_chains {
				universal_chain_id
				display_name
				chain_id
				testnet
				rpc_type
				addr_prefix
			}
		}
	`
)

// GraphQL request/response structures
type GraphQLRequest struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables,omitempty"`
}

type GraphQLResponse struct {
	Data struct {
		V2Transfers []RawTransfer `json:"v2_transfers"`
		V2Chains    []RawChain    `json:"v2_chains"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// Raw response structures matching the GraphQL schema
type RawTransfer struct {
	SourceChain                  RawChain  `json:"source_chain"`
	DestinationChain             RawChain  `json:"destination_chain"`
	SenderCanonical              string    `json:"sender_canonical"`
	SenderDisplay                string    `json:"sender_display"`
	ReceiverCanonical            string    `json:"receiver_canonical"`
	ReceiverDisplay              string    `json:"receiver_display"`
	TransferSendTimestamp        time.Time `json:"transfer_send_timestamp"`
	BaseToken                    string    `json:"base_token"`
	BaseAmount                   string    `json:"base_amount"`
	BaseTokenSymbol              string    `json:"base_token_symbol"`
	BaseTokenDecimals            int       `json:"base_token_decimals"`
	SortOrder                    string    `json:"sort_order"`
	TransferSendTransactionHash  string    `json:"transfer_send_transaction_hash"`
	TransferRecvTimestamp        *time.Time `json:"transfer_recv_timestamp"`
	PacketHash                   string    `json:"packet_hash"`
}

type RawChain struct {
	UniversalChainID string `json:"universal_chain_id"`
	DisplayName      string `json:"display_name"`
	ChainID          string `json:"chain_id"`
	Testnet          bool   `json:"testnet"`
	RpcType          string `json:"rpc_type"`
	AddrPrefix       string `json:"addr_prefix"`
}

// Convert raw transfer to model
func (r RawTransfer) ToModel() models.Transfer {
	return models.Transfer{
		TransferSendTxHash:    r.TransferSendTransactionHash,
		SortOrder:             r.SortOrder,
		TransferSendTimestamp: r.TransferSendTimestamp,
		BaseAmount:            r.BaseAmount,
		BaseTokenSymbol:       r.BaseTokenSymbol,
		SenderCanonical:       r.SenderCanonical,
		ReceiverCanonical:     r.ReceiverCanonical,
		SenderDisplay:         r.SenderDisplay,
		ReceiverDisplay:       r.ReceiverDisplay,
		SourceChain:           r.SourceChain.ToModel(),
		DestinationChain:      r.DestinationChain.ToModel(),
		PacketHash:            r.PacketHash,
	}
}

// Convert raw chain to model
func (r RawChain) ToModel() models.Chain {
	return models.Chain{
		UniversalChainID: r.UniversalChainID,
		ChainID:          r.ChainID,
		DisplayName:      r.DisplayName,
		Testnet:          r.Testnet,
		RpcType:          r.RpcType,
		AddrPrefix:       r.AddrPrefix,
	}
}

// FetchLatestTransfers fetches the latest transfers for baseline
func (c *Client) FetchLatestTransfers(ctx context.Context, limit int, networkFilter []string) ([]models.Transfer, error) {
	variables := map[string]interface{}{
		"limit": limit,
	}
	
	// Convert network filter to single network parameter
	var network *string
	if len(networkFilter) > 0 {
		network = &networkFilter[0]
		variables["network"] = *network
	}
	
	fmt.Printf("[GRAPHQL] Fetching latest transfers (limit: %d, network: %v)\n", limit, network)
	
	result, err := c.executeQuery(ctx, latestTransfersQuery, variables)
	if err != nil {
		return nil, err
	}
	
	// Convert raw transfers to models
	transfers := make([]models.Transfer, len(result.Data.V2Transfers))
	for i, raw := range result.Data.V2Transfers {
		transfers[i] = raw.ToModel()
	}
	
	fmt.Printf("[GRAPHQL] Fetched %d latest transfers\n", len(transfers))
	return transfers, nil
}

// FetchNewTransfers fetches new transfers since the last sort order
func (c *Client) FetchNewTransfers(ctx context.Context, lastSortOrder string, limit int, networkFilter []string) ([]models.Transfer, error) {
	variables := map[string]interface{}{
		"page":  lastSortOrder,
		"limit": limit,
	}
	
	// Convert network filter to single network parameter
	var network *string
	if len(networkFilter) > 0 {
		network = &networkFilter[0]
		variables["network"] = *network
	}
	
	fmt.Printf("[GRAPHQL] Fetching new transfers (page: %s, limit: %d, network: %v)\n", lastSortOrder, limit, network)
	
	result, err := c.executeQuery(ctx, newTransfersQuery, variables)
	if err != nil {
		return nil, err
	}
	
	// Convert raw transfers to models
	transfers := make([]models.Transfer, len(result.Data.V2Transfers))
	for i, raw := range result.Data.V2Transfers {
		transfers[i] = raw.ToModel()
	}
	
	fmt.Printf("[GRAPHQL] Fetched %d new transfers\n", len(transfers))
	return transfers, nil
}

// FetchChains fetches all available chains
func (c *Client) FetchChains(ctx context.Context) ([]models.Chain, error) {
	fmt.Printf("[GRAPHQL] Fetching chains\n")
	
	result, err := c.executeQuery(ctx, chainsQuery, nil)
	if err != nil {
		return nil, err
	}
	
	// Convert raw chains to models
	chains := make([]models.Chain, len(result.Data.V2Chains))
	for i, raw := range result.Data.V2Chains {
		chains[i] = raw.ToModel()
	}
	
	fmt.Printf("[GRAPHQL] Fetched %d chains\n", len(chains))
	return chains, nil
}

// executeQuery executes a GraphQL query
func (c *Client) executeQuery(ctx context.Context, query string, variables map[string]interface{}) (*GraphQLResponse, error) {
	request := GraphQLRequest{
		Query:     query,
		Variables: variables,
	}
	
	jsonData, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}
	
	req, err := http.NewRequestWithContext(ctx, "POST", c.endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("Content-Type", "application/json")
	
	start := time.Now()
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()
	
	fmt.Printf("[GRAPHQL] Request completed in %v, status: %d\n", time.Since(start), resp.StatusCode)
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	
	var result GraphQLResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}
	
	if len(result.Errors) > 0 {
		return nil, fmt.Errorf("GraphQL errors: %v", result.Errors)
	}
	
	return &result, nil
} 