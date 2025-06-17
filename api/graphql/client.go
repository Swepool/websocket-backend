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
		query TransferListLatest($limit: Int!, $network: [String!]) {
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
				packet_hash
			}
		}
	`

	newTransfersQuery = `
		query TransferListPage($page: String!, $limit: Int!, $network: [String!]) {
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

	latencyQuery = `
		query LatencyStats($sourceChainId: String!, $destinationChainId: String!) {
			v2_stats_latency(args: {
				p_source_universal_chain_id: $sourceChainId,
				p_destination_universal_chain_id: $destinationChainId
			}) {
				secs_until_packet_ack {
					p5
					median
					p95
				}
				secs_until_packet_recv {
					p5
					median
					p95
				}
				secs_until_write_ack {
					p5
					median
					p95
				}
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
		V2Transfers  []RawTransfer  `json:"v2_transfers"`
		V2Chains     []RawChain     `json:"v2_chains"`
		V2StatsLatency []RawLatency `json:"v2_stats_latency"`
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

type RawLatency struct {
	SecsUntilPacketAck  LatencyStats `json:"secs_until_packet_ack"`
	SecsUntilPacketRecv LatencyStats `json:"secs_until_packet_recv"`
	SecsUntilWriteAck   LatencyStats `json:"secs_until_write_ack"`
}

type LatencyStats struct {
	P5     float64 `json:"p5"`
	Median float64 `json:"median"`
	P95    float64 `json:"p95"`
}

// Convert raw transfer to model
func (r RawTransfer) ToModel() models.Transfer {
	return models.Transfer{
		PacketHash:            r.PacketHash,
		SortOrder:             r.SortOrder,
		TransferSendTimestamp: r.TransferSendTimestamp,
		SenderCanonical:       r.SenderCanonical,
		SenderDisplay:         r.SenderDisplay,
		ReceiverCanonical:     r.ReceiverCanonical,
		ReceiverDisplay:       r.ReceiverDisplay,
		SourceChain:           r.SourceChain.ToModel(),
		DestinationChain:      r.DestinationChain.ToModel(),
		BaseAmount:            r.BaseAmount,
		BaseTokenSymbol:       r.BaseTokenSymbol,
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
func (c *Client) FetchLatestTransfers(ctx context.Context, limit int, network []string) ([]models.Transfer, error) {
	variables := map[string]interface{}{
		"limit":   limit,
		"network": network,
	}
	
	result, err := c.executeQuery(ctx, latestTransfersQuery, variables)
	if err != nil {
		return nil, err
	}
	
	// Convert raw transfers to models
	transfers := make([]models.Transfer, len(result.Data.V2Transfers))
	for i, raw := range result.Data.V2Transfers {
		transfers[i] = raw.ToModel()
	}
	
	return transfers, nil
}

// FetchNewTransfers fetches new transfers since the last sort order
func (c *Client) FetchNewTransfers(ctx context.Context, lastSortOrder string, limit int, network []string) ([]models.Transfer, error) {
	variables := map[string]interface{}{
		"page":    lastSortOrder,
		"limit":   limit,
		"network": network,
	}
	
	result, err := c.executeQuery(ctx, newTransfersQuery, variables)
	if err != nil {
		return nil, err
	}
	
	// Convert raw transfers to models
	transfers := make([]models.Transfer, len(result.Data.V2Transfers))
	for i, raw := range result.Data.V2Transfers {
		transfers[i] = raw.ToModel()
	}
	
	return transfers, nil
}

// FetchChains fetches all available chains
func (c *Client) FetchChains(ctx context.Context) ([]models.Chain, error) {
	result, err := c.executeQuery(ctx, chainsQuery, nil)
	if err != nil {
		return nil, err
	}
	
	// Convert raw chains to models
	chains := make([]models.Chain, len(result.Data.V2Chains))
	for i, raw := range result.Data.V2Chains {
		chains[i] = raw.ToModel()
	}
	
	return chains, nil
}

// FetchLatency fetches latency statistics for a specific chain pair
func (c *Client) FetchLatency(ctx context.Context, sourceChainID, destinationChainID string) (*models.LatencyData, error) {
	variables := map[string]interface{}{
		"sourceChainId":      sourceChainID,
		"destinationChainId": destinationChainID,
	}
	
	result, err := c.executeQuery(ctx, latencyQuery, variables)
	if err != nil {
		return nil, err
	}
	
	// Check if we have latency data
	if len(result.Data.V2StatsLatency) == 0 {
		return nil, nil // No latency data available for this chain pair
	}
	
	rawLatency := result.Data.V2StatsLatency[0]
	
	// Convert to model
	latencyData := &models.LatencyData{
		SourceChain:      sourceChainID,
		DestinationChain: destinationChainID,
		PacketAck: models.LatencyStats{
			P5:     rawLatency.SecsUntilPacketAck.P5,
			Median: rawLatency.SecsUntilPacketAck.Median,
			P95:    rawLatency.SecsUntilPacketAck.P95,
		},
		PacketRecv: models.LatencyStats{
			P5:     rawLatency.SecsUntilPacketRecv.P5,
			Median: rawLatency.SecsUntilPacketRecv.Median,
			P95:    rawLatency.SecsUntilPacketRecv.P95,
		},
		WriteAck: models.LatencyStats{
			P5:     rawLatency.SecsUntilWriteAck.P5,
			Median: rawLatency.SecsUntilWriteAck.Median,
			P95:    rawLatency.SecsUntilWriteAck.P95,
		},
	}
	
	return latencyData, nil
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
	
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GraphQL request returned status %d", resp.StatusCode)
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