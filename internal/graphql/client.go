package graphql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"websocket-backend/internal/models"
)

// Client represents a GraphQL client with optimized HTTP client reuse
type Client struct {
	endpoint   string
	httpClient *http.Client // Reusable HTTP client for connection pooling
}

// New creates a new GraphQL client with optimized HTTP client
func New(endpoint string) *Client {
	// Create a single HTTP client with optimized settings for reuse
	httpClient := &http.Client{
		Timeout: 10 * time.Second, // 10 second timeout
		Transport: &http.Transport{
			MaxIdleConns:          100,              // Maximum idle connections across all hosts
			MaxIdleConnsPerHost:   20,               // Maximum idle connections per host (increased for high frequency)
			MaxConnsPerHost:       50,               // Maximum connections per host
			IdleConnTimeout:       90 * time.Second, // Idle connection timeout
			TLSHandshakeTimeout:   10 * time.Second, // TLS handshake timeout
			ExpectContinueTimeout: 1 * time.Second,  // Expect continue timeout
			DisableCompression:    false,            // Enable compression
			DisableKeepAlives:     false,            // Enable keep-alives for connection reuse
		},
	}
	
	return &Client{
		endpoint:   endpoint,
		httpClient: httpClient,
	}
}

// GraphQL queries
const (
	transferListFragment = `
		fragment TransferListItem on v2_transfer_type {
			source_chain {
				universal_chain_id
				display_name
				chain_id
				testnet
			}
			destination_chain {
				universal_chain_id
				display_name
				chain_id
				testnet
			}
			sender_canonical
			receiver_canonical
			transfer_send_timestamp
			transfer_send_transaction_hash
			transfer_recv_timestamp
			packet_hash
			base_token
			base_amount
			quote_token
			quote_amount
			sort_order
		}
	`

	latestTransfersQuery = `
		query TransferListLatest($limit: Int!, $network: String) {
			v2_transfers(args: {
				p_limit: $limit,
				p_network: $network
			}) {
				...TransferListItem
			}
		}
		%s
	`

	newTransfersQuery = `
		query TransferListPage($page: String!, $limit: Int!, $network: String) {
			v2_transfers(args: {
				p_limit: $limit,
				p_sort_order: $page,
				p_comparison: "gt",
				p_network: $network
			}) {
				...TransferListItem
			}
		}
		%s
	`



	chainsQuery = `
		query Chains {
			v2_chains {
				universal_chain_id
				display_name
				chain_id
				testnet
			}
		}
	`
)

// GraphQLRequest represents a GraphQL request
type GraphQLRequest struct {
	Query     string      `json:"query"`
	Variables interface{} `json:"variables"`
}

// GraphQLResponse represents a GraphQL response
type GraphQLResponse struct {
	Data struct {
		V2Transfers []models.Transfer `json:"v2_transfers"`
		V2Chains    []models.Chain    `json:"v2_chains"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

// fetchGraphQL makes a GraphQL request
func (c *Client) fetchGraphQL(ctx context.Context, query string, variables interface{}) (*GraphQLResponse, error) {
	reqBody := GraphQLRequest{
		Query:     query,
		Variables: variables,
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("error marshaling request: %v", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", c.endpoint, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("error creating request: %v", err)
	}

	req.Header.Set("Content-Type", "application/json")

	fmt.Printf("[GRAPHQL] Making request to %s\n", c.endpoint)
	start := time.Now()
	
	resp, err := c.httpClient.Do(req)
	if err != nil {
		fmt.Printf("[GRAPHQL] Request failed after %v: %v\n", time.Since(start), err)
		return nil, fmt.Errorf("error making request: %v", err)
	}
	defer resp.Body.Close()

	fmt.Printf("[GRAPHQL] Request completed in %v, status: %d\n", time.Since(start), resp.StatusCode)

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var result GraphQLResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("error decoding response: %v", err)
	}

	if len(result.Errors) > 0 {
		return nil, fmt.Errorf("graphql errors: %v", result.Errors)
	}

	fmt.Printf("[GRAPHQL] Response decoded successfully\n")
	return &result, nil
}

// FetchLatestTransfers fetches the latest transfers
func (c *Client) FetchLatestTransfers(ctx context.Context, limit int, network *string) ([]models.Transfer, error) {
	variables := map[string]interface{}{
		"limit": limit,
	}
	if network != nil {
		variables["network"] = *network
	}

	result, err := c.fetchGraphQL(ctx, fmt.Sprintf(latestTransfersQuery, transferListFragment), variables)
	if err != nil {
		return nil, err
	}

	return result.Data.V2Transfers, nil
}

// FetchNewTransfers fetches new transfers after a given sort order
func (c *Client) FetchNewTransfers(ctx context.Context, lastSortOrder string, limit int, network *string) ([]models.Transfer, error) {
	variables := map[string]interface{}{
		"page":  lastSortOrder,
		"limit": limit,
	}
	if network != nil {
		variables["network"] = *network
	}

	result, err := c.fetchGraphQL(ctx, fmt.Sprintf(newTransfersQuery, transferListFragment), variables)
	if err != nil {
		return nil, err
	}

	return result.Data.V2Transfers, nil
}

// FetchChains fetches chain information
func (c *Client) FetchChains(ctx context.Context) ([]models.Chain, error) {
	result, err := c.fetchGraphQL(ctx, chainsQuery, nil)
	if err != nil {
		return nil, err
	}

	return result.Data.V2Chains, nil
}

// Close closes the HTTP client and cleans up connections
func (c *Client) Close() {
	if c.httpClient != nil && c.httpClient.Transport != nil {
		if transport, ok := c.httpClient.Transport.(*http.Transport); ok {
			transport.CloseIdleConnections()
		}
	}
}

// GetConnectionStats returns HTTP client connection statistics for monitoring
func (c *Client) GetConnectionStats() map[string]interface{} {
	stats := map[string]interface{}{
		"endpoint": c.endpoint,
		"timeout":  c.httpClient.Timeout.String(),
	}
	
	if transport, ok := c.httpClient.Transport.(*http.Transport); ok {
		stats["transport"] = map[string]interface{}{
			"maxIdleConns":        transport.MaxIdleConns,
			"maxIdleConnsPerHost": transport.MaxIdleConnsPerHost,
			"maxConnsPerHost":     transport.MaxConnsPerHost,
			"idleConnTimeout":     transport.IdleConnTimeout.String(),
			"tlsHandshakeTimeout": transport.TLSHandshakeTimeout.String(),
			"disableKeepAlives":   transport.DisableKeepAlives,
			"disableCompression":  transport.DisableCompression,
		}
	}
	
	return stats
} 