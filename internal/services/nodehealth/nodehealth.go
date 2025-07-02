package nodehealth

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
	"websocket-backend/api/graphql"
	"websocket-backend/internal/utils"
	"websocket-backend/models"
)

// Health check thresholds
const (
	HealthyThreshold   = 0.8 // 80% or more checks must pass
	DegradedThreshold  = 0.5 // 50-79% checks pass
	// Below 50% = unhealthy
)

// Config holds node health checker configuration
type Config struct {
	GraphQLURL      string        `json:"graphqlUrl"`      // GraphQL endpoint URL
	CheckInterval   time.Duration `json:"checkInterval"`   // How often to check nodes (default: 5 minutes)
	RequestTimeout  time.Duration `json:"requestTimeout"`  // Timeout for individual RPC requests (default: 10 seconds)
	MaxConcurrency  int           `json:"maxConcurrency"`  // Max concurrent health checks (default: 10)
}

// DefaultNodeHealthConfig returns default node health configuration
func DefaultNodeHealthConfig() Config {
	return Config{
		GraphQLURL:     "https://staging.graphql.union.build/v1/graphql",
		CheckInterval:  5 * time.Minute, // Check every 5 minutes
		RequestTimeout: 10 * time.Second, // 10 second timeout per request
		MaxConcurrency: 10,               // Max 10 concurrent health checks
	}
}

// HealthUpdateCallback is called when health data is updated
type HealthUpdateCallback func([]models.NodeHealthData)

// CacheInterface defines the interface for caching health data
type CacheInterface interface {
	SetWithTTL(key string, data interface{}, ttl time.Duration)
}

// Service manages node health checking
type Service struct {
	config          Config
	graphql         *graphql.Client
	healthData      map[string]models.NodeHealthData
	mu              sync.RWMutex
	httpClient      *http.Client
	healthCallback  HealthUpdateCallback
	semaphore       chan struct{} // For limiting concurrent checks
	cache           CacheInterface // Cache interface for storing health data
}

// NewService creates a new node health service
func NewService(config Config) *Service {
	return &Service{
		config:     config,
		graphql:    graphql.NewClient(config.GraphQLURL),
		healthData: make(map[string]models.NodeHealthData),
		httpClient: &http.Client{
			Timeout: config.RequestTimeout,
			Transport: &http.Transport{
				MaxIdleConns:        50,
				MaxIdleConnsPerHost: 10,
				MaxConnsPerHost:     20,
				IdleConnTimeout:     30 * time.Second,
				DisableKeepAlives:   false,
			},
		},
		semaphore: make(chan struct{}, config.MaxConcurrency),
	}
}

// SetHealthCallback sets the callback function for health updates
func (s *Service) SetHealthCallback(callback HealthUpdateCallback) {
	s.healthCallback = callback
}

// SetCache sets the cache interface for storing health data
func (s *Service) SetCache(cache CacheInterface) {
	s.cache = cache
}

// Start begins the node health service
func (s *Service) Start(ctx context.Context) {
	utils.LogInfo("NODE_HEALTH", "Starting node health service")
	
	// Initial health check with timeout
	checkCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	if err := s.performHealthChecks(checkCtx); err != nil {
		utils.LogError("NODE_HEALTH", "Failed to perform initial health checks: %v", err)
	}
	cancel()
	
	ticker := time.NewTicker(s.config.CheckInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			if err := s.performHealthChecks(checkCtx); err != nil {
				utils.LogError("NODE_HEALTH", "Failed to perform health checks: %v", err)
			}
			cancel()
		}
	}
}

// performHealthChecks fetches chains and checks all RPC endpoints
func (s *Service) performHealthChecks(ctx context.Context) error {
	utils.LogInfo("NODE_HEALTH", "Starting health checks")
	
	// Fetch chains with RPCs
	chains, err := s.graphql.FetchChainsWithRpcs(ctx)
	if err != nil {
		return fmt.Errorf("failed to fetch chains: %w", err)
	}
	
	// Count total RPC endpoints (excluding GRPC/REST)
	totalEndpoints := 0
	actualRpcCount := 0
	for _, chain := range chains {
		for _, rpc := range chain.Rpcs {
			totalEndpoints++
			if s.isRpcEndpoint(rpc.URL) {
				actualRpcCount++
			}
		}
	}
	utils.LogInfo("NODE_HEALTH", "Found %d chains with %d total endpoints (%d RPC, %d filtered out)", 
		len(chains), totalEndpoints, actualRpcCount, totalEndpoints-actualRpcCount)
	
	var wg sync.WaitGroup
	var healthData []models.NodeHealthData
	var healthMu sync.Mutex
	
	// Check each RPC endpoint (filter out GRPC and REST endpoints)
	for _, chain := range chains {
		for _, rpc := range chain.Rpcs {
			// Only check actual RPC endpoints, not GRPC or REST/API endpoints
			if !s.isRpcEndpoint(rpc.URL) {
				utils.LogDebug("NODE_HEALTH", "Skipping non-RPC endpoint: %s", rpc.URL)
				continue
			}
			
			wg.Add(1)
			go func(chain models.Chain, rpc models.Rpc) {
				defer wg.Done()
				
				// Acquire semaphore to limit concurrency
				s.semaphore <- struct{}{}
				defer func() { <-s.semaphore }()
				
				healthResult := s.checkSingleNode(ctx, chain, rpc)
				
				healthMu.Lock()
				healthData = append(healthData, healthResult)
				healthMu.Unlock()
			}(chain, rpc)
		}
	}
	
	wg.Wait()
	
	// Update stored health data
	s.updateHealthData(healthData)
	
	// Count health results
	healthy := 0
	degraded := 0
	unhealthy := 0
	for _, data := range healthData {
		switch data.Status {
		case "healthy":
			healthy++
		case "degraded":
			degraded++
		case "unhealthy":
			unhealthy++
		}
	}
	
	// Trigger callback if set
	if s.healthCallback != nil {
		s.healthCallback(healthData)
		utils.LogInfo("NODE_HEALTH", "Triggered health callback with %d nodes", len(healthData))
	}
	
	utils.LogInfo("NODE_HEALTH", "Completed health checks for %d nodes: %d healthy, %d degraded, %d unhealthy", 
		len(healthData), healthy, degraded, unhealthy)
	return nil
}

// HealthCheckResult represents the result of a single health check
type HealthCheckResult struct {
	Name         string
	Success      bool
	ResponseTime time.Duration
	Error        error
	Data         map[string]interface{}
}

// checkSingleNode performs comprehensive health checks on a single RPC endpoint
func (s *Service) checkSingleNode(ctx context.Context, chain models.Chain, rpc models.Rpc) models.NodeHealthData {
	// Create initial health data
	healthData := models.NodeHealthData{
		ChainID:       chain.UniversalChainID,
		ChainName:     chain.DisplayName,
		RpcURL:        rpc.URL,
		RpcType:       chain.RpcType,
		Status:        "unhealthy", // Default to unhealthy, will be updated based on checks
		LastCheckTime: time.Now().Unix(),
	}
	
	// Perform multiple health checks
	var results []HealthCheckResult
	totalResponseTime := time.Duration(0)
	
	if chain.RpcType == "cosmos" {
		results = s.performCosmosHealthChecks(ctx, rpc.URL)
	} else {
		results = s.performEvmHealthChecks(ctx, rpc.URL)
	}
	
	// Analyze results and determine overall health
	successfulChecks := 0
	var blockHeight *int64
	var errorMessages []string
	
	for _, result := range results {
		totalResponseTime += result.ResponseTime
		if result.Success {
			successfulChecks++
			// Extract block height from successful checks
			if height, ok := result.Data["blockHeight"].(int64); ok && blockHeight == nil {
				blockHeight = &height
			}
		} else if result.Error != nil {
			errorMessages = append(errorMessages, fmt.Sprintf("%s: %v", result.Name, result.Error))
		}
	}
	
	// Calculate average response time
	if len(results) > 0 {
		avgResponseTime := totalResponseTime / time.Duration(len(results))
		healthData.ResponseTimeMs = int(avgResponseTime.Milliseconds())
	}
	
	// Determine overall health status based on success rate
	healthData.Status, healthData.ErrorMessage = s.determineHealthStatus(successfulChecks, len(results), blockHeight, errorMessages, chain, rpc)
	
	// Set block height for healthy and degraded nodes
	if healthData.Status == "healthy" || healthData.Status == "degraded" {
		healthData.LatestBlockHeight = blockHeight
	}
	
	return healthData
}

// performCosmosHealthChecks performs multiple health checks on a Cosmos RPC endpoint
func (s *Service) performCosmosHealthChecks(ctx context.Context, rpcURL string) []HealthCheckResult {
	var results []HealthCheckResult
	
	// Check 1: Basic status and block height
	results = append(results, s.checkCosmosStatus(ctx, rpcURL))
	
	// Check 2: Network info and peer connectivity
	results = append(results, s.checkCosmosNetInfo(ctx, rpcURL))
	
	// Check 3: Application info
	results = append(results, s.checkCosmosAbciInfo(ctx, rpcURL))
	
	return results
}

// performEvmHealthChecks performs multiple health checks on an EVM RPC endpoint
func (s *Service) performEvmHealthChecks(ctx context.Context, rpcURL string) []HealthCheckResult {
	var results []HealthCheckResult
	
	// Check 1: Basic block number
	results = append(results, s.checkEvmBlockNumber(ctx, rpcURL))
	
	// Check 2: Sync status
	results = append(results, s.checkEvmSyncing(ctx, rpcURL))
	
	// Check 3: Network version
	results = append(results, s.checkEvmNetVersion(ctx, rpcURL))
	
	return results
}

// checkCosmosStatus performs status check on a Cosmos RPC endpoint
func (s *Service) checkCosmosStatus(ctx context.Context, rpcURL string) HealthCheckResult {
	start := time.Now()
	result := HealthCheckResult{
		Name: "status",
		Data: make(map[string]interface{}),
	}
	
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "status",
		"params":  []interface{}{},
	}
	
	// Perform the basic JSON-RPC call and parse Cosmos-specific response
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		result.Error = fmt.Errorf("failed to marshal request: %w", err)
		result.ResponseTime = time.Since(start)
		return result
	}
	
	req, err := http.NewRequestWithContext(ctx, "POST", rpcURL, bytes.NewBuffer(jsonData))
	if err != nil {
		result.Error = fmt.Errorf("failed to create request: %w", err)
		result.ResponseTime = time.Since(start)
		return result
	}
	
	req.Header.Set("Content-Type", "application/json")
	
	resp, err := s.httpClient.Do(req)
	if err != nil {
		result.Error = fmt.Errorf("request failed: %w", err)
		result.ResponseTime = time.Since(start)
		return result
	}
	defer resp.Body.Close()
	
	result.ResponseTime = time.Since(start)
	
	if resp.StatusCode != http.StatusOK {
		result.Error = fmt.Errorf("HTTP %d", resp.StatusCode)
		return result
	}
	
	var response struct {
		Result struct {
			SyncInfo struct {
				LatestBlockHeight string `json:"latest_block_height"`
				CatchingUp        bool   `json:"catching_up"`
			} `json:"sync_info"`
			NodeInfo struct {
				Moniker string `json:"moniker"`
				Network string `json:"network"`
			} `json:"node_info"`
		} `json:"result"`
		Error *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		result.Error = fmt.Errorf("failed to decode response: %w", err)
		return result
	}
	
	if response.Error != nil {
		result.Error = fmt.Errorf("RPC error %d: %s", response.Error.Code, response.Error.Message)
		return result
	}
	
	if response.Result.SyncInfo.LatestBlockHeight == "" {
		result.Error = fmt.Errorf("missing block height in response")
		return result
	}
	
	blockHeight, err := strconv.ParseInt(response.Result.SyncInfo.LatestBlockHeight, 10, 64)
	if err != nil {
		result.Error = fmt.Errorf("invalid block height: %w", err)
		return result
	}
	
	result.Success = true
	result.Data["blockHeight"] = blockHeight
	result.Data["catchingUp"] = response.Result.SyncInfo.CatchingUp
	result.Data["moniker"] = response.Result.NodeInfo.Moniker
	result.Data["network"] = response.Result.NodeInfo.Network
	
	return result
}

// checkCosmosNetInfo performs network info check on a Cosmos RPC endpoint
func (s *Service) checkCosmosNetInfo(ctx context.Context, rpcURL string) HealthCheckResult {
	start := time.Now()
	result := HealthCheckResult{
		Name: "net_info",
		Data: make(map[string]interface{}),
	}
	
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "net_info",
		"params":  []interface{}{},
	}
	
	err := s.performJsonRpcCall(ctx, rpcURL, reqBody, &result)
	result.ResponseTime = time.Since(start)
	
	if err != nil {
		result.Error = err
		return result
	}
	
	result.Success = true
	return result
}

// checkCosmosAbciInfo performs ABCI info check on a Cosmos RPC endpoint
func (s *Service) checkCosmosAbciInfo(ctx context.Context, rpcURL string) HealthCheckResult {
	start := time.Now()
	result := HealthCheckResult{
		Name: "abci_info",
		Data: make(map[string]interface{}),
	}
	
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "abci_info",
		"params":  []interface{}{},
	}
	
	err := s.performJsonRpcCall(ctx, rpcURL, reqBody, &result)
	result.ResponseTime = time.Since(start)
	
	if err != nil {
		result.Error = err
		return result
	}
	
	result.Success = true
	return result
}

// checkEvmBlockNumber performs block number check on an EVM RPC endpoint
func (s *Service) checkEvmBlockNumber(ctx context.Context, rpcURL string) HealthCheckResult {
	start := time.Now()
	result := HealthCheckResult{
		Name: "eth_blockNumber",
		Data: make(map[string]interface{}),
	}
	
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_blockNumber",
		"params":  []interface{}{},
	}
	
	// Perform the basic JSON-RPC call and parse EVM-specific response
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		result.Error = fmt.Errorf("failed to marshal request: %w", err)
		result.ResponseTime = time.Since(start)
		return result
	}
	
	req, err := http.NewRequestWithContext(ctx, "POST", rpcURL, bytes.NewBuffer(jsonData))
	if err != nil {
		result.Error = fmt.Errorf("failed to create request: %w", err)
		result.ResponseTime = time.Since(start)
		return result
	}
	
	req.Header.Set("Content-Type", "application/json")
	
	resp, err := s.httpClient.Do(req)
	if err != nil {
		result.Error = fmt.Errorf("request failed: %w", err)
		result.ResponseTime = time.Since(start)
		return result
	}
	defer resp.Body.Close()
	
	result.ResponseTime = time.Since(start)
	
	if resp.StatusCode != http.StatusOK {
		result.Error = fmt.Errorf("HTTP %d", resp.StatusCode)
		return result
	}
	
	var response struct {
		Result string `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		result.Error = fmt.Errorf("failed to decode response: %w", err)
		return result
	}
	
	if response.Error != nil {
		result.Error = fmt.Errorf("RPC error %d: %s", response.Error.Code, response.Error.Message)
		return result
	}
	
	if response.Result == "" {
		result.Error = fmt.Errorf("missing block number in response")
		return result
	}
	
	// Parse hex block number
	blockHeight, err := strconv.ParseInt(response.Result, 0, 64)
	if err != nil {
		result.Error = fmt.Errorf("invalid block number: %w", err)
		return result
	}
	
	result.Success = true
	result.Data["blockHeight"] = blockHeight
	result.Data["blockNumberHex"] = response.Result
	
	return result
}

// checkEvmSyncing performs sync status check on an EVM RPC endpoint
func (s *Service) checkEvmSyncing(ctx context.Context, rpcURL string) HealthCheckResult {
	start := time.Now()
	result := HealthCheckResult{
		Name: "eth_syncing",
		Data: make(map[string]interface{}),
	}
	
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "eth_syncing",
		"params":  []interface{}{},
	}
	
	err := s.performJsonRpcCall(ctx, rpcURL, reqBody, &result)
	result.ResponseTime = time.Since(start)
	
	if err != nil {
		result.Error = err
		return result
	}
	
	result.Success = true
	return result
}

// checkEvmNetVersion performs network version check on an EVM RPC endpoint
func (s *Service) checkEvmNetVersion(ctx context.Context, rpcURL string) HealthCheckResult {
	start := time.Now()
	result := HealthCheckResult{
		Name: "net_version",
		Data: make(map[string]interface{}),
	}
	
	reqBody := map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      1,
		"method":  "net_version",
		"params":  []interface{}{},
	}
	
	err := s.performJsonRpcCall(ctx, rpcURL, reqBody, &result)
	result.ResponseTime = time.Since(start)
	
	if err != nil {
		result.Error = err
		return result
	}
	
	result.Success = true
	return result
}

// performJsonRpcCall is a helper function for simple JSON-RPC calls
func (s *Service) performJsonRpcCall(ctx context.Context, rpcURL string, reqBody map[string]interface{}, result *HealthCheckResult) error {
	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}
	
	req, err := http.NewRequestWithContext(ctx, "POST", rpcURL, bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	
	req.Header.Set("Content-Type", "application/json")
	
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	
	var response struct {
		Result interface{} `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return fmt.Errorf("failed to decode response: %w", err)
	}
	
	if response.Error != nil {
		return fmt.Errorf("RPC error %d: %s", response.Error.Code, response.Error.Message)
	}
	
	return nil
}

// updateHealthData updates the stored health data atomically and caches it
func (s *Service) updateHealthData(healthData []models.NodeHealthData) {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	// Clear old data and store new data
	s.healthData = make(map[string]models.NodeHealthData)
	for _, data := range healthData {
		s.healthData[data.RpcURL] = data // URL is unique identifier
	}
	
	// Cache the health summary data for chart broadcaster (6-minute TTL to match other chart data)
	if s.cache != nil && len(healthData) > 0 {
		// Get the summary object that the frontend expects
		summary := s.getHealthSummaryInternal() // Use internal method to avoid double-locking
		s.cache.SetWithTTL("node_health_data", summary, 6*time.Minute)
		utils.LogInfo("NODE_HEALTH", "✅ Cached node health summary data with 6-minute TTL (%d total nodes)", len(healthData))
	}
}

// GetHealthData returns current health data
func (s *Service) GetHealthData() []models.NodeHealthData {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	result := make([]models.NodeHealthData, 0, len(s.healthData))
	for _, data := range s.healthData {
		result = append(result, data)
	}
	
	return result
}

// GetHealthDataInterface returns current health data as []interface{} for chart broadcaster
func (s *Service) GetHealthDataInterface() []interface{} {
	// Return the summary object that the frontend expects, not the raw array
	summary := s.GetHealthSummary()
	return []interface{}{summary}
}

// determineHealthStatus determines the health status based on success rate
func (s *Service) determineHealthStatus(successfulChecks, totalChecks int, blockHeight *int64, errorMessages []string, chain models.Chain, rpc models.Rpc) (status string, errorMessage string) {
	successRate := float64(successfulChecks) / float64(totalChecks)
	
	switch {
	case successRate >= HealthyThreshold:
		utils.LogDebug("NODE_HEALTH", "Node %s (%s) healthy: %d/%d checks passed", 
			chain.DisplayName, rpc.URL, successfulChecks, totalChecks)
		return "healthy", ""
		
	case successRate >= DegradedThreshold:
		errorMsg := fmt.Sprintf("Partial failure: %d/%d checks failed", 
			totalChecks-successfulChecks, totalChecks)
		utils.LogWarn("NODE_HEALTH", "Node %s (%s) degraded: %d/%d checks passed", 
			chain.DisplayName, rpc.URL, successfulChecks, totalChecks)
		return "degraded", errorMsg
		
	default:
		var errorMsg string
		if len(errorMessages) > 0 {
			errorMsg = strings.Join(errorMessages, "; ")
		} else {
			errorMsg = fmt.Sprintf("Failed %d/%d health checks", 
				totalChecks-successfulChecks, totalChecks)
		}
		utils.LogWarn("NODE_HEALTH", "Node %s (%s) unhealthy: %d/%d checks failed", 
			chain.DisplayName, rpc.URL, totalChecks-successfulChecks, totalChecks)
		return "unhealthy", errorMsg
	}
}

// convertNodeToInterface converts NodeHealthData to interface{} map for frontend
func (s *Service) convertNodeToInterface(data models.NodeHealthData) map[string]interface{} {
	nodeInterface := map[string]interface{}{
		"chainId":         data.ChainID,
		"chainName":       data.ChainName,
		"rpcUrl":          data.RpcURL,
		"rpcType":         data.RpcType,
		"status":          data.Status,
		"responseTimeMs":  data.ResponseTimeMs,
		"lastCheckTime":   data.LastCheckTime,
	}
	
	// Add optional fields
	if data.LatestBlockHeight != nil {
		nodeInterface["latestBlockHeight"] = *data.LatestBlockHeight
	}
	if data.ErrorMessage != "" {
		nodeInterface["errorMessage"] = data.ErrorMessage
	}
	
	return nodeInterface
}

// aggregateStatusCounts counts the different health statuses
func (s *Service) aggregateStatusCounts(healthData map[string]models.NodeHealthData) (healthy, degraded, unhealthy, totalNodes int, totalResponseTime int) {
	for _, data := range healthData {
		totalNodes++
		switch data.Status {
		case "healthy":
			healthy++
		case "degraded":
			degraded++
		case "unhealthy":
			unhealthy++
		}
		totalResponseTime += data.ResponseTimeMs
	}
	return
}

// buildChainHealthStats calculates per-chain health statistics
func (s *Service) buildChainHealthStats(healthData map[string]models.NodeHealthData) map[string]interface{} {
	chainCounts := make(map[string]map[string]int)
	chainResponseTimes := make(map[string][]int)
	
	// Collect data per chain
	for _, data := range healthData {
		if chainCounts[data.ChainName] == nil {
			chainCounts[data.ChainName] = make(map[string]int)
		}
		chainCounts[data.ChainName]["total"]++
		chainCounts[data.ChainName][data.Status]++
		chainResponseTimes[data.ChainName] = append(chainResponseTimes[data.ChainName], data.ResponseTimeMs)
	}
	
	// Calculate stats per chain
	chainHealthStats := make(map[string]interface{})
	for chainName, counts := range chainCounts {
		avgResponseTime := 0.0
		if len(chainResponseTimes[chainName]) > 0 {
			sum := 0
			for _, rt := range chainResponseTimes[chainName] {
				sum += rt
			}
			avgResponseTime = float64(sum) / float64(len(chainResponseTimes[chainName]))
		}
		
		chainHealthStats[chainName] = map[string]interface{}{
			"chainName":       chainName,
			"healthyNodes":    counts["healthy"],
			"totalNodes":      counts["total"],
			"avgResponseTime": avgResponseTime,
		}
	}
	
	return chainHealthStats
}

// GetHealthSummary returns health data in the NodeHealthSummary format expected by the frontend
func (s *Service) GetHealthSummary() interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	
	return s.getHealthSummaryInternal()
}

// getHealthSummaryInternal returns health summary without acquiring locks (for internal use)
func (s *Service) getHealthSummaryInternal() interface{} {
	// Convert health data to interface{} array for nodesWithRpcs
	nodesWithRpcs := make([]interface{}, 0, len(s.healthData))
	for _, data := range s.healthData {
		nodesWithRpcs = append(nodesWithRpcs, s.convertNodeToInterface(data))
	}
	
	// Aggregate status counts and response times
	healthyNodes, degradedNodes, unhealthyNodes, totalNodes, totalResponseTime := s.aggregateStatusCounts(s.healthData)
	
	// Calculate average response time
	avgResponseTime := 0.0
	if totalNodes > 0 {
		avgResponseTime = float64(totalResponseTime) / float64(totalNodes)
	}
	
	// Build per-chain health statistics
	chainHealthStats := s.buildChainHealthStats(s.healthData)
	
	// Create the summary object that matches the frontend interface
	summary := map[string]interface{}{
		"totalNodes":       totalNodes,
		"healthyNodes":     healthyNodes,
		"degradedNodes":    degradedNodes,
		"unhealthyNodes":   unhealthyNodes,
		"avgResponseTime":  avgResponseTime,
		"nodesWithRpcs":    nodesWithRpcs,
		"chainHealthStats": chainHealthStats,
	}
	
	return summary
}

// isRpcEndpoint checks if a URL is an actual RPC endpoint (not GRPC or REST)
func (s *Service) isRpcEndpoint(url string) bool {
	// Convert to lowercase for case-insensitive matching
	lowerURL := strings.ToLower(url)
	
	// Skip GRPC endpoints
	if strings.Contains(lowerURL, "grpc") {
		return false
	}
	
	// Skip REST/API endpoints
	if strings.Contains(lowerURL, "/rest/") || strings.Contains(lowerURL, "rest.") || 
	   strings.Contains(lowerURL, "api.") || strings.Contains(lowerURL, "/api/") {
		return false
	}
	
	// Accept RPC endpoints (should contain "rpc" in hostname or path)
	if strings.Contains(lowerURL, "rpc") {
		return true
	}
	
	return true
}

 