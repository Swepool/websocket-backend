package stats

import (
	"sync"
	"time"
	"sort"
	"strings"
	"fmt"
	"websocket-backend-new/models"
	"websocket-backend-new/internal/utils"
	"github.com/axiomhq/hyperloglog"
)

// Config holds stats collection configuration
type Config struct {
	RetentionHours         int           // How long to keep detailed buckets (default: 24) - DEPRECATED: use specific retention fields
	TopItemsLimit          int           // Max items to return for charts (default: 20)
	TopItemsTimeScale      int           // Max items to return for time scale data (default: 10)
	CleanupInterval        time.Duration // How often to run cleanup (default: 15 minutes for aggressive cleanup)
	TenSecondRetention     time.Duration // How long to keep 10s buckets (default: 30 minutes for memory optimization)
	
	// 5-tier retention system with 1-year maximum
	OneMinuteRetention     time.Duration // How long to keep 1m buckets before aggregating to 1h (default: 6 hours)
	OneHourRetention       time.Duration // How long to keep 1h buckets before aggregating to daily (default: 7 days)
	DailyRetention         time.Duration // How long to keep daily buckets before aggregating to weekly (default: 30 days)
	WeeklyRetention        time.Duration // How long to keep weekly buckets before deletion (default: 1 year)
	MonthlyRetention       time.Duration // DEPRECATED - no longer used (set to 0)
	YearlyRetention        time.Duration // DEPRECATED - no longer used (set to 0)
}

// AddressChainInfo stores chain context for address formatting
type AddressChainInfo struct {
	RpcType    string
	AddrPrefix string
}

// Memory pools for bucket reuse and HLL optimization
var (
	bucketPool = sync.Pool{
		New: func() interface{} {
			return &StatsBucket{
				Routes:            make(map[string]*RouteStats),
				ChainOutgoing:     make(map[string]int64),
				ChainIncoming:     make(map[string]int64),
				ChainSendersHLL:   make(map[string]*hyperloglog.Sketch),
				ChainReceiversHLL: make(map[string]*hyperloglog.Sketch),
				ChainUsersHLL:     make(map[string]*hyperloglog.Sketch),
				ChainAssets:       make(map[string]map[string]*ChainAssetStats),
				Senders:           make(map[string]int64),
				Receivers:         make(map[string]int64),
				Assets:            make(map[string]*AssetStats),
				// HLL sketches will be created with optimized precision when needed
			}
		},
	}

	// Note: HLL sketches cannot be pooled since axiomhq/hyperloglog doesn't provide a Clear() method
	// Memory savings will come from reduced precision (New14 vs New16) rather than pooling
)

// DefaultConfig returns optimized defaults for 1-year maximum retention
func DefaultConfig() Config {
	return Config{
		RetentionHours:     24,  // Kept for backward compatibility - will be ignored in favor of specific retention fields
		TopItemsLimit:      20,
		TopItemsTimeScale:  10,
		CleanupInterval:    15 * time.Minute, // More aggressive cleanup
		TenSecondRetention: 30 * time.Minute, // 10s→1m after 30 minutes
		
		// 5-tier retention for 1-year maximum (simplified system)
		OneMinuteRetention: 6 * time.Hour,        // 1m→1h after 6 hours
		OneHourRetention:   7 * 24 * time.Hour,   // 1h→daily after 7 days  
		DailyRetention:     30 * 24 * time.Hour,  // daily→weekly after 30 days
		WeeklyRetention:    365 * 24 * time.Hour, // weekly→delete after 1 year
		MonthlyRetention:   0,                    // REMOVED - no monthly buckets
		YearlyRetention:    0,                    // REMOVED - no yearly buckets
	}
}

// Collector tracks transfer statistics using time-based buckets and HyperLogLog
type Collector struct {
	config     Config
	mu         sync.RWMutex
	startTime  time.Time
	
	// 5-tier time-based buckets for 1-year maximum retention
	buckets10s     map[time.Time]*StatsBucket // 10-second granularity (0-30min)
	buckets1m      map[time.Time]*StatsBucket // 1-minute granularity (30min-6h)
	buckets1h      map[time.Time]*StatsBucket // 1-hour granularity (6h-7d)
	bucketsDaily   map[time.Time]*StatsBucket // Daily granularity (7d-30d)
	bucketsWeekly  map[time.Time]*StatsBucket // Weekly granularity (30d-1y, then delete)
	
	// Global HLL sketches for overall unique counting (keep high precision)
	globalSendersHLL   *hyperloglog.Sketch
	globalReceiversHLL *hyperloglog.Sketch
	globalWalletsHLL   *hyperloglog.Sketch
	
	// Asset symbol mapping
	denomToSymbol map[string]string
	
	// Address formatting
	addressFormatter *utils.AddressFormatter
	addressChainInfo map[string]*AddressChainInfo // address -> chain info for formatting
	
	// Latency data
	latencyData []models.LatencyData
	
	// Previous period snapshots for percentage calculations
	previousMinute  *PeriodStats
	previousHour    *PeriodStats
	previousDay     *PeriodStats
	previous7Days   *PeriodStats
	previous14Days  *PeriodStats
	previous30Days  *PeriodStats
	
	// Last snapshot times
	lastMinuteSnapshot  time.Time
	lastHourSnapshot    time.Time
	lastDaySnapshot     time.Time
	last7DaysSnapshot   time.Time
	last14DaysSnapshot  time.Time
	last30DaysSnapshot  time.Time
	
	// Last cleanup time
	lastCleanup time.Time
}

// getOrCreateOptimizedHLL creates HLL with appropriate precision for the use case
func getOrCreateOptimizedHLL(hllType string) *hyperloglog.Sketch {
	switch hllType {
	case "bucket": // Per-bucket unique counting - medium precision
		return hyperloglog.New14() // 1.5KB, 1.63% error - good balance for bucket-level tracking
	case "chain", "asset": // Per-chain/asset unique counting - lower precision is acceptable
		return hyperloglog.New14() // Use same precision since New12() doesn't exist
	default: // Global/critical counting - high precision
		return hyperloglog.New16() // 6KB, 0.81% error - highest precision for global metrics
	}
}

// returnOptimizedHLL is no longer needed since HLL sketches can't be reset/pooled
// The axiomhq/hyperloglog library doesn't provide a Clear() method, so we can't safely pool HLL sketches
func returnOptimizedHLL(hll *hyperloglog.Sketch, hllType string) {
	// No-op: HLL sketches cannot be reset, so no pooling
	// Memory will be handled by Go's garbage collector
}

// getBucketFromPool gets a bucket from the pool and initializes it
func getBucketFromPool(timestamp time.Time, granularity string) *StatsBucket {
	bucket := bucketPool.Get().(*StatsBucket)
	
	// Reset bucket state
	bucket.Timestamp = timestamp
	bucket.Granularity = granularity
	bucket.TransferCount = 0
	
	// Clear maps but reuse underlying storage
	for k := range bucket.Routes {
		delete(bucket.Routes, k)
	}
	for k := range bucket.ChainOutgoing {
		delete(bucket.ChainOutgoing, k)
	}
	for k := range bucket.ChainIncoming {
		delete(bucket.ChainIncoming, k)
	}
	for k := range bucket.ChainSendersHLL {
		delete(bucket.ChainSendersHLL, k)
	}
	for k := range bucket.ChainReceiversHLL {
		delete(bucket.ChainReceiversHLL, k)
	}
	for k := range bucket.ChainUsersHLL {
		delete(bucket.ChainUsersHLL, k)
	}
	for k := range bucket.ChainAssets {
		delete(bucket.ChainAssets, k)
	}
	for k := range bucket.Senders {
		delete(bucket.Senders, k)
	}
	for k := range bucket.Receivers {
		delete(bucket.Receivers, k)
	}
	for k := range bucket.Assets {
		delete(bucket.Assets, k)
	}
	
	// Initialize optimized HLL sketches
	bucket.SendersHLL = getOrCreateOptimizedHLL("bucket")
	bucket.ReceiversHLL = getOrCreateOptimizedHLL("bucket")
	bucket.WalletsHLL = getOrCreateOptimizedHLL("bucket")
	
	return bucket
}

// returnBucketToPool returns a bucket to the pool after cleanup
func returnBucketToPool(bucket *StatsBucket) {
	if bucket == nil {
		return
	}
	
	// HLL sketches cannot be pooled since they can't be reset
	// They will be garbage collected naturally
	
	// Clear references to allow garbage collection
	bucket.SendersHLL = nil
	bucket.ReceiversHLL = nil
	bucket.WalletsHLL = nil
	
	// Clear chain HLL references
	for k := range bucket.ChainSendersHLL {
		bucket.ChainSendersHLL[k] = nil
		delete(bucket.ChainSendersHLL, k)
	}
	for k := range bucket.ChainReceiversHLL {
		bucket.ChainReceiversHLL[k] = nil
		delete(bucket.ChainReceiversHLL, k)
	}
	for k := range bucket.ChainUsersHLL {
		bucket.ChainUsersHLL[k] = nil
		delete(bucket.ChainUsersHLL, k)
	}
	
	// Clear asset HLL references
	for _, assets := range bucket.ChainAssets {
		for _, asset := range assets {
			asset.HoldersHLL = nil
		}
	}
	for _, asset := range bucket.Assets {
		asset.HoldersHLL = nil
	}
	
	// Return bucket structure to pool (maps will be reused)
	bucketPool.Put(bucket)
}

// StatsBucket contains all stats for a specific time period
type StatsBucket struct {
	Timestamp   time.Time `json:"timestamp"`
	Granularity string    `json:"granularity"` // "10s", "1m", "1h", "daily", "weekly", "monthly", "yearly"
	
	// Transfer counts
	TransferCount int64 `json:"transferCount"`
	
	// Route tracking: "sourceChain→destChain" -> count
	Routes map[string]*RouteStats `json:"routes"`
	
	// Chain flow tracking
	ChainOutgoing map[string]int64 `json:"chainOutgoing"` // chainID -> count
	ChainIncoming map[string]int64 `json:"chainIncoming"` // chainID -> count
	
	// Chain user tracking (not serialized)
	ChainSendersHLL   map[string]*hyperloglog.Sketch `json:"-"` // chainID -> unique senders HLL
	ChainReceiversHLL map[string]*hyperloglog.Sketch `json:"-"` // chainID -> unique receivers HLL
	ChainUsersHLL     map[string]*hyperloglog.Sketch `json:"-"` // chainID -> unique users HLL
	
	// Chain asset tracking: chainID -> assetSymbol -> stats (not serialized)
	ChainAssets map[string]map[string]*ChainAssetStats `json:"-"`
	
	// Wallet activity tracking: address -> count
	Senders   map[string]int64 `json:"senders"`   // address -> send count
	Receivers map[string]int64 `json:"receivers"` // address -> receive count
	
	// Asset tracking: assetSymbol -> stats
	Assets map[string]*AssetStats `json:"assets"`
	
	// HLL for unique counting (not serialized)
	SendersHLL   *hyperloglog.Sketch `json:"-"`
	ReceiversHLL *hyperloglog.Sketch `json:"-"`
	WalletsHLL   *hyperloglog.Sketch `json:"-"`
}

// RouteStats tracks transfer counts for a specific route
type RouteStats struct {
	Count       int64   `json:"count"`
	FromChain   string  `json:"fromChain"`
	ToChain     string  `json:"toChain"`
	FromName    string  `json:"fromName"`
	ToName      string  `json:"toName"`
	Route       string  `json:"route"` // "ChainA → ChainB"
	CountChange float64 `json:"countChange,omitempty"` // Percentage change
}

type AssetRouteStats struct {
	FromChain     string  `json:"fromChain"`
	ToChain       string  `json:"toChain"`
	FromName      string  `json:"fromName"`
	ToName        string  `json:"toName"`
	Route         string  `json:"route"`         // "ChainA → ChainB"
	Count         int64   `json:"count"`         // Number of transfers
	Volume        float64 `json:"volume"`        // Total volume
	Percentage    float64 `json:"percentage"`    // Percentage of asset's total volume
	LastActivity  string  `json:"lastActivity"`  // ISO timestamp
}

// ChainAssetStats tracks asset flow data for a specific asset on a specific chain
type ChainAssetStats struct {
	AssetSymbol     string  `json:"assetSymbol"`
	AssetName       string  `json:"assetName"`
	OutgoingCount   int64   `json:"outgoingCount"`   // Transfers leaving this chain
	IncomingCount   int64   `json:"incomingCount"`   // Transfers coming to this chain
	NetFlow         int64   `json:"netFlow"`         // Incoming - Outgoing
	TotalVolume     float64 `json:"totalVolume"`     // Total volume for this asset on this chain
	AverageAmount   float64 `json:"averageAmount"`   // Average transfer amount
	Percentage      float64 `json:"percentage"`      // Percentage of chain's total activity
	LastActivity    string  `json:"lastActivity"`    // ISO timestamp
	
	// HLL for unique holder tracking (not serialized)
	HoldersHLL *hyperloglog.Sketch `json:"-"`
}

// AssetStats tracks volume and transfer data for a specific asset
type AssetStats struct {
	AssetSymbol     string  `json:"assetSymbol"`     // Frontend expects assetSymbol
	AssetName       string  `json:"assetName"`       // Frontend expects assetName
	TransferCount   int64   `json:"transferCount"`
	TotalVolume     float64 `json:"totalVolume"`
	LargestTransfer float64 `json:"largestTransfer"`
	AverageAmount   float64 `json:"averageAmount"`
	VolumeChange    float64 `json:"volumeChange,omitempty"`    // Percentage change
	CountChange     float64 `json:"countChange,omitempty"`     // Percentage change in transfer count
	UniqueHolders   int     `json:"uniqueHolders,omitempty"`   // Estimated unique holders (from HLL)
	LastActivity    string  `json:"lastActivity"`              // ISO timestamp
	TopRoutes       []AssetRouteStats `json:"topRoutes"`       // Top routes for this asset
	
	// Route tracking (not serialized - used for aggregation)
	Routes map[string]*AssetRouteStats `json:"-"`
	// HLL for unique holder counting (not serialized)
	HoldersHLL *hyperloglog.Sketch `json:"-"`
}

// WalletStats tracks individual wallet activity for frontend charts
type WalletStats struct {
	Address         string `json:"address"`
	DisplayAddress  string `json:"displayAddress"`
	Count           int64  `json:"count"`
	LastActivity    string `json:"lastActivity"`
}

// ChartData represents data ready for frontend charts
type ChartData struct {
	// Transfer rates and unique counts for all time periods
	TransferRates TransferRates `json:"transferRates"`
	
	// Global unique wallet counts (all time)
	UniqueWallets struct {
		Senders   int64 `json:"senders"`
		Receivers int64 `json:"receivers"`
		Total     int64 `json:"total"`
	} `json:"uniqueWallets"`
	
	// Top routes
	TopRoutes []RouteStats `json:"topRoutes"`
	
	// Chain flows
	ChainFlows []ChainFlowStats `json:"chainFlows"`
	
	// Top assets
	TopAssets []AssetStats `json:"topAssets"`
	
	// Metadata
	Timestamp time.Time `json:"timestamp"`
	Uptime    float64   `json:"uptime"`
}

// ChainFlowStats represents chain flow data
type ChainFlowStats struct {
	UniversalChainID string  `json:"universal_chain_id"`
	ChainName        string  `json:"chainName"`
	OutgoingCount    int64   `json:"outgoingCount"`
	IncomingCount    int64   `json:"incomingCount"`
	NetFlow          int64   `json:"netFlow"`
	OutgoingChange   float64 `json:"outgoingChange,omitempty"`
	IncomingChange   float64 `json:"incomingChange,omitempty"`
	NetFlowChange    float64 `json:"netFlowChange,omitempty"`
	UniqueSenders    int     `json:"uniqueSenders,omitempty"`    // Unique wallets sending FROM this chain
	UniqueReceivers  int     `json:"uniqueReceivers,omitempty"`  // Unique wallets receiving TO this chain
	UniqueUsers      int     `json:"uniqueUsers,omitempty"`      // Total unique wallets (senders + receivers)
	LastActivity     string  `json:"lastActivity"`
	Percentage       float64 `json:"percentage,omitempty"` // Keep for internal use
	TopAssets        []ChainAssetStats `json:"topAssets,omitempty"` // Top assets flowing through this chain
}

// NewCollector creates a new stats collector with 5-tier bucket system (1-year max)
func NewCollector(config Config) *Collector {
	return &Collector{
		config:         config,
		startTime:      time.Now(),
		buckets10s:     make(map[time.Time]*StatsBucket),
		buckets1m:      make(map[time.Time]*StatsBucket),
		buckets1h:      make(map[time.Time]*StatsBucket),
		bucketsDaily:   make(map[time.Time]*StatsBucket),
		bucketsWeekly:  make(map[time.Time]*StatsBucket),  // Final tier - deleted after 1 year
		globalSendersHLL:   hyperloglog.New16(),
		globalReceiversHLL: hyperloglog.New16(),
		globalWalletsHLL:   hyperloglog.New16(),
		denomToSymbol:      make(map[string]string),
		addressFormatter:   utils.NewAddressFormatter(),
		addressChainInfo:   make(map[string]*AddressChainInfo),
		lastCleanup:        time.Now(),
	}
}

// ProcessTransfer adds a transfer to the stats buckets
func (c *Collector) ProcessTransfer(transfer models.Transfer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	
	// Get or create 10-second bucket
	bucketTime := transfer.TransferSendTimestamp.Truncate(10 * time.Second)
	bucket := c.getOrCreateBucket(bucketTime, "10s")
	
	// Update bucket stats
	c.updateBucket(bucket, transfer)
	
	// Update global HLL
	senderBytes := []byte(strings.ToLower(transfer.SenderCanonical))
	receiverBytes := []byte(strings.ToLower(transfer.ReceiverCanonical))
	
	c.globalSendersHLL.Insert(senderBytes)
	c.globalReceiversHLL.Insert(receiverBytes)
	c.globalWalletsHLL.Insert(senderBytes)
	c.globalWalletsHLL.Insert(receiverBytes)
	
	// Store asset symbol mapping
	if transfer.BaseTokenSymbol != "" {
		c.denomToSymbol[transfer.BaseTokenSymbol] = transfer.BaseTokenSymbol
	}
	
	// Periodic cleanup
	if time.Since(c.lastCleanup) > c.config.CleanupInterval {
		c.cleanup()
		c.lastCleanup = time.Now()
	}
}

// getOrCreateBucket gets or creates a bucket for the given timestamp using optimized memory pools
func (c *Collector) getOrCreateBucket(timestamp time.Time, granularity string) *StatsBucket {
	var buckets map[time.Time]*StatsBucket
	
	switch granularity {
	case "10s":
		buckets = c.buckets10s
	case "1m":
		buckets = c.buckets1m
	case "1h":
		buckets = c.buckets1h
	case "daily":
		buckets = c.bucketsDaily
	case "weekly":
		buckets = c.bucketsWeekly
	default:
		buckets = c.buckets10s
	}
	
	bucket, exists := buckets[timestamp]
	if !exists {
		bucket = getBucketFromPool(timestamp, granularity)
		buckets[timestamp] = bucket
	} else {
		// CRITICAL FIX: Ensure HLL sketches are initialized even for existing buckets
		// This handles cases where cleanup nullified HLL but bucket remained in map
		if bucket.SendersHLL == nil {
			bucket.SendersHLL = getOrCreateOptimizedHLL("bucket")
		}
		if bucket.ReceiversHLL == nil {
			bucket.ReceiversHLL = getOrCreateOptimizedHLL("bucket")
		}
		if bucket.WalletsHLL == nil {
			bucket.WalletsHLL = getOrCreateOptimizedHLL("bucket")
		}
	}
	
	return bucket
}

// updateBucket updates bucket stats with a new transfer
func (c *Collector) updateBucket(bucket *StatsBucket, transfer models.Transfer) {
	// Increment transfer count
	bucket.TransferCount++
	
	// Validate addresses before HLL operations
	if transfer.SenderCanonical == "" || transfer.ReceiverCanonical == "" {
		return // Skip this transfer if addresses are invalid
	}
	
	// Update route stats
	routeKey := fmt.Sprintf("%s→%s", transfer.SourceChain.UniversalChainID, transfer.DestinationChain.UniversalChainID)
	route, exists := bucket.Routes[routeKey]
	if !exists {
		route = &RouteStats{
			FromChain: transfer.SourceChain.UniversalChainID,
			ToChain:   transfer.DestinationChain.UniversalChainID,
			FromName:  transfer.SourceChain.DisplayName,
			ToName:    transfer.DestinationChain.DisplayName,
			Route:     fmt.Sprintf("%s → %s", transfer.SourceChain.DisplayName, transfer.DestinationChain.DisplayName),
		}
		bucket.Routes[routeKey] = route
	}
	route.Count++
	
	// Update chain flows
	bucket.ChainOutgoing[transfer.SourceChain.UniversalChainID]++
	bucket.ChainIncoming[transfer.DestinationChain.UniversalChainID]++
	
	// Track unique users per chain using HLL
	senderBytes := []byte(strings.ToLower(transfer.SenderCanonical))
	receiverBytes := []byte(strings.ToLower(transfer.ReceiverCanonical))
	
	// Track sender for source chain using optimized HLL precision
	sourceChainID := transfer.SourceChain.UniversalChainID
	if bucket.ChainSendersHLL[sourceChainID] == nil {
			bucket.ChainSendersHLL[sourceChainID] = getOrCreateOptimizedHLL("chain")
	}
	if bucket.ChainUsersHLL[sourceChainID] == nil {
		bucket.ChainUsersHLL[sourceChainID] = getOrCreateOptimizedHLL("chain")
	}
	bucket.ChainSendersHLL[sourceChainID].Insert(senderBytes)
	bucket.ChainUsersHLL[sourceChainID].Insert(senderBytes)
	
	// Track receiver for destination chain using optimized HLL precision
	destChainID := transfer.DestinationChain.UniversalChainID
	if bucket.ChainReceiversHLL[destChainID] == nil {
		bucket.ChainReceiversHLL[destChainID] = getOrCreateOptimizedHLL("chain")
	}
	if bucket.ChainUsersHLL[destChainID] == nil {
		bucket.ChainUsersHLL[destChainID] = getOrCreateOptimizedHLL("chain")
	}
	bucket.ChainReceiversHLL[destChainID].Insert(receiverBytes)
	bucket.ChainUsersHLL[destChainID].Insert(receiverBytes)
	
	// Update wallet activity tracking
	bucket.Senders[transfer.SenderCanonical]++
	bucket.Receivers[transfer.ReceiverCanonical]++
	
	// Store chain info for address formatting (if not already stored)
	if _, exists := c.addressChainInfo[transfer.SenderCanonical]; !exists {
		c.addressChainInfo[transfer.SenderCanonical] = &AddressChainInfo{
			RpcType:    transfer.SourceChain.RpcType,
			AddrPrefix: transfer.SourceChain.AddrPrefix,
		}
	}
	if _, exists := c.addressChainInfo[transfer.ReceiverCanonical]; !exists {
		c.addressChainInfo[transfer.ReceiverCanonical] = &AddressChainInfo{
			RpcType:    transfer.DestinationChain.RpcType,
			AddrPrefix: transfer.DestinationChain.AddrPrefix,
		}
	}
	
	// Update asset stats using same logic as original implementation
	symbol := transfer.BaseTokenSymbol
	if symbol == "" {
		symbol = "unknown" // fallback if no symbol
	}
	
	var asset *AssetStats
	asset, exists = bucket.Assets[symbol]
	if !exists {
		asset = &AssetStats{
			AssetSymbol:  symbol,
			AssetName:    symbol, // Use symbol as name initially
			LastActivity: transfer.TransferSendTimestamp.Format(time.RFC3339),
			Routes:       make(map[string]*AssetRouteStats),
			HoldersHLL:   getOrCreateOptimizedHLL("asset"), // Initialize optimized HLL for unique holder tracking
		}
		bucket.Assets[symbol] = asset
	}
	
	// Update last activity if this transfer is more recent
	if transfer.TransferSendTimestamp.Format(time.RFC3339) > asset.LastActivity {
		asset.LastActivity = transfer.TransferSendTimestamp.Format(time.RFC3339)
	}
	
	asset.TransferCount++
	if amount := parseAmount(transfer.BaseAmount); amount > 0 {
		asset.TotalVolume += amount
		if amount > asset.LargestTransfer {
			asset.LargestTransfer = amount
		}
		asset.AverageAmount = asset.TotalVolume / float64(asset.TransferCount)
	}
	
	// Track unique holders for this asset using HLL (reuse bytes from chain tracking)
	asset.HoldersHLL.Insert(senderBytes)
	asset.HoldersHLL.Insert(receiverBytes)
	

	
	// Track asset route
	routeKey = fmt.Sprintf("%s→%s", transfer.SourceChain.UniversalChainID, transfer.DestinationChain.UniversalChainID)
	var assetRoute *AssetRouteStats
	assetRoute, exists = asset.Routes[routeKey]
	if !exists {
		assetRoute = &AssetRouteStats{
			FromChain:    transfer.SourceChain.UniversalChainID,
			ToChain:      transfer.DestinationChain.UniversalChainID,
			FromName:     transfer.SourceChain.DisplayName,
			ToName:       transfer.DestinationChain.DisplayName,
			Route:        fmt.Sprintf("%s → %s", transfer.SourceChain.DisplayName, transfer.DestinationChain.DisplayName),
			LastActivity: transfer.TransferSendTimestamp.Format(time.RFC3339),
		}
		asset.Routes[routeKey] = assetRoute
	}
	
	assetRoute.Count++
	if amount := parseAmount(transfer.BaseAmount); amount > 0 {
		assetRoute.Volume += amount
	}
	
	// Update last activity if this transfer is more recent
	if transfer.TransferSendTimestamp.Format(time.RFC3339) > assetRoute.LastActivity {
		assetRoute.LastActivity = transfer.TransferSendTimestamp.Format(time.RFC3339)
	}
	
	// Track chain-asset relationships
	amount := parseAmount(transfer.BaseAmount)
	
	// Track asset on source chain (outgoing)
	if bucket.ChainAssets[sourceChainID] == nil {
		bucket.ChainAssets[sourceChainID] = make(map[string]*ChainAssetStats)
	}
	sourceChainAsset, exists := bucket.ChainAssets[sourceChainID][symbol]
	if !exists {
		sourceChainAsset = &ChainAssetStats{
			AssetSymbol:  symbol,
			AssetName:    symbol,
			LastActivity: transfer.TransferSendTimestamp.Format(time.RFC3339),
			HoldersHLL:   getOrCreateOptimizedHLL("asset"),
		}
		bucket.ChainAssets[sourceChainID][symbol] = sourceChainAsset
	}
	sourceChainAsset.OutgoingCount++
	sourceChainAsset.TotalVolume += amount
	if amount > 0 {
		sourceChainAsset.AverageAmount = sourceChainAsset.TotalVolume / float64(sourceChainAsset.OutgoingCount + sourceChainAsset.IncomingCount)
	}
	sourceChainAsset.NetFlow = sourceChainAsset.IncomingCount - sourceChainAsset.OutgoingCount
	if transfer.TransferSendTimestamp.Format(time.RFC3339) > sourceChainAsset.LastActivity {
		sourceChainAsset.LastActivity = transfer.TransferSendTimestamp.Format(time.RFC3339)
	}
	sourceChainAsset.HoldersHLL.Insert(senderBytes)
	sourceChainAsset.HoldersHLL.Insert(receiverBytes)
	
	// Track asset on destination chain (incoming)
	if bucket.ChainAssets[destChainID] == nil {
		bucket.ChainAssets[destChainID] = make(map[string]*ChainAssetStats)
	}
	destChainAsset, exists := bucket.ChainAssets[destChainID][symbol]
	if !exists {
		destChainAsset = &ChainAssetStats{
			AssetSymbol:  symbol,
			AssetName:    symbol,
			LastActivity: transfer.TransferSendTimestamp.Format(time.RFC3339),
			HoldersHLL:   getOrCreateOptimizedHLL("asset"),
		}
		bucket.ChainAssets[destChainID][symbol] = destChainAsset
	}
	destChainAsset.IncomingCount++
	destChainAsset.TotalVolume += amount
	if amount > 0 {
		destChainAsset.AverageAmount = destChainAsset.TotalVolume / float64(destChainAsset.OutgoingCount + destChainAsset.IncomingCount)
	}
	destChainAsset.NetFlow = destChainAsset.IncomingCount - destChainAsset.OutgoingCount
	if transfer.TransferSendTimestamp.Format(time.RFC3339) > destChainAsset.LastActivity {
		destChainAsset.LastActivity = transfer.TransferSendTimestamp.Format(time.RFC3339)
	}
	destChainAsset.HoldersHLL.Insert(senderBytes)
	destChainAsset.HoldersHLL.Insert(receiverBytes)

	// Ensure bucket HLL sketches are always initialized
	if bucket.SendersHLL == nil {
		bucket.SendersHLL = getOrCreateOptimizedHLL("bucket")
	}
	if bucket.ReceiversHLL == nil {
		bucket.ReceiversHLL = getOrCreateOptimizedHLL("bucket")
	}
	if bucket.WalletsHLL == nil {
		bucket.WalletsHLL = getOrCreateOptimizedHLL("bucket")
	}

	// Update bucket HLL (with validated byte slices)
	bucket.SendersHLL.Insert(senderBytes)
	bucket.ReceiversHLL.Insert(receiverBytes)
	bucket.WalletsHLL.Insert(senderBytes)
	bucket.WalletsHLL.Insert(receiverBytes)
}

// GetChartData returns aggregated chart data for the last period
func (c *Collector) GetChartData() ChartData {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	now := time.Now()
	
	// Calculate transfer rates
	transferRates := c.calculateTransferRates(now)
	
	// Calculate unique wallets
	uniqueWallets := struct {
		Senders   int64 `json:"senders"`
		Receivers int64 `json:"receivers"`
		Total     int64 `json:"total"`
	}{
		Senders:   int64(c.globalSendersHLL.Estimate()),
		Receivers: int64(c.globalReceiversHLL.Estimate()),
		Total:     int64(c.globalWalletsHLL.Estimate()),
	}
	
	// Get top routes, chains, and assets (using same timeframe as transfer rates)
	topRoutes := c.getTopRoutes(now.Add(-time.Minute), c.config.TopItemsLimit)
	chainFlows := c.getChainFlows(now.Add(-time.Minute))
	topAssets := c.getTopAssets(now.Add(-time.Minute), c.config.TopItemsLimit) // Use 1-minute rolling window
	
	return ChartData{
		TransferRates: transferRates,
		UniqueWallets: uniqueWallets,
		TopRoutes:     topRoutes,
		ChainFlows:    chainFlows,
		TopAssets:     topAssets,
		Timestamp:     now,
		Uptime:        time.Since(c.startTime).Seconds(),
	}
}

// GetChartDataForFrontend returns chart data formatted for frontend compatibility
func (c *Collector) GetChartDataForFrontend() interface{} {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	now := time.Now()
	
	// Calculate transfer rates
	transferRates := c.calculateTransferRates(now)
	

	
	// Transform to frontend-expected format with percentage changes  
	transferRatesForFrontend := map[string]interface{}{
		"txPerMinute":         float64(transferRates.LastMinute.Transfers),
		"txPerHour":           float64(transferRates.LastHour.Transfers),
		"txPerDay":            float64(transferRates.LastDay.Transfers),
		"txPer7Days":          float64(transferRates.Last7Days.Transfers),
		"txPer14Days":         float64(transferRates.Last14Days.Transfers),
		"txPer30Days":         float64(transferRates.Last30Days.Transfers),
		"txPerMinuteChange":   transferRates.LastMinute.TransfersChange,
		"txPerHourChange":     transferRates.LastHour.TransfersChange,
		"txPerDayChange":      transferRates.LastDay.TransfersChange,
		"txPer7DaysChange":    transferRates.Last7Days.TransfersChange,
		"txPer14DaysChange":   transferRates.Last14Days.TransfersChange,
		"txPer30DaysChange":   transferRates.Last30Days.TransfersChange,
		"totalTracked":        transferRates.Total.Transfers,
		"dataAvailability":    transferRates.DataAvailability,
		"serverUptimeSeconds": transferRates.Uptime, // Frontend needs this for uptime display
	}
	
	// Active wallet rates matching frontend expectations with percentage changes
	activeWalletRates := map[string]interface{}{
		"sendersLastMin":          transferRates.LastMinute.Senders,
		"sendersLastHour":         transferRates.LastHour.Senders,
		"sendersLastDay":          transferRates.LastDay.Senders,
		"sendersLast7d":           transferRates.Last7Days.Senders,
		"sendersLast14d":          transferRates.Last14Days.Senders,
		"sendersLast30d":          transferRates.Last30Days.Senders,
		"sendersLastMinChange":    transferRates.LastMinute.SendersChange,
		"sendersLastHourChange":   transferRates.LastHour.SendersChange,
		"sendersLastDayChange":    transferRates.LastDay.SendersChange,
		"sendersLast7dChange":     transferRates.Last7Days.SendersChange,
		"sendersLast14dChange":    transferRates.Last14Days.SendersChange,
		"sendersLast30dChange":    transferRates.Last30Days.SendersChange,
		"receiversLastMin":        transferRates.LastMinute.Receivers,
		"receiversLastHour":       transferRates.LastHour.Receivers,
		"receiversLastDay":        transferRates.LastDay.Receivers,
		"receiversLast7d":         transferRates.Last7Days.Receivers,
		"receiversLast14d":        transferRates.Last14Days.Receivers,
		"receiversLast30d":        transferRates.Last30Days.Receivers,
		"receiversLastMinChange":  transferRates.LastMinute.ReceiversChange,
		"receiversLastHourChange": transferRates.LastHour.ReceiversChange,
		"receiversLastDayChange":  transferRates.LastDay.ReceiversChange,
		"receiversLast7dChange":   transferRates.Last7Days.ReceiversChange,
		"receiversLast14dChange":  transferRates.Last14Days.ReceiversChange,
		"receiversLast30dChange":  transferRates.Last30Days.ReceiversChange,
		"totalLastMin":            transferRates.LastMinute.Total,
		"totalLastHour":           transferRates.LastHour.Total,
		"totalLastDay":            transferRates.LastDay.Total,
		"totalLast7d":             transferRates.Last7Days.Total,
		"totalLast14d":            transferRates.Last14Days.Total,
		"totalLast30d":            transferRates.Last30Days.Total,
		"totalLastMinChange":      transferRates.LastMinute.TotalChange,
		"totalLastHourChange":     transferRates.LastHour.TotalChange,
		"totalLastDayChange":      transferRates.LastDay.TotalChange,
		"totalLast7dChange":       transferRates.Last7Days.TotalChange,
		"totalLast14dChange":      transferRates.Last14Days.TotalChange,
		"totalLast30dChange":      transferRates.Last30Days.TotalChange,
		"uniqueSendersTotal":      int64(c.globalSendersHLL.Estimate()),
		"uniqueReceiversTotal":    int64(c.globalReceiversHLL.Estimate()),
		"uniqueTotalWallets":      int64(c.globalWalletsHLL.Estimate()),
		"dataAvailability":        transferRates.DataAvailability,
		"serverUptimeSeconds":     transferRates.Uptime,
	}
	
	// Get top routes and transform to expected format (using 1 minute timeframe)
	topRoutes := c.getTopRoutes(now.Add(-time.Minute), c.config.TopItemsLimit)
	
	// Build response matching frontend expectations
	frontendData := map[string]interface{}{
		"currentRates":           transferRatesForFrontend,  // REVERT: Keep original key name for backward compatibility
		"activeWalletRates":      activeWalletRates,
		"popularRoutes":          topRoutes,
		"popularRoutesTimeScale": c.getPopularRoutesTimeScale(), // Implement time scale data
		"activeSenders":          c.getTopSenders(now.Add(-time.Minute), c.config.TopItemsLimit),
		"activeReceivers":        c.getTopReceivers(now.Add(-time.Minute), c.config.TopItemsLimit),
		"activeSendersTimeScale": c.getActiveSendersTimeScale(), // Implement time scale data
		"activeReceiversTimeScale": c.getActiveReceiversTimeScale(), // Implement time scale data
		"chainFlowData": map[string]interface{}{
			"chains":             c.getChainFlows(now.Add(-time.Minute)),
			"chainFlowTimeScale": c.getChainFlowTimeScale(), // Implement time scale data
			"totalOutgoing":      c.calculateTotalOutgoing(now.Add(-time.Minute)),
			"totalIncoming":      c.calculateTotalIncoming(now.Add(-time.Minute)),
			"serverUptimeSeconds": transferRates.Uptime,
		},
		"assetVolumeData": c.getAssetVolumeDataForFrontend(now.Add(-time.Minute), transferRates.Uptime), // Use 1-minute rolling window like other components
		"dataAvailability": transferRates.DataAvailability, // Top-level data availability for wallet stats
		"latencyData": c.getLatencyData(), // Add latency data
	}
	
	return frontendData
}

// UpdateLatencyData updates the stored latency data
func (c *Collector) UpdateLatencyData(latencyData []models.LatencyData) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.latencyData = latencyData
}

// getLatencyData returns a copy of the current latency data
func (c *Collector) getLatencyData() []models.LatencyData {
	c.mu.RLock()
	defer c.mu.RUnlock()
	
	// Return a copy to prevent race conditions
	result := make([]models.LatencyData, len(c.latencyData))
	copy(result, c.latencyData)
	return result
}

// Helper function to parse amount from string
func parseAmount(amountStr string) float64 {
	var amount float64
	n, err := fmt.Sscanf(amountStr, "%f", &amount)
	if err != nil || n != 1 {
		return 0
	}
	return amount
}

// PeriodStats contains transfer and unique wallet counts for a time period
type PeriodStats struct {
	Transfers        int64   `json:"transfers"`
	Senders          int64   `json:"senders"`
	Receivers        int64   `json:"receivers"`
	Total            int64   `json:"total"`
	TransfersChange  float64 `json:"transfersChange"`
	SendersChange    float64 `json:"sendersChange"`
	ReceiversChange  float64 `json:"receiversChange"`
	TotalChange      float64 `json:"totalChange"`
}

// DataAvailability represents data availability for different time periods
type DataAvailability struct {
	HasMinute bool `json:"hasMinute"`
	HasHour   bool `json:"hasHour"`
	HasDay    bool `json:"hasDay"`
	Has7Days  bool `json:"has7Days"`
	Has14Days bool `json:"has14Days"`
	Has30Days bool `json:"has30Days"`
}

// TransferRates contains transfer rates and unique counts for all time periods
type TransferRates struct {
	LastMinute       PeriodStats      `json:"lastMinute"`
	LastHour         PeriodStats      `json:"lastHour"`
	LastDay          PeriodStats      `json:"lastDay"`
	Last7Days        PeriodStats      `json:"last7Days"`
	Last14Days       PeriodStats      `json:"last14Days"`
	Last30Days       PeriodStats      `json:"last30Days"`
	Total            PeriodStats      `json:"total"`
	DataAvailability DataAvailability `json:"dataAvailability"`
	Uptime           float64          `json:"uptime"`
}

// calculateTransferRates calculates transfer rates and unique counts for all time periods
func (c *Collector) calculateTransferRates(now time.Time) TransferRates {
	uptime := now.Sub(c.startTime).Seconds()
	
	// Calculate current stats for each period (without changes first)
	lastMinuteCurrent := c.calculatePeriodStats(now.Add(-time.Minute))
	lastHourCurrent := c.calculatePeriodStats(now.Add(-time.Hour))
	lastDayCurrent := c.calculatePeriodStats(now.Add(-24*time.Hour))
	last7DaysCurrent := c.calculatePeriodStats(now.AddDate(0, 0, -7))
	last14DaysCurrent := c.calculatePeriodStats(now.AddDate(0, 0, -14))
	last30DaysCurrent := c.calculatePeriodStats(now.AddDate(0, 0, -30))
	total := c.calculatePeriodStats(time.Time{}) // All time
	
	// Update snapshots before calculating changes
	c.updateSnapshots(now, &lastMinuteCurrent, &lastHourCurrent, &lastDayCurrent, &last7DaysCurrent, &last14DaysCurrent, &last30DaysCurrent)
	
	// Now calculate with changes using updated snapshots
	lastMinute := c.addPercentageChanges(lastMinuteCurrent, c.previousMinute)
	lastHour := c.addPercentageChanges(lastHourCurrent, c.previousHour)
	lastDay := c.addPercentageChanges(lastDayCurrent, c.previousDay)
	last7Days := c.addPercentageChanges(last7DaysCurrent, c.previous7Days)
	last14Days := c.addPercentageChanges(last14DaysCurrent, c.previous14Days)
	last30Days := c.addPercentageChanges(last30DaysCurrent, c.previous30Days)
	
	return TransferRates{
		LastMinute:       lastMinute,
		LastHour:         lastHour,
		LastDay:          lastDay,
		Last7Days:        last7Days,
		Last14Days:       last14Days,
		Last30Days:       last30Days,
		Total:            total, // No changes for total
		DataAvailability: c.calculateDataAvailability(now),
		Uptime:           uptime,
	}
}

// calculateDataAvailability determines what time periods have sufficient data
// calculateDataAvailability determines what time ranges have sufficient data available
// Conservative approach: only show data for timeframes where we have adequate system uptime AND data
func (c *Collector) calculateDataAvailability(now time.Time) DataAvailability {
	uptime := now.Sub(c.startTime)
	
	// Helper function to check if we have adequate data coverage for a timeframe
	hasSufficientData := func(duration time.Duration, since time.Time) bool {
		// Must have at least the minimum uptime for this timeframe
		if uptime < duration {
			return false
		}
		
		// Count buckets with data in the timeframe to ensure sufficient coverage
		bucketCount := 0
		
		// Check appropriate bucket tier based on timeframe
		if duration <= time.Hour {
			// For short timeframes, check 10s and 1m buckets
			for timestamp := range c.buckets10s {
				if timestamp.After(since) {
					bucketCount++
				}
			}
			for timestamp := range c.buckets1m {
				if timestamp.After(since) {
					bucketCount++
				}
			}
			// Need at least a few buckets for reliable data
			return bucketCount >= 3
		} else if duration <= 24*time.Hour {
			// For medium timeframes, check 1m and 1h buckets  
			for timestamp := range c.buckets1m {
				if timestamp.After(since) {
					bucketCount++
				}
			}
			for timestamp := range c.buckets1h {
				if timestamp.After(since) {
					bucketCount++
				}
			}
			// Need at least several buckets for daily data
			return bucketCount >= 6
		} else {
			// For long timeframes, check 1h, daily, weekly buckets
			for timestamp := range c.buckets1h {
				if timestamp.After(since) {
					bucketCount++
				}
			}
			for timestamp := range c.bucketsDaily {
				if timestamp.After(since) {
					bucketCount++
				}
			}
			for timestamp := range c.bucketsWeekly {
				if timestamp.After(since) {
					bucketCount++
				}
			}
			// Need meaningful bucket coverage for long-term data
			if duration <= 7*24*time.Hour {
				return bucketCount >= 12 // At least half a day of hourly data
			} else {
				return bucketCount >= 3 // At least a few daily buckets
			}
		}
	}
	
	return DataAvailability{
		HasMinute: hasSufficientData(time.Minute, now.Add(-time.Minute)),
		HasHour:   hasSufficientData(time.Hour, now.Add(-time.Hour)),
		HasDay:    hasSufficientData(24*time.Hour, now.Add(-24*time.Hour)),
		Has7Days:  hasSufficientData(7*24*time.Hour, now.Add(-7*24*time.Hour)),
		Has14Days: hasSufficientData(14*24*time.Hour, now.Add(-14*24*time.Hour)),
		Has30Days: hasSufficientData(30*24*time.Hour, now.Add(-30*24*time.Hour)),
	}
}

// calculatePeriodStats calculates stats for transfers since the given time
func (c *Collector) calculatePeriodStats(since time.Time) PeriodStats {
	now := time.Now()
	var transferCount int64
	var bucketsProcessed int
	var bucketsWithHLL int
	
	// Create temporary HLL sketches for unique counting
	sendersHLL := hyperloglog.New16() // Use higher precision for aggregation
	receiversHLL := hyperloglog.New16()
	totalHLL := hyperloglog.New16()
	
	// Also track exact unique counts for small datasets (fallback)
	uniqueSenders := make(map[string]bool)
	uniqueReceivers := make(map[string]bool)
	uniqueTotal := make(map[string]bool)
	
	// Aggregate from 10-second buckets (most recent data)
	for timestamp, bucket := range c.buckets10s {
		if since.IsZero() || timestamp.After(since) {
			transferCount += bucket.TransferCount
			bucketsProcessed++
			
			// Track exact counts for validation (small dataset handling)
			for sender := range bucket.Senders {
				uniqueSenders[sender] = true
				uniqueTotal[sender] = true
			}
			for receiver := range bucket.Receivers {
				uniqueReceivers[receiver] = true
				uniqueTotal[receiver] = true
			}
			
			// Merge HLL sketches for unique counting
			if bucket.SendersHLL != nil {
				sendersHLL.Merge(bucket.SendersHLL)
				bucketsWithHLL++
			}
			if bucket.ReceiversHLL != nil {
				receiversHLL.Merge(bucket.ReceiversHLL)
			}
			if bucket.WalletsHLL != nil {
				totalHLL.Merge(bucket.WalletsHLL)
			}
		}
	}
	
	// Include 1-minute buckets for older data (beyond 1 hour)
	for timestamp, bucket := range c.buckets1m {
		if (since.IsZero() || timestamp.After(since)) && timestamp.Before(now.Add(-time.Hour)) {
			transferCount += bucket.TransferCount
			bucketsProcessed++
			
			// Track exact counts for validation
			for sender := range bucket.Senders {
				uniqueSenders[sender] = true
				uniqueTotal[sender] = true
			}
			for receiver := range bucket.Receivers {
				uniqueReceivers[receiver] = true
				uniqueTotal[receiver] = true
			}
			
			if bucket.SendersHLL != nil {
				sendersHLL.Merge(bucket.SendersHLL)
				bucketsWithHLL++
			}
			if bucket.ReceiversHLL != nil {
				receiversHLL.Merge(bucket.ReceiversHLL)
			}
			if bucket.WalletsHLL != nil {
				totalHLL.Merge(bucket.WalletsHLL)
			}
		}
	}
	
	// Include 1-hour buckets for oldest data (beyond 1 day)
	for timestamp, bucket := range c.buckets1h {
		if (since.IsZero() || timestamp.After(since)) && timestamp.Before(now.Add(-24*time.Hour)) {
			transferCount += bucket.TransferCount
			bucketsProcessed++
			
			// Track exact counts for validation
			for sender := range bucket.Senders {
				uniqueSenders[sender] = true
				uniqueTotal[sender] = true
			}
			for receiver := range bucket.Receivers {
				uniqueReceivers[receiver] = true
				uniqueTotal[receiver] = true
			}
			
			if bucket.SendersHLL != nil {
				sendersHLL.Merge(bucket.SendersHLL)
				bucketsWithHLL++
			}
			if bucket.ReceiversHLL != nil {
				receiversHLL.Merge(bucket.ReceiversHLL)
			}
			if bucket.WalletsHLL != nil {
				totalHLL.Merge(bucket.WalletsHLL)
			}
		}
	}
	
	// Calculate estimates
	sendersEstimate := int64(sendersHLL.Estimate())
	receiversEstimate := int64(receiversHLL.Estimate())
	totalEstimate := int64(totalHLL.Estimate())
	
	// For small datasets, use exact counts if HLL estimates are 0 or too low
	exactSenders := int64(len(uniqueSenders))
	exactReceivers := int64(len(uniqueReceivers))
	exactTotal := int64(len(uniqueTotal))
	
	// Use exact counts if they're small or HLL is giving 0
	if exactSenders <= 100 || sendersEstimate == 0 {
		sendersEstimate = exactSenders
	}
	if exactReceivers <= 100 || receiversEstimate == 0 {
		receiversEstimate = exactReceivers
	}
	if exactTotal <= 100 || totalEstimate == 0 {
		totalEstimate = exactTotal
	}
	

	
	return PeriodStats{
		Transfers: transferCount,
		Senders:   sendersEstimate,
		Receivers: receiversEstimate,
		Total:     totalEstimate,
	}
}

// calculatePeriodStatsWithChanges calculates stats with percentage changes vs previous period
func (c *Collector) calculatePeriodStatsWithChanges(since time.Time, previous *PeriodStats) PeriodStats {
	current := c.calculatePeriodStats(since)
	
	if previous == nil {
		// No previous data, no changes
		return current
	}
	
	// Calculate percentage changes
	current.TransfersChange = c.calculatePercentageChange(float64(current.Transfers), float64(previous.Transfers))
	current.SendersChange = c.calculatePercentageChange(float64(current.Senders), float64(previous.Senders))
	current.ReceiversChange = c.calculatePercentageChange(float64(current.Receivers), float64(previous.Receivers))
	current.TotalChange = c.calculatePercentageChange(float64(current.Total), float64(previous.Total))
	
	return current
}

// calculatePercentageChange calculates percentage change between current and previous values
func (c *Collector) calculatePercentageChange(current, previous float64) float64 {
	if previous == 0 {
		return 0 // No previous data means no change to report
	}
	
	return ((current - previous) / previous) * 100
}

// addPercentageChanges adds percentage changes to current stats using previous stats
func (c *Collector) addPercentageChanges(current PeriodStats, previous *PeriodStats) PeriodStats {
	if previous == nil {
		// No previous data, return current without changes
		return current
	}
	
	// Add percentage changes
	current.TransfersChange = c.calculatePercentageChange(float64(current.Transfers), float64(previous.Transfers))
	current.SendersChange = c.calculatePercentageChange(float64(current.Senders), float64(previous.Senders))
	current.ReceiversChange = c.calculatePercentageChange(float64(current.Receivers), float64(previous.Receivers))
	current.TotalChange = c.calculatePercentageChange(float64(current.Total), float64(previous.Total))
	
	return current
}

// updateSnapshots updates previous period snapshots for percentage calculations
func (c *Collector) updateSnapshots(now time.Time, minute, hour, day, week7, week14, month *PeriodStats) {
	// Update minute snapshot every minute - store current data for next comparison
	if now.Sub(c.lastMinuteSnapshot) >= time.Minute {
		c.previousMinute = &PeriodStats{
			Transfers: minute.Transfers,
			Senders:   minute.Senders,
			Receivers: minute.Receivers,
			Total:     minute.Total,
		}
		c.lastMinuteSnapshot = now
	}
	
	// Update hour snapshot every hour
	if now.Sub(c.lastHourSnapshot) >= time.Hour {
		c.previousHour = &PeriodStats{
			Transfers: hour.Transfers,
			Senders:   hour.Senders,
			Receivers: hour.Receivers,
			Total:     hour.Total,
		}
		c.lastHourSnapshot = now
	}
	
	// Update day snapshot every day
	if now.Sub(c.lastDaySnapshot) >= 24*time.Hour {
		c.previousDay = &PeriodStats{
			Transfers: day.Transfers,
			Senders:   day.Senders,
			Receivers: day.Receivers,
			Total:     day.Total,
		}
		c.lastDaySnapshot = now
	}
	
	// Update 7-day snapshot every 7 days
	if now.Sub(c.last7DaysSnapshot) >= 7*24*time.Hour {
		c.previous7Days = &PeriodStats{
			Transfers: week7.Transfers,
			Senders:   week7.Senders,
			Receivers: week7.Receivers,
			Total:     week7.Total,
		}
		c.last7DaysSnapshot = now
	}
	
	// Update 14-day snapshot every 14 days
	if now.Sub(c.last14DaysSnapshot) >= 14*24*time.Hour {
		c.previous14Days = &PeriodStats{
			Transfers: week14.Transfers,
			Senders:   week14.Senders,
			Receivers: week14.Receivers,
			Total:     week14.Total,
		}
		c.last14DaysSnapshot = now
	}
	
	// Update 30-day snapshot every 30 days
	if now.Sub(c.last30DaysSnapshot) >= 30*24*time.Hour {
		c.previous30Days = &PeriodStats{
			Transfers: month.Transfers,
			Senders:   month.Senders,
			Receivers: month.Receivers,
			Total:     month.Total,
		}
		c.last30DaysSnapshot = now
	}
}

// getTopRoutes returns the top routes by transfer count since the given time
func (c *Collector) getTopRoutes(since time.Time, limit int) []RouteStats {
	now := time.Now()
	routeAggregates := make(map[string]*RouteStats)
	
	// Aggregate routes from 10-second buckets (most recent data)
	for timestamp, bucket := range c.buckets10s {
		if since.IsZero() || timestamp.After(since) {
			for routeKey, route := range bucket.Routes {
				if existing, exists := routeAggregates[routeKey]; exists {
					existing.Count += route.Count
				} else {
					routeAggregates[routeKey] = &RouteStats{
						Count:     route.Count,
						FromChain: route.FromChain,
						ToChain:   route.ToChain,
						FromName:  route.FromName,
						ToName:    route.ToName,
						Route:     route.Route,
					}
				}
			}
		}
	}
	
	// Include 1-minute buckets for older data (beyond 1 hour)
	for timestamp, bucket := range c.buckets1m {
		if (since.IsZero() || timestamp.After(since)) && timestamp.Before(now.Add(-time.Hour)) {
			for routeKey, route := range bucket.Routes {
				if existing, exists := routeAggregates[routeKey]; exists {
					existing.Count += route.Count
				} else {
					routeAggregates[routeKey] = &RouteStats{
						Count:     route.Count,
						FromChain: route.FromChain,
						ToChain:   route.ToChain,
						FromName:  route.FromName,
						ToName:    route.ToName,
						Route:     route.Route,
					}
				}
			}
		}
	}
	
	// Include 1-hour buckets for oldest data (beyond 1 day)
	for timestamp, bucket := range c.buckets1h {
		if (since.IsZero() || timestamp.After(since)) && timestamp.Before(now.Add(-24*time.Hour)) {
			for routeKey, route := range bucket.Routes {
				if existing, exists := routeAggregates[routeKey]; exists {
					existing.Count += route.Count
				} else {
					routeAggregates[routeKey] = &RouteStats{
						Count:     route.Count,
						FromChain: route.FromChain,
						ToChain:   route.ToChain,
						FromName:  route.FromName,
						ToName:    route.ToName,
						Route:     route.Route,
					}
				}
			}
		}
	}
	
	// Convert to slice and sort
	routes := make([]RouteStats, 0, len(routeAggregates))
	for _, route := range routeAggregates {
		routes = append(routes, *route)
	}
	
	sort.Slice(routes, func(i, j int) bool {
		return routes[i].Count > routes[j].Count
	})
	
	if len(routes) > limit {
		routes = routes[:limit]
	}
	
	return routes
}

// getChainFlows returns chain flow statistics since the given time
func (c *Collector) getChainFlows(since time.Time) []ChainFlowStats {
	buckets := c.aggregateBucketsForPeriod(since)
	chainOutgoing := make(map[string]int64)
	chainIncoming := make(map[string]int64)
	chainNames := make(map[string]string)
	chainSendersHLL := make(map[string]*hyperloglog.Sketch)
	chainReceiversHLL := make(map[string]*hyperloglog.Sketch)
	chainUsersHLL := make(map[string]*hyperloglog.Sketch)
	
	// Aggregate data from all relevant buckets
	for _, bucket := range buckets {
		// Aggregate transfer counts
		for chainID, count := range bucket.ChainOutgoing {
			chainOutgoing[chainID] += count
		}
		for chainID, count := range bucket.ChainIncoming {
			chainIncoming[chainID] += count
		}
		
		// Merge HLL sketches with optimized precision
		for chainID, hll := range bucket.ChainSendersHLL {
			if chainSendersHLL[chainID] == nil {
				chainSendersHLL[chainID] = getOrCreateOptimizedHLL("chain")
			}
			chainSendersHLL[chainID].Merge(hll)
		}
		for chainID, hll := range bucket.ChainReceiversHLL {
			if chainReceiversHLL[chainID] == nil {
				chainReceiversHLL[chainID] = getOrCreateOptimizedHLL("chain")
			}
			chainReceiversHLL[chainID].Merge(hll)
		}
		for chainID, hll := range bucket.ChainUsersHLL {
			if chainUsersHLL[chainID] == nil {
				chainUsersHLL[chainID] = getOrCreateOptimizedHLL("chain")
			}
			chainUsersHLL[chainID].Merge(hll)
		}
		
		// Get chain names from routes
		for _, route := range bucket.Routes {
			chainNames[route.FromChain] = route.FromName
			chainNames[route.ToChain] = route.ToName
		}
	}
	
	// Calculate total for percentages
	var totalTransfers int64
	for _, count := range chainOutgoing {
		totalTransfers += count
	}
	for _, count := range chainIncoming {
		totalTransfers += count
	}
	
	// Build chain flow stats
	chainSet := make(map[string]bool)
	for chainID := range chainOutgoing {
		chainSet[chainID] = true
	}
	for chainID := range chainIncoming {
		chainSet[chainID] = true
	}
	for chainID := range chainSendersHLL {
		chainSet[chainID] = true
	}
	for chainID := range chainReceiversHLL {
		chainSet[chainID] = true
	}
	
	// Aggregate chain assets from all buckets
	chainAssetAggregates := make(map[string]map[string]*ChainAssetStats)
	for _, bucket := range buckets {
		for chainID, assets := range bucket.ChainAssets {
			if chainAssetAggregates[chainID] == nil {
				chainAssetAggregates[chainID] = make(map[string]*ChainAssetStats)
			}
			for symbol, asset := range assets {
				if existing, exists := chainAssetAggregates[chainID][symbol]; exists {
					// Merge asset stats
					existing.OutgoingCount += asset.OutgoingCount
					existing.IncomingCount += asset.IncomingCount
					existing.TotalVolume += asset.TotalVolume
					existing.NetFlow = existing.IncomingCount - existing.OutgoingCount
					if existing.OutgoingCount+existing.IncomingCount > 0 {
						existing.AverageAmount = existing.TotalVolume / float64(existing.OutgoingCount+existing.IncomingCount)
					}
					if asset.LastActivity > existing.LastActivity {
						existing.LastActivity = asset.LastActivity
					}
					// Merge HLL sketches
					if asset.HoldersHLL != nil {
						if existing.HoldersHLL == nil {
							existing.HoldersHLL = hyperloglog.New16()
						}
						existing.HoldersHLL.Merge(asset.HoldersHLL)
					}
				} else {
					// Create new aggregate
					newAsset := &ChainAssetStats{
						AssetSymbol:   asset.AssetSymbol,
						AssetName:     asset.AssetName,
						OutgoingCount: asset.OutgoingCount,
						IncomingCount: asset.IncomingCount,
						NetFlow:       asset.NetFlow,
						TotalVolume:   asset.TotalVolume,
						AverageAmount: asset.AverageAmount,
						LastActivity:  asset.LastActivity,
						HoldersHLL:    hyperloglog.New16(),
					}
					if asset.HoldersHLL != nil {
						newAsset.HoldersHLL.Merge(asset.HoldersHLL)
					}
					chainAssetAggregates[chainID][symbol] = newAsset
				}
			}
		}
	}

	flows := make([]ChainFlowStats, 0, len(chainSet))
	for chainID := range chainSet {
		outgoing := chainOutgoing[chainID]
		incoming := chainIncoming[chainID]
		netFlow := incoming - outgoing
		
		var percentage float64
		if totalTransfers > 0 {
			percentage = float64(outgoing+incoming) / float64(totalTransfers) * 100
		}
		
		chainName := chainNames[chainID]
		if chainName == "" {
			chainName = chainID
		}
		
		// Calculate unique user counts from HLL
		var uniqueSenders, uniqueReceivers, uniqueUsers int
		if hll := chainSendersHLL[chainID]; hll != nil {
			uniqueSenders = int(hll.Estimate())
		}
		if hll := chainReceiversHLL[chainID]; hll != nil {
			uniqueReceivers = int(hll.Estimate())
		}
		if hll := chainUsersHLL[chainID]; hll != nil {
			uniqueUsers = int(hll.Estimate())
		}
		
		// Get top assets for this chain
		var topAssets []ChainAssetStats
		if assets := chainAssetAggregates[chainID]; assets != nil {
			// Calculate total activity for percentage calculation
			var totalChainActivity int64
			for _, asset := range assets {
				totalChainActivity += asset.OutgoingCount + asset.IncomingCount
			}
			
			// Convert to slice and calculate percentages
			assetSlice := make([]ChainAssetStats, 0, len(assets))
			for _, asset := range assets {
				assetActivity := asset.OutgoingCount + asset.IncomingCount
				if totalChainActivity > 0 {
					asset.Percentage = float64(assetActivity) / float64(totalChainActivity) * 100
				}
				// Use symbol mapping if available
				if mappedSymbol, exists := c.denomToSymbol[asset.AssetSymbol]; exists && mappedSymbol != "" {
					asset.AssetSymbol = mappedSymbol
					asset.AssetName = mappedSymbol
				}
				assetSlice = append(assetSlice, *asset)
			}
			
			// Sort by total activity (outgoing + incoming) and take top 5
			sort.Slice(assetSlice, func(i, j int) bool {
				return (assetSlice[i].OutgoingCount + assetSlice[i].IncomingCount) > (assetSlice[j].OutgoingCount + assetSlice[j].IncomingCount)
			})
			
			limit := 5
			if len(assetSlice) > limit {
				topAssets = assetSlice[:limit]
			} else {
				topAssets = assetSlice
			}
		}
		
		flows = append(flows, ChainFlowStats{
			UniversalChainID: chainID,
			ChainName:        chainName,
			OutgoingCount:    outgoing,
			IncomingCount:    incoming,
			NetFlow:          netFlow,
			UniqueSenders:    uniqueSenders,
			UniqueReceivers:  uniqueReceivers,
			UniqueUsers:      uniqueUsers,
			LastActivity:     time.Now().Format(time.RFC3339),
			Percentage:       percentage,
			TopAssets:        topAssets,
		})
	}
	
	// Sort by total activity (outgoing + incoming)
	sort.Slice(flows, func(i, j int) bool {
		return flows[i].OutgoingCount+flows[i].IncomingCount > flows[j].OutgoingCount+flows[j].IncomingCount
	})
	
	return flows
}

// calculateTotalOutgoing calculates total outgoing transfers across all chains
func (c *Collector) calculateTotalOutgoing(since time.Time) int64 {
	buckets := c.aggregateBucketsForPeriod(since)
	var total int64
	
	for _, bucket := range buckets {
		for _, count := range bucket.ChainOutgoing {
			total += count
		}
	}
	
	return total
}

// calculateTotalIncoming calculates total incoming transfers across all chains
func (c *Collector) calculateTotalIncoming(since time.Time) int64 {
	buckets := c.aggregateBucketsForPeriod(since)
	var total int64
	
	for _, bucket := range buckets {
		for _, count := range bucket.ChainIncoming {
			total += count
		}
	}
	
	return total
}

// getTopSenders returns the top active senders by transfer count since the given time
func (c *Collector) getTopSenders(since time.Time, limit int) []WalletStats {
	buckets := c.aggregateBucketsForPeriod(since)
	senderAggregates := make(map[string]int64)
	senderLastActivity := make(map[string]time.Time)
	
	for _, bucket := range buckets {
		for address, count := range bucket.Senders {
			senderAggregates[address] += count
			if bucket.Timestamp.After(senderLastActivity[address]) {
				senderLastActivity[address] = bucket.Timestamp
			}
		}
	}
	
	// Convert to slice and sort by count
	senders := make([]WalletStats, 0, len(senderAggregates))
	for address, count := range senderAggregates {
		lastActivity := senderLastActivity[address]
		if lastActivity.IsZero() {
			lastActivity = time.Now()
		}
		
		// Format address using chain context
		displayAddress := address // fallback to canonical
		if chainInfo, exists := c.addressChainInfo[address]; exists {
			displayAddress = c.addressFormatter.FormatAddress(address, chainInfo.RpcType, chainInfo.AddrPrefix)
		}
		
		senders = append(senders, WalletStats{
			Address:        address,
			DisplayAddress: displayAddress,
			Count:          count,
			LastActivity:   lastActivity.Format(time.RFC3339),
		})
	}
	
	sort.Slice(senders, func(i, j int) bool {
		return senders[i].Count > senders[j].Count
	})
	
	if len(senders) > limit {
		senders = senders[:limit]
	}
	
	return senders
}

// getTopReceivers returns the top active receivers by transfer count since the given time
func (c *Collector) getTopReceivers(since time.Time, limit int) []WalletStats {
	buckets := c.aggregateBucketsForPeriod(since)
	receiverAggregates := make(map[string]int64)
	receiverLastActivity := make(map[string]time.Time)
	
	for _, bucket := range buckets {
		for address, count := range bucket.Receivers {
			receiverAggregates[address] += count
			if bucket.Timestamp.After(receiverLastActivity[address]) {
				receiverLastActivity[address] = bucket.Timestamp
			}
		}
	}
	
	// Convert to slice and sort by count
	receivers := make([]WalletStats, 0, len(receiverAggregates))
	for address, count := range receiverAggregates {
		lastActivity := receiverLastActivity[address]
		if lastActivity.IsZero() {
			lastActivity = time.Now()
		}
		
		// Format address using chain context
		displayAddress := address // fallback to canonical
		if chainInfo, exists := c.addressChainInfo[address]; exists {
			displayAddress = c.addressFormatter.FormatAddress(address, chainInfo.RpcType, chainInfo.AddrPrefix)
		}
		
		receivers = append(receivers, WalletStats{
			Address:        address,
			DisplayAddress: displayAddress,
			Count:          count,
			LastActivity:   lastActivity.Format(time.RFC3339),
		})
	}
	
	sort.Slice(receivers, func(i, j int) bool {
		return receivers[i].Count > receivers[j].Count
	})
	
	if len(receivers) > limit {
		receivers = receivers[:limit]
	}
	
	return receivers
}

// getTopAssets returns the top assets by transfer count since the given time
func (c *Collector) getTopAssets(since time.Time, limit int) []AssetStats {
	buckets := c.aggregateBucketsForPeriod(since)
	assetAggregates := make(map[string]*AssetStats)
	
	// Aggregate assets from all relevant buckets
	for _, bucket := range buckets {
		for symbol, asset := range bucket.Assets {
				if existing, exists := assetAggregates[symbol]; exists {
					existing.TransferCount += asset.TransferCount
					existing.TotalVolume += asset.TotalVolume
					if asset.LargestTransfer > existing.LargestTransfer {
						existing.LargestTransfer = asset.LargestTransfer
					}
					existing.AverageAmount = existing.TotalVolume / float64(existing.TransferCount)
					
					// Update last activity if this asset is more recent
					if asset.LastActivity > existing.LastActivity {
						existing.LastActivity = asset.LastActivity
					}
					
					// Merge HLL sketches for unique holder counting
					if asset.HoldersHLL != nil {
						if existing.HoldersHLL == nil {
							existing.HoldersHLL = hyperloglog.New16()
						}
						existing.HoldersHLL.Merge(asset.HoldersHLL)
					}
					
					// Merge routes
					for routeKey, route := range asset.Routes {
						if existingRoute, exists := existing.Routes[routeKey]; exists {
							existingRoute.Count += route.Count
							existingRoute.Volume += route.Volume
							if route.LastActivity > existingRoute.LastActivity {
								existingRoute.LastActivity = route.LastActivity
							}
						} else {
							existing.Routes[routeKey] = &AssetRouteStats{
								FromChain:    route.FromChain,
								ToChain:      route.ToChain,
								FromName:     route.FromName,
								ToName:       route.ToName,
								Route:        route.Route,
								Count:        route.Count,
								Volume:       route.Volume,
								LastActivity: route.LastActivity,
							}
						}
					}
				} else {
					// Create new aggregate entry
					newAsset := &AssetStats{
						AssetSymbol:     asset.AssetSymbol,
						AssetName:       asset.AssetName,
						TransferCount:   asset.TransferCount,
						TotalVolume:     asset.TotalVolume,
						LargestTransfer: asset.LargestTransfer,
						AverageAmount:   asset.AverageAmount,
						LastActivity:    asset.LastActivity,
						Routes:          make(map[string]*AssetRouteStats),
						HoldersHLL:      hyperloglog.New16(), // Initialize HLL for unique holder tracking
					}
					
					// Copy HLL data if available
					if asset.HoldersHLL != nil {
						newAsset.HoldersHLL.Merge(asset.HoldersHLL)
					}
					
					// Copy routes
					for routeKey, route := range asset.Routes {
						newAsset.Routes[routeKey] = &AssetRouteStats{
							FromChain:    route.FromChain,
							ToChain:      route.ToChain,
							FromName:     route.FromName,
							ToName:       route.ToName,
							Route:        route.Route,
							Count:        route.Count,
							Volume:       route.Volume,
							LastActivity: route.LastActivity,
						}
					}
					
					assetAggregates[symbol] = newAsset
				}
			}
		}
	
	// Convert to slice with proper asset naming and percentage changes
	assets := make([]AssetStats, 0, len(assetAggregates))
	var totalVolume float64
	var totalTransfers int64
	
	for denom, asset := range assetAggregates {
		// Set display symbol using symbol mapping
		assetSymbol := asset.AssetSymbol
		if mappedSymbol, exists := c.denomToSymbol[denom]; exists && mappedSymbol != "" {
			assetSymbol = mappedSymbol
		}
		
		// Calculate average amount
		averageAmount := float64(0)
		if asset.TransferCount > 0 {
			averageAmount = asset.TotalVolume / float64(asset.TransferCount)
		}
		
		// Calculate top routes for this asset
		routes := make([]AssetRouteStats, 0, len(asset.Routes))
		for _, route := range asset.Routes {
			// Calculate percentage of asset's total volume
			percentage := float64(0)
			if asset.TotalVolume > 0 {
				percentage = (route.Volume / asset.TotalVolume) * 100
			}
			route.Percentage = percentage
			routes = append(routes, *route)
		}
		
		// Sort routes by volume (descending) and take top 5
		sort.Slice(routes, func(i, j int) bool {
			return routes[i].Volume > routes[j].Volume
		})
		if len(routes) > 5 {
			routes = routes[:5]
		}

		// Calculate unique holders from HLL
		uniqueHolders := 0
		if asset.HoldersHLL != nil {
			uniqueHolders = int(asset.HoldersHLL.Estimate())
		}
		
		resultAsset := AssetStats{
			AssetSymbol:     assetSymbol,
			AssetName:       assetSymbol, // Use symbol as name for now
			TransferCount:   asset.TransferCount,
			TotalVolume:     asset.TotalVolume,
			LargestTransfer: asset.LargestTransfer,
			AverageAmount:   averageAmount,
			UniqueHolders:   uniqueHolders, // Include unique holder count
			LastActivity:    asset.LastActivity,
			TopRoutes:       routes,
		
		}
		assets = append(assets, resultAsset)
		totalVolume += asset.TotalVolume
		totalTransfers += asset.TransferCount
	}
	
	// Sort by transfer count (descending) for final output
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].TransferCount > assets[j].TransferCount
	})
	
	if len(assets) > limit {
		assets = assets[:limit]
	}
	
		return assets
}

// getAssetVolumeDataForFrontend returns asset volume data formatted for frontend
func (c *Collector) getAssetVolumeDataForFrontend(since time.Time, uptime float64) map[string]interface{} {
	assets := c.getTopAssets(since, c.config.TopItemsLimit)
	
	// Calculate totals from all assets
	var totalVolume float64
	var totalTransfers int64
	totalAssets := 0
	
	// Aggregate totals from all buckets, not just top assets
	buckets := c.aggregateBucketsForPeriod(since)
	uniqueAssets := make(map[string]bool)
	
	for _, bucket := range buckets {
		for _, asset := range bucket.Assets {
			totalVolume += asset.TotalVolume
			totalTransfers += asset.TransferCount
		}
		for symbol := range bucket.Assets {
			uniqueAssets[symbol] = true
		}
	}
	totalAssets = len(uniqueAssets)
	
	// Convert assets to simple map to avoid JSON serialization issues
	simpleAssets := make([]map[string]interface{}, len(assets))
	for i, asset := range assets {
		// Convert TopRoutes to simple maps
		topRoutes := make([]map[string]interface{}, len(asset.TopRoutes))
		for j, route := range asset.TopRoutes {
			topRoutes[j] = map[string]interface{}{
				"fromChain":    route.FromChain,
				"toChain":      route.ToChain,
				"fromName":     route.FromName,
				"toName":       route.ToName,
				"route":        route.Route,
				"count":        route.Count,
				"volume":       route.Volume,
				"percentage":   route.Percentage,
				"lastActivity": route.LastActivity,
			}
		}
		
		simpleAssets[i] = map[string]interface{}{
			"assetSymbol":     asset.AssetSymbol,
			"assetName":       asset.AssetName,
			"transferCount":   asset.TransferCount,
			"totalVolume":     asset.TotalVolume,
			"largestTransfer": asset.LargestTransfer,
			"averageAmount":   asset.AverageAmount,
			"uniqueHolders":   asset.UniqueHolders, // Include unique holder count
			"lastActivity":    asset.LastActivity,
			"topRoutes":       topRoutes,
		}
	}
	
	return map[string]interface{}{
		"assets":              simpleAssets,
		"assetVolumeTimeScale": c.getAssetVolumeTimeScale(),
		"totalAssets":         totalAssets,
		"totalVolume":         totalVolume,
		"totalTransfers":      totalTransfers,
		"serverUptimeSeconds": uptime,
	}
}

// cleanup removes old buckets and aggregates through 5-tier system: 10s->1m->1h->daily->weekly->delete
func (c *Collector) cleanup() {
	now := time.Now()
	
	// Phase 1: Aggregate 10-second buckets to 1-minute buckets (after 30 minutes)
	tenSecondCutoff := now.Add(-c.config.TenSecondRetention)
	c.aggregateBuckets(tenSecondCutoff, "10s", "1m")
	
	// Phase 2: Aggregate 1-minute buckets to 1-hour buckets (after 6 hours)
	oneMinuteCutoff := now.Add(-c.config.OneMinuteRetention)
	c.aggregateBuckets(oneMinuteCutoff, "1m", "1h")
	
	// Phase 3: Aggregate 1-hour buckets to daily buckets (after 7 days)
	oneHourCutoff := now.Add(-c.config.OneHourRetention)
	c.aggregateBuckets(oneHourCutoff, "1h", "daily")
	
	// Phase 4: Aggregate daily buckets to weekly buckets (after 30 days)
	dailyCutoff := now.Add(-c.config.DailyRetention)
	c.aggregateBuckets(dailyCutoff, "daily", "weekly")
	
	// Phase 5: Delete weekly buckets older than 1 year
	weeklyCutoff := now.Add(-c.config.WeeklyRetention)
	c.removeBucketsOlderThanWithCleanup(weeklyCutoff)
}

// aggregateBuckets aggregates buckets from one granularity to another
func (c *Collector) aggregateBuckets(cutoff time.Time, fromGranularity, toGranularity string) {
	var sourceBuckets map[time.Time]*StatsBucket
	var targetBuckets map[time.Time]*StatsBucket
	var aggregationInterval time.Duration
	
	switch fromGranularity {
	case "10s":
		sourceBuckets = c.buckets10s
	case "1m":
		sourceBuckets = c.buckets1m
	case "1h":
		sourceBuckets = c.buckets1h
	case "daily":
		sourceBuckets = c.bucketsDaily
	case "weekly":
		sourceBuckets = c.bucketsWeekly
	default:
		return
	}
	
	switch toGranularity {
	case "1m":
		targetBuckets = c.buckets1m
		aggregationInterval = time.Minute
	case "1h":
		targetBuckets = c.buckets1h
		aggregationInterval = time.Hour
	case "daily":
		targetBuckets = c.bucketsDaily
		aggregationInterval = 24 * time.Hour
	case "weekly":
		targetBuckets = c.bucketsWeekly
		aggregationInterval = 7 * 24 * time.Hour
	default:
		return
	}
	
	// Group old buckets by target time interval
	aggregationGroups := make(map[time.Time][]*StatsBucket)
	
	for timestamp, bucket := range sourceBuckets {
		if timestamp.Before(cutoff) {
			targetTime := timestamp.Truncate(aggregationInterval)
			aggregationGroups[targetTime] = append(aggregationGroups[targetTime], bucket)
		}
	}
	
	// Aggregate each group
	for targetTime, buckets := range aggregationGroups {
		aggregated := c.aggregateStatsBuckets(buckets, toGranularity)
		aggregated.Timestamp = targetTime
		targetBuckets[targetTime] = aggregated
		
		// Remove original buckets
		for _, bucket := range buckets {
			delete(sourceBuckets, bucket.Timestamp)
		}
	}
}

// aggregateStatsBuckets combines multiple stats buckets into one with optimized HLL usage
func (c *Collector) aggregateStatsBuckets(buckets []*StatsBucket, granularity string) *StatsBucket {
	result := &StatsBucket{
		Granularity:       granularity,
		Routes:            make(map[string]*RouteStats),
		ChainOutgoing:     make(map[string]int64),
		ChainIncoming:     make(map[string]int64),
		ChainSendersHLL:   make(map[string]*hyperloglog.Sketch),
		ChainReceiversHLL: make(map[string]*hyperloglog.Sketch),
		ChainUsersHLL:     make(map[string]*hyperloglog.Sketch),
		ChainAssets:       make(map[string]map[string]*ChainAssetStats),
		Senders:           make(map[string]int64),
		Receivers:         make(map[string]int64),
		Assets:            make(map[string]*AssetStats),
		SendersHLL:        getOrCreateOptimizedHLL("bucket"),
		ReceiversHLL:      getOrCreateOptimizedHLL("bucket"),
		WalletsHLL:        getOrCreateOptimizedHLL("bucket"),
	}
	
	for _, bucket := range buckets {
		if bucket == nil {
			continue
		}
		
		// Aggregate basic counts
		result.TransferCount += bucket.TransferCount
		
		// Aggregate routes
		for routeKey, route := range bucket.Routes {
			if existing, exists := result.Routes[routeKey]; exists {
				existing.Count += route.Count
			} else {
				result.Routes[routeKey] = &RouteStats{
					Count:     route.Count,
					FromChain: route.FromChain,
					ToChain:   route.ToChain,
					FromName:  route.FromName,
					ToName:    route.ToName,
					Route:     route.Route,
				}
			}
		}
		
		// Aggregate chain flows
		for chainID, count := range bucket.ChainOutgoing {
			result.ChainOutgoing[chainID] += count
		}
		for chainID, count := range bucket.ChainIncoming {
			result.ChainIncoming[chainID] += count
		}
		
		// Merge chain HLL sketches with optimized precision
		for chainID, hll := range bucket.ChainSendersHLL {
			if result.ChainSendersHLL[chainID] == nil {
				result.ChainSendersHLL[chainID] = getOrCreateOptimizedHLL("chain")
			}
			result.ChainSendersHLL[chainID].Merge(hll)
		}
		for chainID, hll := range bucket.ChainReceiversHLL {
			if result.ChainReceiversHLL[chainID] == nil {
				result.ChainReceiversHLL[chainID] = getOrCreateOptimizedHLL("chain")
			}
			result.ChainReceiversHLL[chainID].Merge(hll)
		}
		for chainID, hll := range bucket.ChainUsersHLL {
			if result.ChainUsersHLL[chainID] == nil {
				result.ChainUsersHLL[chainID] = getOrCreateOptimizedHLL("chain")
			}
			result.ChainUsersHLL[chainID].Merge(hll)
		}

		// Merge chain assets
		for chainID, assets := range bucket.ChainAssets {
			if result.ChainAssets[chainID] == nil {
				result.ChainAssets[chainID] = make(map[string]*ChainAssetStats)
			}
			for symbol, asset := range assets {
				if existing, exists := result.ChainAssets[chainID][symbol]; exists {
					// Merge asset stats
					existing.OutgoingCount += asset.OutgoingCount
					existing.IncomingCount += asset.IncomingCount
					existing.TotalVolume += asset.TotalVolume
					existing.NetFlow = existing.IncomingCount - existing.OutgoingCount
					if existing.OutgoingCount+existing.IncomingCount > 0 {
						existing.AverageAmount = existing.TotalVolume / float64(existing.OutgoingCount+existing.IncomingCount)
					}
					if asset.LastActivity > existing.LastActivity {
						existing.LastActivity = asset.LastActivity
					}
					// Merge HLL sketches with optimized precision
					if asset.HoldersHLL != nil {
						if existing.HoldersHLL == nil {
							existing.HoldersHLL = getOrCreateOptimizedHLL("asset")
						}
						existing.HoldersHLL.Merge(asset.HoldersHLL)
					}
				} else {
					// Create new aggregate with optimized HLL
					newAsset := &ChainAssetStats{
						AssetSymbol:   asset.AssetSymbol,
						AssetName:     asset.AssetName,
						OutgoingCount: asset.OutgoingCount,
						IncomingCount: asset.IncomingCount,
						NetFlow:       asset.NetFlow,
						TotalVolume:   asset.TotalVolume,
						AverageAmount: asset.AverageAmount,
						LastActivity:  asset.LastActivity,
						HoldersHLL:    getOrCreateOptimizedHLL("asset"),
					}
					if asset.HoldersHLL != nil {
						newAsset.HoldersHLL.Merge(asset.HoldersHLL)
					}
					result.ChainAssets[chainID][symbol] = newAsset
				}
			}
		}
		
		// Aggregate wallet activity
		for address, count := range bucket.Senders {
			result.Senders[address] += count
		}
		for address, count := range bucket.Receivers {
			result.Receivers[address] += count
		}
		
		// Aggregate assets
		for symbol, asset := range bucket.Assets {
			if existing, exists := result.Assets[symbol]; exists {
				existing.TransferCount += asset.TransferCount
				existing.TotalVolume += asset.TotalVolume
				if asset.LargestTransfer > existing.LargestTransfer {
					existing.LargestTransfer = asset.LargestTransfer
				}
				existing.AverageAmount = existing.TotalVolume / float64(existing.TransferCount)
				
				// Update last activity if more recent
				if asset.LastActivity > existing.LastActivity {
					existing.LastActivity = asset.LastActivity
				}
				
				// Merge HLL sketches for unique holder counting with optimized precision
				if asset.HoldersHLL != nil {
					if existing.HoldersHLL == nil {
						existing.HoldersHLL = getOrCreateOptimizedHLL("asset")
					}
					existing.HoldersHLL.Merge(asset.HoldersHLL)
				}
				
				// Merge routes
				if existing.Routes == nil {
					existing.Routes = make(map[string]*AssetRouteStats)
				}
				for routeKey, route := range asset.Routes {
					if existingRoute, exists := existing.Routes[routeKey]; exists {
						existingRoute.Count += route.Count
						existingRoute.Volume += route.Volume
						if route.LastActivity > existingRoute.LastActivity {
							existingRoute.LastActivity = route.LastActivity
						}
					} else {
						existing.Routes[routeKey] = &AssetRouteStats{
							FromChain:    route.FromChain,
							ToChain:      route.ToChain,
							FromName:     route.FromName,
							ToName:       route.ToName,
							Route:        route.Route,
							Count:        route.Count,
							Volume:       route.Volume,
							LastActivity: route.LastActivity,
						}
					}
				}

			} else {
				newAsset := &AssetStats{
					AssetSymbol:     asset.AssetSymbol,
					AssetName:       asset.AssetName,
					TransferCount:   asset.TransferCount,
					TotalVolume:     asset.TotalVolume,
					LargestTransfer: asset.LargestTransfer,
					AverageAmount:   asset.AverageAmount,
					LastActivity:    asset.LastActivity,
					Routes:          make(map[string]*AssetRouteStats), // Initialize Routes map
					HoldersHLL:      getOrCreateOptimizedHLL("asset"), // Initialize optimized HLL for aggregation
				}
				
				// Copy HLL data if available
				if asset.HoldersHLL != nil {
					newAsset.HoldersHLL.Merge(asset.HoldersHLL)
				}
				
				// Copy routes from original asset
				for routeKey, route := range asset.Routes {
					newAsset.Routes[routeKey] = &AssetRouteStats{
						FromChain:    route.FromChain,
						ToChain:      route.ToChain,
						FromName:     route.FromName,
						ToName:       route.ToName,
						Route:        route.Route,
						Count:        route.Count,
						Volume:       route.Volume,
						LastActivity: route.LastActivity,
					}
				}
				
				result.Assets[symbol] = newAsset
			}
		}
		
		// Merge HLL sketches
		if bucket.SendersHLL != nil {
			result.SendersHLL.Merge(bucket.SendersHLL)
		}
		if bucket.ReceiversHLL != nil {
			result.ReceiversHLL.Merge(bucket.ReceiversHLL)
		}
		if bucket.WalletsHLL != nil {
			result.WalletsHLL.Merge(bucket.WalletsHLL)
		}
	}
	
	return result
}

// removeBucketsOlderThan removes buckets older than the given cutoff time
func (c *Collector) removeBucketsOlderThan(cutoff time.Time) {
	// Clean 10-second buckets
	for timestamp := range c.buckets10s {
		if timestamp.Before(cutoff) {
			delete(c.buckets10s, timestamp)
		}
	}
	
	// Clean 1-minute buckets
	for timestamp := range c.buckets1m {
		if timestamp.Before(cutoff) {
			delete(c.buckets1m, timestamp)
		}
	}
	
	// Clean 1-hour buckets
	for timestamp := range c.buckets1h {
		if timestamp.Before(cutoff) {
			delete(c.buckets1h, timestamp)
		}
	}
	
	// Clean daily buckets
	for timestamp := range c.bucketsDaily {
		if timestamp.Before(cutoff) {
			delete(c.bucketsDaily, timestamp)
		}
	}
	
	// Clean weekly buckets
	for timestamp := range c.bucketsWeekly {
		if timestamp.Before(cutoff) {
			delete(c.bucketsWeekly, timestamp)
		}
	}
}

// removeBucketsOlderThanWithCleanup removes buckets older than cutoff time and returns them to memory pools
func (c *Collector) removeBucketsOlderThanWithCleanup(cutoff time.Time) {
	// Clean 10-second buckets with proper pool return
	for timestamp, bucket := range c.buckets10s {
		if timestamp.Before(cutoff) {
			returnBucketToPool(bucket)
			delete(c.buckets10s, timestamp)
		}
	}
	
	// Clean 1-minute buckets with proper pool return
	for timestamp, bucket := range c.buckets1m {
		if timestamp.Before(cutoff) {
			returnBucketToPool(bucket)
			delete(c.buckets1m, timestamp)
		}
	}
	
	// Clean 1-hour buckets with proper pool return
	for timestamp, bucket := range c.buckets1h {
		if timestamp.Before(cutoff) {
			returnBucketToPool(bucket)
			delete(c.buckets1h, timestamp)
		}
	}
	
	// Clean daily buckets with proper pool return
	for timestamp, bucket := range c.bucketsDaily {
		if timestamp.Before(cutoff) {
			returnBucketToPool(bucket)
			delete(c.bucketsDaily, timestamp)
		}
	}
	
	// Clean weekly buckets with proper pool return
	for timestamp, bucket := range c.bucketsWeekly {
		if timestamp.Before(cutoff) {
			returnBucketToPool(bucket)
			delete(c.bucketsWeekly, timestamp)
		}
	}
}

// aggregateBucketsForPeriod returns all buckets that should be included for a given time period
// This ensures consistent bucket selection across all aggregation methods using the 4-tier system
func (c *Collector) aggregateBucketsForPeriod(since time.Time) []*StatsBucket {
	now := time.Now()
	var buckets []*StatsBucket
	
	// Include 10-second buckets (most recent data: 0-30min)
	for timestamp, bucket := range c.buckets10s {
		if bucket != nil && (since.IsZero() || timestamp.After(since)) {
			buckets = append(buckets, bucket)
		}
	}
	
	// Include 1-minute buckets (recent data: 30min-6h)
	for timestamp, bucket := range c.buckets1m {
		if bucket != nil && (since.IsZero() || timestamp.After(since)) && timestamp.Before(now.Add(-c.config.TenSecondRetention)) {
			buckets = append(buckets, bucket)
		}
	}
	
	// Include 1-hour buckets (medium-term data: 6h-7d)
	for timestamp, bucket := range c.buckets1h {
		if bucket != nil && (since.IsZero() || timestamp.After(since)) && timestamp.Before(now.Add(-c.config.OneMinuteRetention)) {
			buckets = append(buckets, bucket)
		}
	}
	
	// Include daily buckets (medium-term data: 7d-30d)
	for timestamp, bucket := range c.bucketsDaily {
		if bucket != nil && (since.IsZero() || timestamp.After(since)) && timestamp.Before(now.Add(-c.config.OneHourRetention)) {
			buckets = append(buckets, bucket)
		}
	}
	
	// Include weekly buckets (final tier: 30d-1y, then delete)
	for timestamp, bucket := range c.bucketsWeekly {
		if bucket != nil && (since.IsZero() || timestamp.After(since)) && timestamp.Before(now.Add(-c.config.DailyRetention)) {
			buckets = append(buckets, bucket)
		}
	}
	
	return buckets
}

// getPopularRoutesTimeScale returns popular routes data for different time scales
func (c *Collector) getPopularRoutesTimeScale() map[string]interface{} {
	timeScales := map[string]time.Duration{
		"1m":  time.Minute,
		"1h":  time.Hour,
		"1d":  24 * time.Hour,
		"7d":  7 * 24 * time.Hour,
		"14d": 14 * 24 * time.Hour,
		"30d": 30 * 24 * time.Hour,
	}
	
	result := make(map[string]interface{})
	now := time.Now()
	
	for timeframe, duration := range timeScales {
		since := now.Add(-duration)
		routes := c.getTopRoutes(since, c.config.TopItemsTimeScale)
		result[timeframe] = routes
	}
	
	return result
}

// getActiveSendersTimeScale returns active senders data for different time scales
func (c *Collector) getActiveSendersTimeScale() map[string]interface{} {
	timeScales := map[string]time.Duration{
		"1m":  time.Minute,
		"1h":  time.Hour,
		"1d":  24 * time.Hour,
		"7d":  7 * 24 * time.Hour,
		"14d": 14 * 24 * time.Hour,
		"30d": 30 * 24 * time.Hour,
	}
	
	result := make(map[string]interface{})
	now := time.Now()
	
	for timeframe, duration := range timeScales {
		since := now.Add(-duration)
		senders := c.getTopSenders(since, c.config.TopItemsTimeScale)
		result[timeframe] = senders
	}
	
	return result
}

// getActiveReceiversTimeScale returns active receivers data for different time scales
func (c *Collector) getActiveReceiversTimeScale() map[string]interface{} {
	timeScales := map[string]time.Duration{
		"1m":  time.Minute,
		"1h":  time.Hour,
		"1d":  24 * time.Hour,
		"7d":  7 * 24 * time.Hour,
		"14d": 14 * 24 * time.Hour,
		"30d": 30 * 24 * time.Hour,
	}
	
	result := make(map[string]interface{})
	now := time.Now()
	
	for timeframe, duration := range timeScales {
		since := now.Add(-duration)
		receivers := c.getTopReceivers(since, c.config.TopItemsTimeScale)
		result[timeframe] = receivers
	}
	
	return result
}

// getChainFlowTimeScale returns chain flow data for different time scales
func (c *Collector) getChainFlowTimeScale() map[string]interface{} {
	timeScales := map[string]time.Duration{
		"1m":  time.Minute,
		"1h":  time.Hour,
		"1d":  24 * time.Hour,
		"7d":  7 * 24 * time.Hour,
		"14d": 14 * 24 * time.Hour,
		"30d": 30 * 24 * time.Hour,
	}
	
	result := make(map[string]interface{})
	now := time.Now()
	
	for timeframe, duration := range timeScales {
		since := now.Add(-duration)
		chains := c.getChainFlows(since)
		result[timeframe] = chains
	}
	
	return result
}

// getAssetVolumeTimeScale returns asset volume data for different time scales
func (c *Collector) getAssetVolumeTimeScale() map[string]interface{} {
	result := make(map[string]interface{})
	now := time.Now()
	
	// Define time periods with specific bucket selection logic
	timeScales := map[string]func() []AssetStats{
		"1m": func() []AssetStats {
			// Last minute: use only 10s buckets from last minute
			since := now.Add(-time.Minute)
			return c.getTopAssetsFromBuckets(since, now, []string{"10s"}, c.config.TopItemsTimeScale)
		},
		"1h": func() []AssetStats {
			// Last hour: use 1m buckets if available, otherwise 10s buckets
			since := now.Add(-time.Hour)
			return c.getTopAssetsFromBuckets(since, now, []string{"1m", "10s"}, c.config.TopItemsTimeScale)
		},
		"1d": func() []AssetStats {
			// Last day: use 1h buckets if available, otherwise 1m buckets
			since := now.Add(-24 * time.Hour)
			return c.getTopAssetsFromBuckets(since, now, []string{"1h", "1m"}, c.config.TopItemsTimeScale)
		},
		"7d": func() []AssetStats {
			// Last 7 days: use 1h buckets
			since := now.Add(-7 * 24 * time.Hour)
			return c.getTopAssetsFromBuckets(since, now, []string{"1h"}, c.config.TopItemsTimeScale)
		},
		"14d": func() []AssetStats {
			// Last 14 days: use daily buckets for long-term data, fallback to 1h buckets for recent data
			since := now.Add(-14 * 24 * time.Hour)
			return c.getTopAssetsFromBuckets(since, now, []string{"daily", "1h"}, c.config.TopItemsTimeScale)
		},
		"30d": func() []AssetStats {
			// Last 30 days: use daily buckets for long-term data, fallback to 1h buckets for recent data
			since := now.Add(-30 * 24 * time.Hour)
			return c.getTopAssetsFromBuckets(since, now, []string{"daily", "1h"}, c.config.TopItemsTimeScale)
		},
	}
	
	for timeframe, getAssets := range timeScales {
		assets := getAssets()
		
		// Convert to simple format for JSON serialization
		simpleAssets := make([]map[string]interface{}, len(assets))
		for i, asset := range assets {
			// Convert TopRoutes to simple maps
			topRoutes := make([]map[string]interface{}, len(asset.TopRoutes))
			for j, route := range asset.TopRoutes {
				topRoutes[j] = map[string]interface{}{
					"fromChain":    route.FromChain,
					"toChain":      route.ToChain,
					"fromName":     route.FromName,
					"toName":       route.ToName,
					"route":        route.Route,
					"count":        route.Count,
					"volume":       route.Volume,
					"percentage":   route.Percentage,
					"lastActivity": route.LastActivity,
				}
			}
			
			simpleAssets[i] = map[string]interface{}{
				"assetSymbol":     asset.AssetSymbol,
				"assetName":       asset.AssetName,
				"transferCount":   asset.TransferCount,
				"totalVolume":     asset.TotalVolume,
				"largestTransfer": asset.LargestTransfer,
				"averageAmount":   asset.AverageAmount,
				"uniqueHolders":   asset.UniqueHolders, // Include unique holder count
				"lastActivity":    asset.LastActivity,
				"topRoutes":       topRoutes,
			}
		}
		
		result[timeframe] = simpleAssets
	}
	
	return result
}

// getTopAssetsFromBuckets gets top assets from specific bucket types within a time range
func (c *Collector) getTopAssetsFromBuckets(since, until time.Time, granularities []string, limit int) []AssetStats {
	assetAggregates := make(map[string]*AssetStats)
	
	// Process each granularity in order of preference
	for _, granularity := range granularities {
		var buckets map[time.Time]*StatsBucket
		
		switch granularity {
		case "10s":
			buckets = c.buckets10s
		case "1m":
			buckets = c.buckets1m
		case "1h":
			buckets = c.buckets1h
		case "daily":
			buckets = c.bucketsDaily
		case "weekly":
			buckets = c.bucketsWeekly
		default:
			continue
		}
		
		// Aggregate from buckets in the time range
		for timestamp, bucket := range buckets {
			if timestamp.After(since) && timestamp.Before(until) {
				for symbol, asset := range bucket.Assets {
					if existing, exists := assetAggregates[symbol]; exists {
						// Merge with existing
						existing.TransferCount += asset.TransferCount
						existing.TotalVolume += asset.TotalVolume
						if asset.LargestTransfer > existing.LargestTransfer {
							existing.LargestTransfer = asset.LargestTransfer
						}
						// Update last activity if more recent
						if asset.LastActivity > existing.LastActivity {
							existing.LastActivity = asset.LastActivity
						}
						
						// Merge HLL sketches for unique holder counting with optimized precision
						if asset.HoldersHLL != nil {
							if existing.HoldersHLL == nil {
								existing.HoldersHLL = getOrCreateOptimizedHLL("asset")
							}
							existing.HoldersHLL.Merge(asset.HoldersHLL)
						}
						
						// Merge routes
						if existing.Routes == nil {
							existing.Routes = make(map[string]*AssetRouteStats)
						}
						for routeKey, route := range asset.Routes {
							if existingRoute, exists := existing.Routes[routeKey]; exists {
								existingRoute.Count += route.Count
								existingRoute.Volume += route.Volume
								if route.LastActivity > existingRoute.LastActivity {
									existingRoute.LastActivity = route.LastActivity
								}
							} else {
								existing.Routes[routeKey] = &AssetRouteStats{
									FromChain:    route.FromChain,
									ToChain:      route.ToChain,
									FromName:     route.FromName,
									ToName:       route.ToName,
									Route:        route.Route,
									Count:        route.Count,
									Volume:       route.Volume,
									LastActivity: route.LastActivity,
								}
							}
						}
					} else {
						// Create new aggregate with optimized HLL
						newAsset := &AssetStats{
							AssetSymbol:     asset.AssetSymbol,
							AssetName:       asset.AssetName,
							TransferCount:   asset.TransferCount,
							TotalVolume:     asset.TotalVolume,
							LargestTransfer: asset.LargestTransfer,
							LastActivity:    asset.LastActivity,
							Routes:          make(map[string]*AssetRouteStats),
							HoldersHLL:      getOrCreateOptimizedHLL("asset"), // Initialize optimized HLL for aggregation
						}
						
						// Copy HLL data if available
						if asset.HoldersHLL != nil {
							newAsset.HoldersHLL.Merge(asset.HoldersHLL)
						}
						
						// Copy routes
						for routeKey, route := range asset.Routes {
							newAsset.Routes[routeKey] = &AssetRouteStats{
								FromChain:    route.FromChain,
								ToChain:      route.ToChain,
								FromName:     route.FromName,
								ToName:       route.ToName,
								Route:        route.Route,
								Count:        route.Count,
								Volume:       route.Volume,
								LastActivity: route.LastActivity,
							}
						}
						
						assetAggregates[symbol] = newAsset
					}
				}
			}
		}
		
		// If we found data in this granularity, we can stop (prefer higher resolution)
		if len(assetAggregates) > 0 {
			break
		}
	}
	
	// Convert to slice and finalize
	assets := make([]AssetStats, 0, len(assetAggregates))
	for denom, assetStats := range assetAggregates {
		// Set display symbol using symbol mapping
		assetSymbol := assetStats.AssetSymbol
		if mappedSymbol, exists := c.denomToSymbol[denom]; exists && mappedSymbol != "" {
			assetSymbol = mappedSymbol
		}
		assetStats.AssetSymbol = assetSymbol
		assetStats.AssetName = assetSymbol
		
		// Calculate average amount
		if assetStats.TransferCount > 0 {
			assetStats.AverageAmount = assetStats.TotalVolume / float64(assetStats.TransferCount)
		}
		
		// Calculate unique holders from HLL
		if assetStats.HoldersHLL != nil {
			assetStats.UniqueHolders = int(assetStats.HoldersHLL.Estimate())
		}
		
		// Calculate top routes for this asset
		routes := make([]AssetRouteStats, 0, len(assetStats.Routes))
		for _, route := range assetStats.Routes {
			// Calculate percentage of asset's total volume
			percentage := float64(0)
			if assetStats.TotalVolume > 0 {
				percentage = (route.Volume / assetStats.TotalVolume) * 100
			}
			route.Percentage = percentage
			routes = append(routes, *route)
		}
		
		// Sort routes by volume (descending) and take top 5
		sort.Slice(routes, func(i, j int) bool {
			return routes[i].Volume > routes[j].Volume
		})
		if len(routes) > 5 {
			routes = routes[:5]
		}
		
		assetStats.TopRoutes = routes
		assets = append(assets, *assetStats)
	}
	
	// Sort by transfer count (descending)
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].TransferCount > assets[j].TransferCount
	})
	
	if len(assets) > limit {
		assets = assets[:limit]
	}
	
	return assets
} 