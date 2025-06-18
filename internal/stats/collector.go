package stats

import (
	"sync"
	"sync/atomic"
	"time"
	"sort"
	"strings"
	"strconv"
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
	
	// Performance optimization settings
	MaxHLLMemoryMB         int64         // Maximum memory for HLL sketches (default: 50MB)
	BatchProcessingSize    int           // Transfers to batch before processing (default: 10)
	CacheUpdateInterval    time.Duration // How often to update aggregate cache (default: 5 seconds)
}

// AddressChainInfo stores chain context for address formatting
type AddressChainInfo struct {
	RpcType    string
	AddrPrefix string
}

// HLLManager manages HyperLogLog memory usage and precision
type HLLManager struct {
	memoryUsage    int64 // Atomic counter
	maxMemoryMB    int64
	precisionLevel int32 // Atomic: 14, 15, or 16
}

// AggregateCache stores pre-computed results for fast retrieval
type AggregateCache struct {
	mu               sync.RWMutex
	lastUpdated      time.Time
	
	// Cached results
	transferRates    *TransferRates
	topRoutes        []RouteStats
	chainFlows       []ChainFlowStats  
	topAssets        []AssetStats
	uniqueWallets    struct {
		Senders   int64
		Receivers int64
		Total     int64
	}
	
	// Cache validity
	validUntil       time.Time
}

// Transfer batch for efficient processing
type TransferBatch struct {
	transfers []models.Transfer
	timestamp time.Time
}

// String interning pool for route keys
var (
	routeStringPool = sync.Map{} // Cache "ChainA→ChainB" strings
	
	// Object pools for memory efficiency
	routeStatsPool = sync.Pool{
		New: func() interface{} { return &RouteStats{} },
	}
	assetStatsPool = sync.Pool{
		New: func() interface{} { return &AssetStats{} },
	}
	chainStatsPool = sync.Pool{
		New: func() interface{} { return &ChainFlowStats{} },
	}
	transferBatchPool = sync.Pool{
		New: func() interface{} { return &TransferBatch{transfers: make([]models.Transfer, 0, 50)} },
	}
)

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
			}
		},
	}
	
	hllManager = &HLLManager{
		maxMemoryMB:    50, // 50MB default limit
		precisionLevel: 14, // Start with medium precision
	}
)

// DefaultConfig returns optimized defaults for 1-year maximum retention
func DefaultConfig() Config {
	return Config{
		RetentionHours:       24,  // Kept for backward compatibility
		TopItemsLimit:        20,
		TopItemsTimeScale:    10,
		CleanupInterval:      15 * time.Minute,
		TenSecondRetention:   30 * time.Minute,
		OneMinuteRetention:   6 * time.Hour,
		OneHourRetention:     7 * 24 * time.Hour,
		DailyRetention:       30 * 24 * time.Hour,
		WeeklyRetention:      365 * 24 * time.Hour,
		MonthlyRetention:     0,
		YearlyRetention:      0,
		
		// Performance settings
		MaxHLLMemoryMB:       50,
		BatchProcessingSize:  10,
		CacheUpdateInterval:  5 * time.Second,
	}
}

// Collector tracks transfer statistics using time-based buckets and HyperLogLog
type Collector struct {
	config     Config
	startTime  time.Time
	
	// Granular locks for better concurrency
	transferMu     sync.RWMutex // Protects transfer processing and buckets
	computeMu      sync.RWMutex // Protects chart computation
	cleanupMu      sync.Mutex   // Protects cleanup operations
	snapshotMu     sync.RWMutex // Protects snapshot operations
	
	// 5-tier time-based buckets for 1-year maximum retention
	buckets10s     map[time.Time]*StatsBucket
	buckets1m      map[time.Time]*StatsBucket
	buckets1h      map[time.Time]*StatsBucket
	bucketsDaily   map[time.Time]*StatsBucket
	bucketsWeekly  map[time.Time]*StatsBucket
	
	// Global HLL sketches for overall unique counting
	globalSendersHLL   *hyperloglog.Sketch
	globalReceiversHLL *hyperloglog.Sketch
	globalWalletsHLL   *hyperloglog.Sketch
	
	// HLL memory management
	hllManager *HLLManager
	
	// Asset symbol mapping
	denomToSymbol map[string]string
	
	// Address formatting
	addressFormatter *utils.AddressFormatter
	addressChainInfo map[string]*AddressChainInfo
	
	// Latency data
	latencyData   []models.LatencyData
	latencyDataMu sync.RWMutex
	
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
	
	// Cleanup and batch processing
	lastCleanup        time.Time
	batchChannel       chan models.Transfer
	batchProcessorDone chan struct{}
	
	// Pre-computed aggregate cache
	aggregateCache *AggregateCache
	
	// Performance counters (atomic)
	transfersProcessed int64
	batchesProcessed   int64
	cacheHits          int64
	cacheMisses        int64
}

// HLL memory management functions
func (h *HLLManager) getCurrentMemoryMB() int64 {
	return atomic.LoadInt64(&h.memoryUsage) / (1024 * 1024)
}

func (h *HLLManager) addMemoryUsage(bytes int64) {
	atomic.AddInt64(&h.memoryUsage, bytes)
}

func (h *HLLManager) getPrecision() int {
	return int(atomic.LoadInt32(&h.precisionLevel))
}

func (h *HLLManager) adjustPrecision() {
	currentMB := h.getCurrentMemoryMB()
	currentPrecision := h.getPrecision()
	
	if currentMB > h.maxMemoryMB && currentPrecision == 16 {
		// Reduce precision under memory pressure (16 -> 14)
		atomic.StoreInt32(&h.precisionLevel, 14)
		utils.LogInfo("stats.hll_manager", "Reduced HLL precision due to memory pressure: %d MB > %d MB, precision: %d", currentMB, h.maxMemoryMB, 14)
	} else if currentMB < h.maxMemoryMB/2 && currentPrecision == 14 {
		// Increase precision when memory is available (14 -> 16)
		atomic.StoreInt32(&h.precisionLevel, 16)
		utils.LogInfo("stats.hll_manager", "Increased HLL precision with available memory: %d MB < %d MB, precision: %d", currentMB, h.maxMemoryMB/2, 16)
	}
}

// getOptimizedHLL creates HLL with dynamic precision based on memory pressure
func (h *HLLManager) getOptimizedHLL(hllType string) *hyperloglog.Sketch {
	precision := h.getPrecision()
	
	var hll *hyperloglog.Sketch
	var memorySize int64
	
	switch precision {
	case 14:
		hll = hyperloglog.New14() // ~1.5KB, 1.63% error
		memorySize = 1536
	case 16:
		hll = hyperloglog.New16() // ~6KB, 0.81% error
		memorySize = 6144
	default:
		hll = hyperloglog.New14()
		memorySize = 1536
	}
	
	h.addMemoryUsage(memorySize)
	return hll
}

// getInternedRouteString returns cached route string to reduce allocations
func getInternedRouteString(fromChain, toChain, fromName, toName string) (string, string) {
	routeKey := fromChain + "→" + toChain
	routeDisplay := fromName + " → " + toName
	
	// Try to get from cache
	if cached, ok := routeStringPool.Load(routeKey); ok {
		if pair, ok := cached.([2]string); ok {
			return pair[0], pair[1]
		}
	}
	
	// Store in cache
	routeStringPool.Store(routeKey, [2]string{routeKey, routeDisplay})
	return routeKey, routeDisplay
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
		// Return route stats to pool
		routeStatsPool.Put(bucket.Routes[k])
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
		// Return asset stats to pool
		assetStatsPool.Put(bucket.Assets[k])
		delete(bucket.Assets, k)
	}
	
	// Initialize optimized HLL sketches
	bucket.SendersHLL = hllManager.getOptimizedHLL("bucket")
	bucket.ReceiversHLL = hllManager.getOptimizedHLL("bucket")
	bucket.WalletsHLL = hllManager.getOptimizedHLL("bucket")
	
	return bucket
}

// returnBucketToPool returns a bucket to the pool after cleanup
func returnBucketToPool(bucket *StatsBucket) {
	if bucket == nil {
		return
	}
	
	// Return objects to pools
	for _, route := range bucket.Routes {
		routeStatsPool.Put(route)
	}
	for _, asset := range bucket.Assets {
		assetStatsPool.Put(asset)
	}
	
	// Clear HLL references (they can't be pooled)
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
	
	// Return bucket structure to pool
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

// BucketSource provides unified access to buckets (live collector or snapshots)
type BucketSource interface {
	Get10sBuckets() map[time.Time]*StatsBucket
	Get1mBuckets() map[time.Time]*StatsBucket  
	Get1hBuckets() map[time.Time]*StatsBucket
	GetDailyBuckets() map[time.Time]*StatsBucket
	GetWeeklyBuckets() map[time.Time]*StatsBucket
	GetGlobalHLLEstimates() (senders, receivers, total int64)
	GetDenomToSymbol() map[string]string
	GetStartTime() time.Time
}

// LiveBucketSource wraps the live collector to implement BucketSource
type LiveBucketSource struct {
	collector *Collector
}

func (l *LiveBucketSource) Get10sBuckets() map[time.Time]*StatsBucket { return l.collector.buckets10s }
func (l *LiveBucketSource) Get1mBuckets() map[time.Time]*StatsBucket { return l.collector.buckets1m }
func (l *LiveBucketSource) Get1hBuckets() map[time.Time]*StatsBucket { return l.collector.buckets1h }
func (l *LiveBucketSource) GetDailyBuckets() map[time.Time]*StatsBucket { return l.collector.bucketsDaily }
func (l *LiveBucketSource) GetWeeklyBuckets() map[time.Time]*StatsBucket { return l.collector.bucketsWeekly }
func (l *LiveBucketSource) GetGlobalHLLEstimates() (int64, int64, int64) {
	return int64(l.collector.globalSendersHLL.Estimate()),
		   int64(l.collector.globalReceiversHLL.Estimate()),
		   int64(l.collector.globalWalletsHLL.Estimate())
}
func (l *LiveBucketSource) GetDenomToSymbol() map[string]string { return l.collector.denomToSymbol }
func (l *LiveBucketSource) GetStartTime() time.Time { return l.collector.startTime }

// SnapshotBucketSource wraps snapshot data to implement BucketSource
type SnapshotBucketSource struct {
	buckets10s       map[time.Time]*StatsBucket
	buckets1m        map[time.Time]*StatsBucket
	buckets1h        map[time.Time]*StatsBucket
	bucketsDaily     map[time.Time]*StatsBucket
	bucketsWeekly    map[time.Time]*StatsBucket
	globalSenders    int64
	globalReceivers  int64
	globalTotal      int64
	denomToSymbol    map[string]string
	startTime        time.Time
}

func (s *SnapshotBucketSource) Get10sBuckets() map[time.Time]*StatsBucket { return s.buckets10s }
func (s *SnapshotBucketSource) Get1mBuckets() map[time.Time]*StatsBucket { return s.buckets1m }
func (s *SnapshotBucketSource) Get1hBuckets() map[time.Time]*StatsBucket { return s.buckets1h }
func (s *SnapshotBucketSource) GetDailyBuckets() map[time.Time]*StatsBucket { return s.bucketsDaily }
func (s *SnapshotBucketSource) GetWeeklyBuckets() map[time.Time]*StatsBucket { return s.bucketsWeekly }
func (s *SnapshotBucketSource) GetGlobalHLLEstimates() (int64, int64, int64) {
	return s.globalSenders, s.globalReceivers, s.globalTotal
}
func (s *SnapshotBucketSource) GetDenomToSymbol() map[string]string { return s.denomToSymbol }
func (s *SnapshotBucketSource) GetStartTime() time.Time { return s.startTime }

// NewCollector creates a new stats collector with optimized performance
func NewCollector(config Config) *Collector {
	// Update HLL manager with config
	hllManager.maxMemoryMB = config.MaxHLLMemoryMB
	
	collector := &Collector{
		config:              config,
		startTime:           time.Now(),
		buckets10s:          make(map[time.Time]*StatsBucket),
		buckets1m:           make(map[time.Time]*StatsBucket),
		buckets1h:           make(map[time.Time]*StatsBucket),
		bucketsDaily:        make(map[time.Time]*StatsBucket),
		bucketsWeekly:       make(map[time.Time]*StatsBucket),
		globalSendersHLL:    hllManager.getOptimizedHLL("global"),
		globalReceiversHLL:  hllManager.getOptimizedHLL("global"),
		globalWalletsHLL:    hllManager.getOptimizedHLL("global"),
		hllManager:          hllManager,
		denomToSymbol:       make(map[string]string),
		addressFormatter:    utils.NewAddressFormatter(),
		addressChainInfo:    make(map[string]*AddressChainInfo),
		lastCleanup:         time.Now(),
		batchChannel:        make(chan models.Transfer, config.BatchProcessingSize*2),
		batchProcessorDone:  make(chan struct{}),
		aggregateCache: &AggregateCache{
			validUntil: time.Now().Add(-time.Hour), // Force initial cache miss
		},
	}
	
	// Start batch processor goroutine
	go collector.batchProcessor()
	
	// Start cache updater goroutine  
	go collector.cacheUpdater()
	
	// Start HLL memory monitor
	go collector.hllMemoryMonitor()
	
	return collector
}

// ProcessTransfer adds a transfer to the batch channel for efficient processing
func (c *Collector) ProcessTransfer(transfer models.Transfer) {
	atomic.AddInt64(&c.transfersProcessed, 1)
	
	// Try to send to batch channel (non-blocking)
	select {
	case c.batchChannel <- transfer:
		// Successfully queued for batch processing
	default:
		// Channel full, process immediately to avoid blocking
		c.processTransferImmediate(transfer)
		utils.LogInfo("stats.collector", "Batch channel full, processing transfer immediately: %d/%d", len(c.batchChannel), cap(c.batchChannel))
	}
}

// processTransferImmediate processes a single transfer immediately (fallback)
func (c *Collector) processTransferImmediate(transfer models.Transfer) {
	c.transferMu.Lock()
	defer c.transferMu.Unlock()
	
	c.processTransferUnsafe(transfer)
}

// processTransferUnsafe processes a transfer without acquiring locks (caller must hold transferMu)
func (c *Collector) processTransferUnsafe(transfer models.Transfer) {
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
	
	// Periodic cleanup (run in background)
	if time.Since(c.lastCleanup) > c.config.CleanupInterval {
		go c.asyncCleanup()
		c.lastCleanup = time.Now()
	}
}

// batchProcessor processes transfers in batches for better performance
func (c *Collector) batchProcessor() {
	batch := transferBatchPool.Get().(*TransferBatch)
	batch.transfers = batch.transfers[:0] // Reset slice
	
	ticker := time.NewTicker(25 * time.Millisecond) // Process batches every 25ms for faster data appearance
	defer ticker.Stop()
	
	for {
		select {
		case transfer := <-c.batchChannel:
			batch.transfers = append(batch.transfers, transfer)
			
			// Process batch when full or after timeout
			if len(batch.transfers) >= c.config.BatchProcessingSize {
				c.processBatch(batch)
				batch.transfers = batch.transfers[:0] // Reset slice
			}
			
		case <-ticker.C:
			// Process any pending transfers
			if len(batch.transfers) > 0 {
				c.processBatch(batch)
				batch.transfers = batch.transfers[:0] // Reset slice
			}
			
		case <-c.batchProcessorDone:
			// Final processing before shutdown
			if len(batch.transfers) > 0 {
				c.processBatch(batch)
			}
			transferBatchPool.Put(batch)
			return
		}
	}
}

// processBatch processes a batch of transfers efficiently
func (c *Collector) processBatch(batch *TransferBatch) {
	if len(batch.transfers) == 0 {
		return
	}
	
	atomic.AddInt64(&c.batchesProcessed, 1)
	
	c.transferMu.Lock()
	defer c.transferMu.Unlock()
	
	// Process all transfers in the batch while holding lock once
	for _, transfer := range batch.transfers {
		c.processTransferUnsafe(transfer)
	}
	
	utils.LogDebug("stats.collector", "Processed transfer batch: size=%d, total_processed=%d, total_batches=%d", 
		len(batch.transfers), atomic.LoadInt64(&c.transfersProcessed), atomic.LoadInt64(&c.batchesProcessed))
}

// asyncCleanup runs cleanup operations in background without blocking transfers
func (c *Collector) asyncCleanup() {
	c.cleanupMu.Lock()
	defer c.cleanupMu.Unlock()
	
	// Run cleanup operations
	c.cleanup()
	
	// Adjust HLL precision based on memory pressure
	c.hllManager.adjustPrecision()
}

// cacheUpdater maintains the aggregate cache
func (c *Collector) cacheUpdater() {
	ticker := time.NewTicker(c.config.CacheUpdateInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			c.updateAggregateCache()
		case <-c.batchProcessorDone:
			return
		}
	}
}

// hllMemoryMonitor monitors HLL memory usage
func (c *Collector) hllMemoryMonitor() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			memoryMB := c.hllManager.getCurrentMemoryMB()
			if memoryMB > c.hllManager.maxMemoryMB {
				utils.LogInfo("stats.hll_monitor", "HLL memory usage high: %d MB > %d MB, precision: %d", memoryMB, c.hllManager.maxMemoryMB, c.hllManager.getPrecision())
				c.hllManager.adjustPrecision()
			}
		case <-c.batchProcessorDone:
			return
		}
	}
}

// updateAggregateCache updates the pre-computed cache
func (c *Collector) updateAggregateCache() {
	c.computeMu.RLock()
	
	// Take snapshot for computation
	now := time.Now()
	
	// Quick check if cache is still valid
	c.aggregateCache.mu.RLock()
	if now.Before(c.aggregateCache.validUntil) {
		c.aggregateCache.mu.RUnlock()
		c.computeMu.RUnlock()
		return
	}
	c.aggregateCache.mu.RUnlock()
	
	// Compute new values
	transferRates := c.calculateTransferRates(now)
	topRoutes := c.getTopRoutes(now.Add(-time.Minute), c.config.TopItemsLimit)
	chainFlows := c.getChainFlows(now.Add(-time.Minute))
	topAssets := c.getTopAssets(now.Add(-time.Minute), c.config.TopItemsLimit)
	
	uniqueWallets := struct {
		Senders   int64
		Receivers int64
		Total     int64
	}{
		Senders:   int64(c.globalSendersHLL.Estimate()),
		Receivers: int64(c.globalReceiversHLL.Estimate()),
		Total:     int64(c.globalWalletsHLL.Estimate()),
	}
	
	c.computeMu.RUnlock()
	
	// Update cache
	c.aggregateCache.mu.Lock()
	c.aggregateCache.transferRates = &transferRates
	c.aggregateCache.topRoutes = topRoutes
	c.aggregateCache.chainFlows = chainFlows
	c.aggregateCache.topAssets = topAssets
	c.aggregateCache.uniqueWallets = uniqueWallets
	c.aggregateCache.lastUpdated = now
	c.aggregateCache.validUntil = now.Add(c.config.CacheUpdateInterval * 2) // Cache valid for 2x update interval (10s responsiveness)
	c.aggregateCache.mu.Unlock()
	
	utils.LogDebug("stats.cache", "Updated aggregate cache: routes=%d, chains=%d, assets=%d, valid_until=%s", 
		len(topRoutes), len(chainFlows), len(topAssets), c.aggregateCache.validUntil.Format(time.RFC3339))
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
			bucket.SendersHLL = hllManager.getOptimizedHLL("bucket")
		}
		if bucket.ReceiversHLL == nil {
			bucket.ReceiversHLL = hllManager.getOptimizedHLL("bucket")
		}
		if bucket.WalletsHLL == nil {
			bucket.WalletsHLL = hllManager.getOptimizedHLL("bucket")
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
	
	var routeDisplay string
	
	// Update route stats using optimized string interning  
	routeKey, routeDisplay := getInternedRouteString(
		transfer.SourceChain.UniversalChainID, 
		transfer.DestinationChain.UniversalChainID,
		transfer.SourceChain.DisplayName,
		transfer.DestinationChain.DisplayName,
	)
	route, exists := bucket.Routes[routeKey]
	if !exists {
		route = routeStatsPool.Get().(*RouteStats)
		route.FromChain = transfer.SourceChain.UniversalChainID
		route.ToChain = transfer.DestinationChain.UniversalChainID
		route.FromName = transfer.SourceChain.DisplayName
		route.ToName = transfer.DestinationChain.DisplayName
		route.Route = routeDisplay
		route.Count = 0
		route.CountChange = 0
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
			bucket.ChainSendersHLL[sourceChainID] = hllManager.getOptimizedHLL("chain")
	}
	if bucket.ChainUsersHLL[sourceChainID] == nil {
		bucket.ChainUsersHLL[sourceChainID] = hllManager.getOptimizedHLL("chain")
	}
	bucket.ChainSendersHLL[sourceChainID].Insert(senderBytes)
	bucket.ChainUsersHLL[sourceChainID].Insert(senderBytes)
	
	// Track receiver for destination chain using optimized HLL precision
	destChainID := transfer.DestinationChain.UniversalChainID
	if bucket.ChainReceiversHLL[destChainID] == nil {
		bucket.ChainReceiversHLL[destChainID] = hllManager.getOptimizedHLL("chain")
	}
	if bucket.ChainUsersHLL[destChainID] == nil {
		bucket.ChainUsersHLL[destChainID] = hllManager.getOptimizedHLL("chain")
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
			HoldersHLL:   hllManager.getOptimizedHLL("asset"), // Initialize optimized HLL for unique holder tracking
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
	

	
	// Track asset route using optimized string interning
	routeKey, routeDisplay = getInternedRouteString(
		transfer.SourceChain.UniversalChainID,
		transfer.DestinationChain.UniversalChainID,
		transfer.SourceChain.DisplayName,
		transfer.DestinationChain.DisplayName,
	)
	var assetRoute *AssetRouteStats
	assetRoute, exists = asset.Routes[routeKey]
	if !exists {
		assetRoute = &AssetRouteStats{
			FromChain:    transfer.SourceChain.UniversalChainID,
			ToChain:      transfer.DestinationChain.UniversalChainID,
			FromName:     transfer.SourceChain.DisplayName,
			ToName:       transfer.DestinationChain.DisplayName,
			Route:        routeDisplay,
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
			HoldersHLL:   hllManager.getOptimizedHLL("asset"),
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
			HoldersHLL:   hllManager.getOptimizedHLL("asset"),
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
		bucket.SendersHLL = hllManager.getOptimizedHLL("bucket")
	}
	if bucket.ReceiversHLL == nil {
		bucket.ReceiversHLL = hllManager.getOptimizedHLL("bucket")
	}
	if bucket.WalletsHLL == nil {
		bucket.WalletsHLL = hllManager.getOptimizedHLL("bucket")
	}

	// Update bucket HLL (with validated byte slices)
	bucket.SendersHLL.Insert(senderBytes)
	bucket.ReceiversHLL.Insert(receiverBytes)
	bucket.WalletsHLL.Insert(senderBytes)
	bucket.WalletsHLL.Insert(receiverBytes)
}

// GetChartData returns aggregated chart data for the last period
func (c *Collector) GetChartData() ChartData {
	now := time.Now()
	
	// Try to get from cache first
	c.aggregateCache.mu.RLock()
	if now.Before(c.aggregateCache.validUntil) && c.aggregateCache.transferRates != nil {
		// Cache hit - return cached data
		transferRates := *c.aggregateCache.transferRates
		uniqueWallets := c.aggregateCache.uniqueWallets
		topRoutes := c.aggregateCache.topRoutes
		chainFlows := c.aggregateCache.chainFlows
		topAssets := c.aggregateCache.topAssets
		c.aggregateCache.mu.RUnlock()
		
		atomic.AddInt64(&c.cacheHits, 1)
		
		return ChartData{
			TransferRates: transferRates,
			UniqueWallets: struct {
				Senders   int64 `json:"senders"`
				Receivers int64 `json:"receivers"`
				Total     int64 `json:"total"`
			}{
				Senders:   uniqueWallets.Senders,
				Receivers: uniqueWallets.Receivers,
				Total:     uniqueWallets.Total,
			},
			TopRoutes:  topRoutes,
			ChainFlows: chainFlows,
			TopAssets:  topAssets,
			Timestamp:  now,
			Uptime:     time.Since(c.startTime).Seconds(),
		}
	}
	c.aggregateCache.mu.RUnlock()
	
	// Cache miss - compute fresh data
	atomic.AddInt64(&c.cacheMisses, 1)
	
	c.computeMu.RLock()
	defer c.computeMu.RUnlock()
	
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
	topAssets := c.getTopAssets(now.Add(-time.Minute), c.config.TopItemsLimit)
	
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
	now := time.Now()
	
	// Try to get from cache first (optimized path)
	c.aggregateCache.mu.RLock()
	if now.Before(c.aggregateCache.validUntil) && c.aggregateCache.transferRates != nil {
		// Cache hit - use cached data to build frontend response
		transferRates := *c.aggregateCache.transferRates
		uniqueWallets := c.aggregateCache.uniqueWallets
		topRoutes := c.aggregateCache.topRoutes
		c.aggregateCache.mu.RUnlock()
		
		atomic.AddInt64(&c.cacheHits, 1)
		
		return c.buildFrontendResponse(transferRates, uniqueWallets, topRoutes, now)
	}
	c.aggregateCache.mu.RUnlock()
	
	// Cache miss - need to compute fresh data
	atomic.AddInt64(&c.cacheMisses, 1)
	
	// Take a quick snapshot of the data while holding the lock
	c.snapshotMu.RLock()
	
	// Quick snapshot - just copy the essential data structures
	buckets10sSnapshot := make(map[time.Time]*StatsBucket)
	buckets1mSnapshot := make(map[time.Time]*StatsBucket)
	buckets1hSnapshot := make(map[time.Time]*StatsBucket)
	bucketsDaily := make(map[time.Time]*StatsBucket)
	bucketsWeekly := make(map[time.Time]*StatsBucket)
	
	// Copy bucket references (shallow copy is fine for read-only operations)
	for k, v := range c.buckets10s {
		buckets10sSnapshot[k] = v
	}
	for k, v := range c.buckets1m {
		buckets1mSnapshot[k] = v
	}
	for k, v := range c.buckets1h {
		buckets1hSnapshot[k] = v
	}
	for k, v := range c.bucketsDaily {
		bucketsDaily[k] = v
	}
	for k, v := range c.bucketsWeekly {
		bucketsWeekly[k] = v
	}
	
	// Copy global HLL estimates
	globalSendersEstimate := int64(c.globalSendersHLL.Estimate())
	globalReceiversEstimate := int64(c.globalReceiversHLL.Estimate())
	globalWalletsEstimate := int64(c.globalWalletsHLL.Estimate())
	
	// Copy other essential data
	startTime := c.startTime
	latencyDataCopy := make([]models.LatencyData, len(c.latencyData))
	copy(latencyDataCopy, c.latencyData)
	
	// Copy denomToSymbol mapping for snapshot methods
	denomToSymbolCopy := make(map[string]string)
	for k, v := range c.denomToSymbol {
		denomToSymbolCopy[k] = v
	}
	
	// Copy previous snapshots for percentage calculations
	var prevMinute, prevHour, prevDay, prev7Days, prev14Days, prev30Days *PeriodStats
	if c.previousMinute != nil {
		prevMinute = &PeriodStats{
			Transfers: c.previousMinute.Transfers,
			Senders:   c.previousMinute.Senders,
			Receivers: c.previousMinute.Receivers,
			Total:     c.previousMinute.Total,
		}
	}
	if c.previousHour != nil {
		prevHour = &PeriodStats{
			Transfers: c.previousHour.Transfers,
			Senders:   c.previousHour.Senders,
			Receivers: c.previousHour.Receivers,
			Total:     c.previousHour.Total,
		}
	}
	if c.previousDay != nil {
		prevDay = &PeriodStats{
			Transfers: c.previousDay.Transfers,
			Senders:   c.previousDay.Senders,
			Receivers: c.previousDay.Receivers,
			Total:     c.previousDay.Total,
		}
	}
	if c.previous7Days != nil {
		prev7Days = &PeriodStats{
			Transfers: c.previous7Days.Transfers,
			Senders:   c.previous7Days.Senders,
			Receivers: c.previous7Days.Receivers,
			Total:     c.previous7Days.Total,
		}
	}
	if c.previous14Days != nil {
		prev14Days = &PeriodStats{
			Transfers: c.previous14Days.Transfers,
			Senders:   c.previous14Days.Senders,
			Receivers: c.previous14Days.Receivers,
			Total:     c.previous14Days.Total,
		}
	}
	if c.previous30Days != nil {
		prev30Days = &PeriodStats{
			Transfers: c.previous30Days.Transfers,
			Senders:   c.previous30Days.Senders,
			Receivers: c.previous30Days.Receivers,
			Total:     c.previous30Days.Total,
		}
	}
	
	// CRITICAL: Release the lock immediately after taking snapshot
	c.snapshotMu.RUnlock()
	
	// NOW do all heavy computation without holding any locks
	now = time.Now()
	
	// Calculate transfer rates using snapshots
	transferRates := c.calculateTransferRatesFromSnapshot(now, startTime, 
		buckets10sSnapshot, buckets1mSnapshot, buckets1hSnapshot, bucketsDaily, bucketsWeekly,
		prevMinute, prevHour, prevDay, prev7Days, prev14Days, prev30Days)
	
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
		"uniqueSendersTotal":      globalSendersEstimate,
		"uniqueReceiversTotal":    globalReceiversEstimate,
		"uniqueTotalWallets":      globalWalletsEstimate,
		"dataAvailability":        transferRates.DataAvailability,
		"serverUptimeSeconds":     transferRates.Uptime,
	}
	
	// Get other data using live methods (simplified and optimized)
	topRoutes := c.getTopRoutes(now.Add(-time.Minute), c.config.TopItemsLimit)
	
	// Build response matching frontend expectations - use live methods for simplified approach
	frontendData := map[string]interface{}{
		"currentRates":           transferRatesForFrontend,  // REVERT: Keep original key name for backward compatibility
		"activeWalletRates":      activeWalletRates,
		"popularRoutes":          topRoutes,
		"popularRoutesTimeScale": c.getPopularRoutesTimeScale(), // Use live version
		"activeSenders":          c.getTopSenders(now.Add(-time.Minute), c.config.TopItemsLimit),
		"activeReceivers":        c.getTopReceivers(now.Add(-time.Minute), c.config.TopItemsLimit),
		"activeSendersTimeScale": c.getActiveSendersTimeScale(), // Use live version
		"activeReceiversTimeScale": c.getActiveReceiversTimeScale(), // Use live version
		"chainFlowData": map[string]interface{}{
			"chains":             c.getChainFlows(now.Add(-time.Minute)),
			"chainFlowTimeScale": c.getChainFlowTimeScale(), // Use live version
			"totalOutgoing":      c.calculateTotalOutgoing(now.Add(-time.Minute)),
			"totalIncoming":      c.calculateTotalIncoming(now.Add(-time.Minute)),
			"serverUptimeSeconds": transferRates.Uptime,
		},
		"assetVolumeData": c.getAssetVolumeDataForFrontend(now.Add(-time.Minute), transferRates.Uptime), // Use live version
		"dataAvailability": transferRates.DataAvailability, // Top-level data availability for wallet stats
		"latencyData": latencyDataCopy, // Use the copied latency data
	}
	
	return frontendData
}

// buildFrontendResponse builds the frontend response with cached transfer rates but live chart data
func (c *Collector) buildFrontendResponse(transferRates TransferRates, uniqueWallets struct {
	Senders   int64
	Receivers int64
	Total     int64
}, topRoutes []RouteStats, now time.Time) interface{} {
	
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
		"serverUptimeSeconds": transferRates.Uptime,
	}
	
	// Active wallet rates matching frontend expectations
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
		"uniqueSendersTotal":      uniqueWallets.Senders,
		"uniqueReceiversTotal":    uniqueWallets.Receivers,
		"uniqueTotalWallets":      uniqueWallets.Total,
		"dataAvailability":        transferRates.DataAvailability,
		"serverUptimeSeconds":     transferRates.Uptime,
	}
	
	// Get latency data
	latencyData := c.getLatencyData()
	
	// Build response with LIVE DATA even for cached responses
	return map[string]interface{}{
		"currentRates":           transferRatesForFrontend,
		"activeWalletRates":      activeWalletRates,
		"popularRoutes":          topRoutes,
		"popularRoutesTimeScale": c.getPopularRoutesTimeScale(), // Use live data
		"activeSenders":          c.getTopSenders(now.Add(-time.Minute), c.config.TopItemsLimit), // Use live data
		"activeReceivers":        c.getTopReceivers(now.Add(-time.Minute), c.config.TopItemsLimit), // Use live data
		"activeSendersTimeScale": c.getActiveSendersTimeScale(), // Use live data
		"activeReceiversTimeScale": c.getActiveReceiversTimeScale(), // Use live data
		"chainFlowData": map[string]interface{}{
			"chains":             c.getChainFlows(now.Add(-time.Minute)), // Use live data
			"chainFlowTimeScale": c.getChainFlowTimeScale(), // Use live data
			"totalOutgoing":      c.calculateTotalOutgoing(now.Add(-time.Minute)), // Use live data
			"totalIncoming":      c.calculateTotalIncoming(now.Add(-time.Minute)), // Use live data
			"serverUptimeSeconds": transferRates.Uptime,
		},
		"assetVolumeData":  c.getAssetVolumeDataForFrontend(now.Add(-time.Minute), transferRates.Uptime), // Use live data
		"dataAvailability": transferRates.DataAvailability,
		"latencyData":      latencyData,
		"_cached":          true, // Debug marker
	}
}

// UpdateLatencyData updates the stored latency data
func (c *Collector) UpdateLatencyData(latencyData []models.LatencyData) {
	c.latencyDataMu.Lock()
	defer c.latencyDataMu.Unlock()
	c.latencyData = latencyData
}

// getLatencyData returns a copy of the current latency data
func (c *Collector) getLatencyData() []models.LatencyData {
	c.latencyDataMu.RLock()
	defer c.latencyDataMu.RUnlock()
	
	// Return a copy to prevent race conditions
	result := make([]models.LatencyData, len(c.latencyData))
	copy(result, c.latencyData)
	return result
}

// Helper function to parse amount from string
func parseAmount(amountStr string) float64 {
	amount, err := strconv.ParseFloat(amountStr, 64)
	if err != nil {
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
		DataAvailability: c.calculateDataAvailabilityUnified(now, &LiveBucketSource{collector: c}),
		Uptime:           uptime,
	}
}

// calculateDataAvailabilityUnified determines what time ranges have sufficient data available
// Works with both live data and snapshots via BucketSource interface
// Conservative approach: only show data for timeframes where we have adequate system uptime AND data
func (c *Collector) calculateDataAvailabilityUnified(now time.Time, source BucketSource) DataAvailability {
	uptime := now.Sub(source.GetStartTime())
	
	// Helper function to check if we have adequate data coverage for a timeframe
	hasSufficientDataContent := func(duration time.Duration, since time.Time) bool {
		// Must have at least the minimum uptime for this timeframe (with reasonable buffer)
		requiredUptime := duration
		if duration <= time.Minute {
			requiredUptime = 30 * time.Second // Need 30s for 1-minute data
		} else if duration <= time.Hour {
			requiredUptime = 5 * time.Minute // Need 5 minutes for 1-hour data
		} else if duration <= 24*time.Hour {
			requiredUptime = 30 * time.Minute // Need 30 minutes for 1-day data
		} else if duration <= 7*24*time.Hour {
			requiredUptime = 2 * time.Hour // Need 2 hours for 7-day data
		} else {
			requiredUptime = 6 * time.Hour // Need 6 hours for longer periods
		}
		
		if uptime < requiredUptime {
			return false
		}
		
		// Use the SAME logic as aggregateBucketsForPeriod to check for actual data
		var buckets []*StatsBucket
		
		// Include 10-second buckets (most recent data: 0-30min)
		for timestamp, bucket := range source.Get10sBuckets() {
			if bucket != nil && bucket.TransferCount > 0 && (since.IsZero() || timestamp.After(since)) {
				buckets = append(buckets, bucket)
			}
		}
		
		// Include 1-minute buckets (recent data: 30min-6h)
		for timestamp, bucket := range source.Get1mBuckets() {
			if bucket != nil && bucket.TransferCount > 0 && (since.IsZero() || timestamp.After(since)) && timestamp.Before(now.Add(-c.config.TenSecondRetention)) {
				buckets = append(buckets, bucket)
			}
		}
		
		// Include 1-hour buckets (medium-term data: 6h-7d)
		for timestamp, bucket := range source.Get1hBuckets() {
			if bucket != nil && bucket.TransferCount > 0 && (since.IsZero() || timestamp.After(since)) && timestamp.Before(now.Add(-c.config.OneMinuteRetention)) {
				buckets = append(buckets, bucket)
			}
		}
		
		// Include daily buckets (medium-term data: 7d-30d)
		for timestamp, bucket := range source.GetDailyBuckets() {
			if bucket != nil && bucket.TransferCount > 0 && (since.IsZero() || timestamp.After(since)) && timestamp.Before(now.Add(-c.config.OneHourRetention)) {
				buckets = append(buckets, bucket)
			}
		}
		
		// Include weekly buckets (final tier: 30d-1y, then delete)
		for timestamp, bucket := range source.GetWeeklyBuckets() {
			if bucket != nil && bucket.TransferCount > 0 && (since.IsZero() || timestamp.After(since)) && timestamp.Before(now.Add(-c.config.DailyRetention)) {
				buckets = append(buckets, bucket)
			}
		}
		
		// Count total transfers to ensure we have meaningful data
		var totalTransfers int64
		for _, bucket := range buckets {
			totalTransfers += bucket.TransferCount
		}
		
		// REASONABLE: Show data if we have any transfers AND sufficient uptime
		return totalTransfers > 0 && len(buckets) > 0
	}
	
	return DataAvailability{
		HasMinute: hasSufficientDataContent(time.Minute, now.Add(-time.Minute)),
		HasHour:   hasSufficientDataContent(time.Hour, now.Add(-time.Hour)),
		HasDay:    hasSufficientDataContent(24*time.Hour, now.Add(-24*time.Hour)),
		Has7Days:  hasSufficientDataContent(7*24*time.Hour, now.Add(-7*24*time.Hour)),
		Has14Days: hasSufficientDataContent(14*24*time.Hour, now.Add(-14*24*time.Hour)),
		Has30Days: hasSufficientDataContent(30*24*time.Hour, now.Add(-30*24*time.Hour)),
	}
}

// calculatePeriodStats calculates stats for transfers since the given time using any BucketSource
func (c *Collector) calculatePeriodStats(since time.Time) PeriodStats {
	source := &LiveBucketSource{collector: c}
	return c.calculatePeriodStatsUnified(since, source)
}

// calculatePeriodStatsUnified calculates stats using unified BucketSource interface
func (c *Collector) calculatePeriodStatsUnified(since time.Time, source BucketSource) PeriodStats {
	now := time.Now()
	var transferCount int64
	
	// Create temporary HLL sketches for unique counting
	sendersHLL := hyperloglog.New16()
	receiversHLL := hyperloglog.New16()
	totalHLL := hyperloglog.New16()
	
	// Also track exact unique counts for small datasets (fallback)
	uniqueSenders := make(map[string]bool)
	uniqueReceivers := make(map[string]bool)
	uniqueTotal := make(map[string]bool)
	
	// Aggregate from 10-second buckets (most recent data)
	for timestamp, bucket := range source.Get10sBuckets() {
		if since.IsZero() || timestamp.After(since) {
			transferCount += bucket.TransferCount
			
			// Track exact counts for validation (small dataset handling)
			for sender := range bucket.Senders {
				uniqueSenders[sender] = true
				uniqueTotal[sender] = true
			}
			for receiver := range bucket.Receivers {
				uniqueReceivers[receiver] = true
				uniqueTotal[receiver] = true
			}
			
			// Merge HLL sketches for unique counting with thread safety
			if bucket.SendersHLL != nil {
				c.mergeHLLSketchSafe(sendersHLL, bucket.SendersHLL)
			}
			if bucket.ReceiversHLL != nil {
				c.mergeHLLSketchSafe(receiversHLL, bucket.ReceiversHLL)
			}
			if bucket.WalletsHLL != nil {
				c.mergeHLLSketchSafe(totalHLL, bucket.WalletsHLL)
			}
		}
	}
	
			// Include 1-minute buckets for older data (beyond 1 hour)
	for timestamp, bucket := range source.Get1mBuckets() {
		if (since.IsZero() || timestamp.After(since)) && timestamp.Before(now.Add(-time.Hour)) {
			transferCount += bucket.TransferCount
			
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
				c.mergeHLLSketchSafe(sendersHLL, bucket.SendersHLL)
			}
			if bucket.ReceiversHLL != nil {
				c.mergeHLLSketchSafe(receiversHLL, bucket.ReceiversHLL)
			}
			if bucket.WalletsHLL != nil {
				c.mergeHLLSketchSafe(totalHLL, bucket.WalletsHLL)
			}
		}
	}
	
			// Include 1-hour buckets for oldest data (beyond 1 day)
	for timestamp, bucket := range source.Get1hBuckets() {
		if (since.IsZero() || timestamp.After(since)) && timestamp.Before(now.Add(-24*time.Hour)) {
			transferCount += bucket.TransferCount
			
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
				c.mergeHLLSketchSafe(sendersHLL, bucket.SendersHLL)
			}
			if bucket.ReceiversHLL != nil {
				c.mergeHLLSketchSafe(receiversHLL, bucket.ReceiversHLL)
			}
			if bucket.WalletsHLL != nil {
				c.mergeHLLSketchSafe(totalHLL, bucket.WalletsHLL)
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
		
		// Merge HLL sketches with optimized precision and thread safety
		for chainID, hll := range bucket.ChainSendersHLL {
			if chainSendersHLL[chainID] == nil {
				chainSendersHLL[chainID] = hllManager.getOptimizedHLL("chain")
			}
			c.mergeHLLSketchSafe(chainSendersHLL[chainID], hll)
		}
		for chainID, hll := range bucket.ChainReceiversHLL {
			if chainReceiversHLL[chainID] == nil {
				chainReceiversHLL[chainID] = hllManager.getOptimizedHLL("chain")
			}
			c.mergeHLLSketchSafe(chainReceiversHLL[chainID], hll)
		}
		for chainID, hll := range bucket.ChainUsersHLL {
			if chainUsersHLL[chainID] == nil {
				chainUsersHLL[chainID] = hllManager.getOptimizedHLL("chain")
			}
			c.mergeHLLSketchSafe(chainUsersHLL[chainID], hll)
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
	chainAssetAggregates := c.aggregateChainAssets(buckets)

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
	return c.getTopWallets(since, limit, "senders")
}

// getTopReceivers returns the top active receivers by transfer count since the given time
func (c *Collector) getTopReceivers(since time.Time, limit int) []WalletStats {
	return c.getTopWallets(since, limit, "receivers")
}

// getTopAssets returns the top assets by transfer count since the given time
func (c *Collector) getTopAssets(since time.Time, limit int) []AssetStats {
	buckets := c.aggregateBucketsForPeriod(since)
	assetAggregates := c.aggregateAssetStats(buckets, c.denomToSymbol)
	return c.convertAssetsToSlice(assetAggregates, limit)
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
	
	// Use transfer lock for bucket operations (caller already holds cleanupMu)
	c.transferMu.Lock()
	defer c.transferMu.Unlock()
	
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
	
	utils.LogDebug("stats.cleanup", "Completed bucket cleanup: 10s=%d, 1m=%d, 1h=%d, daily=%d, weekly=%d, memory=%dMB", 
		len(c.buckets10s), len(c.buckets1m), len(c.buckets1h), len(c.bucketsDaily), len(c.bucketsWeekly), c.hllManager.getCurrentMemoryMB())
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
		SendersHLL:        hllManager.getOptimizedHLL("bucket"),
		ReceiversHLL:      hllManager.getOptimizedHLL("bucket"),
		WalletsHLL:        hllManager.getOptimizedHLL("bucket"),
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
				result.ChainSendersHLL[chainID] = hllManager.getOptimizedHLL("chain")
			}
			result.ChainSendersHLL[chainID].Merge(hll)
		}
		for chainID, hll := range bucket.ChainReceiversHLL {
			if result.ChainReceiversHLL[chainID] == nil {
				result.ChainReceiversHLL[chainID] = hllManager.getOptimizedHLL("chain")
			}
			result.ChainReceiversHLL[chainID].Merge(hll)
		}
		for chainID, hll := range bucket.ChainUsersHLL {
			if result.ChainUsersHLL[chainID] == nil {
				result.ChainUsersHLL[chainID] = hllManager.getOptimizedHLL("chain")
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
							existing.HoldersHLL = hllManager.getOptimizedHLL("asset")
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
						HoldersHLL:    hllManager.getOptimizedHLL("asset"),
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
						existing.HoldersHLL = hllManager.getOptimizedHLL("asset")
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
					HoldersHLL:      hllManager.getOptimizedHLL("asset"), // Initialize optimized HLL for aggregation
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
	return c.generateTimeScaleData("routes", func(since time.Time, limit int) interface{} {
		return c.getTopRoutes(since, limit)
	})
}

// getActiveSendersTimeScale returns active senders data for different time scales
func (c *Collector) getActiveSendersTimeScale() map[string]interface{} {
	return c.generateTimeScaleData("senders", func(since time.Time, limit int) interface{} {
		return c.getTopSenders(since, limit)
	})
}

// getActiveReceiversTimeScale returns active receivers data for different time scales
func (c *Collector) getActiveReceiversTimeScale() map[string]interface{} {
	return c.generateTimeScaleData("receivers", func(since time.Time, limit int) interface{} {
		return c.getTopReceivers(since, limit)
	})
}

// getChainFlowTimeScale returns chain flow data for different time scales
func (c *Collector) getChainFlowTimeScale() map[string]interface{} {
	return c.generateTimeScaleData("chains", func(since time.Time, limit int) interface{} {
		return c.getChainFlows(since)
	})
}

// getAssetVolumeTimeScale returns asset volume data for different time scales
func (c *Collector) getAssetVolumeTimeScale() map[string]interface{} {
	result := make(map[string]interface{})
	now := time.Now()
	
	// Optimize: Calculate only the 3 most important timeframes for performance
	// Each calculation uses the appropriate bucket granularity for data accuracy
	
	// 1-minute data: use 10s buckets for highest precision
	oneMinuteAssets := c.getTopAssetsFromBuckets(now.Add(-time.Minute), now, []string{"10s"}, c.config.TopItemsTimeScale)
	
	// 1-hour data: use 1m + 10s buckets for good precision
	oneHourAssets := c.getTopAssetsFromBuckets(now.Add(-time.Hour), now, []string{"1m", "10s"}, c.config.TopItemsTimeScale)
	
	// 1-day data: use 1h + 1m buckets for accurate daily view
	oneDayAssets := c.getTopAssetsFromBuckets(now.Add(-24*time.Hour), now, []string{"1h", "1m"}, c.config.TopItemsTimeScale)
	
	// Convert each timeframe to simple format for JSON serialization
	convertToSimpleFormat := func(assets []AssetStats) []map[string]interface{} {
		
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
				"uniqueHolders":   asset.UniqueHolders,
				"lastActivity":    asset.LastActivity,
				"topRoutes":       topRoutes,
			}
		}
		return simpleAssets
	}
	
	result["1m"] = convertToSimpleFormat(oneMinuteAssets)
	result["1h"] = convertToSimpleFormat(oneHourAssets)
	result["1d"] = convertToSimpleFormat(oneDayAssets)
	// Use 1d data for longer timeframes to avoid expensive bucket aggregations
	result["7d"] = convertToSimpleFormat(oneDayAssets)   // Reasonable approximation for UI responsiveness
	result["14d"] = convertToSimpleFormat(oneDayAssets)  // Reasonable approximation for UI responsiveness
	result["30d"] = convertToSimpleFormat(oneDayAssets)  // Reasonable approximation for UI responsiveness
	
	return result
}

// getTopAssetsFromBuckets gets top assets from specific bucket types within a time range
func (c *Collector) getTopAssetsFromBuckets(since, until time.Time, granularities []string, limit int) []AssetStats {
	// Collect relevant buckets from ALL specified granularities (not just the first one with data)
	var relevantBuckets []*StatsBucket
	
	// Process each granularity and collect ALL relevant buckets
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
		
		// Collect buckets in the time range from this granularity
		for timestamp, bucket := range buckets {
			if timestamp.After(since) && timestamp.Before(until) {
				relevantBuckets = append(relevantBuckets, bucket)
			}
		}
	}
	
	// Use consolidated asset aggregation logic
	assetAggregates := c.aggregateAssetStats(relevantBuckets, c.denomToSymbol)
	return c.convertAssetsToSlice(assetAggregates, limit)
}

// Snapshot-based methods for non-blocking chart data generation
// These methods operate on snapshots rather than the original data, avoiding lock contention

// calculateTransferRatesFromSnapshot calculates transfer rates using snapshots
func (c *Collector) calculateTransferRatesFromSnapshot(now, startTime time.Time, 
	buckets10s, buckets1m, buckets1h, bucketsDaily, bucketsWeekly map[time.Time]*StatsBucket,
	prevMinute, prevHour, prevDay, prev7Days, prev14Days, prev30Days *PeriodStats) TransferRates {
	
	uptime := now.Sub(startTime).Seconds()
	
	// Create snapshot source
	source := &SnapshotBucketSource{
		buckets10s:    buckets10s,
		buckets1m:     buckets1m,
		buckets1h:     buckets1h,
		bucketsDaily:  bucketsDaily,
		bucketsWeekly: bucketsWeekly,
		startTime:     startTime,
	}
	
	// Calculate current stats for each period using unified method
	lastMinuteCurrent := c.calculatePeriodStatsUnified(now.Add(-time.Minute), source)
	lastHourCurrent := c.calculatePeriodStatsUnified(now.Add(-time.Hour), source)
	lastDayCurrent := c.calculatePeriodStatsUnified(now.Add(-24*time.Hour), source)
	last7DaysCurrent := c.calculatePeriodStatsUnified(now.AddDate(0, 0, -7), source)
	last14DaysCurrent := c.calculatePeriodStatsUnified(now.AddDate(0, 0, -14), source)
	last30DaysCurrent := c.calculatePeriodStatsUnified(now.AddDate(0, 0, -30), source)
	total := c.calculatePeriodStatsUnified(time.Time{}, source) // All time
	
	// Calculate with changes using provided snapshots
	lastMinute := c.addPercentageChanges(lastMinuteCurrent, prevMinute)
	lastHour := c.addPercentageChanges(lastHourCurrent, prevHour)
	lastDay := c.addPercentageChanges(lastDayCurrent, prevDay)
	last7Days := c.addPercentageChanges(last7DaysCurrent, prev7Days)
	last14Days := c.addPercentageChanges(last14DaysCurrent, prev14Days)
	last30Days := c.addPercentageChanges(last30DaysCurrent, prev30Days)
	
	return TransferRates{
		LastMinute:       lastMinute,
		LastHour:         lastHour,
		LastDay:          lastDay,
		Last7Days:        last7Days,
		Last14Days:       last14Days,
		Last30Days:       last30Days,
		Total:            total,
		DataAvailability: c.calculateDataAvailabilityUnified(now, &SnapshotBucketSource{
		buckets10s: buckets10s, buckets1m: buckets1m, buckets1h: buckets1h, 
		bucketsDaily: bucketsDaily, bucketsWeekly: bucketsWeekly, startTime: startTime}),
		Uptime:           uptime,
	}
}





// getTopRoutesFromSnapshot gets top routes using snapshots
func (c *Collector) getTopRoutesFromSnapshot(since time.Time, limit int, 
	buckets10s, buckets1m, buckets1h map[time.Time]*StatsBucket) []RouteStats {
	
	routeStats := make(map[string]*RouteStats)
	
	// Aggregate from snapshots
	for timestamp, bucket := range buckets10s {
		if since.IsZero() || timestamp.After(since) {
			for routeKey, route := range bucket.Routes {
				if existing, exists := routeStats[routeKey]; exists {
					existing.Count += route.Count
				} else {
					routeStats[routeKey] = &RouteStats{
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
	
	// Sort and limit
	routes := make([]RouteStats, 0, len(routeStats))
	for _, route := range routeStats {
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

// Light snapshot-based methods for time scale data

// getAssetVolumeTimeScaleFromSnapshot gets asset volume time scale data using snapshots
func (c *Collector) getAssetVolumeTimeScaleFromSnapshot(buckets10s, buckets1m map[time.Time]*StatsBucket, denomToSymbol map[string]string) map[string]interface{} {
	result := make(map[string]interface{})
	now := time.Now()
	
	// Simplified version: only calculate key timeframes for performance
	// Use snapshot data to avoid locks
	
	// 1-minute data from 10s buckets
	oneMinuteAssets := c.getTopAssetsFromSnapshotBuckets(now.Add(-time.Minute), now, 
		buckets10s, nil, nil, c.config.TopItemsTimeScale, denomToSymbol)
	
	// 1-hour data from 10s + 1m buckets  
	oneHourAssets := c.getTopAssetsFromSnapshotBuckets(now.Add(-time.Hour), now,
		buckets10s, buckets1m, nil, c.config.TopItemsTimeScale, denomToSymbol)
	
	// 1-day data from 1m buckets (simplified)
	oneDayAssets := c.getTopAssetsFromSnapshotBuckets(now.Add(-24*time.Hour), now,
		nil, buckets1m, nil, c.config.TopItemsTimeScale, denomToSymbol)
	
	// Convert to simple format for JSON serialization
	convertToSimpleFormat := func(assets []AssetStats) []map[string]interface{} {
		simpleAssets := make([]map[string]interface{}, len(assets))
		for i, asset := range assets {
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
				"uniqueHolders":   asset.UniqueHolders,
				"lastActivity":    asset.LastActivity,
				"topRoutes":       topRoutes,
			}
		}
		return simpleAssets
	}
	
	result["1m"] = convertToSimpleFormat(oneMinuteAssets)
	result["1h"] = convertToSimpleFormat(oneHourAssets)
	result["1d"] = convertToSimpleFormat(oneDayAssets)
	// Use 1d data for longer timeframes for performance
	result["7d"] = convertToSimpleFormat(oneDayAssets)
	result["14d"] = convertToSimpleFormat(oneDayAssets)
	result["30d"] = convertToSimpleFormat(oneDayAssets)
	
	return result
}

// getTopAssetsFromSnapshotBuckets gets top assets from snapshot buckets within a time range
func (c *Collector) getTopAssetsFromSnapshotBuckets(since, until time.Time, 
	buckets10s, buckets1m, buckets1h map[time.Time]*StatsBucket, limit int, denomToSymbol map[string]string) []AssetStats {
	
	assetAggregates := make(map[string]*AssetStats)
	
	// Process ALL bucket types (not just the first one with data)
	bucketSets := []map[time.Time]*StatsBucket{buckets10s, buckets1m, buckets1h}
	
	for _, buckets := range bucketSets {
		if buckets == nil {
			continue
		}
		
		for timestamp, bucket := range buckets {
			if timestamp.After(since) && timestamp.Before(until) {
				for symbol, asset := range bucket.Assets {
					if existing, exists := assetAggregates[symbol]; exists {
						existing.TransferCount += asset.TransferCount
						existing.TotalVolume += asset.TotalVolume
						if asset.LargestTransfer > existing.LargestTransfer {
							existing.LargestTransfer = asset.LargestTransfer
						}
						if asset.LastActivity > existing.LastActivity {
							existing.LastActivity = asset.LastActivity
						}
						
						// Merge routes if available
						if asset.Routes != nil {
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
						}
					} else {
						newAsset := &AssetStats{
							AssetSymbol:     asset.AssetSymbol,
							AssetName:       asset.AssetName,
							TransferCount:   asset.TransferCount,
							TotalVolume:     asset.TotalVolume,
							LargestTransfer: asset.LargestTransfer,
							LastActivity:    asset.LastActivity,
							Routes:          make(map[string]*AssetRouteStats),
						}
						
						// Copy routes if available
						if asset.Routes != nil {
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
						}
						
						assetAggregates[symbol] = newAsset
					}
				}
			}
		}
	}
	
	// Convert to slice and finalize
	assets := make([]AssetStats, 0, len(assetAggregates))
	for symbol, assetStats := range assetAggregates {
		// Use symbol mapping if available
		if mappedSymbol, exists := denomToSymbol[symbol]; exists && mappedSymbol != "" {
			assetStats.AssetSymbol = mappedSymbol
			assetStats.AssetName = mappedSymbol
		}
		
		// Calculate average amount
		if assetStats.TransferCount > 0 {
			assetStats.AverageAmount = assetStats.TotalVolume / float64(assetStats.TransferCount)
		}
		
		// Calculate top routes for this asset
		routes := make([]AssetRouteStats, 0, len(assetStats.Routes))
		for _, route := range assetStats.Routes {
			percentage := float64(0)
			if assetStats.TotalVolume > 0 {
				percentage = (route.Volume / assetStats.TotalVolume) * 100
			}
			route.Percentage = percentage
			routes = append(routes, *route)
		}
		
		// Sort routes by volume and take top 5
		sort.Slice(routes, func(i, j int) bool {
			return routes[i].Volume > routes[j].Volume
		})
		if len(routes) > 5 {
			routes = routes[:5]
		}
		
		assetStats.TopRoutes = routes
		assets = append(assets, *assetStats)
	}
	
	// Sort by transfer count
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].TransferCount > assets[j].TransferCount
	})
	
	if len(assets) > limit {
		assets = assets[:limit]
	}
	
	return assets
}

// ============================================================================
// CONSOLIDATED UTILITY FUNCTIONS - Eliminates 800+ lines of duplication
// ============================================================================

// getTopWallets is a generic aggregator that replaces getTopSenders and getTopReceivers
func (c *Collector) getTopWallets(since time.Time, limit int, walletType string) []WalletStats {
	buckets := c.aggregateBucketsForPeriod(since)
	aggregates := make(map[string]int64)
	lastActivity := make(map[string]time.Time)
	
	for _, bucket := range buckets {
		var walletMap map[string]int64
		switch walletType {
		case "senders":
			walletMap = bucket.Senders
		case "receivers":
			walletMap = bucket.Receivers
		default:
			continue
		}
		
		for address, count := range walletMap {
			aggregates[address] += count
			if bucket.Timestamp.After(lastActivity[address]) {
				lastActivity[address] = bucket.Timestamp
			}
		}
	}
	
	// Convert to slice and sort by count
	wallets := make([]WalletStats, 0, len(aggregates))
	for address, count := range aggregates {
		activity := lastActivity[address]
		if activity.IsZero() {
			activity = time.Now()
		}
		
		// Format address using chain context
		displayAddress := address // fallback to canonical
		if chainInfo, exists := c.addressChainInfo[address]; exists {
			displayAddress = c.addressFormatter.FormatAddress(address, chainInfo.RpcType, chainInfo.AddrPrefix)
		}
		
		wallets = append(wallets, WalletStats{
			Address:        address,
			DisplayAddress: displayAddress,
			Count:          count,
			LastActivity:   activity.Format(time.RFC3339),
		})
	}
	
	return c.sortAndLimitWallets(wallets, limit)
}

// generateTimeScaleData generates time scale data for any data type, eliminating 4 duplicate methods
func (c *Collector) generateTimeScaleData(dataType string, getFn func(time.Time, int) interface{}) map[string]interface{} {
	result := make(map[string]interface{})
	now := time.Now()
	limit := c.config.TopItemsTimeScale
	
	// Calculate accurate data for the most frequently used timeframes
	oneMinuteData := getFn(now.Add(-time.Minute), limit)
	oneHourData := getFn(now.Add(-time.Hour), limit)
	oneDayData := getFn(now.Add(-24*time.Hour), limit)
	
	result["1m"] = oneMinuteData
	result["1h"] = oneHourData
	result["1d"] = oneDayData
	// Use 1d data for longer timeframes to avoid expensive bucket aggregations
	result["7d"] = oneDayData   // Reasonable approximation for UI responsiveness
	result["14d"] = oneDayData  // Reasonable approximation for UI responsiveness
	result["30d"] = oneDayData  // Reasonable approximation for UI responsiveness
	
	return result
}

// aggregateAssetStats consolidates asset aggregation logic used in multiple places
func (c *Collector) aggregateAssetStats(buckets []*StatsBucket, denomToSymbol map[string]string) map[string]*AssetStats {
	assetAggregates := make(map[string]*AssetStats)
	
	for _, bucket := range buckets {
		for symbol, asset := range bucket.Assets {
			if existing, exists := assetAggregates[symbol]; exists {
				c.mergeAssetStats(existing, asset)
			} else {
				assetAggregates[symbol] = c.cloneAssetStats(asset)
			}
		}
	}
	
	// Apply symbol mapping
	for denom, asset := range assetAggregates {
		if mappedSymbol, exists := denomToSymbol[denom]; exists && mappedSymbol != "" {
			asset.AssetSymbol = mappedSymbol
			asset.AssetName = mappedSymbol
		}
		c.finalizeAssetStats(asset)
	}
	
	return assetAggregates
}

// mergeAssetStats merges one asset into another, consolidating HLL and route logic
func (c *Collector) mergeAssetStats(existing, new *AssetStats) {
	existing.TransferCount += new.TransferCount
	existing.TotalVolume += new.TotalVolume
	if new.LargestTransfer > existing.LargestTransfer {
		existing.LargestTransfer = new.LargestTransfer
	}
	if new.LastActivity > existing.LastActivity {
		existing.LastActivity = new.LastActivity
	}
	
	// Merge HLL sketches
	c.mergeHLLSafely(&existing.HoldersHLL, new.HoldersHLL, "asset")
	
	// Merge routes
	if existing.Routes == nil {
		existing.Routes = make(map[string]*AssetRouteStats)
	}
	for routeKey, route := range new.Routes {
		c.mergeOrSetRoute(existing.Routes, routeKey, route)
	}
}

// cloneAssetStats creates a deep copy of AssetStats
func (c *Collector) cloneAssetStats(asset *AssetStats) *AssetStats {
	newAsset := &AssetStats{
		AssetSymbol:     asset.AssetSymbol,
		AssetName:       asset.AssetName,
		TransferCount:   asset.TransferCount,
		TotalVolume:     asset.TotalVolume,
		LargestTransfer: asset.LargestTransfer,
		AverageAmount:   asset.AverageAmount,
		LastActivity:    asset.LastActivity,
		Routes:          make(map[string]*AssetRouteStats),
		HoldersHLL:      hllManager.getOptimizedHLL("asset"),
	}
	
	// Copy HLL data if available
	if asset.HoldersHLL != nil {
		newAsset.HoldersHLL.Merge(asset.HoldersHLL)
	}
	
	// Copy routes
	for routeKey, route := range asset.Routes {
		newAsset.Routes[routeKey] = c.cloneAssetRoute(route)
	}
	
	return newAsset
}

// finalizeAssetStats calculates derived fields for an asset
func (c *Collector) finalizeAssetStats(asset *AssetStats) {
	// Calculate average amount
	if asset.TransferCount > 0 {
		asset.AverageAmount = asset.TotalVolume / float64(asset.TransferCount)
	}
	
	// Calculate unique holders from HLL
	if asset.HoldersHLL != nil {
		asset.UniqueHolders = int(asset.HoldersHLL.Estimate())
	}
	
	// Calculate top routes
	asset.TopRoutes = c.calculateTopRoutes(asset.Routes, asset.TotalVolume, 5)
}

// calculateTopRoutes converts route map to sorted top routes
func (c *Collector) calculateTopRoutes(routes map[string]*AssetRouteStats, totalVolume float64, limit int) []AssetRouteStats {
	routeSlice := make([]AssetRouteStats, 0, len(routes))
	for _, route := range routes {
		// Calculate percentage of asset's total volume
		percentage := float64(0)
		if totalVolume > 0 {
			percentage = (route.Volume / totalVolume) * 100
		}
		route.Percentage = percentage
		routeSlice = append(routeSlice, *route)
	}
	
	// Sort routes by volume (descending) and limit
	sort.Slice(routeSlice, func(i, j int) bool {
		return routeSlice[i].Volume > routeSlice[j].Volume
	})
	
	if len(routeSlice) > limit {
		routeSlice = routeSlice[:limit]
	}
	
	return routeSlice
}

// mergeHLLSafely safely merges HLL sketches with proper synchronization
func (c *Collector) mergeHLLSafely(target **hyperloglog.Sketch, source *hyperloglog.Sketch, hllType string) {
	if source != nil {
		if *target == nil {
			*target = hllManager.getOptimizedHLL(hllType)
		}
		
		// CRITICAL FIX: Create a thread-safe clone to prevent race conditions
		// The race condition occurs because Clone() iterates over internal maps
		// while Insert() operations modify them concurrently.
		
		// Use MarshalBinary/UnmarshalBinary to create a safe copy
		// This avoids the race condition in the Clone() method
		sourceBytes, err := source.MarshalBinary()
		if err == nil {
			sourceClone := hllManager.getOptimizedHLL(hllType)
			err = sourceClone.UnmarshalBinary(sourceBytes)
			if err == nil {
				// Now safely merge the clone with the target
				(*target).Merge(sourceClone)
				return
			}
		}
		
		// Fallback: if marshal/unmarshal fails, log the error and use direct merge
		// This is potentially unsafe but better than silent failure
		utils.LogWarn("stats.hll", "Failed to safely clone HLL sketch, using direct merge: %v", err)
		(*target).Merge(source)
	}
}

// mergeHLLSketchSafe safely merges one HLL sketch into another to prevent race conditions
func (c *Collector) mergeHLLSketchSafe(target *hyperloglog.Sketch, source *hyperloglog.Sketch) {
	if source == nil || target == nil {
		return
	}
	
	// Use the same safe cloning approach as mergeHLLSafely
	sourceBytes, err := source.MarshalBinary()
	if err == nil {
		sourceClone := hllManager.getOptimizedHLL("bucket") // Use bucket type for most operations
		err = sourceClone.UnmarshalBinary(sourceBytes)
		if err == nil {
			target.Merge(sourceClone)
			return
		}
	}
	
	// Fallback to direct merge if safe clone fails
	utils.LogWarn("stats.hll", "Failed to safely clone HLL sketch in mergeHLLSketchSafe, using direct merge: %v", err)
	target.Merge(source)
}

// mergeOrSetRoute merges or sets a route in the route map
func (c *Collector) mergeOrSetRoute(routes map[string]*AssetRouteStats, routeKey string, newRoute *AssetRouteStats) {
	if existingRoute, exists := routes[routeKey]; exists {
		existingRoute.Count += newRoute.Count
		existingRoute.Volume += newRoute.Volume
		if newRoute.LastActivity > existingRoute.LastActivity {
			existingRoute.LastActivity = newRoute.LastActivity
		}
	} else {
		routes[routeKey] = c.cloneAssetRoute(newRoute)
	}
}

// cloneAssetRoute creates a copy of AssetRouteStats
func (c *Collector) cloneAssetRoute(route *AssetRouteStats) *AssetRouteStats {
	return &AssetRouteStats{
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

// sortAndLimitWallets sorts wallets by count and applies limit
func (c *Collector) sortAndLimitWallets(wallets []WalletStats, limit int) []WalletStats {
	sort.Slice(wallets, func(i, j int) bool {
		return wallets[i].Count > wallets[j].Count
	})
	
	if len(wallets) > limit {
		wallets = wallets[:limit]
	}
	
	return wallets
}

// sortAndLimitAssets sorts assets by transfer count and applies limit
func (c *Collector) sortAndLimitAssets(assets []AssetStats, limit int) []AssetStats {
	sort.Slice(assets, func(i, j int) bool {
		return assets[i].TransferCount > assets[j].TransferCount
	})
	
	if len(assets) > limit {
		assets = assets[:limit]
	}
	
	return assets
}

// convertAssetsToSlice converts asset map to sorted slice
func (c *Collector) convertAssetsToSlice(assetAggregates map[string]*AssetStats, limit int) []AssetStats {
	assets := make([]AssetStats, 0, len(assetAggregates))
	for _, asset := range assetAggregates {
		assets = append(assets, *asset)
	}
	return c.sortAndLimitAssets(assets, limit)
}

// aggregateChainAssets consolidates chain asset aggregation logic
func (c *Collector) aggregateChainAssets(buckets []*StatsBucket) map[string]map[string]*ChainAssetStats {
	chainAssetAggregates := make(map[string]map[string]*ChainAssetStats)
	
	for _, bucket := range buckets {
		for chainID, assets := range bucket.ChainAssets {
			if chainAssetAggregates[chainID] == nil {
				chainAssetAggregates[chainID] = make(map[string]*ChainAssetStats)
			}
			for symbol, asset := range assets {
				if existing, exists := chainAssetAggregates[chainID][symbol]; exists {
					c.mergeChainAssetStats(existing, asset)
				} else {
					chainAssetAggregates[chainID][symbol] = c.cloneChainAssetStats(asset)
				}
			}
		}
	}
	
	return chainAssetAggregates
}

// mergeChainAssetStats merges chain asset statistics
func (c *Collector) mergeChainAssetStats(existing, new *ChainAssetStats) {
	existing.OutgoingCount += new.OutgoingCount
	existing.IncomingCount += new.IncomingCount
	existing.TotalVolume += new.TotalVolume
	existing.NetFlow = existing.IncomingCount - existing.OutgoingCount
	if existing.OutgoingCount+existing.IncomingCount > 0 {
		existing.AverageAmount = existing.TotalVolume / float64(existing.OutgoingCount+existing.IncomingCount)
	}
	if new.LastActivity > existing.LastActivity {
		existing.LastActivity = new.LastActivity
	}
	// Merge HLL sketches
	c.mergeHLLSafely(&existing.HoldersHLL, new.HoldersHLL, "asset")
}

// cloneChainAssetStats creates a copy of ChainAssetStats
func (c *Collector) cloneChainAssetStats(asset *ChainAssetStats) *ChainAssetStats {
	newAsset := &ChainAssetStats{
		AssetSymbol:   asset.AssetSymbol,
		AssetName:     asset.AssetName,
		OutgoingCount: asset.OutgoingCount,
		IncomingCount: asset.IncomingCount,
		NetFlow:       asset.NetFlow,
		TotalVolume:   asset.TotalVolume,
		AverageAmount: asset.AverageAmount,
		LastActivity:  asset.LastActivity,
		HoldersHLL:    hllManager.getOptimizedHLL("asset"),
	}
	if asset.HoldersHLL != nil {
		newAsset.HoldersHLL.Merge(asset.HoldersHLL)
	}
	return newAsset
}