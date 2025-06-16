package stats

import (
	"sync"
	"time"
	"github.com/axiomhq/hyperloglog"
)

// RouteStats represents statistics for a specific route
type RouteStats struct {
	Count        int64     `json:"count"`
	CountChange  float64   `json:"countChange"`
	FromChain    string    `json:"fromChain"`
	ToChain      string    `json:"toChain"`
	FromName     string    `json:"fromName"`
	ToName       string    `json:"toName"`
	Route        string    `json:"route"`
	LastActivity time.Time `json:"lastActivity"`
}

// WalletStats represents statistics for a wallet address
type WalletStats struct {
	Count        int64     `json:"count"`
	Address      string    `json:"address"`
	LastActivity time.Time `json:"lastActivity"`
}

// TransferRates represents current transfer rate statistics
type TransferRates struct {
	TxPerMinute         float64          `json:"txPerMinute"`
	TxPerHour           float64          `json:"txPerHour"`
	TxPerDay            float64          `json:"txPerDay"`
	TxPer7Days          float64          `json:"txPer7Days"`
	TxPer14Days         float64          `json:"txPer14Days"`
	TxPer30Days         float64          `json:"txPer30Days"`
	// Percentage changes from previous periods
	TxPerMinuteChange   float64          `json:"txPerMinuteChange"`
	TxPerHourChange     float64          `json:"txPerHourChange"`
	TxPerDayChange      float64          `json:"txPerDayChange"`
	TxPer7DaysChange    float64          `json:"txPer7DaysChange"`
	TxPer14DaysChange   float64          `json:"txPer14DaysChange"`
	TxPer30DaysChange   float64          `json:"txPer30DaysChange"`

	TotalTracked        int64            `json:"totalTracked"`
	DataAvailability    DataAvailability `json:"dataAvailability"`
	ServerUptimeSeconds float64          `json:"serverUptimeSeconds"`
}

// ActiveWalletRates represents active wallet rate statistics
type ActiveWalletRates struct {
	SendersLastMin      int `json:"sendersLastMin"`
	SendersLastHour     int `json:"sendersLastHour"`
	SendersLastDay      int `json:"sendersLastDay"`
	SendersLast7d       int `json:"sendersLast7d"`
	SendersLast14d      int `json:"sendersLast14d"`
	SendersLast30d      int `json:"sendersLast30d"`
	// Percentage changes for senders
	SendersLastMinChange    float64 `json:"sendersLastMinChange"`
	SendersLastHourChange   float64 `json:"sendersLastHourChange"`
	SendersLastDayChange    float64 `json:"sendersLastDayChange"`
	SendersLast7dChange     float64 `json:"sendersLast7dChange"`
	SendersLast14dChange    float64 `json:"sendersLast14dChange"`
	SendersLast30dChange    float64 `json:"sendersLast30dChange"`
	ReceiversLastMin    int `json:"receiversLastMin"`
	ReceiversLastHour   int `json:"receiversLastHour"`
	ReceiversLastDay    int `json:"receiversLastDay"`
	ReceiversLast7d     int `json:"receiversLast7d"`
	ReceiversLast14d    int `json:"receiversLast14d"`
	ReceiversLast30d    int `json:"receiversLast30d"`
	// Percentage changes for receivers
	ReceiversLastMinChange  float64 `json:"receiversLastMinChange"`
	ReceiversLastHourChange float64 `json:"receiversLastHourChange"`
	ReceiversLastDayChange  float64 `json:"receiversLastDayChange"`
	ReceiversLast7dChange   float64 `json:"receiversLast7dChange"`
	ReceiversLast14dChange  float64 `json:"receiversLast14dChange"`
	ReceiversLast30dChange  float64 `json:"receiversLast30dChange"`
	// Totals
	TotalLastMin        int `json:"totalLastMin"`
	TotalLastHour       int `json:"totalLastHour"`
	TotalLastDay        int `json:"totalLastDay"`
	TotalLast7d         int `json:"totalLast7d"`
	TotalLast14d        int `json:"totalLast14d"`
	TotalLast30d        int `json:"totalLast30d"`
	// Percentage changes for total active wallets
	TotalLastMinChange      float64 `json:"totalLastMinChange"`
	TotalLastHourChange     float64 `json:"totalLastHourChange"`
	TotalLastDayChange      float64 `json:"totalLastDayChange"`
	TotalLast7dChange       float64 `json:"totalLast7dChange"`
	TotalLast14dChange      float64 `json:"totalLast14dChange"`
	TotalLast30dChange      float64 `json:"totalLast30dChange"`
	
	UniqueSendersTotal  int `json:"uniqueSendersTotal"`
	UniqueReceiversTotal int `json:"uniqueReceiversTotal"`
	UniqueTotalWallets  int `json:"uniqueTotalWallets"`
	ServerUptimeSeconds float64 `json:"serverUptimeSeconds"`
}

// ChartData represents basic chart data structure
type ChartData struct {
	CurrentRates    TransferRates    `json:"currentRates"`
	PopularRoutes   []*RouteStats    `json:"popularRoutes"`
	ActiveSenders   []*WalletStats   `json:"activeSenders"`
	ActiveReceivers []*WalletStats   `json:"activeReceivers"`
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

// EnhancedChartData represents enhanced chart data with additional fields
type EnhancedChartData struct {
	CurrentRates             TransferRates                    `json:"currentRates"`
	ActiveWalletRates        ActiveWalletRates                `json:"activeWalletRates"`
	PopularRoutes            []*RouteStats                    `json:"popularRoutes"`
	PopularRoutesTimeScale   map[string][]*RouteStats         `json:"popularRoutesTimeScale"`
	ActiveSenders            []*WalletStats                   `json:"activeSenders"`
	ActiveReceivers          []*WalletStats                   `json:"activeReceivers"`
	ActiveSendersTimeScale   map[string][]*WalletStats        `json:"activeSendersTimeScale"`
	ActiveReceiversTimeScale map[string][]*WalletStats        `json:"activeReceiversTimeScale"`
	ChainFlowData            ChainFlowData                    `json:"chainFlowData"`
	DataAvailability         DataAvailability                 `json:"dataAvailability"`
}

// Time-based data structures with hierarchical granularity
type WalletActivityBucket struct {
	Timestamp       time.Time           `json:"timestamp"`
	SenderCounts    map[string]int64    `json:"senderCounts"`    // wallet -> transfer count as sender
	ReceiverCounts  map[string]int64    `json:"receiverCounts"`  // wallet -> transfer count as receiver
	Granularity     string              `json:"granularity"`     // "10s", "1m", or "1h"
	
	// HyperLogLog for unique counting (not serialized to JSON)
	SenderHLL       *hyperloglog.Sketch `json:"-"`
	ReceiverHLL     *hyperloglog.Sketch `json:"-"`
	CombinedHLL     *hyperloglog.Sketch `json:"-"` // For total unique wallets
}

type RouteActivityBucket struct {
	Timestamp   time.Time         `json:"timestamp"`
	RouteCounts map[string]int64  `json:"routeCounts"`
	Granularity string            `json:"granularity"`     // "10s", "1m", or "1h"
}

type TimeBucket struct {
	Timestamp     time.Time `json:"timestamp"`
	TransferCount int64     `json:"transferCount"`
	SenderCount   int64     `json:"senderCount"`
	ReceiverCount int64     `json:"receiverCount"`
	TotalCount    int64     `json:"totalCount"`
	Granularity   string    `json:"granularity"`     // "10s", "1m", or "1h"
}

// Individual transfer records for precise time calculations (memory optimized)
type TransferRecord struct {
	Timestamp time.Time `json:"timestamp"`
	Sender    string    `json:"sender"`
	Receiver  string    `json:"receiver"`
	RouteKey  string    `json:"routeKey"`
}

// PreviousPeriodData stores data from previous periods for comparison
type PreviousPeriodData struct {
	TransferRates     TransferRates            `json:"transferRates"`
	ActiveWalletRates ActiveWalletRates        `json:"activeWalletRates"`
	PopularRoutes     map[string][]*RouteStats `json:"popularRoutes"` // timeframe -> routes
	ChainFlowData     ChainFlowData            `json:"chainFlowData"`  // chain flow data for percentage calculations
	Timestamp         time.Time                `json:"timestamp"`
	WalletData        *WalletData              `json:"walletData"`
	AssetVolumeData   *AssetVolumeData         `json:"assetVolumeData"`
}

// PeriodCounts holds all counts for a specific time period
type PeriodCounts struct {
	Transfers int64
	Senders   int
	Receivers int
	Total     int
}

// HLLPool manages reusable HyperLogLog instances to reduce allocations
type HLLPool struct {
	senders   *hyperloglog.Sketch
	receivers *hyperloglog.Sketch
	combined  *hyperloglog.Sketch
}

// WalletActivityType defines the type of wallet activity to track
type WalletActivityType int

const (
	SenderActivity WalletActivityType = iota
	ReceiverActivity
)

// Chain Flow Types
type ChainFlowStats struct {
	UniversalChainID string    `json:"universal_chain_id"`
	ChainName        string    `json:"chainName"`
	OutgoingCount    int64     `json:"outgoingCount"`
	IncomingCount    int64     `json:"incomingCount"`
	NetFlow          int64     `json:"netFlow"`          // incoming - outgoing
	OutgoingChange   float64   `json:"outgoingChange"`   // percentage change
	IncomingChange   float64   `json:"incomingChange"`   // percentage change
	NetFlowChange    float64   `json:"netFlowChange"`    // percentage change
	LastActivity     time.Time `json:"lastActivity"`
}

type ChainFlowData struct {
	Chains           []*ChainFlowStats                `json:"chains"`
	ChainFlowTimeScale map[string][]*ChainFlowStats   `json:"chainFlowTimeScale"` // timeframe -> chain stats
	TotalOutgoing    int64                           `json:"totalOutgoing"`
	TotalIncoming    int64                           `json:"totalIncoming"`
	ServerUptimeSeconds float64                      `json:"serverUptimeSeconds"`
}

type ChainFlowBucket struct {
	Timestamp     time.Time                 `json:"timestamp"`
	OutgoingCounts map[string]int64         `json:"outgoingCounts"` // chainID -> outgoing transfer count
	IncomingCounts map[string]int64         `json:"incomingCounts"` // chainID -> incoming transfer count
	Granularity   string                   `json:"granularity"`    // "10s", "1m", or "1h"
}

// Asset Volume Types
type AssetVolumeStats struct {
	AssetSymbol      string    `json:"assetSymbol"`
	AssetName        string    `json:"assetName"`
	TransferCount    int64     `json:"transferCount"`
	TotalVolume      float64   `json:"totalVolume"`      // in token units (adjusted for decimals)
	AverageAmount    float64   `json:"averageAmount"`    // average transfer size
	LargestTransfer  float64   `json:"largestTransfer"`  // biggest single transfer
	UniqueHolders    int       `json:"uniqueHolders"`    // unique senders + receivers
	VolumeChange     float64   `json:"volumeChange"`     // percentage change
	CountChange      float64   `json:"countChange"`      // percentage change in transfer count
	PopularityRank   int       `json:"popularityRank"`   // rank by transfer count
	VolumeRank       int       `json:"volumeRank"`       // rank by total volume
	LastActivity     time.Time `json:"lastActivity"`
	// Per-asset chain flow analytics
	ChainFlows       map[string]*AssetChainFlowData `json:"chainFlows"`       // flow between specific chains for this asset
	TopSourceChains  []*AssetChainStats `json:"topSourceChains"`  // chains this asset flows FROM most
	TopDestChains    []*AssetChainStats `json:"topDestChains"`    // chains this asset flows TO most
}

type AssetVolumeData struct {
	Assets              []*AssetVolumeStats                `json:"assets"`
	AssetVolumeTimeScale map[string][]*AssetVolumeStats    `json:"assetVolumeTimeScale"` // timeframe -> asset stats
	TotalAssets         int                               `json:"totalAssets"`
	TotalVolume         float64                           `json:"totalVolume"`          // across all assets
	TotalTransfers      int64                             `json:"totalTransfers"`       // across all assets
	ServerUptimeSeconds float64                           `json:"serverUptimeSeconds"`
}

type AssetVolumeBucket struct {
	Timestamp     time.Time                 `json:"timestamp"`
	AssetVolumes  map[string]*AssetBucketData `json:"assetVolumes"`  // assetSymbol -> volume data
	Granularity   string                   `json:"granularity"`    // "10s", "1m", or "1h"
}

type AssetBucketData struct {
	TransferCount     int64              `json:"transferCount"`
	TotalVolume       float64            `json:"totalVolume"`
	LargestTransfer   float64            `json:"largestTransfer"`
	UniqueHolderCount int64              `json:"uniqueHolderCount"`  // count of unique wallet addresses in this bucket
	// Per-asset chain flow tracking
	ChainFlows        map[string]*AssetChainFlowData `json:"chainFlows"` // "sourceChain->destChain" -> flow data
	
	// HyperLogLog for unique holder counting (not serialized to JSON)
	UniqueHoldersHLL  *hyperloglog.Sketch `json:"-"`
}

type AssetChainFlow struct {
	SourceChain      string    `json:"sourceChain"`      // source chain ID
	SourceChainName  string    `json:"sourceChainName"`  // source chain display name
	DestChain        string    `json:"destChain"`        // destination chain ID
	DestChainName    string    `json:"destChainName"`    // destination chain display name
	TransferCount    int64     `json:"transferCount"`    // number of transfers on this route
	TotalVolume      float64   `json:"totalVolume"`      // total volume on this route
	AverageAmount    float64   `json:"averageAmount"`    // average transfer size on this route
	LargestTransfer  float64   `json:"largestTransfer"`  // largest single transfer on this route
	VolumePercentage float64   `json:"volumePercentage"` // percentage of total asset volume
	CountPercentage  float64   `json:"countPercentage"`  // percentage of total asset transfers
	LastActivity     time.Time `json:"lastActivity"`     // last transfer on this route
}

type AssetChainStats struct {
	ChainID          string    `json:"chainId"`          // chain ID
	ChainName        string    `json:"chainName"`        // chain display name
	TransferCount    int64     `json:"transferCount"`    // total transfers involving this chain
	TotalVolume      float64   `json:"totalVolume"`      // total volume involving this chain
	VolumePercentage float64   `json:"volumePercentage"` // percentage of total asset volume
	CountPercentage  float64   `json:"countPercentage"`  // percentage of total asset transfers
	LastActivity     time.Time `json:"lastActivity"`     // last activity on this chain
}

type AssetChainFlowData struct {
	TransferCount   int64     `json:"transferCount"`
	TotalVolume     float64   `json:"totalVolume"`
	LargestTransfer float64   `json:"largestTransfer"`
	LastActivity    time.Time `json:"lastActivity"`
}

type WalletData struct {
	Wallets             []*WalletStats                 `json:"wallets"`
	WalletActivityTimeScale map[string][]*WalletActivityBucket `json:"walletActivityTimeScale"` // timeframe -> wallet activity
	TotalWallets          int                              `json:"totalWallets"`
	ServerUptimeSeconds   float64                          `json:"serverUptimeSeconds"`
}

type BroadcastData struct {
	WalletData      *WalletData      `json:"walletData"`
	ChainFlowData   *ChainFlowData   `json:"chainFlowData"`
	AssetVolumeData *AssetVolumeData `json:"assetVolumeData"`
	ServerUptime    float64          `json:"serverUptime"`
	Timestamp       time.Time        `json:"timestamp"`
}

// EnhancedCollector provides enhanced statistics collection
type EnhancedCollector struct {
	config           StatsConfig                     // Configuration for stats collection
	mu               sync.RWMutex
	routeStats       map[string]*RouteStats
	senderStats      map[string]*WalletStats
	receiverStats    map[string]*WalletStats
	chainFlowStats   map[string]*ChainFlowStats      // chainID -> flow stats
	
	// Time-based buckets for different granularities
	timeBuckets      map[time.Time]*TimeBucket       // 10-second buckets
	walletActivityBuckets map[time.Time]*WalletActivityBucket // wallet activity tracking
	routeActivityBuckets  map[time.Time]*RouteActivityBucket  // route activity tracking
	chainFlowBuckets      map[time.Time]*ChainFlowBucket      // chain flow tracking
	
	// Asset volume buckets for different time scales
	assetVolumeBuckets10s map[time.Time]*AssetVolumeBucket // 10-second granularity
	assetVolumeBuckets1m  map[time.Time]*AssetVolumeBucket // 1-minute aggregated
	assetVolumeBuckets1h  map[time.Time]*AssetVolumeBucket // 1-hour aggregated
	
	// Asset and chain mappings
	denomToSymbol    map[string]string               // denomination -> symbol mapping
	
	// HyperLogLog for unique counting
	globalSendersHLL   *hyperloglog.Sketch
	globalReceiversHLL *hyperloglog.Sketch
	globalWalletsHLL   *hyperloglog.Sketch
	hllPool            *HLLPool                      // Pool for reusable HLL sketches
	
	// Previous period data for percentage change calculations
	previousMinute   *PreviousPeriodData
	previousHour     *PreviousPeriodData
	previousDay      *PreviousPeriodData
	previous7Days    *PreviousPeriodData
	previous14Days   *PreviousPeriodData
	previous30Days   *PreviousPeriodData
	
	// Transfer counting
	totalTransfers   int64
	
	// Time tracking for data availability and cleanup
	startTime        time.Time
	lastUpdateTime   time.Time
	lastMinuteCheck  time.Time
	lastHourCheck    time.Time
	lastDayCheck     time.Time
	last7DaysCheck   time.Time
	last14DaysCheck  time.Time
	last30DaysCheck  time.Time
} 