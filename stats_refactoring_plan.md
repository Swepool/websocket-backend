# Stats Package Refactoring Plan

## Current State
- **File**: `internal/stats/stats.go` 
- **Size**: 2,469 lines
- **Issues**: Single massive file, hard to navigate, mixed concerns

## Proposed File Structure

### 1. **`collector.go`** - Core Collector & Main API
**Lines**: ~300-400
**Purpose**: Main collector struct, constructor, public API methods
```go
// Types & Constructor
type EnhancedCollector struct { ... }
func NewEnhancedCollector() *EnhancedCollector

// Main Public API
func (ec *EnhancedCollector) UpdateTransferStats(transfers []models.Transfer)
func (ec *EnhancedCollector) GetChartData() ChartData
func (ec *EnhancedCollector) GetEnhancedChartData() EnhancedChartData
func (ec *EnhancedCollector) GetBroadcastData() interface{}

// Core utilities
func (ec *EnhancedCollector) cleanupOldTimeData(cutoff time.Time)
func (ec *EnhancedCollector) calculateDataAvailability() DataAvailability
```

### 2. **`types.go`** - All Data Structures & Types
**Lines**: ~200-250
**Purpose**: All struct definitions, constants, enums
```go
// Core stats types
type RouteStats struct { ... }
type WalletStats struct { ... }
type TransferRates struct { ... }
type ActiveWalletRates struct { ... }

// Chart data types
type ChartData struct { ... }
type EnhancedChartData struct { ... }
type DataAvailability struct { ... }

// Bucket types
type WalletActivityBucket struct { ... }
type RouteActivityBucket struct { ... }
type TimeBucket struct { ... }

// Utility types
type PeriodCounts struct { ... }
type WalletActivityType int
type HLLPool struct { ... }
```

### 3. **`transfer_rates.go`** - Transfer Rate Calculations
**Lines**: ~200-300
**Purpose**: Transfer counting, rate calculations, period management
```go
// Transfer rate calculations
func (ec *EnhancedCollector) calculateTransferRates() TransferRates

// Period counting (optimized consolidated functions)
func (ec *EnhancedCollector) countAllInPeriod(since time.Time) PeriodCounts
func (ec *EnhancedCollector) countTransfersInPeriod(since time.Time) int64

// Period management
func (ec *EnhancedCollector) checkAndRotatePeriods(now time.Time)
func (ec *EnhancedCollector) rotatePeriod(period string, timestamp time.Time)

// Bucket filtering helpers
func (ec *EnhancedCollector) getTimeBucketsInPeriod(since time.Time) []*TimeBucket
func (ec *EnhancedCollector) getWalletActivityBucketsInPeriod(since time.Time) []*WalletActivityBucket
```

### 4. **`wallet_stats.go`** - Wallet Activity Analytics
**Lines**: ~300-400
**Purpose**: Wallet counting, top wallets, wallet activity tracking
```go
// Wallet rate calculations
func (ec *EnhancedCollector) calculateActiveWalletRates() ActiveWalletRates
func (ec *EnhancedCollector) calculateWalletData() *WalletData

// Unique wallet counting
func (ec *EnhancedCollector) countActiveWalletsInPeriod(since time.Time) int
func (ec *EnhancedCollector) countActiveSendersInPeriod(since time.Time) int
func (ec *EnhancedCollector) countActiveReceiversInPeriod(since time.Time) int
func (ec *EnhancedCollector) countTotalUniqueWallets() int

// Top wallet functions (consolidated)
func (ec *EnhancedCollector) getTopWalletsInPeriod(since time.Time, n int, activityType WalletActivityType) []*WalletStats
func (ec *EnhancedCollector) getTopSenders(n int) []*WalletStats
func (ec *EnhancedCollector) getTopReceivers(n int) []*WalletStats

// Time scale functions
func (ec *EnhancedCollector) getActiveSendersTimeScale() map[string][]*WalletStats
func (ec *EnhancedCollector) getActiveReceiversTimeScale() map[string][]*WalletStats
func (ec *EnhancedCollector) getWalletActivityTimeScale() map[string][]*WalletActivityBucket
```

### 5. **`route_stats.go`** - Route Analytics
**Lines**: ~200-300
**Purpose**: Route tracking, popular routes, route activity
```go
// Route calculations
func (ec *EnhancedCollector) getTopRoutes(n int) []*RouteStats
func (ec *EnhancedCollector) getTopRoutesInPeriod(since time.Time, n int, timeframeKey string) []*RouteStats
func (ec *EnhancedCollector) getPopularRoutesTimeScale() map[string][]*RouteStats

// Route percentage changes
func (ec *EnhancedCollector) calculateRoutePercentageChanges(routeCounts map[string]*RouteStats, timeframeKey string)
```

### 6. **`chain_flow.go`** - Chain Flow Analytics
**Lines**: ~300-400
**Purpose**: Chain flow tracking, cross-chain analytics
```go
// Chain flow types
type ChainFlowStats struct { ... }
type ChainFlowData struct { ... }
type ChainFlowBucket struct { ... }

// Chain flow calculations
func (ec *EnhancedCollector) calculateChainFlowData() ChainFlowData
func (ec *EnhancedCollector) getChainFlowTimeScale() map[string][]*ChainFlowStats
func (ec *EnhancedCollector) getChainFlowInPeriod(since time.Time) []*ChainFlowStats
```

### 7. **`asset_volume.go`** - Asset Volume Analytics
**Lines**: ~600-700
**Purpose**: Asset tracking, volume analytics, asset chain flows
```go
// Asset volume types
type AssetVolumeStats struct { ... }
type AssetVolumeData struct { ... }
type AssetVolumeBucket struct { ... }
type AssetBucketData struct { ... }
type AssetChainFlow struct { ... }
type AssetChainStats struct { ... }
type AssetChainFlowData struct { ... }

// Asset volume calculations
func (ec *EnhancedCollector) calculateAssetVolumeData() *AssetVolumeData
func (ec *EnhancedCollector) getAssetVolumeTimeScale() map[string][]*AssetVolumeStats
func (ec *EnhancedCollector) getAssetVolumeInPeriod(since time.Time) []*AssetVolumeStats

// Asset chain flow analytics
func (ec *EnhancedCollector) calculateAssetChainFlows(...) []*AssetChainFlow
func (ec *EnhancedCollector) calculateTopSourceChains(...) []*AssetChainStats
func (ec *EnhancedCollector) calculateTopDestChains(...) []*AssetChainStats

// Asset utilities
func (ec *EnhancedCollector) calculateAssetPercentageChanges(asset *AssetVolumeStats)
func (ec *EnhancedCollector) findAssetInPreviousData(assetSymbol string, previousData *AssetVolumeData) *AssetVolumeStats
func (ec *EnhancedCollector) extractSymbolFromDenom(canonicalDenom string) string
func (ec *EnhancedCollector) getChainDisplayName(chainID string) string
```

### 8. **`bucket_aggregation.go`** - Bucket Management
**Lines**: ~200-300
**Purpose**: Time bucket aggregation, data compression
```go
// Bucket aggregation
func (ec *EnhancedCollector) aggregateOldBuckets()
func (ec *EnhancedCollector) aggregateBucketsToMinutes(cutoff time.Time)
func (ec *EnhancedCollector) aggregateBucketsToHours(cutoff time.Time)
```

### 9. **`utils.go`** - Shared Utilities
**Lines**: ~100-150
**Purpose**: Common utilities, time helpers, HLL pool
```go
// Time period utilities
var standardTimePeriods = map[string]time.Duration{ ... }
func getTimePeriodsFromNow() map[string]time.Time

// HyperLogLog pool
func NewHLLPool() *HLLPool
func (p *HLLPool) Reset()
func (p *HLLPool) GetSenders() *hyperloglog.Sketch
func (p *HLLPool) GetReceivers() *hyperloglog.Sketch
func (p *HLLPool) GetCombined() *hyperloglog.Sketch

// General utilities
func (ec *EnhancedCollector) calculatePercentageChange(current, previous float64) float64
```

## Benefits of This Structure

### 📁 **Logical Organization**
- Each file has a single, clear responsibility
- Related functionality is grouped together
- Easy to find specific features

### 🔍 **Improved Maintainability**
- Smaller files are easier to understand and modify
- Reduced merge conflicts when multiple developers work on stats
- Clear separation of concerns

### 🚀 **Better Development Experience**
- Faster IDE navigation and search
- Easier to write focused unit tests
- Clearer code review process

### 📊 **File Size Comparison**
| Current | Proposed | Reduction |
|---------|----------|-----------|
| 1 file (2,469 lines) | 9 files (~200-700 lines each) | 70% smaller files |

## Implementation Strategy

### Phase 1: Create New Files
1. Create all 9 new files with proper package declarations
2. Move type definitions to `types.go`
3. Move utility functions to `utils.go`

### Phase 2: Move Core Functions
1. Move collector struct and constructor to `collector.go`
2. Move main API methods to `collector.go`
3. Ensure all imports are correct

### Phase 3: Move Specialized Functions
1. Move transfer rate functions to `transfer_rates.go`
2. Move wallet functions to `wallet_stats.go`
3. Move route functions to `route_stats.go`
4. Move chain flow functions to `chain_flow.go`
5. Move asset volume functions to `asset_volume.go`
6. Move bucket aggregation to `bucket_aggregation.go`

### Phase 4: Cleanup & Testing
1. Remove original `stats.go` file
2. Run tests to ensure everything works
3. Update any external imports if needed

## Backward Compatibility
- All public APIs remain exactly the same
- No changes to external packages required
- Same import path: `websocket-backend/internal/stats`

This refactoring will make the codebase much more maintainable while preserving all existing functionality! 