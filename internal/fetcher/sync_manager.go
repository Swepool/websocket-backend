package fetcher

import (
	"context"
	"time"
	"websocket-backend/config"
	"websocket-backend/internal/core"
	"websocket-backend/internal/utils"
)

// ChainProvider interface defines what the sync manager needs for chain data
type ChainProvider interface {
	GetChains() ([]interface{}, error)
}

// SyncManager coordinates both forward and backward fetchers for complete sync coverage
type SyncManager struct {
	fetcherConfig        config.FetcherConfig
	backwardFetcherConfig config.BackwardFetcherConfig
	channels             *core.Channels
	chainProvider        ChainProvider
	dbWriter             DatabaseWriter
	forwardFetcher       *Fetcher
	backwardFetcher      *BackwardFetcher
	
	// Configuration
	gapThreshold time.Duration // If gap > threshold, start backward sync
}

// NewSyncManager creates a new sync manager
func NewSyncManager(fetcherCfg config.FetcherConfig, backwardFetcherCfg config.BackwardFetcherConfig, channels *core.Channels, chainProvider ChainProvider, dbWriter DatabaseWriter) *SyncManager {
	return &SyncManager{
		fetcherConfig:         fetcherCfg,
		backwardFetcherConfig: backwardFetcherCfg,
		channels:              channels,
		chainProvider:         chainProvider,
		dbWriter:              dbWriter,
		gapThreshold:          6 * time.Hour, // If oldest transfer > 6h old, start backward sync
	}
}

// SetGapThreshold configures the gap detection threshold
func (sm *SyncManager) SetGapThreshold(threshold time.Duration) {
	sm.gapThreshold = threshold
}

// Start begins coordinated sync operation
func (sm *SyncManager) Start(ctx context.Context) {
	utils.LogInfo("SYNC_MANAGER", "Starting coordinated sync system")
	utils.LogInfo("SYNC_MANAGER", "🔍 DEBUG: Backward fetcher config - Enabled: %v, MaxDepthDays: %d", sm.backwardFetcherConfig.Enabled, sm.backwardFetcherConfig.MaxDepthDays)
	
	// Initialize forward fetcher
	forwardFetcher := NewFetcher(sm.fetcherConfig, sm.channels, sm.dbWriter)
	sm.forwardFetcher = forwardFetcher
	
	// Check if we need backward sync
	needsBackwardSync, reason := sm.shouldStartBackwardSync()
	utils.LogInfo("SYNC_MANAGER", "🔍 DEBUG: shouldStartBackwardSync() = %v, reason: %s", needsBackwardSync, reason)
	
	if needsBackwardSync && sm.backwardFetcherConfig.Enabled {
		utils.LogInfo("SYNC_MANAGER", "🚀 STARTING BIDIRECTIONAL SYNC: %s", reason)
		utils.LogInfo("SYNC_MANAGER", "   🔴 Forward fetcher: NEW transfers → store + broadcast")
		utils.LogInfo("SYNC_MANAGER", "   🔵 Backward fetcher: HISTORICAL transfers (max %d days) → store only", sm.backwardFetcherConfig.MaxDepthDays)
		
		// Initialize backward fetcher with its own config
		sm.backwardFetcher = NewBackwardFetcher(sm.backwardFetcherConfig, sm.channels, sm.dbWriter)
		
		// Start both fetchers concurrently
		go sm.forwardFetcher.Start(ctx)
		go sm.backwardFetcher.Start(ctx)
		
		utils.LogInfo("SYNC_MANAGER", "✅ Both forward and backward sync started")
	} else {
		if !sm.backwardFetcherConfig.Enabled {
			utils.LogInfo("SYNC_MANAGER", "🚀 FORWARD SYNC ONLY: Backward sync disabled in configuration")
		} else {
			utils.LogInfo("SYNC_MANAGER", "🚀 FORWARD SYNC ONLY: %s", reason)
		}
		utils.LogInfo("SYNC_MANAGER", "   🔴 Forward fetcher: NEW transfers → store + broadcast")
		
		// Start only forward fetcher
		go sm.forwardFetcher.Start(ctx)
		utils.LogInfo("SYNC_MANAGER", "✅ Forward sync started")
	}
	
	// Monitor sync progress
	sm.monitorProgress(ctx)
}

// shouldStartBackwardSync determines if backward sync is needed
func (sm *SyncManager) shouldStartBackwardSync() (bool, string) {
	// Check if database has any transfers
	count, err := sm.dbWriter.GetTransferCount()
	if err != nil {
		utils.LogError("SYNC_MANAGER", "🔍 DEBUG: Failed to get transfer count: %v", err)
		return false, "failed to check database state"
	}
	
	utils.LogInfo("SYNC_MANAGER", "🔍 DEBUG: Database has %d transfers", count)
	
	if count == 0 {
		utils.LogInfo("SYNC_MANAGER", "🔍 DEBUG: Database is empty - no backward sync needed")
		return false, "database is empty - no backward sync needed"
	}
	
	// Get earliest transfer timestamp to check for gaps
	earliestSortOrder, err := sm.dbWriter.GetEarliestSortOrder()
	if err != nil {
		utils.LogError("SYNC_MANAGER", "🔍 DEBUG: Failed to get earliest sort order: %v", err)
		return false, "failed to get earliest sort order"
	}
	
	utils.LogInfo("SYNC_MANAGER", "🔍 DEBUG: Earliest sort order: %s", earliestSortOrder)
	
	if earliestSortOrder == "" {
		utils.LogWarn("SYNC_MANAGER", "🔍 DEBUG: No sort order available")
		return false, "no sort order available"
	}
	
	// Parse timestamp from sort order (sort orders are typically timestamp-based)
	// For now, we'll use a simple heuristic: if we have less than expected transfers for the time period
	
	// If database has transfers but they're sparse, we might need backward sync
	// This is a simplified check - in production you'd want more sophisticated gap detection
	
	// For now, always start backward sync if enabled and we have existing transfers
	// This ensures we fill any historical gaps
	utils.LogInfo("SYNC_MANAGER", "🔍 DEBUG: Conditions met - will start backward sync to fill gaps")
	return true, "has existing transfers - filling historical gaps"
}

// monitorProgress monitors the sync progress and provides status updates
func (sm *SyncManager) monitorProgress(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second) // Status update every 30 seconds
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			utils.LogInfo("SYNC_MANAGER", "Stopping sync progress monitoring")
			return
			
		case <-ticker.C:
			sm.logSyncStatus()
		}
	}
}

// logSyncStatus logs current sync status
func (sm *SyncManager) logSyncStatus() {
	// Get database stats
	count, err := sm.dbWriter.GetTransferCount()
	if err != nil {
		count = 0
	}
	
	var status string
	var emoji string
	if sm.backwardFetcher != nil && sm.backwardFetcher.IsRunning() {
		status = "🔴 Forward (live) + 🔵 Backward (historical)"
		emoji = "⚡"
	} else {
		status = "🔴 Forward (live) only"
		emoji = "📡"
	}
	
	utils.LogInfo("SYNC_MANAGER", "%s BIDIRECTIONAL SYNC: %s | Database: %d transfers", emoji, status, count)
}

// Stop stops all sync operations
func (sm *SyncManager) Stop() {
	utils.LogInfo("SYNC_MANAGER", "Stopping all sync operations")
	
	if sm.backwardFetcher != nil {
		sm.backwardFetcher.Stop()
	}
	
	// Note: Forward fetcher stops when context is cancelled
}

// GetStatus returns current sync status
func (sm *SyncManager) GetStatus() map[string]interface{} {
	count, _ := sm.dbWriter.GetTransferCount()
	
	status := map[string]interface{}{
		"total_transfers":         count,
		"forward_active":          true, // Always active
		"backward_active":         sm.backwardFetcher != nil && sm.backwardFetcher.IsRunning(),
		"backward_enabled":        sm.backwardFetcherConfig.Enabled,
		"backward_max_depth_days": sm.backwardFetcherConfig.MaxDepthDays,
		"gap_threshold_hours":     sm.gapThreshold.Hours(),
		"backward_config": map[string]interface{}{
			"batch_size":              sm.backwardFetcherConfig.BatchSize,
			"http_timeout_seconds":    sm.backwardFetcherConfig.HTTPTimeout.Seconds(),
			"retry_delay_ms":          sm.backwardFetcherConfig.RetryDelay.Milliseconds(),
			"rate_limit_delay_ms":     sm.backwardFetcherConfig.RateLimitDelay.Milliseconds(),
			"backpressure_timeout_ms": sm.backwardFetcherConfig.BackpressureTimeoutMs,
		},
	}
	
	return status
} 