package fetcher

import (
	"context"
	"time"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/internal/utils"
)

// SyncManager coordinates both forward and backward fetchers for complete sync coverage
type SyncManager struct {
	config           Config
	channels         *channels.Channels
	chainProvider    ChainProvider
	dbWriter         DatabaseWriter
	forwardFetcher   *Fetcher
	backwardFetcher  *BackwardFetcher
	
	// Configuration
	enableBackwardSync bool
	backwardSyncDepth  int  // Days to sync backwards
	gapThreshold       time.Duration // If gap > threshold, start backward sync
}

// NewSyncManager creates a new sync manager
func NewSyncManager(config Config, channels *channels.Channels, chainProvider ChainProvider, dbWriter DatabaseWriter) *SyncManager {
	return &SyncManager{
		config:             config,
		channels:           channels,
		chainProvider:      chainProvider,
		dbWriter:           dbWriter,
		enableBackwardSync: true,
		backwardSyncDepth:  7,                // Default: sync back 7 days
		gapThreshold:       6 * time.Hour,    // If oldest transfer > 6h old, start backward sync
	}
}

// SetBackwardSyncConfig configures backward sync behavior
func (sm *SyncManager) SetBackwardSyncConfig(enabled bool, depthDays int, gapThreshold time.Duration) {
	sm.enableBackwardSync = enabled
	sm.backwardSyncDepth = depthDays
	sm.gapThreshold = gapThreshold
}

// Start begins coordinated sync operation
func (sm *SyncManager) Start(ctx context.Context) {
	utils.LogInfo("SYNC_MANAGER", "Starting coordinated sync system")
	
	// Initialize forward fetcher
	forwardFetcher, err := NewFetcher(sm.config, sm.channels, sm.chainProvider)
	if err != nil {
		utils.LogError("SYNC_MANAGER", "Failed to create forward fetcher: %v", err)
		return
	}
	forwardFetcher.SetDatabaseWriter(sm.dbWriter)
	sm.forwardFetcher = forwardFetcher
	
	// Check if we need backward sync
	needsBackwardSync, reason := sm.shouldStartBackwardSync()
	
	if needsBackwardSync && sm.enableBackwardSync {
		utils.LogInfo("SYNC_MANAGER", "🚀 STARTING BIDIRECTIONAL SYNC: %s", reason)
		utils.LogInfo("SYNC_MANAGER", "   🔴 Forward fetcher: NEW transfers → store + broadcast")
		utils.LogInfo("SYNC_MANAGER", "   🔵 Backward fetcher: HISTORICAL transfers → store only")
		
		// Initialize backward fetcher
		sm.backwardFetcher = NewBackwardFetcher(sm.config, sm.channels, sm.dbWriter)
		sm.backwardFetcher.SetMaxDepth(sm.backwardSyncDepth)
		
		// Start both fetchers concurrently
		go sm.forwardFetcher.Start(ctx)
		go sm.backwardFetcher.Start(ctx)
		
		utils.LogInfo("SYNC_MANAGER", "✅ Both forward and backward sync started")
	} else {
		if !sm.enableBackwardSync {
			utils.LogInfo("SYNC_MANAGER", "🚀 FORWARD SYNC ONLY: Backward sync disabled")
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
		return false, "failed to check database state"
	}
	
	if count == 0 {
		return false, "database is empty - no backward sync needed"
	}
	
	// Get earliest transfer timestamp to check for gaps
	earliestSortOrder, err := sm.dbWriter.GetEarliestSortOrder()
	if err != nil {
		return false, "failed to get earliest sort order"
	}
	
	if earliestSortOrder == "" {
		return false, "no sort order available"
	}
	
	// Parse timestamp from sort order (sort orders are typically timestamp-based)
	// For now, we'll use a simple heuristic: if we have less than expected transfers for the time period
	
	// If database has transfers but they're sparse, we might need backward sync
	// This is a simplified check - in production you'd want more sophisticated gap detection
	
	// For now, always start backward sync if enabled and we have existing transfers
	// This ensures we fill any historical gaps
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
		"total_transfers":    count,
		"forward_active":     true, // Always active
		"backward_active":    sm.backwardFetcher != nil && sm.backwardFetcher.IsRunning(),
		"backward_enabled":   sm.enableBackwardSync,
		"sync_depth_days":    sm.backwardSyncDepth,
		"gap_threshold_hours": sm.gapThreshold.Hours(),
	}
	
	return status
} 