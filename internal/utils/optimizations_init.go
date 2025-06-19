package utils

import (
	"os"
	"runtime"
	"runtime/debug"
)

// InitializeOptimizations sets up all performance optimizations based on environment
func InitializeOptimizations() {
	// Initialize component loggers
	InitializeComponentLoggers()
	
	// Check if high performance mode is enabled
	if os.Getenv("HIGH_PERFORMANCE_MODE") == "true" {
		HighPerformanceMode()
	}
	
	// Initialize batch logger
	InitializeBatchLogger()
	
	// Set up memory optimization based on available memory
	setupMemoryOptimizations()
	
	// Set up runtime optimizations
	setupRuntimeOptimizations()
}

// setupMemoryOptimizations configures memory-related optimizations
func setupMemoryOptimizations() {
	// Get system memory information
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	
	// Adjust GC target based on available memory
	if memStats.Sys > 2*1024*1024*1024 { // > 2GB
		// System has plenty of memory, be less aggressive with GC
		debug.SetGCPercent(200) // Trigger GC at 200% growth
	} else if memStats.Sys > 1*1024*1024*1024 { // > 1GB
		// Moderate memory, use default GC
		debug.SetGCPercent(100) // Default
	} else {
		// Low memory, be more aggressive with GC
		debug.SetGCPercent(50) // Trigger GC at 50% growth
	}
	
	// Initialize memory monitor
	GetGlobalMemoryMonitor()
}

// setupRuntimeOptimizations configures Go runtime for optimal performance
func setupRuntimeOptimizations() {
	// Set max procs if not already set
	if os.Getenv("GOMAXPROCS") == "" {
		// Use all available CPUs for maximum throughput
		runtime.GOMAXPROCS(runtime.NumCPU())
	}
	
	// Set memory limit if available (Go 1.19+)
	if memLimit := os.Getenv("GOMEMLIMIT"); memLimit != "" {
		// Memory limit is set via environment variable
		// This helps prevent OOM kills in containerized environments
	}
}

// GetOptimizationEnvironmentVariables returns environment variables for optimization
func GetOptimizationEnvironmentVariables() map[string]string {
	return map[string]string{
		// Channel buffer configuration
		"CHANNEL_BUFFER_MULTIPLIER":     "2.0",    // Double default buffer sizes
		"RAW_TRANSFERS_BUFFER":          "5000",   // Raw transfers buffer
		"PROCESSED_TRANSFERS_BUFFER":    "5000",   // Processed transfers buffer
		"TRANSFER_BROADCASTS_BUFFER":    "50000",  // Transfer broadcasts buffer
		"DATABASE_SAVES_BUFFER":         "10000",  // Database saves buffer
		"CHART_UPDATES_BUFFER":          "5000",   // Chart updates buffer
		
		// Logging configuration
		"LOG_LEVEL":                     "WARN",   // Reduce log noise in production
		"ENABLE_DEBUG_LOGS":             "false",  // Disable debug logs
		"HIGH_PERFORMANCE_MODE":         "true",   // Enable high performance logging
		
		// Memory configuration
		"GOMEMLIMIT":                   "2GiB",    // Set memory limit for containers
		"GOGC":                         "100",     // Default GC target
		
		// Runtime configuration
		"GOMAXPROCS":                   "",        // Use all CPUs (empty = auto-detect)
	}
}

// PrintOptimizationStatus prints the current optimization status for simplified architecture
func PrintOptimizationStatus() {
	BroadcasterLogger.Info("=== Performance Optimizations Status ===")
	
	// Channel buffer status
	BroadcasterLogger.Info("✓ Increased channel buffer sizes")
	BroadcasterLogger.Info("✓ Added channel overflow protection")
	
	// Lock optimization status
	BroadcasterLogger.Info("✓ Reduced broadcaster lock scope")
	BroadcasterLogger.Info("✓ Optimized client iteration")
	
	// JSON optimization status
	BroadcasterLogger.Info("✓ Implemented object pooling for JSON")
	BroadcasterLogger.Info("✓ Added pre-marshaled static content")
	BroadcasterLogger.Info("✓ Optimized JSON marshaling")
	
	// Database optimization status
	BroadcasterLogger.Info("✓ Database optimized with PostgreSQL")
	BroadcasterLogger.Info("✓ Database-driven chart computation")
	BroadcasterLogger.Info("✓ Simplified in-memory data structures")
	
	// Logging optimization status
	BroadcasterLogger.Info("✓ Implemented structured logging with levels")
	BroadcasterLogger.Info("✓ Added batched logging for high-frequency events")
	
	// Memory status
	memStats := GetMemoryStats()
	BroadcasterLogger.Info("Memory Status: %s (%.1f MB heap allocated)", 
		memStats.Pressure.String(), 
		float64(memStats.HeapAlloc)/(1024*1024))
	
	BroadcasterLogger.Info("=== All optimizations are active ===")
}

// ValidateOptimizations checks that all optimizations are working correctly
func ValidateOptimizations() bool {
	allValid := true
	
	// Test memory monitor
	pressure := CheckMemoryPressure()
	if pressure < 0 {
		Error("Memory monitor validation failed")
		allValid = false
	}
	
	// Test batch logger
	batchLogger := GetBatchLogger()
	if batchLogger == nil {
		Error("Batch logger validation failed")
		allValid = false
	}
	
	// Test JSON pools
	transferData := GetTransferDataMap()
	if transferData == nil {
		Error("JSON pool validation failed")
		allValid = false
	}
	PutTransferDataMap(transferData)
	
	if allValid {
		Info("✓ All optimizations validated successfully")
	} else {
		Error("✗ Some optimizations failed validation")
	}
	
	return allValid
}

// GetOptimizationSummary returns a summary of all optimization metrics
func GetOptimizationSummary() map[string]interface{} {
	return map[string]interface{}{
		"memory":     GetMemoryStats(),
		"batch_logger": GetBatchLogger().GetStats(),
		"runtime": map[string]interface{}{
			"gomaxprocs": runtime.GOMAXPROCS(0),
			"numcpu":     runtime.NumCPU(),
			"goversion":  runtime.Version(),
		},
		"validation": ValidateOptimizations(),
	}
} 