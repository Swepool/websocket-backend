package utils

import (
	"runtime"
	"sync/atomic"
	"time"
)

// MemoryPressure represents different levels of memory pressure
type MemoryPressure int

const (
	MemoryPressureNone MemoryPressure = iota
	MemoryPressureLow
	MemoryPressureMedium
	MemoryPressureHigh
	MemoryPressureCritical
)

// String returns string representation of memory pressure
func (mp MemoryPressure) String() string {
	switch mp {
	case MemoryPressureNone:
		return "None"
	case MemoryPressureLow:
		return "Low"
	case MemoryPressureMedium:
		return "Medium"
	case MemoryPressureHigh:
		return "High"
	case MemoryPressureCritical:
		return "Critical"
	default:
		return "Unknown"
	}
}

// MemoryMonitor tracks memory usage and pressure
type MemoryMonitor struct {
	// Atomic counters for performance
	currentPressure   int32
	lastCheckTime     int64 // Unix timestamp
	forceGCRequests   int64
	cleanupTriggers   int64
	
	// Memory thresholds (in bytes)
	lowPressureThreshold      uint64 // 256MB
	mediumPressureThreshold   uint64 // 512MB
	highPressureThreshold     uint64 // 1GB
	criticalPressureThreshold uint64 // 2GB
	
	// Cleanup configuration
	aggressiveCleanupEnabled bool
	lastCleanupTime          int64
	minCleanupInterval       time.Duration
}

// MemoryStats holds memory statistics
type MemoryStats struct {
	HeapAlloc      uint64
	HeapSys        uint64
	HeapIdle       uint64
	HeapInuse      uint64
	GCCycles       uint32
	LastGC         time.Time
	Pressure       MemoryPressure
	PressureReason string
}

// NewMemoryMonitor creates a new memory monitor with default thresholds
func NewMemoryMonitor() *MemoryMonitor {
	return &MemoryMonitor{
		currentPressure:           int32(MemoryPressureNone),
		lowPressureThreshold:      256 * 1024 * 1024, // 256MB
		mediumPressureThreshold:   512 * 1024 * 1024, // 512MB
		highPressureThreshold:     1024 * 1024 * 1024, // 1GB
		criticalPressureThreshold: 2048 * 1024 * 1024, // 2GB
		aggressiveCleanupEnabled:  true,
		minCleanupInterval:        30 * time.Second,
	}
}

// GetCurrentMemoryStats returns current memory statistics
func (mm *MemoryMonitor) GetCurrentMemoryStats() MemoryStats {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	
	pressure := mm.GetCurrentPressure()
	
	return MemoryStats{
		HeapAlloc:      memStats.HeapAlloc,
		HeapSys:        memStats.HeapSys,
		HeapIdle:       memStats.HeapIdle,
		HeapInuse:      memStats.HeapInuse,
		GCCycles:       memStats.NumGC,
		LastGC:         time.Unix(0, int64(memStats.LastGC)),
		Pressure:       pressure,
		PressureReason: mm.getPressureReason(memStats.HeapAlloc),
	}
}

// CheckMemoryPressure updates the current memory pressure level
func (mm *MemoryMonitor) CheckMemoryPressure() MemoryPressure {
	now := time.Now().Unix()
	atomic.StoreInt64(&mm.lastCheckTime, now)
	
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	
	var newPressure MemoryPressure
	heapAlloc := memStats.HeapAlloc
	
	switch {
	case heapAlloc >= mm.criticalPressureThreshold:
		newPressure = MemoryPressureCritical
	case heapAlloc >= mm.highPressureThreshold:
		newPressure = MemoryPressureHigh
	case heapAlloc >= mm.mediumPressureThreshold:
		newPressure = MemoryPressureMedium
	case heapAlloc >= mm.lowPressureThreshold:
		newPressure = MemoryPressureLow
	default:
		newPressure = MemoryPressureNone
	}
	
	atomic.StoreInt32(&mm.currentPressure, int32(newPressure))
	
	return newPressure
}

// GetCurrentPressure returns the current memory pressure level
func (mm *MemoryMonitor) GetCurrentPressure() MemoryPressure {
	return MemoryPressure(atomic.LoadInt32(&mm.currentPressure))
}

// getPressureReason returns a description of why pressure is at current level
func (mm *MemoryMonitor) getPressureReason(heapAlloc uint64) string {
	switch {
	case heapAlloc >= mm.criticalPressureThreshold:
		return "Heap allocation exceeds 2GB - critical cleanup needed"
	case heapAlloc >= mm.highPressureThreshold:
		return "Heap allocation exceeds 1GB - aggressive cleanup needed"
	case heapAlloc >= mm.mediumPressureThreshold:
		return "Heap allocation exceeds 512MB - moderate cleanup needed"
	case heapAlloc >= mm.lowPressureThreshold:
		return "Heap allocation exceeds 256MB - light cleanup recommended"
	default:
		return "Memory usage within normal limits"
	}
}

// ShouldTriggerCleanup returns true if cleanup should be triggered based on memory pressure
func (mm *MemoryMonitor) ShouldTriggerCleanup() bool {
	pressure := mm.GetCurrentPressure()
	
	if pressure <= MemoryPressureLow {
		return false
	}
	
	// Check minimum interval between cleanups
	lastCleanup := atomic.LoadInt64(&mm.lastCleanupTime)
	if lastCleanup > 0 {
		elapsed := time.Since(time.Unix(lastCleanup, 0))
		if elapsed < mm.minCleanupInterval {
			return false
		}
	}
	
	return true
}

// TriggerCleanup records that a cleanup was triggered
func (mm *MemoryMonitor) TriggerCleanup() {
	atomic.StoreInt64(&mm.lastCleanupTime, time.Now().Unix())
	atomic.AddInt64(&mm.cleanupTriggers, 1)
}

// ShouldForceGC returns true if garbage collection should be forced
func (mm *MemoryMonitor) ShouldForceGC() bool {
	pressure := mm.GetCurrentPressure()
	return pressure >= MemoryPressureHigh
}

// ForceGC forces garbage collection and records the event
func (mm *MemoryMonitor) ForceGC() {
	runtime.GC()
	atomic.AddInt64(&mm.forceGCRequests, 1)
}

// GetCleanupAggression returns how aggressive cleanup should be based on memory pressure
func (mm *MemoryMonitor) GetCleanupAggression() float64 {
	pressure := mm.GetCurrentPressure()
	
	switch pressure {
	case MemoryPressureCritical:
		return 1.0 // Most aggressive - clean everything old
	case MemoryPressureHigh:
		return 0.8 // Very aggressive
	case MemoryPressureMedium:
		return 0.6 // Moderate
	case MemoryPressureLow:
		return 0.4 // Light cleanup
	default:
		return 0.2 // Minimal cleanup
	}
}

// GetRetentionMultiplier returns a multiplier for retention periods based on memory pressure
func (mm *MemoryMonitor) GetRetentionMultiplier() float64 {
	pressure := mm.GetCurrentPressure()
	
	switch pressure {
	case MemoryPressureCritical:
		return 0.25 // Keep only 25% of normal retention
	case MemoryPressureHigh:
		return 0.5 // Keep 50% of normal retention
	case MemoryPressureMedium:
		return 0.75 // Keep 75% of normal retention
	default:
		return 1.0 // Normal retention
	}
}

// OptimizeMemoryUsage performs memory optimization based on current pressure
func (mm *MemoryMonitor) OptimizeMemoryUsage() {
	pressure := mm.CheckMemoryPressure()
	
	switch pressure {
	case MemoryPressureCritical:
		// Immediate and aggressive action
		mm.ForceGC()
		runtime.GC() // Double GC for critical situations
		
	case MemoryPressureHigh:
		// Force GC
		mm.ForceGC()
		
	case MemoryPressureMedium:
		// Suggest GC but don't force
		if mm.ShouldForceGC() {
			mm.ForceGC()
		}
	}
}

// GetStats returns memory monitor statistics
func (mm *MemoryMonitor) GetStats() map[string]interface{} {
	stats := mm.GetCurrentMemoryStats()
	
	return map[string]interface{}{
		"current_pressure":        stats.Pressure.String(),
		"pressure_reason":         stats.PressureReason,
		"heap_alloc_mb":          float64(stats.HeapAlloc) / (1024 * 1024),
		"heap_sys_mb":            float64(stats.HeapSys) / (1024 * 1024),
		"heap_idle_mb":           float64(stats.HeapIdle) / (1024 * 1024),
		"heap_inuse_mb":          float64(stats.HeapInuse) / (1024 * 1024),
		"gc_cycles":              stats.GCCycles,
		"last_gc":                stats.LastGC.Format(time.RFC3339),
		"force_gc_requests":      atomic.LoadInt64(&mm.forceGCRequests),
		"cleanup_triggers":       atomic.LoadInt64(&mm.cleanupTriggers),
		"cleanup_aggression":     mm.GetCleanupAggression(),
		"retention_multiplier":   mm.GetRetentionMultiplier(),
	}
}

// Global memory monitor instance
var globalMemoryMonitor *MemoryMonitor

// GetGlobalMemoryMonitor returns the global memory monitor
func GetGlobalMemoryMonitor() *MemoryMonitor {
	if globalMemoryMonitor == nil {
		globalMemoryMonitor = NewMemoryMonitor()
	}
	return globalMemoryMonitor
}

// Convenience functions for global memory monitor
func CheckMemoryPressure() MemoryPressure {
	return GetGlobalMemoryMonitor().CheckMemoryPressure()
}

func ShouldTriggerCleanup() bool {
	return GetGlobalMemoryMonitor().ShouldTriggerCleanup()
}

func TriggerCleanup() {
	GetGlobalMemoryMonitor().TriggerCleanup()
}

func OptimizeMemoryUsage() {
	GetGlobalMemoryMonitor().OptimizeMemoryUsage()
}

func GetMemoryStats() MemoryStats {
	return GetGlobalMemoryMonitor().GetCurrentMemoryStats()
}

func GetRetentionMultiplier() float64 {
	return GetGlobalMemoryMonitor().GetRetentionMultiplier()
} 