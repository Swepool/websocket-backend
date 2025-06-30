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
	
	// Memory thresholds (in bytes)
	lowPressureThreshold      uint64 // 256MB
	mediumPressureThreshold   uint64 // 512MB
	highPressureThreshold     uint64 // 1GB
	criticalPressureThreshold uint64 // 2GB
	
	// Cleanup configuration
	aggressiveCleanupEnabled bool
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
		"cleanup_aggression":     mm.GetCleanupAggression(),
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

func GetMemoryStats() MemoryStats {
	return GetGlobalMemoryMonitor().GetCurrentMemoryStats()
} 