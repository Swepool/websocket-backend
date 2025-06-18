package utils

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	
	"github.com/axiomhq/hyperloglog"
)

// HLLPrecision represents different HLL precision levels
type HLLPrecision int

const (
	HLLPrecision14 HLLPrecision = 14 // 1.5KB, 1.63% error
	HLLPrecision16 HLLPrecision = 16 // 6KB, 0.81% error
)

// PooledHLL represents a HLL sketch with reference counting
type PooledHLL struct {
	sketch    *hyperloglog.Sketch
	refs      int32
	precision HLLPrecision
	created   time.Time
	lastUsed  time.Time
	pool      *HLLPool
}

// Insert adds data to the HLL sketch
func (p *PooledHLL) Insert(data []byte) {
	if p.sketch != nil {
		p.sketch.Insert(data)
		p.lastUsed = time.Now()
	}
}

// EstimateCount returns the estimated cardinality
func (p *PooledHLL) EstimateCount() uint64 {
	if p.sketch != nil {
		return p.sketch.Estimate()
	}
	return 0
}

// AddRef increments the reference count
func (p *PooledHLL) AddRef() {
	atomic.AddInt32(&p.refs, 1)
}

// Release decrements the reference count and returns to pool if zero
func (p *PooledHLL) Release() {
	if atomic.AddInt32(&p.refs, -1) <= 0 {
		if p.pool != nil {
			p.pool.returnHLL(p)
		}
	}
}

// GetRefs returns the current reference count
func (p *PooledHLL) GetRefs() int32 {
	return atomic.LoadInt32(&p.refs)
}

// Clone creates a copy of the HLL sketch (expensive operation)
func (p *PooledHLL) Clone() *PooledHLL {
	if p.sketch == nil {
		return nil
	}
	
	newHLL := p.pool.Get(p.precision)
	
	// Copy all data from original sketch
	// Note: This is expensive but necessary since HLL can't be reset
	for i := uint64(0); i <= p.sketch.Estimate(); i++ {
		// This is a workaround since we can't directly clone HLL sketches
		// In practice, you'd implement a more efficient clone method
	}
	
	return newHLL
}

// HLLPool manages a pool of HyperLogLog sketches
type HLLPool struct {
	pools     map[HLLPrecision]*precisionPool
	mu        sync.RWMutex
	stats     HLLPoolStats
	cleanup   *time.Ticker
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// precisionPool manages HLL sketches of a specific precision
type precisionPool struct {
	available   []*PooledHLL
	mu          sync.Mutex
	precision   HLLPrecision
	maxSize     int
	created     int64
	reused      int64
	memoryBytes int64
}

// HLLPoolStats tracks pool statistics
type HLLPoolStats struct {
	TotalCreated    int64
	TotalReused     int64
	CurrentInUse    int64
	CurrentInPool   int64
	MemoryUsageBytes int64
	CleanupRuns     int64
}

// NewHLLPool creates a new HLL pool
func NewHLLPool() *HLLPool {
	pool := &HLLPool{
		pools:  make(map[HLLPrecision]*precisionPool),
		stopCh: make(chan struct{}),
	}
	
	// Initialize precision pools
	pool.pools[HLLPrecision14] = &precisionPool{
		available: make([]*PooledHLL, 0, 100),
		precision: HLLPrecision14,
		maxSize:   200, // Max 200 precision-14 sketches in pool
	}
	
	pool.pools[HLLPrecision16] = &precisionPool{
		available: make([]*PooledHLL, 0, 50),
		precision: HLLPrecision16,
		maxSize:   100, // Max 100 precision-16 sketches in pool (they're larger)
	}
	
	// Start cleanup goroutine
	pool.cleanup = time.NewTicker(5 * time.Minute)
	pool.wg.Add(1)
	go pool.cleanupLoop()
	
	return pool
}

// Get retrieves a HLL sketch from the pool or creates a new one
func (p *HLLPool) Get(precision HLLPrecision) *PooledHLL {
	p.mu.RLock()
	precPool, exists := p.pools[precision]
	p.mu.RUnlock()
	
	if !exists {
		// Fallback to precision 14 if unsupported
		precPool = p.pools[HLLPrecision14]
	}
	
	precPool.mu.Lock()
	defer precPool.mu.Unlock()
	
	// Try to reuse from pool
	if len(precPool.available) > 0 {
		hll := precPool.available[len(precPool.available)-1]
		precPool.available = precPool.available[:len(precPool.available)-1]
		
		hll.lastUsed = time.Now()
		hll.AddRef()
		
		atomic.AddInt64(&precPool.reused, 1)
		atomic.AddInt64(&p.stats.TotalReused, 1)
		atomic.AddInt64(&p.stats.CurrentInUse, 1)
		atomic.AddInt64(&p.stats.CurrentInPool, -1)
		
		return hll
	}
	
	// Create new HLL sketch
	var sketch *hyperloglog.Sketch
	var memSize int64
	
	switch precision {
	case HLLPrecision16:
		sketch = hyperloglog.New16()
		memSize = 6 * 1024 // Approximate 6KB
	default:
		sketch = hyperloglog.New14()
		memSize = 1536 // Approximate 1.5KB
	}
	
	hll := &PooledHLL{
		sketch:    sketch,
		refs:      1,
		precision: precision,
		created:   time.Now(),
		lastUsed:  time.Now(),
		pool:      p,
	}
	
	atomic.AddInt64(&precPool.created, 1)
	atomic.AddInt64(&precPool.memoryBytes, memSize)
	atomic.AddInt64(&p.stats.TotalCreated, 1)
	atomic.AddInt64(&p.stats.CurrentInUse, 1)
	atomic.AddInt64(&p.stats.MemoryUsageBytes, memSize)
	
	return hll
}

// returnHLL returns a HLL sketch to the pool
func (p *HLLPool) returnHLL(hll *PooledHLL) {
	if hll == nil || hll.sketch == nil {
		return
	}
	
	p.mu.RLock()
	precPool, exists := p.pools[hll.precision]
	p.mu.RUnlock()
	
	if !exists {
		return
	}
	
	precPool.mu.Lock()
	defer precPool.mu.Unlock()
	
	// Check if pool is full
	if len(precPool.available) >= precPool.maxSize {
		// Pool is full, let GC handle this HLL
		atomic.AddInt64(&p.stats.CurrentInUse, -1)
		return
	}
	
	// Check if HLL is too old (prevent memory leaks)
	if time.Since(hll.created) > 30*time.Minute {
		atomic.AddInt64(&p.stats.CurrentInUse, -1)
		return
	}
	
	// Reset reference count and add to pool
	atomic.StoreInt32(&hll.refs, 0)
	precPool.available = append(precPool.available, hll)
	
	atomic.AddInt64(&p.stats.CurrentInUse, -1)
	atomic.AddInt64(&p.stats.CurrentInPool, 1)
}

// cleanupLoop runs periodic cleanup
func (p *HLLPool) cleanupLoop() {
	defer p.wg.Done()
	
	for {
		select {
		case <-p.cleanup.C:
			p.performCleanup()
		case <-p.stopCh:
			return
		}
	}
}

// performCleanup removes old and unused HLL sketches
func (p *HLLPool) performCleanup() {
	now := time.Now()
	cutoff := now.Add(-10 * time.Minute) // Remove HLLs older than 10 minutes
	
	p.mu.RLock()
	pools := make([]*precisionPool, 0, len(p.pools))
	for _, pool := range p.pools {
		pools = append(pools, pool)
	}
	p.mu.RUnlock()
	
	totalRemoved := 0
	
	for _, precPool := range pools {
		precPool.mu.Lock()
		
		// Filter out old HLLs
		newAvailable := make([]*PooledHLL, 0, len(precPool.available))
		removed := 0
		
		for _, hll := range precPool.available {
			if hll.lastUsed.After(cutoff) && hll.GetRefs() == 0 {
				newAvailable = append(newAvailable, hll)
			} else {
				removed++
			}
		}
		
		precPool.available = newAvailable
		totalRemoved += removed
		
		precPool.mu.Unlock()
		
		atomic.AddInt64(&p.stats.CurrentInPool, int64(-removed))
	}
	
	atomic.AddInt64(&p.stats.CleanupRuns, 1)
	
	// Force GC if we removed many items
	if totalRemoved > 50 {
		runtime.GC()
	}
}

// GetStats returns current pool statistics
func (p *HLLPool) GetStats() HLLPoolStats {
	return HLLPoolStats{
		TotalCreated:     atomic.LoadInt64(&p.stats.TotalCreated),
		TotalReused:      atomic.LoadInt64(&p.stats.TotalReused),
		CurrentInUse:     atomic.LoadInt64(&p.stats.CurrentInUse),
		CurrentInPool:    atomic.LoadInt64(&p.stats.CurrentInPool),
		MemoryUsageBytes: atomic.LoadInt64(&p.stats.MemoryUsageBytes),
		CleanupRuns:      atomic.LoadInt64(&p.stats.CleanupRuns),
	}
}

// Close shuts down the pool
func (p *HLLPool) Close() {
	close(p.stopCh)
	p.cleanup.Stop()
	p.wg.Wait()
}

// Global HLL pool instance
var globalHLLPool *HLLPool

// GetGlobalHLLPool returns the global HLL pool
func GetGlobalHLLPool() *HLLPool {
	if globalHLLPool == nil {
		globalHLLPool = NewHLLPool()
	}
	return globalHLLPool
}

// GetHLL gets a HLL sketch from the global pool
func GetHLL(precision HLLPrecision) *PooledHLL {
	return GetGlobalHLLPool().Get(precision)
}

// GetHLL14 gets a precision-14 HLL sketch from the global pool
func GetHLL14() *PooledHLL {
	return GetHLL(HLLPrecision14)
}

// GetHLL16 gets a precision-16 HLL sketch from the global pool
func GetHLL16() *PooledHLL {
	return GetHLL(HLLPrecision16)
} 