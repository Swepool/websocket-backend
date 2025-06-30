package storage

import (
	"sync"
	"time"
	"websocket-backend/internal/utils"
)

// CacheEntry represents a cached item with metadata
type CacheEntry struct {
	Data      interface{}
	Timestamp time.Time
	TTL       time.Duration
	Key       string
}

// IsExpired checks if the cache entry has expired
func (c *CacheEntry) IsExpired() bool {
	return time.Since(c.Timestamp) > c.TTL
}

// ChartDataCache provides thread-safe caching for chart data with metrics
type ChartDataCache struct {
	entries    map[string]*CacheEntry
	mu         sync.RWMutex
	metrics    CacheMetrics
	defaultTTL time.Duration
}

// CacheMetrics tracks cache performance
type CacheMetrics struct {
	Hits         int64
	Misses       int64
	Invalidations int64
	Evictions     int64
	TotalEntries  int64
	mu           sync.RWMutex
}

// NewChartDataCache creates a new cache with default TTL
func NewChartDataCache(defaultTTL time.Duration) *ChartDataCache {
	cache := &ChartDataCache{
		entries:    make(map[string]*CacheEntry),
		defaultTTL: defaultTTL,
	}
	
	// Start cleanup goroutine to remove expired entries
	go cache.startCleanupRoutine()
	
	utils.LogInfo("CACHE", "Chart data cache initialized with default TTL: %v", defaultTTL)
	return cache
}

// Get retrieves a value from cache if it exists and isn't expired
func (c *ChartDataCache) Get(key string) (interface{}, bool) {
	c.mu.RLock()
	entry, exists := c.entries[key]
	c.mu.RUnlock()
	
	if !exists || entry.IsExpired() {
		// Cache miss or expired
		c.updateMetrics(false)
		if exists && entry.IsExpired() {
			// Remove expired entry
			c.Delete(key)
			c.metrics.mu.Lock()
			c.metrics.Evictions++
			c.metrics.mu.Unlock()
		}
		return nil, false
	}
	
	// Cache hit
	c.updateMetrics(true)
	return entry.Data, true
}

// Set stores a value in cache with default TTL
func (c *ChartDataCache) Set(key string, data interface{}) {
	c.SetWithTTL(key, data, c.defaultTTL)
}

// SetWithTTL stores a value in cache with custom TTL
func (c *ChartDataCache) SetWithTTL(key string, data interface{}, ttl time.Duration) {
	entry := &CacheEntry{
		Data:      data,
		Timestamp: time.Now(),
		TTL:       ttl,
		Key:       key,
	}
	
	c.mu.Lock()
	wasNew := c.entries[key] == nil
	c.entries[key] = entry
	if wasNew {
		c.metrics.TotalEntries++
	}
	c.mu.Unlock()
	
	utils.LogDebug("CACHE", "Cached %s with TTL %v", key, ttl)
}

// Delete removes a key from cache
func (c *ChartDataCache) Delete(key string) {
	c.mu.Lock()
	if _, exists := c.entries[key]; exists {
		delete(c.entries, key)
		c.metrics.TotalEntries--
		c.metrics.mu.Lock()
		c.metrics.Invalidations++
		c.metrics.mu.Unlock()
	}
	c.mu.Unlock()
}

// Clear removes all entries from cache
func (c *ChartDataCache) Clear() {
	c.mu.Lock()
	entryCount := len(c.entries)
	c.entries = make(map[string]*CacheEntry)
	c.metrics.TotalEntries = 0
	c.mu.Unlock()
	
	c.metrics.mu.Lock()
	c.metrics.Invalidations += int64(entryCount)
	c.metrics.mu.Unlock()
	
	utils.LogInfo("CACHE", "Cleared all cache entries (%d removed)", entryCount)
}

// GetOrSet retrieves from cache or executes function and stores result
func (c *ChartDataCache) GetOrSet(key string, fetchFunc func() (interface{}, error)) (interface{}, error) {
	return c.GetOrSetWithTTL(key, c.defaultTTL, fetchFunc)
}

// GetOrSetWithTTL retrieves from cache or executes function with custom TTL
func (c *ChartDataCache) GetOrSetWithTTL(key string, ttl time.Duration, fetchFunc func() (interface{}, error)) (interface{}, error) {
	// Try cache first
	if data, hit := c.Get(key); hit {
		return data, nil
	}
	
	// Cache miss - fetch fresh data
	data, err := fetchFunc()
	if err != nil {
		return nil, err
	}
	
	// Store in cache
	c.SetWithTTL(key, data, ttl)
	return data, nil
}

// GetMetrics returns current cache metrics
func (c *ChartDataCache) GetMetrics() CacheMetrics {
	c.metrics.mu.RLock()
	defer c.metrics.mu.RUnlock()
	
	return CacheMetrics{
		Hits:         c.metrics.Hits,
		Misses:       c.metrics.Misses,
		Invalidations: c.metrics.Invalidations,
		Evictions:    c.metrics.Evictions,
		TotalEntries: c.metrics.TotalEntries,
	}
}

// GetCacheInfo returns detailed cache information
func (c *ChartDataCache) GetCacheInfo() map[string]interface{} {
	c.mu.RLock()
	entries := make([]map[string]interface{}, 0, len(c.entries))
	for key, entry := range c.entries {
		entries = append(entries, map[string]interface{}{
			"key":         key,
			"age_seconds": time.Since(entry.Timestamp).Seconds(),
			"ttl_seconds": entry.TTL.Seconds(),
			"expired":     entry.IsExpired(),
		})
	}
	c.mu.RUnlock()
	
	metrics := c.GetMetrics()
	hitRate := float64(0)
	if metrics.Hits+metrics.Misses > 0 {
		hitRate = float64(metrics.Hits) / float64(metrics.Hits+metrics.Misses) * 100
	}
	
	return map[string]interface{}{
		"default_ttl_seconds": c.defaultTTL.Seconds(),
		"total_entries":       len(entries),
		"entries":            entries,
		"metrics": map[string]interface{}{
			"hits":          metrics.Hits,
			"misses":        metrics.Misses,
			"hit_rate_pct":  hitRate,
			"invalidations": metrics.Invalidations,
			"evictions":     metrics.Evictions,
		},
	}
}

// updateMetrics updates cache hit/miss metrics
func (c *ChartDataCache) updateMetrics(hit bool) {
	c.metrics.mu.Lock()
	defer c.metrics.mu.Unlock()
	
	if hit {
		c.metrics.Hits++
	} else {
		c.metrics.Misses++
	}
}

// startCleanupRoutine periodically removes expired entries
func (c *ChartDataCache) startCleanupRoutine() {
	ticker := time.NewTicker(5 * time.Minute) // Cleanup every 5 minutes
	defer ticker.Stop()
	
	for range ticker.C {
		c.cleanupExpiredEntries()
	}
}

// cleanupExpiredEntries removes all expired entries from cache
func (c *ChartDataCache) cleanupExpiredEntries() {
	c.mu.Lock()
	expiredKeys := make([]string, 0)
	
	for key, entry := range c.entries {
		if entry.IsExpired() {
			expiredKeys = append(expiredKeys, key)
		}
	}
	
	// Remove expired entries
	for _, key := range expiredKeys {
		delete(c.entries, key)
		c.metrics.TotalEntries--
	}
	c.mu.Unlock()
	
	if len(expiredKeys) > 0 {
		c.metrics.mu.Lock()
		c.metrics.Evictions += int64(len(expiredKeys))
		c.metrics.mu.Unlock()
		
		utils.LogDebug("CACHE", "Cleaned up %d expired entries", len(expiredKeys))
	}
}

// CacheConfig holds configuration for different cache types
type CacheConfig struct {
	TransferRatesTTL     time.Duration
	ActiveWalletsTTL     time.Duration
	PopularRoutesTTL     time.Duration
	ChartDataTTL         time.Duration
	DefaultTTL           time.Duration
}

// DefaultCacheConfig returns sensible default cache configuration
func DefaultCacheConfig() CacheConfig {
	return CacheConfig{
		TransferRatesTTL:     30 * time.Second,  // Transfer rates change frequently
		ActiveWalletsTTL:     45 * time.Second,  // Wallet activity updates
		PopularRoutesTTL:     2 * time.Minute,   // Routes change slower
		ChartDataTTL:         30 * time.Second,  // Combined chart data
		DefaultTTL:           30 * time.Second,  // Default for new cache types
	}
} 