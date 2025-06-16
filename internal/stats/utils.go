package stats

import (
	"time"
	"github.com/axiomhq/hyperloglog"
)

// Standard time periods used across all time scale functions
var standardTimePeriods = map[string]time.Duration{
	"1m":  time.Minute,
	"1h":  time.Hour,
	"1d":  24 * time.Hour,
	"7d":  7 * 24 * time.Hour,
	"14d": 14 * 24 * time.Hour,
	"30d": 30 * 24 * time.Hour,
}

// getTimePeriodsFromNow returns time periods calculated from current time
func getTimePeriodsFromNow() map[string]time.Time {
	now := time.Now()
	periods := make(map[string]time.Time)
	for key, duration := range standardTimePeriods {
		periods[key] = now.Add(-duration)
	}
	return periods
}

// NewHLLPool creates a new HyperLogLog pool
func NewHLLPool() *HLLPool {
	return &HLLPool{
		senders:   hyperloglog.New16(),
		receivers: hyperloglog.New16(),
		combined:  hyperloglog.New16(),
	}
}

// Reset clears all HLL instances for reuse
func (p *HLLPool) Reset() {
	// Create new instances since axiomhq/hyperloglog doesn't have a Reset method
	p.senders = hyperloglog.New16()
	p.receivers = hyperloglog.New16()
	p.combined = hyperloglog.New16()
}

// GetSenders returns the senders HLL instance
func (p *HLLPool) GetSenders() *hyperloglog.Sketch {
	return p.senders
}

// GetReceivers returns the receivers HLL instance
func (p *HLLPool) GetReceivers() *hyperloglog.Sketch {
	return p.receivers
}

// GetCombined returns the combined HLL instance
func (p *HLLPool) GetCombined() *hyperloglog.Sketch {
	return p.combined
}

// calculatePercentageChange calculates percentage change between current and previous values
func (ec *EnhancedCollector) calculatePercentageChange(current, previous float64) float64 {
	if previous == 0 {
		if current == 0 {
			return 0
		}
		return 100 // 100% increase from 0
	}
	return ((current - previous) / previous) * 100
} 