package utils

import (
	"strconv"
	"sync"
	"time"
)

// LogEntry represents a single log entry for batching
type LogEntry struct {
	Level     LogLevel
	Component string
	Message   string
	Time      time.Time
}

// BatchLogger accumulates log entries and flushes them periodically
type BatchLogger struct {
	entries    chan LogEntry
	batch      []LogEntry
	ticker     *time.Ticker
	batchSize  int
	flushRate  time.Duration
	logger     *Logger
	mu         sync.Mutex
	running    bool
	stopCh     chan struct{}
	wg         sync.WaitGroup
}

// BatchLoggerConfig holds configuration for batch logger
type BatchLoggerConfig struct {
	BatchSize int           // Flush when batch reaches this size
	FlushRate time.Duration // Flush every X duration
	ChBuffer  int           // Channel buffer size
}

// DefaultBatchLoggerConfig returns sensible defaults
func DefaultBatchLoggerConfig() BatchLoggerConfig {
	return BatchLoggerConfig{
		BatchSize: 100,               // Flush every 100 entries
		FlushRate: 1 * time.Second,   // Flush every second
		ChBuffer:  1000,              // 1000 entry buffer
	}
}

// NewBatchLogger creates a new batch logger
func NewBatchLogger(config BatchLoggerConfig, logger *Logger) *BatchLogger {
	if logger == nil {
		logger = globalLogger
	}
	
	bl := &BatchLogger{
		entries:   make(chan LogEntry, config.ChBuffer),
		batch:     make([]LogEntry, 0, config.BatchSize),
		batchSize: config.BatchSize,
		flushRate: config.FlushRate,
		logger:    logger,
		stopCh:    make(chan struct{}),
	}
	
	return bl
}

// Start begins the batch logger background processing
func (bl *BatchLogger) Start() {
	bl.mu.Lock()
	if bl.running {
		bl.mu.Unlock()
		return
	}
	bl.running = true
	bl.mu.Unlock()
	
	bl.ticker = time.NewTicker(bl.flushRate)
	bl.wg.Add(1)
	
	go func() {
		defer bl.wg.Done()
		defer bl.ticker.Stop()
		
		for {
			select {
			case entry := <-bl.entries:
				bl.addToBatch(entry)
				if len(bl.batch) >= bl.batchSize {
					bl.flush()
				}
				
			case <-bl.ticker.C:
				bl.flush() // Periodic flush
				
			case <-bl.stopCh:
				bl.flush() // Final flush
				return
			}
		}
	}()
}

// Stop stops the batch logger and flushes remaining entries
func (bl *BatchLogger) Stop() {
	bl.mu.Lock()
	if !bl.running {
		bl.mu.Unlock()
		return
	}
	bl.running = false
	bl.mu.Unlock()
	
	close(bl.stopCh)
	bl.wg.Wait()
}

// addToBatch adds an entry to the current batch
func (bl *BatchLogger) addToBatch(entry LogEntry) {
	bl.batch = append(bl.batch, entry)
}

// flush writes all batched entries to the logger
func (bl *BatchLogger) flush() {
	if len(bl.batch) == 0 {
		return
	}
	
	// Group by component and level for more efficient output
	grouped := make(map[string]map[LogLevel][]LogEntry)
	
	for _, entry := range bl.batch {
		if grouped[entry.Component] == nil {
			grouped[entry.Component] = make(map[LogLevel][]LogEntry)
		}
		grouped[entry.Component][entry.Level] = append(grouped[entry.Component][entry.Level], entry)
	}
	
	// Write grouped entries
	for component, levels := range grouped {
		for level, entries := range levels {
			if len(entries) == 1 {
				// Single entry - log normally
				bl.logSingle(entries[0])
			} else {
				// Multiple entries - log as summary
				bl.logSummary(component, level, entries)
			}
		}
	}
	
	// Reset batch
	bl.batch = bl.batch[:0]
}

// logSingle logs a single entry
func (bl *BatchLogger) logSingle(entry LogEntry) {
	switch entry.Level {
	case DEBUG:
		bl.logger.Debug("[%s] %s", entry.Component, entry.Message)
	case INFO:
		bl.logger.Info("[%s] %s", entry.Component, entry.Message)
	case WARN:
		bl.logger.Warn("[%s] %s", entry.Component, entry.Message)
	case ERROR:
		bl.logger.Error("[%s] %s", entry.Component, entry.Message)
	}
}

// logSummary logs multiple entries as a summary
func (bl *BatchLogger) logSummary(component string, level LogLevel, entries []LogEntry) {
	if len(entries) <= 1 {
		if len(entries) == 1 {
			bl.logSingle(entries[0])
		}
		return
	}
	
	// Create summary message
	summary := ""
	if len(entries) <= 3 {
		// Show all messages if few entries
		for i, entry := range entries {
			if i > 0 {
				summary += "; "
			}
			summary += entry.Message
		}
	} else {
		// Show first and last, with count
		summary = entries[0].Message + 
			" ... (+" + strconv.Itoa(len(entries)-2) + " similar) ... " + 
			entries[len(entries)-1].Message
	}
	
	// Log the summary
	switch level {
	case DEBUG:
		bl.logger.Debug("[%s] BATCH(%d): %s", component, len(entries), summary)
	case INFO:
		bl.logger.Info("[%s] BATCH(%d): %s", component, len(entries), summary)
	case WARN:
		bl.logger.Warn("[%s] BATCH(%d): %s", component, len(entries), summary)
	case ERROR:
		bl.logger.Error("[%s] BATCH(%d): %s", component, len(entries), summary)
	}
}

// Log adds an entry to the batch queue
func (bl *BatchLogger) Log(level LogLevel, component, message string) {
	if !bl.logger.shouldLog(level) {
		return // Skip if level not enabled
	}
	
	entry := LogEntry{
		Level:     level,
		Component: component,
		Message:   message,
		Time:      time.Now(),
	}
	
	select {
	case bl.entries <- entry:
		// Successfully queued
	default:
		// Channel full - log directly to avoid blocking
		bl.logSingle(entry)
	}
}

// Debug logs a debug message
func (bl *BatchLogger) Debug(component, message string) {
	bl.Log(DEBUG, component, message)
}

// Info logs an info message
func (bl *BatchLogger) Info(component, message string) {
	bl.Log(INFO, component, message)
}

// Warn logs a warning message
func (bl *BatchLogger) Warn(component, message string) {
	bl.Log(WARN, component, message)
}

// Error logs an error message
func (bl *BatchLogger) Error(component, message string) {
	bl.Log(ERROR, component, message)
}

// GetStats returns batch logger statistics
func (bl *BatchLogger) GetStats() map[string]interface{} {
	bl.mu.Lock()
	defer bl.mu.Unlock()
	
	return map[string]interface{}{
		"running":         bl.running,
		"current_batch":   len(bl.batch),
		"queue_length":    len(bl.entries),
		"queue_capacity":  cap(bl.entries),
		"batch_size":      bl.batchSize,
		"flush_rate_ms":   bl.flushRate.Milliseconds(),
	}
}

// Global batch logger instance
var globalBatchLogger *BatchLogger

// InitializeBatchLogger sets up the global batch logger
func InitializeBatchLogger() {
	config := DefaultBatchLoggerConfig()
	
	// Adjust configuration based on environment
	if GetGlobalLevel() >= WARN {
		// Reduce overhead in production
		config.BatchSize = 200
		config.FlushRate = 2 * time.Second
		config.ChBuffer = 2000
	}
	
	globalBatchLogger = NewBatchLogger(config, globalLogger)
	globalBatchLogger.Start()
}

// GetBatchLogger returns the global batch logger
func GetBatchLogger() *BatchLogger {
	if globalBatchLogger == nil {
		InitializeBatchLogger()
	}
	return globalBatchLogger
}

// StopBatchLogger stops the global batch logger
func StopBatchLogger() {
	if globalBatchLogger != nil {
		globalBatchLogger.Stop()
		globalBatchLogger = nil
	}
}

// Convenience functions for batch logging
func BatchDebug(component, message string) {
	GetBatchLogger().Debug(component, message)
}

func BatchInfo(component, message string) {
	GetBatchLogger().Info(component, message)
}

func BatchWarn(component, message string) {
	GetBatchLogger().Warn(component, message)
}

func BatchError(component, message string) {
	GetBatchLogger().Error(component, message)
} 