package utils

import (
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
)

// LogLevel represents different logging levels
type LogLevel int32

const (
	DEBUG LogLevel = iota
	INFO
	WARN
	ERROR
	DISABLED
)

// String returns the string representation of log level
func (l LogLevel) String() string {
	switch l {
	case DEBUG:
		return "DEBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERROR"
	case DISABLED:
		return "DISABLED"
	default:
		return "UNKNOWN"
	}
}

// Logger provides structured logging with configurable levels
type Logger struct {
	level     int32 // atomic access
	mu        sync.RWMutex
	isEnabled map[LogLevel]bool
	name      string
}

// Global logger instance
var globalLogger *Logger

// Initialize global logger
func init() {
	globalLogger = NewLogger("GLOBAL")
	
	// Set default level from environment
	if envLevel := os.Getenv("LOG_LEVEL"); envLevel != "" {
		switch envLevel {
		case "DEBUG":
			globalLogger.SetLevel(DEBUG)
		case "INFO":
			globalLogger.SetLevel(INFO)
		case "WARN":
			globalLogger.SetLevel(WARN)
		case "ERROR":
			globalLogger.SetLevel(ERROR)
		case "DISABLED":
			globalLogger.SetLevel(DISABLED)
		}
	} else {
		// Default to INFO level
		globalLogger.SetLevel(INFO)
	}
}

// NewLogger creates a new logger with the given name
func NewLogger(name string) *Logger {
	l := &Logger{
		level:     int32(INFO),
		isEnabled: make(map[LogLevel]bool),
		name:      name,
	}
	
	// Initialize enabled levels
	l.updateEnabledLevels()
	
	return l
}

// SetLevel sets the minimum log level
func (l *Logger) SetLevel(level LogLevel) {
	atomic.StoreInt32(&l.level, int32(level))
	l.updateEnabledLevels()
}

// GetLevel returns the current log level
func (l *Logger) GetLevel() LogLevel {
	return LogLevel(atomic.LoadInt32(&l.level))
}

// updateEnabledLevels updates the enabled levels map
func (l *Logger) updateEnabledLevels() {
	l.mu.Lock()
	defer l.mu.Unlock()
	
	currentLevel := LogLevel(atomic.LoadInt32(&l.level))
	
	l.isEnabled[DEBUG] = currentLevel <= DEBUG
	l.isEnabled[INFO] = currentLevel <= INFO
	l.isEnabled[WARN] = currentLevel <= WARN
	l.isEnabled[ERROR] = currentLevel <= ERROR
}

// shouldLog checks if a level should be logged (fast path)
func (l *Logger) shouldLog(level LogLevel) bool {
	return LogLevel(atomic.LoadInt32(&l.level)) <= level
}

// Debug logs a debug message
func (l *Logger) Debug(format string, args ...interface{}) {
	if l.shouldLog(DEBUG) {
		l.logf(DEBUG, format, args...)
	}
}

// Info logs an info message
func (l *Logger) Info(format string, args ...interface{}) {
	if l.shouldLog(INFO) {
		l.logf(INFO, format, args...)
	}
}

// Warn logs a warning message
func (l *Logger) Warn(format string, args ...interface{}) {
	if l.shouldLog(WARN) {
		l.logf(WARN, format, args...)
	}
}

// Error logs an error message
func (l *Logger) Error(format string, args ...interface{}) {
	if l.shouldLog(ERROR) {
		l.logf(ERROR, format, args...)
	}
}

// logf performs the actual logging
func (l *Logger) logf(level LogLevel, format string, args ...interface{}) {
	var prefix string
	if l.name != "" {
		prefix = fmt.Sprintf("[%s:%s] ", l.name, level.String())
	} else {
		prefix = fmt.Sprintf("[%s] ", level.String())
	}
	
	log.Printf(prefix+format, args...)
}

// Global logging functions for convenience
func Debug(format string, args ...interface{}) {
	globalLogger.Debug(format, args...)
}

func Info(format string, args ...interface{}) {
	globalLogger.Info(format, args...)
}

func Warn(format string, args ...interface{}) {
	globalLogger.Warn(format, args...)
}

func Error(format string, args ...interface{}) {
	globalLogger.Error(format, args...)
}

// SetGlobalLevel sets the global logger level
func SetGlobalLevel(level LogLevel) {
	globalLogger.SetLevel(level)
}

// GetGlobalLevel returns the global logger level
func GetGlobalLevel() LogLevel {
	return globalLogger.GetLevel()
}

// IsDebugEnabled returns true if debug logging is enabled
func IsDebugEnabled() bool {
	return globalLogger.shouldLog(DEBUG)
}

// IsInfoEnabled returns true if info logging is enabled
func IsInfoEnabled() bool {
	return globalLogger.shouldLog(INFO)
}

// Component-specific loggers for different parts of the system
var (
	FetcherLogger     = NewLogger("FETCHER")
	ProcessorLogger   = NewLogger("PROCESSOR")
	SchedulerLogger   = NewLogger("SCHEDULER")
	BroadcasterLogger = NewLogger("BROADCASTER")
	DatabaseLogger    = NewLogger("DATABASE")
	ChainsLogger      = NewLogger("CHAINS")
)

// InitializeComponentLoggers sets up component loggers with appropriate levels
func InitializeComponentLoggers() {
	// Set levels based on component needs
	if os.Getenv("ENABLE_DEBUG_LOGS") == "true" {
		FetcherLogger.SetLevel(DEBUG)
		ProcessorLogger.SetLevel(DEBUG)
		SchedulerLogger.SetLevel(DEBUG)
		BroadcasterLogger.SetLevel(DEBUG)
		DatabaseLogger.SetLevel(DEBUG)
		ChainsLogger.SetLevel(DEBUG)
	} else {
		// Production levels - reduce noise
		FetcherLogger.SetLevel(INFO)
		ProcessorLogger.SetLevel(WARN)   // Less verbose for processor
		SchedulerLogger.SetLevel(WARN)   // Less verbose for scheduler
		BroadcasterLogger.SetLevel(INFO)
		DatabaseLogger.SetLevel(WARN)    // Less verbose for database
		ChainsLogger.SetLevel(INFO)
	}
}

// HighPerformanceMode disables debug and info logging for maximum performance
func HighPerformanceMode() {
	SetGlobalLevel(WARN)
	FetcherLogger.SetLevel(WARN)
	ProcessorLogger.SetLevel(ERROR)
	SchedulerLogger.SetLevel(ERROR)
	BroadcasterLogger.SetLevel(WARN)
	DatabaseLogger.SetLevel(ERROR)
	ChainsLogger.SetLevel(WARN)
} 