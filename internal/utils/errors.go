package utils

import (
	"fmt"
	"runtime"
	"strings"
	"time"
)

// ErrorType represents different categories of errors
type ErrorType string

const (
	ErrorTypeValidation ErrorType = "VALIDATION"
	ErrorTypeNetwork    ErrorType = "NETWORK"
	ErrorTypeGraphQL    ErrorType = "GRAPHQL"
	ErrorTypeWebSocket  ErrorType = "WEBSOCKET"
	ErrorTypeInternal   ErrorType = "INTERNAL"
	ErrorTypeConfig     ErrorType = "CONFIG"
	ErrorTypeTimeout    ErrorType = "TIMEOUT"
)

// AppError represents a structured application error
type AppError struct {
	Type        ErrorType              `json:"type"`
	Code        string                 `json:"code"`
	Message     string                 `json:"message"`
	Details     string                 `json:"details,omitempty"`
	Cause       error                  `json:"-"` // Don't serialize the underlying error
	Context     map[string]interface{} `json:"context,omitempty"`
	Timestamp   time.Time              `json:"timestamp"`
	StackTrace  string                 `json:"stackTrace,omitempty"`
	Retryable   bool                   `json:"retryable"`
	Component   string                 `json:"component"`
}

// Error implements the error interface
func (e *AppError) Error() string {
	if e.Details != "" {
		return fmt.Sprintf("[%s:%s] %s: %s", e.Type, e.Code, e.Message, e.Details)
	}
	return fmt.Sprintf("[%s:%s] %s", e.Type, e.Code, e.Message)
}

// Unwrap returns the underlying error for error unwrapping
func (e *AppError) Unwrap() error {
	return e.Cause
}

// IsRetryable returns whether the error is retryable
func (e *AppError) IsRetryable() bool {
	return e.Retryable
}

// WithContext adds context to the error
func (e *AppError) WithContext(key string, value interface{}) *AppError {
	if e.Context == nil {
		e.Context = make(map[string]interface{})
	}
	e.Context[key] = value
	return e
}

// WithDetails adds additional details to the error
func (e *AppError) WithDetails(details string) *AppError {
	e.Details = details
	return e
}

// NewAppError creates a new application error
func NewAppError(errorType ErrorType, code, message, component string) *AppError {
	return &AppError{
		Type:      errorType,
		Code:      code,
		Message:   message,
		Component: component,
		Timestamp: time.Now().UTC(),
		Context:   make(map[string]interface{}),
	}
}

// WrapError wraps an existing error with application error context
func WrapError(err error, errorType ErrorType, code, message, component string) *AppError {
	appErr := NewAppError(errorType, code, message, component)
	appErr.Cause = err
	
	// Capture stack trace
	if includeStackTrace {
		appErr.StackTrace = getStackTrace()
	}
	
	return appErr
}

// Configuration for error handling
var (
	includeStackTrace = false // Set to true in development
)

// SetIncludeStackTrace configures whether to include stack traces in errors
func SetIncludeStackTrace(include bool) {
	includeStackTrace = include
}

// getStackTrace captures the current stack trace
func getStackTrace() string {
	const depth = 32
	var pcs [depth]uintptr
	n := runtime.Callers(3, pcs[:]) // Skip getStackTrace, WrapError, and the calling function
	
	var trace strings.Builder
	frames := runtime.CallersFrames(pcs[:n])
	
	for {
		frame, more := frames.Next()
		trace.WriteString(fmt.Sprintf("%s:%d %s\n", frame.File, frame.Line, frame.Function))
		if !more {
			break
		}
	}
	
	return trace.String()
}

// Error handling utilities

// IsRetryableError checks if an error is retryable
func IsRetryableError(err error) bool {
	if appErr, ok := err.(*AppError); ok {
		return appErr.IsRetryable()
	}
	
	// Check for common retryable error patterns
	errStr := strings.ToLower(err.Error())
	retryablePatterns := []string{
		"timeout",
		"connection refused",
		"connection reset",
		"temporary failure",
		"service unavailable",
		"too many requests",
	}
	
	for _, pattern := range retryablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}
	
	return false
}

// GetErrorType extracts the error type from an error
func GetErrorType(err error) ErrorType {
	if appErr, ok := err.(*AppError); ok {
		return appErr.Type
	}
	return ErrorTypeInternal
}

// GetErrorCode extracts the error code from an error
func GetErrorCode(err error) string {
	if appErr, ok := err.(*AppError); ok {
		return appErr.Code
	}
	return "UNKNOWN"
}

// LogError logs an error with appropriate context
func LogError(err error, logger *Logger, additionalContext ...map[string]interface{}) {
	if appErr, ok := err.(*AppError); ok {
		fields := make(map[string]interface{})
		
		// Add error fields
		fields["errorType"] = appErr.Type
		fields["errorCode"] = appErr.Code
		fields["retryable"] = appErr.Retryable
		fields["component"] = appErr.Component
		
		// Add error context
		for k, v := range appErr.Context {
			fields[k] = v
		}
		
		// Add additional context
		if len(additionalContext) > 0 {
			for k, v := range additionalContext[0] {
				fields[k] = v
			}
		}
		
		// Add stack trace if available
		if appErr.StackTrace != "" {
			fields["stackTrace"] = appErr.StackTrace
		}
		
		logger.Error(appErr.Message, fields)
	} else {
		// Handle regular errors
		fields := make(map[string]interface{})
		fields["errorType"] = "UNKNOWN"
		
		if len(additionalContext) > 0 {
			for k, v := range additionalContext[0] {
				fields[k] = v
			}
		}
		
		logger.Error(err.Error(), fields)
	}
} 