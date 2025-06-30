package utils

import (
	"fmt"
)



// DefaultLog replaces fmt.Printf with structured logging
func DefaultLog(component, level, message string, args ...interface{}) {
	formattedMessage := fmt.Sprintf(message, args...)
	
	switch level {
	case "DEBUG":
		BatchDebug(component, formattedMessage)
	case "INFO":
		BatchInfo(component, formattedMessage)
	case "WARN":
		BatchWarn(component, formattedMessage)
	case "ERROR":
		BatchError(component, formattedMessage)
	default:
		BatchInfo(component, formattedMessage)
	}
}

// Convenience functions for common logging patterns
func LogInfo(component, message string, args ...interface{}) {
	DefaultLog(component, "INFO", message, args...)
}

func LogWarn(component, message string, args ...interface{}) {
	DefaultLog(component, "WARN", message, args...)
}

func LogError(component, message string, args ...interface{}) {
	DefaultLog(component, "ERROR", message, args...)
}

func LogDebug(component, message string, args ...interface{}) {
	DefaultLog(component, "DEBUG", message, args...)
}

 