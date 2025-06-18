package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"
	"websocket-backend-new/models"
)

// DefaultMarshalTransfer replaces all json.Marshal calls for transfers with optimized version
func DefaultMarshalTransfer(transfer models.Transfer) ([]byte, error) {
	// Build transfer data using pooled objects
	transferData := GetTransferDataMap()
	defer PutTransferDataMap(transferData)
	
	// Populate efficiently
	transferData["source_chain"] = map[string]interface{}{
		"universal_chain_id": transfer.SourceChain.UniversalChainID,
		"display_name":       transfer.SourceChain.DisplayName,
		"chain_id":           transfer.SourceChain.ChainID,
		"testnet":            transfer.SourceChain.Testnet,
		"rpc_type":           transfer.SourceChain.RpcType,
		"addr_prefix":        transfer.SourceChain.AddrPrefix,
	}
	
	transferData["destination_chain"] = map[string]interface{}{
		"universal_chain_id": transfer.DestinationChain.UniversalChainID,
		"display_name":       transfer.DestinationChain.DisplayName,
		"chain_id":           transfer.DestinationChain.ChainID,
		"testnet":            transfer.DestinationChain.Testnet,
		"rpc_type":           transfer.DestinationChain.RpcType,
		"addr_prefix":        transfer.DestinationChain.AddrPrefix,
	}
	
	transferData["packet_hash"] = transfer.PacketHash
	transferData["isTestnetTransfer"] = transfer.IsTestnetTransfer
	transferData["sourceDisplayName"] = transfer.SourceDisplayName
	transferData["destinationDisplayName"] = transfer.DestinationDisplayName
	transferData["formattedTimestamp"] = transfer.FormattedTimestamp
	transferData["routeKey"] = transfer.RouteKey
	transferData["senderDisplay"] = transfer.SenderDisplay
	transferData["receiverDisplay"] = transfer.ReceiverDisplay
	
	// Use optimized marshaling directly with static content
	return MarshalTransferWithStaticContent(transferData)
}

// DefaultMarshalChart replaces all json.Marshal calls for chart data with optimized version
func DefaultMarshalChart(chartData interface{}) ([]byte, error) {
	return MarshalChartWithStaticContent(chartData)
}

// DefaultSendToChannel replaces all manual channel operations with backpressure protection
func DefaultSendToChannel[T any](ch chan<- T, data T, componentName string) bool {
	config := DefaultBackpressureConfig()
	config.DropOnOverflow = false // Conservative - don't drop by default
	config.TimeoutMs = 100        // Standard timeout
	
	return SendWithBackpressure(ch, data, config, nil)
}

// DefaultTrySendToChannel for non-blocking sends that can drop
func DefaultTrySendToChannel[T any](ch chan<- T, data T, componentName string) bool {
	return TrySend(ch, data, nil)
}

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

// Migration helpers to gradually replace old patterns
// These can be removed after consolidation is complete

// ReplaceJsonMarshal is a drop-in replacement for json.Marshal in transfer contexts
func ReplaceJsonMarshal(v interface{}) ([]byte, error) {
	// Check if it's a transfer message pattern
	if m, ok := v.(map[string]interface{}); ok {
		if msgType, exists := m["type"]; exists {
			if msgType == "transfers" && m["data"] != nil {
				// Extract transfer data and use optimized path
				if dataArray, ok := m["data"].([]map[string]interface{}); ok && len(dataArray) > 0 {
					return MarshalTransferWithStaticContent(dataArray[0])
				}
			} else if msgType == "chartData" && m["data"] != nil {
				// Extract chart data and use optimized path
				return MarshalChartWithStaticContent(m["data"])
			}
		}
	}
	
	// Fallback to standard marshal for other cases
	return json.Marshal(v)
}

// ReplaceFmtPrintf is a drop-in replacement for fmt.Printf in component contexts
func ReplaceFmtPrintf(format string, args ...interface{}) {
	// Extract component name from format string if it follows pattern "[COMPONENT] ..."
	component := "UNKNOWN"
	if len(format) > 2 && format[0] == '[' {
		end := -1
		for i := 1; i < len(format); i++ {
			if format[i] == ']' {
				end = i
				break
			}
		}
		if end > 1 {
			component = format[1:end]
			format = format[end+2:] // Skip "] "
		}
	}
	
	DefaultLog(component, "INFO", format, args...)
}

// DefaultGetChannelBufferSize returns optimized buffer size for a channel
func DefaultGetChannelBufferSize(channelName string, fallback int) int {
	// Channel buffer size mapping with optimized defaults
	defaults := map[string]int{
		"TransferBroadcasts": 10000,    // Up from 200 (50x increase)
		"ChartUpdates":      10000,     // Up from 200 (50x increase)
		"TransferUpdates":   5000,      // Up from 2000 (2.5x increase)
		"TransferEnhanced":  5000,      // Up from 2000 (2.5x increase)
		"TransferToSchedule": 5000,     // Up from 2000 (2.5x increase)
		"ClientSend":        250,       // Up from 100 (2.5x increase)
		"ClientRegister":    250,       // Up from 100 (2.5x increase)
		"ClientUnregister":  250,       // Up from 100 (2.5x increase)
	}
	
	// Check for environment variable override
	envVar := fmt.Sprintf("CHANNEL_BUFFER_%s", channelName)
	if envVal := os.Getenv(envVar); envVal != "" {
		if size, err := strconv.Atoi(envVal); err == nil && size > 0 {
			return size
		}
	}
	
	// Return optimized default or fallback
	if size, exists := defaults[channelName]; exists {
		return size
	}
	
	// If no specific default, apply 2.5x multiplier to fallback
	if fallback > 0 {
		return int(float64(fallback) * 2.5)
	}
	
	return 1000 // Ultimate fallback
}



// DefaultGenerateID generates a unique ID with a prefix
func DefaultGenerateID(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
} 