package utils

import (
	"encoding/json"
	"sync"
)

// TransferMessage represents the transfer message structure sent to frontend
type TransferMessage struct {
	Type string                   `json:"type"`
	Data []map[string]interface{} `json:"data"`
}

// ChartMessage represents the chart data message structure sent to frontend  
type ChartMessage struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

// Message pools for reusing objects
var (
	transferMessagePool = sync.Pool{
		New: func() interface{} {
			return &TransferMessage{
				Type: "transfers",
				Data: make([]map[string]interface{}, 0, 1), // Pre-allocate slice with capacity 1
			}
		},
	}
	
	chartMessagePool = sync.Pool{
		New: func() interface{} {
			return &ChartMessage{
				Type: "chartData",
			}
		},
	}
	
	// Pool for transfer data maps to avoid allocations
	transferDataPool = sync.Pool{
		New: func() interface{} {
			return make(map[string]interface{}, 20) // Pre-allocate reasonable capacity
		},
	}
	
	// Pool for JSON byte buffers
	jsonBufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 1024) // Start with 1KB capacity
		},
	}
)

// GetTransferMessage gets a pooled transfer message
func GetTransferMessage() *TransferMessage {
	msg := transferMessagePool.Get().(*TransferMessage)
	msg.Data = msg.Data[:0] // Reset slice but keep capacity
	return msg
}

// PutTransferMessage returns a transfer message to the pool
func PutTransferMessage(msg *TransferMessage) {
	if msg != nil {
		// Return individual transfer data maps to pool
		for _, data := range msg.Data {
			if data != nil {
				PutTransferDataMap(data)
			}
		}
		// Clear the slice
		msg.Data = msg.Data[:0]
		transferMessagePool.Put(msg)
	}
}

// GetChartMessage gets a pooled chart message
func GetChartMessage() *ChartMessage {
	msg := chartMessagePool.Get().(*ChartMessage)
	msg.Data = nil // Reset data
	return msg
}

// PutChartMessage returns a chart message to the pool
func PutChartMessage(msg *ChartMessage) {
	if msg != nil {
		msg.Data = nil
		chartMessagePool.Put(msg)
	}
}

// GetTransferDataMap gets a pooled map for transfer data
func GetTransferDataMap() map[string]interface{} {
	m := transferDataPool.Get().(map[string]interface{})
	// Clear the map but keep capacity
	for k := range m {
		delete(m, k)
	}
	return m
}

// PutTransferDataMap returns a transfer data map to the pool
func PutTransferDataMap(m map[string]interface{}) {
	if m != nil {
		transferDataPool.Put(m)
	}
}

// GetJSONBuffer gets a pooled byte buffer for JSON marshaling
func GetJSONBuffer() []byte {
	buf := jsonBufferPool.Get().([]byte)
	return buf[:0] // Reset length but keep capacity
}

// PutJSONBuffer returns a JSON buffer to the pool
func PutJSONBuffer(buf []byte) {
	if buf != nil {
		// Only keep reasonably sized buffers in pool
		if cap(buf) <= 16*1024 { // Max 16KB
			jsonBufferPool.Put(buf)
		}
	}
}

// MarshalTransferMessagePooled marshals a transfer message using pooled objects
func MarshalTransferMessagePooled(transferData map[string]interface{}) ([]byte, error) {
	msg := GetTransferMessage()
	defer PutTransferMessage(msg)
	
	msg.Data = append(msg.Data, transferData)
	
	return json.Marshal(msg)
}

// MarshalChartMessagePooled marshals a chart message using pooled objects
func MarshalChartMessagePooled(chartData interface{}) ([]byte, error) {
	msg := GetChartMessage()
	defer PutChartMessage(msg)
	
	msg.Data = chartData
	
	return json.Marshal(msg)
}

// OptimizedMarshal marshals with a pooled buffer for better performance
func OptimizedMarshal(v interface{}) ([]byte, error) {
	// For small objects, use standard marshal (it's optimized)
	data, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	
	// Make a copy since the original might be reused
	result := make([]byte, len(data))
	copy(result, data)
	
	return result, nil
} 