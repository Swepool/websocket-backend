package utils

import (
	"bytes"
	"encoding/json"
)

// PreMarshaledContent holds pre-marshaled JSON strings for performance
type PreMarshaledContent struct {
	TransferPrefix []byte // `{"type":"transfers","data":[`
	TransferSuffix []byte // `]}`
	ChartPrefix    []byte // `{"type":"chartData","data":`
	ChartSuffix    []byte // `}`
}

// Global instance of pre-marshaled content
var preMarshaledContent = PreMarshaledContent{
	TransferPrefix: []byte(`{"type":"transfers","data":[`),
	TransferSuffix: []byte(`]}`),
	ChartPrefix:    []byte(`{"type":"chartData","data":`),
	ChartSuffix:    []byte(`}`),
}

// GetPreMarshaledContent returns the global pre-marshaled content
func GetPreMarshaledContent() *PreMarshaledContent {
	return &preMarshaledContent
}

// MarshalTransferWithStaticContent marshals a transfer using pre-marshaled prefixes/suffixes
func MarshalTransferWithStaticContent(transferData map[string]interface{}) ([]byte, error) {
	// Marshal just the transfer data
	transferJSON, err := json.Marshal(transferData)
	if err != nil {
		return nil, err
	}
	
	// Calculate total size needed
	totalSize := len(preMarshaledContent.TransferPrefix) + 
		len(transferJSON) + 
		len(preMarshaledContent.TransferSuffix)
	
	// Create buffer with exact size
	result := make([]byte, 0, totalSize)
	
	// Append components
	result = append(result, preMarshaledContent.TransferPrefix...)
	result = append(result, transferJSON...)
	result = append(result, preMarshaledContent.TransferSuffix...)
	
	return result, nil
}

// MarshalChartWithStaticContent marshals chart data using pre-marshaled prefixes/suffixes
func MarshalChartWithStaticContent(chartData interface{}) ([]byte, error) {
	// Marshal just the chart data
	chartJSON, err := json.Marshal(chartData)
	if err != nil {
		return nil, err
	}
	
	// Calculate total size needed
	totalSize := len(preMarshaledContent.ChartPrefix) + 
		len(chartJSON) + 
		len(preMarshaledContent.ChartSuffix)
	
	// Create buffer with exact size
	result := make([]byte, 0, totalSize)
	
	// Append components
	result = append(result, preMarshaledContent.ChartPrefix...)
	result = append(result, chartJSON...)
	result = append(result, preMarshaledContent.ChartSuffix...)
	
	return result, nil
}

// BuildTransferJSONOptimized builds transfer JSON using a buffer for efficiency
func BuildTransferJSONOptimized(transferData map[string]interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 512)) // Pre-allocate reasonable size
	
	// Write prefix
	buf.Write(preMarshaledContent.TransferPrefix)
	
	// Use streaming encoder for transfer data
	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false) // Disable HTML escaping for performance
	
	if err := encoder.Encode(transferData); err != nil {
		return nil, err
	}
	
	// Remove trailing newline from encoder
	data := buf.Bytes()
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
	}
	
	// Add suffix
	data = append(data, preMarshaledContent.TransferSuffix...)
	
	return data, nil
}

// BuildChartJSONOptimized builds chart JSON using a buffer for efficiency
func BuildChartJSONOptimized(chartData interface{}) ([]byte, error) {
	buf := bytes.NewBuffer(make([]byte, 0, 2048)) // Larger buffer for chart data
	
	// Write prefix
	buf.Write(preMarshaledContent.ChartPrefix)
	
	// Use streaming encoder for chart data
	encoder := json.NewEncoder(buf)
	encoder.SetEscapeHTML(false) // Disable HTML escaping for performance
	
	if err := encoder.Encode(chartData); err != nil {
		return nil, err
	}
	
	// Remove trailing newline from encoder
	data := buf.Bytes()
	if len(data) > 0 && data[len(data)-1] == '\n' {
		data = data[:len(data)-1]
	}
	
	// Add suffix
	data = append(data, preMarshaledContent.ChartSuffix...)
	
	return data, nil
}

 