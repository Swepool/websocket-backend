package utils

import (
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

 