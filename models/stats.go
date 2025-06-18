package models

// LatencyData represents latency statistics between chain pairs
type LatencyData struct {
	SourceChain      string       `json:"sourceChain"`
	DestinationChain string       `json:"destinationChain"`
	SourceName       string       `json:"sourceName"`
	DestinationName  string       `json:"destinationName"`
	PacketAck        LatencyStats `json:"packetAck"`
	PacketRecv       LatencyStats `json:"packetRecv"`
	WriteAck         LatencyStats `json:"writeAck"`
}

// LatencyStats represents latency percentiles
type LatencyStats struct {
	P5     float64 `json:"p5"`
	Median float64 `json:"median"`
	P95    float64 `json:"p95"`
} 