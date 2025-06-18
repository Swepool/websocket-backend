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

// NodeHealthData represents the health status of an RPC node
type NodeHealthData struct {
	ChainID          string    `json:"chainId"`
	ChainName        string    `json:"chainName"`
	RpcURL           string    `json:"rpcUrl"`
	RpcType          string    `json:"rpcType"`
	Status           string    `json:"status"` // "healthy", "degraded", "unhealthy"
	ResponseTimeMs   int       `json:"responseTimeMs"`
	LastCheckTime    int64     `json:"lastCheckTime"`
	LatestBlockHeight *int64   `json:"latestBlockHeight,omitempty"`
	ErrorMessage     string    `json:"errorMessage,omitempty"`
	Uptime           float64   `json:"uptime"` // Percentage uptime over last 24h
}

// NodeHealthSummary represents aggregated health data for chart display
type NodeHealthSummary struct {
	TotalNodes       int                        `json:"totalNodes"`
	HealthyNodes     int                        `json:"healthyNodes"`
	DegradedNodes    int                        `json:"degradedNodes"`
	UnhealthyNodes   int                        `json:"unhealthyNodes"`
	AvgResponseTime  float64                    `json:"avgResponseTime"`
	NodesWithRpcs    []NodeHealthData           `json:"nodesWithRpcs"`
	ChainHealthStats map[string]ChainHealthStat `json:"chainHealthStats"`
	DataAvailability NodeHealthAvailability     `json:"dataAvailability"`
}

// ChainHealthStat represents health statistics for a specific chain
type ChainHealthStat struct {
	ChainName       string  `json:"chainName"`
	HealthyNodes    int     `json:"healthyNodes"`
	TotalNodes      int     `json:"totalNodes"`
	AvgResponseTime float64 `json:"avgResponseTime"`
	Uptime          float64 `json:"uptime"`
}

// NodeHealthAvailability represents data availability for different timeframes
type NodeHealthAvailability struct {
	HasMinute bool `json:"hasMinute"`
	HasHour   bool `json:"hasHour"`
	HasDay    bool `json:"hasDay"`
	Has7Days  bool `json:"has7Days"`
	Has14Days bool `json:"has14Days"`
	Has30Days bool `json:"has30Days"`
} 