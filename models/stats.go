package models

import "time"

// ChartData represents aggregated chart data for frontend
type ChartData struct {
	Timestamp         time.Time   `json:"timestamp"`
	TotalTransfers    int64       `json:"totalTransfers"`
	TransferRates     interface{} `json:"transferRates"`
	TopRoutes         interface{} `json:"topRoutes"`
	ChainFlows        interface{} `json:"chainFlows"`
	TopAssets         interface{} `json:"topAssets"`
	ActiveWallets     interface{} `json:"activeWallets"`
	NetworkStats      interface{} `json:"networkStats"`
	LastUpdated       time.Time   `json:"lastUpdated"`
}

// TransferRate represents transfer rate over time
type TransferRate struct {
	Timestamp time.Time `json:"timestamp"`
	Count     int       `json:"count"`
	Period    string    `json:"period"` // "1min", "5min", "1hour"
}

// PopularRoute represents a popular transfer route
type PopularRoute struct {
	SourceChain      string `json:"sourceChain"`
	DestinationChain string `json:"destinationChain"`
	Count            int    `json:"count"`
	DisplayName      string `json:"displayName"`
}

// ActiveSender represents an active sender
type ActiveSender struct {
	Address       string `json:"address"`
	FormattedAddr string `json:"formattedAddr"`
	Count         int    `json:"count"`
}

// ActiveReceiver represents an active receiver
type ActiveReceiver struct {
	Address       string `json:"address"`
	FormattedAddr string `json:"formattedAddr"`
	Count         int    `json:"count"`
}

// TopAsset represents a top transferred asset
type TopAsset struct {
	Symbol string `json:"symbol"`
	Count  int    `json:"count"`
}

// StatsUpdate represents a stats update message between threads
type StatsUpdate struct {
	Transfers []Transfer `json:"transfers"`
	Timestamp time.Time  `json:"timestamp"`
}

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