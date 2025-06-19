package database

import "time"

// DataAvailability tracks what time periods have data available
type DataAvailability struct {
	HasMinute bool `json:"hasMinute"`
	HasHour   bool `json:"hasHour"`
	HasDay    bool `json:"hasDay"`
	Has7Days  bool `json:"has7Days"`
	Has14Days bool `json:"has14Days"`
	Has30Days bool `json:"has30Days"`
}

// FrontendTransferRates represents transfer rate data for the frontend
type FrontendTransferRates struct {
	// Current period counts
	TxPerMinute  int64 `json:"txPerMinute"`
	TxPerHour    int64 `json:"txPerHour"`
	TxPerDay     int64 `json:"txPerDay"`
	TxPer7Days   int64 `json:"txPer7Days"`
	TxPer14Days  int64 `json:"txPer14Days"`
	TxPer30Days  int64 `json:"txPer30Days"`

	// Percentage changes
	PercentageChangeMin  float64 `json:"percentageChangeMin"`
	PercentageChangeHour float64 `json:"percentageChangeHour"`
	PercentageChangeDay  float64 `json:"percentageChangeDay"`
	PercentageChange7Day float64 `json:"percentageChange7Day"`
	PercentageChange14Day float64 `json:"percentageChange14Day"`
	PercentageChange30Day float64 `json:"percentageChange30Day"`

	// Pointer fields for optional percentage changes
	TxPerMinuteChange  *float64 `json:"txPerMinuteChange,omitempty"`
	TxPerHourChange    *float64 `json:"txPerHourChange,omitempty"`
	TxPerDayChange     *float64 `json:"txPerDayChange,omitempty"`
	TxPer7DaysChange   *float64 `json:"txPer7DaysChange,omitempty"`
	TxPer14DaysChange  *float64 `json:"txPer14DaysChange,omitempty"`
	TxPer30DaysChange  *float64 `json:"txPer30DaysChange,omitempty"`

	// Metadata
	UniqueSendersTotal   int64            `json:"uniqueSendersTotal"`
	UniqueReceiversTotal int64            `json:"uniqueReceiversTotal"`
	TotalTracked         int64            `json:"totalTracked"`
	LastUpdateTime       time.Time        `json:"lastUpdateTime"`
	ServerUptimeSeconds  float64          `json:"serverUptimeSeconds"`
	DataAvailability     DataAvailability `json:"dataAvailability"`
}

// FrontendRouteData represents route information for the frontend
type FrontendRouteData struct {
	FromChain    string    `json:"fromChain"`
	ToChain      string    `json:"toChain"`
	FromName     string    `json:"fromName"`
	ToName       string    `json:"toName"`
	Route        string    `json:"route"`
	Count        int64     `json:"count"`
	Volume       float64   `json:"volume"`
	Percentage   float64   `json:"percentage"`
	LastActivity string    `json:"lastActivity"`
}

// FrontendChainFlow represents chain flow data for the frontend
type FrontendChainFlow struct {
	UniversalChainID string                `json:"universalChainId"`
	ChainName        string                `json:"chainName"`
	OutgoingCount    int64                 `json:"outgoingCount"`
	IncomingCount    int64                 `json:"incomingCount"`
	NetFlow          int64                 `json:"netFlow"`
	LastActivity     string                `json:"lastActivity"`
	TopAssets        []FrontendChainAsset  `json:"topAssets"`
}

// FrontendChainAsset represents asset data within a chain
type FrontendChainAsset struct {
	AssetSymbol     string  `json:"assetSymbol"`
	AssetName       string  `json:"assetName"`
	OutgoingCount   int64   `json:"outgoingCount"`
	IncomingCount   int64   `json:"incomingCount"`
	NetFlow         int64   `json:"netFlow"`
	TotalVolume     float64 `json:"totalVolume"`
	AverageAmount   float64 `json:"averageAmount"`
	Percentage      float64 `json:"percentage"`
	LastActivity    string  `json:"lastActivity"`
}

// FrontendAsset represents asset volume data for the frontend
type FrontendAsset struct {
	AssetSymbol      string                `json:"assetSymbol"`
	AssetName        string                `json:"assetName"`
	TransferCount    int64                 `json:"transferCount"`
	TotalVolume      float64               `json:"totalVolume"`
	LargestTransfer  float64               `json:"largestTransfer"`
	AverageAmount    float64               `json:"averageAmount"`
	Percentage       float64               `json:"percentage"`
	LastActivity     string                `json:"lastActivity"`
	TopRoutes        []FrontendAssetRoute  `json:"topRoutes"`
}

// FrontendAssetRoute represents route data within an asset
type FrontendAssetRoute struct {
	FromChain    string  `json:"fromChain"`
	ToChain      string  `json:"toChain"`
	FromName     string  `json:"fromName"`
	ToName       string  `json:"toName"`
	Route        string  `json:"route"`
	Count        int64   `json:"count"`
	Volume       float64 `json:"volume"`
	Percentage   float64 `json:"percentage"`
	LastActivity string  `json:"lastActivity"`
}

// FrontendWalletData represents wallet activity data for the frontend
type FrontendWalletData struct {
	Address        string `json:"address"`
	DisplayAddress string `json:"displayAddress"`
	Count          int64  `json:"count"`
	LastActivity   string `json:"lastActivity"`
}

// Helper function
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
} 