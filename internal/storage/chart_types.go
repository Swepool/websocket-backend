package storage

import "time"



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
	UniqueSendersTotal   int64     `json:"uniqueSendersTotal"`
	UniqueReceiversTotal int64     `json:"uniqueReceiversTotal"`
	TotalTracked         int64     `json:"totalTracked"`
	LastUpdateTime       time.Time `json:"lastUpdateTime"`
	ServerUptimeSeconds  float64   `json:"serverUptimeSeconds"`
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

// FrontendChainFlowData represents chain flow data for the frontend
type FrontendChainFlowData struct {
	Chains              []FrontendChainData            `json:"chains"`
	ChainFlowTimeScale  map[string][]FrontendChainData `json:"chainFlowTimeScale"`
	TotalOutgoing       int64                          `json:"totalOutgoing"`
	TotalIncoming       int64                          `json:"totalIncoming"`
	ServerUptimeSeconds float64                        `json:"serverUptimeSeconds"`
}

// FrontendChainData represents individual chain data for the frontend
type FrontendChainData struct {
	UniversalChainID string                 `json:"universal_chain_id"`
	ChainName        string                 `json:"chainName"`
	OutgoingCount    int64                  `json:"outgoingCount"`
	IncomingCount    int64                  `json:"incomingCount"`
	NetFlow          int64                  `json:"netFlow"`
	OutgoingChange   *float64               `json:"outgoingChange,omitempty"`
	IncomingChange   *float64               `json:"incomingChange,omitempty"`
	NetFlowChange    *float64               `json:"netFlowChange,omitempty"`
	LastActivity     string                 `json:"lastActivity"`
	TopAssets        []FrontendChainAsset   `json:"topAssets,omitempty"`
}

// FrontendChainAsset represents asset data within a chain for the frontend
type FrontendChainAsset struct {
	AssetSymbol   string  `json:"assetSymbol"`
	AssetName     string  `json:"assetName"`
	OutgoingCount int64   `json:"outgoingCount"`
	IncomingCount int64   `json:"incomingCount"`
	NetFlow       int64   `json:"netFlow"`
	TotalVolume   float64 `json:"totalVolume"`
	AverageAmount float64 `json:"averageAmount"`
	Percentage    float64 `json:"percentage"`
	LastActivity  string  `json:"lastActivity"`
}

// ActiveWalletRates represents wallet activity data for the frontend
type ActiveWalletRates struct {
	// Senders per time period
	SendersLastMin  int64 `json:"sendersLastMin"`
	SendersLastHour int64 `json:"sendersLastHour"`
	SendersLastDay  int64 `json:"sendersLastDay"`
	SendersLast7d   int64 `json:"sendersLast7d"`
	SendersLast14d  int64 `json:"sendersLast14d"`
	SendersLast30d  int64 `json:"sendersLast30d"`

	// Receivers per time period
	ReceiversLastMin  int64 `json:"receiversLastMin"`
	ReceiversLastHour int64 `json:"receiversLastHour"`
	ReceiversLastDay  int64 `json:"receiversLastDay"`
	ReceiversLast7d   int64 `json:"receiversLast7d"`
	ReceiversLast14d  int64 `json:"receiversLast14d"`
	ReceiversLast30d  int64 `json:"receiversLast30d"`

	// Total unique wallets per time period
	TotalLastMin  int64 `json:"totalLastMin"`
	TotalLastHour int64 `json:"totalLastHour"`
	TotalLastDay  int64 `json:"totalLastDay"`
	TotalLast7d   int64 `json:"totalLast7d"`
	TotalLast14d  int64 `json:"totalLast14d"`
	TotalLast30d  int64 `json:"totalLast30d"`

	// Percentage changes (optional)
	SendersLastMinChange  *float64 `json:"sendersLastMinChange,omitempty"`
	SendersLastHourChange *float64 `json:"sendersLastHourChange,omitempty"`
	SendersLastDayChange  *float64 `json:"sendersLastDayChange,omitempty"`
	SendersLast7dChange   *float64 `json:"sendersLast7dChange,omitempty"`
	SendersLast14dChange  *float64 `json:"sendersLast14dChange,omitempty"`
	SendersLast30dChange  *float64 `json:"sendersLast30dChange,omitempty"`

	ReceiversLastMinChange  *float64 `json:"receiversLastMinChange,omitempty"`
	ReceiversLastHourChange *float64 `json:"receiversLastHourChange,omitempty"`
	ReceiversLastDayChange  *float64 `json:"receiversLastDayChange,omitempty"`
	ReceiversLast7dChange   *float64 `json:"receiversLast7dChange,omitempty"`
	ReceiversLast14dChange  *float64 `json:"receiversLast14dChange,omitempty"`
	ReceiversLast30dChange  *float64 `json:"receiversLast30dChange,omitempty"`

	TotalLastMinChange  *float64 `json:"totalLastMinChange,omitempty"`
	TotalLastHourChange *float64 `json:"totalLastHourChange,omitempty"`
	TotalLastDayChange  *float64 `json:"totalLastDayChange,omitempty"`
	TotalLast7dChange   *float64 `json:"totalLast7dChange,omitempty"`
	TotalLast14dChange  *float64 `json:"totalLast14dChange,omitempty"`
	TotalLast30dChange  *float64 `json:"totalLast30dChange,omitempty"`

	// Totals across all time
	UniqueSendersTotal   int64   `json:"uniqueSendersTotal"`
	UniqueReceiversTotal int64   `json:"uniqueReceiversTotal"`
	UniqueTotalWallets   int64   `json:"uniqueTotalWallets"`
	ServerUptimeSeconds  float64 `json:"serverUptimeSeconds"`
}

// FrontendAssetVolumeData represents complete asset volume data for the frontend
type FrontendAssetVolumeData struct {
	Assets               []FrontendAsset                `json:"assets"`
	AssetVolumeTimeScale map[string][]FrontendAsset     `json:"assetVolumeTimeScale"`
	TotalAssets          int64                          `json:"totalAssets"`
	TotalVolume          float64                        `json:"totalVolume"`
	TotalTransfers       int64                          `json:"totalTransfers"`
	ServerUptimeSeconds  float64                        `json:"serverUptimeSeconds"`
}

// Helper function
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
} 