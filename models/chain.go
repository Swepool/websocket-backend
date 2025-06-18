package models

// Chain represents blockchain chain information  
type Chain struct {
	UniversalChainID string `json:"universal_chain_id"`
	ChainID          string `json:"chain_id"`
	DisplayName      string `json:"display_name"`
	Testnet          bool   `json:"testnet"`
	RpcType          string `json:"rpc_type"`
	AddrPrefix       string `json:"addr_prefix"`
	Rpcs             []Rpc  `json:"rpcs,omitempty"`
}

// Rpc represents an RPC endpoint
type Rpc struct {
	URL string `json:"url"`
}

// Native represents native token information (optional, for future use)
type Native struct {
	Name     string `json:"name"`
	Symbol   string `json:"symbol"`
	Decimals int    `json:"decimals"`
} 