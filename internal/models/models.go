package models

import "time"

// Transfer represents a cross-chain transfer matching the Union SDK schema
type Transfer struct {
	// Basic transfer info (Union SDK format)
	PacketHash              string    `json:"packet_hash"`
	SortOrder               string    `json:"sort_order"`
	TransferSendTimestamp   time.Time `json:"transfer_send_timestamp"`
	
	// Sender/Receiver (internal canonical addresses)
	SenderCanonical         string    `json:"sender_canonical"`
	ReceiverCanonical       string    `json:"receiver_canonical"`
	
	// Chain info (Union SDK nested format)
	SourceChain             Chain     `json:"source_chain"`
	DestinationChain        Chain     `json:"destination_chain"`
	
	// Token info
	BaseToken               string    `json:"base_token"`
	BaseTokenSymbol         string    `json:"base_token_symbol"`
	BaseTokenAmountDisplay  string    `json:"base_token_amount_display"`
	
	// Server pre-computed fields (what frontend expects)
	IsTestnetTransfer       bool      `json:"isTestnetTransfer"`
	SenderDisplay           string    `json:"senderDisplay,omitempty"`
	ReceiverDisplay         string    `json:"receiverDisplay,omitempty"`
	FormattedTimestamp      string    `json:"formattedTimestamp,omitempty"`
	RouteKey                string    `json:"routeKey,omitempty"`
}

// Chain represents blockchain information (Union SDK format)
type Chain struct {
	UniversalChainID string `json:"universal_chain_id"`
	ChainID          string `json:"chain_id"`
	DisplayName      string `json:"display_name"`
	Testnet          bool   `json:"testnet"`
	RpcType          string `json:"rpc_type"`
	AddrPrefix       string `json:"addr_prefix"`
}

// ChainInfo represents chain metadata for the frontend (what the chains store expects)
type ChainInfo struct {
	UniversalChainID string `json:"universal_chain_id"`
	ChainID          string `json:"chain_id"`
	DisplayName      string `json:"display_name"`
	Testnet          bool   `json:"testnet"`
	RpcType          string `json:"rpc_type"`
	AddrPrefix       string `json:"addr_prefix"`
	LogoURL          string `json:"logo_url,omitempty"`
	Enabled          bool   `json:"enabled"`
} 