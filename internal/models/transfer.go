package models

import (
	"time"
)

// Transfer represents a blockchain transfer (full version for server storage)
type Transfer struct {
	SourceChain struct {
		UniversalChainID string `json:"universal_chain_id"`
		DisplayName      string `json:"display_name"`
		ChainID          string `json:"chain_id"`
		Testnet          bool   `json:"testnet"`
	} `json:"source_chain"`
	DestinationChain struct {
		UniversalChainID string `json:"universal_chain_id"`
		DisplayName      string `json:"display_name"`
		ChainID          string `json:"chain_id"`
		Testnet          bool   `json:"testnet"`
	} `json:"destination_chain"`
	SenderCanonical        string    `json:"sender_canonical"`
	ReceiverCanonical      string    `json:"receiver_canonical"`
	TransferSendTimestamp  time.Time `json:"transfer_send_timestamp"`
	TransferSendTxHash     string    `json:"transfer_send_transaction_hash"`
	TransferRecvTimestamp  time.Time `json:"transfer_recv_timestamp"`
	PacketHash             string    `json:"packet_hash"`
	BaseToken              string    `json:"base_token"`
	BaseAmount             string    `json:"base_amount"`
	QuoteToken             string    `json:"quote_token"`
	QuoteAmount            string    `json:"quote_amount"`
	SortOrder              string    `json:"sort_order"`
	IsTestnetTransfer      bool      `json:"isTestnetTransfer"`
	SourceDisplayName      string    `json:"sourceDisplayName"`
	DestinationDisplayName string    `json:"destinationDisplayName"`
	FormattedTimestamp     string    `json:"formattedTimestamp"`
	RouteKey               string    `json:"routeKey"`
	SenderDisplay          string    `json:"senderDisplay"`
	ReceiverDisplay        string    `json:"receiverDisplay"`
}

// BroadcastTransfer represents a transfer structure for broadcasting
// Optimized to exclude unnecessary fields to save on broadcast data
type BroadcastTransfer struct {
	SourceChain struct {
		UniversalChainID string `json:"universal_chain_id"`
		DisplayName      string `json:"display_name"`
	} `json:"source_chain"`
	DestinationChain struct {
		UniversalChainID string `json:"universal_chain_id"`
		DisplayName      string `json:"display_name"`
	} `json:"destination_chain"`
	TransferSendTxHash     string    `json:"transfer_send_transaction_hash"`
	PacketHash             string    `json:"packet_hash"`
	SortOrder              string    `json:"sort_order"`             	
	IsTestnetTransfer      bool      `json:"isTestnetTransfer"`
	FormattedTimestamp     string    `json:"formattedTimestamp"`
	RouteKey               string    `json:"routeKey"`
	SenderDisplay          string    `json:"senderDisplay"`
	ReceiverDisplay        string    `json:"receiverDisplay"`
}

// ToBroadcastTransfer converts a Transfer to a BroadcastTransfer with optimized fields
func (t *Transfer) ToBroadcastTransfer() *BroadcastTransfer {
	return &BroadcastTransfer{
		SourceChain: struct {
			UniversalChainID string `json:"universal_chain_id"`
			DisplayName      string `json:"display_name"`
		}{
			UniversalChainID: t.SourceChain.UniversalChainID,
			DisplayName:      t.SourceChain.DisplayName,
		},
		DestinationChain: struct {
			UniversalChainID string `json:"universal_chain_id"`
			DisplayName      string `json:"display_name"`
		}{
			UniversalChainID: t.DestinationChain.UniversalChainID,
			DisplayName:      t.DestinationChain.DisplayName,
		},
		TransferSendTxHash:     t.TransferSendTxHash,
		PacketHash:             t.PacketHash,
		SortOrder:              t.SortOrder,             
		IsTestnetTransfer:      t.IsTestnetTransfer,
		FormattedTimestamp:     t.FormattedTimestamp,
		RouteKey:               t.RouteKey,
		SenderDisplay:          t.SenderDisplay,
		ReceiverDisplay:        t.ReceiverDisplay,
	}
}

// Chain represents blockchain information
type Chain struct {
	UniversalChainID string `json:"universal_chain_id"`
	DisplayName      string `json:"display_name"`
	ChainID          string `json:"chain_id"`
	Testnet          bool   `json:"testnet"`
	RpcType          string `json:"rpc_type"`
	AddrPrefix       string `json:"addr_prefix"`
}

 