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
// Temporarily includes all fields to maintain frontend compatibility
// TODO: Optimize by removing unused fields after frontend analysis
type BroadcastTransfer struct {
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
	BaseToken              string    `json:"base_token"`              // Re-added for frontend compatibility
	BaseAmount             string    `json:"base_amount"`             // Re-added for frontend compatibility
	QuoteToken             string    `json:"quote_token"`             // Re-added for frontend compatibility
	QuoteAmount            string    `json:"quote_amount"`            // Re-added for frontend compatibility
	SortOrder              string    `json:"sort_order"`              // Re-added for frontend compatibility
	IsTestnetTransfer      bool      `json:"isTestnetTransfer"`
	SourceDisplayName      string    `json:"sourceDisplayName"`
	DestinationDisplayName string    `json:"destinationDisplayName"`
	FormattedTimestamp     string    `json:"formattedTimestamp"`
	RouteKey               string    `json:"routeKey"`
	SenderDisplay          string    `json:"senderDisplay"`
	ReceiverDisplay        string    `json:"receiverDisplay"`
}

// ToBroadcastTransfer converts a Transfer to a BroadcastTransfer with all fields
func (t *Transfer) ToBroadcastTransfer() *BroadcastTransfer {
	return &BroadcastTransfer{
		SourceChain:            t.SourceChain,
		DestinationChain:       t.DestinationChain,
		SenderCanonical:        t.SenderCanonical,
		ReceiverCanonical:      t.ReceiverCanonical,
		TransferSendTimestamp:  t.TransferSendTimestamp,
		TransferSendTxHash:     t.TransferSendTxHash,
		TransferRecvTimestamp:  t.TransferRecvTimestamp,
		PacketHash:             t.PacketHash,
		BaseToken:              t.BaseToken,              // Re-added
		BaseAmount:             t.BaseAmount,             // Re-added
		QuoteToken:             t.QuoteToken,             // Re-added
		QuoteAmount:            t.QuoteAmount,            // Re-added
		SortOrder:              t.SortOrder,              // Re-added
		IsTestnetTransfer:      t.IsTestnetTransfer,
		SourceDisplayName:      t.SourceDisplayName,
		DestinationDisplayName: t.DestinationDisplayName,
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
}

 