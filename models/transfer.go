package models

import "time"

// Transfer represents a blockchain transfer
type Transfer struct {
	// Core identifiers
	TransferSendTxHash string `json:"transfer_send_transaction_hash"`
	SortOrder          string `json:"sort_order"`
	
	// Timing
	TransferSendTimestamp time.Time `json:"transfer_send_timestamp"`
	
	// Amount and token
	BaseAmount      string `json:"base_amount"`
	BaseTokenSymbol string `json:"base_token_symbol"`
	
	// Addresses (canonical format)
	SenderCanonical   string `json:"sender_canonical"`
	ReceiverCanonical string `json:"receiver_canonical"`
	
	// Display addresses (if available from GraphQL)
	SenderDisplay   string `json:"sender_display"`
	ReceiverDisplay string `json:"receiver_display"`
	
	// Chain information
	SourceChain      Chain `json:"source_chain"`
	DestinationChain Chain `json:"destination_chain"`
	
	// Additional fields
	PacketHash string `json:"packet_hash"`
	
	// Enhancement fields (added during processing)
	IsTestnetTransfer      bool   `json:"isTestnetTransfer"`
	FormattedTimestamp     string `json:"formattedTimestamp"`
	RouteKey               string `json:"routeKey"`
	SourceDisplayName      string `json:"sourceDisplayName"`
	DestinationDisplayName string `json:"destinationDisplayName"`
}

// BroadcastTransfer represents transfer data sent to WebSocket clients
// This format MUST match exactly what the frontend expects
type BroadcastTransfer struct {
	SourceChain struct {
		UniversalChainID string `json:"universal_chain_id"`
		DisplayName      string `json:"display_name"`
	} `json:"source_chain"`
	DestinationChain struct {
		UniversalChainID string `json:"universal_chain_id"`
		DisplayName      string `json:"display_name"`
	} `json:"destination_chain"`
	TransferSendTxHash      string    `json:"transfer_send_transaction_hash"`
	PacketHash              string    `json:"packet_hash"`
	SortOrder               string    `json:"sort_order"`             	
	IsTestnetTransfer       bool      `json:"isTestnetTransfer"`
	FormattedTimestamp      string    `json:"formattedTimestamp"`
	RouteKey                string    `json:"routeKey"`
	SenderDisplay           string    `json:"senderDisplay"`
	ReceiverDisplay         string    `json:"receiverDisplay"`
	
	// Additional fields needed for broadcasting and display
	BaseAmount              string    `json:"baseAmount"`
	BaseTokenSymbol         string    `json:"baseTokenSymbol"`
	TransferSendTimestamp   time.Time `json:"transferSendTimestamp"`
}

// ToBroadcastTransfer converts Transfer to BroadcastTransfer format
func (t Transfer) ToBroadcastTransfer() BroadcastTransfer {
	// Format timestamp as string
	formattedTimestamp := t.TransferSendTimestamp.Format("2006-01-02 15:04:05")
	
	// Create route key
	routeKey := t.SourceChain.UniversalChainID + "_" + t.DestinationChain.UniversalChainID
	
	// Determine if it's a testnet transfer
	isTestnet := t.SourceChain.Testnet || t.DestinationChain.Testnet
	
	// Use display addresses if available, otherwise use canonical
	senderDisplay := t.SenderDisplay
	if senderDisplay == "" {
		senderDisplay = t.SenderCanonical
	}
	
	receiverDisplay := t.ReceiverDisplay
	if receiverDisplay == "" {
		receiverDisplay = t.ReceiverCanonical
	}
	
	return BroadcastTransfer{
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
		IsTestnetTransfer:      isTestnet,
		FormattedTimestamp:     formattedTimestamp,
		RouteKey:               routeKey,
		SenderDisplay:          senderDisplay,
		ReceiverDisplay:        receiverDisplay,
		BaseAmount:             t.BaseAmount,
		BaseTokenSymbol:        t.BaseTokenSymbol,
		TransferSendTimestamp:  t.TransferSendTimestamp,
	}
} 