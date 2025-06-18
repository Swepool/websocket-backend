package processor

import (
	"context"
	"fmt"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/internal/utils"
	"websocket-backend-new/models"
)

// Config holds processor configuration
type Config struct {
	BufferSize int `json:"bufferSize"` // Buffer size for processing (default: 100)
}

// DefaultConfig returns default processor configuration
func DefaultConfig() Config {
	return Config{
		BufferSize: 100,
	}
}

// Processor handles processing of raw transfers and routing to scheduler & database
type Processor struct {
	config   Config
	channels *channels.Channels
}

// NewProcessor creates a new processor
func NewProcessor(config Config, channels *channels.Channels) *Processor {
	return &Processor{
		config:   config,
		channels: channels,
	}
}

// Start begins the processor thread
func (p *Processor) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
			
		case transfers := <-p.channels.RawTransfers:
			utils.BatchInfo("PROCESSOR", fmt.Sprintf("Received %d transfers from fetcher", len(transfers)))
			p.processTransferBatch(transfers)
		}
	}
}

// processTransferBatch processes a batch of raw transfers
func (p *Processor) processTransferBatch(transfers []models.Transfer) {
	if len(transfers) == 0 {
		return
	}
	
	processed := make([]models.Transfer, len(transfers))
	for i, transfer := range transfers {
		processed[i] = p.processTransfer(transfer)
	}
	
	// Send processed transfers to scheduler for timed broadcasting
	backpressureConfig := utils.DefaultBackpressureConfig()
	backpressureConfig.DropOnOverflow = false
	backpressureConfig.TimeoutMs = 100
	
	if utils.SendWithBackpressure(p.channels.ProcessedTransfers, processed, backpressureConfig, nil) {
		utils.BatchInfo("PROCESSOR", fmt.Sprintf("Sent %d processed transfers to scheduler", len(processed)))
	} else {
		utils.BatchError("PROCESSOR", fmt.Sprintf("Failed to send %d transfers to scheduler (timeout/overflow)", len(processed)))
	}
	
	// Note: Database saving is now handled directly by fetcher (raw transfers)
}

// processTransfer processes a single transfer with additional data
func (p *Processor) processTransfer(transfer models.Transfer) models.Transfer {
	// Set TransferSendTxHash if missing (use PacketHash as fallback)
	if transfer.TransferSendTxHash == "" {
		transfer.TransferSendTxHash = transfer.PacketHash
	}
	
	// Set processing fields
	transfer.IsTestnetTransfer = transfer.SourceChain.Testnet || transfer.DestinationChain.Testnet
	transfer.FormattedTimestamp = transfer.TransferSendTimestamp.Format("2006-01-02 15:04:05")
	transfer.RouteKey = transfer.SourceChain.UniversalChainID + "_" + transfer.DestinationChain.UniversalChainID
	transfer.SourceDisplayName = transfer.SourceChain.DisplayName
	transfer.DestinationDisplayName = transfer.DestinationChain.DisplayName
	
	return transfer
}

// GetStats returns processor statistics
func (p *Processor) GetStats() map[string]interface{} {
	return map[string]interface{}{
		"bufferSize":       p.config.BufferSize,
		"processingMode":   "enhanced_field_processing",
		"fieldsProcessed": []string{
			"TransferSendTxHash",
			"IsTestnetTransfer", 
			"FormattedTimestamp",
			"RouteKey",
			"SourceDisplayName",
			"DestinationDisplayName",
		},
		"fallbackLogic": map[string]interface{}{
			"TransferSendTxHash": "PacketHash fallback enabled",
		},
		"performance": map[string]interface{}{
			"batchProcessing": true,
			"backpressureEnabled": true,
			"timeoutMs": 100,
		},
	}
} 