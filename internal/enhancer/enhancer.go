package enhancer

import (
	"context"
	"fmt"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/internal/utils"
	"websocket-backend-new/models"
)

// Config holds enhancer configuration
type Config struct {
	BufferSize int `json:"bufferSize"` // Buffer size for processing (default: 100)
}

// DefaultConfig returns default enhancer configuration
func DefaultConfig() Config {
	return Config{
		BufferSize: 100,
	}
}

// Enhancer handles enhancement of raw transfers
type Enhancer struct {
	config   Config
	channels *channels.Channels
}

// NewEnhancer creates a new enhancer
func NewEnhancer(config Config, channels *channels.Channels) *Enhancer {
	return &Enhancer{
		config:   config,
		channels: channels,
	}
}

// Start begins the enhancer thread
func (e *Enhancer) Start(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
			
		case transfers := <-e.channels.RawTransfers:
			utils.BatchInfo("ENHANCER", fmt.Sprintf("Received %d transfers from fetcher", len(transfers)))
			e.processTransferBatch(transfers)
		}
	}
}

// processTransferBatch processes a batch of raw transfers
func (e *Enhancer) processTransferBatch(transfers []models.Transfer) {
	if len(transfers) == 0 {
		return
	}
	
	enhanced := make([]models.Transfer, len(transfers))
	for i, transfer := range transfers {
		enhanced[i] = e.enhanceTransfer(transfer)
	}
	
	// Send enhanced transfers to scheduler with backpressure protection
	backpressureConfig := utils.DefaultBackpressureConfig()
	backpressureConfig.DropOnOverflow = false
	backpressureConfig.TimeoutMs = 100
	
	if utils.SendWithBackpressure(e.channels.EnhancedTransfers, enhanced, backpressureConfig, nil) {
		utils.BatchInfo("ENHANCER", fmt.Sprintf("Sent %d enhanced transfers to scheduler", len(enhanced)))
	} else {
		utils.BatchError("ENHANCER", fmt.Sprintf("Failed to send %d transfers to scheduler (timeout/overflow)", len(enhanced)))
	}
	
	// Send each transfer to stats collector with optimized sending
	statsCount := 0
	for _, transfer := range enhanced {
		if utils.TrySend(e.channels.StatsUpdates, transfer, nil) {
			statsCount++
		}
	}
	
	if statsCount > 0 {
		utils.BatchInfo("ENHANCER", fmt.Sprintf("Sent %d transfers to stats collector", statsCount))
	}
}

// enhanceTransfer enhances a single transfer with additional data
func (e *Enhancer) enhanceTransfer(transfer models.Transfer) models.Transfer {
	// Set TransferSendTxHash if missing (use PacketHash as fallback)
	if transfer.TransferSendTxHash == "" {
		transfer.TransferSendTxHash = transfer.PacketHash
	}
	
	// Set enhancement fields
	transfer.IsTestnetTransfer = transfer.SourceChain.Testnet || transfer.DestinationChain.Testnet
	transfer.FormattedTimestamp = transfer.TransferSendTimestamp.Format("2006-01-02 15:04:05")
	transfer.RouteKey = transfer.SourceChain.UniversalChainID + "_" + transfer.DestinationChain.UniversalChainID
	transfer.SourceDisplayName = transfer.SourceChain.DisplayName
	transfer.DestinationDisplayName = transfer.DestinationChain.DisplayName
	
	return transfer
}

 
 