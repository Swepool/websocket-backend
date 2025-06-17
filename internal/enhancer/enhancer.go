package enhancer

import (
	"context"
	"fmt"
	"websocket-backend-new/internal/channels"
	"websocket-backend-new/internal/models"
	"websocket-backend-new/internal/utils"
)

// Config holds enhancer configuration
type Config struct {
	// Future configuration options can be added here
}

// DefaultConfig returns default enhancer configuration
func DefaultConfig() Config {
	return Config{}
}

// Enhancer handles adding metadata to raw transfers
type Enhancer struct {
	config           Config
	channels         *channels.Channels
	addressFormatter *utils.AddressFormatter
}

// NewEnhancer creates a new enhancer
func NewEnhancer(config Config, channels *channels.Channels) *Enhancer {
	return &Enhancer{
		config:           config,
		channels:         channels,
		addressFormatter: utils.NewAddressFormatter(),
	}
}

// Start begins the enhancer thread
func (e *Enhancer) Start(ctx context.Context) {
	fmt.Printf("[ENHANCER] Starting\n")
	
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("[ENHANCER] Shutting down\n")
			return
			
		case transfers := <-e.channels.RawTransfers:
			e.processTransferBatch(transfers)
		}
	}
}

// processTransferBatch enhances a batch of raw transfers
func (e *Enhancer) processTransferBatch(transfers []models.Transfer) {
	if len(transfers) == 0 {
		return
	}
	
	fmt.Printf("[ENHANCER] Processing batch of %d transfers\n", len(transfers))
	
	// Enhance each transfer
	enhanced := make([]models.Transfer, len(transfers))
	for i, transfer := range transfers {
		enhanced[i] = e.enhanceTransfer(transfer)
	}
	
	// Send to scheduler
	select {
	case e.channels.EnhancedTransfers <- enhanced:
		fmt.Printf("[ENHANCER] Sent enhanced batch of %d transfers to scheduler\n", len(enhanced))
	default:
		fmt.Printf("[ENHANCER] Warning: Scheduler channel full, dropping enhanced batch of %d transfers\n", len(enhanced))
	}
	
	// ALSO send each transfer to stats collector (at true fetch rate, before scheduler delays)
	for _, transfer := range enhanced {
		select {
		case e.channels.StatsUpdates <- transfer:
			// Successfully sent to stats
		default:
			fmt.Printf("[ENHANCER] Warning: Stats channel full, dropping stats for transfer %s\n", transfer.PacketHash)
		}
	}
	
	fmt.Printf("[ENHANCER] Sent %d transfers to stats collector at true fetch rate\n", len(enhanced))
}

// enhanceTransfer adds metadata to a single transfer
func (e *Enhancer) enhanceTransfer(transfer models.Transfer) models.Transfer {
	// Make a copy to avoid modifying the original
	enhanced := transfer
	
	// Add server pre-computed fields that frontend expects
	enhanced.SenderDisplay = e.addressFormatter.FormatAddress(transfer.SenderCanonical, transfer.SourceChain.RpcType, transfer.SourceChain.AddrPrefix)
	enhanced.ReceiverDisplay = e.addressFormatter.FormatAddress(transfer.ReceiverCanonical, transfer.DestinationChain.RpcType, transfer.DestinationChain.AddrPrefix)
	enhanced.IsTestnetTransfer = transfer.SourceChain.Testnet || transfer.DestinationChain.Testnet
	enhanced.FormattedTimestamp = transfer.TransferSendTimestamp.Format("15:04:05")
	enhanced.RouteKey = fmt.Sprintf("%s->%s", transfer.SourceChain.UniversalChainID, transfer.DestinationChain.UniversalChainID)
	
	return enhanced
}

 