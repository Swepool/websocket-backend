package fetcher

import (
	"context"
	"fmt"
	"log"
	"websocket-backend-new/models"
	"websocket-backend-new/storage"
)

// Enhancer handles enhancing transfers with metadata
type Enhancer struct {
	memory   *storage.Memory
	channels *storage.Channels
}

// NewEnhancer creates a new enhancer
func NewEnhancer(memory *storage.Memory, channels *storage.Channels) *Enhancer {
	return &Enhancer{
		memory:   memory,
		channels: channels,
	}
}

// Start begins the enhancement loop (Thread 2: Enhance)
func (e *Enhancer) Start(ctx context.Context) {
	log.Printf("[THREAD-2] 🚀 Starting enhancer (Add metadata)")
	
	for {
		select {
		case <-ctx.Done():
			log.Printf("[THREAD-2] 🛑 Context cancelled, stopping enhancer")
			return
			
		case <-e.channels.Shutdown:
			log.Printf("[THREAD-2] 🛑 Shutdown signal received, stopping")
			return
			
		case rawTransfers := <-e.channels.RawTransfers:
			e.enhanceAndSend(rawTransfers)
		}
	}
}

// enhanceAndSend enhances transfers and sends them to scheduler
func (e *Enhancer) enhanceAndSend(transfers []models.Transfer) {
	if len(transfers) == 0 {
		return
	}
	
	log.Printf("[THREAD-2] ✨ Enhancing %d transfers with metadata", len(transfers))
	
	// Get chain data
	chains := e.memory.GetChains()
	
	// Enhance each transfer
	enhanced := make([]models.Transfer, 0, len(transfers))
	for _, transfer := range transfers {
		enhancedTransfer := e.enhanceSingleTransfer(transfer, chains)
		enhanced = append(enhanced, enhancedTransfer)
	}
	
	log.Printf("[THREAD-2] ✅ Enhanced %d transfers", len(enhanced))
	
	// Send to scheduler (non-blocking)
	select {
	case e.channels.EnhancedTransfers <- enhanced:
		log.Printf("[THREAD-2] 📤 Sent %d enhanced transfers to scheduler", len(enhanced))
	default:
		log.Printf("[THREAD-2] ⚠️ Scheduler channel full, dropping %d transfers", len(enhanced))
	}
}

// enhanceSingleTransfer adds metadata to a single transfer
func (e *Enhancer) enhanceSingleTransfer(transfer models.Transfer, chains []models.Chain) models.Transfer {
	// Add metadata
	transfer.IsTestnetTransfer = transfer.SourceChain.Testnet || transfer.DestinationChain.Testnet
	transfer.FormattedTimestamp = transfer.TransferSendTimestamp.Format("2006-01-02T15:04:05Z07:00")
	transfer.RouteKey = fmt.Sprintf("%s-%s", transfer.SourceChain.UniversalChainID, transfer.DestinationChain.UniversalChainID)
	
	// Get chain display names
	transfer.SourceDisplayName = e.getChainDisplayName(chains, transfer.SourceChain.UniversalChainID, transfer.SourceChain.DisplayName)
	transfer.DestinationDisplayName = e.getChainDisplayName(chains, transfer.DestinationChain.UniversalChainID, transfer.DestinationChain.DisplayName)
	
	// Format addresses if not already formatted
	if transfer.SenderDisplay == "" {
		transfer.SenderDisplay = e.formatAddress(transfer.SenderCanonical)
	}
	if transfer.ReceiverDisplay == "" {
		transfer.ReceiverDisplay = e.formatAddress(transfer.ReceiverCanonical)
	}
	
	return transfer
}

// getChainDisplayName gets the best display name for a chain
func (e *Enhancer) getChainDisplayName(chains []models.Chain, universalChainID, fallbackName string) string {
	for _, chain := range chains {
		if chain.UniversalChainID == universalChainID {
			if chain.DisplayName != "" {
				return chain.DisplayName
			}
			if chain.ChainID != "" {
				return chain.ChainID
			}
		}
	}
	
	if fallbackName != "" {
		return fallbackName
	}
	
	return universalChainID
}

// formatAddress formats address for display (no truncation)
func (e *Enhancer) formatAddress(address string) string {
	return address
}

// EnhanceTransfers enhances transfers with chain metadata (for backward compatibility)
func (e *Enhancer) EnhanceTransfers(transfers []models.Transfer) []models.Transfer {
	if len(transfers) == 0 {
		return transfers
	}
	
	chains := e.memory.GetChains()
	
	enhanced := make([]models.Transfer, len(transfers))
	for i, transfer := range transfers {
		enhanced[i] = e.enhanceSingleTransfer(transfer, chains)
	}
	
	return enhanced
}

// buildChainMap creates a map for fast chain lookup (for backward compatibility)
func (e *Enhancer) buildChainMap(chains []models.Chain) map[string]models.Chain {
	chainMap := make(map[string]models.Chain)
	for _, chain := range chains {
		chainMap[chain.UniversalChainID] = chain
	}
	return chainMap
}

// mergeChainInfo merges embedded chain info with full chain data (for backward compatibility)
func (e *Enhancer) mergeChainInfo(embedded models.Chain, full models.Chain) models.Chain {
	// Use full chain data, but preserve embedded data if full is missing
	result := full
	
	if result.DisplayName == "" && embedded.DisplayName != "" {
		result.DisplayName = embedded.DisplayName
	}
	
	if result.ChainID == "" && embedded.ChainID != "" {
		result.ChainID = embedded.ChainID
	}
	
	// Ensure we have a display name
	if result.DisplayName == "" {
		if result.ChainID != "" {
			result.DisplayName = result.ChainID
		} else {
			result.DisplayName = result.UniversalChainID
		}
	}
	
	return result
} 