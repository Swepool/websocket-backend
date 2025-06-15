package models

// ChainFilter represents a filter for transfers by chain
type ChainFilter struct {
	FromChain string `json:"fromChain"`
	ToChain   string `json:"toChain"`
}

// GetFilterKey generates a unique key for the filter for grouping clients
func (cf *ChainFilter) GetFilterKey() string {
	if cf == nil {
		return "no-filter"
	}
	return cf.FromChain + "->" + cf.ToChain
}

// GetFilterKeyFromFilter generates a filter key from a ChainFilter pointer (handles nil)
func GetFilterKeyFromFilter(filter *ChainFilter) string {
	if filter == nil {
		return "no-filter"
	}
	return filter.GetFilterKey()
}

// MatchesFilter checks if a transfer matches the given filter
func (t *Transfer) MatchesFilter(filter *ChainFilter) bool {
	if filter == nil {
		return true
	}
	
	sourceID := t.SourceChain.UniversalChainID
	destID := t.DestinationChain.UniversalChainID
	
	// If both from and to chains are specified, both must match
	if filter.FromChain != "" && filter.ToChain != "" {
		return sourceID == filter.FromChain && destID == filter.ToChain
	}
	
	// If only from chain is specified
	if filter.FromChain != "" && filter.ToChain == "" {
		return sourceID == filter.FromChain
	}
	
	// If only to chain is specified
	if filter.FromChain == "" && filter.ToChain != "" {
		return destID == filter.ToChain
	}
	
	// If no filter is specified, match all
	return true
}

// MatchesFilter checks if a broadcast transfer matches the given filter
func (bt *BroadcastTransfer) MatchesFilter(filter *ChainFilter) bool {
	if filter == nil {
		return true
	}
	
	sourceID := bt.SourceChain.UniversalChainID
	destID := bt.DestinationChain.UniversalChainID
	
	// If both from and to chains are specified, both must match
	if filter.FromChain != "" && filter.ToChain != "" {
		return sourceID == filter.FromChain && destID == filter.ToChain
	}
	
	// If only from chain is specified
	if filter.FromChain != "" && filter.ToChain == "" {
		return sourceID == filter.FromChain
	}
	
	// If only to chain is specified
	if filter.FromChain == "" && filter.ToChain != "" {
		return destID == filter.ToChain
	}
	
	// If no filter is specified, match all
	return true
} 