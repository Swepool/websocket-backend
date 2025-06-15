package utils

import (
	"encoding/hex"
	"fmt"
	"strings"

	"golang.org/x/crypto/ripemd160"
)

// AddressFormatter handles formatting addresses for different chain types
type AddressFormatter struct{}

// FormatAddress formats an address based on the chain type
// For Cosmos chains: constructs bech32 address using prefix + canonical address
// For EVM chains: formats as standard EVM address (0x...)
func (af *AddressFormatter) FormatAddress(canonicalAddress, rpcType, addrPrefix string) string {
	if canonicalAddress == "" {
		return ""
	}

	// Normalize rpc_type for comparison
	rpcType = strings.ToLower(strings.TrimSpace(rpcType))
	
	switch rpcType {
	case "cosmos", "cosmos-sdk":
		return af.formatCosmosAddress(canonicalAddress, addrPrefix)
	case "evm", "ethereum":
		return af.formatEVMAddress(canonicalAddress)
	default:
		// For unknown types, return canonical address as-is
		return canonicalAddress
	}
}

// formatCosmosAddress formats a Cosmos address using bech32 encoding
// This is a simplified implementation - you may want to use a proper bech32 library
func (af *AddressFormatter) formatCosmosAddress(canonicalAddress, prefix string) string {
	if prefix == "" {
		// If no prefix provided, return canonical address
		return canonicalAddress
	}

	// For now, we'll do a simple concatenation
	// In a production environment, you'd want to use proper bech32 encoding
	// like github.com/cosmos/cosmos-sdk/types/bech32
	
	// If the canonical address is already in bech32 format with correct prefix, return it
	if strings.HasPrefix(canonicalAddress, prefix+"1") {
		return canonicalAddress
	}
	
	// If canonical address is hex, convert it to bech32-like format
	if strings.HasPrefix(canonicalAddress, "0x") || af.isHex(canonicalAddress) {
		// This is a simplified approach - you should use proper bech32 library
		cleanHex := strings.TrimPrefix(canonicalAddress, "0x")
		if len(cleanHex) == 40 { // Standard address length
			// Convert to cosmos-style address (simplified)
			return af.hexToBech32(cleanHex, prefix)
		}
	}
	
	// Fallback: return canonical address as-is
	return canonicalAddress
}

// formatEVMAddress formats an EVM address with proper 0x prefix
func (af *AddressFormatter) formatEVMAddress(canonicalAddress string) string {
	// Clean the address
	cleanAddr := strings.TrimSpace(canonicalAddress)
	
	// If it's already a proper EVM address, return it
	if strings.HasPrefix(cleanAddr, "0x") && len(cleanAddr) == 42 {
		return cleanAddr
	}
	
	// If it's hex without 0x prefix, add it
	if af.isHex(cleanAddr) && len(cleanAddr) == 40 {
		return "0x" + cleanAddr
	}
	
	// If it's not hex, it might be a cosmos address that needs conversion
	// This is complex and would require proper address conversion
	// For now, return as-is
	return canonicalAddress
}

// isHex checks if a string is valid hexadecimal
func (af *AddressFormatter) isHex(s string) bool {
	_, err := hex.DecodeString(s)
	return err == nil
}

// hexToBech32 converts a hex address to a bech32-like format (simplified)
// This is a basic implementation - use proper bech32 library in production
func (af *AddressFormatter) hexToBech32(hexAddr, prefix string) string {
	// This is a very simplified conversion
	// In reality, you'd need proper bech32 encoding with checksum
	
	// Decode hex to bytes
	bytes, err := hex.DecodeString(hexAddr)
	if err != nil {
		return hexAddr // Return original if conversion fails
	}
	
	// Hash the bytes (simplified - real bech32 uses different encoding)
	hasher := ripemd160.New()
	hasher.Write(bytes)
	hash := hasher.Sum(nil)
	
	// Convert back to hex (this is NOT proper bech32, just a placeholder)
	hashHex := hex.EncodeToString(hash)
	
	// Return with prefix (this is a simplified format, not real bech32)
	return fmt.Sprintf("%s1%s", prefix, hashHex[:20])
}

// NewAddressFormatter creates a new AddressFormatter instance
func NewAddressFormatter() *AddressFormatter {
	return &AddressFormatter{}
} 