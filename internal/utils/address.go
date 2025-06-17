package utils

import (
	"encoding/hex"
	"strings"

	"github.com/btcsuite/btcutil/bech32"
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

	// Clean the canonical address
	cleanCanonical := strings.TrimSpace(canonicalAddress)
	
	// If the canonical address is already in bech32 format with correct prefix, return it
	if strings.HasPrefix(cleanCanonical, prefix+"1") && len(cleanCanonical) > 39 {
		return cleanCanonical
	}
	
	// If it's already a bech32 address but with different prefix, return canonical as-is
	// (proper conversion between different bech32 prefixes requires decoding/re-encoding)
	if af.isBech32Like(cleanCanonical) {
		return cleanCanonical
	}
	
	// If canonical address is hex, convert it to proper bech32 format
	if strings.HasPrefix(cleanCanonical, "0x") || af.isHex(cleanCanonical) {
		cleanHex := strings.TrimPrefix(cleanCanonical, "0x")
		if len(cleanHex) >= 32 { // At least 16 bytes
			return af.hexToBech32(cleanHex, prefix)
		}
	}
	
	// For other cases, return canonical address as-is
	return canonicalAddress
}

// formatEVMAddress formats an EVM address with proper 0x prefix and validation
func (af *AddressFormatter) formatEVMAddress(canonicalAddress string) string {
	// Clean the address
	cleanAddr := strings.TrimSpace(canonicalAddress)
	
	// If it's already a proper EVM address, return it (with proper case)
	if strings.HasPrefix(cleanAddr, "0x") && len(cleanAddr) == 42 {
		return af.normalizeEVMAddress(cleanAddr)
	}
	
	// If it's hex without 0x prefix, add it
	if af.isHex(cleanAddr) && len(cleanAddr) == 40 {
		return af.normalizeEVMAddress("0x" + cleanAddr)
	}
	
	// If it looks like a bech32 address, we can't easily convert it
	// This would require complex address derivation logic
	if af.isBech32Like(cleanAddr) {
		return canonicalAddress // Return as-is for now
	}
	
	// Handle other hex variations (with or without 0x, different lengths)
	hexAddr := strings.TrimPrefix(cleanAddr, "0x")
	if af.isHex(hexAddr) {
		// If it's shorter than 40 chars, pad with zeros
		if len(hexAddr) < 40 {
			hexAddr = strings.Repeat("0", 40-len(hexAddr)) + hexAddr
		}
		// Preserve full length - no truncation for longer addresses
		return af.normalizeEVMAddress("0x" + hexAddr)
	}
	
	// For non-hex addresses, return as-is
	return canonicalAddress
}

// normalizeEVMAddress ensures proper EVM address format (lowercase for now)
// In production, you might want to implement EIP-55 checksum encoding
func (af *AddressFormatter) normalizeEVMAddress(address string) string {
	if !strings.HasPrefix(address, "0x") {
		return address
	}
	
	// Convert to lowercase for consistency (EIP-55 checksumming would go here)
	// Preserve full length - no restriction to 42 characters
	return strings.ToLower(address)
}

// isHex checks if a string is valid hexadecimal
func (af *AddressFormatter) isHex(s string) bool {
	_, err := hex.DecodeString(s)
	return err == nil
}

// isBech32Like checks if an address looks like a bech32 address
func (af *AddressFormatter) isBech32Like(addr string) bool {
	// Basic check: contains "1" separator and has reasonable length
	parts := strings.Split(addr, "1")
	if len(parts) != 2 {
		return false
	}
	
	prefix := parts[0]
	data := parts[1]
	
	// Prefix should be 3-10 chars, data should be at least 30 chars for typical address
	return len(prefix) >= 3 && len(prefix) <= 10 && len(data) >= 30
}

// hexToBech32 converts a hex address to proper bech32 format
func (af *AddressFormatter) hexToBech32(hexAddr, prefix string) string {
	// Decode hex to bytes
	bytes, err := hex.DecodeString(hexAddr)
	if err != nil {
		return hexAddr // Return original if conversion fails
	}
	
	// For cosmos addresses, we typically use the first 20 bytes if longer
	// This matches the standard cosmos address format
	if len(bytes) > 20 {
		bytes = bytes[:20]
	}
	
	// Convert bytes to 5-bit groups for bech32 encoding
	conv, err := bech32.ConvertBits(bytes, 8, 5, true)
	if err != nil {
		return hexAddr // Return original if conversion fails
	}
	
	// Encode using proper bech32
	encoded, err := bech32.Encode(prefix, conv)
	if err != nil {
		return hexAddr // Return original if encoding fails
	}
	
	return encoded
}

// NewAddressFormatter creates a new AddressFormatter instance
func NewAddressFormatter() *AddressFormatter {
	return &AddressFormatter{}
}

// FormatAddressForDisplay formats an address for display (no truncation)
func FormatAddressForDisplay(addr string) string {
	return addr
} 