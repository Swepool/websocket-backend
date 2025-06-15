package config

import (
	"time"
)

// Config holds the application configuration
// This is the single source of truth for all configuration options
type Config struct {
	Port            string        // Server port
	GraphQLEndpoint string        // GraphQL API endpoint
	PollInterval    int           // Polling interval in milliseconds
	PollLimit       int           // Number of transfers to fetch per polling cycle
	MainnetOnly     bool          // Filter to mainnet only (excludes testnet)
	LastSortOrder   string        // Optional: manually set starting point (skips historical data)
	
	// Real-time monitoring configuration
	TransferAgeThreshold time.Duration // Max age of transfers to broadcast in real-time (older ones stored but not shown)
	
	// Stats broadcasting configuration
	StatsBroadcastInterval time.Duration // Interval for broadcasting stats/charts (5-10s recommended)
	EnableStatsDeduplication bool        // Enable deduplication of unchanged stats data
}

// New creates a new Config instance with hardcoded values
// This is the ONLY place where configuration is defined
func New() *Config {
	config := &Config{
		Port:            "8080",
		GraphQLEndpoint: "https://staging.graphql.union.build/v1/graphql",
		PollInterval:    500,  // Optimized for real-time feel
		PollLimit:       100,  // Good balance for performance
		MainnetOnly:     false, // Include testnet by default
		LastSortOrder:   "", // Start from NOW instead of historical data to avoid memory issues
		
		// Real-time thresholds to prevent overwhelming clients with old data
		TransferAgeThreshold: 1 * time.Minute,  // Only broadcast transfers younger than 1 minute
		
		// Stats broadcasting configuration
		StatsBroadcastInterval: 7 * time.Second, // Broadcast every 7 seconds (5-10s range)
		EnableStatsDeduplication: true,          // Enable deduplication by default to reduce bandwidth
	}
	
	
	return config
}

// GetNetworkFilter returns the network filter based on configuration
func (c *Config) GetNetworkFilter() *string {
	if c.MainnetOnly {
		network := "mainnet"
		return &network
	}
	return nil // Return nil to get all networks (mainnet + testnet)
} 