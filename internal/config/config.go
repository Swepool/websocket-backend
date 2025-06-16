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
	StatsBroadcastInterval time.Duration // Interval for broadcasting stats/charts (15s recommended)
	EnableStatsDeduplication bool        // Enable deduplication of unchanged stats data
	
	// Stats data limits for broadcasting
	TopItemsLimit        int // Number of top items to return for routes, senders, receivers (chart data)
	TopItemsTimeScale    int // Number of top items to return for time scale data
	MaxWalletsInBroadcast int // Maximum number of wallets to include in broadcast (0 = unlimited)
	MaxAssetsInBroadcast  int // Maximum number of assets to include in broadcast (0 = unlimited)
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
		// Common values: 5*time.Second (very fast), 15*time.Second (fast), 30*time.Second (balanced), 60*time.Second (slow)
		StatsBroadcastInterval: 15 * time.Second, // Broadcast every 15 seconds
		EnableStatsDeduplication: true,           // Enable deduplication by default to reduce bandwidth
		
		// Stats data limits for broadcasting
		TopItemsLimit:        10,  // Return top 10 items for chart data (routes, senders, receivers)
		TopItemsTimeScale:    10,  // Return top 10 items for time scale data
		MaxWalletsInBroadcast: 10, // Limit to top 50 wallets in broadcast (0 = unlimited)
		MaxAssetsInBroadcast:  10, // Limit to top 20 assets in broadcast (0 = unlimited)
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