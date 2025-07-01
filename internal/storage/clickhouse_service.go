package storage

import (
	"context"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"
	"websocket-backend/internal/utils"
	"websocket-backend/models"

	"github.com/ClickHouse/clickhouse-go/v2"
	clickhouse_driver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ClickHouseConfig holds the configuration for ClickHouse connection
type ClickHouseConfig struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
	Debug    bool
}

// ClickHouseService handles ClickHouse operations for analytics with caching
type ClickHouseService struct {
	conn               clickhouse_driver.Conn
	config             ClickHouseConfig
	cache              *ChartDataCache
	cacheConfig        CacheConfig
	latencyService     LatencyServiceInterface
	nodeHealthService  NodeHealthServiceInterface
}

// NewClickHouseService creates a new ClickHouse service
func NewClickHouseService(config ClickHouseConfig) (*ClickHouseService, error) {
	// Create connection options
	options := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", config.Host, config.Port)},
		Auth: clickhouse.Auth{
			Database: config.Database,
			Username: config.Username,
			Password: config.Password,
		},
		Debug: config.Debug,
		Debugf: func(format string, v ...interface{}) {
			if config.Debug {
				utils.LogInfo("CLICKHOUSE_DEBUG", format, v...)
			}
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 30,          // Query timeout (client-level)
			"max_memory_usage":   "4000000000", // Max memory per query (client-level)
			"max_threads":        8,            // Max threads per query (client-level)
			// Server-level settings moved to config.xml
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:     15 * time.Second,
		MaxOpenConns:    8,   // Increased for 16-core system (but still controlled)
		MaxIdleConns:    4,   // Keep some idle connections for performance
		ConnMaxLifetime: 30 * time.Minute, // Shorter lifetime
	}

	// Create connection
	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}

	// Test connection
	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	utils.LogInfo("CLICKHOUSE", "Connected to ClickHouse at %s:%d", config.Host, config.Port)

	// Initialize cache with default configuration
	cacheConfig := DefaultCacheConfig()
	cache := NewChartDataCache(cacheConfig.DefaultTTL)

	return &ClickHouseService{
		conn:        conn,
		config:      config,
		cache:       cache,
		cacheConfig: cacheConfig,
	}, nil
}

// Close closes the ClickHouse connection
func (c *ClickHouseService) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// SetLatencyService sets the latency service for initial chart data
func (c *ClickHouseService) SetLatencyService(latencyService LatencyServiceInterface) {
	c.latencyService = latencyService
}

// SetNodeHealthService sets the node health service for initial chart data
func (c *ClickHouseService) SetNodeHealthService(nodeHealthService NodeHealthServiceInterface) {
	c.nodeHealthService = nodeHealthService
}

// InitializeSchema creates the ClickHouse tables and views
func (c *ClickHouseService) InitializeSchema(ctx context.Context) error {
	utils.LogInfo("CLICKHOUSE", "Initializing ClickHouse schema...")

	// Use the external schema file instead of hardcoded SQL
	schemaPath := "internal/storage/clickhouse_schema.sql"
	schemaBytes, err := os.ReadFile(schemaPath)
	if err != nil {
		return fmt.Errorf("failed to read schema file %s: %w", schemaPath, err)
	}

	schemaSQL := string(schemaBytes)
	utils.LogInfo("CLICKHOUSE", "Read schema file successfully, %d bytes", len(schemaBytes))

	// Better SQL parsing: Split by semicolons but handle multi-line statements
	statements := []string{}
	current := ""
	
	lines := strings.Split(schemaSQL, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		
		current += " " + line
		
		// If line ends with semicolon, we have a complete statement
		if strings.HasSuffix(line, ";") {
			stmt := strings.TrimSpace(current)
			stmt = strings.TrimSuffix(stmt, ";") // Remove trailing semicolon
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current = ""
		}
	}
	
	// Add any remaining statement
	if current != "" {
		stmt := strings.TrimSpace(current)
		if stmt != "" {
			statements = append(statements, stmt)
		}
	}

	utils.LogInfo("CLICKHOUSE", "Parsed %d SQL statements from schema", len(statements))

	// Execute each statement
	for i, statement := range statements {
		utils.LogInfo("CLICKHOUSE", "Executing statement %d: %.100s...", i+1, statement)
		
		if err := c.conn.Exec(ctx, statement); err != nil {
			utils.LogError("CLICKHOUSE", "Failed to execute statement %d: %s", i+1, statement)
			return fmt.Errorf("failed to execute schema statement %d: %w", i+1, err)
		}
		
		utils.LogInfo("CLICKHOUSE", "Statement %d executed successfully", i+1)
	}

	utils.LogInfo("CLICKHOUSE", "Schema initialized successfully - %d statements executed", len(statements))
	return nil
}

// InsertTransfers inserts transfers into ClickHouse (using existing schema)
func (c *ClickHouseService) InsertTransfers(ctx context.Context, transfers []models.Transfer) error {
	if len(transfers) == 0 {
		return nil
	}

	// Prepare batch insert using existing schema (no base_token field)
	batch, err := c.conn.PrepareBatch(ctx, `
		INSERT INTO transfers_analytics (
			id, packet_hash, sort_order, source_chain, dest_chain, source_name, dest_name,
			sender, receiver, amount, token_symbol, canonical_token_symbol, timestamp, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, transfer := range transfers {
		// Generate unique ID for this transfer
		id := uint64(time.Now().UnixNano())

		// Parse amount safely
		amount := parseAmount(transfer.BaseAmount)

		err := batch.Append(
			id,
			transfer.PacketHash,
			transfer.SortOrder,
			transfer.SourceChain.UniversalChainID,
			transfer.DestinationChain.UniversalChainID,
			transfer.SourceChain.DisplayName,
			transfer.DestinationChain.DisplayName,
			transfer.SenderCanonical,
			transfer.ReceiverCanonical,
			amount,
			transfer.BaseTokenSymbol,      // Display symbol
			transfer.CanonicalTokenSymbol, // Now properly calculated in GraphQL client
			transfer.TransferSendTimestamp,
			time.Now(),
		)
		if err != nil {
			return fmt.Errorf("failed to append transfer %s: %w", transfer.PacketHash, err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

// InsertLatencyData inserts latency data into ClickHouse
func (c *ClickHouseService) InsertLatencyData(ctx context.Context, data []models.LatencyData) error {
	if len(data) == 0 {
		return nil
	}

	batch, err := c.conn.PrepareBatch(ctx, `
		INSERT INTO latency_analytics (
			id, source_chain, dest_chain, source_name, dest_name,
			packet_ack_p5, packet_ack_median, packet_ack_p95,
			packet_recv_p5, packet_recv_median, packet_recv_p95,
			write_ack_p5, write_ack_median, write_ack_p95,
			fetched_at, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare latency batch: %w", err)
	}

	for i, latency := range data {
		// Generate unique ID using timestamp + counter
		uniqueID := uint64(time.Now().UnixNano()) + uint64(i)
		err := batch.Append(
			uniqueID,
			latency.SourceChain,
			latency.DestinationChain,
			latency.SourceName,
			latency.DestinationName,
			latency.PacketAck.P5,
			latency.PacketAck.Median,
			latency.PacketAck.P95,
			latency.PacketRecv.P5,
			latency.PacketRecv.Median,
			latency.PacketRecv.P95,
			latency.WriteAck.P5,
			latency.WriteAck.Median,
			latency.WriteAck.P95,
			time.Now(), // FetchedAt - current time
			time.Now(),
		)
		if err != nil {
			return fmt.Errorf("failed to append latency data to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send latency batch: %w", err)
	}

	utils.LogInfo("CLICKHOUSE", "Inserted %d latency records", len(data))
	return nil
}



// GetTransferRates gets optimized transfer rate analytics with caching (uses actual blockchain timestamp for accurate stats)
func (c *ClickHouseService) GetTransferRates(ctx context.Context) (*FrontendTransferRates, error) {
	// Try cache first
	if data, hit := c.cache.Get("transfer_rates"); hit {
		utils.LogDebug("CACHE", "Cache hit for transfer_rates")
		return data.(*FrontendTransferRates), nil
	}
	
	// Cache miss - fetch from database
	utils.LogDebug("CACHE", "Cache miss for transfer_rates, fetching from ClickHouse")
	data, err := c.getTransferRatesFromDB(ctx)
	if err != nil {
		return nil, err
	}
	
	// Store in cache
	c.cache.SetWithTTL("transfer_rates", data, c.cacheConfig.TransferRatesTTL)
	return data, nil
}

// getTransferRatesFromDB performs the actual database query for transfer rates
func (c *ClickHouseService) getTransferRatesFromDB(ctx context.Context) (*FrontendTransferRates, error) {
	query := `
		SELECT 
			countIf(timestamp >= now() - INTERVAL 5 MINUTE) as tx_per_minute,
			countIf(timestamp >= now() - INTERVAL 1 HOUR) as tx_per_hour,
			countIf(timestamp >= now() - INTERVAL 1 DAY) as tx_per_day,
			countIf(timestamp >= now() - INTERVAL 7 DAY) as tx_per_7days,
			countIf(timestamp >= now() - INTERVAL 14 DAY) as tx_per_14days,
			countIf(timestamp >= now() - INTERVAL 30 DAY) as tx_per_30days,
			uniqIf(sender, timestamp >= now() - INTERVAL 30 DAY) as unique_senders,
			uniqIf(receiver, timestamp >= now() - INTERVAL 30 DAY) as unique_receivers,
			count() as total_tracked,
			max(timestamp) as last_update
		FROM transfers_analytics
	`

	var (
		txPerMinute, txPerHour, txPerDay, txPer7Days, txPer14Days, txPer30Days uint64
		uniqueSenders, uniqueReceivers, totalTracked uint64
		lastUpdate time.Time
	)

	err := c.conn.QueryRow(ctx, query).Scan(
		&txPerMinute, &txPerHour, &txPerDay, &txPer7Days, &txPer14Days, &txPer30Days,
		&uniqueSenders, &uniqueReceivers, &totalTracked, &lastUpdate,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query transfer rates: %w", err)
	}

	return &FrontendTransferRates{
		TxPerMinute:          int64(txPerMinute),
		TxPerHour:            int64(txPerHour),
		TxPerDay:             int64(txPerDay),
		TxPer7Days:           int64(txPer7Days),
		TxPer14Days:          int64(txPer14Days),
		TxPer30Days:          int64(txPer30Days),
		UniqueSendersTotal:   int64(uniqueSenders),
		UniqueReceiversTotal: int64(uniqueReceivers),
		TotalTracked:         int64(totalTracked),
		LastUpdateTime:       lastUpdate,
		ServerUptimeSeconds:  time.Since(lastUpdate).Seconds(),
	}, nil
}

// GetActiveWalletRates gets wallet activity analytics with caching for different time periods (uses actual blockchain timestamp for accurate stats)
func (c *ClickHouseService) GetActiveWalletRates(ctx context.Context) (*ActiveWalletRates, error) {
	// Try cache first
	if data, hit := c.cache.Get("active_wallet_rates"); hit {
		utils.LogDebug("CACHE", "Cache hit for active_wallet_rates")
		return data.(*ActiveWalletRates), nil
	}
	
	// Cache miss - fetch from database
	utils.LogDebug("CACHE", "Cache miss for active_wallet_rates, fetching from ClickHouse")
	data, err := c.getActiveWalletRatesFromDB(ctx)
	if err != nil {
		return nil, err
	}
	
	// Store in cache
	c.cache.SetWithTTL("active_wallet_rates", data, c.cacheConfig.ActiveWalletsTTL)
	return data, nil
}

// getActiveWalletRatesFromDB performs the actual database query for active wallet rates
func (c *ClickHouseService) getActiveWalletRatesFromDB(ctx context.Context) (*ActiveWalletRates, error) {
	query := `
		SELECT 
			-- Senders per time period
			uniqIf(sender, timestamp >= now() - INTERVAL 5 MINUTE) as senders_min,
			uniqIf(sender, timestamp >= now() - INTERVAL 1 HOUR) as senders_hour,
			uniqIf(sender, timestamp >= now() - INTERVAL 1 DAY) as senders_day,
			uniqIf(sender, timestamp >= now() - INTERVAL 7 DAY) as senders_7d,
			uniqIf(sender, timestamp >= now() - INTERVAL 14 DAY) as senders_14d,
			uniqIf(sender, timestamp >= now() - INTERVAL 30 DAY) as senders_30d,
			
			-- Receivers per time period
			uniqIf(receiver, timestamp >= now() - INTERVAL 5 MINUTE) as receivers_min,
			uniqIf(receiver, timestamp >= now() - INTERVAL 1 HOUR) as receivers_hour,
			uniqIf(receiver, timestamp >= now() - INTERVAL 1 DAY) as receivers_day,
			uniqIf(receiver, timestamp >= now() - INTERVAL 7 DAY) as receivers_7d,
			uniqIf(receiver, timestamp >= now() - INTERVAL 14 DAY) as receivers_14d,
			uniqIf(receiver, timestamp >= now() - INTERVAL 30 DAY) as receivers_30d,
			
			-- Total unique wallets (use separate subqueries to avoid arrayJoin complexity)
			(uniqIf(sender, timestamp >= now() - INTERVAL 5 MINUTE) + uniqIf(receiver, timestamp >= now() - INTERVAL 5 MINUTE)) as total_min,
			(uniqIf(sender, timestamp >= now() - INTERVAL 1 HOUR) + uniqIf(receiver, timestamp >= now() - INTERVAL 1 HOUR)) as total_hour,
			(uniqIf(sender, timestamp >= now() - INTERVAL 1 DAY) + uniqIf(receiver, timestamp >= now() - INTERVAL 1 DAY)) as total_day,
			(uniqIf(sender, timestamp >= now() - INTERVAL 7 DAY) + uniqIf(receiver, timestamp >= now() - INTERVAL 7 DAY)) as total_7d,
			(uniqIf(sender, timestamp >= now() - INTERVAL 14 DAY) + uniqIf(receiver, timestamp >= now() - INTERVAL 14 DAY)) as total_14d,
			(uniqIf(sender, timestamp >= now() - INTERVAL 30 DAY) + uniqIf(receiver, timestamp >= now() - INTERVAL 30 DAY)) as total_30d,
			
			-- All-time totals
			uniq(sender) as unique_senders_total,
			uniq(receiver) as unique_receivers_total,
			(uniq(sender) + uniq(receiver)) as unique_total_wallets,
			
			max(timestamp) as last_update
		FROM transfers_analytics
	`

	var (
		sendersMin, sendersHour, sendersDay, senders7d, senders14d, senders30d uint64
		receiversMin, receiversHour, receiversDay, receivers7d, receivers14d, receivers30d uint64
		totalMin, totalHour, totalDay, total7d, total14d, total30d uint64
		uniqueSendersTotal, uniqueReceiversTotal, uniqueTotalWallets uint64
		lastUpdate time.Time
	)

	err := c.conn.QueryRow(ctx, query).Scan(
		&sendersMin, &sendersHour, &sendersDay, &senders7d, &senders14d, &senders30d,
		&receiversMin, &receiversHour, &receiversDay, &receivers7d, &receivers14d, &receivers30d,
		&totalMin, &totalHour, &totalDay, &total7d, &total14d, &total30d,
		&uniqueSendersTotal, &uniqueReceiversTotal, &uniqueTotalWallets,
		&lastUpdate,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to query active wallet rates: %w", err)
	}

	return &ActiveWalletRates{
		SendersLastMin:       int64(sendersMin),
		SendersLastHour:      int64(sendersHour),
		SendersLastDay:       int64(sendersDay),
		SendersLast7d:        int64(senders7d),
		SendersLast14d:       int64(senders14d),
		SendersLast30d:       int64(senders30d),
		ReceiversLastMin:     int64(receiversMin),
		ReceiversLastHour:    int64(receiversHour),
		ReceiversLastDay:     int64(receiversDay),
		ReceiversLast7d:      int64(receivers7d),
		ReceiversLast14d:     int64(receivers14d),
		ReceiversLast30d:     int64(receivers30d),
		TotalLastMin:         int64(totalMin),
		TotalLastHour:        int64(totalHour),
		TotalLastDay:         int64(totalDay),
		TotalLast7d:          int64(total7d),
		TotalLast14d:         int64(total14d),
		TotalLast30d:         int64(total30d),
		UniqueSendersTotal:   int64(uniqueSendersTotal),
		UniqueReceiversTotal: int64(uniqueReceiversTotal),
		UniqueTotalWallets:   int64(uniqueTotalWallets),
		ServerUptimeSeconds:  time.Since(lastUpdate).Seconds(),
	}, nil
}

// GetChartDataForFrontend gets comprehensive chart data for the frontend with caching (implements broadcaster interface)
func (c *ClickHouseService) GetChartDataForFrontend() (map[string]interface{}, error) {
	// Try cache first for the complete chart data package
	if data, hit := c.cache.Get("chart_data_frontend"); hit {
		utils.LogDebug("CACHE", "Cache hit for chart_data_frontend")
		return data.(map[string]interface{}), nil
	}
	
	// Cache miss - build chart data from individual cached components
	ctx := context.Background()
	utils.LogDebug("CACHE", "Cache miss for chart_data_frontend, building from components")
	
	// Get transfer rates (uses its own cache)
	transferRates, err := c.GetTransferRates(ctx)
	if err != nil {
		utils.LogError("CLICKHOUSE", "Failed to get transfer rates: %v", err)
		return nil, fmt.Errorf("failed to get transfer rates: %w", err)
	}
	
	// Get active wallet rates (uses its own cache)
	activeWalletRates, err := c.GetActiveWalletRates(ctx)
	if err != nil {
		utils.LogError("CLICKHOUSE", "Failed to get active wallet rates: %v", err)
		return nil, fmt.Errorf("failed to get active wallet rates: %w", err)
	}
	
	// Get popular routes for default timeframe (last 7 days)
	popularRoutes, err := c.GetPopularRoutes(ctx, 20, "7d")
	if err != nil {
		utils.LogError("CLICKHOUSE", "Failed to get popular routes: %v", err)
		popularRoutes = []FrontendRouteData{} // Default to empty
	}
	
	// Get popular routes for all timeframes (for timeScale data)
	timeframes := []string{"5m", "1h", "1d", "7d", "14d", "30d"}
	popularRoutesTimeScale := make(map[string][]FrontendRouteData)
	for _, tf := range timeframes {
		routes, err := c.GetPopularRoutes(ctx, 20, tf)
		if err != nil {
			utils.LogDebug("CLICKHOUSE", "Failed to get routes for timeframe %s: %v", tf, err)
			routes = []FrontendRouteData{}
		}
		popularRoutesTimeScale[tf] = routes
	}
	
	// Get chain flow data for default timeframe (last day)
	utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: Getting chain flow data for chart frontend")
	chainFlowData, err := c.GetChainFlowData(ctx, "1d")
	if err != nil {
		utils.LogError("CLICKHOUSE", "❌ DEBUG: Failed to get chain flow data: %v", err)
		// Create empty chain flow data
		chainFlowData = &FrontendChainFlowData{
			Chains:              []FrontendChainData{},
			ChainFlowTimeScale:  make(map[string][]FrontendChainData),
			TotalOutgoing:       0,
			TotalIncoming:       0,
			ServerUptimeSeconds: 0,
		}
	} else {
		utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: Got chain flow data with %d chains", len(chainFlowData.Chains))
	}
	
	// Get chain flow data for all timeframes (for timeScale data)
	chainFlowTimeScale := make(map[string][]FrontendChainData)
	for _, tf := range timeframes {
		chainData, err := c.GetChainFlowData(ctx, tf)
		if err != nil {
			utils.LogDebug("CLICKHOUSE", "Failed to get chain flow for timeframe %s: %v", tf, err)
			chainFlowTimeScale[tf] = []FrontendChainData{}
		} else {
			chainFlowTimeScale[tf] = chainData.Chains
		}
	}
	
	// Update chain flow data with timeScale
	chainFlowData.ChainFlowTimeScale = chainFlowTimeScale
	
	// Get wallet activity for default timeframe (last hour)
	activeSenders, err := c.GetTopSenders(ctx, 10, "1h")
	if err != nil {
		utils.LogError("CLICKHOUSE", "Failed to get active senders: %v", err)
		activeSenders = []FrontendWalletData{} // Default to empty
	}
	
	activeReceivers, err := c.GetTopReceivers(ctx, 10, "1h")
	if err != nil {
		utils.LogError("CLICKHOUSE", "Failed to get active receivers: %v", err)
		activeReceivers = []FrontendWalletData{} // Default to empty
	}
	
	// Get wallet activity for all timeframes (for timeScale data)
	activeSendersTimeScale := make(map[string][]FrontendWalletData)
	activeReceiversTimeScale := make(map[string][]FrontendWalletData)
	
	for _, tf := range timeframes {
		senders, err := c.GetTopSenders(ctx, 10, tf)
		if err != nil {
			utils.LogDebug("CLICKHOUSE", "Failed to get senders for timeframe %s: %v", tf, err)
			senders = []FrontendWalletData{}
		}
		activeSendersTimeScale[tf] = senders
		
		receivers, err := c.GetTopReceivers(ctx, 10, tf)
		if err != nil {
			utils.LogDebug("CLICKHOUSE", "Failed to get receivers for timeframe %s: %v", tf, err)
			receivers = []FrontendWalletData{}
		}
		activeReceiversTimeScale[tf] = receivers
	}
	
	// Get asset volume data for initial client connections
	var assetVolumeData *FrontendAssetVolumeData
	assetVolumeData, err = c.GetAssetVolumes(ctx, "1h")
	if err != nil {
		utils.LogError("CLICKHOUSE", "Failed to get asset volumes for initial data: %v", err)
		// Create empty asset data
		assetVolumeData = &FrontendAssetVolumeData{
			Assets:               []FrontendAsset{},
			AssetVolumeTimeScale: make(map[string][]FrontendAsset),
			TotalAssets:          0,
			TotalVolume:          0,
			TotalTransfers:       0,
			ServerUptimeSeconds:  0,
		}
	}

	// Get asset volume data for all timeframes (for timeScale data)
	// timeframes already declared above
	assetVolumeTimeScale := make(map[string][]FrontendAsset)
	for _, tf := range timeframes {
		assetData, err := c.GetAssetVolumes(ctx, tf)
		if err != nil {
			utils.LogDebug("CLICKHOUSE", "Failed to get asset volumes for timeframe %s in initial data: %v", tf, err)
			assetVolumeTimeScale[tf] = []FrontendAsset{}
		} else {
			assetVolumeTimeScale[tf] = assetData.Assets
		}
	}

	// Update asset volume data with timeScale
	assetVolumeData.AssetVolumeTimeScale = assetVolumeTimeScale

	// Get latency data for initial client connections
	var latencyData []interface{}
	if c.latencyService != nil {
		latencyData = c.latencyService.GetLatencyDataInterface()
		utils.LogInfo("CLICKHOUSE", "Retrieved %d latency data points for initial client data", len(latencyData))
	} else {
		utils.LogWarn("CLICKHOUSE", "No latency service available for initial client data")
		latencyData = []interface{}{}
	}

	// Get node health data for initial client connections
	var nodeHealthData interface{}
	if c.nodeHealthService != nil {
		// Get the summary object directly, not as an array
		if healthDataArray := c.nodeHealthService.GetHealthDataInterface(); len(healthDataArray) > 0 {
			nodeHealthData = healthDataArray[0] // Extract the first (and only) summary object
			utils.LogInfo("CLICKHOUSE", "Retrieved node health summary data for initial client data")
		} else {
			nodeHealthData = nil
			utils.LogWarn("CLICKHOUSE", "No node health data available for initial client data")
		}
	} else {
		utils.LogWarn("CLICKHOUSE", "No node health service available for initial client data")
		nodeHealthData = nil
	}

	// Build combined response with asset volume data and latency data included
	chartData := map[string]interface{}{
		"currentRates":             transferRates,     // Frontend expects "currentRates", not "transferRates"
		"activeWalletRates":        activeWalletRates,
		"popularRoutes":            popularRoutes,
		"popularRoutesTimeScale":   popularRoutesTimeScale,   // Popular routes by timeframe
		"activeSenders":            activeSenders,            // Individual wallet data
		"activeReceivers":          activeReceivers,          // Individual wallet data
		"activeSendersTimeScale":   activeSendersTimeScale,   // Wallet data by timeframe
		"activeReceiversTimeScale": activeReceiversTimeScale, // Wallet data by timeframe
		"chainFlowData":            chainFlowData,            // Chain flow data with timeScale
		"assetVolumeData":          assetVolumeData,          // Asset volume data with timeScale - NOW INCLUDED!
		"latencyData":              latencyData,              // Latency data for cross-chain monitoring
		"nodeHealthData":           nodeHealthData,           // Node health data for monitoring
		"lastUpdated":              time.Now().Format("2006-01-02 15:04:05"),
		"dataSource":               "clickhouse",
		"cached":                   false, // Indicate this was freshly built
	}
	
	// Cache the complete chart data package
	c.cache.SetWithTTL("chart_data_frontend", chartData, c.cacheConfig.ChartDataTTL)
	
	utils.LogInfo("CLICKHOUSE", "Chart data built and cached successfully (includes asset volume data with %d assets, latency data with %d points, node health data)", len(assetVolumeData.Assets), len(latencyData))
	return chartData, nil
}

// GetPopularRoutes gets the most popular transfer routes with caching
func (c *ClickHouseService) GetPopularRoutes(ctx context.Context, limit int, timeframe string) ([]FrontendRouteData, error) {
	// Use limit and timeframe in cache key to handle different requests
	cacheKey := fmt.Sprintf("popular_routes_%s_%d", timeframe, limit)
	
	// Try cache first
	if data, hit := c.cache.Get(cacheKey); hit {
		utils.LogDebug("CACHE", "Cache hit for %s", cacheKey)
		return data.([]FrontendRouteData), nil
	}
	
	// Cache miss - fetch from database
	utils.LogDebug("CACHE", "Cache miss for %s, fetching from ClickHouse", cacheKey)
	data, err := c.getPopularRoutesFromDB(ctx, limit, timeframe)
	if err != nil {
		return nil, err
	}
	
	// Store in cache
	c.cache.SetWithTTL(cacheKey, data, c.cacheConfig.PopularRoutesTTL)
	return data, nil
}

// getPopularRoutesFromDB performs the actual database query for popular routes
func (c *ClickHouseService) getPopularRoutesFromDB(ctx context.Context, limit int, timeframe string) ([]FrontendRouteData, error) {
	// Map timeframe to SQL interval
	intervalMap := map[string]string{
		"5m":  "5 MINUTE",
		"1h":  "1 HOUR",
		"1d":  "1 DAY",
		"7d":  "7 DAY",
		"14d": "14 DAY",
		"30d": "30 DAY",
	}
	
	interval, exists := intervalMap[timeframe]
	if !exists {
		return nil, fmt.Errorf("invalid timeframe: %s", timeframe)
	}
	
	query := fmt.Sprintf(`
		SELECT 
			source_chain,
			dest_chain,
			source_name,
			dest_name,
			route,
			count() as transfer_count,
			sum(amount) as total_volume,
			max(timestamp) as last_activity
		FROM transfers_analytics 
		WHERE timestamp >= now() - INTERVAL %s
		GROUP BY source_chain, dest_chain, source_name, dest_name, route
		ORDER BY transfer_count DESC
		LIMIT ?
	`, interval)

	rows, err := c.conn.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query popular routes: %w", err)
	}
	defer rows.Close()

	var routes []FrontendRouteData
	var totalTransfers int64

	// First pass to calculate total for percentages
	for rows.Next() {
		var route FrontendRouteData
		var transferCount uint64  // ClickHouse count() returns UInt64
		var lastActivity time.Time
		
		err := rows.Scan(
			&route.FromChain, &route.ToChain, &route.FromName, &route.ToName,
			&route.Route, &transferCount, &route.Volume, &lastActivity,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan route: %w", err)
		}

		route.Count = int64(transferCount)  // Convert UInt64 to int64
		route.LastActivity = lastActivity.Format("2006-01-02 15:04:05")
		routes = append(routes, route)
		totalTransfers += route.Count
	}

	// Calculate percentages
	for i := range routes {
		if totalTransfers > 0 {
			routes[i].Percentage = float64(routes[i].Count) / float64(totalTransfers) * 100
		}
	}

	return routes, nil
}

// GetTopSenders gets the most active senders for different time periods with caching
func (c *ClickHouseService) GetTopSenders(ctx context.Context, limit int, timeframe string) ([]FrontendWalletData, error) {
	cacheKey := fmt.Sprintf("top_senders_%s_%d", timeframe, limit)
	
	// Try cache first
	if data, hit := c.cache.Get(cacheKey); hit {
		utils.LogDebug("CACHE", "Cache hit for %s", cacheKey)
		return data.([]FrontendWalletData), nil
	}
	
	// Cache miss - fetch from database
	utils.LogDebug("CACHE", "Cache miss for %s, fetching from ClickHouse", cacheKey)
	data, err := c.getTopSendersFromDB(ctx, limit, timeframe)
	if err != nil {
		return nil, err
	}
	
	// Store in cache
	c.cache.SetWithTTL(cacheKey, data, c.cacheConfig.ActiveWalletsTTL)
	return data, nil
}

// getTopSendersFromDB performs the actual database query for top senders
func (c *ClickHouseService) getTopSendersFromDB(ctx context.Context, limit int, timeframe string) ([]FrontendWalletData, error) {
	// Map timeframe to SQL interval
	intervalMap := map[string]string{
		"5m":  "5 MINUTE",
		"1h":  "1 HOUR", 
		"1d":  "1 DAY",
		"7d":  "7 DAY",
		"14d": "14 DAY",
		"30d": "30 DAY",
	}
	
	interval, exists := intervalMap[timeframe]
	if !exists {
		return nil, fmt.Errorf("invalid timeframe: %s", timeframe)
	}
	
	query := fmt.Sprintf(`
		SELECT 
			sender as address,
			count() as transfer_count,
			max(timestamp) as last_activity
		FROM transfers_analytics 
		WHERE timestamp >= now() - INTERVAL %s
		GROUP BY sender
		ORDER BY transfer_count DESC, last_activity DESC
		LIMIT ?
	`, interval)

	rows, err := c.conn.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query top senders: %w", err)
	}
	defer rows.Close()

	var wallets []FrontendWalletData
	for rows.Next() {
		var wallet FrontendWalletData
		var transferCount uint64
		var lastActivity time.Time
		
		err := rows.Scan(&wallet.Address, &transferCount, &lastActivity)
		if err != nil {
			return nil, fmt.Errorf("failed to scan sender: %w", err)
		}

		wallet.Count = int64(transferCount)
		wallet.DisplayAddress = formatWalletAddress(wallet.Address)
		wallet.LastActivity = lastActivity.Format("2006-01-02 15:04:05")
		wallets = append(wallets, wallet)
	}

	return wallets, nil
}

// GetTopReceivers gets the most active receivers for different time periods with caching
func (c *ClickHouseService) GetTopReceivers(ctx context.Context, limit int, timeframe string) ([]FrontendWalletData, error) {
	cacheKey := fmt.Sprintf("top_receivers_%s_%d", timeframe, limit)
	
	// Try cache first
	if data, hit := c.cache.Get(cacheKey); hit {
		utils.LogDebug("CACHE", "Cache hit for %s", cacheKey)
		return data.([]FrontendWalletData), nil
	}
	
	// Cache miss - fetch from database
	utils.LogDebug("CACHE", "Cache miss for %s, fetching from ClickHouse", cacheKey)
	data, err := c.getTopReceiversFromDB(ctx, limit, timeframe)
	if err != nil {
		return nil, err
	}
	
	// Store in cache
	c.cache.SetWithTTL(cacheKey, data, c.cacheConfig.ActiveWalletsTTL)
	return data, nil
}

// getTopReceiversFromDB performs the actual database query for top receivers
func (c *ClickHouseService) getTopReceiversFromDB(ctx context.Context, limit int, timeframe string) ([]FrontendWalletData, error) {
	// Map timeframe to SQL interval
	intervalMap := map[string]string{
		"5m":  "5 MINUTE",
		"1h":  "1 HOUR",
		"1d":  "1 DAY", 
		"7d":  "7 DAY",
		"14d": "14 DAY",
		"30d": "30 DAY",
	}
	
	interval, exists := intervalMap[timeframe]
	if !exists {
		return nil, fmt.Errorf("invalid timeframe: %s", timeframe)
	}
	
	query := fmt.Sprintf(`
		SELECT 
			receiver as address,
			count() as transfer_count,
			max(timestamp) as last_activity
		FROM transfers_analytics 
		WHERE timestamp >= now() - INTERVAL %s
		GROUP BY receiver
		ORDER BY transfer_count DESC, last_activity DESC
		LIMIT ?
	`, interval)

	rows, err := c.conn.Query(ctx, query, limit)
	if err != nil {
		return nil, fmt.Errorf("failed to query top receivers: %w", err)
	}
	defer rows.Close()

	var wallets []FrontendWalletData
	for rows.Next() {
		var wallet FrontendWalletData
		var transferCount uint64
		var lastActivity time.Time
		
		err := rows.Scan(&wallet.Address, &transferCount, &lastActivity)
		if err != nil {
			return nil, fmt.Errorf("failed to scan receiver: %w", err)
		}

		wallet.Count = int64(transferCount)
		wallet.DisplayAddress = formatWalletAddress(wallet.Address)  
		wallet.LastActivity = lastActivity.Format("2006-01-02 15:04:05")
		wallets = append(wallets, wallet)
	}

	return wallets, nil
}

// formatWalletAddress formats wallet address for display
func formatWalletAddress(address string) string {
	if len(address) <= 16 {
		return address
	}
	return fmt.Sprintf("%s...%s", address[:8], address[len(address)-6:])
}

// GetChainFlowData gets chain flow data with timeframe support and caching
func (c *ClickHouseService) GetChainFlowData(ctx context.Context, timeframe string) (*FrontendChainFlowData, error) {
	cacheKey := fmt.Sprintf("chain_flow_data_%s", timeframe)
	
	// Try cache first
	if data, hit := c.cache.Get(cacheKey); hit {
		utils.LogDebug("CACHE", "Cache hit for %s", cacheKey)
		return data.(*FrontendChainFlowData), nil
	}
	
	// Cache miss - fetch from database
	utils.LogDebug("CACHE", "Cache miss for %s, fetching from ClickHouse", cacheKey)
	data, err := c.getChainFlowDataFromDB(ctx, timeframe)
	if err != nil {
		return nil, err
	}
	
	// Store in cache
	c.cache.SetWithTTL(cacheKey, data, c.cacheConfig.PopularRoutesTTL) // Use routes TTL since similar data
	return data, nil
}

// getChainFlowDataFromDB performs the actual database query for chain flow data
func (c *ClickHouseService) getChainFlowDataFromDB(ctx context.Context, timeframe string) (*FrontendChainFlowData, error) {
	// Map timeframe to SQL interval
	intervalMap := map[string]string{
		"5m":  "5 MINUTE",
		"1h":  "1 HOUR",
		"1d":  "1 DAY",
		"7d":  "7 DAY",
		"14d": "14 DAY",
		"30d": "30 DAY",
	}
	
	interval, exists := intervalMap[timeframe]
	if !exists {
		return nil, fmt.Errorf("invalid timeframe: %s", timeframe)
	}
	
	utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: Querying chain flow data for timeframe %s (interval: %s)", timeframe, interval)
	
	// First, check if we have any data in the timeframe
	countQuery := fmt.Sprintf("SELECT count() FROM transfers_analytics WHERE timestamp >= now() - INTERVAL %s", interval)
	var totalCount uint64
	err := c.conn.QueryRow(ctx, countQuery).Scan(&totalCount)
	if err != nil {
		utils.LogError("CLICKHOUSE", "❌ DEBUG: Failed to count transfers: %v", err)
	} else {
		utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: Found %d total transfers in timeframe %s", totalCount, timeframe)
	}
	
	query := fmt.Sprintf(`
		SELECT 
			source_chain as chain_id,
			source_name as chain_name,
			count() as outgoing_count,
			CAST(0 AS UInt64) as incoming_count,
			max(timestamp) as last_activity
		FROM transfers_analytics 
		WHERE timestamp >= now() - INTERVAL %s
		GROUP BY source_chain, source_name
		
		UNION ALL
		
		SELECT 
			dest_chain as chain_id,
			dest_name as chain_name,
			CAST(0 AS UInt64) as outgoing_count,
			count() as incoming_count,
			max(timestamp) as last_activity
		FROM transfers_analytics 
		WHERE timestamp >= now() - INTERVAL %s
		GROUP BY dest_chain, dest_name
		
		ORDER BY chain_id
	`, interval, interval)

	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		utils.LogError("CLICKHOUSE", "❌ DEBUG: Failed to query chain flow data: %v", err)
		return nil, fmt.Errorf("failed to query chain flow data: %w", err)
	}
	defer rows.Close()

	// Aggregate data by chain
	chainMap := make(map[string]*FrontendChainData)
	var totalOutgoing, totalIncoming int64
	rowCount := 0

	for rows.Next() {
		rowCount++
		var chainID, chainName string
		var outgoingCount, incomingCount uint64
		var lastActivity time.Time
		
		err := rows.Scan(&chainID, &chainName, &outgoingCount, &incomingCount, &lastActivity)
		if err != nil {
			utils.LogError("CLICKHOUSE", "❌ DEBUG: Failed to scan chain flow row: %v", err)
			return nil, fmt.Errorf("failed to scan chain flow: %w", err)
		}

		utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: Row %d - Chain: %s (%s), Out: %d, In: %d, LastActivity: %v", 
			rowCount, chainID, chainName, outgoingCount, incomingCount, lastActivity)

		if chain, exists := chainMap[chainID]; exists {
			// Update existing chain data
			chain.OutgoingCount += int64(outgoingCount)
			chain.IncomingCount += int64(incomingCount)
			chain.NetFlow = chain.OutgoingCount - chain.IncomingCount
			if lastActivity.After(time.Time{}) {
				chain.LastActivity = lastActivity.Format("2006-01-02 15:04:05")
			}
		} else {
			// Create new chain data
			chainMap[chainID] = &FrontendChainData{
				UniversalChainID: chainID,
				ChainName:        chainName,
				OutgoingCount:    int64(outgoingCount),
				IncomingCount:    int64(incomingCount),
				NetFlow:          int64(outgoingCount) - int64(incomingCount),
				LastActivity:     lastActivity.Format("2006-01-02 15:04:05"),
			}
		}
		
		totalOutgoing += int64(outgoingCount)
		totalIncoming += int64(incomingCount)
	}

	utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: Processed %d rows, found %d unique chains", rowCount, len(chainMap))

	// Convert map to slice
	chains := make([]FrontendChainData, 0, len(chainMap))
	for chainID, chain := range chainMap {
		utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: Final chain %s: Out=%d, In=%d, Net=%d", 
			chainID, chain.OutgoingCount, chain.IncomingCount, chain.NetFlow)
		
		// Get top assets for this chain
		assets, err := c.getChainAssets(ctx, chain.UniversalChainID, timeframe, 5)
		if err != nil {
			utils.LogDebug("CLICKHOUSE", "Failed to get assets for chain %s: %v", chain.UniversalChainID, err)
			assets = []FrontendChainAsset{} // Default to empty
		}
		chain.TopAssets = assets
		utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: Chain %s has %d assets", chainID, len(assets))
		chains = append(chains, *chain)
	}

	result := &FrontendChainFlowData{
		Chains:              chains,
		ChainFlowTimeScale:  make(map[string][]FrontendChainData), // Will be populated by caller
		TotalOutgoing:       totalOutgoing,
		TotalIncoming:       totalIncoming,
		ServerUptimeSeconds: 0, // Will be set by caller
	}

	utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: Returning chain flow data with %d chains, TotalOut=%d, TotalIn=%d", 
		len(chains), totalOutgoing, totalIncoming)

	return result, nil
}

// getChainAssets gets the top assets for a specific chain and timeframe
func (c *ClickHouseService) getChainAssets(ctx context.Context, chainID, timeframe string, limit int) ([]FrontendChainAsset, error) {
	cacheKey := fmt.Sprintf("chain_assets_%s_%s_%d", chainID, timeframe, limit)
	
	// Try cache first
	if data, hit := c.cache.Get(cacheKey); hit {
		return data.([]FrontendChainAsset), nil
	}
	
	// Map timeframe to SQL interval
	intervalMap := map[string]string{
		"5m":  "5 MINUTE",
		"1h":  "1 HOUR",
		"1d":  "1 DAY",
		"7d":  "7 DAY",
		"14d": "14 DAY",
		"30d": "30 DAY",
	}
	
	interval, exists := intervalMap[timeframe]
	if !exists {
		return []FrontendChainAsset{}, nil
	}
	
	query := fmt.Sprintf(`
		SELECT 
			token_symbol as asset_symbol,
			canonical_token_symbol as asset_name,
			countIf(source_chain = '%s') as outgoing_count,
			countIf(dest_chain = '%s') as incoming_count,
			sum(amount) as total_volume,
			avg(amount) as average_amount,
			max(timestamp) as last_activity
		FROM transfers_analytics 
		WHERE timestamp >= now() - INTERVAL %s
		  AND (source_chain = '%s' OR dest_chain = '%s')
		GROUP BY token_symbol, canonical_token_symbol
		ORDER BY (outgoing_count + incoming_count) DESC
		LIMIT ?
	`, chainID, chainID, interval, chainID, chainID)

	rows, err := c.conn.Query(ctx, query, limit)
	if err != nil {
		utils.LogDebug("CLICKHOUSE", "Failed to query chain assets for %s: %v", chainID, err)
		return []FrontendChainAsset{}, nil
	}
	defer rows.Close()

	var assets []FrontendChainAsset
	var totalTransfers int64

	for rows.Next() {
		var asset FrontendChainAsset
		var outgoingCount, incomingCount uint64
		var lastActivity time.Time
		
		err := rows.Scan(
			&asset.AssetSymbol, &asset.AssetName, &outgoingCount, &incomingCount,
			&asset.TotalVolume, &asset.AverageAmount, &lastActivity,
		)
		if err != nil {
			utils.LogDebug("CLICKHOUSE", "Failed to scan asset: %v", err)
			continue
		}

		asset.OutgoingCount = int64(outgoingCount)
		asset.IncomingCount = int64(incomingCount)
		asset.NetFlow = asset.OutgoingCount - asset.IncomingCount
		asset.LastActivity = lastActivity.Format("2006-01-02 15:04:05")
		
		assets = append(assets, asset)
		totalTransfers += asset.OutgoingCount + asset.IncomingCount
	}

	// Calculate percentages
	for i := range assets {
		if totalTransfers > 0 {
			assetTotal := assets[i].OutgoingCount + assets[i].IncomingCount
			assets[i].Percentage = float64(assetTotal) / float64(totalTransfers) * 100
		}
	}

	// Store in cache
	c.cache.SetWithTTL(cacheKey, assets, c.cacheConfig.PopularRoutesTTL)
	return assets, nil
}

// GetEarliestSortOrder gets the earliest sort order from transfers_analytics table
func (c *ClickHouseService) GetEarliestSortOrder() (string, error) {
	ctx := context.Background()
	query := `SELECT sort_order FROM transfers_analytics ORDER BY timestamp ASC LIMIT 1`
	
	var sortOrder string
	err := c.conn.QueryRow(ctx, query).Scan(&sortOrder)
	if err != nil {
		// If no rows found, return empty string (not an error)
		if err.Error() == "sql: no rows in result set" {
			return "", nil
		}
		return "", fmt.Errorf("failed to get earliest sort order: %w", err)
	}
	
	return sortOrder, nil
}

// GetLatestSortOrder gets the latest sort order from transfers_analytics table for forward sync resumption
func (c *ClickHouseService) GetLatestSortOrder() (string, error) {
	ctx := context.Background()
	query := `SELECT sort_order FROM transfers_analytics ORDER BY timestamp DESC LIMIT 1`
	
	var sortOrder string
	err := c.conn.QueryRow(ctx, query).Scan(&sortOrder)
	if err != nil {
		// If no rows found, return empty string (not an error)
		if err.Error() == "sql: no rows in result set" {
			return "", nil
		}
		return "", fmt.Errorf("failed to get latest sort order: %w", err)
	}
	
	return sortOrder, nil
}

// GetTransferCount gets the total number of transfers in the database
func (c *ClickHouseService) GetTransferCount() (int64, error) {
	ctx := context.Background()
	query := `SELECT count() FROM transfers_analytics`
	
	var count uint64  // ClickHouse count() returns UInt64
	err := c.conn.QueryRow(ctx, query).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get transfer count: %w", err)
	}
	
	return int64(count), nil  // Convert to int64
}

// Health check for ClickHouse
func (c *ClickHouseService) Health(ctx context.Context) error {
	return c.conn.Ping(ctx)
}

// parseAmount converts string amount to float64 with proper error handling
func parseAmount(amountStr string) float64 {
	if amountStr == "" {
		return 0
	}
	
	// Use strconv.ParseFloat for better precision and error handling
	amount, err := strconv.ParseFloat(amountStr, 64)
	if err != nil {
		utils.LogWarn("CLICKHOUSE", "Failed to parse amount '%s': %v, using 0", amountStr, err)
		return 0
	}
	
	// Check for invalid values that could break materialized views
	if math.IsNaN(amount) || math.IsInf(amount, 0) {
		utils.LogWarn("CLICKHOUSE", "Amount '%s' parsed to invalid value %f, using 0", amountStr, amount)
		return 0
	}
	
	return amount
}

// Cache Management Methods

// ClearCache clears all cached chart data
func (c *ClickHouseService) ClearCache() {
	c.cache.Clear()
	utils.LogInfo("CLICKHOUSE", "Chart data cache cleared")
}

// ClearSpecificCache clears specific cache entries
func (c *ClickHouseService) ClearSpecificCache(keys []string) {
	for _, key := range keys {
		c.cache.Delete(key)
	}
	utils.LogInfo("CLICKHOUSE", "Cleared cache for keys: %v", keys)
}

// GetCacheStats returns comprehensive cache statistics
func (c *ClickHouseService) GetCacheStats() map[string]interface{} {
	return c.cache.GetCacheInfo()
}

// GetCacheMetrics returns cache performance metrics
func (c *ClickHouseService) GetCacheMetrics() CacheMetrics {
	return c.cache.GetMetrics()
}

// InvalidateStaleCache manually removes any stale cache entries
func (c *ClickHouseService) InvalidateStaleCache() {
	// Force cleanup of expired entries
	c.cache.cleanupExpiredEntries()
	utils.LogInfo("CLICKHOUSE", "Stale cache entries invalidated")
}

// UpdateCacheConfig updates cache TTL configuration
func (c *ClickHouseService) UpdateCacheConfig(config CacheConfig) {
	c.cacheConfig = config
	utils.LogInfo("CLICKHOUSE", "Cache configuration updated")
}

// GetCacheConfig returns current cache configuration
func (c *ClickHouseService) GetCacheConfig() CacheConfig {
	return c.cacheConfig
}

// WarmUpCache pre-populates cache with fresh data
func (c *ClickHouseService) WarmUpCache(ctx context.Context) error {
	utils.LogInfo("CLICKHOUSE", "Warming up cache with fresh data...")
	
	// Pre-populate all major cache entries
	_, err := c.GetTransferRates(ctx)
	if err != nil {
		return fmt.Errorf("failed to warm up transfer rates cache: %w", err)
	}
	
	_, err = c.GetActiveWalletRates(ctx)
	if err != nil {
		return fmt.Errorf("failed to warm up active wallet rates cache: %w", err)
	}
	
	_, err = c.GetPopularRoutes(ctx, 20, "7d")
	if err != nil {
		return fmt.Errorf("failed to warm up popular routes cache: %w", err)
	}
	
	_, err = c.GetChainFlowData(ctx, "1d")
	if err != nil {
		return fmt.Errorf("failed to warm up chain flow cache: %w", err)
	}
	
	// Note: Asset volumes handled by ChartBroadcaster, not in basic chart data
	
	_, err = c.GetChartDataForFrontend()
	if err != nil {
		return fmt.Errorf("failed to warm up chart data cache: %w", err)
	}
	
	utils.LogInfo("CLICKHOUSE", "Cache warm-up completed successfully")
	return nil
}

// GetCacheHitRate returns the overall cache hit rate percentage
func (c *ClickHouseService) GetCacheHitRate() float64 {
	metrics := c.cache.GetMetrics()
	if metrics.Hits+metrics.Misses == 0 {
		return 0
	}
	return float64(metrics.Hits) / float64(metrics.Hits+metrics.Misses) * 100
}

// GetAssetVolumes gets asset volume data with timeframe support and caching
func (c *ClickHouseService) GetAssetVolumes(ctx context.Context, timeframe string) (*FrontendAssetVolumeData, error) {
	cacheKey := fmt.Sprintf("asset_volumes_%s", timeframe)
	
	// Try cache first
	if data, hit := c.cache.Get(cacheKey); hit {
		utils.LogDebug("CACHE", "Cache hit for %s", cacheKey)
		return data.(*FrontendAssetVolumeData), nil
	}
	
	// Cache miss - fetch from database
	utils.LogDebug("CACHE", "Cache miss for %s, fetching from ClickHouse", cacheKey)
	data, err := c.getAssetVolumesFromDB(ctx, timeframe)
	if err != nil {
		return nil, err
	}
	
	// Store in cache
	c.cache.SetWithTTL(cacheKey, data, c.cacheConfig.PopularRoutesTTL) // Use routes TTL since similar data
	return data, nil
}

// GetAssetVolumesFresh gets asset volume data bypassing cache (for debugging)
func (c *ClickHouseService) GetAssetVolumesFresh(ctx context.Context, timeframe string) (*FrontendAssetVolumeData, error) {
	utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: Fetching FRESH asset volumes for timeframe %s (bypassing cache)", timeframe)
	return c.getAssetVolumesFromDB(ctx, timeframe)
}

// ClearAssetVolumeCache clears asset volume cache for all timeframes
func (c *ClickHouseService) ClearAssetVolumeCache() {
	timeframes := []string{"5m", "1h", "1d", "7d", "14d", "30d"}
	for _, tf := range timeframes {
		cacheKey := fmt.Sprintf("asset_volumes_%s", tf)
		c.cache.Delete(cacheKey)
		utils.LogInfo("CLICKHOUSE", "Cleared asset volume cache for timeframe %s", tf)
	}
}

// getAssetVolumesFromDB performs the actual database query for asset volumes
func (c *ClickHouseService) getAssetVolumesFromDB(ctx context.Context, timeframe string) (*FrontendAssetVolumeData, error) {
	// Map timeframe to SQL interval
	intervalMap := map[string]string{
		"5m":  "5 MINUTE",
		"1h":  "1 HOUR",
		"1d":  "1 DAY",
		"7d":  "7 DAY",
		"14d": "14 DAY",
		"30d": "30 DAY",
	}
	
	interval, exists := intervalMap[timeframe]
	if !exists {
		return nil, fmt.Errorf("invalid timeframe: %s", timeframe)
	}
	
	utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: Querying asset volumes for timeframe %s (interval: %s)", timeframe, interval)
	
	// First, check how many total transfers exist in this timeframe
	countQuery := fmt.Sprintf(`
		SELECT count() as total_transfers, 
		       uniq(CASE 
				WHEN canonical_token_symbol != '' AND canonical_token_symbol IS NOT NULL 
				THEN canonical_token_symbol 
				ELSE token_symbol 
			   END) as unique_assets
		FROM transfers_analytics 
		WHERE timestamp >= now() - INTERVAL %s
		  AND token_symbol != ''
		  AND token_symbol IS NOT NULL
	`, interval)
	
	var totalTransfersInTimeframe, uniqueAssetsInTimeframe uint64
	err := c.conn.QueryRow(ctx, countQuery).Scan(&totalTransfersInTimeframe, &uniqueAssetsInTimeframe)
	if err != nil {
		utils.LogError("CLICKHOUSE", "❌ DEBUG: Failed to count transfers for timeframe %s: %v", timeframe, err)
	} else {
		utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: Timeframe %s has %d total transfers and %d unique assets", 
			timeframe, totalTransfersInTimeframe, uniqueAssetsInTimeframe)
	}
	
	// Query: Use existing canonical_token_symbol logic, group internally, return frontend-friendly names
	// This approach requires NO schema changes and leverages existing canonical logic
	query := fmt.Sprintf(`
		SELECT 
			CASE 
				WHEN canonical_token_symbol != '' AND canonical_token_symbol IS NOT NULL 
				THEN canonical_token_symbol 
				ELSE token_symbol 
			END as internal_asset_id,
			-- Use the most common display symbol for this asset group
			any(token_symbol) as asset_symbol,
			any(token_symbol) as asset_name,
			count() as transfer_count,
			sum(amount) as total_volume,
			max(amount) as largest_transfer,
			avg(amount) as average_amount,
			max(timestamp) as last_activity
		FROM transfers_analytics 
		WHERE timestamp >= now() - INTERVAL %s
		  AND token_symbol != ''
		  AND token_symbol IS NOT NULL
		GROUP BY 
			CASE 
				WHEN canonical_token_symbol != '' AND canonical_token_symbol IS NOT NULL 
				THEN canonical_token_symbol 
				ELSE token_symbol 
			END
		ORDER BY transfer_count DESC
		LIMIT 20
	`, interval)

	
	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		utils.LogError("CLICKHOUSE", "❌ DEBUG: Failed to query asset volumes: %v", err)
		return nil, fmt.Errorf("failed to query asset volumes: %w", err)
	}
	defer rows.Close()

	var assets []FrontendAsset
	var totalVolume float64
	var totalTransfers int64
	var rowCount int

	for rows.Next() {
		var asset FrontendAsset
		var internalAssetID string
		var transferCount uint64
		var lastActivity time.Time
		
		err := rows.Scan(
			&internalAssetID, &asset.AssetSymbol, &asset.AssetName, &transferCount,
			&asset.TotalVolume, &asset.LargestTransfer, &asset.AverageAmount, &lastActivity,
		)
		if err != nil {
			utils.LogError("CLICKHOUSE", "❌ DEBUG: Failed to scan asset: %v", err)
			continue
		}

		asset.TransferCount = int64(transferCount)
		asset.LastActivity = lastActivity.Format("2006-01-02 15:04:05")
		
		// Get top routes for this asset (using the internal asset ID for proper filtering)
		asset.TopRoutes = c.getAssetTopRoutes(ctx, internalAssetID, timeframe)
		
		assets = append(assets, asset)
		totalVolume += asset.TotalVolume
		totalTransfers += asset.TransferCount
		rowCount++
		
		// Enhanced debugging - show internal vs display names
		if rowCount <= 3 {
			utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: [%s] Asset #%d: %s (internal: %s, display: %s, count=%d, vol=%.2f)", 
				timeframe, rowCount, asset.AssetSymbol, internalAssetID, asset.AssetName, asset.TransferCount, asset.TotalVolume)
		}
	}

	utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: [%s] Processed %d asset rows, TotalVolume=%.2f, TotalTransfers=%d", 
		timeframe, rowCount, totalVolume, totalTransfers)

	// Calculate percentages for each asset
	for i := range assets {
		if totalVolume > 0 {
			assets[i].Percentage = (assets[i].TotalVolume / totalVolume) * 100
		}
	}

	// Build time scale data - for now just populate current timeframe
	timeScaleData := make(map[string][]FrontendAsset)
	timeScaleData[timeframe] = assets

	result := &FrontendAssetVolumeData{
		Assets:               assets,
		AssetVolumeTimeScale: timeScaleData,
		TotalAssets:          int64(len(assets)),
		TotalVolume:          totalVolume,
		TotalTransfers:       totalTransfers,
		ServerUptimeSeconds:  0, // Will be set by caller
	}

	return result, nil
}

// getAssetTopRoutes gets the top routes for a specific asset and timeframe
func (c *ClickHouseService) getAssetTopRoutes(ctx context.Context, assetSymbol, timeframe string) []FrontendAssetRoute {
	// Map timeframe to SQL interval
	intervalMap := map[string]string{
		"5m":  "5 MINUTE",
		"1h":  "1 HOUR",
		"1d":  "1 DAY",
		"7d":  "7 DAY",
		"14d": "14 DAY",
		"30d": "30 DAY",
	}
	
	interval, exists := intervalMap[timeframe]
	if !exists {
		return []FrontendAssetRoute{}
	}
	
	// Add debug logging to trace asset symbol filtering
	utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: Querying routes for asset '%s' in timeframe %s", assetSymbol, timeframe)
	
	// Query routes for this asset using existing canonical_token_symbol logic (no schema changes needed)
	query := fmt.Sprintf(`
		SELECT 
			source_chain as from_chain,
			dest_chain as to_chain,
			source_name as from_name,
			dest_name as to_name,
			count() as route_count,
			sum(amount) as route_volume,
			max(timestamp) as last_activity
		FROM transfers_analytics 
		WHERE timestamp >= now() - INTERVAL %s
		  AND (
		    (canonical_token_symbol != '' AND canonical_token_symbol IS NOT NULL AND canonical_token_symbol = '%s')
		    OR 
		    ((canonical_token_symbol = '' OR canonical_token_symbol IS NULL) AND token_symbol = '%s')
		  )
		GROUP BY source_chain, dest_chain, source_name, dest_name
		ORDER BY route_count DESC
		LIMIT 10
	`, interval, assetSymbol, assetSymbol)

	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		utils.LogError("CLICKHOUSE", "Failed to query asset routes for %s: %v", assetSymbol, err)
		return []FrontendAssetRoute{}
	}
	defer rows.Close()

	var routes []FrontendAssetRoute
	var totalVolume float64

	// First pass - collect data
	type routeData struct {
		route  FrontendAssetRoute
		volume float64
	}
	var routeList []routeData
	var routeCount int

	for rows.Next() {
		var route FrontendAssetRoute
		var routeCountDB uint64
		var lastActivity time.Time
		
		err := rows.Scan(
			&route.FromChain, &route.ToChain, &route.FromName, &route.ToName,
			&routeCountDB, &route.Volume, &lastActivity,
		)
		if err != nil {
			utils.LogError("CLICKHOUSE", "Failed to scan asset route: %v", err)
			continue
		}

		route.Count = int64(routeCountDB)
		route.Route = fmt.Sprintf("%s→%s", route.FromName, route.ToName)
		route.LastActivity = lastActivity.Format("2006-01-02 15:04:05")
		
		routeList = append(routeList, routeData{route: route, volume: route.Volume})
		totalVolume += route.Volume
		routeCount++
		
		// Debug: Log first few routes for this asset
		if routeCount <= 3 {
			utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: Asset '%s' route #%d: %s (count=%d, vol=%.2f)", 
				assetSymbol, routeCount, route.Route, route.Count, route.Volume)
		}
	}

	utils.LogInfo("CLICKHOUSE", "🔍 DEBUG: Asset '%s' has %d routes with total volume %.2f", 
		assetSymbol, routeCount, totalVolume)

	// Second pass - calculate percentages
	for _, rd := range routeList {
		route := rd.route
		if totalVolume > 0 {
			route.Percentage = (rd.volume / totalVolume) * 100
		}
		routes = append(routes, route)
	}

	return routes
}

 