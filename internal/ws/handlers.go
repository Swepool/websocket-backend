package ws

import (
    "encoding/json"
    "fmt"
    "net/http"
    "runtime"
    "runtime/debug"
    "strconv"
    "sync/atomic"
    "time"

    "github.com/gorilla/websocket"
    "websocket-backend/internal/models"
    "websocket-backend/internal/stats"
)

// WebSocket message size limits (protocol layer enforcement)
const (
    maxMessageSize = 1024 // Maximum WebSocket message size in bytes
)

// ErrorMessage represents a structured WebSocket error response
type ErrorMessage struct {
    Type      string `json:"type"`      // always "error"
    Code      string `json:"code"`      // error code (e.g. "invalid_message")
    Message   string `json:"message"`   // human-readable error message
    Timestamp int64  `json:"timestamp"` // timestamp when error occurred
}

// Error codes for structured error responses
const (
    ErrorCodeInvalidMessage = "invalid_message"
    ErrorCodeMessageTooLarge = "message_too_large"
    ErrorCodeUnknownType    = "unknown_message_type"
    ErrorCodeInvalidJSON    = "invalid_json"
    ErrorCodeChainIDTooLong = "chain_id_too_long"
    ErrorCodeEmptyMessage   = "empty_message"
)

var upgrader = websocket.Upgrader{
    CheckOrigin: func(r *http.Request) bool {
        return true // Allow all origins in development
    },
    ReadBufferSize:   1024,
    WriteBufferSize:  1024,
    EnableCompression: true, // Enable WebSocket compression
}

// recoverMiddleware wraps handlers with panic recovery and centralized JSON responses
func (s *Server) recoverMiddleware(handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if err := recover(); err != nil {
				s.logger.Error("Handler panic recovered", map[string]interface{}{
					"error": fmt.Sprintf("%v", err),
					"path":  r.URL.Path,
					"stack": string(debug.Stack()),
				})
				
				writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
					"error":     "Internal server error",
					"timestamp": time.Now().UnixMilli(),
					"path":      r.URL.Path,
				})
			}
		}()
		handler(w, r)
	}
}

// handleWebSocket handles WebSocket connections with enhanced security
func (s *Server) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			s.logger.Error("WebSocket handler panic", map[string]interface{}{
				"error": fmt.Sprintf("%v", err),
				"stack": string(debug.Stack()),
			})
		}
	}()

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("Failed to upgrade WebSocket connection", map[string]interface{}{
			"error": err.Error(),
		})
		return
	}

	// Set read limit for large payload defense (protocol layer enforcement)
	conn.SetReadLimit(maxMessageSize)
	
	// Set close handlers for proper cleanup
	conn.SetCloseHandler(func(code int, text string) error {
		s.logger.Debug("WebSocket close handler triggered", map[string]interface{}{
			"code": code,
			"text": text,
		})
		return nil
	})

	client := NewClient(conn, s.removeClient)
	s.addClient(client)

	// Send initial data with error handling
	if err := client.SendConnected(); err != nil {
		s.logger.Warn("Failed to send connected message", map[string]interface{}{
			"error": err.Error(),
		})
	}
	
	s.transferMu.RLock()
	chains := s.chains
	s.transferMu.RUnlock()
	
	if err := client.SendChains(chains); err != nil {
		s.logger.Warn("Failed to send chains data", map[string]interface{}{
			"error": err.Error(),
		})
	}
	
	if err := client.SendServerInfo(); err != nil {
		s.logger.Warn("Failed to send server info", map[string]interface{}{
			"error": err.Error(),
		})
	}

	// Send current chart data immediately so client doesn't have to wait for next broadcast
	if chartData := s.getChartData(); chartData != nil {
		if err := client.SendChartData(chartData); err != nil {
			s.logger.Warn("Failed to send initial chart data", map[string]interface{}{
				"error": err.Error(),
			})
		} else {
			s.logger.Debug("Sent initial chart data to new client", map[string]interface{}{
				"client_connected": true,
			})
		}
	}

	// Start client goroutines
	go client.WritePump()
	go client.ReadPump(s.handleClientMessage)
}

// ClientMessage represents a validated client message
type ClientMessage struct {
    Type string `json:"type"`
    Data struct {
        FromChain string `json:"fromChain"`
        ToChain   string `json:"toChain"`
    } `json:"data"`
}

// validateClientMessage validates and parses client messages
func (s *Server) validateClientMessage(message []byte) (*ClientMessage, string, error) {
	if len(message) == 0 {
		return nil, ErrorCodeEmptyMessage, fmt.Errorf("empty message")
	}
	
	// Note: Manual size check removed - now handled by SetReadLimit at protocol layer
	// if len(message) > maxMessageSize { ... }

	var data ClientMessage
	if err := json.Unmarshal(message, &data); err != nil {
		return nil, ErrorCodeInvalidJSON, fmt.Errorf("invalid JSON: %v", err)
	}

	// Validate message type
	switch data.Type {
	case "setChainFilter":
		// Validate chain IDs (basic validation)
		if len(data.Data.FromChain) > 100 || len(data.Data.ToChain) > 100 {
			return nil, ErrorCodeChainIDTooLong, fmt.Errorf("chain ID too long")
		}
	case "ping":
		// Allow ping messages
	default:
		return nil, ErrorCodeUnknownType, fmt.Errorf("unknown message type: %s", data.Type)
	}

	return &data, "", nil
}

// handleClientMessage handles client messages with structured error responses
func (s *Server) handleClientMessage(client *Client, message []byte) {
	defer func() {
		if err := recover(); err != nil {
			s.logger.Error("Client message handler panic", map[string]interface{}{
				"error": fmt.Sprintf("%v", err),
				"stack": string(debug.Stack()),
			})
		}
	}()

	data, errorCode, err := s.validateClientMessage(message)
	if err != nil {
		s.logger.Warn("Invalid client message", map[string]interface{}{
			"error":     err.Error(),
			"errorCode": errorCode,
			"message":   string(message),
		})
		
		// Send structured error response to client
		s.sendStructuredError(client, errorCode, err.Error())
		return
	}

	switch data.Type {
	case "setChainFilter":
		client.SetFilter(&models.ChainFilter{
			FromChain: data.Data.FromChain,
			ToChain:   data.Data.ToChain,
		})

		if err := client.SendFilterSet(data.Data.FromChain, data.Data.ToChain); err != nil {
			s.logger.Warn("Failed to send filter confirmation", map[string]interface{}{
				"error": err.Error(),
			})
		}
	case "ping":
		// Respond to ping
		client.Send(map[string]interface{}{
			"type":      "pong",
			"timestamp": time.Now().UnixMilli(),
		})
	}
}

// handleHealth handles health check requests
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			s.logger.Error("Health handler panic", map[string]interface{}{
				"error": fmt.Sprintf("%v", err),
			})
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"error":     "Internal Server Error",
				"timestamp": time.Now().UnixMilli(),
			})
		}
	}()

	s.transferMu.RLock()
	lastSortOrder := s.lastSortOrder
	s.transferMu.RUnlock()

	writeJSON(w, http.StatusOK, struct {
		Status       string  `json:"status"`
		LastSortOrder string  `json:"lastSortOrder"`
		Uptime       float64 `json:"uptime"`
		Timestamp    int64   `json:"timestamp"`
	}{
		Status:       "healthy",
		LastSortOrder: lastSortOrder,
		Uptime:       time.Since(startTime).Seconds(),
		Timestamp:    time.Now().UnixMilli(),
	})
}

// handleStats handles the statistics endpoint
func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
    defer func() {
        if err := recover(); err != nil {
            s.logger.Error("Stats handler panic", map[string]interface{}{
                "error": fmt.Sprintf("%v", err),
                "stack": string(debug.Stack()),
            })
            
            w.Header().Set("Content-Type", "application/json")
            w.WriteHeader(http.StatusInternalServerError)
            json.NewEncoder(w).Encode(map[string]interface{}{
                "error":     "Failed to generate stats",
                "timestamp": time.Now().UnixMilli(),
            })
        }
    }()

    s.transferMu.RLock()
    lastSortOrder := s.lastSortOrder
    isInitialFetch := s.isInitialFetch
    scheduledTransfers := len(s.scheduledTransfers)
    s.transferMu.RUnlock()

    clientCount := atomic.LoadInt64(&s.clientCount)
    
    // Safely get chart data
    var chartData interface{}
    func() {
        defer func() {
            if err := recover(); err != nil {
                s.logger.Error("Chart data panic", map[string]interface{}{
                    "error": fmt.Sprintf("%v", err),
                })
                chartData = nil
            }
        }()
        chartData = s.getChartData()
    }()
    
    var m runtime.MemStats
    runtime.ReadMemStats(&m)

    // Handle both enhanced and basic chart data safely
    var transferRates stats.TransferRates
    var popularRoutes []*stats.RouteStats
    var activeSenders []*stats.WalletStats
    var activeReceivers []*stats.WalletStats

    if chartData != nil {
        if enhancedData, ok := chartData.(stats.EnhancedChartData); ok {
            // Enhanced chart data
            transferRates = enhancedData.CurrentRates
            popularRoutes = enhancedData.PopularRoutes
            activeSenders = enhancedData.ActiveSenders
            activeReceivers = enhancedData.ActiveReceivers
        } else if basicData, ok := chartData.(stats.ChartData); ok {
            // Basic chart data - convert to enhanced format
            transferRates = basicData.CurrentRates
            popularRoutes = basicData.PopularRoutes
            activeSenders = basicData.ActiveSenders
            activeReceivers = basicData.ActiveReceivers
        }
    }

    w.Header().Set("Content-Type", "application/json")
    if err := json.NewEncoder(w).Encode(struct {
        LastSortOrder      string   `json:"lastSortOrder"`
        IsInitialFetch     bool     `json:"isInitialFetch"`
        ScheduledTransfers int      `json:"scheduledTransfers"`
        ClientCount        int64    `json:"clientCount"`
        TransferRates      stats.TransferRates `json:"transferRates"`
        PopularRoutes      []*stats.RouteStats `json:"popularRoutes"`
        ActiveSenders      []*stats.WalletStats `json:"activeSenders"`
        ActiveReceivers    []*stats.WalletStats `json:"activeReceivers"`
        Performance        struct {
            Goroutines     int     `json:"goroutines"`
            MemoryMB       float64 `json:"memoryMB"`
            GCCount        uint32  `json:"gcCount"`
            BroadcastWorkers int   `json:"broadcastWorkers"`
            ClientShards   int     `json:"clientShards"`
        } `json:"performance"`
        Config            struct {
            PollInterval    int    `json:"POLL_INTERVAL"`
            MainnetOnly     bool   `json:"MAINNET_ONLY"`
            GraphQLEndpoint string `json:"GRAPHQL_ENDPOINT"`
            SpreadTimeMs    int    `json:"SPREAD_TIME_MS"`
            FutureBuffer    int    `json:"FUTURE_BUFFER_SECONDS"`
            MaxWorkers      int    `json:"MAX_WORKERS"`
            ClientShards    int    `json:"CLIENT_SHARDS"`
        } `json:"config"`
        Uptime    float64 `json:"uptime"`
        Timestamp int64   `json:"timestamp"`
    }{
        LastSortOrder:      lastSortOrder,
        IsInitialFetch:     isInitialFetch,
        ScheduledTransfers: scheduledTransfers,
        ClientCount:        clientCount,
        TransferRates:      transferRates,
        PopularRoutes:      popularRoutes,
        ActiveSenders:      activeSenders,
        ActiveReceivers:    activeReceivers,
        Performance: struct {
            Goroutines     int     `json:"goroutines"`
            MemoryMB       float64 `json:"memoryMB"`
            GCCount        uint32  `json:"gcCount"`
            BroadcastWorkers int   `json:"broadcastWorkers"`
            ClientShards   int     `json:"clientShards"`
        }{
            Goroutines:     runtime.NumGoroutine(),
            MemoryMB:       float64(m.Alloc)/1024/1024,
            GCCount:        m.NumGC,
            BroadcastWorkers: maxWorkers,
            ClientShards:   clientShards,
        },
        Config: struct {
            PollInterval    int    `json:"POLL_INTERVAL"`
            MainnetOnly     bool   `json:"MAINNET_ONLY"`
            GraphQLEndpoint string `json:"GRAPHQL_ENDPOINT"`
            SpreadTimeMs    int    `json:"SPREAD_TIME_MS"`
            FutureBuffer    int    `json:"FUTURE_BUFFER_SECONDS"`
            MaxWorkers      int    `json:"MAX_WORKERS"`
            ClientShards    int    `json:"CLIENT_SHARDS"`
        }{
            PollInterval:    s.config.PollInterval,
            MainnetOnly:     s.config.MainnetOnly,
            GraphQLEndpoint: s.config.GraphQLEndpoint,
            SpreadTimeMs:    spreadTimeMs,
            FutureBuffer:    futureBufferSeconds,
            MaxWorkers:      maxWorkers,
            ClientShards:    clientShards,
        },
        Uptime:    time.Since(startTime).Seconds(),
        Timestamp: time.Now().UnixMilli(),
    }); err != nil {
        s.logger.Error("Failed to encode stats response", map[string]interface{}{
            "error": err.Error(),
        })
    }
}

// handleSchedulerStats handles the scheduler statistics endpoint
func (s *Server) handleSchedulerStats(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    
    response := map[string]interface{}{
        "enhancedSchedulerEnabled": true, // Always enabled
        "timestamp": time.Now().UnixMilli(),
    }
    
    // Safely get activity stats
    if s.enhancedScheduler != nil {
        func() {
            defer func() {
                if err := recover(); err != nil {
                    s.logger.Error("Scheduler stats panic", map[string]interface{}{
                        "error": fmt.Sprintf("%v", err),
                    })
                    response["activityStats"] = nil
                    response["error"] = "Failed to get activity stats"
                }
            }()
            response["activityStats"] = s.enhancedScheduler.GetActivityStats()
        }()
    } else {
        response["message"] = "Enhanced scheduler is disabled"
    }
    
    if err := json.NewEncoder(w).Encode(response); err != nil {
        s.logger.Error("Failed to encode scheduler stats", map[string]interface{}{
            "error": err.Error(),
        })
    }
}

// handleEnhancedStats handles the enhanced statistics endpoint
func (s *Server) handleEnhancedStats(w http.ResponseWriter, r *http.Request) {
    s.transferMu.RLock()
    lastSortOrder := s.lastSortOrder
    isInitialFetch := s.isInitialFetch
    scheduledTransfers := len(s.scheduledTransfers)
    s.transferMu.RUnlock()

    clientCount := atomic.LoadInt64(&s.clientCount)
    
    // Check if we have enhanced stats collector
    if enhancedCollector, ok := s.statsCollector.(*stats.EnhancedCollector); ok {
        // Safely get enhanced chart data
        var enhancedChartData stats.EnhancedChartData
        func() {
            defer func() {
                if err := recover(); err != nil {
                    s.logger.Error("Enhanced chart data panic", map[string]interface{}{
                        "error": fmt.Sprintf("%v", err),
                    })
                    // Use empty data as fallback
                    enhancedChartData = stats.EnhancedChartData{}
                }
            }()
            enhancedChartData = enhancedCollector.GetEnhancedChartData()
        }()
        
        var m runtime.MemStats
        runtime.ReadMemStats(&m)

        w.Header().Set("Content-Type", "application/json")
        if err := json.NewEncoder(w).Encode(struct {
            LastSortOrder      string   `json:"lastSortOrder"`
            IsInitialFetch     bool     `json:"isInitialFetch"`
            ScheduledTransfers int      `json:"scheduledTransfers"`
            ClientCount        int64    `json:"clientCount"`
            TransferRates      stats.TransferRates `json:"transferRates"`
            ActiveWalletRates  stats.ActiveWalletRates `json:"activeWalletRates"`
            PopularRoutes      []*stats.RouteStats `json:"popularRoutes"`
            PopularRoutesTimeScale map[string][]*stats.RouteStats `json:"popularRoutesTimeScale"`
            ActiveSenders      []*stats.WalletStats `json:"activeSenders"`
            ActiveReceivers    []*stats.WalletStats `json:"activeReceivers"`
            DataAvailability   struct {
                HasMinute bool `json:"hasMinute"`
                HasHour   bool `json:"hasHour"`
                HasDay    bool `json:"hasDay"`
                Has7Days  bool `json:"has7Days"`
                Has14Days bool `json:"has14Days"`
                Has30Days bool `json:"has30Days"`
            } `json:"dataAvailability"`
            Performance        struct {
                Goroutines     int     `json:"goroutines"`
                MemoryMB       float64 `json:"memoryMB"`
                GCCount        uint32  `json:"gcCount"`
                BroadcastWorkers int   `json:"broadcastWorkers"`
                ClientShards   int     `json:"clientShards"`
            } `json:"performance"`
            Config            struct {
                PollInterval      int    `json:"POLL_INTERVAL"`
                MainnetOnly       bool   `json:"MAINNET_ONLY"`
                GraphQLEndpoint   string `json:"GRAPHQL_ENDPOINT"`
                EnhancedScheduler bool   `json:"ENHANCED_SCHEDULER"`
                EnhancedStats     bool   `json:"ENHANCED_STATS"`
                SpreadTimeMs      int    `json:"SPREAD_TIME_MS"`
                FutureBuffer      int    `json:"FUTURE_BUFFER_SECONDS"`
                MaxWorkers        int    `json:"MAX_WORKERS"`
                ClientShards      int    `json:"CLIENT_SHARDS"`
            } `json:"config"`
            Uptime    float64 `json:"uptime"`
            Timestamp int64   `json:"timestamp"`
        }{
            LastSortOrder:      lastSortOrder,
            IsInitialFetch:     isInitialFetch,
            ScheduledTransfers: scheduledTransfers,
            ClientCount:        clientCount,
            TransferRates:      enhancedChartData.CurrentRates,
            ActiveWalletRates:  enhancedChartData.ActiveWalletRates,
            PopularRoutes:      enhancedChartData.PopularRoutes,
            PopularRoutesTimeScale: enhancedChartData.PopularRoutesTimeScale,
            ActiveSenders:      enhancedChartData.ActiveSenders,
            ActiveReceivers:    enhancedChartData.ActiveReceivers,
            DataAvailability:   enhancedChartData.DataAvailability,
            Performance: struct {
                Goroutines     int     `json:"goroutines"`
                MemoryMB       float64 `json:"memoryMB"`
                GCCount        uint32  `json:"gcCount"`
                BroadcastWorkers int   `json:"broadcastWorkers"`
                ClientShards   int     `json:"clientShards"`
            }{
                Goroutines:     runtime.NumGoroutine(),
                MemoryMB:       float64(m.Alloc)/1024/1024,
                GCCount:        m.NumGC,
                BroadcastWorkers: maxWorkers,
                ClientShards:   clientShards,
            },
            Config: struct {
                PollInterval      int    `json:"POLL_INTERVAL"`
                MainnetOnly       bool   `json:"MAINNET_ONLY"`
                GraphQLEndpoint   string `json:"GRAPHQL_ENDPOINT"`
                EnhancedScheduler bool   `json:"ENHANCED_SCHEDULER"`
                EnhancedStats     bool   `json:"ENHANCED_STATS"`
                SpreadTimeMs      int    `json:"SPREAD_TIME_MS"`
                FutureBuffer      int    `json:"FUTURE_BUFFER_SECONDS"`
                MaxWorkers        int    `json:"MAX_WORKERS"`
                ClientShards      int    `json:"CLIENT_SHARDS"`
            }{
                PollInterval:      s.config.PollInterval,
                MainnetOnly:       s.config.MainnetOnly,
                GraphQLEndpoint:   s.config.GraphQLEndpoint,
                EnhancedScheduler: true, // Always enabled
                EnhancedStats:     true, // Always enabled
                SpreadTimeMs:      spreadTimeMs,
                FutureBuffer:      futureBufferSeconds,
                MaxWorkers:        maxWorkers,
                ClientShards:      clientShards,
            },
            Uptime:    time.Since(startTime).Seconds(),
            Timestamp: time.Now().UnixMilli(),
        }); err != nil {
            s.logger.Error("Failed to encode enhanced stats response", map[string]interface{}{
                "error": err.Error(),
            })
        }
    } else {
        // Fallback to regular stats if enhanced stats not available
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{
            "error":     "Enhanced stats not available",
            "timestamp": time.Now().UnixMilli(),
        })
    }
}

// handleDebug handles debug information endpoint
func (s *Server) handleDebug(w http.ResponseWriter, r *http.Request) {
    w.Header().Set("Content-Type", "application/json")
    
    s.transferMu.RLock()
    lastSortOrder := s.lastSortOrder
    isInitialFetch := s.isInitialFetch
    s.transferMu.RUnlock()
    
    debugInfo := map[string]interface{}{
        "timestamp": time.Now().UnixMilli(),
        "config": map[string]interface{}{
            "enhancedStats":   true, // Always enabled
            "pollInterval":    s.config.PollInterval,
            "mainnetOnly":     s.config.MainnetOnly,

            "transferAgeThreshold": s.config.TransferAgeThreshold.String(),
        },
        "server": map[string]interface{}{
            "lastSortOrder":  lastSortOrder,
            "isInitialFetch": isInitialFetch,
        },
        "buckets": s.getTransferBucketStats(),
    }
    
    // Check collector type safely
    if enhancedCollector, ok := s.statsCollector.(*stats.EnhancedCollector); ok {
        debugInfo["collectorType"] = "EnhancedCollector"
        
        // Safely get raw stats for debugging
        func() {
            defer func() {
                if err := recover(); err != nil {
                    s.logger.Error("Debug stats panic", map[string]interface{}{
                        "error": fmt.Sprintf("%v", err),
                    })
                    debugInfo["rawStats"] = map[string]interface{}{
                        "error": "Failed to get debug stats",
                    }
                }
            }()
            
            chartData := enhancedCollector.GetEnhancedChartData()
            debugInfo["rawStats"] = map[string]interface{}{
                "totalTracked":       chartData.CurrentRates.TotalTracked,
                "txPer30Days":        chartData.CurrentRates.TxPer30Days,
                "txPer7Days":         chartData.CurrentRates.TxPer7Days,
                "txPerDay":           chartData.CurrentRates.TxPerDay,
                "popularRoutesCount": len(chartData.PopularRoutes),
                "activeSendersCount": len(chartData.ActiveSenders),
                "activeSenders":      chartData.ActiveWalletRates.SendersLastMin,
                "activeReceivers":    chartData.ActiveWalletRates.ReceiversLastMin,
                "totalActive":        chartData.ActiveWalletRates.TotalLastMin,
            }
            
            // Show top routes if any
            if len(chartData.PopularRoutes) > 0 {
                debugInfo["topRoutes"] = chartData.PopularRoutes[:min(3, len(chartData.PopularRoutes))]
            }
            
            // Show top senders if any
            if len(chartData.ActiveSenders) > 0 {
                debugInfo["topSenders"] = chartData.ActiveSenders[:min(3, len(chartData.ActiveSenders))]
            }
        }()
    } else {
        debugInfo["collectorType"] = "StandardCollector"
        func() {
            defer func() {
                if err := recover(); err != nil {
                    s.logger.Error("Standard debug stats panic", map[string]interface{}{
                        "error": fmt.Sprintf("%v", err),
                    })
                    debugInfo["rawStats"] = map[string]interface{}{
                        "error": "Failed to get standard debug stats",
                    }
                }
            }()
            
            chartData := s.statsCollector.GetChartData()
            debugInfo["rawStats"] = map[string]interface{}{
                "totalTracked":       chartData.CurrentRates.TotalTracked,
                "txPerDay":           chartData.CurrentRates.TxPerDay,
                "popularRoutesCount": len(chartData.PopularRoutes),
                "activeSendersCount": len(chartData.ActiveSenders),
            }
        }()
    }
    
    if err := json.NewEncoder(w).Encode(debugInfo); err != nil {
        s.logger.Error("Failed to encode debug response", map[string]interface{}{
            "error": err.Error(),
        })
    }
}

// handleBuckets handles the bucket information endpoint
func (s *Server) handleBuckets(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			s.logger.Error("Buckets handler panic", map[string]interface{}{
				"error": err,
			})
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
	}()
	
	// Get query parameter for minutes (default to 5)
	minutesStr := r.URL.Query().Get("minutes")
	minutes := 5
	if minutesStr != "" {
		if parsed, err := strconv.Atoi(minutesStr); err == nil && parsed > 0 && parsed <= 60 {
			minutes = parsed
		}
	}
	
	// Get recent transfers from buckets
	recentTransfers := s.getRecentTransfers(minutes)
	bucketStats := s.getTransferBucketStats()
	
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"bucketStats": bucketStats,
		"recentTransfers": map[string]interface{}{
			"minutesRequested": minutes,
			"transferCount": len(recentTransfers),
			"transfers": recentTransfers,
		},
		"timestamp": time.Now().UnixMilli(),
	})
}

// Helper function for min
func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

// handleSlowClientStats handles the slow client statistics endpoint
func (s *Server) handleSlowClientStats(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			s.logger.Error("Slow client stats handler panic", map[string]interface{}{
				"error": err,
			})
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"error":     "Internal server error",
				"timestamp": time.Now().UnixMilli(),
			})
		}
	}()
	
	// Get query parameter for detail level
	detail := r.URL.Query().Get("detail")
	includeDetails := detail == "true" || detail == "1"
	
	// Get slow client metrics
	metrics := s.getSlowClientMetrics()
	
	response := map[string]interface{}{
		"metrics":   metrics,
		"timestamp": time.Now().UnixMilli(),
	}
	
	// Include detailed client information if requested
	if includeDetails {
		detailedInfo := s.getDetailedSlowClientInfo()
		response["slowClientDetails"] = detailedInfo
		response["detailsIncluded"] = true
	} else {
		response["detailsIncluded"] = false
		response["note"] = "Add ?detail=true to get detailed slow client information"
	}
	
	writeJSON(w, http.StatusOK, response)
}

// handleGraphQLStats handles the GraphQL client statistics endpoint
func (s *Server) handleGraphQLStats(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			s.logger.Error("GraphQL stats handler panic", map[string]interface{}{
				"error": err,
			})
			writeJSON(w, http.StatusInternalServerError, map[string]interface{}{
				"error":     "Internal server error",
				"timestamp": time.Now().UnixMilli(),
			})
		}
	}()
	
	var graphqlStats map[string]interface{}
	if s.graphql != nil {
		graphqlStats = s.graphql.GetConnectionStats()
	} else {
		graphqlStats = map[string]interface{}{
			"error": "GraphQL client not initialized",
		}
	}
	
	response := map[string]interface{}{
		"graphqlClient": graphqlStats,
		"timestamp":     time.Now().UnixMilli(),
		"note":          "These are HTTP client connection pool statistics for the GraphQL client",
	}
	
	writeJSON(w, http.StatusOK, response)
}

// writeJSON is a centralized JSON response utility
func writeJSON(w http.ResponseWriter, statusCode int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		// Fallback error response if JSON encoding fails
		http.Error(w, `{"error":"json_encoding_failed","timestamp":`+fmt.Sprintf("%d", time.Now().UnixMilli())+`}`, http.StatusInternalServerError)
	}
}

// sendStructuredError sends a structured error message to a WebSocket client
func (s *Server) sendStructuredError(client *Client, code, message string) {
	errorMsg := ErrorMessage{
		Type:      "error",
		Code:      code,
		Message:   message,
		Timestamp: time.Now().UnixMilli(),
	}
	
	if err := client.Send(errorMsg); err != nil {
		s.logger.Warn("Failed to send structured error to client", map[string]interface{}{
			"error": err.Error(),
			"code":  code,
		})
	}
} 
