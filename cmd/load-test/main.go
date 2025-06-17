package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

var (
	clients       = flag.Int("clients", 1000, "Number of concurrent WebSocket clients")
	duration      = flag.Duration("duration", 60*time.Second, "Test duration")
	serverURL     = flag.String("url", "ws://localhost:8080/ws", "WebSocket server URL")
	rampUp        = flag.Duration("rampup", 10*time.Second, "Time to ramp up all clients")
	printInterval = flag.Duration("print", 5*time.Second, "Statistics print interval")
)

type Stats struct {
	connected    int64
	disconnected int64
	messages     int64
	errors       int64
}

func main() {
	flag.Parse()
	
	fmt.Printf("🚀 WebSocket Load Test\n")
	fmt.Printf("📊 Configuration:\n")
	fmt.Printf("   Clients: %d\n", *clients)
	fmt.Printf("   Duration: %v\n", *duration)
	fmt.Printf("   Server: %s\n", *serverURL)
	fmt.Printf("   Ramp-up: %v\n", *rampUp)
	fmt.Printf("\n")
	
	// Parse URL
	u, err := url.Parse(*serverURL)
	if err != nil {
		log.Fatalf("Invalid URL: %v", err)
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	
	var stats Stats
	var wg sync.WaitGroup
	
	// Start statistics reporter
	go reportStats(ctx, &stats)
	
	// Start clients with ramp-up
	clientInterval := *rampUp / time.Duration(*clients)
	
	fmt.Printf("🔄 Starting %d clients over %v (%.2fms interval)\n", 
		*clients, *rampUp, float64(clientInterval.Nanoseconds())/1e6)
	
	for i := 0; i < *clients; i++ {
		wg.Add(1)
		go startClient(ctx, &wg, u, i, &stats)
		
		// Ramp up delay
		if clientInterval > 0 {
			time.Sleep(clientInterval)
		}
		
		// Quick status during ramp-up
		if (i+1)%100 == 0 {
			fmt.Printf("   Started %d/%d clients...\n", i+1, *clients)
		}
	}
	
	fmt.Printf("✅ All %d clients started\n", *clients)
	
	// Wait for test completion or interrupt
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	
	select {
	case <-ctx.Done():
		fmt.Printf("\n⏰ Test duration completed\n")
	case <-sigChan:
		fmt.Printf("\n🛑 Interrupted by user\n")
		cancel()
	}
	
	fmt.Printf("⏳ Waiting for clients to disconnect...\n")
	wg.Wait()
	
	// Final statistics
	fmt.Printf("\n📈 Final Statistics:\n")
	fmt.Printf("   Peak Connected: %d\n", atomic.LoadInt64(&stats.connected))
	fmt.Printf("   Total Messages: %d\n", atomic.LoadInt64(&stats.messages))
	fmt.Printf("   Total Errors: %d\n", atomic.LoadInt64(&stats.errors))
	fmt.Printf("   Success Rate: %.2f%%\n", 
		100.0*float64(atomic.LoadInt64(&stats.messages))/float64(atomic.LoadInt64(&stats.messages)+atomic.LoadInt64(&stats.errors)))
}

func startClient(ctx context.Context, wg *sync.WaitGroup, u *url.URL, clientID int, stats *Stats) {
	defer wg.Done()
	
	// Connect to WebSocket
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		atomic.AddInt64(&stats.errors, 1)
		return
	}
	defer conn.Close()
	
	atomic.AddInt64(&stats.connected, 1)
	defer atomic.AddInt64(&stats.disconnected, 1)
	
	// Set up message handling
	go func() {
		defer conn.Close()
		for {
			select {
			case <-ctx.Done():
				return
			default:
				_, message, err := conn.ReadMessage()
				if err != nil {
					if !websocket.IsCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
						atomic.AddInt64(&stats.errors, 1)
					}
					return
				}
				
				atomic.AddInt64(&stats.messages, 1)
				
				// Optional: Log first few messages for verification
				if atomic.LoadInt64(&stats.messages) <= 5 {
					fmt.Printf("📨 Client %d received: %s\n", clientID, string(message)[:min(100, len(message))])
				}
			}
		}
	}()
	
	// Keep connection alive
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Send ping to keep connection alive
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				atomic.AddInt64(&stats.errors, 1)
				return
			}
		}
	}
}

func reportStats(ctx context.Context, stats *Stats) {
	ticker := time.NewTicker(*printInterval)
	defer ticker.Stop()
	
	var lastMessages int64
	startTime := time.Now()
	
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			connected := atomic.LoadInt64(&stats.connected)
			disconnected := atomic.LoadInt64(&stats.disconnected)
			messages := atomic.LoadInt64(&stats.messages)
			errors := atomic.LoadInt64(&stats.errors)
			
			currentActive := connected - disconnected
			messagesThisInterval := messages - lastMessages
			messageRate := float64(messagesThisInterval) / printInterval.Seconds()
			totalRate := float64(messages) / time.Since(startTime).Seconds()
			
			fmt.Printf("📊 [STATS] Active: %d | Messages: %d (+%d) | Rate: %.1f/s (avg: %.1f/s) | Errors: %d\n",
				currentActive, messages, messagesThisInterval, messageRate, totalRate, errors)
			
			lastMessages = messages
		}
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
} 