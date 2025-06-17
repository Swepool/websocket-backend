package fetcher

import (
	"context"
	"log"
	"sync"
	"websocket-backend-new/config"
	"websocket-backend-new/storage"
	"websocket-backend-new/threads/scheduler"
)

// Coordinator manages the clean pipeline: Fetch → Enhance → Schedule → Stream
type Coordinator struct {
	config    *config.Config
	memory    *storage.Memory
	channels  *storage.Channels
	fetcher   *Fetcher
	enhancer  *Enhancer
	scheduler *scheduler.Scheduler
}

// NewCoordinator creates a new main processing coordinator with clean pipeline
func NewCoordinator(config *config.Config, memory *storage.Memory, channels *storage.Channels, graphqlClient GraphQLClient) *Coordinator {
	return &Coordinator{
		config:    config,
		memory:    memory,
		channels:  channels,
		fetcher:   NewFetcher(config, memory, channels, graphqlClient),
		enhancer:  NewEnhancer(memory, channels),
		scheduler: scheduler.NewScheduler(memory, channels),
	}
}

// Start begins the clean pipeline (Thread 1, 2, 3)
func (c *Coordinator) Start(ctx context.Context) {
	log.Printf("[COORDINATOR] 🚀 Starting clean pipeline (Fetch → Enhance → Schedule → Stream)")
	log.Printf("[COORDINATOR] 🎬 Natural streaming enabled - transfers will arrive one by one")
	
	var wg sync.WaitGroup
	
	// Start Thread 1: Fetcher
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.fetcher.Start(ctx)
	}()
	
	// Start Thread 2: Enhancer
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.enhancer.Start(ctx)
	}()
	
	// Start Thread 3: Scheduler
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.scheduler.Start(ctx)
	}()
	
	// Wait for all threads to complete
	wg.Wait()
	log.Printf("[COORDINATOR] ✅ All pipeline threads stopped")
}

// GetStatus returns the current status of the coordinator
func (c *Coordinator) GetStatus() map[string]interface{} {
	lastSortOrder, isInitial := c.memory.GetPollingState()
	
	status := map[string]interface{}{
		"thread":         "clean-pipeline-coordinator", 
		"mode":           "natural-streaming",
		"lastSortOrder":  lastSortOrder,
		"isInitialFetch": isInitial,
		"pollInterval":   c.config.PollInterval.String(),
	}
	
	// Add scheduler status if available
	if c.scheduler != nil {
		schedulerStatus := c.scheduler.GetQueueStatus()
		for k, v := range schedulerStatus {
			status["scheduler_"+k] = v
		}
	}
	
	return status
} 