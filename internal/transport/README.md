# Sharded Broadcaster Implementation

## Overview

The sharded broadcaster is a high-performance, scalable WebSocket broadcasting system designed to handle thousands of concurrent clients efficiently. It addresses the limitations of the original single-threaded broadcaster by distributing clients across multiple shards and using worker pools for message broadcasting.

## Architecture

```
Main Broadcaster
├── Shard Manager
│   ├── Shard 0 (Worker Pool: 4 workers)
│   │   └── Clients: 0-999
│   ├── Shard 1 (Worker Pool: 4 workers)
│   │   └── Clients: 1000-1999
│   └── Shard N (Worker Pool: 4 workers)
│       └── Clients: N*1000...
```

### Key Components

1. **ShardedBroadcaster**: Main coordinator managing multiple shards
2. **Shard**: Container for a subset of clients with its own worker pool
3. **ShardWorker**: Individual workers that handle broadcasting to clients
4. **ShardedClient**: Enhanced client with shard affinity

## Performance Benefits

### Original Broadcaster Bottlenecks:
- Single goroutine handling all broadcasts
- Mutex contention on client map access
- Goroutine explosion (1 goroutine per client per broadcast)
- Fixed channel capacity causing blocking

### Sharded Broadcaster Solutions:
- **Parallel Processing**: Multiple shards process clients simultaneously
- **Reduced Contention**: Each shard has its own mutex and client map
- **Worker Pools**: Controlled number of workers prevent goroutine explosion
- **Load Distribution**: Clients distributed using consistent hashing
- **Horizontal Scaling**: Add more shards for increased capacity

## When to Use Sharding

| Client Count | Recommendation | Configuration |
|--------------|---------------|---------------|
| < 1,000      | Original Broadcaster | `useSharding: false` |
| 1,000-5,000  | Light Sharding | `numShards: 2, workersPerShard: 4` |
| 5,000-10,000 | Medium Sharding | `numShards: 4, workersPerShard: 6` |
| 10,000+      | Heavy Sharding | `numShards: 8+, workersPerShard: 8` |

## Configuration

### Basic Configuration

```go
config := broadcaster.Config{
    MaxClients:      1000,  // Max clients per shard
    BufferSize:      100,   // Buffer size per client
    DropSlowClients: true,  // Drop slow clients
    UseSharding:     true,  // Enable sharding
    NumShards:       4,     // Number of shards
    WorkersPerShard: 4,     // Workers per shard
}
```

### Automatic Configuration

```go
// Get recommended config based on expected load
config := broadcaster.GetRecommendedConfig(expectedClients)
broadcaster := broadcaster.CreateBroadcaster(config, channels, statsCollector)
```

### Manual Optimization

```go
// High-performance configuration for 20,000+ clients
config := broadcaster.Config{
    MaxClients:      2000,  // 2000 clients per shard
    BufferSize:      256,   // Larger buffers
    DropSlowClients: true,
    UseSharding:     true,
    NumShards:       12,    // 12 shards = 24,000 total capacity
    WorkersPerShard: 8,     // 8 workers per shard
}
```

## Performance Characteristics

### Throughput Scaling
- **Original**: ~1,000 clients @ 100 TPS
- **Sharded (4 shards)**: ~4,000 clients @ 400 TPS
- **Sharded (8 shards)**: ~8,000 clients @ 800 TPS

### Memory Usage
- **Per Client**: ~8KB (websocket conn + buffers)
- **Per Shard**: ~2-4MB (maps, channels, workers)
- **Total Overhead**: NumShards * 4MB

### Latency
- **Client Distribution**: O(1) using hash-based sharding
- **Broadcast Latency**: O(clients/shards) per shard
- **Worker Dispatch**: O(1) with worker pools

## Implementation Details

### Client Distribution
Clients are distributed across shards using FNV-1a hash of client ID:
```go
shardID := hash(clientID) % numShards
```

### Worker Load Balancing
Within each shard, workers handle clients using the same hash distribution:
```go
workerID := hash(clientID) % workersPerShard
```

### Fallback Mechanisms
- If worker pool is full, shard broadcasts directly
- If shard broadcast channel is full, message is dropped with warning
- Failed client sends trigger automatic cleanup

## Monitoring & Statistics

### Shard Statistics
```go
stats := broadcaster.GetShardStats()
// Returns:
// {
//   "shard_0": {"clients": 1250, "workers": 4},
//   "shard_1": {"clients": 1180, "workers": 4},
//   "total_clients": 2430,
//   "total_shards": 2
// }
```

### Performance Metrics
- Client count per shard
- Worker utilization
- Broadcast success/failure rates
- Channel buffer usage

## Best Practices

### Shard Sizing
- **1,000-2,000 clients per shard** for optimal performance
- Avoid too many small shards (overhead)
- Avoid too few large shards (contention)

### Worker Pool Sizing
- **4-8 workers per shard** typically optimal
- More workers for CPU-intensive workloads
- Fewer workers for memory-constrained environments

### Buffer Sizing
- **100-256 bytes per client** for typical workloads
- Larger buffers for high-frequency updates
- Monitor dropped messages and adjust accordingly

### Load Testing
```bash
# Test with increasing client counts
websocket-test --clients 1000 --duration 60s
websocket-test --clients 5000 --duration 60s
websocket-test --clients 10000 --duration 60s
```

## Migration Guide

### From Original to Sharded

1. **Update Configuration**:
```go
config.UseSharding = true
config.NumShards = broadcaster.calculateOptimalShards(expectedClients)
```

2. **Use Factory Pattern**:
```go
// Replace direct instantiation
broadcaster := broadcaster.CreateBroadcaster(config, channels, statsCollector)
```

3. **Monitor Performance**:
- Check shard statistics
- Monitor client distribution
- Verify no single shard is overloaded

### Gradual Rollout
1. Start with 2 shards for testing
2. Monitor performance and client distribution
3. Increase shards based on load patterns
4. Fine-tune worker counts based on CPU usage

## Troubleshooting

### Common Issues

**Uneven Client Distribution**:
- Check client ID generation (should be random/unique)
- Verify hash function is working correctly

**High Memory Usage**:
- Reduce buffer sizes
- Decrease workers per shard
- Consider fewer shards with more clients each

**Dropped Messages**:
- Increase buffer sizes
- Add more workers per shard
- Check for slow clients causing backpressure

**High CPU Usage**:
- Reduce workers per shard
- Implement client throttling
- Consider horizontal scaling (more servers)

## Future Enhancements

- **Dynamic Shard Rebalancing**: Automatically adjust shard count based on load
- **Cross-Shard Load Balancing**: Move clients between shards for better distribution
- **Metrics Integration**: Prometheus/monitoring integration
- **Circuit Breakers**: Automatic fallbacks for failed shards
- **Persistence**: Redis-backed client state for multi-instance deployments 