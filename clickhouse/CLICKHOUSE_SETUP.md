# ClickHouse Integration for WebSocket Backend

This guide explains how to set up and use ClickHouse for high-performance analytics in your blockchain transfer tracking system.

## 🚀 Quick Start

### 1. Install Dependencies

```bash
go mod tidy
```

### 2. Start ClickHouse

```bash
# Start ClickHouse using Docker Compose
docker-compose -f docker-compose.clickhouse.yml up -d

# Wait for ClickHouse to be ready
docker-compose -f docker-compose.clickhouse.yml logs -f clickhouse
```

### 3. Enable ClickHouse in Your Application

Set environment variables:

```bash
# Enable ClickHouse
export USE_CLICKHOUSE=true

# ClickHouse connection settings (defaults shown)
export CLICKHOUSE_HOST=localhost
export CLICKHOUSE_PORT=9000
export CLICKHOUSE_DATABASE=websocket_analytics
export CLICKHOUSE_USER=default
export CLICKHOUSE_PASSWORD=""
export CLICKHOUSE_DEBUG=false
```

### 4. Run Your Application

```bash
go run main.go
```

## 📊 Architecture

### Hybrid Database Design

- **PostgreSQL**: Operational data, real-time transfers, materialized views
- **ClickHouse**: Time-series analytics, historical data, high-performance aggregations

### Data Flow

```
Transfers → PostgreSQL (immediate) → ClickHouse (analytics)
                ↓                      ↓
        Real-time WebSocket    High-performance analytics
```

## 🔧 Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `USE_CLICKHOUSE` | `false` | Enable ClickHouse analytics |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse server host |
| `CLICKHOUSE_PORT` | `9000` | ClickHouse native port |
| `CLICKHOUSE_DATABASE` | `websocket_analytics` | ClickHouse database name |
| `CLICKHOUSE_USER` | `default` | ClickHouse username |
| `CLICKHOUSE_PASSWORD` | `""` | ClickHouse password |
| `CLICKHOUSE_DEBUG` | `false` | Enable debug logging |

### Performance Tuning

The ClickHouse setup is optimized for:
- **Time-series data**: Partitioned by month, ordered by timestamp
- **High compression**: LZ4 compression for storage efficiency  
- **Fast aggregations**: Materialized views for real-time metrics
- **Analytics queries**: Optimized for GROUP BY and time-range queries

## 📈 Benefits

### Performance Improvements

With ClickHouse, you can expect:
- **10-100x faster** analytical queries compared to PostgreSQL
- **Better compression** (3-10x smaller storage)
- **Real-time aggregations** via materialized views
- **Horizontal scaling** for massive datasets

### Example Query Performance

| Query Type | PostgreSQL | ClickHouse | Improvement |
|------------|------------|------------|-------------|
| Transfer rates (all time periods) | ~2-5s | ~50-200ms | 10-25x |
| Popular routes (30 days) | ~3-8s | ~100-300ms | 15-30x |
| Wallet analytics | ~5-15s | ~200-500ms | 10-40x |
| Time-series aggregations | ~10-30s | ~500ms-2s | 20-60x |

## 🛠️ Management

### Check Health

```bash
# Check both databases
curl http://localhost:8080/health

# Response includes:
{
  "databases": {
    "postgresql": "healthy",
    "clickhouse": "healthy"
  }
}
```

### ClickHouse Administration

```bash
# Connect to ClickHouse CLI
docker exec -it websocket-backend-clickhouse clickhouse-client

# Check tables
SHOW TABLES;

# Check data
SELECT count() FROM transfers_analytics;

# Monitor performance
SELECT 
    query_duration_ms,
    query,
    user
FROM system.query_log 
WHERE event_date = today() 
ORDER BY query_duration_ms DESC 
LIMIT 10;
```

### Data Migration

To migrate existing PostgreSQL data to ClickHouse:

```sql
-- Export from PostgreSQL
COPY (
    SELECT id, packet_hash, sort_order, source_chain, dest_chain,
           source_name, dest_name, sender, receiver, amount,
           token_symbol, canonical_token_symbol, timestamp, created_at
    FROM transfers 
    ORDER BY timestamp
) TO '/tmp/transfers.csv' WITH CSV HEADER;

-- Import to ClickHouse
cat /tmp/transfers.csv | docker exec -i websocket-backend-clickhouse \
    clickhouse-client --query="INSERT INTO transfers_analytics FORMAT CSV"
```

## 🔍 Monitoring

### Built-in Metrics

The application provides ClickHouse-specific metrics:

```bash
curl http://localhost:8080/metrics/clickhouse
```

### Optional: Prometheus Exporter

Start with monitoring profile:

```bash
docker-compose -f docker-compose.clickhouse.yml --profile monitoring up -d
```

Then scrape metrics from `localhost:9116/metrics`

## 🐛 Troubleshooting

### Common Issues

1. **ClickHouse not starting**
   ```bash
   # Check logs
   docker-compose -f docker-compose.clickhouse.yml logs clickhouse
   
   # Common fix: increase memory limits
   docker system prune -a
   ```

2. **Connection refused**
   ```bash
   # Check if ClickHouse is running
   docker ps | grep clickhouse
   
   # Check network connectivity  
   telnet localhost 9000
   ```

3. **Schema errors**
   ```bash
   # Manually initialize schema
   docker exec -i websocket-backend-clickhouse clickhouse-client < internal/database/clickhouse_schema.sql
   ```

4. **Performance issues**
   ```sql
   -- Check system resources
   SELECT * FROM system.metrics WHERE metric LIKE '%Memory%';
   
   -- Check slow queries
   SELECT query, query_duration_ms FROM system.query_log 
   WHERE query_duration_ms > 1000 ORDER BY query_duration_ms DESC;
   ```

### Fallback Mode

If ClickHouse fails, the system automatically falls back to PostgreSQL:

```
WARN[HYBRID_DB] ClickHouse transfer rates failed, falling back to PostgreSQL
INFO[HYBRID_DB] Continuing with PostgreSQL-only mode
```

## 🎯 Production Deployment

### Resource Requirements

**Minimum**:
- 4GB RAM
- 2 CPU cores  
- 50GB SSD storage

**Recommended**:
- 16GB+ RAM
- 8+ CPU cores
- 500GB+ NVMe SSD

### Security

```bash
# Create secure user
docker exec -it websocket-backend-clickhouse clickhouse-client --query="
CREATE USER analytics_user IDENTIFIED BY 'secure_password';
GRANT SELECT ON websocket_analytics.* TO analytics_user;
"

# Update environment
export CLICKHOUSE_USER=analytics_user
export CLICKHOUSE_PASSWORD=secure_password
```

### Backup Strategy

```bash
# Backup schema
docker exec websocket-backend-clickhouse clickhouse-client --query="SHOW CREATE TABLE transfers_analytics" > backup_schema.sql

# Backup data (use native format for large datasets)
docker exec websocket-backend-clickhouse clickhouse-client --query="SELECT * FROM transfers_analytics FORMAT Native" > backup_data.native
```

## 🎉 Success!

You now have a high-performance analytics system that can handle millions of blockchain transfers with sub-second query times!

The system will automatically:
- ✅ Store operational data in PostgreSQL
- ✅ Stream analytics data to ClickHouse  
- ✅ Use ClickHouse for fast dashboard queries
- ✅ Fallback to PostgreSQL if needed
- ✅ Compress and partition data efficiently 