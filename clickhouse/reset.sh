#!/bin/bash
set -e

echo "🔄 Resetting ClickHouse database..."

# Stop and remove containers/volumes
echo "📦 Stopping containers and removing volumes..."
docker-compose -f docker-compose.clickhouse.yml down -v

# Start fresh containers
echo "🚀 Starting fresh ClickHouse containers..."
docker-compose -f docker-compose.clickhouse.yml up -d

# Wait for ClickHouse to be ready
echo "⏳ Waiting for ClickHouse to be ready..."
sleep 5

# Apply schema (includes database creation and user setup)
echo "📋 Applying schema..."
docker exec -i websocket-backend-clickhouse clickhouse-client < ../internal/storage/clickhouse_schema.sql

echo "✅ ClickHouse reset complete!"
echo "🔗 Database: websocket_analytics"
echo "👤 User: websocket_user (no password)"
echo "🌐 Connection: localhost:9000" 