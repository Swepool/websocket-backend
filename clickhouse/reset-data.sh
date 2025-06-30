#!/bin/bash
set -e

echo "🔄 Resetting ClickHouse data only..."

# Drop and recreate database
echo "🗑️  Dropping database..."
docker exec -it websocket-backend-clickhouse clickhouse-client --query "DROP DATABASE IF EXISTS websocket_analytics"

echo "📋 Recreating database and applying schema..."
docker exec -i websocket-backend-clickhouse clickhouse-client < ../internal/storage/clickhouse_schema.sql

echo "✅ ClickHouse data reset complete!"
echo "🔗 Database: websocket_analytics"
echo "👤 User: websocket_user (no password)" 