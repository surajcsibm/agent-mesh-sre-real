#!/usr/bin/env bash
# check-real-cluster.sh
# 
# Check the real Kafka cluster status and metrics.
#
# Usage:
#   ./scripts/check-real-cluster.sh

set -euo pipefail

API_URL="${API_URL:-http://localhost:3000}"

echo "╭──────────────────────────────────────────────────────────────────────╮"
echo "│  Agent Mesh SRE — Real Cluster Status                                │"
echo "╰──────────────────────────────────────────────────────────────────────╯"
echo ""

# Check health
echo "→ Checking cluster health..."
HEALTH=$(curl -s "$API_URL/api/cluster/health")
echo "$HEALTH" | jq .
echo ""

MODE=$(echo "$HEALTH" | jq -r '.mode')
if [ "$MODE" != "real" ]; then
  echo "⚠ Warning: Running in $MODE mode. Set KAFKA_MODE=real for real cluster interaction."
  exit 0
fi

# Get full metrics
echo "→ Fetching cluster metrics..."
METRICS=$(curl -s "$API_URL/api/cluster/metrics")

OK=$(echo "$METRICS" | jq -r '.ok')
if [ "$OK" = "true" ]; then
  echo ""
  echo "Cluster Overview:"
  echo "  Brokers:                  $(echo "$METRICS" | jq -r '.metrics.brokerCount')"
  echo "  Controller Epoch:         $(echo "$METRICS" | jq -r '.metrics.controllerEpoch')"
  echo "  Under-replicated Parts:   $(echo "$METRICS" | jq -r '.metrics.underReplicatedPartitions')"
  echo ""
  
  echo "Topics:"
  echo "$METRICS" | jq -r '.metrics.topics[] | "  - \(.name): \(.partitions) partitions, RF=\(.replicationFactor)"'
  echo ""
  
  echo "Consumer Groups:"
  echo "$METRICS" | jq -r '.metrics.consumerGroups[] | "  - \(.groupId): state=\(.state), members=\(.memberCount), lag=\(.lag)"'
else
  echo "✗ Error fetching metrics:"
  echo "$METRICS" | jq .
fi

echo ""
echo "╰──────────────────────────────────────────────────────────────────────╯"
