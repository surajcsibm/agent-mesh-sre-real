#!/usr/bin/env bash
# trigger-real-lag-spike.sh
# 
# Trigger a REAL consumer lag spike on the Kafka cluster.
# This produces actual messages to create genuine lag.
#
# Usage:
#   ./scripts/trigger-real-lag-spike.sh [message_count] [topic]
#
# Examples:
#   ./scripts/trigger-real-lag-spike.sh              # 1000 messages to demo.payments.events
#   ./scripts/trigger-real-lag-spike.sh 5000         # 5000 messages
#   ./scripts/trigger-real-lag-spike.sh 2000 my-topic

set -euo pipefail

MESSAGE_COUNT="${1:-1000}"
TOPIC="${2:-demo.payments.events}"
API_URL="${API_URL:-http://localhost:3000}"

echo "╭──────────────────────────────────────────────────────────────────────╮"
echo "│  Agent Mesh SRE — Real Lag Spike Trigger                             │"
echo "╰──────────────────────────────────────────────────────────────────────╯"
echo ""
echo "  Topic:          $TOPIC"
echo "  Message Count:  $MESSAGE_COUNT"
echo "  API URL:        $API_URL"
echo ""

# Check cluster health first
echo "→ Checking cluster health..."
HEALTH=$(curl -s "$API_URL/api/cluster/health")
HEALTHY=$(echo "$HEALTH" | jq -r '.ok')
MODE=$(echo "$HEALTH" | jq -r '.mode')

if [ "$MODE" != "real" ]; then
  echo "✗ Error: Cluster is in $MODE mode. Set KAFKA_MODE=real in .env.local"
  exit 1
fi

if [ "$HEALTHY" != "true" ]; then
  echo "✗ Error: Cluster is not healthy"
  echo "$HEALTH" | jq .
  exit 1
fi

BROKER_COUNT=$(echo "$HEALTH" | jq -r '.brokerCount')
echo "✓ Cluster healthy: $BROKER_COUNT broker(s)"
echo ""

# Trigger the lag spike
echo "→ Triggering lag spike..."
RESULT=$(curl -s -X POST "$API_URL/api/cluster/trigger" \
  -H "Content-Type: application/json" \
  -d "{\"scenario\": \"lag-spike\", \"options\": {\"topic\": \"$TOPIC\", \"messageCount\": $MESSAGE_COUNT}}")

OK=$(echo "$RESULT" | jq -r '.ok')
if [ "$OK" = "true" ]; then
  PRODUCED=$(echo "$RESULT" | jq -r '.result.produced')
  echo "✓ Lag spike triggered: $PRODUCED messages produced to $TOPIC"
  echo ""
  echo "→ Checking consumer lag..."
  sleep 2
  
  LAG_RESULT=$(curl -s -X POST "$API_URL/api/cluster/trigger" \
    -H "Content-Type: application/json" \
    -d "{\"scenario\": \"get-lag\", \"options\": {\"topic\": \"$TOPIC\", \"groupId\": \"payments-consumer\"}}")
  
  LAG=$(echo "$LAG_RESULT" | jq -r '.result.lag // "unknown"')
  echo "✓ Current lag for payments-consumer: $LAG messages"
else
  echo "✗ Error triggering lag spike:"
  echo "$RESULT" | jq .
  exit 1
fi

echo ""
echo "╭──────────────────────────────────────────────────────────────────────╮"
echo "│  Done! The Monitor Agent should detect this lag spike shortly.       │"
echo "│  Watch the UI at $API_URL for the MRAL loop to trigger.              │"
echo "╰──────────────────────────────────────────────────────────────────────╯"
