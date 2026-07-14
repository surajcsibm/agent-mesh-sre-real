#!/usr/bin/env bash
# test-real-kafka.sh
# 
# Comprehensive test of real Kafka integration.
# Tests all endpoints and verifies real cluster connectivity.
#
# Usage:
#   ./scripts/test-real-kafka.sh

set -euo pipefail

API_URL="${API_URL:-http://localhost:3000}"
PASSED=0
FAILED=0

echo "╭──────────────────────────────────────────────────────────────────────╮"
echo "│  Agent Mesh SRE — Real Kafka Integration Tests                       │"
echo "╰──────────────────────────────────────────────────────────────────────╯"
echo ""
echo "API URL: $API_URL"
echo ""

test_endpoint() {
  local name="$1"
  local method="$2"
  local endpoint="$3"
  local body="${4:-}"
  local expected_field="${5:-ok}"
  
  echo -n "  Testing $name... "
  
  if [ "$method" = "GET" ]; then
    RESPONSE=$(curl -s "$API_URL$endpoint" 2>/dev/null || echo '{"error":"connection failed"}')
  else
    RESPONSE=$(curl -s -X POST "$API_URL$endpoint" \
      -H "Content-Type: application/json" \
      -d "$body" 2>/dev/null || echo '{"error":"connection failed"}')
  fi
  
  OK=$(echo "$RESPONSE" | jq -r ".$expected_field // \"false\"" 2>/dev/null || echo "false")
  
  if [ "$OK" = "true" ]; then
    echo "✓ PASS"
    ((PASSED++))
  else
    echo "✗ FAIL"
    echo "    Response: $(echo "$RESPONSE" | jq -c . 2>/dev/null || echo "$RESPONSE")"
    ((FAILED++))
  fi
}

echo "─── Health Checks ───────────────────────────────────────────────────────"
test_endpoint "Cluster Health" "GET" "/api/cluster/health"

echo ""
echo "─── Mode Detection ──────────────────────────────────────────────────────"
MODE_RESPONSE=$(curl -s "$API_URL/api/cluster/health" 2>/dev/null)
MODE=$(echo "$MODE_RESPONSE" | jq -r '.mode' 2>/dev/null || echo "unknown")
echo "  Current mode: $MODE"

if [ "$MODE" != "real" ]; then
  echo ""
  echo "⚠ Warning: Not in REAL mode. Some tests will be skipped."
  echo "  Set KAFKA_MODE=real in .env.local for full testing."
  echo ""
fi

echo ""
echo "─── Cluster Metrics ─────────────────────────────────────────────────────"
if [ "$MODE" = "real" ]; then
  test_endpoint "Cluster Metrics" "GET" "/api/cluster/metrics"
  
  METRICS=$(curl -s "$API_URL/api/cluster/metrics" 2>/dev/null)
  BROKER_COUNT=$(echo "$METRICS" | jq -r '.metrics.brokerCount // 0' 2>/dev/null)
  TOPIC_COUNT=$(echo "$METRICS" | jq -r '.metrics.topics | length // 0' 2>/dev/null)
  CG_COUNT=$(echo "$METRICS" | jq -r '.metrics.consumerGroups | length // 0' 2>/dev/null)
  
  echo "    Brokers: $BROKER_COUNT"
  echo "    Topics: $TOPIC_COUNT"
  echo "    Consumer Groups: $CG_COUNT"
else
  echo "  Skipped (not in REAL mode)"
fi

echo ""
echo "─── Scenario Triggers ───────────────────────────────────────────────────"
if [ "$MODE" = "real" ]; then
  test_endpoint "Health Check Trigger" "POST" "/api/cluster/trigger" '{"scenario":"health-check"}'
  
  # Test lag spike with small message count
  echo -n "  Testing Lag Spike (10 msgs)... "
  LAG_RESPONSE=$(curl -s -X POST "$API_URL/api/cluster/trigger" \
    -H "Content-Type: application/json" \
    -d '{"scenario":"lag-spike","options":{"messageCount":10}}' 2>/dev/null)
  LAG_OK=$(echo "$LAG_RESPONSE" | jq -r '.ok' 2>/dev/null || echo "false")
  if [ "$LAG_OK" = "true" ]; then
    PRODUCED=$(echo "$LAG_RESPONSE" | jq -r '.result.produced' 2>/dev/null)
    echo "✓ PASS (produced $PRODUCED messages)"
    ((PASSED++))
  else
    echo "✗ FAIL"
    echo "    Response: $(echo "$LAG_RESPONSE" | jq -c . 2>/dev/null)"
    ((FAILED++))
  fi
  
  # Test get-lag
  test_endpoint "Get Consumer Lag" "POST" "/api/cluster/trigger" '{"scenario":"get-lag","options":{"groupId":"payments-consumer","topic":"demo.payments.events"}}'
else
  echo "  Skipped (not in REAL mode)"
fi

echo ""
echo "─── Mesh State ──────────────────────────────────────────────────────────"
test_endpoint "Events SSE Endpoint" "GET" "/api/events" "" "type"

echo ""
echo "─── Scenario API ────────────────────────────────────────────────────────"
# These work in both modes
test_endpoint "Lag Spike Scenario" "POST" "/api/scenario" '{"kind":"lag-spike"}'

echo ""
echo "╭──────────────────────────────────────────────────────────────────────╮"
echo "│  Test Results                                                         │"
echo "╰──────────────────────────────────────────────────────────────────────╯"
echo ""
echo "  Passed: $PASSED"
echo "  Failed: $FAILED"
echo ""

if [ "$FAILED" -gt 0 ]; then
  echo "  ⚠ Some tests failed. Check the output above for details."
  exit 1
else
  echo "  ✓ All tests passed!"
fi
