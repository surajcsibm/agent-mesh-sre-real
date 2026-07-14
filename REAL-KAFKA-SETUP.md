# Real Kafka Integration Guide

This document explains how to configure and use **real Kafka cluster interactions** in the Agent Mesh SRE demo.

## Overview

The application supports two modes:
- **MOCK mode** (default): In-memory simulation, no real Kafka required
- **REAL mode**: Connects to an actual Kafka cluster (Confluent Cloud, Aiven, RedPanda, Strimzi, etc.)

In REAL mode, all operations interact with your actual Kafka cluster:
- Metrics are collected from real brokers
- Messages are produced to real topics
- Consumer group lag is measured from actual offsets
- Scenarios trigger real cluster conditions

---

## Quick Start

### 1. Run the Setup Script

```bash
./scripts/setup-real-mode.sh
```

This interactive script will:
- Ask for your Kafka provider (Confluent, Aiven, RedPanda, Strimzi, or custom)
- Collect connection details (bootstrap server, credentials)
- Generate a `.env.local` file with the correct settings
- Test the connection

### 2. Manual Configuration

Alternatively, create `.env.local` manually:

```bash
# ── Kafka Mode ────────────────────────────────────────────────────────────────
KAFKA_MODE=real

# ── Connection Settings ───────────────────────────────────────────────────────
KAFKA_BOOTSTRAP=your-broker.example.com:9092
KAFKA_USERNAME=your-username
KAFKA_PASSWORD=your-password
KAFKA_SASL_MECHANISM=scram-sha-256   # or: plain, scram-sha-512
KAFKA_SSL_ENABLED=true

# For custom CA (Strimzi, Aiven with custom cert):
# KAFKA_CA_CERT_BASE64=<base64-encoded-ca-cert>
```

### 3. Start the Application

```bash
npm run dev
```

---

## Provider-Specific Configuration

### Confluent Cloud

```bash
KAFKA_MODE=real
KAFKA_BOOTSTRAP=pkc-xxxxx.us-east-1.aws.confluent.cloud:9092
KAFKA_USERNAME=<your-api-key>
KAFKA_PASSWORD=<your-api-secret>
KAFKA_SASL_MECHANISM=plain
KAFKA_SSL_ENABLED=true
```

### Aiven

```bash
KAFKA_MODE=real
KAFKA_BOOTSTRAP=kafka-xxxxx.aivencloud.com:12345
KAFKA_USERNAME=avnadmin
KAFKA_PASSWORD=<your-password>
KAFKA_SASL_MECHANISM=scram-sha-256
KAFKA_SSL_ENABLED=true
KAFKA_CA_CERT_BASE64=<base64-ca-cert>
```

### RedPanda Cloud

```bash
KAFKA_MODE=real
KAFKA_BOOTSTRAP=seed-xxxxx.cloud.redpanda.com:9092
KAFKA_USERNAME=agent-mesh-controller
KAFKA_PASSWORD=<your-password>
KAFKA_SASL_MECHANISM=scram-sha-256
KAFKA_SSL_ENABLED=true
```

### Strimzi on Kubernetes

```bash
KAFKA_MODE=real
KAFKA_BOOTSTRAP=<external-bootstrap>:9094
KAFKA_USERNAME=agent-mesh-controller
KAFKA_PASSWORD=<scram-password-from-secret>
KAFKA_SASL_MECHANISM=scram-sha-512
KAFKA_SSL_ENABLED=true
KAFKA_CA_CERT_BASE64=<base64-cluster-ca>
```

---

## API Endpoints

### Health Check
```bash
curl http://localhost:3000/api/cluster/health
```

Returns cluster connectivity status, broker count, and controller ID.

### Cluster Metrics
```bash
curl http://localhost:3000/api/cluster/metrics
```

Returns comprehensive metrics:
- Broker count and controller epoch
- Topic list with partition counts
- Consumer groups with lag
- Under-replicated partitions

### Trigger Real Scenarios
```bash
# Create real consumer lag
curl -X POST http://localhost:3000/api/cluster/trigger \
  -H "Content-Type: application/json" \
  -d '{"scenario": "lag-spike", "options": {"messageCount": 1000}}'

# Check consumer lag
curl -X POST http://localhost:3000/api/cluster/trigger \
  -H "Content-Type: application/json" \
  -d '{"scenario": "get-lag", "options": {"groupId": "payments-consumer"}}'

# Health check
curl -X POST http://localhost:3000/api/cluster/trigger \
  -H "Content-Type: application/json" \
  -d '{"scenario": "health-check"}'
```

---

## Shell Scripts

### Check Cluster Status
```bash
./scripts/check-real-cluster.sh
```

### Trigger Lag Spike
```bash
./scripts/trigger-real-lag-spike.sh [message_count] [topic]

# Examples:
./scripts/trigger-real-lag-spike.sh              # 1000 messages
./scripts/trigger-real-lag-spike.sh 5000         # 5000 messages
./scripts/trigger-real-lag-spike.sh 2000 my-topic
```

### Test Integration
```bash
./scripts/test-real-kafka.sh
```

---

## How Real Mode Works

### Metrics Collection

In REAL mode, the Monitor Agent's polling loop (`monitor-poll.ts`) collects metrics from:

1. **real-kafka-client.ts** - Primary source using KafkaJS Admin API
   - Broker count and controller epoch
   - Topic metadata
   - Consumer group state and lag
   - Under-replicated partitions

2. **kafka-admin-cfk.ts** - Fallback/supplemental
   - Topic sizes
   - Schema registry subjects

### Scenario Execution

When you trigger a scenario in REAL mode:

1. **Lag Spike**: Produces actual messages to create real consumer lag
2. **Controller Failover**: Reads real controller epoch from cluster
3. **Share Group**: Monitors real KIP-932 share group state

The MRAL loop then:
- **Monitor**: Detects real conditions from cluster metrics
- **Reason**: Analyzes real data (lag values, broker state)
- **Act**: Logs real cluster state in audit records
- **Learn**: Records lessons based on actual outcomes

### Message Production

All audit records, lessons, and notifications are produced to real Kafka topics:
- `ops.actions.audit.v1`
- `ops.lessons.v1`
- `ops.notifications.v1`
- `ops.incidents.v1`

---

## Required Topics

Create these topics on your cluster (or let the app auto-create them):

| Topic | Partitions | Retention |
|-------|------------|-----------|
| `ops.requests.v1` | 6 | 7 days |
| `ops.kafka.metrics.v1` | 6 | 1 day |
| `ops.incidents.v1` | 6 | 30 days |
| `ops.actions.audit.v1` | 12 | 365 days |
| `ops.lessons.v1` | 3 | Compacted |
| `ops.notifications.v1` | 3 | 7 days |
| `demo.payments.events` | 24 | 3 days |

---

## Troubleshooting

### Connection Refused
- Verify `KAFKA_BOOTSTRAP` is correct
- Check firewall/security group rules
- Ensure SSL settings match your cluster

### Authentication Failed
- Verify `KAFKA_USERNAME` and `KAFKA_PASSWORD`
- Check `KAFKA_SASL_MECHANISM` matches your cluster
- For Confluent Cloud, use API Key as username

### SSL Handshake Failed
- Set `KAFKA_SSL_ENABLED=true` for TLS clusters
- For custom CA, provide `KAFKA_CA_CERT_BASE64`
- For public CAs (Confluent, RedPanda), omit the CA cert

### No Metrics in REAL Mode
- Check `/api/cluster/health` returns `healthy: true`
- Verify topics exist on the cluster
- Check consumer groups are active

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Agent Mesh SRE                               │
│                                                                  │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │ Monitor Poll │───▶│ real-kafka-  │───▶│ KafkaJS      │       │
│  │ (30s cycle)  │    │ client.ts    │    │ Admin API    │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│         │                   │                    │               │
│         ▼                   ▼                    ▼               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐       │
│  │ Mesh State   │    │ Producer     │    │ Consumer     │       │
│  │ (broker,     │    │ (audit,      │    │ (metrics     │       │
│  │  groups)     │    │  lessons)    │    │  subscriber) │       │
│  └──────────────┘    └──────────────┘    └──────────────┘       │
│                              │                    │              │
└──────────────────────────────┼────────────────────┼──────────────┘
                               │                    │
                               ▼                    ▼
                    ┌──────────────────────────────────┐
                    │     Real Kafka Cluster           │
                    │  (Confluent/Aiven/RedPanda/etc)  │
                    └──────────────────────────────────┘
```

---

## Files Added/Modified

### New Files
- `src/lib/real-kafka-client.ts` - Core real Kafka operations
- `src/app/api/cluster/metrics/route.ts` - Metrics endpoint
- `src/app/api/cluster/trigger/route.ts` - Scenario trigger endpoint
- `src/app/api/cluster/health/route.ts` - Health check endpoint
- `scripts/setup-real-mode.sh` - Interactive setup
- `scripts/check-real-cluster.sh` - Status check
- `scripts/trigger-real-lag-spike.sh` - Lag spike trigger
- `scripts/test-real-kafka.sh` - Integration tests

### Modified Files
- `src/lib/monitor-poll.ts` - Uses real-kafka-client for metrics
- `src/lib/mesh.ts` - Uses real-kafka-client for scenario actions
