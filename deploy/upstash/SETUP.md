# Upstash Kafka — Free Serverless Kafka Setup

Upstash Kafka is permanently free (10,000 messages/day, no credit card needed).
It uses standard Kafka protocol (KafkaJS works directly, no REST wrapper needed).

---

## Step 1 — Sign up

1. Go to **https://console.upstash.com**
2. Click **Sign up** → use GitHub or Google (much simpler than RedPanda)
3. You land on the Upstash Console dashboard

---

## Step 2 — Create a Kafka cluster

1. In the left sidebar click **Kafka**
2. Click **Create Cluster**
3. Fill in:
   - **Name**: `agent-mesh-kafka`
   - **Region**: pick closest to you (e.g. `us-east-1` or `eu-west-1`)
   - **Plan**: **Free** (shown at the top — 10K messages/day)
4. Click **Create Cluster** — ready in ~10 seconds

---

## Step 3 — Copy your credentials

On the cluster overview page you'll see a **Connect** tab or **Details** section:

| Field | Where to find it | .env.local key |
|-------|-----------------|---------------|
| Bootstrap endpoint | `Endpoint` field | `KAFKA_BOOTSTRAP` |
| Username | `Username` field | `KAFKA_USERNAME` |
| Password | `Password` field (click eye icon) | `KAFKA_PASSWORD` |

The endpoint looks like: `knowing-condor-12345-us1-kafka.upstash.io:9092`

---

## Step 4 — Create the topics

In your cluster, click **Topics** → **Create Topic** for each:

| Topic name              | Partitions | Retention  |
|-------------------------|------------|------------|
| `ops.requests.v1`       | 1          | 7 days     |
| `ops.kafka.metrics.v1`  | 1          | 1 day      |
| `ops.incidents.v1`      | 1          | 30 days    |
| `ops.actions.audit.v1`  | 1          | 365 days   |
| `ops.lessons.v1`        | 1          | Compact    |
| `ops.notifications.v1`  | 1          | 7 days     |
| `demo.payments.events`  | 1          | 3 days     |

---

## Step 5 — Write your .env.local

Run the interactive script:

```bash
./deploy/scripts/configure-upstash.sh
```

Or set manually in `.env.local`:

```env
KAFKA_MODE=real
KAFKA_BOOTSTRAP=knowing-condor-12345-us1-kafka.upstash.io:9092
KAFKA_USERNAME=your-upstash-username
KAFKA_PASSWORD=your-upstash-password
KAFKA_SASL_MECHANISM=scram-sha-256
```

No `KAFKA_CA_CERT_BASE64` needed — Upstash uses public CAs.

---

## Step 6 — Start the app

```bash
npm run dev
```

Open **http://localhost:3000**

---

## Vercel deployment

Add these 5 vars in **Vercel Dashboard → Settings → Environment Variables**:

```
KAFKA_MODE              real
KAFKA_BOOTSTRAP         knowing-condor-12345-us1-kafka.upstash.io:9092
KAFKA_USERNAME          your-upstash-username
KAFKA_PASSWORD          your-upstash-password
KAFKA_SASL_MECHANISM    scram-sha-256
```

Trigger a redeploy after saving.

---

## Free tier limits

| Limit               | Value              |
|---------------------|--------------------|
| Messages/day        | 10,000             |
| Storage             | 100 MB             |
| Max message size    | 1 MB               |
| Topics              | Unlimited          |
| Credit card         | Not required       |
