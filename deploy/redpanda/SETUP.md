# RedPanda Cloud Serverless — Free Kafka Cluster Setup

RedPanda Cloud's Serverless tier is permanently free (no credit card, no expiry).
It speaks standard Kafka protocol, so the existing KafkaJS code works with no changes
beyond setting three environment variables.

---

## Step 1 — Create a RedPanda Cloud account

1. Go to **https://cloud.redpanda.com**
2. Click **Sign Up** → use your Google account or email
3. Verify your email if prompted

---

## Step 2 — Create a Serverless cluster

1. After login, click **Create cluster**
2. Choose **Serverless** (the free tier — look for the "Free" badge)
3. Pick a region close to you (e.g. `us-east-1` or `eu-west-1`)
4. Name it `agent-mesh-kafka` (or anything you like)
5. Click **Create** — the cluster spins up in about 30 seconds

---

## Step 3 — Get the Bootstrap URL

1. Open your new cluster from the dashboard
2. Go to **Overview** tab
3. Under **Connection info**, copy the **Bootstrap server** URL
   - It looks like: `seed-xxxxxxxx.cloud.redpanda.com:9092`
   - This is your `KAFKA_BOOTSTRAP` value

---

## Step 4 — Create a user (SASL credentials)

1. In your cluster, go to **Security** → **Users**
2. Click **Create user**
3. Username: `agent-mesh-controller`
4. Set a strong password and copy it — RedPanda won't show it again
5. Click **Create**

---

## Step 5 — Set ACLs for the user

1. Still in **Security**, click **ACLs**
2. Click **Create ACL**
3. Set:
   - **Principal**: `User:agent-mesh-controller`
   - **Resource type**: `Topic` / **Resource name**: `*` / **Pattern**: `Literal`
   - **Operations**: `All`
4. Repeat for **Resource type**: `Group`, name `*`, operations `All`
5. Repeat for **Resource type**: `Cluster`, name `kafka-cluster`, operations `Describe, Alter`

---

## Step 6 — Create the topics

In **Topics**, create each topic below with **1 partition, replication factor 1**:

| Topic name              | Retention |
|-------------------------|-----------|
| `ops.requests.v1`       | 7 days    |
| `ops.kafka.metrics.v1`  | 1 day     |
| `ops.incidents.v1`      | 30 days   |
| `ops.actions.audit.v1`  | 365 days  |
| `ops.lessons.v1`        | Compact   |
| `ops.notifications.v1`  | 7 days    |
| `demo.payments.events`  | 3 days    |

---

## Step 7 — Write your .env.local

Run the interactive script which prompts you for the values above:

```bash
./deploy/scripts/configure-redpanda.sh
```

Or set the vars manually:

```bash
# .env.local
KAFKA_MODE=real
KAFKA_BOOTSTRAP=seed-xxxxxxxx.cloud.redpanda.com:9092
KAFKA_USERNAME=agent-mesh-controller
KAFKA_PASSWORD=your-password-here
KAFKA_SASL_MECHANISM=scram-sha-256
# No KAFKA_CA_CERT_BASE64 needed — RedPanda uses public CAs (Let's Encrypt)
```

---

## Step 8 — Start the app

```bash
npm run dev
```

Open **http://localhost:3000** → the Setup Wizard will connect to your RedPanda cluster.

---

## Vercel deployment

Copy the same four vars into:
**Vercel Dashboard → Your project → Settings → Environment Variables**

```
KAFKA_MODE              real
KAFKA_BOOTSTRAP         seed-xxxxxxxx.cloud.redpanda.com:9092
KAFKA_USERNAME          agent-mesh-controller
KAFKA_PASSWORD          your-password-here
KAFKA_SASL_MECHANISM    scram-sha-256
```

Trigger a redeploy after saving the vars.

---

## Free tier limits

| Limit               | Value          |
|---------------------|----------------|
| Storage             | 10 GiB         |
| Throughput (in/out) | 10 MiB/s burst |
| Retention           | Up to 24 h on free tier (configurable) |
| Topics              | Unlimited       |
| Credit card         | Not required    |
