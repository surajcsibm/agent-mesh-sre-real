# Aiven for Apache Kafka — Free Trial Setup

Aiven gives $300 free credit (no charge until it runs out, ~30 days on the
smallest Kafka plan). Standard Kafka protocol — the existing KafkaJS code
works without any changes beyond setting environment variables.

---

## Step 1 — Sign up

1. Go to **https://aiven.io**
2. Click **Start free trial** → sign up with Google or email
3. Verify your email — you land on the Aiven Console

---

## Step 2 — Create a Kafka service

1. Click **+ Create service**
2. Choose **Apache Kafka**
3. Settings:
   - **Cloud provider**: Google Cloud (or any)
   - **Region**: pick closest to you
   - **Plan**: **Startup-2** (smallest — ~$0.17/hr, well within $300 credit)
   - **Service name**: `agent-mesh-kafka`
4. Click **Create service** — takes ~3 minutes to provision

---

## Step 3 — Enable SASL authentication

By default Aiven uses mTLS client certificates. Switch to SASL so
the existing KafkaJS username/password flow works:

1. Open your `agent-mesh-kafka` service
2. Go to the **Advanced configuration** tab
3. Search for `kafka_authentication_methods`
4. Enable **SASL** → click **Save advanced configuration**
5. Wait ~30 seconds for the service to apply the change

---

## Step 4 — Get the connection details

On the service **Overview** tab, find:

| What | Where | .env.local key |
|------|-------|---------------|
| Bootstrap URI | **Connection information → Service URI** | `KAFKA_BOOTSTRAP` |
| Username | **Users** tab → `avnadmin` → show password | `KAFKA_USERNAME` |
| Password | Same as above | `KAFKA_PASSWORD` |
| CA cert | **Overview → Connection information → Download CA cert** | saved as `ca.pem` |

The bootstrap URI looks like:
`kafka-agent-mesh-kafka-yourproject.aivencloud.com:12691`

---

## Step 5 — Create the topics

Go to the **Topics** tab → **Add topic** for each:

| Topic name              | Partitions | Replication | Retention  |
|-------------------------|------------|-------------|------------|
| `ops.requests.v1`       | 1          | 1           | 604800000  |
| `ops.kafka.metrics.v1`  | 1          | 1           | 86400000   |
| `ops.incidents.v1`      | 1          | 1           | 2592000000 |
| `ops.actions.audit.v1`  | 1          | 1           | -1 (unlimited) |
| `ops.lessons.v1`        | 1          | 1           | -1 (compact) |
| `ops.notifications.v1`  | 1          | 1           | 604800000  |
| `demo.payments.events`  | 1          | 1           | 259200000  |

---

## Step 6 — Write your .env.local

Run the interactive script (it reads your `ca.pem` file automatically):

```bash
./deploy/scripts/configure-aiven.sh
```

You will be prompted for:
- Bootstrap URI (from Overview)
- Username (default: `avnadmin`)
- Password
- Path to the downloaded `ca.pem` file

---

## Step 7 — Start the app

```bash
npm run dev
```

Open **http://localhost:3000**

---

## Vercel deployment

Add these 6 vars in **Vercel Dashboard → Settings → Environment Variables**:

```
KAFKA_MODE              real
KAFKA_BOOTSTRAP         kafka-agent-mesh-kafka-yourproject.aivencloud.com:12691
KAFKA_USERNAME          avnadmin
KAFKA_PASSWORD          your-password
KAFKA_SASL_MECHANISM    scram-sha-256
KAFKA_CA_CERT_BASE64    <value from .env.local>
```

The `KAFKA_CA_CERT_BASE64` value is printed by `configure-aiven.sh` —
copy the long base64 string from your `.env.local`.

Trigger a redeploy after saving the vars.

---

## Cost estimate

| Plan | Cost/hr | Days on $300 credit |
|------|---------|---------------------|
| Startup-2 (1 node, 2 CPU, 2 GB) | ~$0.17 | ~73 days |

Delete the service when not needed: Aiven Console → service → **Delete service**.
