#!/usr/bin/env bash
# setup-gke-cluster.sh — Create a GKE Autopilot cluster and install the
# Strimzi Kafka operator. Run this once before install-gke.sh.
#
# Prerequisites:
#   - gcloud CLI installed and authenticated (gcloud auth login)
#   - A GCP project with billing enabled
#
# Usage:
#   ./deploy/scripts/setup-gke-cluster.sh
#   ./deploy/scripts/setup-gke-cluster.sh --project my-gcp-project
#   ./deploy/scripts/setup-gke-cluster.sh --project myproject --region europe-west1

set -euo pipefail

# ── Defaults (override via env or --flags) ────────────────────────────────────
PROJECT="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null || true)}"
REGION="${GCP_REGION:-us-central1}"
GKE_CLUSTER="${GKE_CLUSTER:-agent-mesh-gke}"
STRIMZI_VERSION="0.51.0"
NS="agent-mesh-sre"

# ── Argument parsing ──────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --project) PROJECT="$2"; shift 2 ;;
    --region)  REGION="$2";  shift 2 ;;
    --cluster) GKE_CLUSTER="$2"; shift 2 ;;
    -h|--help)
      grep -E '^# ' "$0" | sed 's/^# //'
      exit 0 ;;
    *) echo "Unknown flag: $1"; exit 1 ;;
  esac
done

# ── Validate ──────────────────────────────────────────────────────────────────
if [[ -z "$PROJECT" ]]; then
  echo "Error: GCP project is not set."
  echo ""
  echo "  Fix with one of:"
  echo "    gcloud config set project YOUR_PROJECT_ID"
  echo "    GCP_PROJECT=myproject ./deploy/scripts/setup-gke-cluster.sh"
  exit 1
fi

command -v gcloud >/dev/null || { echo "gcloud not found. Install from https://cloud.google.com/sdk"; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo "kubectl not found. Run: gcloud components install kubectl"; exit 1; }

echo "═══════════════════════════════════════════════════════════"
echo " Agent Mesh SRE — GKE + Strimzi bootstrap"
echo " Project : $PROJECT"
echo " Region  : $REGION"
echo " Cluster : $GKE_CLUSTER"
echo " Strimzi : $STRIMZI_VERSION"
echo "═══════════════════════════════════════════════════════════"
echo ""

# ── Step 1: Enable Container API ──────────────────────────────────────────────
echo "▶ Enabling container.googleapis.com"
gcloud services enable container.googleapis.com \
  --project="$PROJECT" --quiet

# ── Step 2: Create GKE Autopilot cluster ─────────────────────────────────────
echo "▶ Creating GKE Autopilot cluster: $GKE_CLUSTER"
echo "  (this takes ~3 min)"
gcloud container clusters create-auto "$GKE_CLUSTER" \
  --project="$PROJECT" \
  --region="$REGION" \
  --release-channel=stable \
  --quiet

# ── Step 3: Fetch credentials ─────────────────────────────────────────────────
echo "▶ Fetching cluster credentials"
gcloud container clusters get-credentials "$GKE_CLUSTER" \
  --project="$PROJECT" \
  --region="$REGION"

echo "  Active context: $(kubectl config current-context)"

# ── Step 4: Create namespace ──────────────────────────────────────────────────
echo "▶ Creating namespace: $NS"
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -

# ── Step 5: Install Strimzi ───────────────────────────────────────────────────
echo "▶ Installing Strimzi $STRIMZI_VERSION"
STRIMZI_URL="https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VERSION}/strimzi-cluster-operator-${STRIMZI_VERSION}.yaml"

# Strimzi bundles default to "myproject" namespace — patch it to ours.
curl -sL "$STRIMZI_URL" \
  | sed "s/namespace: myproject/namespace: $NS/g" \
  | kubectl apply -f - -n "$NS"

echo "▶ Waiting for Strimzi cluster operator (up to 5 min)"
kubectl rollout status deployment/strimzi-cluster-operator \
  -n "$NS" --timeout=300s

echo ""
echo "╭──────────────────────────────────────────────────────────────────────╮"
echo "│  ✓ GKE cluster ready + Strimzi $STRIMZI_VERSION installed              │"
echo "│                                                                        │"
echo "│  Next — deploy the Kafka cluster:                                      │"
echo "│    ./deploy/scripts/install-gke.sh --wait                              │"
echo "│                                                                        │"
echo "│  Estimated time: 10-15 min for Kafka to become Ready.                  │"
echo "│  LoadBalancer IP assignment takes an additional 2-3 min.               │"
echo "╰──────────────────────────────────────────────────────────────────────╯"
