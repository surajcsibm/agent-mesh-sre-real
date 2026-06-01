#!/usr/bin/env bash
# install-gke.sh — Deploy Agent Mesh SRE onto a GKE cluster that already
# has Strimzi installed (run setup-gke-cluster.sh first).
#
# Uses deploy/gke/ overlays for the cluster-specific manifests
# (LoadBalancer listener, single-broker sizing) and reuses the shared
# deploy/base/ files for topics, users, and workloads.
#
# Idempotent: safe to re-run.
#
# Usage:
#   ./deploy/scripts/install-gke.sh            # apply manifests
#   ./deploy/scripts/install-gke.sh --wait     # apply + wait for Ready
#   ./deploy/scripts/install-gke.sh --uninstall

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BASE_DIR="$(cd "$SCRIPT_DIR/../base" && pwd)"
GKE_DIR="$(cd "$SCRIPT_DIR/../gke" && pwd)"
NS="${NAMESPACE:-agent-mesh-sre}"
CLUSTER="${KAFKA_CLUSTER:-agent-mesh-kafka}"
WAIT_FOR_READY=false
UNINSTALL=false

for arg in "$@"; do
  case "$arg" in
    --wait)      WAIT_FOR_READY=true ;;
    --uninstall) UNINSTALL=true ;;
    -h|--help)
      grep -E '^# ' "$0" | sed 's/^# //'
      exit 0 ;;
  esac
done

command -v kubectl >/dev/null || { echo "kubectl not found on PATH"; exit 1; }

# ── Pre-flight ────────────────────────────────────────────────────────────────
preflight() {
  echo "▶ Pre-flight check"
  if ! kubectl api-resources --api-group=kafka.strimzi.io 2>/dev/null | grep -q Kafka; then
    echo ""
    echo "✗ Strimzi CRDs not found in this cluster."
    echo "  Run setup-gke-cluster.sh first:"
    echo "    ./deploy/scripts/setup-gke-cluster.sh"
    exit 1
  fi
  local v
  v=$(kubectl get crd kafkas.kafka.strimzi.io \
    -o jsonpath='{.metadata.labels.app\.kubernetes\.io/version}' 2>/dev/null || echo "unknown")
  echo "  Strimzi CRDs present (version: $v)"
  echo "  Context: $(kubectl config current-context)"
}

# ── Uninstall ─────────────────────────────────────────────────────────────────
uninstall() {
  echo "▶ Uninstalling Agent Mesh SRE from namespace $NS"
  for f in 05-demo-workloads 04-kafka-users 03-kafka-topics; do
    kubectl delete -f "$BASE_DIR/${f}.yaml" --ignore-not-found=true 2>/dev/null || true
  done
  kubectl delete -f "$GKE_DIR/02-kafka-cluster.yaml" --ignore-not-found=true 2>/dev/null || true
  kubectl delete -f "$GKE_DIR/01-kafka-nodepools.yaml" --ignore-not-found=true 2>/dev/null || true
  kubectl delete -f "$BASE_DIR/00-namespace.yaml" --ignore-not-found=true 2>/dev/null || true
  echo "✓ Uninstall complete (PVCs deleted — deleteClaim=true in node pool specs)."
}

# ── Apply helpers ─────────────────────────────────────────────────────────────
apply_step() {
  local file="$1" desc="$2"
  printf "▶ %-50s " "$desc"
  kubectl apply -f "$file" >/tmp/apply.out 2>&1 || {
    echo "FAIL"
    cat /tmp/apply.out >&2
    exit 1
  }
  awk '/created|configured|unchanged/{n++} END{printf "%d resource(s)\n", n}' /tmp/apply.out
}

# ── Wait for Ready ────────────────────────────────────────────────────────────
wait_for_ready() {
  echo ""
  echo "▶ Waiting for Kafka cluster $CLUSTER to be Ready (up to 15 min)…"
  kubectl wait kafka/"$CLUSTER" -n "$NS" \
    --for=condition=Ready --timeout=900s

  echo "✓ Kafka cluster is Ready"

  echo "▶ Waiting for KafkaTopics…"
  kubectl wait kafkatopic --all -n "$NS" --for=condition=Ready --timeout=120s || true

  echo "▶ Waiting for KafkaUsers…"
  kubectl wait kafkauser --all -n "$NS" --for=condition=Ready --timeout=120s || true

  echo ""
  local ext
  ext=$(kubectl get kafka "$CLUSTER" -n "$NS" \
    -o jsonpath='{.status.listeners[?(@.name=="external")].bootstrapServers}' 2>/dev/null || true)
  echo "External bootstrap : ${ext:-<pending — run get-credentials-gke.sh once IP is assigned>}"
}

# ── Main ──────────────────────────────────────────────────────────────────────
main() {
  if $UNINSTALL; then uninstall; exit 0; fi

  preflight

  apply_step "$BASE_DIR/00-namespace.yaml"        "Namespace ($NS)"
  apply_step "$GKE_DIR/01-kafka-nodepools.yaml"   "KafkaNodePools (controller 1 + broker 1)"
  apply_step "$GKE_DIR/02-kafka-cluster.yaml"     "Kafka cluster (KRaft, LoadBalancer external)"
  apply_step "$BASE_DIR/03-kafka-topics.yaml"     "KafkaTopics (ops.* + demo.payments.events)"
  apply_step "$BASE_DIR/04-kafka-users.yaml"      "KafkaUsers (agents + controller)"
  apply_step "$BASE_DIR/05-demo-workloads.yaml"   "Demo producer/consumer workloads"

  if $WAIT_FOR_READY; then
    wait_for_ready
  fi

  echo ""
  echo "╭──────────────────────────────────────────────────────────────────────╮"
  echo "│  ✓ Agent Mesh SRE manifests applied                                   │"
  echo "│                                                                        │"
  echo "│  Monitor progress:                                                     │"
  echo "│    kubectl get kafka,kafkatopic,kafkauser -n agent-mesh-sre            │"
  echo "│    kubectl get svc -n agent-mesh-sre | grep external                  │"
  echo "│                                                                        │"
  echo "│  Once Ready + LoadBalancer IP is assigned:                             │"
  echo "│    ./deploy/scripts/get-credentials-gke.sh > .env.local               │"
  echo "│    npm run dev                                                         │"
  echo "│                                                                        │"
  echo "│  For Vercel: copy the values from .env.local into                      │"
  echo "│    Vercel Dashboard → Settings → Environment Variables                 │"
  echo "╰──────────────────────────────────────────────────────────────────────╯"
}

main "$@"
