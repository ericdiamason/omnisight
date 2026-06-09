#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
# OmniSight — Airflow connections & variables bootstrap
# ═══════════════════════════════════════════════════════════════════
# Run once after Airflow is initialized.
# Reads secrets from .env — never hardcodes values.
# Usage: bash scripts/setup_airflow_connections.sh
# ═══════════════════════════════════════════════════════════════════

set -euo pipefail

# Load .env
if [ -f .env ]; then
  export "$(grep -v '^#' .env | xargs)"
else
  echo "ERROR: .env file not found. Copy .env.example → .env and fill in values."
  exit 1
fi

echo "Setting up Airflow connections and variables..."

# ── PostgreSQL connection ─────────────────────────────────────────
airflow connections add omnisight_postgres \
  --conn-type postgres \
  --conn-host "${POSTGRES_HOST:-localhost}" \
  --conn-login "${POSTGRES_USER}" \
  --conn-password "${POSTGRES_PASSWORD}" \
  --conn-schema "${POSTGRES_DB}" \
  --conn-port "${POSTGRES_PORT:-5432}" \
  --conn-description "OmniSight PostgreSQL — partitioned fact tables" \
  2>/dev/null || \
airflow connections set omnisight_postgres \
  --conn-type postgres \
  --conn-host "${POSTGRES_HOST:-localhost}" \
  --conn-login "${POSTGRES_USER}" \
  --conn-password "${POSTGRES_PASSWORD}" \
  --conn-schema "${POSTGRES_DB}" \
  --conn-port "${POSTGRES_PORT:-5432}"

echo "  ✓ omnisight_postgres connection set"

# ── Slack webhook connection ──────────────────────────────────────
if [ -n "${SLACK_WEBHOOK_URL:-}" ]; then
  airflow connections add slack_webhook \
    --conn-type http \
    --conn-host "${SLACK_WEBHOOK_URL}" \
    --conn-description "OmniSight Slack failure alerts" \
    2>/dev/null || \
  airflow connections set slack_webhook \
    --conn-type http \
    --conn-host "${SLACK_WEBHOOK_URL}"
  echo "  ✓ slack_webhook connection set"
else
  echo "  ⚠ SLACK_WEBHOOK_URL not set — skipping Slack connection"
fi

# ── Airflow Variables ─────────────────────────────────────────────
airflow variables set omnisight_node_url "${OMNISIGHT_NODE_URL}"
echo "  ✓ omnisight_node_url variable set"

airflow variables set omnisight_batch_size "500"
echo "  ✓ omnisight_batch_size = 500"

# ── Airflow Pool (limit concurrent node RPC calls) ────────────────
airflow pools set blockchain_pool 3 "OmniSight blockchain RPC calls" \
  2>/dev/null || true
echo "  ✓ blockchain_pool created (slots: 3)"

echo ""
echo "Setup complete. Start the pipeline:"
echo "  airflow dags unpause omnisight_blockchain_pipeline"
echo "  airflow dags trigger omnisight_blockchain_pipeline"
