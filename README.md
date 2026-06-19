# OmniSight Web3 Data Engine

**Real-time blockchain intelligence — Base Mainnet USDC flows, ML wallet risk scoring, production FastAPI gateway.**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/ericdiamason/omnisight/blob/main/LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9-blue)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.128-green)](https://fastapi.tiangolo.com)
[![Airflow](https://img.shields.io/badge/Airflow-3-red)](https://airflow.apache.org)

**Live demo:** [omnisight.ericdiamason.tech](https://omnisight.ericdiamason.tech)
**Public API:** [omnisight.ericdiamason.tech/docs](https://omnisight.ericdiamason.tech/docs)
**Whale alerts:** [omnisight.ericdiamason.tech/api/v1/metrics/whale-alerts](https://omnisight.ericdiamason.tech/api/v1/metrics/whale-alerts)
**Live stats:** [omnisight.ericdiamason.tech/api/v1/stats](https://omnisight.ericdiamason.tech/api/v1/stats)

Part of the [Eric Dia Mason](https://ericdiamason.tech) intelligence systems portfolio — see also [FiscalTrace](https://fiscaltrace.ericdiamason.tech).

---

## What this is

OmniSight is an autonomous, end-to-end on-chain intelligence system built from scratch. It ingests every USDC Transfer event on Base Mainnet, decodes raw EVM hex payloads into structured analytics data, scores wallet behaviour with a trained ML anomaly model, and exposes the results through a live, documented REST API and public dashboard.

It runs unattended on Oracle Cloud Infrastructure. No manual intervention. No babysitting.

**Current production stats** (live at `/api/v1/stats` — numbers below were accurate at last update, but check the endpoint for the current count):
- 396,000+ clean USDC transfer records indexed
- 9,400+ wallets eligible for ML scoring (2+ transactions)
- ML model retrained on 9,418 wallet profiles
- Zero duplicate records — enforced by unique constraint on partitioned table
- Sub-second ingestion latency from Base Mainnet to dashboard

This project intentionally exposes a `/api/v1/stats` endpoint so the frontend — and this README — never has to rely on numbers that go stale the moment the pipeline ingests more data.

---

## Architecture

```
Base Mainnet (JSON-RPC via Alchemy)
        │
        ▼
  Airflow 3 DAG                ← runs every 120s, idempotent, self-healing
  (web3.py EVM decoder)        ← 32-byte topics → wallet addresses, 6-decimal USDC
        │
        ▼
  PostgreSQL 15                ← block-range partitioned fact tables
  (omnisight schema)           ← ON CONFLICT (block_number, transaction_hash) DO NOTHING
        │
        ├──▶  RobustScaler + Isolation Forest  ← wallet anomaly scoring, <5ms inference
        │     (Scikit-Learn Pipeline)           ← unsupervised, no labeled dataset required
        │
        ▼
  FastAPI gateway               ← authenticated + public rate-limited endpoints
  Nginx + Let's Encrypt          ← TLS termination, HTTPS, ACME auto-renewal
        │
        ▼
  Live dashboard                ← real-time KPIs, whale alerts feed, wallet risk explorer
  (omnisight.ericdiamason.tech)
```

OmniSight runs on its own subdomain with its own Nginx server block, systemd service, and database user — fully isolated from other projects on the same infrastructure (see [FiscalTrace](https://github.com/ericdiamason/fiscaltrace) for the sibling project).

---

## Key technical decisions

| Decision | Rationale |
|---|---|
| PostgreSQL block-range partitioning | Sub-second range scans; `usdc_transfers_era_47m`, `era_48m` grow automatically |
| RobustScaler before Isolation Forest | Blockchain data has extreme outliers (whale wallets). RobustScaler uses median/IQR — resistant to extremes without discarding signal |
| Isolation Forest over supervised models | No labeled fraud dataset required; unsupervised anomaly detection on real wallet behaviour |
| ON CONFLICT (block_number, transaction_hash) | Idempotent inserts — pipeline safe to re-run against any block range |
| asyncpg connection pool (min=5, max=20) | Persistent async connections; handles concurrent API requests without per-request TCP overhead |
| Secrets via systemd EnvironmentFile | Zero credentials in source code or git history; instant rotation without code changes |
| Public rate-limited endpoint | 10 requests/IP/minute via slowapi — open access without API key, abuse-protected |
| `/api/v1/stats` live metrics endpoint | Frontend and documentation never display stale counts — single source of truth |
| Dedicated subdomain | `omnisight.ericdiamason.tech` runs fully isolated from sibling projects — own Nginx block, systemd service, DB user |

---

## Repository structure

```
omnisight/
├── airflow/
│   ├── dags/
│   │   └── omnisight_pipeline.py    # production ETL DAG — single source of truth
│   └── tests/
│       └── test_dag.py              # pytest suite — DAG structure, EVM decoders, ETL logic
├── api/
│   └── main_api.py                  # FastAPI gateway — whale alerts, ML scoring, live stats
├── ml/
│   └── train_model.py               # RobustScaler + IsolationForest training pipeline
├── scripts/
│   ├── init_db.sql                  # PostgreSQL schema, partitions, indexes
│   ├── setup_env.sh                 # /etc/omnisight.env creation wizard
│   ├── logrotate_omnisight          # log rotation config for open_api.log
│   └── omnisight-api.service        # systemd service unit for the FastAPI server
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── docs/
│   └── index.html                   # standalone dashboard — live terminal UI
├── .env.example                     # environment variable template — copy to /etc/omnisight.env
├── .gitignore                       # excludes .pkl, .log, .env, __pycache__
└── README.md
```

---

## API reference

Full interactive docs at [omnisight.ericdiamason.tech/docs](https://omnisight.ericdiamason.tech/docs)

| Method | Endpoint | Auth | Description |
|---|---|---|---|
| `GET` | `/` | None | System health — API status, version, model loaded state |
| `GET` | `/api/v1/stats` | None | Live operational metrics — record count, wallet count, model version |
| `GET` | `/api/v1/metrics/whale-alerts` | None | Latest USDC transfers ≥ $50,000 USD (up to 100 records) |
| `GET` | `/api/v1/public/wallet-risk` | None (rate limited) | ML risk score — 10 req/min per IP |
| `GET` | `/api/v1/predict/wallet-risk` | `X-API-Key` header | ML risk score — authenticated, unlimited |

### Example: live stats

```bash
curl https://omnisight.ericdiamason.tech/api/v1/stats
```

```json
{
  "total_records": 396140,
  "eligible_wallets": 9463,
  "model_version": "v20260619_0019",
  "model_trained_at": "2026-06-19T00:19:11.620960+00:00",
  "model_wallets_trained_on": 9418,
  "timestamp": "2026-06-19T00:45:43.651944"
}
```

### Example: whale alerts

```bash
curl https://omnisight.ericdiamason.tech/api/v1/metrics/whale-alerts?limit=5
```

### Example: wallet risk (public endpoint)

```bash
curl "https://omnisight.ericdiamason.tech/api/v1/public/wallet-risk?wallet_address=0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59"
```

Response:
```json
{
  "wallet_address": "0xb2cc224c1c9fee385f8ad6a55b4d94e92359dc59",
  "transaction_count": 9216,
  "total_volume_usd": 901192479.19,
  "average_transaction_size": 97785.64,
  "ai_classification": "SUSPICIOUS_HIGH_VELOCITY_ANOMALY",
  "risk_score": -0.8923,
  "threat_alert": true,
  "evaluated_at": "2026-06-19T00:51:48Z"
}
```

---

## ML model

The wallet risk scorer uses a Scikit-Learn `Pipeline` wrapping `RobustScaler` and `IsolationForest`:

**Features:**
- `tx_count` — number of outgoing USDC transfers
- `total_volume_usd` — total USD sent
- `avg_tx_size` — average transfer size

**Why RobustScaler:** Blockchain financial data contains extreme outliers — whale wallets with billions in volume coexist with retail wallets moving a few dollars. RobustScaler uses the median and interquartile range instead of mean/variance, making it resistant to those extremes without discarding the information they carry.

**Model governance:** Every training run writes `threat_model_metadata.json` recording the training timestamp, wallet count, feature names, and hyperparameters. This metadata is served live via `/api/v1/stats` so anyone can verify exactly when and on what data the running model was trained.

**Retraining cadence:** Retrain whenever the eligible-wallet count has grown meaningfully since the last training run — check `/api/v1/stats` and compare `eligible_wallets` against `model_wallets_trained_on`. A gap of more than ~20% is a good signal to retrain.

```bash
# Retrain the model
cd /home/omnisight
source venv/bin/activate
export $(sudo grep -v "^#" /etc/omnisight.env | grep -v "^$" | xargs)
python train_model.py
sudo systemctl restart omnisight-api.service
```

---

## Production deployment

Deployed on OCI Linux (Always Free tier):

- **OS:** Oracle Linux 8, `opc` service user
- **Process management:** systemd — `omnisight-api.service` auto-restarts on failure
- **TLS:** Let's Encrypt via Certbot, auto-renewing, covers `omnisight.ericdiamason.tech` and `fiscaltrace.ericdiamason.tech` under one certificate
- **Reverse proxy:** Nginx, dedicated server block for the subdomain — no shared routing with sibling projects
- **Secrets:** `/etc/omnisight.env` with `chmod 600` — loaded via systemd `EnvironmentFile`
- **SELinux:** venv binaries and web root labelled `httpd_sys_content_t` / `bin_t` to allow Nginx and systemd execution

### Environment variables

```bash
# Copy template and fill in values
sudo cp .env.example /etc/omnisight.env
sudo nano /etc/omnisight.env
sudo chmod 600 /etc/omnisight.env
```

Required variables:

```
OMNISIGHT_NODE_URL      # Alchemy Base Mainnet endpoint (includes API key)
OMNISIGHT_DB_HOST       # PostgreSQL host (default: 127.0.0.1)
OMNISIGHT_DB_NAME       # PostgreSQL database (default: postgres)
OMNISIGHT_DB_USER       # PostgreSQL user (default: omnisight_user)
OMNISIGHT_DB_PASS       # PostgreSQL password
OMNISIGHT_API_KEY       # API key for authenticated endpoint
OMNISIGHT_MODEL_PATH    # Path to threat_model.pkl
```

### Database setup

```bash
psql -U postgres -f scripts/init_db.sql
```

### Start services

```bash
sudo systemctl enable omnisight-api.service
sudo systemctl start omnisight-api.service
sudo systemctl restart airflow3.service
```

### Before deploying any change to `main_api.py`

The server and this repo have drifted out of sync before — once, dropping the public wallet-risk endpoint in production for several hours. Always verify before and after deploying:

```bash
curl -s https://omnisight.ericdiamason.tech/openapi.json | python3 -c "import json,sys; d=json.load(sys.stdin); print('\n'.join(d['paths'].keys()))"
```

Compare the route list against what's expected before assuming a copy or deploy succeeded.

---

## Running tests

```bash
cd omnisight
pip install pytest pytest-cov pytest-mock
pytest airflow/tests/ -v --cov=airflow/dags --cov-report=term-missing
```

Tests cover: DAG structure validation, EVM address decoding, USDC amount decoding, normal ETL run, idle at chain tip, block failure recovery, and DB connection failure — all with mocked external dependencies.

---

## Security

- Zero credentials in source code or git history — all secrets via environment variables
- API key authentication on the `/api/v1/predict/wallet-risk` endpoint
- Public endpoint rate-limited to 10 requests/IP/minute via slowapi
- CORS locked to `https://ericdiamason.tech`, `https://www.ericdiamason.tech`, and `https://omnisight.ericdiamason.tech` — no wildcard origins
- Wallet address validated against EIP-55 format before any database query
- `threat_model.pkl` excluded from git via `.gitignore` — use model registry for artifacts
- Logs excluded from git — managed by logrotate

---

## About

Built by **Eric Dia Mason** — Senior Data Architect and Web3 Data Engineer with 20+ years of self-taught experience. OmniSight demonstrates end-to-end ownership of a production blockchain intelligence system: from EVM event decoding to ML inference to live public API.

[ericdiamason.tech](https://ericdiamason.tech) · [omnisight.ericdiamason.tech](https://omnisight.ericdiamason.tech) · [LinkedIn](https://www.linkedin.com/in/eric-mason-dba/) · admin@ericdiamason.tech
