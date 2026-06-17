# OmniSight Web3 Data Engine

**Real-time blockchain intelligence — Base Mainnet USDC flows, ML wallet risk scoring, production FastAPI gateway.**

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://github.com/ericdiamason/omnisight/blob/main/LICENSE)
[![Python](https://img.shields.io/badge/Python-3.9-blue)](https://python.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.128-green)](https://fastapi.tiangolo.com)
[![Airflow](https://img.shields.io/badge/Airflow-3-red)](https://airflow.apache.org)

**Live demo:** [ericdiamason.tech](https://ericdiamason.tech)  
**Public API:** [ericdiamason.tech/docs](https://ericdiamason.tech/docs)  
**Whale alerts:** [ericdiamason.tech/api/v1/metrics/whale-alerts](https://ericdiamason.tech/api/v1/metrics/whale-alerts)

---

## What this is

OmniSight is an autonomous, end-to-end on-chain intelligence system built from scratch. It ingests every USDC Transfer event on Base Mainnet, decodes raw EVM hex payloads into structured analytics data, scores wallet behaviour with a trained ML anomaly model, and exposes the results through a live, documented REST API and public dashboard.

It runs unattended on Oracle Cloud Infrastructure. No manual intervention. No babysitting.

**Current production stats:**
- 214,000+ clean USDC transfer records indexed
- 6,079 wallet profiles trained on by the ML model
- $2.9M+ in whale transfers tracked in real time
- Zero duplicate records — enforced by unique constraint on partitioned table
- 15-second average ingestion latency from Base Mainnet to dashboard

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
  FastAPI v2.2 gateway         ← authenticated + public rate-limited endpoints
  Nginx + Let's Encrypt        ← TLS termination, HTTPS, ACME auto-renewal
        │
        ▼
  Live dashboard               ← real-time KPIs, whale alerts feed, wallet risk explorer
  (ericdiamason.tech)
```

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
│   └── main_api.py                  # FastAPI gateway v2.2 — whale alerts + ML scoring
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
├── .env.example                     # environment variable template — copy to /etc/omnisight.env
├── .gitignore                       # excludes .pkl, .log, .env, __pycache__
└── README.md
```

---

## API reference

Full interactive docs at [ericdiamason.tech/docs](https://ericdiamason.tech/docs)

| Method | Endpoint | Auth | Description |
|---|---|---|---|
| `GET` | `/` | None | System health — API status, version, model loaded state |
| `GET` | `/api/v1/metrics/whale-alerts` | None | Latest USDC transfers ≥ $50,000 USD (up to 100 records) |
| `GET` | `/api/v1/public/wallet-risk` | None (rate limited) | ML risk score — 10 req/min per IP |
| `GET` | `/api/v1/predict/wallet-risk` | `X-API-Key` header | ML risk score — authenticated, unlimited |

### Example: whale alerts

```bash
curl https://ericdiamason.tech/api/v1/metrics/whale-alerts?limit=5
```

### Example: wallet risk (public endpoint)

```bash
curl "https://ericdiamason.tech/api/v1/public/wallet-risk?wallet_address=0xa9d51f7cf1548bc6636bc405ef480fe502cc71a8"
```

Response:
```json
{
  "wallet_address": "0xa9d51f7cf1548bc6636bc405ef480fe502cc71a8",
  "transaction_count": 43,
  "total_volume_usd": 2321748.06,
  "average_transaction_size": 53994.14,
  "ai_classification": "SUSPICIOUS_HIGH_VELOCITY_ANOMALY",
  "risk_score": -0.7993,
  "threat_alert": true,
  "evaluated_at": "2026-06-17T18:25:59Z"
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

**Model governance:** Every training run writes `threat_model_metadata.json` recording the training timestamp, wallet count, feature names, and hyperparameters.

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
- **TLS:** Let's Encrypt via Certbot, auto-renewing
- **Reverse proxy:** Nginx with HTTP/2, www → bare redirect
- **Secrets:** `/etc/omnisight.env` with `chmod 600` — loaded via systemd `EnvironmentFile`
- **SELinux:** venv binaries labelled `bin_t` to allow systemd execution

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
- CORS locked to `https://ericdiamason.tech` — no wildcard origins
- Wallet address validated against EIP-55 format before any database query
- `threat_model.pkl` excluded from git via `.gitignore` — use model registry for artifacts
- Logs excluded from git — managed by logrotate

---

## About

Built by **Eric Dia Mason** — Senior Data Architect and Web3 Data Engineer with 20+ years of self-taught experience. OmniSight demonstrates end-to-end ownership of a production blockchain intelligence system: from EVM event decoding to ML inference to live public API.

[ericdiamason.tech](https://ericdiamason.tech) · [LinkedIn](https://www.linkedin.com/in/eric-mason-dba/) · admin@ericdiamason.tech
