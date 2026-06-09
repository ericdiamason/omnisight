# OmniSight Web3 Data Engine

**Real-time blockchain compliance and intelligence — Base Mainnet USDC flows, ML wallet risk scoring, production FastAPI gateway.**

[![CI](https://github.com/ericdiamason/omnisight/actions/workflows/ci.yml/badge.svg)](https://github.com/ericdiamason/omnisight/actions)
[![Coverage](https://codecov.io/gh/ericdiamason/omnisight/branch/main/graph/badge.svg)](https://codecov.io/gh/ericdiamason/omnisight)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

---

## What this is

OmniSight is an autonomous, end-to-end on-chain intelligence system. It ingests every USDC transfer event on Base Mainnet, decodes raw hex payloads into structured analytics data, scores wallet behaviour with a deployed ML model, and exposes the results through a live, documented API.

It runs unattended on an OCI Linux node. No manual intervention. No babysitting.

**Live demo:** [ericdiamason.tech](https://ericdiamason.tech)  
**Public API:** [ericdiamason.tech/docs](https://ericdiamason.tech/docs)

---

## Architecture

```
Base Mainnet (JSON-RPC)
        │
        ▼
  Airflow 3 DAG          ← runs every 120s, idempotent, self-healing
  (web3.py decoder)      ← hex → typed fields, 6-decimal USDC normalization
        │
        ▼
  PostgreSQL 15           ← time-partitioned fact tables, sub-second queries
  (omnisight schema)      ← usdc_transfers partitioned by block range
        │
        ├──▶  Isolation Forest  ← wallet risk scoring, <5ms inference
        │     (Scikit-Learn)    ← unsupervised, no labeled dataset required
        │
        ▼
  FastAPI gateway         ← streaming metrics API, OpenAPI docs
  Nginx (TLS)             ← HTTPS, www→bare redirect, ACME auto-renewal
        │
        ▼
  Portfolio dashboard     ← live KPIs, whale alerts, wallet risk explorer
```

---

## Key technical decisions

| Decision | Rationale |
|---|---|
| PostgreSQL partitioning by block range | Sub-second range scans over 50M+ rows without index bloat |
| Isolation Forest over supervised models | No labeled fraud data required; <5ms inference at scoring time |
| Airflow 3 for orchestration | DAG-level retries, SLA monitoring, idempotent task design |
| FastAPI + Server-Sent Events | Push metrics without polling; Nginx handles TLS at edge |
| Credentials via Airflow Connections | Zero secrets in source code; rotatable without redeployment |
| Multi-stage Docker build | No build tools in production image; minimal attack surface |

---

## Repository structure

```
omnisight/
├── airflow/
│   ├── dags/
│   │   └── omnisight_pipeline.py    # production ETL DAG
│   └── tests/
│       └── test_dag.py              # pytest suite (80%+ coverage)
├── api/
│   └── main_api.py                  # FastAPI gateway
├── docker/
│   ├── Dockerfile                   # multi-stage build
│   └── docker-compose.yml           # full local stack
├── scripts/
│   └── init_db.sql                  # schema + partitions + indexes
├── .github/
│   └── workflows/
│       └── ci.yml                   # lint → test → security → build → push
├── .env.example                     # environment variable template
├── requirements.txt                 # pinned dependencies
└── README.md
```

---

## CI/CD pipeline

Every push to `main` runs:

1. **Lint** — Ruff (style + imports) + mypy (type checking)
2. **Test** — pytest with 80% coverage threshold enforced
3. **Security** — Bandit (Python AST scan) + detect-secrets (credential scan)
4. **Build** — Docker multi-stage image build with layer caching
5. **Push** — Docker Hub with SHA and `latest` tags
6. **Scan** — Trivy image vulnerability scan (blocks on CRITICAL)

Pull requests run steps 1–3 only. No image push on PRs.

---

## Local development

```bash
# 1. Clone
git clone https://github.com/ericdiamason/omnisight.git
cd omnisight

# 2. Configure secrets
cp .env.example .env
nano .env   # fill in POSTGRES_PASSWORD, OMNISIGHT_NODE_URL, etc.

# 3. Start full stack
docker compose -f docker/docker-compose.yml up -d

# 4. Run tests
pip install pytest pytest-cov pytest-mock
pytest airflow/tests/ -v --cov=airflow/dags

# 5. Initialize Airflow connections (one-time)
bash scripts/setup_airflow_connections.sh
```

---

## API reference

Full interactive docs: [ericdiamason.tech/docs](https://ericdiamason.tech/docs)

| Endpoint | Description |
|---|---|
| `GET /api/v1/metrics/whale-alerts` | Latest high-value USDC transfers |
| `GET /api/v1/predict/wallet-risk?wallet_address=0x...` | ML risk score for any wallet |
| `GET /` | Pipeline health status |

---

## Production deployment

Deployed on OCI Linux (Always Free tier):

- **OS:** Oracle Linux 8, non-root service user
- **Process management:** systemd units with auto-restart
- **TLS:** Let's Encrypt via Certbot, auto-renewing
- **Reverse proxy:** Nginx with HTTP/2, www→bare redirect
- **Secrets:** Environment variables via systemd `EnvironmentFile`, never in source

---

## About

Built by **Eric Diamason** — Principal Architect & Web3 Data Engineer.  
Specialising in production data pipelines, ML-driven risk scoring, and blockchain infrastructure.

[ericdiamason.tech](https://ericdiamason.tech) · [LinkedIn](https://linkedin.com/in/ericdiamason) · [eric@ericdiamason.tech](mailto:eric@ericdiamason.tech)
