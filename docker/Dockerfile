# ═══════════════════════════════════════════════════════════════════
# OmniSight Airflow Worker Image
# ═══════════════════════════════════════════════════════════════════
# Multi-stage build:
#   Stage 1 (builder) — install Python deps into /venv
#   Stage 2 (runtime) — copy /venv only, no build tools in prod image
#
# Build:  docker build -t omnisight-airflow:latest .
# Run:    see docker-compose.yml
# ═══════════════════════════════════════════════════════════════════

# ── Stage 1: dependency builder ───────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# System build deps (compile psycopg2 C extension)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Create isolated venv
RUN python -m venv /venv
ENV PATH="/venv/bin:$PATH"

# Install Python packages — pinned for reproducibility
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt


# ── Stage 2: production runtime ───────────────────────────────────
FROM python:3.11-slim AS runtime

LABEL maintainer="Eric Diamason <eric@ericdiamason.tech>"
LABEL description="OmniSight Airflow worker — Base Mainnet USDC ingestion pipeline"
LABEL version="1.0.0"

# Runtime system deps only (libpq for psycopg2)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq5 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Non-root user — never run as root in production
RUN useradd --create-home --shell /bin/bash omnisight
USER omnisight
WORKDIR /home/omnisight

# Copy venv from builder — no pip/gcc in the final image
COPY --from=builder /venv /venv
ENV PATH="/venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Copy DAG files
COPY --chown=omnisight:omnisight airflow/dags ./dags

# Airflow env — override at runtime via docker-compose or K8s secrets
ENV AIRFLOW_HOME=/home/omnisight/airflow
ENV AIRFLOW__CORE__DAGS_FOLDER=/home/omnisight/dags
ENV AIRFLOW__CORE__EXECUTOR=LocalExecutor
ENV AIRFLOW__CORE__LOAD_EXAMPLES=False
ENV AIRFLOW__LOGGING__LOGGING_LEVEL=INFO

# Healthcheck — verify Airflow webserver responds
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

EXPOSE 8080

CMD ["airflow", "standalone"]
