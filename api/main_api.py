import json
import os
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

import asyncpg
import joblib
import numpy as np
from fastapi import Depends, FastAPI, HTTPException, Request, Security, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DB_HOST = os.getenv("OMNISIGHT_DB_HOST", "127.0.0.1")
DB_NAME = os.getenv("OMNISIGHT_DB_NAME", "postgres")
DB_USER = os.getenv("OMNISIGHT_DB_USER", "omnisight_user")
MODEL_PATH = Path(os.getenv("OMNISIGHT_MODEL_PATH", "/home/omnisight/threat_model.pkl"))
METADATA_PATH = Path("/home/omnisight/threat_model_metadata.json")
WHALE_THRESHOLD_USD = 50_000.0
ETH_ADDRESS_RE = re.compile(r"^0x[0-9a-fA-F]{40}$")
limiter = Limiter(key_func=get_remote_address)

def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable {name!r} is not set.")
    return value

app = FastAPI(
    title="OmniSight Web3 Data Engine API",
    description=(
        "Real-time Base Mainnet USDC intelligence: whale transfer alerts, "
        "AI-powered wallet anomaly scoring, and pipeline health monitoring.\n\n"
        "**Authentication**: The `/api/v1/predict/wallet-risk` endpoint requires "
        "an `X-API-Key` header."
    ),
    version="2.1.0",
    contact={"name": "Eric Dia Mason", "url": "https://ericdiamason.tech"},
    license_info={"name": "MIT", "url": "https://opensource.org/licenses/MIT"},
)

ALLOWED_ORIGINS = ["https://ericdiamason.tech", "https://www.ericdiamason.tech"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET"],
    allow_headers=["X-API-Key", "Content-Type"],
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

API_KEY_HEADER = APIKeyHeader(name="X-API-Key", auto_error=False)

async def require_api_key(api_key: Optional[str] = Security(API_KEY_HEADER)):
    if not api_key:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
                            detail="API key required. Include X-API-Key header.")
    valid_key = os.getenv("OMNISIGHT_API_KEY")
    if not valid_key or api_key != valid_key:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,
                            detail="Invalid API key.")
    return api_key

@app.on_event("startup")
async def startup_event():
    try:
        db_password = _require_env("OMNISIGHT_DB_PASS")
        app.state.db_pool = await asyncpg.create_pool(
            host=DB_HOST, database=DB_NAME, user=DB_USER, password=db_password,
            min_size=5, max_size=20,
        )
        log.info("[STARTUP] PostgreSQL connection pool established.")
    except Exception as exc:
        log.critical("[STARTUP] Database pool failed: %s", exc)
        raise

    try:
        if MODEL_PATH.exists():
            app.state.ai_model = joblib.load(MODEL_PATH)
            log.info("[STARTUP] Threat model loaded from: %s", MODEL_PATH)
        else:
            app.state.ai_model = None
            log.warning("[STARTUP] Model file not found at %s. Run ml/train_model.py.", MODEL_PATH)
    except Exception as exc:
        log.error("[STARTUP] Failed to load threat model: %s", exc)
        app.state.ai_model = None

@app.on_event("shutdown")
async def shutdown_event():
    await app.state.db_pool.close()
    log.info("[SHUTDOWN] Database connection pool closed.")

class HealthResponse(BaseModel):
    status: str
    engine: str
    version: str
    model_loaded: bool
    timestamp: datetime

class WhaleAlertResponse(BaseModel):
    block_number: int
    transaction_hash: str
    sender_address: str
    receiver_address: str
    amount_usd: float
    ingested_at: datetime

class AIWalletRiskResponse(BaseModel):
    wallet_address: str
    transaction_count: int
    total_volume_usd: float
    average_transaction_size: float
    ai_classification: str
    risk_score: float
    threat_alert: bool
    evaluated_at: datetime

class StatsResponse(BaseModel):
    total_records: int
    eligible_wallets: int
    model_version: str
    model_trained_at: str
    model_wallets_trained_on: int
    timestamp: datetime


@app.get("/api/v1/stats", response_model=StatsResponse, tags=["System Check"])
async def stats():
    """
    Live operational statistics. No authentication required.

    Returns current record counts and ML model metadata so frontend
    displays never go stale relative to the actual pipeline state.
    """
    async with app.state.db_pool.acquire() as conn:
        total_records = await conn.fetchval(
            "SELECT COUNT(*) FROM omnisight.usdc_transfers;"
        )
        eligible_wallets = await conn.fetchval("""
            SELECT COUNT(*) FROM (
                SELECT sender_address FROM omnisight.usdc_transfers
                GROUP BY sender_address HAVING COUNT(*) >= 2
            ) sub;
        """)

    model_version = "unknown"
    model_trained_at = "unknown"
    model_wallets = 0
    try:
        if METADATA_PATH.exists():
            with open(METADATA_PATH) as f:
                meta = json.load(f)
                model_version = meta.get("model_version", "unknown")
                model_trained_at = meta.get("trained_at", "unknown")
                model_wallets = meta.get("row_count", 0)
    except Exception as exc:
        log.warning("[STATS] Could not read model metadata: %s", exc)

    return StatsResponse(
        total_records=total_records,
        eligible_wallets=eligible_wallets,
        model_version=model_version,
        model_trained_at=model_trained_at,
        model_wallets_trained_on=model_wallets,
        timestamp=datetime.utcnow(),
    )


@app.get("/", response_model=HealthResponse, tags=["System Check"])
async def root_health():
    return HealthResponse(
        status="ONLINE", engine="OmniSight AI Core", version="2.1.0",
        model_loaded=app.state.ai_model is not None,
        timestamp=datetime.utcnow(),
    )

@app.get("/api/v1/metrics/whale-alerts", response_model=List[WhaleAlertResponse],
         tags=["Web3 Analytics"], summary="Latest high-value USDC transfers")
async def get_whale_alerts(limit: int = 25):
    limit = min(limit, 100)
    query = """
        SELECT block_number, transaction_hash, sender_address,
               receiver_address, adjusted_amount, ingested_at
        FROM omnisight.usdc_transfers
        WHERE adjusted_amount >= $1
        ORDER BY block_number DESC, transfer_id DESC
        LIMIT $2;
    """
    async with app.state.db_pool.acquire() as conn:
        try:
            rows = await conn.fetch(query, WHALE_THRESHOLD_USD, limit)
        except Exception as exc:
            log.error("[WHALE-ALERTS] DB query failed: %s", exc)
            raise HTTPException(status_code=500, detail="Database query failed.")
    return [
        WhaleAlertResponse(
            block_number=row["block_number"], transaction_hash=row["transaction_hash"],
            sender_address=row["sender_address"], receiver_address=row["receiver_address"],
            amount_usd=float(row["adjusted_amount"]), ingested_at=row["ingested_at"],
        ) for row in rows
    ]

async def _score_wallet(wallet_address: str) -> AIWalletRiskResponse:
    """Shared scoring logic used by both authenticated and public endpoints."""
    if app.state.ai_model is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                            detail="AI scoring engine offline. Run ml/train_model.py.")
    if not ETH_ADDRESS_RE.match(wallet_address):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                            detail=f"Invalid wallet address: {wallet_address!r}.")
    wallet_lower = wallet_address.lower()
    query = """
        SELECT COUNT(transfer_id) AS tx_count,
               COALESCE(SUM(adjusted_amount), 0) AS total_volume,
               COALESCE(AVG(adjusted_amount), 0) AS avg_size
        FROM omnisight.usdc_transfers
        WHERE sender_address = $1;
    """
    async with app.state.db_pool.acquire() as conn:
        row = await conn.fetchrow(query, wallet_lower)
    if not row or row["tx_count"] == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="Wallet has no recorded outgoing USDC transfers in the index.")
    features = np.array([[row["tx_count"], float(row["total_volume"]), float(row["avg_size"])]])
    prediction = app.state.ai_model.predict(features)[0]
    raw_score = float(app.state.ai_model.score_samples(features)[0])
    is_threat = prediction == -1
    classification = "SUSPICIOUS_HIGH_VELOCITY_ANOMALY" if is_threat else "STANDARD_RETAIL_USER"
    log.info("[WALLET-RISK] %s -> %s (score: %.4f)", wallet_address, classification, raw_score)
    return AIWalletRiskResponse(
        wallet_address=wallet_address, transaction_count=row["tx_count"],
        total_volume_usd=float(row["total_volume"]),
        average_transaction_size=float(row["avg_size"]),
        ai_classification=classification, risk_score=raw_score,
        threat_alert=is_threat, evaluated_at=datetime.utcnow(),
    )


@app.get("/api/v1/predict/wallet-risk", response_model=AIWalletRiskResponse,
         tags=["Artificial Intelligence"], summary="ML risk score - authenticated")
async def predict_wallet_risk(wallet_address: str, _: str = Depends(require_api_key)):
    """Authenticated wallet scoring. Requires X-API-Key header. Unlimited requests."""
    return await _score_wallet(wallet_address)


@app.get("/api/v1/public/wallet-risk", response_model=AIWalletRiskResponse,
         tags=["Artificial Intelligence"], summary="ML risk score - public (rate limited)")
@limiter.limit("10/minute")
async def predict_wallet_risk_public(request: Request, wallet_address: str):
    """Public wallet scoring. No authentication required. Rate limited to 10 req/min per IP."""
    return await _score_wallet(wallet_address)
