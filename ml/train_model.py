import os
import sys
import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import pandas as pd
import joblib
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import RobustScaler
from sklearn.pipeline import Pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

MODEL_PATH    = Path("/home/omnisight") / "threat_model.pkl"
METADATA_PATH = Path("/home/omnisight") / "threat_model_metadata.json"
MIN_TRAINING_ROWS = 50
CONTAMINATION     = 0.05
N_ESTIMATORS      = 200
RANDOM_STATE      = 42

def _get_db_connection():
    password = os.getenv("OMNISIGHT_DB_PASS")
    if not password:
        raise RuntimeError("OMNISIGHT_DB_PASS is not set.")
    return psycopg2.connect(
        host=os.getenv("OMNISIGHT_DB_HOST", "127.0.0.1"),
        database=os.getenv("OMNISIGHT_DB_NAME", "postgres"),
        user=os.getenv("OMNISIGHT_DB_USER", "omnisight_user"),
        password=password,
    )

def extract_wallet_features() -> pd.DataFrame:
    query = """
        SELECT sender_address AS wallet_address,
               COUNT(transfer_id)    AS tx_count,
               SUM(adjusted_amount)  AS total_volume_usd,
               AVG(adjusted_amount)  AS avg_tx_size
        FROM omnisight.usdc_transfers
        GROUP BY sender_address
        HAVING COUNT(transfer_id) >= 2
        ORDER BY tx_count DESC;
    """
    try:
        conn = _get_db_connection()
        df = pd.read_sql_query(query, conn)
        conn.close()
        log.info("[EXTRACT] %s wallet profiles extracted.", len(df))
        return df
    except Exception as exc:
        log.error("[EXTRACT] Feature extraction failed: %s", exc)
        sys.exit(1)

def train_anomaly_model() -> None:
    log.info("=" * 60)
    log.info("[OMNISIGHT ML] Starting model training pipeline")
    log.info("=" * 60)
    df = extract_wallet_features()
    if len(df) < MIN_TRAINING_ROWS:
        log.warning("[TRAIN] Only %s profiles. Minimum: %s.", len(df), MIN_TRAINING_ROWS)
        sys.exit(0)
    feature_columns = ["tx_count", "total_volume_usd", "avg_tx_size"]
    X = df[feature_columns].fillna(0)
    log.info("[TRAIN] Feature matrix: %s rows x %s features", *X.shape)
    log.info("[TRAIN] Feature summary:\n%s", X.describe().to_string())
    model_pipeline = Pipeline([
        ("scaler", RobustScaler()),
        ("isolation_forest", IsolationForest(
            contamination=CONTAMINATION,
            n_estimators=N_ESTIMATORS,
            random_state=RANDOM_STATE,
            n_jobs=-1,
        )),
    ])
    log.info("[TRAIN] Fitting RobustScaler + IsolationForest...")
    model_pipeline.fit(X)
    log.info("[TRAIN] Fitting complete.")
    joblib.dump(model_pipeline, MODEL_PATH)
    log.info("[SAVE] Model saved to: %s", MODEL_PATH)
    metadata = {
        "trained_at":    datetime.now(timezone.utc).isoformat(),
        "model_version": datetime.now(timezone.utc).strftime("v%Y%m%d_%H%M"),
        "row_count":     len(df),
        "features":      feature_columns,
        "hyperparams": {
            "algorithm":     "IsolationForest",
            "scaler":        "RobustScaler",
            "contamination": CONTAMINATION,
            "n_estimators":  N_ESTIMATORS,
            "random_state":  RANDOM_STATE,
        },
        "known_limitations": [
            "Trained on sender wallet behaviour only.",
            "Data gaps during pipeline pauses may skew anomaly boundaries.",
            "contamination=0.05 is a heuristic — tune from operational feedback.",
            "Does not account for known exchange or protocol wallets.",
        ],
    }
    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=2)
    log.info("[SAVE] Metadata saved to: %s", METADATA_PATH)
    log.info("[DONE] Version: %s | Wallets trained on: %s",
             metadata["model_version"], metadata["row_count"])

if __name__ == "__main__":
    train_anomaly_model()
