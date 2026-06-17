"""
omnisight_pipeline.py
=====================
OmniSight Core Airflow DAG — Base Mainnet USDC Ingestion Pipeline
"""

import logging
import os
import sys
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Runtime path injection — required because Airflow runs as the 'airflow'
# user but packages are installed in the omnisight venv.
# noqa: E402 applied to third-party imports below.
# ---------------------------------------------------------------------------
WORKING_VENV_PATH = "/home/omnisight/venv/lib/python3.9/site-packages"
if WORKING_VENV_PATH not in sys.path:
    sys.path.insert(0, WORKING_VENV_PATH)

import psycopg2  # noqa: E402
from airflow import DAG  # noqa: E402
from airflow.operators.python import PythonOperator  # noqa: E402
from web3 import Web3  # noqa: E402

log = logging.getLogger(__name__)

USDC_CONTRACT_ADDRESS = "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913"
TRANSFER_EVENT_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
USDC_DECIMALS = 10 ** 6
GENESIS_BLOCK = 47_025_286
MAX_BLOCKS_PER_RUN = 5


def _get_node_url() -> str:
    url = os.getenv("OMNISIGHT_NODE_URL")
    if not url:
        raise RuntimeError("OMNISIGHT_NODE_URL is not set in /etc/omnisight.env")
    return url


def _get_db_connection():
    password = os.getenv("OMNISIGHT_DB_PASS")
    if not password:
        raise RuntimeError("OMNISIGHT_DB_PASS is not set in /etc/omnisight.env")
    return psycopg2.connect(
        host=os.getenv("OMNISIGHT_DB_HOST", "127.0.0.1"),
        database=os.getenv("OMNISIGHT_DB_NAME", "postgres"),
        user=os.getenv("OMNISIGHT_DB_USER", "omnisight_user"),
        password=password,
    )


def decode_evm_address(topic_bytes) -> str:
    """Converts 32-byte EVM topic to 42-char wallet address."""
    hex_str = topic_bytes.hex() if isinstance(topic_bytes, bytes) else str(topic_bytes)
    return "0x" + hex_str.replace("0x", "").replace("0X", "")[-40:].lower()


def decode_usdc_amount(data_field) -> tuple:
    """Decodes Transfer data field to (raw_int, usd_float)."""
    raw_hex = data_field.hex() if isinstance(data_field, bytes) else str(data_field)
    raw_int = int(raw_hex, 16) if raw_hex and raw_hex not in ("0x", "0X", "") else 0
    return raw_int, raw_int / USDC_DECIMALS


def incremental_blockchain_etl() -> None:
    """
    Incremental Base Mainnet USDC ingestion.

    Finds MAX(block_number) in PostgreSQL as checkpoint, fetches the next
    batch of blocks from Alchemy, decodes Transfer events, and inserts
    records with ON CONFLICT (block_number, transaction_hash) DO NOTHING.
    Commits per block so partial progress is preserved on failure.
    """
    log.info("=" * 60)
    log.info("[OMNISIGHT] Starting incremental blockchain ETL")

    try:
        conn = _get_db_connection()
        cursor = conn.cursor()
        log.info("[DB] Connected to PostgreSQL.")
    except Exception as exc:
        log.error("[DB] Connection failed: %s", exc)
        raise

    try:
        cursor.execute("SELECT MAX(block_number) FROM omnisight.usdc_transfers;")
        result = cursor.fetchone()
        last_block = result[0] if result and result[0] else GENESIS_BLOCK - 1
        log.info("[DB] Checkpoint: block #%s", f"{last_block:,}")
    except Exception as exc:
        log.error("[DB] Checkpoint query failed: %s", exc)
        conn.close()
        raise

    try:
        w3 = Web3(Web3.HTTPProvider(_get_node_url()))
        if not w3.is_connected():
            raise ConnectionError("Web3 provider returned disconnected state.")
        chain_tip = w3.eth.block_number
        log.info("[NODE] Chain tip: #%s", f"{chain_tip:,}")
    except Exception as exc:
        log.error("[NODE] Alchemy connection failed: %s", exc)
        conn.close()
        raise

    start_block = last_block + 1
    end_block = min(chain_tip, start_block + MAX_BLOCKS_PER_RUN - 1)

    if start_block > chain_tip:
        log.info("[SYNC] Already at chain tip. Nothing to process.")
        cursor.close()
        conn.close()
        return

    log.info("[SYNC] Processing #%s → #%s", f"{start_block:,}", f"{end_block:,}")

    total_inserted = 0

    for block_num in range(start_block, end_block + 1):
        try:
            raw_logs = w3.eth.get_logs({
                "fromBlock": block_num,
                "toBlock": block_num,
                "address": w3.to_checksum_address(USDC_CONTRACT_ADDRESS),
                "topics": [TRANSFER_EVENT_TOPIC],
            })
            log.info("[BLOCK #%s] %s events.", f"{block_num:,}", len(raw_logs))

            block_inserted = 0
            for event_log in raw_logs:
                try:
                    tx_hash = (
                        event_log["transactionHash"].hex()
                        if isinstance(event_log["transactionHash"], bytes)
                        else event_log["transactionHash"]
                    )
                    sender = decode_evm_address(event_log["topics"][1])
                    receiver = decode_evm_address(event_log["topics"][2])
                    raw_amount, usd_amount = decode_usdc_amount(event_log["data"])

                    cursor.execute(
                        """
                        INSERT INTO omnisight.usdc_transfers
                            (block_number, transaction_hash, sender_address,
                             receiver_address, raw_amount, adjusted_amount)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (block_number, transaction_hash) DO NOTHING;
                        """,
                        (block_num, tx_hash, sender, receiver,
                         raw_amount, usd_amount),
                    )
                    block_inserted += 1

                except Exception as log_err:
                    log.warning("[BLOCK #%s] Skipping log: %s", f"{block_num:,}", log_err)
                    continue

            conn.commit()
            total_inserted += block_inserted
            log.info("[BLOCK #%s] Committed %s records.", f"{block_num:,}", block_inserted)

        except Exception as block_err:
            log.warning("[BLOCK #%s] Skipping: %s", f"{block_num:,}", block_err)
            conn.rollback()
            continue

    cursor.close()
    conn.close()
    log.info("[OMNISIGHT] ETL complete. %s records inserted.", total_inserted)


default_args = {
    "owner": "omnisight",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="omnisight_blockchain_pipeline",
    default_args=default_args,
    description="Incremental Base Mainnet USDC ingestion — 120s cadence, 5 blocks/run.",
    schedule=timedelta(minutes=2),
    catchup=False,
    max_active_runs=1,
    tags=["omnisight", "web3", "usdc", "base"],
) as dag:

    PythonOperator(
        task_id="execute_blockchain_etl",
        python_callable=incremental_blockchain_etl,
    )
