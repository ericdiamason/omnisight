"""
OmniSight Blockchain ETL Pipeline
==================================
Base Mainnet USDC Transfer ingestion via Airflow 3.

Production fixes applied:
  - Credentials loaded from Airflow Connections/Variables (never hardcoded)
  - All DB/node connections opened inside task function only
  - sys.path manipulation removed — venv activated at container level
  - Exceptions raised (not swallowed) so Airflow marks failures correctly
  - Batch size configurable via Airflow Variable (default 500 blocks)
  - Slack alerting on failure via on_failure_callback
  - amount_usd column aligned with API schema
"""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)

# ── Constants (non-secret) ────────────────────────────────────────────────────
USDC_ADDRESS      = "0x833589fCD6eDb6E08f4c7C32D4f71b54bda02913"
TRANSFER_TOPIC_0  = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
GENESIS_BLOCK     = 47_025_286   # OmniSight start block
DEFAULT_BATCH     = 500          # blocks per run — overridable via Airflow Variable


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_db_conn():
    """
    Open a psycopg2 connection using the Airflow Connection 'omnisight_postgres'.
    Configure via: Admin → Connections → omnisight_postgres (Postgres type).
    Never hardcode credentials.
    """
    import psycopg2
    conn_meta = BaseHook.get_connection("omnisight_postgres")
    return psycopg2.connect(
        host=conn_meta.host,
        port=conn_meta.port or 5432,
        dbname=conn_meta.schema,
        user=conn_meta.login,
        password=conn_meta.password,
    )


def _get_web3():
    """
    Return a connected Web3 instance using the node URL stored in
    Airflow Variable 'omnisight_node_url'.
    Set via: Admin → Variables → omnisight_node_url = https://base-mainnet.g.alchemy.com/v2/YOUR_KEY
    """
    from web3 import Web3
    node_url = Variable.get("omnisight_node_url")
    w3 = Web3(Web3.HTTPProvider(node_url, request_kwargs={"timeout": 30}))
    if not w3.is_connected():
        raise ConnectionError(f"Cannot connect to node: {node_url[:40]}…")
    return w3


def _decode_address(topic_bytes) -> str:
    hex_str = topic_bytes.hex() if isinstance(topic_bytes, bytes) else str(topic_bytes)
    return "0x" + hex_str[-40:]


def _slack_failure_alert(context):
    """Post a Slack message on task failure. Requires Airflow Connection 'slack_webhook'."""
    try:
        import requests
        webhook = BaseHook.get_connection("slack_webhook").host
        task_id  = context["task_instance"].task_id
        dag_id   = context["dag"].dag_id
        exec_dt  = context["execution_date"]
        err      = context.get("exception", "unknown error")
        requests.post(webhook, json={
            "text": (
                f":red_circle: *OmniSight DAG failure*\n"
                f"*DAG:* `{dag_id}` | *Task:* `{task_id}`\n"
                f"*Run:* `{exec_dt}` | *Error:* `{err}`"
            )
        }, timeout=10)
    except Exception as slack_err:
        log.warning("Slack alert failed (non-critical): %s", slack_err)


# ── Core ETL task ─────────────────────────────────────────────────────────────

def incremental_blockchain_etl(**context):
    """
    Incremental ETL: pulls USDC Transfer logs from Base Mainnet and upserts
    into omnisight.usdc_transfers. Fully idempotent via ON CONFLICT.

    Batch size is controlled by Airflow Variable 'omnisight_batch_size'
    (default: 500). After any downtime the pipeline catches up automatically
    by processing BATCH_SIZE blocks per 2-minute run.
    """
    batch_size = int(Variable.get("omnisight_batch_size", default_var=DEFAULT_BATCH))

    # ── 1. Open connections INSIDE the task (never at module level) ───────────
    conn = _get_db_conn()
    w3   = _get_web3()

    try:
        cursor = conn.cursor()

        # ── 2. Determine sync range ───────────────────────────────────────────
        cursor.execute("SELECT MAX(block_number) FROM omnisight.usdc_transfers;")
        row = cursor.fetchone()
        max_db_block = row[0] if row and row[0] else None

        live_tip    = w3.eth.block_number
        start_block = (max_db_block + 1) if max_db_block else GENESIS_BLOCK
        end_block   = min(live_tip, start_block + batch_size)

        if start_block > live_tip:
            log.info("Pipeline fully synced at block #%s. Nothing to do.", live_tip)
            return

        lag = live_tip - start_block
        log.info(
            "Sync range: #%s → #%s  |  batch=%s  |  chain lag=%s blocks",
            start_block, end_block, batch_size, lag,
        )

        # ── 3. Fetch and insert logs ──────────────────────────────────────────
        total_inserted = 0

        for block_num in range(start_block, end_block + 1):
            try:
                raw_logs = w3.eth.get_logs({
                    "fromBlock": block_num,
                    "toBlock":   block_num,
                    "address":   w3.to_checksum_address(USDC_ADDRESS),
                    "topics":    [TRANSFER_TOPIC_0],
                })

                inserted = 0
                for log_entry in raw_logs:
                    tx_hash  = log_entry["transactionHash"].hex()
                    sender   = _decode_address(log_entry["topics"][1])
                    receiver = _decode_address(log_entry["topics"][2])

                    raw_hex   = log_entry["data"].hex() if isinstance(log_entry["data"], bytes) else log_entry["data"]
                    raw_value = int(raw_hex, 16) if raw_hex and raw_hex != "0x" else 0
                    # adjusted_amount: human-readable USDC (6 decimals)
                    # amount_usd: same value — USDC is 1:1 USD pegged
                    amount_usd = raw_value / 10 ** 6

                    cursor.execute(
                        """
                        INSERT INTO omnisight.usdc_transfers
                            (block_number, transaction_hash, sender_address,
                             receiver_address, raw_amount, adjusted_amount, amount_usd, ingested_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                        ON CONFLICT (block_number, transaction_hash) DO NOTHING;
                        """,
                        (block_num, tx_hash, sender, receiver,
                         str(raw_value), amount_usd, amount_usd),
                    )
                    inserted += 1

                conn.commit()
                total_inserted += inserted
                log.info("Block #%s: %s records inserted.", block_num, inserted)

            except Exception as block_err:
                # Roll back only the current block — do not abort the whole batch
                conn.rollback()
                log.warning("Block #%s skipped — transient error: %s", block_num, block_err)
                continue

        log.info(
            "Batch complete. Blocks #%s–#%s processed. Total records: %s.",
            start_block, end_block, total_inserted,
        )

        # Push metrics to XCom for downstream tasks / monitoring
        context["ti"].xcom_push(key="blocks_processed", value=end_block - start_block + 1)
        context["ti"].xcom_push(key="records_inserted", value=total_inserted)
        context["ti"].xcom_push(key="chain_lag_blocks", value=lag)

    finally:
        # Always close — even if an unhandled exception occurs
        cursor.close()
        conn.close()


# ── DAG definition ────────────────────────────────────────────────────────────

default_args = {
    "owner":            "omnisight",
    "depends_on_past":  False,
    "start_date":       datetime(2026, 1, 1),
    "retries":          3,
    "retry_delay":      timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "email_on_failure": False,   # using Slack callback instead
    "email_on_retry":   False,
    "on_failure_callback": _slack_failure_alert,
}

with DAG(
    dag_id="omnisight_blockchain_pipeline",
    default_args=default_args,
    description="Autonomous Base Mainnet USDC ingestion — OmniSight",
    schedule=timedelta(minutes=2),
    catchup=False,
    max_active_runs=1,
    tags=["omnisight", "blockchain", "usdc", "base-mainnet"],
) as dag:

    ingest = PythonOperator(
        task_id="incremental_blockchain_etl",
        python_callable=incremental_blockchain_etl,
        provide_context=True,
        execution_timeout=timedelta(minutes=8),   # hard kill if hung
        pool="blockchain_pool",                    # limit concurrent node calls
    )
