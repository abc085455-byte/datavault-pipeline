"""
DataVault Pipeline - Airflow DAG
CoinGecko API → S3 Bronze → Snowflake Gold
"""

import sys
import os
from datetime import datetime, timezone, timedelta

sys.path.insert(0, "/opt/datavault-pipeline")

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    "owner": "datavault",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": False,
}


def extract(**context):
    sys.path.insert(0, "/opt/datavault-pipeline")

    from scripts.extract.api_extractor import CoinGeckoExtractor

    # Airflow 3.x compatible
    execution_date = (
        context.get("logical_date")
        or context.get("data_interval_start")
        or datetime.now(timezone.utc)
    )

    extractor = CoinGeckoExtractor()
    market_data = extractor.fetch_market_data()

    run_id = execution_date.strftime("%Y%m%d_%H%M%S")
    payload = {
        "metadata": {
            "run_id": run_id,
            "extracted_at": execution_date.isoformat(),
            "source": "coingecko_api_v3",
            "layer": "bronze",
            "coin_count": len(market_data),
            "coins": extractor.coins,
        },
        "market_data": market_data,
    }

    filename = f"crypto_{run_id}.json"
    key = extractor.s3.build_raw_key(filename, execution_date)
    uri = extractor.s3.write_json(data=payload, key=key)

    context["ti"].xcom_push(key="s3_uri", value=uri)
    context["ti"].xcom_push(key="run_id", value=run_id)
    context["ti"].xcom_push(key="payload", value=payload)

    print(f"✅ Extract complete | {len(market_data)} coins | {uri}")
    return uri


def load(**context):
    sys.path.insert(0, "/opt/datavault-pipeline")

    from scripts.load.snowflake_loader import SnowflakeLoader

    ti = context["ti"]
    payload = ti.xcom_pull(task_ids="extract_task", key="payload")
    run_id = ti.xcom_pull(task_ids="extract_task", key="run_id")

    print(f"Loading run_id: {run_id}")
    loader = SnowflakeLoader()
    result = loader.load(payload)

    print(f"✅ Load complete | {result['rows_loaded']} rows")
    return result


def notify(**context):
    ti = context["ti"]
    run_id = ti.xcom_pull(task_ids="extract_task", key="run_id")
    s3_uri = ti.xcom_pull(task_ids="extract_task", key="s3_uri")
    load_result = ti.xcom_pull(task_ids="load_task")

    print("=" * 50)
    print("  DataVault Pipeline Complete! ✅")
    print("=" * 50)
    print(f"  Run ID      : {run_id}")
    print(f"  S3 URI      : {s3_uri}")
    print(f"  Rows Loaded : {load_result['rows_loaded']}")
    print("=" * 50)


with DAG(
    dag_id="datavault_crypto_pipeline",
    description="CoinGecko → S3 Bronze → Snowflake Gold",
    default_args=default_args,
    start_date=datetime(2026, 3, 14, tzinfo=timezone.utc),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["datavault", "crypto", "pipeline"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load,
    )

    notify_task = PythonOperator(
        task_id="notify_task",
        python_callable=notify,
    )

    extract_task >> load_task >> notify_task