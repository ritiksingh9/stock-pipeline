"""Daily Alpha Vantage-to-PostgreSQL Airflow DAG."""
from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

sys.path.insert(0, "/opt/airflow")
from stock_pipeline import configured_symbols, fetch_daily_prices, load_daily_prices  # noqa: E402

with DAG(
    dag_id="alpha_vantage_stock_market_pipeline",
    description="Fetch daily prices from Alpha Vantage and upsert them into PostgreSQL.",
    start_date=datetime(2025, 1, 1),
    schedule="0 18 * * 1-5",
    catchup=False,
    max_active_runs=1,
    default_args={"owner": "ritik", "retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["stocks", "alpha-vantage", "postgres"],
) as dag:
    @task
    def get_symbols() -> list[str]:
        return configured_symbols()

    # Alpha Vantage free keys allow only one request per second.  Dynamic map
    # tasks would otherwise start together, so keep these API calls serial.
    @task(max_active_tis_per_dag=1)
    def extract_and_load(symbol: str) -> dict[str, int | str]:
        return {"symbol": symbol, "rows_upserted": load_daily_prices(fetch_daily_prices(symbol))}

    extract_and_load.expand(symbol=get_symbols())
