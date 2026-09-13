"""Fetch, validate, parse, and upsert Alpha Vantage stock data to PostgreSQL."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Any

import requests
from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

LOGGER = logging.getLogger(__name__)
API_URL = "https://www.alphavantage.co/query"
# TIME_SERIES_DAILY is available with a free Alpha Vantage key.  The adjusted
# variant is a premium endpoint, so adjusted_close_price remains null here.
DAILY_FUNCTION = "TIME_SERIES_DAILY"
SERIES_KEY = "Time Series (Daily)"


@dataclass(frozen=True)
class DailyPrice:
    symbol: str
    trading_date: date
    open_price: Decimal
    high_price: Decimal
    low_price: Decimal
    close_price: Decimal
    adjusted_close_price: Decimal | None
    volume: int


def configured_symbols() -> list[str]:
    """Read, normalize, and deduplicate tickers from STOCK_SYMBOLS."""
    symbols = list(dict.fromkeys(s.strip().upper() for s in os.environ.get("STOCK_SYMBOLS", "").split(",") if s.strip()))
    if not symbols:
        raise AirflowException("STOCK_SYMBOLS is empty. Example: IBM,MSFT,AAPL")
    return symbols


def fetch_daily_prices(symbol: str) -> list[DailyPrice]:
    """Request Alpha Vantage daily data and return valid records only."""
    api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
    if not api_key:
        raise AirflowException("ALPHA_VANTAGE_API_KEY is not configured")
    try:
        response = requests.get(
            API_URL,
            params={"function": DAILY_FUNCTION, "symbol": symbol, "outputsize": "compact", "apikey": api_key},
            timeout=(10, 45),
        )
        response.raise_for_status()
        payload: dict[str, Any] = response.json()
    except requests.RequestException as exc:
        raise AirflowException(f"Alpha Vantage request failed for {symbol}: {exc}") from exc
    except ValueError as exc:
        raise AirflowException(f"Alpha Vantage returned invalid JSON for {symbol}") from exc

    for key in ("Error Message", "Information", "Note"):
        if payload.get(key):
            raise AirflowException(f"Alpha Vantage rejected {symbol}: {payload[key]}")
    series = payload.get(SERIES_KEY)
    if not isinstance(series, dict) or not series:
        raise AirflowException(f"No {SERIES_KEY!r} data received for {symbol}")

    prices: list[DailyPrice] = []
    for trade_date, values in series.items():
        try:
            prices.append(DailyPrice(
                symbol=symbol,
                trading_date=date.fromisoformat(trade_date),
                open_price=Decimal(values["1. open"]),
                high_price=Decimal(values["2. high"]),
                low_price=Decimal(values["3. low"]),
                close_price=Decimal(values["4. close"]),
                adjusted_close_price=None,
                volume=int(values["5. volume"]),
            ))
        except (KeyError, TypeError, ValueError, InvalidOperation) as exc:
            LOGGER.warning("Skipping malformed %s row for %s: %s", trade_date, symbol, exc)
    if not prices:
        raise AirflowException(f"All daily price rows were malformed for {symbol}")
    return prices


UPSERT_SQL = """
INSERT INTO stock_daily_prices (
    symbol, trading_date, open_price, high_price, low_price, close_price,
    adjusted_close_price, volume, source, fetched_at, updated_at
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'alpha_vantage', NOW(), NOW())
ON CONFLICT (symbol, trading_date) DO UPDATE SET
    open_price = EXCLUDED.open_price,
    high_price = EXCLUDED.high_price,
    low_price = EXCLUDED.low_price,
    close_price = EXCLUDED.close_price,
    adjusted_close_price = EXCLUDED.adjusted_close_price,
    volume = EXCLUDED.volume,
    fetched_at = NOW(),
    updated_at = NOW();
"""


def load_daily_prices(prices: list[DailyPrice]) -> int:
    """Upsert observations so reruns update, rather than duplicate, a market date."""
    rows = [
        (p.symbol, p.trading_date, p.open_price, p.high_price, p.low_price, p.close_price, p.adjusted_close_price, p.volume)
        for p in prices
    ]
    if rows:
        hook = PostgresHook(postgres_conn_id="stock_postgres")
        with hook.get_conn() as connection, connection.cursor() as cursor:
            cursor.executemany(UPSERT_SQL, rows)
            connection.commit()
    return len(rows)
