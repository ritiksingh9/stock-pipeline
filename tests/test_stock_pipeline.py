"""Unit tests for the current Alpha Vantage to PostgreSQL pipeline."""
from __future__ import annotations

import sys
import types
import unittest
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

# Keep these tests runnable locally without installing the complete Airflow UI.
if "airflow" not in sys.modules:
    airflow = types.ModuleType("airflow")
    exceptions = types.ModuleType("airflow.exceptions")

    class AirflowException(Exception):
        pass

    exceptions.AirflowException = AirflowException
    postgres = types.ModuleType("airflow.providers.postgres.hooks.postgres")
    postgres.PostgresHook = object
    sys.modules.update({
        "airflow": airflow,
        "airflow.exceptions": exceptions,
        "airflow.providers": types.ModuleType("airflow.providers"),
        "airflow.providers.postgres": types.ModuleType("airflow.providers.postgres"),
        "airflow.providers.postgres.hooks": types.ModuleType("airflow.providers.postgres.hooks"),
        "airflow.providers.postgres.hooks.postgres": postgres,
    })

import stock_pipeline


class FakeResponse:
    def __init__(self, payload: dict):
        self.payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self.payload


class StockPipelineTests(unittest.TestCase):
    def test_configured_symbols_normalizes_and_deduplicates(self) -> None:
        with patch.dict("os.environ", {"STOCK_SYMBOLS": "ibm, MSFT, ibm"}, clear=False):
            self.assertEqual(stock_pipeline.configured_symbols(), ["IBM", "MSFT"])

    def test_fetch_daily_prices_parses_free_daily_response(self) -> None:
        payload = {"Time Series (Daily)": {
            "2026-09-11": {
                "1. open": "100.00", "2. high": "105.00", "3. low": "99.00",
                "4. close": "104.00", "5. volume": "1000",
            }
        }}
        with patch.dict("os.environ", {"ALPHA_VANTAGE_API_KEY": "test-key"}, clear=False), \
             patch.object(stock_pipeline.requests, "get", return_value=FakeResponse(payload)) as get:
            prices = stock_pipeline.fetch_daily_prices("IBM")

        self.assertEqual(len(prices), 1)
        self.assertEqual(prices[0].symbol, "IBM")
        self.assertEqual(prices[0].trading_date, date(2026, 9, 11))
        self.assertEqual(prices[0].close_price, Decimal("104.00"))
        self.assertEqual(prices[0].volume, 1000)
        self.assertEqual(get.call_args.kwargs["params"]["function"], "TIME_SERIES_DAILY")

    def test_fetch_daily_prices_rejects_api_error(self) -> None:
        with patch.dict("os.environ", {"ALPHA_VANTAGE_API_KEY": "test-key"}, clear=False), \
             patch.object(stock_pipeline.requests, "get", return_value=FakeResponse({"Information": "rate limited"})):
            with self.assertRaises(stock_pipeline.AirflowException):
                stock_pipeline.fetch_daily_prices("IBM")

    def test_load_daily_prices_uses_batch_upsert(self) -> None:
        row = stock_pipeline.DailyPrice(
            symbol="IBM", trading_date=date(2026, 9, 11), open_price=Decimal("100"),
            high_price=Decimal("105"), low_price=Decimal("99"), close_price=Decimal("104"),
            adjusted_close_price=None, volume=1000,
        )
        cursor = MagicMock()
        connection = MagicMock()
        connection.cursor.return_value.__enter__.return_value = cursor
        with patch.object(stock_pipeline, "PostgresHook") as hook_class:
            hook_class.return_value.get_conn.return_value.__enter__.return_value = connection
            self.assertEqual(stock_pipeline.load_daily_prices([row]), 1)

        cursor.executemany.assert_called_once()
        connection.commit.assert_called_once()


if __name__ == "__main__":
    unittest.main()
