"""Central configuration for the stock market data pipeline."""
from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")
STOCK_SYMBOLS = [s.strip().upper() for s in os.getenv("STOCK_SYMBOLS", "IBM").split(",") if s.strip()]
DATABASE_PATH = BASE_DIR / os.getenv("DATABASE_PATH", "data/stock_market.db")
OUTPUT_DIR = BASE_DIR / os.getenv("OUTPUT_DIR", "outputs")
RAW_DATA_DIR = BASE_DIR / os.getenv("RAW_DATA_DIR", "data/raw")
API_URL = "https://www.alphavantage.co/query"
