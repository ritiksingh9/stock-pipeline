CREATE TABLE IF NOT EXISTS stock_daily_prices (
    id BIGSERIAL PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL,
    trading_date DATE NOT NULL,
    open_price NUMERIC(18, 6) NOT NULL,
    high_price NUMERIC(18, 6) NOT NULL,
    low_price NUMERIC(18, 6) NOT NULL,
    close_price NUMERIC(18, 6) NOT NULL,
    adjusted_close_price NUMERIC(18, 6),
    volume BIGINT NOT NULL,
    source VARCHAR(50) NOT NULL DEFAULT 'alpha_vantage',
    fetched_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_stock_daily_prices_symbol_date UNIQUE (symbol, trading_date)
);

CREATE INDEX IF NOT EXISTS idx_stock_daily_prices_symbol_date
    ON stock_daily_prices (symbol, trading_date DESC);
