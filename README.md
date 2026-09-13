# Dockerized Stock Market Data Pipeline

An Airflow ETL pipeline that obtains daily stock-market JSON from Alpha Vantage, validates/parses it, and idempotently upserts it into PostgreSQL. It uses the supplied weather-pipeline repository as an architecture blueprint, but implements the assignment as a stock pipeline.

## Architecture

```text
Alpha Vantage API -> Airflow DAG -> PostgreSQL <- pgAdmin
                                      
```

The DAG runs on weekdays at 18:00, creates one mapped task per ticker, retries transient failures twice, rejects rate-limit/API error responses clearly, skips malformed individual records, and uses an `ON CONFLICT` upsert so reruns do not duplicate rows.

## Files for submission

```text
dags/stock_market_pipeline_dag.py  Airflow orchestration
stock_pipeline.py                  requests, JSON parsing, and Postgres upsert logic
postgres/init.sql                  existing PostgreSQL target table
docker-compose.yml                 one-command Docker deployment
Dockerfile                         Airflow PostgreSQL provider image
.env.example                       secret/configuration template
```

## Start to finish (Windows PowerShell)

1. Install and start Docker Desktop. Create a free Alpha Vantage key at https://www.alphavantage.co/support/#api-key.

2. Create your local secrets file:

   ```powershell
   Copy-Item .env.example .env
   ```

3. Open `.env` and replace `ALPHA_VANTAGE_API_KEY=replace_with_your_key`. Set secure passwords and choose tickers, for example `STOCK_SYMBOLS=IBM,MSFT,AAPL`. Do not commit `.env`.

4. Build and start everything:

   ```powershell
   docker compose up --build -d
   ```

5. Open Airflow at http://localhost:8080 and sign in using `AIRFLOW_ADMIN_USER` and `AIRFLOW_ADMIN_PASSWORD` from `.env`. Find `alpha_vantage_stock_market_pipeline`, unpause it if needed, then trigger it with the play button. Scheduled runs happen at 18:00 on weekdays.

6. Inspect the loaded data in pgAdmin at http://localhost:5050. When registering the server, use host `postgres`, port `5432`, and PostgreSQL values from `.env`. Or run:

   ```powershell
   docker compose exec postgres psql -U stock_user -d stock_market -c "SELECT symbol, trading_date, close_price, volume FROM stock_daily_prices ORDER BY trading_date DESC, symbol LIMIT 20;"
   ```

## Run unit tests

The unit tests cover symbol configuration, free-API JSON parsing, API error handling, and PostgreSQL batch upserts. They run without requiring a local Airflow installation:

```powershell
python -m unittest discover -s tests -v
```

## Optional dashboard

The dashboard service deliberately uses a Compose profile, so it does not consume resources unless wanted:

```powershell
docker compose --profile dashboard up -d metabase
```

Open http://localhost:3000 and connect Metabase to `postgres:5432`; the table is `public.stock_daily_prices`.

## Notes and troubleshooting

- Alpha Vantage free accounts are rate-limited. Start with a small `STOCK_SYMBOLS` list; the pipeline makes one API request per symbol per DAG run.
- Initializing the Airflow metadata database can take a minute. Use `docker compose logs -f airflow-init` if the UI does not start.
- The SQL init script runs only on first creation of the named Postgres volume. To fully reset local database data, run `docker compose down -v` (this deletes local Postgres data) and then start again.
- Stop services with `docker compose down`.
