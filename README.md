# Crypto Incremental Pipeline

An incremental ELT (Extract, Load, Transform) pipeline that pulls live cryptocurrency market data from the CoinGecko API and loads it into a PostgreSQL data warehouse - built to mirror how real companies sync external API data into their own databases (e.g. Stripe → warehouse, Salesforce → warehouse).

Instead of re-pulling and re-processing the entire dataset on every run, this pipeline tracks a **watermark** (a saved checkpoint) so it only processes records that are new or updated since the last successful run.

## Why this exists

Most beginner data projects either hardcode static data or naively re-pull everything on every run. This project intentionally implements the patterns real production pipelines rely on:

- **Incremental loading** via a watermark/checkpoint pattern
- **Idempotency** - running the pipeline any number of times never creates duplicate rows
- **Retry logic with exponential backoff** for handling transient API failures
- **Structured run logging** stored directly in the database, so every run's outcome is queryable

## Architecture

```
 CoinGecko API
       │
       │  (1) Extract: fetch top 100 coins by market cap
       ▼
 extract.py  - retry w/ exponential backoff (tenacity)
       │
       │  (2) Filter: keep only coins newer than the saved watermark
       ▼
 load.py
       │
       │  (3) Load: upsert into PostgreSQL (no duplicates on rerun)
       ▼
 PostgreSQL (Supabase)
   ├── coin_prices         → the actual market data
   ├── pipeline_watermark  → last successfully processed timestamp
   └── pipeline_logs       → history of every run (success/failure, row counts, duration)
```

## Tech stack

| Layer | Tool |
|---|---|
| Language | Python 3 |
| Package/env management | [uv](https://github.com/astral-sh/uv) |
| Data source | [CoinGecko API](https://www.coingecko.com/en/api) |
| Database | PostgreSQL (hosted on Supabase) |
| DB driver | psycopg2 |
| Retry handling | tenacity |

## Project structure

```
.
├── extract.py     # API extraction logic, wrapped with retry/backoff
├── load.py         # Main pipeline: orchestrates extract → filter → load → log
├── db.py           # Database connection, watermark read/write, run logging
├── .env.example    # Template for required environment variables
└── README.md
```

## Database schema

**coin_prices** — the core data table
| Column | Type |
|---|---|
| id | TEXT (PRIMARY KEY) |
| symbol | TEXT |
| name | TEXT |
| current_price | NUMERIC |
| market_cap | BIGINT |
| market_cap_rank | INTEGER |
| total_volume | BIGINT |
| price_change_percentage_24h | NUMERIC |
| last_updated | TIMESTAMP |

**pipeline_watermark** — tracks incremental load progress
| Column | Type |
|---|---|
| pipeline_name | TEXT (PRIMARY KEY) |
| last_pulled_at | TIMESTAMP |

**pipeline_logs** — run history
| Column | Type |
|---|---|
| id | SERIAL (PRIMARY KEY) |
| pipeline_name | TEXT |
| status | TEXT (`success` / `failed`) |
| rows_fetched | INTEGER |
| rows_loaded | INTEGER |
| error_message | TEXT |
| started_at | TIMESTAMP |
| finished_at | TIMESTAMP |

## How to run it

**1. Clone the repo**
```bash
git clone https://github.com/YOUR_USERNAME/crypto-incremental-pipeline.git
cd crypto-incremental-pipeline
```

**2. Install dependencies with uv**
```bash
uv sync
```

**3. Set up your environment variables**

Copy `.env.example` to `.env` and fill in your own PostgreSQL connection string:
```bash
cp .env.example .env
```

**4. Create the required tables**

Run this SQL against your PostgreSQL database:
```sql
CREATE TABLE coin_prices (
    id TEXT PRIMARY KEY,
    symbol TEXT,
    name TEXT,
    current_price NUMERIC,
    market_cap BIGINT,
    market_cap_rank INTEGER,
    total_volume BIGINT,
    price_change_percentage_24h NUMERIC,
    last_updated TIMESTAMP
);

CREATE TABLE pipeline_watermark (
    pipeline_name TEXT PRIMARY KEY,
    last_pulled_at TIMESTAMP
);

INSERT INTO pipeline_watermark (pipeline_name, last_pulled_at)
VALUES ('coingecko_prices', '2000-01-01 00:00:00');

CREATE TABLE pipeline_logs (
    id SERIAL PRIMARY KEY,
    pipeline_name TEXT,
    status TEXT,
    rows_fetched INTEGER,
    rows_loaded INTEGER,
    error_message TEXT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP
);
```

**5. Run the pipeline**
```bash
uv run python load.py
```

## Sample query

Once it's run a few times, you can check the most recently updated coins:
```sql
SELECT name, current_price, last_updated
FROM coin_prices
ORDER BY last_updated DESC
LIMIT 10;
```

Or check pipeline health over time:
```sql
SELECT status, rows_loaded, started_at, finished_at
FROM pipeline_logs
ORDER BY started_at DESC
LIMIT 10;
```

## What I learned building this

- Designing a schema around real API response data instead of an idealized dataset
- Implementing incremental loading using a watermark pattern instead of full re-pulls
- Writing idempotent upserts (`ON CONFLICT DO UPDATE`) so reruns never create duplicates
- Handling inconsistent data formats from a real API (CoinGecko's timestamps weren't always uniformly formatted)
- Adding retry logic with exponential backoff for resilience against transient API failures
- Logging every pipeline run's outcome directly into the database for observability

## Possible next steps

- Schedule this pipeline to run automatically with a cron job or Apache Airflow
- Containerize with Docker for portable deployment
- Add a lightweight dashboard (e.g. Streamlit) to visualize price trends over time