# 🚀 DataVault Pipeline

> A production-grade Data Engineering pipeline that collects live cryptocurrency
> market data every hour — fully automated, cloud-native, and GitHub-ready.

![Pipeline](https://img.shields.io/badge/Pipeline-Production%20Ready-brightgreen)
![Airflow](https://img.shields.io/badge/Airflow-3.x-blue)
![Snowflake](https://img.shields.io/badge/Snowflake-Gold%20Layer-cyan)
![AWS](https://img.shields.io/badge/AWS-S3%20Bronze-orange)
![Python](https://img.shields.io/badge/Python-3.10+-yellow)

---

## 📌 What Does This Project Do?

Every hour, this pipeline automatically:

1. 📥 **Extracts** live crypto prices from CoinGecko API
2. 📦 **Stores** raw JSON data in AWS S3 (Bronze Layer)
3. ❄️ **Loads** cleaned data into Snowflake tables (Gold Layer)
4. ✅ **Orchestrates** everything via Apache Airflow DAG

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Apache Airflow DAG                  │
│              (Runs Every Hour Automatically)         │
│                                                      │
│  ┌─────────────┐   ┌─────────────┐   ┌───────────┐  │
│  │ extract_task│──▶│  load_task  │──▶│notify_task│  │
│  └─────────────┘   └─────────────┘   └───────────┘  │
│         │                 │                          │
│         ▼                 ▼                          │
│  ┌─────────────┐   ┌─────────────┐                  │
│  │   AWS S3    │   │  Snowflake  │                  │
│  │   Bronze    │   │    Gold     │                  │
│  └─────────────┘   └─────────────┘                  │
└─────────────────────────────────────────────────────┘
         ▲
         │
┌─────────────────┐
│  CoinGecko API  │
│  (Live Crypto)  │
└─────────────────┘
```

---

## 🛠️ Tech Stack

| Technology | Version | Purpose |
|-----------|---------|---------|
| Python | 3.10+ | Core language |
| Apache Airflow | 3.x | Pipeline orchestration |
| AWS S3 | - | Raw data storage (Bronze Layer) |
| Snowflake | - | Data warehouse (Gold Layer) |
| CoinGecko API | v3 | Live crypto market data |
| Docker | - | Airflow containerization |
| boto3 | 1.34+ | AWS SDK |
| snowflake-connector | 3.7+ | Snowflake SDK |

---

## 📁 Project Structure

```
datavault-pipeline/
│
├── 📂 config/
│   ├── __init__.py
│   └── settings.py           # All environment configs
│
├── 📂 dags/
│   └── datavault_pipeline_dag.py   # Airflow DAG definition
│
├── 📂 scripts/
│   ├── 📂 extract/
│   │   └── api_extractor.py  # CoinGecko API → S3
│   ├── 📂 load/
│   │   └── snowflake_loader.py  # S3 → Snowflake
│   └── 📂 utils/
│       ├── logger.py          # Structured logging
│       └── s3_helper.py       # S3 read/write helper
│
├── .env.example               # Environment template
├── .gitignore                 # Git ignore rules
├── requirements.txt           # Python dependencies
└── README.md
```

---

## ⚡ Quick Start

### 1️⃣ Clone the Repository

```bash
git clone https://github.com/abc085455-byte/datavault-pipeline.git
cd datavault-pipeline
```

### 2️⃣ Setup Environment Variables

```bash
cp .env.example .env
```

Now open `.env` and fill in your credentials:

```env
# ── AWS ──────────────────────────────────────
AWS_ACCESS_KEY_ID=your_aws_access_key
AWS_SECRET_ACCESS_KEY=your_aws_secret_key
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET_NAME=your-s3-bucket-name
S3_RAW_PREFIX=raw/crypto
S3_PROCESSED_PREFIX=processed/crypto

# ── Snowflake ─────────────────────────────────
SNOWFLAKE_ACCOUNT=youraccountid.region.aws
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_DATABASE=DATAVAULT_DB
SNOWFLAKE_SCHEMA=GOLD
SNOWFLAKE_WAREHOUSE=DATAVAULT_WH
SNOWFLAKE_ROLE=ACCOUNTADMIN

# ── CoinGecko ─────────────────────────────────
COINGECKO_API_URL=https://api.coingecko.com/api/v3
COINGECKO_API_KEY=your_api_key_optional
CRYPTO_COINS=bitcoin,ethereum,solana

# ── Pipeline ──────────────────────────────────
PIPELINE_ENV=development
LOG_LEVEL=INFO
```

### 3️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

---

## ☁️ Cloud Setup

### AWS S3 Setup

1. Login → [AWS Console](https://console.aws.amazon.com)
2. Go to **S3** → Create bucket: `your-bucket-name`
3. Go to **IAM** → Create user → Attach `AmazonS3FullAccess`
4. Create **Access Keys** → Copy to `.env`

### Snowflake Setup

Run these SQL scripts in Snowflake Worksheet **in order**:

```sql
-- Step 1: Create Warehouse & Database
CREATE WAREHOUSE IF NOT EXISTS DATAVAULT_WH
    WITH WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS DATAVAULT_DB;
CREATE SCHEMA IF NOT EXISTS DATAVAULT_DB.GOLD;

-- Step 2: Use them
USE WAREHOUSE DATAVAULT_WH;
USE DATABASE DATAVAULT_DB;
USE SCHEMA GOLD;

-- Step 3: Create Tables
CREATE TABLE IF NOT EXISTS CRYPTO_RAW (
    id                     NUMBER AUTOINCREMENT PRIMARY KEY,
    run_id                 VARCHAR(64),
    extracted_at           TIMESTAMP_TZ,
    coin_id                VARCHAR(64),
    symbol                 VARCHAR(16),
    name                   VARCHAR(256),
    current_price_usd      FLOAT,
    high_24h_usd           FLOAT,
    low_24h_usd            FLOAT,
    market_cap_usd         FLOAT,
    market_cap_rank        INTEGER,
    total_volume_usd       FLOAT,
    circulating_supply     FLOAT,
    total_supply           FLOAT,
    price_change_pct_24h   FLOAT,
    price_change_pct_1h    FLOAT,
    price_change_pct_7d    FLOAT,
    all_time_high_usd      FLOAT,
    all_time_low_usd       FLOAT,
    market_cap_tier        VARCHAR(32),
    performance_24h        VARCHAR(32),
    loaded_at              TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS CRYPTO_DAILY_SUMMARY (
    id                       NUMBER AUTOINCREMENT PRIMARY KEY,
    price_date               DATE,
    coin_id                  VARCHAR(64),
    symbol                   VARCHAR(16),
    name                     VARCHAR(256),
    avg_price_usd            FLOAT,
    day_high_usd             FLOAT,
    day_low_usd              FLOAT,
    total_volume_usd         FLOAT,
    avg_price_change_pct_24h FLOAT,
    observation_count        INTEGER,
    latest_run_id            VARCHAR(64),
    created_at               TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at               TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT uq_coin_date UNIQUE (coin_id, price_date)
);

-- Step 4: Create Views
CREATE OR REPLACE VIEW V_CRYPTO_LATEST AS
SELECT cr.* FROM CRYPTO_RAW cr
INNER JOIN (
    SELECT coin_id, MAX(extracted_at) AS max_extracted_at
    FROM CRYPTO_RAW GROUP BY coin_id
) latest
ON cr.coin_id = latest.coin_id
AND cr.extracted_at = latest.max_extracted_at;

CREATE OR REPLACE VIEW V_TOP_MOVERS_24H AS
SELECT coin_id, symbol, name,
       current_price_usd,
       price_change_pct_24h,
       market_cap_usd,
       performance_24h
FROM V_CRYPTO_LATEST
ORDER BY ABS(price_change_pct_24h) DESC;

CREATE OR REPLACE VIEW V_MARKET_DOMINANCE AS
SELECT coin_id, symbol, name,
       market_cap_usd,
       ROUND(market_cap_usd / SUM(market_cap_usd) OVER () * 100, 2)
           AS market_dominance_pct,
       market_cap_rank
FROM V_CRYPTO_LATEST
ORDER BY market_cap_rank;
```

---

## 🐳 Airflow Setup (Docker)

### Update docker-compose.yml

Add to **volumes** section:
```yaml
volumes:
    - D:/datavault-pipeline/dags:/opt/airflow/dags
    - D:/datavault-pipeline:/opt/datavault-pipeline
    - ./logs:/opt/airflow/logs
    - ./config:/opt/airflow/config
    - ./plugins:/opt/airflow/plugins
```

Add to **environment** section:
```yaml
environment:
    SNOWFLAKE_ACCOUNT: your_account
    SNOWFLAKE_USER: your_user
    SNOWFLAKE_PASSWORD: your_password
    SNOWFLAKE_DATABASE: DATAVAULT_DB
    SNOWFLAKE_SCHEMA: GOLD
    SNOWFLAKE_WAREHOUSE: DATAVAULT_WH
    SNOWFLAKE_ROLE: ACCOUNTADMIN
    CRYPTO_COINS: bitcoin,ethereum,solana
    S3_BUCKET_NAME: your-bucket-name
    AWS_ACCESS_KEY_ID: your_key
    AWS_SECRET_ACCESS_KEY: your_secret
    AWS_DEFAULT_REGION: us-east-1
```

### Start Airflow

```bash
cd /path/to/airflow
docker-compose up -d
```

---

## ✅ Test Connections

Before running the pipeline, test each connection:

```bash
# 1. Test CoinGecko API
python -c "
from scripts.extract.api_extractor import CoinGeckoExtractor
e = CoinGeckoExtractor()
data = e.fetch_market_data()
print(f'API OK — {len(data)} coins fetched')
"

# 2. Test AWS S3
python -c "
from scripts.utils.s3_helper import S3Helper
s = S3Helper()
print('S3 OK' if s.check_connection() else 'S3 FAILED')
"

# 3. Test Snowflake
python -c "
from scripts.load.snowflake_loader import SnowflakeLoader
s = SnowflakeLoader()
s.connect()
print('Snowflake OK')
s.disconnect()
"
```

---

## 🎯 Run the Pipeline

### Option A — Manual (Local)

```bash
python -c "
from scripts.extract.api_extractor import CoinGeckoExtractor
from scripts.load.snowflake_loader import SnowflakeLoader
from datetime import datetime, timezone

dt = datetime.now(timezone.utc)
run_id = dt.strftime('%Y%m%d_%H%M%S')

extractor = CoinGeckoExtractor()
market_data = extractor.fetch_market_data()

payload = {
    'metadata': {
        'run_id': run_id,
        'extracted_at': dt.isoformat(),
        'source': 'coingecko_api_v3',
        'layer': 'bronze',
        'coin_count': len(market_data),
        'coins': extractor.coins,
    },
    'market_data': market_data,
}

key = extractor.s3.build_raw_key(f'crypto_{run_id}.json', dt)
extractor.s3.write_json(data=payload, key=key)

loader = SnowflakeLoader()
result = loader.load(payload)
print(f'Done! Rows loaded: {result[\"rows_loaded\"]}')
"
```

### Option B — Airflow UI (Recommended)

1. Open → [http://localhost:8080](http://localhost:8080)
2. Login: `airflow` / `airflow`
3. Find: `datavault_crypto_pipeline`
4. Click ▶️ **Trigger** button

**DAG Flow:**
```
extract_task ──▶ load_task ──▶ notify_task
```

| Task | What it does |
|------|-------------|
| `extract_task` | Fetches live data from CoinGecko → saves to S3 |
| `load_task` | Reads payload → inserts into Snowflake CRYPTO_RAW |
| `notify_task` | Prints run summary to logs |

**Schedule:** Runs automatically every hour `0 * * * *`

---

## 📊 Query Your Data

After pipeline runs, query in Snowflake:

```sql
-- Latest price snapshot
SELECT * FROM V_CRYPTO_LATEST;

-- Biggest movers today
SELECT * FROM V_TOP_MOVERS_24H;

-- Market dominance
SELECT * FROM V_MARKET_DOMINANCE;

-- Full history
SELECT * FROM CRYPTO_RAW ORDER BY loaded_at DESC;

-- Daily summary
SELECT * FROM CRYPTO_DAILY_SUMMARY ORDER BY price_date DESC;
```

---

## 🔒 Security Notes

- ✅ `.env` file is in `.gitignore` — never committed to GitHub
- ✅ AWS keys stored as environment variables only
- ✅ Snowflake password never hardcoded in source code
- ⚠️ Use IAM roles instead of access keys in production
- ⚠️ Use Snowflake key-pair authentication in production

---

## 🤝 Contributing

1. Fork the repository
2. Create your branch: `git checkout -b feature/my-feature`
3. Commit changes: `git commit -m "Add my feature"`
4. Push: `git push origin feature/my-feature`
5. Open a Pull Request

---

## 📄 License

MIT License — feel free to use and modify.

---

## 👨‍💻 Author

Built with by **Muhammad Shawail**

> DataVault Pipeline — From API to Warehouse, Fully Automated.
````
