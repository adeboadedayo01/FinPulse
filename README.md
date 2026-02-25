# FinPulse – Real‑Time Market Data Pipeline

FinPulse is an infrastructure stack for **real‑time market data ingestion and storage** using **Kafka**, **Postgres**, **Apache Spark**, and **Azure Delta Lake**, orchestrated alongside **Kestra**.

The main Python consumer (`price_consumer.py`) listens to a Kafka topic of raw trade/price events and writes them both to:

- **Postgres** table `prices_raw` for fast querying
- **Azure Data Lake (Delta Lake)** table `prices` for analytics and long‑term storage

---

## Architecture

- **Producer (external)** → publishes messages to Kafka topic `raw_prices`
- **Kafka** → `raw_prices` topic (ports **9092** internal / **29092** on host)
- **Price consumer (`price_consumer.py`)**
  - Consumes JSON messages from Kafka
  - Normalises fields (`symbol`, `price`, `size`, `timestamp_ms`, `exchange`)
  - Batches inserts into Postgres table `prices_raw`
  - Appends the same batch to an **Azure Delta Lake** table at `abfss://<container>@<account>.dfs.core.windows.net/prices`
- **Postgres** → stores raw tick data
- **Kestra** → workflow orchestration UI (optional, for scheduling/monitoring flows)

---

## Quick Start

### 1. Environment

1. Copy `.env.example` to `.env` and fill in your values:

   ```bash
   cp .env.example .env
   ```

2. Make sure you have:

   - Docker & Docker Compose
   - Python 3.12 (or compatible)
   - Java and Spark installed if you want to run `price_consumer.py` locally with PySpark

### 2. Start the infrastructure

From the project root:

```bash
docker compose up -d
```

This brings up:

- Kafka (KRaft mode, no ZooKeeper)
- Postgres (database `finpulse`)
- Kestra UI

Verify containers are healthy (e.g. with `docker ps`) before starting the consumer.

### 3. Configure environment variables

`price_consumer.py` relies on the following variables in `.env`:

- **Postgres**
  - `POSTGRES_HOST` (default: `localhost`)
  - `POSTGRES_PORT` (default: `5433`)
  - `POSTGRES_DB` (default: `finpulse`)
  - `POSTGRES_USER` (default: `finpulse`)
  - `POSTGRES_PASSWORD` (**required**)

- **Azure / Delta Lake**
  - `AZURE_STORAGE_ACCOUNT_NAME` (**required**)
  - `AZURE_STORAGE_ACCOUNT_KEY` (**required**)
  - `AZURE_CONTAINER_NAME` (default: `delta-lake`)

Optionally, update the PySpark Python path in `price_consumer.py` if your local Python is not at `/opt/anaconda3/bin/python3.12`.

### 4. Install Python dependencies

Create and activate a virtualenv if desired, then:

```bash
pip install -r requirements.txt  # or install kafka-python, psycopg2-binary, python-dotenv, pyspark, delta-spark
```

### 5. Run the price consumer

With Docker services running and `.env` configured:

```bash
python price_consumer.py
```

The consumer will:

- Connect to Postgres and ensure connectivity
- Connect to Kafka on `localhost:29092`
- Consume messages from topic `raw_prices`
- Batch every 5 messages into:
  - `prices_raw` table in Postgres
  - Delta Lake table `prices` in your Azure Data Lake

Stop it any time with `Ctrl+C`; remaining buffered messages are flushed on shutdown.

---

## Services & Ports

| Service  | Port(s)                  | Description                       |
|----------|--------------------------|-----------------------------------|
| Kafka    | 9092 (internal), 29092   | Message broker                    |
| Postgres | 5433                     | Database (`finpulse`)             |
| Kestra   | 8080                     | Workflow orchestration UI         |

---

## Implementation Notes

- Kafka consumer group id: `finpulse-price-consumer`
- Topic: `raw_prices`
- Batched inserts via `psycopg2.extras.execute_values` for efficiency
- Delta Lake writes use `mergeSchema=true` so schema evolution is tolerated
- Spark session is configured with:
  - `io.delta:delta-spark_2.12:3.1.0`
  - `org.apache.hadoop:hadoop-azure:3.3.4`
  - `com.microsoft.azure:azure-storage:8.6.6`

If you change your PySpark version, update the Delta Lake dependency in `price_consumer.py` accordingly.
