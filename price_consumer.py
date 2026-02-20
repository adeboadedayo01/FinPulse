from kafka import KafkaConsumer  # type: ignore
from dotenv import load_dotenv  # type: ignore
import os  # type: ignore
import json  # type: ignore
import psycopg2  # type: ignore
from psycopg2.extras import execute_values  # type: ignore
import time  # type: ignore
from datetime import datetime  # type: ignore

from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql.functions import lit  # type: ignore

load_dotenv()

os.environ["PYSPARK_PYTHON"] = "/opt/anaconda3/bin/python3.12"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/opt/anaconda3/bin/python3.12"

# ── Config ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "raw_prices"

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = os.getenv("POSTGRES_PORT", "5433")
PG_DB = os.getenv("POSTGRES_DB", "finpulse")
PG_USER = os.getenv("POSTGRES_USER", "finpulse")
PG_PASS = os.getenv("POSTGRES_PASSWORD")

# Azure Delta Lake config (from .env)
STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
STORAGE_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
CONTAINER = os.getenv("AZURE_CONTAINER_NAME", "delta-lake")

delta_path = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/prices"

# ── Spark Session with Delta Lake + Azure ADLS Gen2 JARs ─────────
# Match delta package to your PySpark version:
#   PySpark 3.5.x → io.delta:delta-spark_2.12:3.1.0  ✅ (default below)
#   PySpark 3.4.x → io.delta:delta-spark_2.12:2.4.0
#   PySpark 3.3.x → io.delta:delta-spark_2.12:2.3.0
# Check your version: python -c "import pyspark; print(pyspark.__version__)"

spark = SparkSession.builder \
    .appName("FinPulseDeltaSink") \
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.1.0,"
        "org.apache.hadoop:hadoop-azure:3.3.4,"
        "com.microsoft.azure:azure-storage:8.6.6"
    ) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config(f"spark.hadoop.fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", STORAGE_KEY) \
    .config("spark.hadoop.fs.azure.account.auth.type", "SharedKey") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")  # Reduce Spark noise in logs

print(f"✅ Spark session started — Delta Lake enabled")
print(f"Will append to Azure Delta Lake: {delta_path}")

if not PG_PASS:
    raise ValueError("POSTGRES_PASSWORD not found in .env")

if not STORAGE_ACCOUNT or not STORAGE_KEY:
    raise ValueError("AZURE_STORAGE_ACCOUNT_NAME or AZURE_STORAGE_ACCOUNT_KEY not found in .env")

# ── Postgres connection ───────────────────────────────────────────
try:
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS
    )
    cur = conn.cursor()
    print("✅ Connected to Postgres successfully")
except Exception as e:
    print(f"❌ Failed to connect to Postgres: {e}")
    exit(1)

# ── Kafka Consumer ────────────────────────────────────────────────
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='finpulse-price-consumer',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Starting consumer on topic '{TOPIC}'...")

batch = []
BATCH_SIZE = 5  # Insert every 5 messages


def write_batch_to_delta(batch_rows):
    """Write a batch of rows to Azure Delta Lake."""
    df = spark.createDataFrame(
        batch_rows,
        schema=["symbol", "price", "size", "timestamp_ms", "exchange"]
    )
    df = df.withColumn("ingested_at", lit(time.time()))

    df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(delta_path)

    print(f"  → ✅ Appended {len(batch_rows)} rows to Azure Delta Lake at {delta_path}")


try:
    for message in consumer:
        data = message.value

        symbol = data.get('symbol', 'unknown')
        price = data.get('price', None)

        if price is None:
            print(f"⚠️  Skipping invalid message (no price): {data}")
            continue

        print(f"Received: {symbol} @ ${price} @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        size = None
        if 'size' in data:
            size = float(data['size'])
        elif 'qty' in data:
            size = float(data['qty'])

        timestamp_ms = data.get('timestamp_ms')
        if timestamp_ms is None:
            timestamp_ms = int(time.time() * 1000)

        print(f"   → size: {size}, timestamp_ms: {timestamp_ms}")

        row = (
            symbol,
            float(price),
            size,
            timestamp_ms,
            'binance'
        )
        batch.append(row)

        if len(batch) >= BATCH_SIZE:
            # ── Insert into Postgres ───────────────────────────────
            try:
                execute_values(
                    cur,
                    """
                    INSERT INTO prices_raw 
                    (symbol, price, size, timestamp_ms, exchange)
                    VALUES %s
                    """,
                    batch
                )
                conn.commit()
                print(f"✅ Inserted {len(batch)} rows into Postgres (prices_raw)")
            except Exception as pg_err:
                print(f"❌ Postgres insert failed: {pg_err}")
                conn.rollback()

            # ── Write to Azure Delta Lake ──────────────────────────
            try:
                write_batch_to_delta(batch)
            except Exception as delta_err:
                print(f"❌ Delta Lake write failed: {delta_err}")

            batch = []

except KeyboardInterrupt:
    print("\n⏹  Stopping consumer...")

except Exception as e:
    print(f"❌ Consumer error: {e}")

finally:
    # ── Flush remaining batch ──────────────────────────────────────
    if batch:
        try:
            execute_values(
                cur,
                "INSERT INTO prices_raw (symbol, price, size, timestamp_ms, exchange) VALUES %s",
                batch
            )
            conn.commit()
            print(f"✅ Flushed final {len(batch)} rows to Postgres")
        except Exception as final_pg_err:
            print(f"❌ Final Postgres flush failed: {final_pg_err}")
            conn.rollback()

        try:
            write_batch_to_delta(batch)
        except Exception as final_delta_err:
            print(f"❌ Final Delta Lake flush failed: {final_delta_err}")

    # ── Cleanup ────────────────────────────────────────────────────
    cur.close()
    conn.close()
    consumer.close()
    spark.stop()
    print("✅ Consumer shutdown complete")