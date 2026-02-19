from kafka import KafkaConsumer #type: ignore   
from dotenv import load_dotenv #type: ignore
import os #type: ignore
import json #type: ignore
import psycopg2 #type: ignore
from psycopg2.extras import execute_values #type: ignore
import time #type: ignore
from datetime import datetime #type: ignore  

load_dotenv()

# ── Config ────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "raw_prices"

PG_HOST = "localhost"
PG_PORT = "5433"
PG_DB = os.getenv("POSTGRES_DB", "finpulse")
PG_USER = os.getenv("POSTGRES_USER", "finpulse")
PG_PASS = os.getenv("POSTGRES_PASSWORD")

if not PG_PASS:
    raise ValueError("POSTGRES_PASSWORD not found in .env")

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
    print("Connected to Postgres successfully")
except Exception as e:
    print(f"Failed to connect to Postgres: {e}")
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

batch = []           # Buffer for batch insert
BATCH_SIZE = 5      # Insert every 5 messages (adjust as needed)

try:
    for message in consumer:
        data = message.value

        symbol = data.get('symbol', 'unknown')
        price = data.get('price', None)

        if price is None:
            print(f"Skipping invalid message (no price): {data}")
            continue

        print(f"Received: {symbol} @ ${price} @ {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        # Try to get real values from the message, fallback gracefully
        size = None
        if 'size' in data:
            size = float(data['size'])
        elif 'qty' in data:               # Binance trades endpoint uses "qty"
            size = float(data['qty'])

        timestamp_ms = data.get('timestamp_ms')
        if timestamp_ms is None:
            timestamp_ms = int(time.time() * 1000)  # fallback to now

            # ── DEBUG ──────────────────────────────────────────────────────
        print(f"   → size from message: {size}, timestamp_ms: {timestamp_ms}")

        row = (
            symbol,
            float(price),
            size,                     
            timestamp_ms,
            'binance'
        )
        batch.append(row)

        # Batch insert when enough messages collected
        if len(batch) >= BATCH_SIZE:
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
                print(f"Inserted {len(batch)} rows into prices_raw")
                batch = []
            except Exception as insert_err:
                print(f"Insert failed: {insert_err}")
                conn.rollback()  # rollback on error

except KeyboardInterrupt:
    print("\nStopping consumer...")

except Exception as e:
    print(f"Consumer error: {e}")

finally:
    # Flush any remaining batch
    if batch:
        try:
            execute_values(
                cur,
                "INSERT INTO prices_raw (symbol, price, size, timestamp_ms, exchange) VALUES %s",
                batch
            )
            conn.commit()
            print(f"Flushed final batch: {len(batch)} rows")
        except Exception as final_err:
            print(f"Final batch insert failed: {final_err}")
            conn.rollback()

    # Clean up
    cur.close()
    conn.close()
    consumer.close()
    print("Consumer shutdown complete")