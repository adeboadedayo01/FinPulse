import requests #type: ignore
import json #type: ignore
import time #type: ignore
from kafka import KafkaProducer #type: ignore
from dotenv import load_dotenv #type: ignore
import os #type: ignore
load_dotenv()

# ── Config ────────────────────────────────────────────────────────
SYMBOL = "BTCUSDT"               # Binance symbol (change to ETHUSDT, BNBUSDT etc. if desired)
INTERVAL_SECONDS = 30            

KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "raw_prices"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    acks=1
)

print(f"Starting Binance trades → Kafka producer for {SYMBOL}...")

while True:
    try:
        # ── Fetch latest trade (limit=1 gives most recent trade) ───────────────
        url = f"https://api.binance.com/api/v3/trades?symbol={SYMBOL}&limit=1"
        response = requests.get(url, timeout=10)
        response.raise_for_status()

        trades = response.json()

        if not trades:
            print("No trades returned from Binance")
            time.sleep(INTERVAL_SECONDS)
            continue

        latest = trades[0]

        # Build message with real fields from trade
        message = {
            "symbol": SYMBOL,
            "price": float(latest["price"]),
            "size": float(latest["qty"]),          # ← qty = trade size/volume
            "timestamp_ms": latest["time"],        # ← Binance trade timestamp (ms)
            "buyer_order_id": latest.get("b"),    
            "seller_order_id": latest.get("a"),    
            "is_buyer_maker": latest.get("m"),    
            "source": "binance",
            "ingested_at": time.time()
        }

        producer.send(TOPIC, value=message)
        producer.flush()

        print(f"✅ Sent: {message['symbol']} @ ${message['price']}  size: {message['size']}  @ {time.ctime(message['timestamp_ms']/1000)}")

    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error: {http_err}")
        if response.status_code == 429:
            print("Rate limit hit — waiting longer...")
            time.sleep(60)
        else:
            time.sleep(INTERVAL_SECONDS)

    except Exception as e:
        print(f"Unexpected error: {e}")
        time.sleep(INTERVAL_SECONDS)

    time.sleep(INTERVAL_SECONDS)