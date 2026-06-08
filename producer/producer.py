import pandas as pd
import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

df = pd.read_csv('data/raw/stock_prices_daily.csv')
print(f"Loaded {len(df)} rows. Starting to produce...")

for _, row in df.iterrows():
    message = {
        "date": str(row['Date']),
        "ticker": str(row['Ticker']),
        "company": str(row['Company_Name']),
        "sector": str(row['Sector']),
        "industry": str(row['Industry']),
        "open": float(row['Open']),
        "high": float(row['High']),
        "low": float(row['Low']),
        "close": float(row['Close']),
        "adj_close": float(row['Adj_Close']),
        "volume": int(row['Volume']),
        "event_time": pd.Timestamp.now().isoformat()
    }
    producer.send('stock-events', value=message)
    print(f"Sent: {message['ticker']} @ {message['close']}")
    time.sleep(0.005)

producer.flush()