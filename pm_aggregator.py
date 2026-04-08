import time
import json
import requests
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

DATA_FILE = "kvt_price_history.jsonl"
POLL_INTERVAL_SEC = 30

def get_coingecko_data():
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "kinesis-velocity-token,gold,silver,tether",
            "vs_currencies": "usd",
            "include_24hr_change": "true"
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "timestamp": datetime.now().isoformat(),
                "kvt_usd": float(data.get("kinesis-velocity-token", {}).get("usd", 0)),
                "gold_usd": float(data.get("gold", {}).get("usd", 0)),
                "silver_usd": float(data.get("silver", {}).get("usd", 0)),
                "c1usd_usd": float(data.get("tether", {}).get("usd", 1.0)),   # C1USD is pegged to USD
            }
    except Exception as e:
        print(f"CoinGecko error: {e}")
    
    # Fallback values
    return {
        "timestamp": datetime.now().isoformat(),
        "kvt_usd": 472.0,
        "gold_usd": 2650.0,
        "silver_usd": 73.18,
        "c1usd_usd": 1.0,
    }

def main_collector():
    print("🚀 Starting KVT + C1USD + Spot Gold + Spot Silver collector")
    print("Polling every 30 seconds from CoinGecko")
    while True:
        data = get_coingecko_data()
        now = datetime.now()

        with open(DATA_FILE, "a") as f:
            f.write(json.dumps(data) + "\n")

        print(f"[{now.strftime('%H:%M:%S')}] "
              f"KVT: ${data['kvt_usd']:.2f} | "
              f"C1USD: ${data['c1usd_usd']:.4f} | "
              f"Gold: ${data['gold_usd']:.2f} | "
              f"Silver: ${data['silver_usd']:.4f}")

        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    main_collector()
