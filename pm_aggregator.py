import time
import json
import requests
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

DATA_FILE = "price_history.jsonl"
POLL_INTERVAL_SEC = 30

def get_metals_spot():
    """Gold and Silver from metals.live (same as your aggregator)"""
    try:
        gold = requests.get("https://api.metals.live/v1/gold", timeout=5).json()
        silver = requests.get("https://api.metals.live/v1/silver", timeout=5).json()
        return {
            "gold_usd": float(gold[0]["price"]),
            "silver_usd": float(silver[0]["price"])
        }
    except Exception as e:
        print(f"Metals.live error: {e}")
        return {"gold_usd": 2650.0, "silver_usd": 73.18}

def get_coingecko_kvt_c1usd():
    """KVT and C1USD from CoinGecko"""
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "kinesis-velocity-token,tether",
            "vs_currencies": "usd"
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "kvt_usd": float(data.get("kinesis-velocity-token", {}).get("usd", 472.0)),
                "c1usd_usd": float(data.get("tether", {}).get("usd", 1.0))
            }
    except:
        pass
    return {"kvt_usd": 472.0, "c1usd_usd": 1.0}

def main_collector():
    print("🚀 Starting hybrid price collector (metals.live + CoinGecko)")
    while True:
        metals = get_metals_spot()
        kvt_data = get_coingecko_kvt_c1usd()
        now = datetime.now()

        entry = {
            "timestamp": now.isoformat(),
            "kvt_usd": kvt_data["kvt_usd"],
            "c1usd_usd": kvt_data["c1usd_usd"],
            "gold_usd": metals["gold_usd"],
            "silver_usd": metals["silver_usd"]
        }

        with open(DATA_FILE, "a") as f:
            f.write(json.dumps(entry) + "\n")

        print(f"[{now.strftime('%H:%M:%S')}] "
              f"KVT: ${kvt_data['kvt_usd']:.2f} | "
              f"C1USD: ${kvt_data['c1usd_usd']:.4f} | "
              f"Gold: ${metals['gold_usd']:.2f} | "
              f"Silver: ${metals['silver_usd']:.4f}")

        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    main_collector()
