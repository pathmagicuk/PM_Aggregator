import os
import time
import json
import requests
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

DATA_FILE = "kvt_price_history.jsonl"
POLL_INTERVAL_SEC = 30

def get_coingecko_kvt():
    """Get KVT prices from CoinGecko (main pairs)"""
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "kinesis-velocity-token",
            "vs_currencies": "usd",
            "include_market_cap": "false",
            "include_24hr_vol": "true",
            "include_24hr_change": "true"
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            kvt_usd = data.get("kinesis-velocity-token", {}).get("usd", 0)
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "kvt_usd": kvt_usd,
                "kvt_24h_change": data.get("kinesis-velocity-token", {}).get("usd_24h_change", 0)
            }
    except Exception as e:
        print(f"CoinGecko error: {e}")
    return {"timestamp": datetime.utcnow().isoformat(), "kvt_usd": 450.0, "kvt_24h_change": 0}

def get_metals_spot():
    """Gold and Silver spot from metals.live"""
    try:
        gold = requests.get("https://api.metals.live/v1/gold", timeout=5).json()
        silver = requests.get("https://api.metals.live/v1/silver", timeout=5).json()
        return {
            "gold_spot": float(gold[0]["price"]),
            "silver_spot": float(silver[0]["price"])
        }
    except:
        return {"gold_spot": 2650.0, "silver_spot": 73.18}

def main_collector():
    print("🚀 Starting KVT + Spot collector (CoinGecko + metals.live)")
    while True:
        kvt = get_coingecko_kvt()
        metals = get_metals_spot()

        entry = {
            "timestamp": kvt["timestamp"],
            "kvt_usd": kvt["kvt_usd"],
            "gold_spot": metals["gold_spot"],
            "silver_spot": metals["silver_spot"]
        }

        with open(DATA_FILE, "a") as f:
            f.write(json.dumps(entry) + "\n")

        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] "
              f"KVT/USD: ${kvt['kvt_usd']:.2f} | "
              f"Gold: ${metals['gold_spot']:.2f} | "
              f"Silver: ${metals['silver_spot']:.4f}")

        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    main_collector()
