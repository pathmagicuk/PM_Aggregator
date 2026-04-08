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
    """Gold and Silver from a more reliable free API"""
    try:
        # Free metalpriceapi.com (no key needed for basic use)
        gold = requests.get("https://metalpriceapi.com/api/latest?base=USD&currencies=XAU", timeout=8).json()
        silver = requests.get("https://metalpriceapi.com/api/latest?base=USD&currencies=XAG", timeout=8).json()
        
        gold_price = float(gold.get("rates", {}).get("XAU", 2650))
        silver_price = float(silver.get("rates", {}).get("XAG", 73.18))
        
        return {
            "gold_usd": gold_price,
            "silver_usd": silver_price
        }
    except Exception as e:
        print(f"Metalpriceapi error: {e}")
        return {"gold_usd": 2650.0, "silver_usd": 73.18}

def get_coingecko_kvt_c1usd():
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
    print("🚀 Starting hybrid price collector (CoinGecko + metalpriceapi)")
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
