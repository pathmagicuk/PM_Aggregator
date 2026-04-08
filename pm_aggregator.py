import time
import json
import requests
from datetime import datetime
import pandas as pd

DATA_FILE = "kinesis_arbitrage_history.jsonl"
POLL_INTERVAL_SEC = 60  # Safer for free tiers

def get_coingecko_prices():
    """One call for KVT + KAU + KAG + USDT"""
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "kinesis-velocity-token,kinesis-gold,kinesis-silver,tether",
            "vs_currencies": "usd",
            "include_24hr_change": "true"
        }
        headers = {"User-Agent": "PM_Aggregator_TradingBot/2.1"}
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        return {
            "kvt_usd": float(data.get("kinesis-velocity-token", {}).get("usd", 0)),
            "kau_usd": float(data.get("kinesis-gold", {}).get("usd", 0)),
            "kag_usd": float(data.get("kinesis-silver", {}).get("usd", 0)),
            "usdt_usd": float(data.get("tether", {}).get("usd", 1.0))
        }
    except Exception as e:
        print(f"CoinGecko error: {e}")
        return {"kvt_usd": 0, "kau_usd": 0, "kag_usd": 0, "usdt_usd": 1.0}

def get_metals_spot():
    """TODO: Add your metalpriceapi key here or switch source"""
    try:
        # Replace with your key once registered
        api_key = "YOUR_METALPRICEAPI_KEY_HERE"  
        url = f"https://metalpriceapi.com/api/latest?base=USD&currencies=XAU,XAG&api_key={api_key}"
        resp = requests.get(url, timeout=8)
        data = resp.json()
        return {
            "gold_usd": float(data.get("rates", {}).get("XAU", 0)),
            "silver_usd": float(data.get("rates", {}).get("XAG", 0))
        }
    except Exception as e:
        print(f"Metals API error: {e} → Using CoinGecko KAU/KAG as proxy for now")
        return {"gold_usd": 0, "silver_usd": 0}

def main_collector():
    print("🚀 Kinesis Arbitrage Collector v2.1 (KVT/KAU/KAG ratios ready for MA signals)")
    print("Goal: Historical backtest + forward moving average arbitrage detection")
    
    while True:
        cg = get_coingecko_prices()
        metals = get_metals_spot()
        now = datetime.utcnow()

        kvt_kau = round(cg["kvt_usd"] / cg["kau_usd"], 6) if cg["kau_usd"] > 0 else 0
        kvt_kag = round(cg["kvt_usd"] / cg["kag_usd"], 6) if cg["kag_usd"] > 0 else 0

        entry = {
            "timestamp": now.isoformat(),
            "kvt_usd": cg["kvt_usd"],
            "kau_usd": cg["kau_usd"],
            "kag_usd": cg["kag_usd"],
            "gold_spot_usd": metals["gold_usd"],
            "silver_spot_usd": metals["silver_usd"],
            "kvt_kau_ratio": kvt_kau,
            "kvt_kag_ratio": kvt_kag,
            "usdt_usd": cg["usdt_usd"]
        }

        with open(DATA_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S UTC')}] "
              f"KVT ${cg['kvt_usd']:.2f} | "
              f"KAU ${cg['kau_usd']:.2f} | "
              f"KAG ${cg['kag_usd']:.2f} | "
              f"KVT/KAU {kvt_kau:.5f} | "
              f"KVT/KAG {kvt_kag:.5f}")

        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    main_collector()
