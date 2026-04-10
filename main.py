import time
import json
import requests
import pandas as pd
import os
from datetime import datetime, UTC

DATA_FILE = "kinesis_arbitrage_history.jsonl"
POLL_INTERVAL_SEC = 60
ANALYZER_INTERVAL = 120   # every 2 minutes

def get_coingecko_prices():
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "kinesis-velocity-token,kinesis-gold,kinesis-silver,tether",
            "vs_currencies": "usd",
            "include_24hr_change": "true"
        }
        headers = {"User-Agent": "PM_Aggregator_TradingBot/2.6"}
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        return {
            "kvt_usd": float(data.get("kinesis-velocity-token", {}).get("usd", 0)),
            "kau_usd": float(data.get("kinesis-gold", {}).get("usd", 0)),
            "kag_usd": float(data.get("kinesis-silver", {}).get("usd", 0)),
            "c1usd_usd": float(data.get("tether", {}).get("usd", 1.0))
        }
    except Exception as e:
        if "429" in str(e):
            print("⚠️ Rate limit - backing off")
            time.sleep(10)
        else:
            print(f"⚠️ CoinGecko error: {e}")
        return None

def run_analyzer():
    try:
        if not os.path.exists(DATA_FILE) or os.path.getsize(DATA_FILE) == 0:
            print("No data file yet...")
            return

        data = []
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    entry = json.loads(line.strip())
                    ts = pd.to_datetime(entry["timestamp"].replace("Z", "+00:00"))
                    entry["timestamp"] = ts
                    data.append(entry)

        print(f"Analyzer: {len(data)} total records")

        if len(data) < 20:
            print(f"Need more data ({len(data)}/20) for reliable MA signals")
            return

        df = pd.DataFrame(data)
        df = df.set_index("timestamp")
        df = df.sort_index()

        # Use .last() correctly on DatetimeIndex
        recent = df.last("48H")

        recent["kvt_kau_ma60"] = recent["kvt_kau_ratio"].rolling("60T").mean()
        recent["kvt_kag_ma60"] = recent["kvt_kag_ratio"].rolling("60T").mean()

        recent["kvt_kau_z"] = (recent["kvt_kau_ratio"] - recent["kvt_kau_ma60"]) / recent["kvt_kau_ratio"].rolling("60T").std()
        recent["kvt_kag_z"] = (recent["kvt_kag_ratio"] - recent["kvt_kag_ma60"]) / recent["kvt_kag_ratio"].rolling("60T").std()

        recent["signal"] = "HOLD"
        recent.loc[(recent["kvt_kau_z"] < -1.5) & (recent["kvt_kag_z"] < -1.0), "signal"] = "STRONG BUY KVT"
        recent.loc[(recent["kvt_kau_z"] > 1.5) | (recent["kvt_kag_z"] > 1.5), "signal"] = "SELL KVT / ROTATE to KAU or KAG"

        print("\n=== LATEST ARBITRAGE SIGNALS ===")
        print(recent.tail(10)[["kvt_usd", "kau_usd", "kag_usd", "c1usd_usd",
                               "kvt_kau_ratio", "kvt_kau_ma60", "kvt_kau_z",
                               "kvt_kag_ratio", "kvt_kag_ma60", "kvt_kag_z", "signal"]].round(4))
    except Exception as e:
        print(f"Analyzer error: {e}")

# ====================== MAIN ======================
print("🚀 Combined Kinesis Collector + Analyzer v1.2")
print(f"Data file: {os.path.abspath(DATA_FILE)}")

# Start fresh
with open(DATA_FILE, "w", encoding="utf-8") as f:
    f.write("")

last_analyzer = 0

while True:
    prices = get_coingecko_prices()
    now = datetime.now(UTC)

    if prices and all(v > 0 for v in [prices.get(k, 0) for k in ["kvt_usd", "kau_usd", "kag_usd"]]):
        kvt_kau = round(prices["kvt_usd"] / prices["kau_usd"], 6)
        kvt_kag = round(prices["kvt_usd"] / prices["kag_usd"], 6)

        entry = {
            "timestamp": now.isoformat(),
            "kvt_usd": round(prices["kvt_usd"], 4),
            "kau_usd": round(prices["kau_usd"], 4),
            "kag_usd": round(prices["kag_usd"], 4),
            "c1usd_usd": round(prices["c1usd_usd"], 4),
            "kvt_kau_ratio": kvt_kau,
            "kvt_kag_ratio": kvt_kag,
        }

        with open(DATA_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

        print(f"[{now.strftime('%H:%M:%S UTC')}] KVT ${prices['kvt_usd']:.2f} | KAU ${prices['kau_usd']:.2f} | KAG ${prices['kag_usd']:.2f} | KVT/KAU {kvt_kau:.5f} | KVT/KAG {kvt_kag:.5f}")
    else:
        print(f"[{now.strftime('%H:%M:%S UTC')}] Skipped (rate limit)")

    # Run analyzer every 2 minutes
    if time.time() - last_analyzer > ANALYZER_INTERVAL:
        print("\n--- Running Analyzer ---")
        run_analyzer()
        last_analyzer = time.time()

    time.sleep(POLL_INTERVAL_SEC)
