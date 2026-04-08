import time
import json
import requests
from datetime import datetime, UTC

DATA_FILE = "kinesis_arbitrage_history.jsonl"
POLL_INTERVAL_SEC = 60  # Safe & sustainable for free CoinGecko tier

def get_coingecko_prices():
    """Single optimized call: KVT + KAU + KAG + USDT"""
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "kinesis-velocity-token,kinesis-gold,kinesis-silver,tether",
            "vs_currencies": "usd",
            "include_24hr_change": "true"
        }
        headers = {"User-Agent": "PM_Aggregator_TradingBot/2.3"}
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
        print(f"⚠️ CoinGecko error: {e}")
        return {"kvt_usd": 0, "kau_usd": 0, "kag_usd": 0, "usdt_usd": 1.0}


def main_collector():
    print("🚀 Kinesis Arbitrage Collector v2.3 – Metals API removed | KVT/KAU/KAG ratios only")
    print("Data is now clean for historical arbitrage backtest + forward MA signals")

    while True:
        cg = get_coingecko_prices()
        now = datetime.now(UTC)

        # Core trading ratios (what you'll use for MA and deviation signals)
        kvt_kau_ratio = round(cg["kvt_usd"] / cg["kau_usd"], 6) if cg["kau_usd"] > 0 else 0
        kvt_kag_ratio = round(cg["kvt_usd"] / cg["kag_usd"], 6) if cg["kag_usd"] > 0 else 0

        entry = {
            "timestamp": now.isoformat(),
            "kvt_usd": round(cg["kvt_usd"], 4),
            "kau_usd": round(cg["kau_usd"], 4),
            "kag_usd": round(cg["kag_usd"], 4),
            "kvt_kau_ratio": kvt_kau_ratio,
            "kvt_kag_ratio": kvt_kag_ratio,
            "usdt_usd": cg["usdt_usd"]
        }

        with open(DATA_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S UTC')}] "
              f"KVT ${cg['kvt_usd']:.2f} | "
              f"KAU ${cg['kau_usd']:.2f} | "
              f"KAG ${cg['kag_usd']:.2f} | "
              f"KVT/KAU {kvt_kau_ratio:.5f} | "
              f"KVT/KAG {kvt_kag_ratio:.5f}")

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main_collector()
