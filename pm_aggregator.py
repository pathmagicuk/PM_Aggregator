import time
import json
import requests
from datetime import datetime, UTC

DATA_FILE = "kinesis_arbitrage_history.jsonl"
POLL_INTERVAL_SEC = 60

def get_coingecko_prices():
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "kinesis-velocity-token,kinesis-gold,kinesis-silver,tether",
            "vs_currencies": "usd",
            "include_24hr_change": "true"
        }
        headers = {"User-Agent": "PM_Aggregator_TradingBot/2.5"}
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        return {
            "kvt_usd": float(data.get("kinesis-velocity-token", {}).get("usd", 0)),
            "kau_usd": float(data.get("kinesis-gold", {}).get("usd", 0)),
            "kag_usd": float(data.get("kinesis-silver", {}).get("usd", 0)),
            "c1usd_usd": float(data.get("tether", {}).get("usd", 1.0))
        }
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print("⚠️ CoinGecko rate limit – backing off 5s")
            time.sleep(5)
        else:
            print(f"⚠️ CoinGecko error: {e}")
        return {"kvt_usd": 0, "kau_usd": 0, "kag_usd": 0, "c1usd_usd": 1.0}
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")
        return {"kvt_usd": 0, "kau_usd": 0, "kag_usd": 0, "c1usd_usd": 1.0}


def main_collector():
    print("🚀 Kinesis Arbitrage Collector v2.5 – C1USD stable reference | Rate-limit safe")
    print("Optimized for KVT vs KAU/KAG ratio MA signals + future KMS orderbook overlay")

    while True:
        cg = get_coingecko_prices()
        now = datetime.now(UTC)

        kvt_kau_ratio = round(cg["kvt_usd"] / cg["kau_usd"], 6) if cg["kau_usd"] > 0 else 0
        kvt_kag_ratio = round(cg["kvt_usd"] / cg["kag_usd"], 6) if cg["kag_usd"] > 0 else 0

        entry = {
            "timestamp": now.isoformat(),
            "kvt_usd": round(cg["kvt_usd"], 4),
            "kau_usd": round(cg["kau_usd"], 4),
            "kag_usd": round(cg["kag_usd"], 4),
            "c1usd_usd": round(cg["c1usd_usd"], 4),
            "kvt_kau_ratio": kvt_kau_ratio,
            "kvt_kag_ratio": kvt_kag_ratio,
        }

        with open(DATA_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

        print(f"[{now.strftime('%Y-%m-%d %H:%M:%S UTC')}] "
              f"KVT ${cg['kvt_usd']:.2f} USD | "
              f"KAU ${cg['kau_usd']:.2f} USD | "
              f"KAG ${cg['kag_usd']:.2f} USD | "
              f"C1USD ${cg['c1usd_usd']:.4f} | "
              f"KVT/KAU {kvt_kau_ratio:.5f} | "
              f"KVT/KAG {kvt_kag_ratio:.5f}")

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main_collector()
