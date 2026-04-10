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
        headers = {"User-Agent": "PM_Aggregator_TradingBot/2.6"}
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        prices = {
            "kvt_usd": float(data.get("kinesis-velocity-token", {}).get("usd", 0)),
            "kau_usd": float(data.get("kinesis-gold", {}).get("usd", 0)),
            "kag_usd": float(data.get("kinesis-silver", {}).get("usd", 0)),
            "c1usd_usd": float(data.get("tether", {}).get("usd", 1.0))
        }
        return prices

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 429:
            print("⚠️ CoinGecko 429 Rate Limit - backing off 8 seconds")
            time.sleep(8)
        else:
            print(f"⚠️ HTTP Error: {e}")
        return None
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")
        return None


def main():
    print("🚀 Starting Kinesis Collector v2.6 (Rate-limit safe)")
    print(f"Writing to: {DATA_FILE}")

    # Start fresh file
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        f.write("")

    while True:
        prices = get_coingecko_prices()
        now = datetime.now(UTC)

        if prices and prices["kvt_usd"] > 0 and prices["kau_usd"] > 0 and prices["kag_usd"] > 0:
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

            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S UTC')}] "
                  f"KVT ${prices['kvt_usd']:.2f} USD | "
                  f"KAU ${prices['kau_usd']:.2f} USD | "
                  f"KAG ${prices['kag_usd']:.2f} USD | "
                  f"C1USD ${prices['c1usd_usd']:.4f} | "
                  f"KVT/KAU {kvt_kau:.5f} | "
                  f"KVT/KAG {kvt_kag:.5f}")
        else:
            print(f"[{now.strftime('%Y-%m-%d %H:%M:%S UTC')}] Skipped - no valid prices (rate limit or error)")

        time.sleep(POLL_INTERVAL_SEC)


if __name__ == "__main__":
    main()
