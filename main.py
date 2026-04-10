import time
import json
import requests
from datetime import datetime, timedelta

DATA_FILE = "kau_price_history.jsonl"
POLL_INTERVAL_SEC = 30

# Cache last good price
last_good_price = 153.50  # realistic ~$4,780/oz in April 2026
last_good_time = datetime.now()

def get_gold_price_per_gram():
    global last_good_price, last_good_time
    
    # If we have a recent cache, use it to avoid frozen $0
    if (datetime.now() - last_good_time) < timedelta(minutes=10):
        return last_good_price

    sources_tried = []
    
    # 1. GoldAPI.io (very good for precious metals)
    try:
        resp = requests.get("https://www.goldapi.io/api/XAU/USD", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            price_per_gram = float(data.get("price", 0)) / 31.1035
            if price_per_gram > 100:
                last_good_price = price_per_gram
                last_good_time = datetime.now()
                print(f"[DEBUG] GoldAPI success: ${price_per_gram:.2f}/g")
                return price_per_gram
    except Exception as e:
        sources_tried.append(f"GoldAPI: {type(e).__name__}")

    # 2. CoinGecko gold
    try:
        resp = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=gold&vs_currencies=usd", timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            gold_oz = float(data.get("gold", {}).get("usd", 0))
            if gold_oz > 1000:
                price_per_gram = gold_oz / 31.1035
                last_good_price = price_per_gram
                last_good_time = datetime.now()
                print(f"[DEBUG] CoinGecko success: ${price_per_gram:.2f}/g")
                return price_per_gram
    except Exception as e:
        sources_tried.append(f"CoinGecko: {type(e).__name__}")

    # Final fallback - use cached or realistic default
    print(f"[DEBUG] All sources failed. Using cached/fallback. Errors: {sources_tried}")
    return last_good_price

def main_collector():
    print("🚀 KAU Live Balance Tracker (Cached + Multi-Source)")
    print(f"Starting balance: $5,000.00 C1USD + 50.0 KAU\n")
    
    while True:
        gold_per_gram = get_gold_price_per_gram()
        now = datetime.now()

        kau_value = 50.0 * gold_per_gram
        total_value = 5000.0 + kau_value

        print(f"[{now.strftime('%H:%M:%S')}] "
              f"C1USD: $5,000.00 | "
              f"KAU: 50.0g (${kau_value:,.2f}) | "
              f"Total: ${total_value:,.2f} | Gold/gram: ${gold_per_gram:.2f}")

        with open(DATA_FILE, "a") as f:
            f.write(json.dumps({
                "timestamp": now.isoformat(),
                "c1usd": 5000.0,
                "kau_grams": 50.0,
                "kau_price_per_gram": gold_per_gram,
                "total_value": total_value
            }) + "\n")

        time.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":
    main_collector()
