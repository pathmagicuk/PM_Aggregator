import time
import json
import requests
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

DATA_FILE = "kvt_price_history.jsonl"
POLL_INTERVAL_SEC = 30

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

def get_metals_spot():
    """Gold and Silver from metals.live (more stable for metals)"""
    try:
        gold = requests.get("https://api.metals.live/v1/gold", timeout=8).json()
        silver = requests.get("https://api.metals.live/v1/silver", timeout=8).json()
        return {
            "gold_usd": float(gold[0]["price"]),
            "silver_usd": float(silver[0]["price"])
        }
    except Exception as e:
        print(f"Metals.live error: {e}")
        return {"gold_usd": 2650.0, "silver_usd": 73.18}

def main_collector():
    print("🚀 Starting hybrid KVT collector (CoinGecko + metals.live)")
    print("Collecting data every 30 seconds for 15-minute averages")
    
    while True:
        kvt_data = get_coingecko_kvt_c1usd()
        metals = get_metals_spot()
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

def analyze_and_plot():
    if not os.path.exists(DATA_FILE):
        print("No data collected yet. Let the collector run for a while.")
        return

    data = []
    with open(DATA_FILE, "r") as f:
        for line in f:
            try:
                data.append(json.loads(line))
            except:
                pass

    if not data:
        print("No valid data found yet.")
        return

    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")

    # Resample to 5-minute bars
    piv = df.pivot_table(index="timestamp", columns=None, values=["kvt_usd", "c1usd_usd", "gold_usd", "silver_usd"], aggfunc="last")
    piv = piv.resample("5T").last().ffill()

    # Calculate EMAs
    for col in ["kvt_usd", "gold_usd", "silver_usd"]:
        piv[f"{col}_ema15"] = piv[col].ewm(span=15, adjust=False).mean()
        piv[f"{col}_ema60"] = piv[col].ewm(span=60, adjust=False).mean()

    # Relative cost of KVT in each route (normalized)
    aligned = piv.copy()
    aligned["kvt_via_c1usd"] = aligned["kvt_usd"] / aligned["c1usd_usd"]   # KVT per USD
    aligned["kvt_via_gold"] = aligned["kvt_usd"] / (aligned["gold_usd"] / 31.1035)  # adjust for grams vs ounces if needed
    aligned["kvt_via_silver"] = aligned["kvt_usd"] / aligned["silver_usd"]

    # Simple average for comparison
    avg_route = (aligned["kvt_via_c1usd"] + aligned["kvt_via_gold"] + aligned["kvt_via_silver"]) / 3

    # Highlight cheaper routes
    cheaper_c1usd = aligned["kvt_via_c1usd"] < avg_route * (1 - 0.005)
    cheaper_gold = aligned["kvt_via_gold"] < avg_route * (1 - 0.005)
    cheaper_silver = aligned["kvt_via_silver"] < avg_route * (1 - 0.005)

    # Plot the waves
    plt.figure(figsize=(14, 8))
    plt.plot(aligned.index, aligned["kvt_via_c1usd"], label="KVT via C1USD", linewidth=2)
    plt.plot(aligned.index, aligned["kvt_via_gold"], label="
