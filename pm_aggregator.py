import time
import json
import requests
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

DATA_FILE = "kvt_price_history.jsonl"
POLL_INTERVAL_SEC = 30

def get_coingecko_data():
    try:
        url = "https://api.coingecko.com/api/v3/simple/price"
        params = {
            "ids": "kinesis-velocity-token,tether,gold,silver",
            "vs_currencies": "usd"
        }
        resp = requests.get(url, params=params, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return {
                "timestamp": datetime.now().isoformat(),
                "kvt_usd": float(data.get("kinesis-velocity-token", {}).get("usd", 0)),
                "c1usd_usd": float(data.get("tether", {}).get("usd", 1.0)),
                "gold_usd": float(data.get("gold", {}).get("usd", 0)),
                "silver_usd": float(data.get("silver", {}).get("usd", 0)),
            }
    except Exception as e:
        print(f"CoinGecko error: {e}")
    
    # Fallback
    return {
        "timestamp": datetime.now().isoformat(),
        "kvt_usd": 472.0,
        "c1usd_usd": 1.0,
        "gold_usd": 2650.0,
        "silver_usd": 73.18,
    }

def main_collector():
    print("🚀 Starting KVT + C1USD + Gold + Silver collector (CoinGecko only)")
    while True:
        data = get_coingecko_data()
        now = datetime.now()

        with open(DATA_FILE, "a") as f:
            f.write(json.dumps(data) + "\n")

        print(f"[{now.strftime('%H:%M:%S')}] "
              f"KVT: ${data['kvt_usd']:.2f} | "
              f"C1USD: ${data['c1usd_usd']:.4f} | "
              f"Gold: ${data['gold_usd']:.2f} | "
              f"Silver: ${data['silver_usd']:.4f}")

        time.sleep(POLL_INTERVAL_SEC)

def analyze_and_plot():
    if not os.path.exists(DATA_FILE):
        print("No data file found yet.")
        return

    data = []
    with open(DATA_FILE, "r") as f:
        for line in f:
            try:
                data.append(json.loads(line))
            except:
                pass

    if len(data) < 20:
        print(f"Only {len(data)} records so far. Let it run longer for good waves.")
        return

    df = pd.DataFrame(data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df = df.sort_values("timestamp")

    piv = df.set_index("timestamp")[["kvt_usd", "c1usd_usd", "gold_usd", "silver_usd"]]
    piv = piv.resample("5T").last().ffill()

    for col in ["kvt_usd", "gold_usd", "silver_usd"]:
        piv[f"{col}_ema15"] = piv[col].ewm(span=15, adjust=False).mean()

    aligned = piv.copy()

    # Relative cost of 1 KVT in each route
    aligned["via_c1usd"] = aligned["kvt_usd"] / aligned["c1usd_usd"]
    aligned["via_gold"] = aligned["kvt_usd"] / (aligned["gold_usd"] / 31.1035)   # grams to ounces
    aligned["via_silver"] = aligned["kvt_usd"] / aligned["silver_usd"]

    avg_route = (aligned["via_c1usd"] + aligned["via_gold"] + aligned["via_silver"]) / 3

    cheaper_c1usd = aligned["via_c1usd"] < avg_route * (1 - 0.005)
    cheaper_gold = aligned["via_gold"] < avg_route * (1 - 0.005)
    cheaper_silver = aligned["via_silver"] < avg_route * (1 - 0.005)

    plt.figure(figsize=(14, 8))
    plt.plot(aligned.index, aligned["via_c1usd"], label="KVT via C1USD", linewidth=2)
    plt.plot(aligned.index, aligned["via_gold"], label="KVT via Gold", linewidth=2)
    plt.plot(aligned.index, aligned["via_silver"], label="KVT via Silver", linewidth=2)

    plt.fill_between(aligned.index, aligned["via_gold"], aligned["via_gold"]*0.99,
                     where=cheaper_gold, color='gold', alpha=0.15, label="Gold route cheaper")
    plt.fill_between(aligned.index, aligned["via_silver"], aligned["via_silver"]*0.99,
                     where=cheaper_silver, color='silver', alpha=0.15, label="Silver route cheaper")
    plt.fill_between(aligned.index, aligned["via_c1usd"], aligned["via_c1usd"]*0.99,
                     where=cheaper_c1usd, color='blue', alpha=0.15, label="C1USD route cheaper")

    plt.title("KVT Relative Cost Waves - Cheaper Accumulation Windows")
    plt.xlabel("Time (UTC)")
    plt.ylabel("Relative Cost of 1 KVT")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

    print(f"\nAnalyzed {len(data)} records.")

if __name__ == "__main__":
    main_collector()
    # analyze_and_plot()   # Uncomment this to generate the plot from saved data
