import pandas as pd
import json
import os

# Try multiple possible locations (Railway common cases)
POSSIBLE_FILES = [
    "kinesis_arbitrage_history.jsonl",
    "/app/kinesis_arbitrage_history.jsonl",
    "/data/kinesis_arbitrage_history.jsonl"
]

def find_data_file():
    for path in POSSIBLE_FILES:
        if os.path.exists(path):
            print(f"✅ Found data file at: {path}")
            return path
    print("❌ Could not find kinesis_arbitrage_history.jsonl")
    print("Current directory:", os.getcwd())
    print("Files present:", os.listdir("."))
    return None

def load_history():
    data_file = find_data_file()
    if not data_file:
        return pd.DataFrame()

    print(f"Loading from {data_file}...")
    data = []
    try:
        with open(data_file, "r", encoding="utf-8") as f:
            lines = f.readlines()
            print(f"Found {len(lines)} lines in file")

        for line in lines:
            if line.strip():
                entry = json.loads(line.strip())
                ts = pd.to_datetime(entry["timestamp"].replace("Z", "+00:00"))
                entry["timestamp"] = ts
                data.append(entry)

        if not data:
            print("No valid price records yet. Collector needs more time.")
            return pd.DataFrame()

        df = pd.DataFrame(data).set_index("timestamp")
        df = df.sort_index()
        print(f"✅ Loaded {len(df)} clean records. Latest: {df.index[-1]}")
        return df

    except Exception as e:
        print(f"Error loading file: {e}")
        return pd.DataFrame()

def generate_signals(df):
    if df.empty:
        return

    recent = df.last("48H").copy()

    recent["kvt_kau_ma60"] = recent["kvt_kau_ratio"].rolling("60T").mean()
    recent["kvt_kag_ma60"] = recent["kvt_kag_ratio"].rolling("60T").mean()

    recent["kvt_kau_z"] = (recent["kvt_kau_ratio"] - recent["kvt_kau_ma60"]) / recent["kvt_kau_ratio"].rolling("60T").std()
    recent["kvt_kag_z"] = (recent["kvt_kag_ratio"] - recent["kvt_kag_ma60"]) / recent["kvt_kag_ratio"].rolling("60T").std()

    recent["signal"] = "HOLD"
    recent.loc[(recent["kvt_kau_z"] < -1.5) & (recent["kvt_kag_z"] < -1.0), "signal"] = "STRONG BUY KVT"
    recent.loc[(recent["kvt_kau_z"] > 1.5) | (recent["kvt_kag_z"] > 1.5), "signal"] = "SELL KVT / ROTATE to KAU or KAG"

    print("\n=== LATEST 10 SIGNALS ===")
    cols = ["kvt_usd", "kau_usd", "kag_usd", "c1usd_usd", "kvt_kau_ratio", "kvt_kau_ma60", "kvt_kau_z", 
            "kvt_kag_ratio", "kvt_kag_ma60", "kvt_kag_z", "signal"]
    print(recent.tail(10)[cols].round(4))

if __name__ == "__main__":
    print("🚀 Kinesis Arbitrage Analyzer v1.2 – Railway-friendly")
    df = load_history()
    generate_signals(df)
