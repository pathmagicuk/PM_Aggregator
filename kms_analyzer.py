import pandas as pd
import json

DATA_FILE = "kinesis_arbitrage_history.jsonl"

def load_history():
    print(f"Looking for {DATA_FILE}...")
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
        print(f"Found {len(lines)} lines")

        data = []
        for line in lines:
            if line.strip():
                entry = json.loads(line.strip())
                ts = pd.to_datetime(entry["timestamp"].replace("Z", "+00:00"))
                entry["timestamp"] = ts
                data.append(entry)

        if not data:
            print("No price data yet. Wait for collector to write some lines.")
            return pd.DataFrame()

        df = pd.DataFrame(data).set_index("timestamp")
        df = df.sort_index()
        print(f"Loaded {len(df)} records from {df.index[0]} to {df.index[-1]}")
        return df

    except FileNotFoundError:
        print(f"❌ {DATA_FILE} not found. Make sure collector is running first.")
        return pd.DataFrame()
    except Exception as e:
        print(f"Error: {e}")
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
    recent.loc[(recent["kvt_kau_z"] > 1.5) | (recent["kvt_kag_z"] > 1.5), "signal"] = "SELL KVT / ROTATE"

    print("\n=== LATEST SIGNALS ===")
    print(recent.tail(10)[["kvt_usd", "kau_usd", "kag_usd", "c1usd_usd", 
                           "kvt_kau_ratio", "kvt_kau_ma60", "kvt_kau_z", 
                           "kvt_kag_ratio", "kvt_kag_ma60", "kvt_kag_z", "signal"]].round(4))

if __name__ == "__main__":
    df = load_history()
    generate_signals(df)
