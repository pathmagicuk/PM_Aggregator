import pandas as pd
import json
from datetime import datetime
import glob

# Use the exact live file your v2.5 collector writes to
DATA_FILE = "kinesis_arbitrage_history.jsonl"

def load_history():
    print(f"🔍 Loading live data from: {DATA_FILE}")
    
    data = []
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            lines = f.readlines()
            print(f"Found {len(lines)} lines in the file")
            
            for i, line in enumerate(lines[:10]):   # Show first 10 for debugging
                if line.strip():
                    try:
                        entry = json.loads(line.strip())
                        if "timestamp" in entry:
                            ts_str = entry["timestamp"].replace("Z", "+00:00")
                            entry["timestamp"] = pd.to_datetime(ts_str)
                            data.append(entry)
                    except json.JSONDecodeError:
                        print(f"Line {i} is not valid JSON: {line[:100]}...")
        
        if not data:
            print("❌ No valid price entries found yet. Collector may still be starting.")
            print("   Please wait 2-3 minutes for the collector to write real data.")
            return pd.DataFrame()
            
        df = pd.DataFrame(data)
        print(f"✅ Loaded {len(df)} clean price records")
        print(f"Columns: {df.columns.tolist()}")
        print(f"Date range: {df['timestamp'].min()} → {df['timestamp'].max()}")
        
        df = df.sort_values("timestamp").set_index("timestamp")
        df = df[~df.index.duplicated(keep='last')]
        return df
        
    except FileNotFoundError:
        print(f"❌ File {DATA_FILE} not found. Make sure the collector is running in the same directory.")
        return pd.DataFrame()
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return pd.DataFrame()

def generate_signals(df):
    if df.empty:
        print("No data available for signals yet.")
        return None
    
    recent = df.last("2880T").copy()  # last 48 hours
    
    # Rolling MAs
    recent["kvt_kau_ma5"]  = recent["kvt_kau_ratio"].rolling("5T").mean()
    recent["kvt_kau_ma15"] = recent["kvt_kau_ratio"].rolling("15T").mean()
    recent["kvt_kau_ma60"] = recent["kvt_kau_ratio"].rolling("60T").mean()
    
    recent["kvt_kag_ma5"]  = recent["kvt_kag_ratio"].rolling("5T").mean()
    recent["kvt_kag_ma15"] = recent["kvt_kag_ratio"].rolling("15T").mean()
    recent["kvt_kag_ma60"] = recent["kvt_kag_ratio"].rolling("60T").mean()
    
    # Z-scores
    recent["kvt_kau_z"] = (recent["kvt_kau_ratio"] - recent["kvt_kau_ma60"]) / recent["kvt_kau_ratio"].rolling("60T").std()
    recent["kvt_kag_z"] = (recent["kvt_kag_ratio"] - recent["kvt_kag_ma60"]) / recent["kvt_kag_ratio"].rolling("60T").std()
    
    # Signals
    recent["signal"] = "HOLD"
    recent.loc[(recent["kvt_kau_z"] < -1.5) & (recent["kvt_kag_z"] < -1.0), "signal"] = "STRONG BUY KVT"
    recent.loc[(recent["kvt_kau_z"] >  1.5) | (recent["kvt_kag_z"] >  1.5), "signal"] = "SELL KVT / ROTATE to KAU or KAG"
    
    print("\n=== LATEST 15 SIGNALS ===")
    print(recent.tail(15)[["kvt_usd", "kau_usd", "kag_usd", "c1usd_usd",
                           "kvt_kau_ratio", "kvt_kau_ma60", "kvt_kau_z",
                           "kvt_kag_ratio", "kvt_kag_ma60", "kvt_kag_z", "signal"]].round(4))
    
    recent.to_csv("kinesis_signals_latest.csv")
    print("\n✅ Signals saved to kinesis_signals_latest.csv")
    return recent

if __name__ == "__main__":
    print("🚀 Kinesis Arbitrage Analyzer v1.1 – Improved debugging")
    df = load_history()
    if not df.empty:
        generate_signals(df)
    else:
        print("Run the collector for a few more minutes and try again.")
