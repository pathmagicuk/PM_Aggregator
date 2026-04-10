import pandas as pd
import json
from datetime import datetime
import numpy as np
import glob

DATA_FILES = ["kinesis_arbitrage_history.jsonl"]   # Add more filenames if you have multiple

def load_history():
    data = []
    for file in DATA_FILES:
        try:
            with open(file, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        entry = json.loads(line.strip())
                        # Handle both ISO with Z and without
                        ts_str = entry["timestamp"].replace("Z", "+00:00")
                        entry["timestamp"] = pd.to_datetime(ts_str)
                        data.append(entry)
        except Exception as e:
            print(f"Error reading {file}: {e}")
    
    df = pd.DataFrame(data)
    df = df.sort_values("timestamp").set_index("timestamp")
    df = df[~df.index.duplicated(keep='last')]  # Remove exact duplicate timestamps
    print(f"Loaded {len(df)} price ticks from {df.index[0]} to {df.index[-1]}")
    return df

def generate_signals(df, lookback_minutes=2880):  # Last 2 days by default
    # Focus on recent data for signals
    recent = df.last(f"{lookback_minutes}T").copy()
    
    # Rolling MAs on ratios (time-based)
    recent["kvt_kau_ma5"]  = recent["kvt_kau_ratio"].rolling("5T").mean()
    recent["kvt_kau_ma15"] = recent["kvt_kau_ratio"].rolling("15T").mean()
    recent["kvt_kau_ma60"] = recent["kvt_kau_ratio"].rolling("60T").mean()
    
    recent["kvt_kag_ma5"]  = recent["kvt_kag_ratio"].rolling("5T").mean()
    recent["kvt_kag_ma15"] = recent["kvt_kag_ratio"].rolling("15T").mean()
    recent["kvt_kag_ma60"] = recent["kvt_kag_ratio"].rolling("60T").mean()
    
    # Z-scores (deviation from 60-min MA)
    recent["kvt_kau_z"] = (recent["kvt_kau_ratio"] - recent["kvt_kau_ma60"]) / recent["kvt_kau_ratio"].rolling("60T").std()
    recent["kvt_kag_z"] = (recent["kvt_kag_ratio"] - recent["kvt_kag_ma60"]) / recent["kvt_kag_ratio"].rolling("60T").std()
    
    # Agentic Signals (tunable thresholds)
    recent["signal"] = "HOLD"
    recent.loc[(recent["kvt_kau_z"] < -1.5) & (recent["kvt_kag_z"] < -1.0), "signal"] = "STRONG BUY KVT"
    recent.loc[(recent["kvt_kau_z"] >  1.5) | (recent["kvt_kag_z"] >  1.5), "signal"] = "SELL KVT / ROTATE to KAU or KAG"
    
    # Summary of latest signals
    latest = recent.tail(20)
    print("\n=== LATEST SIGNALS (last 20 ticks) ===")
    print(latest[["kvt_usd", "kau_usd", "kag_usd", "c1usd_usd", 
                  "kvt_kau_ratio", "kvt_kau_ma60", "kvt_kau_z",
                  "kvt_kag_ratio", "kvt_kag_ma60", "kvt_kag_z", "signal"]].round(4))
    
    # Historical good windows summary
    buy_windows = recent[recent["signal"] == "STRONG BUY KVT"]
    sell_windows = recent[recent["signal"] == "SELL KVT / ROTATE to KAU or KAG"]
    
    print(f"\n=== HISTORICAL SUMMARY (last {lookback_minutes//60} hours) ===")
    print(f"Strong Buy KVT windows : {len(buy_windows)}")
    print(f"Sell/Rotate windows    : {len(sell_windows)}")
    if not buy_windows.empty:
        print(f"Cheapest KVT (lowest KVT/KAU): ${buy_windows['kvt_kau_ratio'].min():.4f} at {buy_windows['kvt_kau_ratio'].idxmin()}")
    
    # Save clean data for agents
    recent.to_csv("kinesis_signals_latest.csv")
    print("\nSaved detailed signals to kinesis_signals_latest.csv")
    
    return recent

if __name__ == "__main__":
    print("🚀 Kinesis Arbitrage Analyzer v1.0 – Rolling MA + Z-Score Signals")
    df = load_history()
    signals_df = generate_signals(df, lookback_minutes=2880)  # Change to 1440 for last 24h only
