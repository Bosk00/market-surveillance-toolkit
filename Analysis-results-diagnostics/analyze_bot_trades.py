import pandas as pd
import numpy as np
import json
from datetime import datetime

# -----------------------------
# File paths
# -----------------------------
MOMENTUM_CSV = "momentum_data.csv"
TRADES_XLSX = "trades_analysis.xlsx"
CACHED_JSON = "results_cache.json"   # <-- using results_cache.json
OUTPUT_CSV = "bot_trade_error_analysis.csv"

# -----------------------------
# Helpers
# -----------------------------
def to_unix_5min_from_dt(dt):
    unix = int(dt.timestamp())
    return unix - (unix % 300)

def to_unix_5min_from_string(ts_str):
    dt = pd.to_datetime(ts_str)
    return to_unix_5min_from_dt(dt)

def to_unix_5min_from_date_time(date_val, time_val):
    dt = pd.to_datetime(f"{date_val} {time_val}")
    return to_unix_5min_from_dt(dt)

# -----------------------------
# 1. Load momentum data
# -----------------------------
momentum = pd.read_csv(MOMENTUM_CSV)
momentum.columns = [c.lower().strip() for c in momentum.columns]

if "window_ts" not in momentum.columns:
    raise ValueError("momentum_data.csv must contain 'window_ts' column")

momentum["window_ts"] = momentum["window_ts"].astype(int)

# -----------------------------
# 2. Load trade_analysis.xlsx
# -----------------------------
trades = pd.read_excel(TRADES_XLSX)
trades.columns = [c.lower().strip() for c in trades.columns]

# Build window_ts
trades["window_ts"] = trades.apply(
    lambda row: to_unix_5min_from_date_time(row["date"], row["time"]),
    axis=1
)

# Keep relevant columns
trades_subset = trades[[
    "asset",
    "window_ts",
    "result",
    "p&l ($)",
    "roi (%)",
    "confidence",
    "entry",
    "market ask",
    "size ($)"
]].rename(columns={
    "result": "ledger_result",
    "p&l ($)": "ledger_pnl",
    "roi (%)": "ledger_roi",
    "market ask": "ledger_market_ask",
    "size ($)": "ledger_size"
})

# -----------------------------
# 3. Load results_cache.json
# -----------------------------
with open(CACHED_JSON, "r") as f:
    cached = json.load(f)

trade_rows = []
for trade_id, t in cached.items():
    asset_raw = t.get("asset", "")  # e.g. "ETH Up"
    parts = asset_raw.split()
    asset = parts[0] if len(parts) > 0 else None
    direction = parts[1] if len(parts) > 1 else None

    trade_rows.append({
        "trade_id": trade_id,
        "asset": asset,
        "bot_trade_direction": direction,
        "bot_trade_result": t.get("result"),
        "bot_trade_time": t.get("time"),
        "bot_trade_entry": t.get("entry"),
        "bot_trade_size": t.get("size"),
        "bot_trade_conf": t.get("conf"),
        "bot_trade_market_ask": t.get("market_ask"),
    })

cached_df = pd.DataFrame(trade_rows)

# Build window_ts from bot_trade_time
cached_df["window_ts"] = cached_df["bot_trade_time"].apply(to_unix_5min_from_string)

# -----------------------------
# 4. Merge: bot trades + momentum + ledger
# -----------------------------
df = cached_df.merge(
    momentum,
    on=["asset", "window_ts"],
    how="left"
)

df = df.merge(
    trades_subset,
    on=["asset", "window_ts"],
    how="left"
)

# -----------------------------
# 5. Determine correctness using JSON results
# -----------------------------
def trade_correct(row):
    result = str(row.get("bot_trade_result", "")).upper()

    if "WIN" in result:
        return 1
    if "LOSS" in result:
        return 0
    return np.nan  # OPEN or unknown

df["bot_trade_correct"] = df.apply(trade_correct, axis=1)

mistakes = df[df["bot_trade_correct"] == 0].copy()

# -----------------------------
# 6. Diagnose WHY the bot was wrong
# -----------------------------
def diagnose(row):
    reasons = []

    cycle_buy_pct = row.get("cycle_buy_pct")
    last30_dir = row.get("last30_dir")
    accel = row.get("accel")
    direction = row.get("bot_trade_direction")

    if pd.notna(cycle_buy_pct):
        if cycle_buy_pct < 40 and direction == "Up":
            reasons.append("Bot longed into heavy sell pressure")
        if cycle_buy_pct > 60 and direction == "Down":
            reasons.append("Bot shorted into heavy buy pressure")

    if isinstance(last30_dir, str):
        if last30_dir == "Up" and direction == "Down":
            reasons.append("Bot ignored last30 Up momentum")
        if last30_dir == "Down" and direction == "Up":
            reasons.append("Bot ignored last30 Down momentum")

    if pd.notna(accel):
        if accel > 0 and direction == "Down":
            reasons.append("Bot ignored positive acceleration")
        if accel < 0 and direction == "Up":
            reasons.append("Bot ignored negative acceleration")

    if pd.notna(cycle_buy_pct) and 45 <= cycle_buy_pct <= 55:
        reasons.append("Bot traded a neutral/noise cycle")

    return "; ".join(reasons) if reasons else "Unknown reason"

mistakes["why_wrong"] = mistakes.apply(diagnose, axis=1)

# -----------------------------
# 7. Suggest WHAT would have fixed it
# -----------------------------
def correction(row):
    text = str(row.get("why_wrong", ""))

    if "sell pressure" in text or "buy pressure" in text:
        return "Use cycle_buy_pct threshold"

    if "momentum" in text:
        return "Use last30_dir / last60_dir"

    if "acceleration" in text:
        return "Use accel filter"

    if "neutral" in text:
        return "Skip neutral cycles"

    return "Unknown"

mistakes["suggested_fix"] = mistakes.apply(lambda r: correction(r), axis=1)

# -----------------------------
# 8. Save report
# -----------------------------
mistakes.to_csv(OUTPUT_CSV, index=False)

print(f"\nSaved: {OUTPUT_CSV}\n")
print("Error reasons:\n")
print(mistakes["why_wrong"].value_counts(dropna=False))
print("\nSuggested fixes:\n")
print(mistakes["suggested_fix"].value_counts(dropna=False))
