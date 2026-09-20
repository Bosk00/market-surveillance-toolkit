"""
momentum_tracker.py  v2
========================
Standalone — run alongside the bot, zero changes to it.

Captures per 5-min window:
  • Full cycle: total buy/sell volume, buy%, open-to-close price move
  • Last 60s:   buy%, price move, direction
  • Last 30s:   buy%, price move, direction, acceleration vs 60s
  • This window's market outcome  (filled when bot trade resolves)
  • NEXT window's market outcome  (filled 5 min later)

Outputs
-------
  momentum_summary.txt   human-readable window-by-window report
  momentum_data.csv      raw data for analysis
  tracker.log            live console mirror
"""

import asyncio
import websockets
import json
import os
import csv
import time
from datetime import datetime
from collections import deque

# ── Config ────────────────────────────────────────────────────────────────────

ASSETS = {
    "btcusdt": "BTC",
    "ethusdt": "ETH",
    "xrpusdt": "XRP",
}

WINDOW_SECS   = 300
MIN_VOL       = 50_000
PRICE_MIN_PCT = 0.01
POLL_SECS     = 1.0
TRADES_LOG    = "trades_log.txt"
SUMMARY_TXT   = "momentum_summary.txt"
DATA_CSV      = "momentum_data.csv"
TRACKER_LOG   = "tracker.log"

# ── State ─────────────────────────────────────────────────────────────────────

price_windows      = {a: deque(maxlen=2000) for a in ASSETS}
trade_windows      = {a: []                 for a in ASSETS}
current_prices     = {a: 0.0                for a in ASSETS}
window_open_prices = {a: {}                 for a in ASSETS}
window_open_times  = {a: 0                  for a in ASSETS}

snapshots:   dict = {}
seen_trades: set  = set()

CSV_HEADER = [
    "asset", "window_ts", "window_time",
    "cycle_buy_vol", "cycle_sell_vol", "cycle_total_vol", "cycle_buy_pct",
    "cycle_move_pct",
    "last60_buy_pct", "last60_move_pct", "last60_dir",
    "last30_buy_pct", "last30_move_pct", "last30_dir",
    "accel",
    "outcome_this", "outcome_next",
    "last30_predicted_this", "last30_correct_this",
    "last30_predicted_next", "last30_correct_next",
]

# ── Logging ───────────────────────────────────────────────────────────────────

def ts():
    return datetime.now().strftime("%H:%M:%S")

def log(msg):
    line = f"[{ts()}] {msg}"
    print(line)
    with open(TRACKER_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")

# ── Flow/price helpers ────────────────────────────────────────────────────────

def flow_over(asset_binance, seconds, now):
    cutoff = now - seconds
    trades = [t for t in trade_windows[asset_binance] if t["time"] >= cutoff]
    if not trades:
        return 50.0, 0.0
    buy  = sum(t["vol"] for t in trades if t["side"] == "BUY")
    sell = sum(t["vol"] for t in trades if t["side"] == "SELL")
    tot  = buy + sell
    return round(buy / tot * 100, 1) if tot > 0 else 50.0, round(tot, 0)

def price_move_over(asset_binance, seconds, now):
    cutoff = now - seconds
    prices = [p for p in price_windows[asset_binance] if p["time"] >= cutoff]
    if len(prices) < 2:
        return 0.0, "Flat"
    pct = (prices[-1]["price"] - prices[0]["price"]) / prices[0]["price"] * 100
    if pct > PRICE_MIN_PCT:    direction = "Up"
    elif pct < -PRICE_MIN_PCT: direction = "Down"
    else:                      direction = "Flat"
    return round(pct, 4), direction

# ── Snapshot ──────────────────────────────────────────────────────────────────

def take_snapshot(asset_binance, asset_name, closing_window_ts):
    now = time.time()

    # full cycle
    cycle_trades = [t for t in trade_windows[asset_binance]
                    if t["time"] >= closing_window_ts]
    c_buy  = sum(t["vol"] for t in cycle_trades if t["side"] == "BUY")
    c_sell = sum(t["vol"] for t in cycle_trades if t["side"] == "SELL")
    c_tot  = c_buy + c_sell
    c_buy_pct = round(c_buy / c_tot * 100, 1) if c_tot > 0 else 50.0

    open_price  = window_open_prices[asset_binance].get(closing_window_ts)
    close_price = current_prices[asset_binance]
    cycle_move  = round((close_price - open_price) / open_price * 100, 4) if open_price else 0.0

    # last 60s / 30s
    b60, _ = flow_over(asset_binance, 60, now)
    m60, d60 = price_move_over(asset_binance, 60, now)
    b30, _ = flow_over(asset_binance, 30, now)
    m30, d30 = price_move_over(asset_binance, 30, now)

    accel = round(abs(m30) - abs(m60), 4)

    snap = {
        "asset":           asset_name,
        "window_ts":       closing_window_ts,
        "window_time":     datetime.fromtimestamp(closing_window_ts).strftime("%Y-%m-%d %H:%M"),
        "cycle_buy_vol":   round(c_buy, 0),
        "cycle_sell_vol":  round(c_sell, 0),
        "cycle_total_vol": round(c_tot, 0),
        "cycle_buy_pct":   c_buy_pct,
        "cycle_move_pct":  cycle_move,
        "last60_buy_pct":  b60,
        "last60_move_pct": m60,
        "last60_dir":      d60,
        "last30_buy_pct":  b30,
        "last30_move_pct": m30,
        "last30_dir":      d30,
        "accel":           accel,
        "outcome_this":    None,
        "outcome_next":    None,
    }

    key = (asset_name, closing_window_ts)
    snapshots[key] = snap

    # keep last 60 per asset
    asset_keys = sorted([k for k in snapshots if k[0] == asset_name], key=lambda k: k[1])
    for old in asset_keys[:-60]:
        snapshots.pop(old, None)

    vol_str = f"${c_tot/1_000_000:.2f}M" if c_tot >= 1_000_000 else f"${c_tot/1_000:.0f}k"
    log(f"📸 {asset_name} | cycle: {c_buy_pct:.0f}% buy {vol_str} {cycle_move:+.3f}% | "
        f"30s: {m30:+.3f}% ({d30}) buy={b30:.0f}% | "
        f"accel: {'+' if accel >= 0 else ''}{accel:.3f}%")

# ── Summary writer ────────────────────────────────────────────────────────────

def correct_symbol(pred, actual):
    if pred in ("?", "Unknown") or not actual:
        return "?"
    return "✅" if pred == actual else "❌"

def write_summary_row(f, snap):
    o_this = snap["outcome_this"] or "pending"
    o_next = snap["outcome_next"] or "pending"
    pred30 = snap["last30_dir"] if snap["last30_dir"] != "Flat" else "?"

    vol = snap["cycle_total_vol"]
    vol_str = f"${vol/1_000_000:.2f}M" if vol >= 1_000_000 else f"${vol/1_000:.0f}k"

    accel_label = (
        "accelerating" if snap["accel"] > 0.005 else
        "decelerating" if snap["accel"] < -0.005 else
        "steady"
    )

    f.write(f"\n{'─'*60}\n")
    f.write(f"  {snap['asset']}  |  {snap['window_time']}\n")
    f.write(f"{'─'*60}\n")
    f.write(f"  FULL CYCLE\n")
    f.write(f"    Volume:     {vol_str} total  "
            f"(buy ${snap['cycle_buy_vol']/1_000:.0f}k  /  sell ${snap['cycle_sell_vol']/1_000:.0f}k)\n")
    f.write(f"    Buy ratio:  {snap['cycle_buy_pct']:.1f}%\n")
    f.write(f"    Price move: {snap['cycle_move_pct']:+.3f}%\n")
    f.write(f"  FINAL 60s\n")
    f.write(f"    Move:       {snap['last60_move_pct']:+.3f}% ({snap['last60_dir']})   "
            f"buy ratio: {snap['last60_buy_pct']:.1f}%\n")
    f.write(f"  FINAL 30s\n")
    f.write(f"    Move:       {snap['last30_move_pct']:+.3f}% ({snap['last30_dir']})   "
            f"buy ratio: {snap['last30_buy_pct']:.1f}%\n")
    f.write(f"    Accel:      {'+' if snap['accel'] >= 0 else ''}{snap['accel']:.3f}% ({accel_label})\n")
    f.write(f"  OUTCOMES\n")
    f.write(f"    This window:  {o_this:<8}  30s predicted: {pred30}  "
            f"{correct_symbol(pred30, snap['outcome_this'])}\n")
    f.write(f"    Next window:  {o_next:<8}  30s predicted: {pred30}  "
            f"{correct_symbol(pred30, snap['outcome_next'])}\n")
    f.write(f"{'─'*60}\n")


def flush_all():
    all_snaps = sorted(snapshots.values(), key=lambda s: (s["asset"], s["window_ts"]))
    if not all_snaps:
        return

    # CSV
    with open(DATA_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=CSV_HEADER)
        w.writeheader()
        for snap in all_snaps:
            pred30 = snap["last30_dir"] if snap["last30_dir"] != "Flat" else "Unknown"

            def c(pred, actual):
                if pred == "Unknown" or not actual:
                    return "N/A"
                return "1" if pred == actual else "0"

            w.writerow({
                "asset":                   snap["asset"],
                "window_ts":               snap["window_ts"],
                "window_time":             snap["window_time"],
                "cycle_buy_vol":           snap["cycle_buy_vol"],
                "cycle_sell_vol":          snap["cycle_sell_vol"],
                "cycle_total_vol":         snap["cycle_total_vol"],
                "cycle_buy_pct":           snap["cycle_buy_pct"],
                "cycle_move_pct":          snap["cycle_move_pct"],
                "last60_buy_pct":          snap["last60_buy_pct"],
                "last60_move_pct":         snap["last60_move_pct"],
                "last60_dir":              snap["last60_dir"],
                "last30_buy_pct":          snap["last30_buy_pct"],
                "last30_move_pct":         snap["last30_move_pct"],
                "last30_dir":              snap["last30_dir"],
                "accel":                   snap["accel"],
                "outcome_this":            snap["outcome_this"] or "",
                "outcome_next":            snap["outcome_next"] or "",
                "last30_predicted_this":   pred30,
                "last30_correct_this":     c(pred30, snap["outcome_this"]),
                "last30_predicted_next":   pred30,
                "last30_correct_next":     c(pred30, snap["outcome_next"]),
            })

    # Summary txt
    with open(SUMMARY_TXT, "w", encoding="utf-8") as f:
        f.write("MOMENTUM TRACKER — window-by-window summary\n")
        f.write(f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        for snap in all_snaps:
            write_summary_row(f, snap)

    log(f"💾 Flushed {len(all_snaps)} windows → {DATA_CSV} + {SUMMARY_TXT}")

# ── Accuracy report ───────────────────────────────────────────────────────────

def print_accuracy():
    all_snaps = list(snapshots.values())
    if len(all_snaps) < 5:
        return

    log("─" * 58)
    for asset in ["BTC", "ETH", "XRP", "ALL"]:
        subset = all_snaps if asset == "ALL" else [s for s in all_snaps if s["asset"] == asset]
        if not subset:
            continue

        def acc(pairs):
            if not pairs: return 0.0, 0
            return sum(1 for p, o in pairs if p == o) / len(pairs) * 100, len(pairs)

        this_pairs = [(s["last30_dir"], s["outcome_this"]) for s in subset
                      if s["last30_dir"] != "Flat" and s["outcome_this"]]
        next_pairs = [(s["last30_dir"], s["outcome_next"]) for s in subset
                      if s["last30_dir"] != "Flat" and s["outcome_next"]]
        hi_next    = [(s["last30_dir"], s["outcome_next"]) for s in subset
                      if s["last30_dir"] != "Flat" and s["outcome_next"]
                      and s["cycle_total_vol"] >= 5_000_000]
        accel_next = [(s["last30_dir"], s["outcome_next"]) for s in subset
                      if s["last30_dir"] != "Flat" and s["outcome_next"]
                      and s["accel"] > 0.005]

        ta, tn = acc(this_pairs)
        na, nn = acc(next_pairs)
        ha, hn = acc(hi_next)
        aa, an = acc(accel_next)

        log(f"  {asset:3s} | 30s→this: {ta:.1f}%({tn}) | "
            f"30s→next: {na:.1f}%({nn}) | "
            f"hi-vol→next: {ha:.1f}%({hn}) | "
            f"accel→next: {aa:.1f}%({an})")
    log("─" * 58)

# ── Trades log parser ─────────────────────────────────────────────────────────

def parse_trades_log():
    if not os.path.exists(TRADES_LOG):
        return []
    trades, current = [], {}
    with open(TRADES_LOG, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if line.startswith("Asset:"):
                parts = line.replace("Asset:", "").strip().split()
                if len(parts) >= 2:
                    current = {"asset": parts[0], "direction": parts[1]}
            elif line.startswith("Time:"):
                current["time"] = line.replace("Time:", "").strip()
            elif line.startswith("Result:"):
                result = line.replace("Result:", "").strip()
                if result in ("WIN", "LOSS") and "asset" in current:
                    current["result"] = result
                    trades.append(dict(current))
                    current = {}
    return trades

# ── Outcome checker ───────────────────────────────────────────────────────────

async def outcome_checker():
    log(f"🔍 Outcome checker watching {TRADES_LOG}")
    while True:
        await asyncio.sleep(POLL_SECS)
        try:
            changed = False
            for trade in parse_trades_log():
                uid = f"{trade['asset']}_{trade['direction']}_{trade['time']}"
                if uid in seen_trades:
                    continue
                seen_trades.add(uid)

                outcome = trade["direction"] if trade["result"] == "WIN" else (
                    "Down" if trade["direction"] == "Up" else "Up"
                )

                try:
                    trade_unix = datetime.strptime(trade["time"], "%Y-%m-%d %H:%M:%S").timestamp()
                    this_ts    = (int(trade_unix) // WINDOW_SECS) * WINDOW_SECS
                    prev_ts    = this_ts - WINDOW_SECS
                except Exception:
                    continue

                this_key = (trade["asset"], this_ts)
                prev_key = (trade["asset"], prev_ts)

                if this_key in snapshots and snapshots[this_key]["outcome_this"] is None:
                    snapshots[this_key]["outcome_this"] = outcome
                    changed = True

                if prev_key in snapshots and snapshots[prev_key]["outcome_next"] is None:
                    snapshots[prev_key]["outcome_next"] = outcome
                    changed = True

            if changed:
                flush_all()

        except Exception as e:
            log(f"⚠️  Outcome checker error: {e}")

# ── Binance stream ────────────────────────────────────────────────────────────

async def watch_stream(asset_binance):
    asset_name = ASSETS[asset_binance]
    url = f"wss://stream.binance.com:9443/ws/{asset_binance}@aggTrade"
    log(f"🔌 Connecting: {asset_name}")
    while True:
        try:
            async with websockets.connect(url, ping_interval=None) as ws:
                while True:
                    data  = json.loads(await ws.recv())
                    price = float(data.get("p", 0))
                    qty   = float(data.get("q", 0))
                    side  = "SELL" if data.get("m", False) else "BUY"
                    now   = time.time()

                    price_windows[asset_binance].append({"time": now, "price": price})
                    trade_windows[asset_binance].append({"time": now, "side": side, "vol": price * qty})
                    current_prices[asset_binance] = price

                    # trim to 6 min
                    cutoff = now - 360
                    trade_windows[asset_binance] = [
                        t for t in trade_windows[asset_binance] if t["time"] > cutoff
                    ]

                    window_ts = (int(now) // WINDOW_SECS) * WINDOW_SECS
                    if window_ts != window_open_times[asset_binance]:
                        old_ts = window_open_times[asset_binance]
                        if old_ts > 0:
                            take_snapshot(asset_binance, asset_name, old_ts)
                            flush_all()
                        window_open_prices[asset_binance][window_ts] = price
                        window_open_times[asset_binance] = window_ts

        except Exception as e:
            log(f"⚠️  {asset_name} error: {e} — retry in 2s")
            await asyncio.sleep(2)

# ── Periodic report ───────────────────────────────────────────────────────────

async def periodic_report():
    await asyncio.sleep(600)
    while True:
        await asyncio.sleep(1800)
        print_accuracy()

# ── Main ──────────────────────────────────────────────────────────────────────

async def main():
    log("=" * 60)
    log("🚀 Momentum Tracker v2")
    log(f"   Assets:  {', '.join(ASSETS.values())}")
    log(f"   Summary: {SUMMARY_TXT}")
    log(f"   Data:    {DATA_CSV}")
    log(f"   Trades:  {TRADES_LOG}")
    log("   Waits one full 5-min window before first snapshot")
    log("=" * 60)
    await asyncio.gather(
        *[watch_stream(a) for a in ASSETS],
        outcome_checker(),
        periodic_report(),
    )

if __name__ == "__main__":
    asyncio.run(main())