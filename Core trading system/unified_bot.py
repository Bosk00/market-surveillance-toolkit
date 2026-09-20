import asyncio
import aiohttp
import json
import math
import time
import websockets
from datetime import datetime
from collections import deque
from dotenv import load_dotenv
import os

load_dotenv()

'''
========================= CHANGE LOG =========================

DATA-BACKED CHANGES (all from 11,608 trade analysis):

1.  BNB + DOGE REMOVED — 49.6% and 53.1% win rates respectively (coin flip).
    Core assets BTC/ETH/XRP/SOL hold at 82.7%. No edge in the new assets.

2.  MIN_SIGNAL_ENTRY = 0.45 — Signal trades at entry < 0.40 had 28.5% WR
    (193 trades, 138 losses). Hard floor added to all non-hedge signal paths.

3.  REPRICE tightened — Repriced signals: 75.4% WR vs 88.9% for normal.
    Timeout cut from 180s → 50s. Reprice only executes with < 55s left in
    the window (close to expiry is where the edge exists).

4.  NEXT_CYCLE removed — 3W 9L (25% WR) over 12 trades. Not enough data
    to justify firing, and direction suggests it's net negative. All
    CYCLE_PROB_* code removed.

5.  Time-weight guard — Trades at time_weight ≥ 100% had 47.2% WR (25W 28L).
    Added explicit clamp and MIN_SECS_LEFT_PRICE = 8 to avoid firing right
    at window boundary where edge collapses.

6.  Rolling win rate filter — ML pipeline confirmed rolling_wr_10 is the
    second most important feature (SHAP 0.1722) after entry price. Added a
    per-asset rolling tracker: skip price signals on an asset if its last
    10 resolved trades are below 65% WR. Does not block sweep signals.

ML-INFORMED CHANGES (from pipeline: AUC 0.678, top features):

7.  Entry price remains king (SHAP 0.6207). The entry floor change (#2)
    directly addresses this. Higher entry = closer to 1.00 payout = better
    signal clarity.

8.  is_repriced (SHAP 0.0561) penalises repriced trades. Combined with #3,
    repricing is now last-resort-only.

CLEANUP:
-  PAPER_TRADE_ASSETS removed (unnecessary complexity)
-  Passive cycle tracker simplified (no longer updates cycle stats)
-  ARB_MIN_GUARANTEED raised from 0.70 → 1.20 (require real lockable profit)
===========================================================================
'''

# ============================================================
# ML FILTER — loaded once at startup
# ============================================================

try:
    from trade_ml_pipeline import TradeFilter
    ml_filter = TradeFilter.load()
    print("✅ ML filter loaded")
except Exception as _ml_err:
    ml_filter = None
    print(f"⚠️  ML filter not available: {_ml_err}")

try:
    from ml_shadow_log import shadow_log, shadow_resolve, print_shadow_summary
    _shadow_enabled = True
    print("✅ ML shadow logger loaded")
except Exception as _sl_err:
    _shadow_enabled = False
    print(f"⚠️  Shadow logger not available: {_sl_err}")
    def shadow_log(*a, **kw):      pass
    def shadow_resolve(*a, **kw):  pass
    def print_shadow_summary():    pass


def build_trade_dict(asset, direction, entry, market_ask,
                     confidence, size, secs_left,
                     score=0, flow="NEUTRAL", strength="NEUTRAL",
                     price_edge=0.0, path_score=0.0):
    asset_binance  = next((k for k, v in ASSETS.items() if v == asset), None)
    price_now      = current_prices.get(asset_binance, 0)
    window_ts      = window_open_times.get(asset_binance, 0)
    open_price     = window_open_prices.get(asset_binance, {}).get(window_ts, price_now)
    price_move_pct = ((price_now - open_price) / open_price * 100) if open_price else 0.0

    conf_with_stats = (
        f"{confidence} ({price_move_pct:+.3f}% x {min(int(abs(price_move_pct)*100), 100)}%)"
    )
    return {
        "datetime":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "asset":       asset,
        "direction":   direction,
        "confidence":  conf_with_stats,
        "entry":       entry,
        "market_ask":  market_ask,
        "size":        size,
        "_secs_left":  secs_left,
        "_score":      score,
        "_flow":       flow,
        "_strength":   strength,
        "_price_edge": price_edge,
        "_path_score": path_score,
    }


def ml_check(trade_dict, label=""):
    if ml_filter is None:
        return True, 1.0
    try:
        take, prob = ml_filter.should_take(trade_dict)
        return take, prob
    except Exception as e:
        print(f"  ⚠️  ML filter error{' (' + label + ')' if label else ''}: {e}")
        return True, 1.0


# ============================================================
# SETTINGS
# ============================================================

PAPER_TRADE          = True

MAX_ENTRY            = 0.99
MIN_SIGNAL_ENTRY     = 0.45   # CHANGE #2: hard floor — 28.5% WR below 0.40
TRADE_SIZE           = 5
MAX_DAILY_LOSS       = 40
MAX_OPEN_POSITIONS   = 500

WINDOW_SECONDS       = 30
IMBALANCE_THRESHOLD  = 0.58

# ============================================================
# TIME GATE
# ============================================================

BLOCKED_HOURS = set()

def is_trading_allowed():
    return datetime.now().hour not in BLOCKED_HOURS

# ============================================================
# REPRICE SETTINGS (CHANGE #3)
# Timeout cut 180→50s. Only executes with < 55s left.
# Repriced signals had 75.4% WR vs 88.9% for normal signals.
# ============================================================
REPRICE_MAX_ENTRY    = 0.75
REPRICE_MIN_ENTRY    = 0.45   # matches MIN_SIGNAL_ENTRY
REPRICE_TIMEOUT      = 50     # CHANGED: was 180
REPRICE_MAX_SECS_LEFT = 55    # CHANGED: new — only reprice close to expiry

# ============================================================
# TIME-WEIGHT GUARD (CHANGE #5)
# Trades at time_weight >= 100% had 47.2% WR (25W 28L).
# Don't fire price signals too close to window boundary.
# ============================================================
MIN_SECS_LEFT_PRICE  = 8     # don't enter price signal trades this close to expiry

# ============================================================
# ROLLING WIN RATE FILTER (CHANGE #6)
# ML: rolling_wr_10 is 2nd most important feature (SHAP 0.1722).
# Per-asset rolling tracker: skip price signals if last 10 < 65%.
# ============================================================
ROLLING_WR_WINDOW    = 10
ROLLING_WR_MIN       = 0.65

# ============================================================
# ARB / HEDGE SCANNER SETTINGS
# Raised MIN_GUARANTEED from 0.70 → 1.20 (real profit only)
# ============================================================
ARB_SCAN_ENABLED   = True
ARB_MIN_GUARANTEED = 2.00   # CHANGED: was 0.70
ARB_MAX_HEDGE      = 9.00
ARB_COOLDOWN       = 1

# ============================================================
# ASSETS — CHANGE #1: BNB and DOGE removed (49.6% / 53.1% WR)
# ============================================================

MIN_VOLUME = {
    "btcusdt":  10000,
    "ethusdt":  5000,
    "xrpusdt":  500,
    "solusdt":  2000,
}
PRICE_MOVE_THRESHOLD = 0.02

SWEEP_THRESHOLD     = 3000
SWEEP_THRESHOLD_ALT = 800

MIN_TIME_LEFT = 1
MAX_TIME_LEFT = 200
POLL_INTERVAL = 0.5

CLOB_API  = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com/markets/slug"

ASSETS = {
    "btcusdt":  "BTC",
    "ethusdt":  "ETH",
    "xrpusdt":  "XRP",
    "solusdt":  "SOL",
}

MARKETS = [
    {"slug": "btc-updown-5m",  "interval": 300, "asset": "BTC"},
    {"slug": "eth-updown-5m",  "interval": 300, "asset": "ETH"},
    {"slug": "xrp-updown-5m",  "interval": 300, "asset": "XRP"},
    {"slug": "sol-updown-5m",  "interval": 300, "asset": "SOL"},
]

# ============================================================
# PER-ASSET PRICE EDGE SETTINGS
# ============================================================

ASSET_EDGE_SETTINGS = {
    "BTC":  {"edge_min": 0.02, "edge_confirm": 0.03, "edge_conflict": 0.07},
    "ETH":  {"edge_min": 0.04, "edge_confirm": 0.03, "edge_conflict": 0.07},
    "XRP":  {"edge_min": 0.06, "edge_confirm": 0.03, "edge_conflict": 0.08},
    "SOL":  {"edge_min": 0.04, "edge_confirm": 0.03, "edge_conflict": 0.07},
}

def get_edge_settings(asset_name):
    return ASSET_EDGE_SETTINGS.get(asset_name, {
        "edge_min": 0.05, "edge_confirm": 0.03, "edge_conflict": 0.10
    })

PRICE_SIGNAL_TRADE_SIZE = 5

# ============================================================
# PATH RESISTANCE SETTINGS
# ============================================================

PATH_STRONG_THRESHOLD = 3.0
PATH_WEAK_THRESHOLD   = 0.5
PATH_SCAN_RANGE_PCT   = 0.30

# ============================================================
# SWEEP PRICE CONFIRMATION WINDOW
# ============================================================

SWEEP_PRICE_CONFIRM_WINDOW   = 15
SWEEP_PRICE_CONFIRM_MIN_MOVE = 0.0

# ============================================================
# BOTH-SIDES SWEEP FILTER
# ============================================================

BOTH_SIDES_WINDOW = 20

# ============================================================
# SHARED STATE
# ============================================================

trade_windows = {asset: [] for asset in ASSETS.keys()}
price_windows = {asset: deque(maxlen=500) for asset in ASSETS.keys()}
flow_signals  = {asset: "NEUTRAL" for asset in ASSETS.keys()}
flow_strength = {asset: "NEUTRAL" for asset in ASSETS.keys()}
previous_ask_sizes  = {}
open_positions      = {}
recent_sweeps       = {}
reprice_candidates  = {}
hedged_positions    = {}
arb_attempted       = {}
daily_pnl = 0.0
peak_pnl  = 0.0
wins      = 0
losses    = 0

# CHANGE #6: rolling win rate tracker (per asset, last N outcomes)
rolling_outcomes: dict[str, deque] = {
    asset: deque(maxlen=ROLLING_WR_WINDOW)
    for asset in ["BTC", "ETH", "XRP", "SOL"]
}

window_open_prices = {asset: {} for asset in ASSETS.keys()}
window_open_times  = {asset: 0  for asset in ASSETS.keys()}
current_prices     = {asset: 0.0 for asset in ASSETS.keys()}

depth_cache = {asset: {"bids": [], "asks": [], "ts": 0} for asset in ASSETS.keys()}

# ============================================================
# CYCLE STATS (kept for passive observation, no longer trades on it)
# CHANGE #4: NEXT_CYCLE trading removed entirely
# ============================================================

CYCLE_STATS_FILE = "cycle_stats.json"

def load_cycle_stats(filepath):
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
            for asset in ["BTC", "ETH", "XRP", "SOL"]:
                if asset not in data:
                    data[asset] = {"after_up": {"up": 0, "down": 0},
                                   "after_down": {"up": 0, "down": 0},
                                   "last_outcome": None}
            return data
    except:
        return {
            asset: {"after_up": {"up": 0, "down": 0},
                    "after_down": {"up": 0, "down": 0},
                    "last_outcome": None}
            for asset in ["BTC", "ETH", "XRP", "SOL"]
        }

def save_cycle_stats(stats, filepath):
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2)
    except Exception as e:
        print(f"  ⚠️  Could not save cycle stats: {e}")

cycle_stats      = load_cycle_stats(CYCLE_STATS_FILE)
last_stats_print = 0.0
passive_cycle_seen: set = set()

# ============================================================
# UTILITIES
# ============================================================

def ts():
    return datetime.now().strftime("%H:%M:%S")

def get_current_timestamp(interval):
    now = int(time.time())
    return (now // interval) * interval

def get_slug(base_slug, interval):
    ts_val = get_current_timestamp(interval)
    return f"{base_slug}-{ts_val}"

def seconds_until(end_date_str):
    try:
        end_ts = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        now_ts = datetime.now(end_ts.tzinfo)
        return int((end_ts - now_ts).total_seconds())
    except:
        return None

def parse_end_time(end_date_str):
    try:
        end_ts = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        return end_ts.timestamp()
    except:
        return 0

def log_trade(asset, direction, entry, size, confidence, token_id, quoted_ask=None):
    with open("trades_log.txt", "a", encoding="utf-8") as f:
        f.write(f"\n{'='*50}\n")
        f.write(f"Time:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Asset:      {asset} {direction}\n")
        f.write(f"Entry:      {entry}\n")
        f.write(f"Quoted ask: {quoted_ask if quoted_ask is not None else entry}\n")
        if quoted_ask is not None and quoted_ask != entry:
            f.write(f"Slippage:   {entry - quoted_ask:+.4f}\n")
        f.write(f"Size:       ${size}\n")
        f.write(f"Confidence: {confidence}\n")
        f.write(f"Token ID:   {token_id}\n")
        f.write(f"Result:     PENDING\n")

# ============================================================
# ROLLING WIN RATE HELPERS (CHANGE #6)
# ============================================================

def record_rolling_outcome(asset: str, won: bool):
    """Record trade outcome in the per-asset rolling window."""
    if asset in rolling_outcomes:
        rolling_outcomes[asset].append(1 if won else 0)

def get_rolling_wr(asset: str) -> float | None:
    """Return rolling win rate for asset, or None if insufficient data."""
    dq = rolling_outcomes.get(asset)
    if dq is None or len(dq) < ROLLING_WR_WINDOW:
        return None
    return sum(dq) / len(dq)

def rolling_wr_allows_price_signal(asset: str) -> bool:
    """
    Block price signals on an asset if rolling WR is below threshold.
    Does NOT block sweep signals (they're independent of price trend).
    """
    wr = get_rolling_wr(asset)
    if wr is None:
        return True  # not enough data — allow
    if wr < ROLLING_WR_MIN:
        print(f"  ⚠️  Rolling WR gate: {asset} last {ROLLING_WR_WINDOW} trades "
              f"= {wr:.0%} < {ROLLING_WR_MIN:.0%} — skip price signal")
        return False
    return True

# ============================================================
# SWEEP PRICE CONFIRMATION HELPER
# ============================================================

def sweep_confirmed_by_price(asset_binance, direction):
    now    = time.time()
    cutoff = now - SWEEP_PRICE_CONFIRM_WINDOW
    recent = [p for p in price_windows[asset_binance] if p["time"] >= cutoff]
    if len(recent) < 2:
        return True
    price_move = recent[-1]["price"] - recent[0]["price"]
    if direction == "Up"   and price_move > SWEEP_PRICE_CONFIRM_MIN_MOVE:
        return True
    if direction == "Down" and price_move < -SWEEP_PRICE_CONFIRM_MIN_MOVE:
        return True
    return False

# ============================================================
# WALLET CONNECTION
# ============================================================

def get_client():
    try:
        from py_clob_client_v2 import ApiCreds, ClobClient
        creds = ApiCreds(
            api_key=os.getenv("POLY_API_KEY"),
            api_secret=os.getenv("POLY_SECRET"),
            api_passphrase=os.getenv("POLY_PASSPHRASE"),
        )
        client = ClobClient(
            host="https://clob.polymarket.com",
            chain_id=137,
            key=os.getenv("PRIVATE_KEY"),
            creds=creds,
            signature_type=2,
            funder=os.getenv("PROXY_WALLET"),
        )
        return client
    except Exception as e:
        print(f"Wallet connection failed: {e}")
        return None

# ============================================================
# HEARTBEAT
# ============================================================

async def send_heartbeat():
    heartbeat_id = ""
    client = get_client() if not PAPER_TRADE else None
    while True:
        try:
            if client:
                resp = client.post_heartbeat(heartbeat_id)
                heartbeat_id = resp.get("heartbeat_id", "")
        except Exception:
            heartbeat_id = ""
        await asyncio.sleep(5)

# ============================================================
# FLOW SIGNAL
# ============================================================

def calculate_imbalance(asset):
    now = time.time()
    cutoff = now - WINDOW_SECONDS
    trade_windows[asset] = [t for t in trade_windows[asset] if t["time"] > cutoff]
    trades = trade_windows[asset]
    if not trades:
        return None, None, None
    buy_vol   = sum(t["volume"] for t in trades if t["side"] == "BUY")
    sell_vol  = sum(t["volume"] for t in trades if t["side"] == "SELL")
    total_vol = buy_vol + sell_vol
    if total_vol < MIN_VOLUME[asset]:
        return None, None, total_vol
    return buy_vol, sell_vol, total_vol

def calculate_price_trend(asset):
    now    = time.time()
    cutoff = now - WINDOW_SECONDS
    prices = [p for p in price_windows[asset] if p["time"] > cutoff]
    if len(prices) < 2:
        return "FLAT", 0.0
    oldest = prices[0]["price"]
    newest = prices[-1]["price"]
    if oldest == 0:
        return "FLAT", 0.0
    pct = (newest - oldest) / oldest * 100
    if pct > PRICE_MOVE_THRESHOLD:
        return "UP", pct
    elif pct < -PRICE_MOVE_THRESHOLD:
        return "DOWN", pct
    return "FLAT", pct

async def watch_trades(asset):
    global window_open_prices, window_open_times, current_prices
    url = f"wss://stream.binance.com:9443/ws/{asset}@aggTrade"
    while True:
        try:
            async with websockets.connect(url, ping_interval=None) as ws:
                while True:
                    data     = json.loads(await ws.recv())
                    price    = float(data.get("p", 0))
                    quantity = float(data.get("q", 0))
                    is_buyer_maker = data.get("m", False)
                    side = "SELL" if is_buyer_maker else "BUY"
                    trade_windows[asset].append({
                        "time": time.time(), "side": side,
                        "volume": price * quantity, "price": price
                    })
                    price_windows[asset].append({"time": time.time(), "price": price})
                    current_prices[asset] = price
                    now_ts    = int(time.time())
                    window_ts = (now_ts // 300) * 300
                    if window_ts != window_open_times[asset]:
                        window_open_prices[asset][window_ts] = price
                        window_open_times[asset] = window_ts
        except:
            await asyncio.sleep(1)

async def watch_depth(asset):
    url = f"wss://stream.binance.com:9443/ws/{asset}@depth20@100ms"
    while True:
        try:
            async with websockets.connect(url, ping_interval=None) as ws:
                while True:
                    data = json.loads(await ws.recv())
                    depth_cache[asset]["bids"] = [
                        (float(b[0]), float(b[1])) for b in data.get("bids", [])
                    ]
                    depth_cache[asset]["asks"] = [
                        (float(a[0]), float(a[1])) for a in data.get("asks", [])
                    ]
                    depth_cache[asset]["ts"] = time.time()
        except:
            await asyncio.sleep(1)

def get_path_resistance(asset_binance, direction, secs_left):
    price_now  = current_prices.get(asset_binance, 0)
    window_ts  = window_open_times.get(asset_binance, 0)
    open_price = window_open_prices.get(asset_binance, {}).get(window_ts)
    depth      = depth_cache.get(asset_binance, {})

    if not price_now or not open_price:
        return 0.0, "NO DATA"

    bids = depth.get("bids", [])
    asks = depth.get("asks", [])
    if not bids or not asks:
        return 0.0, "NO DEPTH"

    gap        = abs(price_now - open_price)
    scan_range = price_now * PATH_SCAN_RANGE_PCT / 100
    scan_dist  = max(gap, scan_range)

    if direction == "Up":
        if price_now >= open_price:
            supporting_bids = sum(size * price for price, size in bids if price >= price_now - scan_dist)
            near_asks       = sum(size * price for price, size in asks if price <= price_now + scan_range)
            ratio    = supporting_bids / near_asks if near_asks > 0 else 10.0
            dist_pct = (price_now - open_price) / open_price * 100
            label    = (f"UP winning +{dist_pct:.3f}% | bid support: ${supporting_bids:,.0f} vs ask pressure: ${near_asks:,.0f} (ratio {ratio:.1f}x)")
        else:
            asks_in_path = sum(size * price for price, size in asks if price_now < price <= price_now + scan_dist)
            bid_momentum = sum(size * price for price, size in bids if price >= price_now - scan_range)
            ratio    = bid_momentum / asks_in_path if asks_in_path > 0 else 0.0
            dist_pct = (open_price - price_now) / open_price * 100
            label    = (f"UP needs +{dist_pct:.3f}% | ask wall: ${asks_in_path:,.0f} bid momentum: ${bid_momentum:,.0f} (ratio {ratio:.1f}x)")
    else:
        if price_now <= open_price:
            supporting_asks = sum(size * price for price, size in asks if price <= price_now + scan_dist)
            near_bids       = sum(size * price for price, size in bids if price >= price_now - scan_range)
            ratio    = supporting_asks / near_bids if near_bids > 0 else 10.0
            dist_pct = (open_price - price_now) / open_price * 100
            label    = (f"DOWN winning -{dist_pct:.3f}% | ask support: ${supporting_asks:,.0f} vs bid pressure: ${near_bids:,.0f} (ratio {ratio:.1f}x)")
        else:
            bids_in_path = sum(size * price for price, size in bids if price_now - scan_dist <= price < price_now)
            ask_momentum = sum(size * price for price, size in asks if price <= price_now + scan_range)
            ratio    = ask_momentum / bids_in_path if bids_in_path > 0 else 0.0
            dist_pct = (price_now - open_price) / open_price * 100
            label    = (f"DOWN needs -{dist_pct:.3f}% | bid wall: ${bids_in_path:,.0f} ask momentum: ${ask_momentum:,.0f} (ratio {ratio:.1f}x)")

    if ratio >= PATH_STRONG_THRESHOLD:   score = 3.0
    elif ratio >= 1.5:                   score = 2.0
    elif ratio >= 1.0:                   score = 1.0
    elif ratio >= PATH_WEAK_THRESHOLD:   score = -1.0
    else:                                score = -2.0

    return score, label

async def flow_monitor():
    prev_signals = {asset: "NEUTRAL" for asset in ASSETS.keys()}
    while True:
        await asyncio.sleep(5)
        for asset, name in ASSETS.items():
            buy_vol, sell_vol, total_vol = calculate_imbalance(asset)
            price_trend, pct_change = calculate_price_trend(asset)
            if total_vol is None or buy_vol is None or sell_vol is None:
                continue
            buy_pct  = buy_vol  / total_vol * 100
            sell_pct = sell_vol / total_vol * 100
            if buy_vol / total_vol >= IMBALANCE_THRESHOLD:
                signal = "UP"
            elif sell_vol / total_vol >= IMBALANCE_THRESHOLD:
                signal = "DOWN"
            else:
                signal = "NEUTRAL"
            if signal != "NEUTRAL":
                if signal == price_trend:
                    dominant = buy_pct if signal == "UP" else sell_pct
                    strength = "STRONG" if dominant > 80 else "MODERATE"
                elif price_trend == "FLAT":
                    strength = "EARLY"
                else:
                    strength = "⚠️  CONFLICTED"
            else:
                strength = "NEUTRAL"
            flow_signals[asset]  = signal
            flow_strength[asset] = strength
            if signal != "NEUTRAL" and strength != "⚠️  CONFLICTED":
                emoji          = "🟢" if signal == "UP" else "🔴"
                price_emoji    = "📈" if price_trend == "UP" else "📉" if price_trend == "DOWN" else "➡️"
                strength_emoji = "💪" if strength == "STRONG" else "✅" if strength == "MODERATE" else "⏳"
                print(f"[{ts()}] {emoji} FLOW {name} | "
                      f"Buy: {buy_pct:.0f}% Sell: {sell_pct:.0f}% | "
                      f"Vol: ${total_vol/1000:.0f}k | "
                      f"Price: {price_emoji} {pct_change:+.3f}% | "
                      f"{strength_emoji} {strength} | Signal: {signal}")
            prev_signals[asset] = signal

# ============================================================
# PRICE VS WINDOW OPEN SIGNAL
# ============================================================

def get_price_signal(asset_binance, secs_left, asset_name=None):
    """
    Compare current price to window open price.
    CHANGE #5: time_weight is explicitly clamped to [0, 1].
    If secs_left < MIN_SECS_LEFT_PRICE the caller should skip — but the
    function still computes cleanly so it can be used for scoring.
    """
    price_now  = current_prices.get(asset_binance, 0)
    window_ts  = window_open_times.get(asset_binance, 0)
    open_price = window_open_prices.get(asset_binance, {}).get(window_ts)

    if not price_now or not open_price:
        return None, 0.0, "NO DATA"

    pct_move = (price_now - open_price) / open_price * 100

    now_ts       = int(time.time())
    time_elapsed = now_ts - window_ts
    # CHANGE #5: hard clamp — never exceed 1.0
    time_weight  = min(max(time_elapsed / 300, 0.0), 1.0)
    edge         = abs(pct_move) * time_weight

    if pct_move > 0:
        direction = "Up"
    elif pct_move < 0:
        direction = "Down"
    else:
        return None, 0.0, "FLAT"

    name     = asset_name or ASSETS.get(asset_binance, asset_binance.upper())
    edge_cfg = get_edge_settings(name)

    if edge >= edge_cfg["edge_min"]:
        label = f"STRONG {'UP' if pct_move > 0 else 'DOWN'} {name} ({pct_move:+.3f}% x {time_weight:.0%})"
    elif edge >= edge_cfg["edge_confirm"]:
        label = f"WEAK {'UP' if pct_move > 0 else 'DOWN'} {name} ({pct_move:+.3f}%)"
    else:
        label = f"FLAT {name} ({pct_move:+.3f}%)"

    return direction, edge, label

# ============================================================
# SPOOF DETECTOR
# ============================================================

SPOOF_MIN_SIZE  = 2.5
SPOOF_MAX_SIZE  = 8.0
SPOOF_PROXIMITY = 25
SPOOF_MIN_HOLD  = 15
SPOOF_MAX_HOLD  = 90
SPOOF_BOUNCE    = 15

spoof_lockout_until = 0

async def spoof_detector():
    global spoof_lockout_until
    url     = "wss://stream.binance.com:9443/ws/btcusdt@depth20@100ms"
    tracked = {}
    while True:
        try:
            async with websockets.connect(url, ping_interval=None) as ws:
                while True:
                    data = json.loads(await ws.recv())
                    bids = data.get("bids", [])
                    asks = data.get("asks", [])
                    if not bids:
                        continue
                    current_price = float(bids[0][0])
                    now           = time.time()
                    current_bids  = {float(b[0]): float(b[1]) for b in bids}
                    current_asks  = {float(a[0]): float(a[1]) for a in asks}
                    for price, size in current_bids.items():
                        if SPOOF_MIN_SIZE <= size <= SPOOF_MAX_SIZE:
                            if abs(price - current_price) <= SPOOF_PROXIMITY:
                                key = f"BID_{price:.0f}"
                                if key not in tracked:
                                    tracked[key] = {"price": price, "size": size,
                                                    "side": "BID", "first_seen": now,
                                                    "price_at_appearance": current_price}
                    for price, size in current_asks.items():
                        if SPOOF_MIN_SIZE <= size <= SPOOF_MAX_SIZE:
                            if abs(price - current_price) <= SPOOF_PROXIMITY:
                                key = f"ASK_{price:.0f}"
                                if key not in tracked:
                                    tracked[key] = {"price": price, "size": size,
                                                    "side": "ASK", "first_seen": now,
                                                    "price_at_appearance": current_price}
                    for key in list(tracked.keys()):
                        order      = tracked[key]
                        side       = order["side"]
                        price      = order["price"]
                        held       = now - order["first_seen"]
                        price_then = order["price_at_appearance"]
                        gone       = (price not in current_bids and price not in current_asks)
                        if gone:
                            del tracked[key]
                            if held < SPOOF_MIN_HOLD or held > SPOOF_MAX_HOLD:
                                continue
                            bounced = False
                            if side == "ASK" and current_price > price_then + SPOOF_BOUNCE:
                                bounced = True
                            elif side == "BID" and current_price < price_then - SPOOF_BOUNCE:
                                bounced = True
                            if bounced:
                                direction = "UP" if side == "ASK" else "DOWN"
                                spoof_lockout_until = time.time() + 30
                                print(f"\n[{ts()}] 🚨 SPOOF CONFIRMED — {direction} | Locked 30s")
        except:
            await asyncio.sleep(1)

# ============================================================
# ORDERBOOK HELPERS
# ============================================================

async def fetch_market_tokens(session, market):
    slug = get_slug(market["slug"], market["interval"])
    try:
        async with session.get(
            f"{GAMMA_API}/{slug}",
            timeout=aiohttp.ClientTimeout(total=8)
        ) as resp:
            if resp.status != 200:
                return None
            data      = await resp.json()
            token_ids = json.loads(data.get("clobTokenIds", "[]"))
            end_date  = data.get("endDate", "")
            return {
                "asset":        market["asset"],
                "slug":         slug,
                "end_date":     end_date,
                "end_time":     parse_end_time(end_date),
                "condition_id": data.get("conditionId", ""),
                "up_token":     token_ids[0] if len(token_ids) > 0 else None,
                "down_token":   token_ids[1] if len(token_ids) > 1 else None,
            }
    except:
        return None

async def fetch_orderbook(session, token_id):
    try:
        async with session.get(
            f"{CLOB_API}/book",
            params={"token_id": token_id},
            timeout=aiohttp.ClientTimeout(total=5)
        ) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except:
        return None

def get_ask_size_at_price(orderbook, price="0.99"):
    if not orderbook:
        return 0
    for ask in orderbook.get("asks", []):
        if ask.get("price") == price:
            return float(ask.get("size", 0))
    return 0

def get_best_ask(orderbook):
    if not orderbook:
        return None
    valid = [
        float(ask.get("price", 0))
        for ask in orderbook.get("asks", [])
        if float(ask.get("size", 0)) > 0 and float(ask.get("price", 0)) < 0.97
    ]
    return min(valid) if valid else None

# ============================================================
# ORDER PLACEMENT
# ============================================================

async def place_order(session, token_id, price, size, direction, asset,
                      bypass_min=False, gtc=False):
    global daily_pnl

    if PAPER_TRADE:
        fill_price = price
        shares     = size / fill_price
        order_type = "GTC" if gtc else "FAK"
        print(f"  📝 PAPER TRADE ({order_type}): BUY {asset} {direction} | "
              f"${size:.4f} at {fill_price:.2f} (sim) = {shares:.4f} shares | "
              f"Est payout: ${shares:.4f} | Market ask: {price:.2f}")
        return True, shares, fill_price

    try:
        order_size = size if bypass_min else max(size, 5.0)
        async with session.post(
            "http://127.0.0.1:3001",
            json={
                "tokenId": token_id,
                "price":   price,
                "size":    order_size,
                "side":    "BUY",
                "gtc":     gtc,
            },
            headers={"x-internal-secret": os.getenv("ORDER_SERVER_SECRET", "")},
            timeout=aiohttp.ClientTimeout(total=10)
        ) as resp:
            output = await resp.json()

        response = output.get("response", {})
        valid_statuses = {"matched", "live", "delayed"}
        if output.get("success") and not response.get("errorMsg") and response.get("status") in valid_statuses:
            shares = float(response.get("takingAmount") or 0)
            spent  = float(response.get("makingAmount") or 0)

            if shares == 0 and response.get("status") == "live":
                shares       = round(size / price, 6)
                actual_entry = price
                print(f"  ✅ GTC ORDER LIVE: {shares:.4f} shares @ {actual_entry:.4f}")
                return True, shares, actual_entry

            if shares <= 0:
                print(f"  ❌ Ghost fill — matched but 0 shares received")
                return False, 0, 0

            actual_entry = round(spent / shares, 4)
            if actual_entry <= 0 or actual_entry >= 0.999:
                print(f"  ❌ Bad fill — actual entry {actual_entry:.4f} is invalid")
                return False, 0, 0

            slippage = actual_entry - price
            if slippage > 0.05:
                print(f"  ⚠️  High slippage: quoted {price:.3f}, got {actual_entry:.3f} (+{slippage:.3f})")

            print(f"  ✅ SHARES BOUGHT: {shares:.4f} @ actual entry {actual_entry:.4f} | Breathe, nothing is 100%")
            return True, shares, actual_entry
        else:
            print(f"  ❌ Order failed: {response.get('error') or output.get('error')}")
            return False, 0, 0

    except Exception as e:
        print(f"  ❌ Order failed: {e}")
        return False, 0, 0

# ============================================================
# REDEMPTION
# ============================================================

async def run_claimer():
    await asyncio.sleep(30)
    while True:
        try:
            proc = await asyncio.create_subprocess_exec(
                "py", "-3.12", "claimer.py", "--batch", "20",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=os.path.dirname(os.path.abspath(__file__)),
                env={**os.environ, "PYTHONIOENCODING": "utf-8"}
            )
            stdout, stderr = await proc.communicate()
            output  = (stdout or b"").decode("utf-8", errors="replace")
            output += (stderr or b"").decode("utf-8", errors="replace")

            claimed = 0
            failed  = 0
            found   = 0
            for line in output.splitlines():
                line = line.strip()
                if not line: continue
                if "Found" in line and "redeemable" in line:
                    try:
                        found = int(line.split("Found")[1].split("redeemable")[0].strip())
                    except: pass
                    print(f"  💰 CLAIMER: {found} position(s) to redeem")
                elif "✓ Claimed" in line or ("Claimed" in line and "tx=" in line):
                    claimed += 1
                elif "Failed" in line:
                    failed += 1
                elif "Batch done" in line:
                    print(f"  💰 CLAIMER: ✅ {claimed} claimed{f', ❌ {failed} failed' if failed else ''}")
                elif "No redeemable" in line:
                    print(f"  💰 CLAIMER: nothing to claim")
                elif "ERROR" in line:
                    print(f"  💰 CLAIMER: ⚠️ {line.split('ERROR')[-1].strip()}")

            if proc.returncode != 0 and found == 0 and claimed == 0:
                print(f"  💰 CLAIMER: exited with code {proc.returncode}")
        except Exception as e:
            print(f"[{ts()}] ⚠️  Claimer error: {e}")
        await asyncio.sleep(120)

# ============================================================
# TRADE GATE (CHANGE #2, #5)
# ============================================================

def should_take_trade(asset, direction, entry, confidence):
    if not is_trading_allowed():
        print(f"  ❌ Outside trading hours — skipping")
        return False

    # CHANGE #2: hard entry floor for all signal trades
    if entry < MIN_SIGNAL_ENTRY:
        print(f"  ❌ Entry {entry:.3f} below floor {MIN_SIGNAL_ENTRY} — skipping")
        return False

    if entry < 0.25 and "PRICE STRONG" not in confidence:
        print(f"  ❌ Ghost/invalid entry {entry:.3f} — skipping")
        return False

    max_entry = ASSET_MAX_ENTRY.get(asset, MAX_ENTRY) if 'ASSET_MAX_ENTRY' in dir() else MAX_ENTRY
    if entry > max_entry:
        print(f"  ❌ Entry {entry:.3f} > max {max_entry:.3f} for {asset} — skipping")
        return False

    if asset == "BTC":
        if "PRICE STRONG" in confidence:
            return True
        print(f"  ❌ BTC non-PRICE-STRONG ({confidence}) — skipping")
        return False

    if asset == "ETH":
        if "PRICE STRONG" in confidence:
            return True
        print(f"  ❌ ETH non-PRICE-STRONG ({confidence}) — skipping")
        return False

    if asset == "XRP":
        if "PRICE STRONG" not in confidence:
            print(f"  ❌ XRP non-PRICE-STRONG ({confidence}) — skipping")
            return False
        return True

    if "PRICE STRONG" in confidence:
        return True

    print(f"  ❌ {asset} non-PRICE-STRONG ({confidence}) — skipping")
    return False


ASSET_MAX_ENTRY = {
    "BTC": 0.97,
    "ETH": 0.97,
    "XRP": 0.97,
    "SOL": 0.97,
}

# ============================================================
# P&L TRACKER
# ============================================================

def _resolve_cycle(pos, won):
    if pos.get("is_hedge"):
        return
    asset_key         = pos["asset"]
    winning_direction = pos["direction"] if won else (
        "Down" if pos["direction"] == "Up" else "Up"
    )
    # Record to cycle stats (observation only — no longer trades on it)
    if asset_key in cycle_stats:
        last = cycle_stats[asset_key].get("last_outcome")
        if last is not None:
            bucket = "after_up" if last == "Up" else "after_down"
            key    = "up" if winning_direction == "Up" else "down"
            cycle_stats[asset_key][bucket][key] += 1
        cycle_stats[asset_key]["last_outcome"] = winning_direction
        save_cycle_stats(cycle_stats, CYCLE_STATS_FILE)

    # CHANGE #6: update rolling win rate
    record_rolling_outcome(asset_key, won)

    # ML feedback
    if ml_filter is not None and pos.get("ml_trade_dict"):
        try:
            if hasattr(ml_filter, "record_outcome"):
                ml_filter.record_outcome(pos["ml_trade_dict"], won)
        except Exception as e:
            print(f"  ⚠️  ML feedback error: {e}")


async def check_resolved_positions(session):
    global daily_pnl, open_positions, wins, losses

    resolved = []
    for token_id, pos in list(open_positions.items()):
        if time.time() < pos.get("end_time", 0):
            continue

        is_paper  = PAPER_TRADE
        hedge_tag = " [HEDGE]" if pos.get("is_hedge") else ""

        try:
            async with session.get(
                f"{GAMMA_API}/{pos['slug']}",
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json()
        except Exception as e:
            print(f"  ⚠️  Position check error: {e}")
            continue

        if not data.get("closed"):
            continue

        token_ids = json.loads(data.get("clobTokenIds", "[]"))
        prices = (
            json.loads(data.get("outcomePrices", "[]"))
            if isinstance(data.get("outcomePrices"), str)
            else data.get("outcomePrices", [])
        )

        winning_token_id = None
        for i, price in enumerate(prices):
            if float(price) >= 0.99 and i < len(token_ids):
                winning_token_id = token_ids[i]
                break

        if winning_token_id is None:
            continue

        won = (token_id == winning_token_id)
        shadow_resolve(token_id, won, size=pos.get("size", TRADE_SIZE), shares=pos.get("shares", 0.0))

        if won:
            shares = pos.get("shares", pos["size"] / pos["entry"])
            profit = shares - pos["size"]
            daily_pnl += profit
            wins += 1
            mode_tag = "[PAPER] " if is_paper else ""
            print(f"\n✅ {mode_tag}WIN [{ts()}] {pos['asset']} {pos['direction']}{hedge_tag} | "
                  f"+${profit:.2f} | Record: {wins}W/{losses}L | P&L: ${daily_pnl:+.2f}")
        else:
            daily_pnl -= pos["size"]
            losses += 1
            mode_tag = "[PAPER] " if is_paper else ""
            print(f"\n❌ {mode_tag}LOSS [{ts()}] {pos['asset']} {pos['direction']}{hedge_tag} | "
                  f"-${pos['size']:.2f} | Record: {wins}W/{losses}L | P&L: ${daily_pnl:+.2f}")

        _resolve_cycle(pos, won)
        resolved.append(token_id)
        hedged_positions.pop(token_id, None)

    for token_id in resolved:
        open_positions.pop(token_id, None)

# ============================================================
# CYCLE STATS PRINT (simplified — observation only)
# ============================================================

def print_cycle_stats():
    lines = [f"\n📊 CYCLE STATS (observation) [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"]
    any_data = False
    for asset in ["BTC", "ETH", "XRP", "SOL"]:
        s = cycle_stats.get(asset, {})
        for bucket in ["after_up", "after_down"]:
            b     = s.get(bucket, {"up": 0, "down": 0})
            total = b["up"] + b["down"]
            if total >= 5:
                any_data = True
                lines.append(
                    f"  {asset} {bucket}: "
                    f"UP {b['up']/total:.0%} | DOWN {b['down']/total:.0%} | n={total}"
                )

    # Rolling WR status
    lines.append("\n📊 ROLLING WIN RATES (last 10 trades per asset):")
    for asset in ["BTC", "ETH", "XRP", "SOL"]:
        wr = get_rolling_wr(asset)
        dq = rolling_outcomes.get(asset, deque())
        n  = len(dq)
        if n == 0:
            lines.append(f"  {asset}: no data yet")
        elif wr is None:
            lines.append(f"  {asset}: {n}/{ROLLING_WR_WINDOW} trades collected")
        else:
            status = "✅" if wr >= ROLLING_WR_MIN else "🔴 GATED"
            lines.append(f"  {asset}: {wr:.0%} ({n} trades) {status}")

    if not any_data:
        lines.append("  (not enough cycle data yet — need 5+ cycles per bucket)")
    print("\n".join(lines))
    print_shadow_summary()

# ============================================================
# PASSIVE CYCLE TRACKER (simplified — just records outcomes)
# ============================================================

async def passive_cycle_tracker(session):
    while True:
        await asyncio.sleep(60)
        for market in MARKETS:
            for offset in [1, 2, 3]:
                ts_val = get_current_timestamp(market["interval"]) - (offset * market["interval"])
                slug   = f"{market['slug']}-{ts_val}"
                if slug in passive_cycle_seen:
                    continue
                try:
                    async with session.get(
                        f"{GAMMA_API}/{slug}",
                        timeout=aiohttp.ClientTimeout(total=8)
                    ) as resp:
                        if resp.status != 200:
                            continue
                        data = await resp.json()
                except:
                    continue
                if not data.get("closed"):
                    continue
                token_ids = json.loads(data.get("clobTokenIds", "[]"))
                prices = (
                    json.loads(data.get("outcomePrices", "[]"))
                    if isinstance(data.get("outcomePrices"), str)
                    else data.get("outcomePrices", [])
                )
                winning_token_id = None
                for i, price in enumerate(prices):
                    if float(price) >= 0.99 and i < len(token_ids):
                        winning_token_id = token_ids[i]
                        break
                if winning_token_id is None:
                    continue
                outcome = "Up" if winning_token_id == token_ids[0] else "Down"
                passive_cycle_seen.add(slug)

# ============================================================
# ARB / HEDGE SCANNER (CHANGE: MIN_GUARANTEED raised to 1.20)
# ============================================================

def calculate_arb_hedge(shares_main: float, opp_ask: float, stake_main: float):
    raw_hedge_dollar = shares_main * opp_ask
    hedge_dollar     = min(raw_hedge_dollar, ARB_MAX_HEDGE)
    hedge_shares     = hedge_dollar / opp_ask
    hedge_shares     = min(hedge_shares, shares_main)
    hedge_dollar     = hedge_shares * opp_ask
    is_partial       = round(hedge_shares, 6) < round(shares_main, 6)
    total_spent      = stake_main + hedge_dollar
    guaranteed_profit = round(hedge_shares - total_spent, 4)

    if guaranteed_profit > ARB_MIN_GUARANTEED:
        arb_type = "PARTIAL_ARB" if is_partial else "TRUE_ARB"
        return arb_type, hedge_dollar, hedge_shares, guaranteed_profit, is_partial

    return "NONE", 0.0, 0.0, 0.0, False


async def check_arb_opportunities(session, market_infos):
    if not ARB_SCAN_ENABLED or not open_positions:
        return

    slug_map = {info["slug"]: info for info in market_infos}

    for token_id, pos in list(open_positions.items()):
        if pos.get("is_hedge"):
            continue
        if token_id in hedged_positions:
            continue
        if arb_attempted.get(token_id, 0) > time.time() - ARB_COOLDOWN:
            continue

        slug      = pos.get("slug", "")
        direction = pos.get("direction", "")
        entry     = pos.get("entry", 0.0)
        stake     = pos.get("size", TRADE_SIZE)
        asset     = pos.get("asset", "")
        shares_main = pos.get("shares", stake / entry if entry > 0 else 0)
        if shares_main <= 0:
            continue

        info = slug_map.get(slug)
        if not info:
            continue

        opp_direction = "Down" if direction == "Up" else "Up"
        opp_token_id  = info["down_token"] if direction == "Up" else info["up_token"]
        if not opp_token_id:
            continue

        secs_left = int(pos["end_time"] - time.time())
        if secs_left < 10:
            continue

        ob      = await fetch_orderbook(session, opp_token_id)
        opp_ask = get_best_ask(ob)
        if not opp_ask:
            continue

        arb_type, hedge_dollar, hedge_shares, guaranteed_profit, is_partial = \
            calculate_arb_hedge(shares_main, opp_ask, stake)
        if arb_type == "NONE":
            continue

        # Double-check with fresh price
        ob_live      = await fetch_orderbook(session, opp_token_id)
        opp_ask_live = get_best_ask(ob_live)
        if not opp_ask_live:
            continue

        arb_type, hedge_dollar, hedge_shares, guaranteed_profit, is_partial = \
            calculate_arb_hedge(shares_main, opp_ask_live, stake)
        if arb_type == "NONE":
            continue

        raw_hedge_dollar = shares_main * opp_ask_live
        label = "🎯 TRUE ARB" if not is_partial else "📐 PARTIAL ARB"

        print(f"\n{'='*55}")
        print(f"{label} [{ts()}] {asset}")
        print(f"  Held:       {direction} @ {entry:.3f} | {shares_main:.4f} shares | cost ${stake:.4f}")
        print(f"  Hedge:      {opp_direction} @ {opp_ask_live:.3f} | {hedge_shares:.4f} shares"
              f"{f' (capped — full hedge ${raw_hedge_dollar:.2f})' if is_partial else ''}"
              f" | cost ${hedge_dollar:.4f}")
        print(f"  Guaranteed: ${guaranteed_profit:.4f}"
              f"{'  (partial)' if is_partial else ''}")
        print(f"  Time left:  {secs_left}s")

        arb_attempted[token_id] = time.time()

        success, shares_filled, actual_entry = await place_order(
            session, opp_token_id, opp_ask_live, hedge_dollar,
            opp_direction, asset, bypass_min=True, gtc=True,
        )

        if success:
            hedged_positions[token_id] = {
                "opp_token_id":      opp_token_id,
                "opp_direction":     opp_direction,
                "hedge_entry":       actual_entry,
                "hedge_shares":      shares_filled,
                "hedge_dollar":      hedge_dollar,
                "guaranteed_profit": guaranteed_profit,
                "is_partial":        is_partial,
            }
            open_positions[opp_token_id] = {
                "asset":         asset,
                "direction":     opp_direction,
                "entry":         actual_entry,
                "size":          hedge_dollar,
                "shares":        shares_filled,
                "time":          time.time(),
                "end_time":      pos["end_time"],
                "slug":          slug,
                "condition_id":  info.get("condition_id", ""),
                "is_hedge":      True,
                "ml_trade_dict": None,
            }
            try:
                conf_label = "PARTIAL_ARB_HEDGE" if is_partial else "ARB_HEDGE"
                log_trade(asset, opp_direction, actual_entry, hedge_dollar,
                          f"{conf_label} (main {direction} @ {entry:.3f}, "
                          f"{shares_main:.4f} shares, hedged {hedge_shares:.4f})",
                          opp_token_id, quoted_ask=opp_ask_live)
            except Exception as e:
                print(f"  ⚠️ Hedge log failed: {e}")
            print(f"  ✅ Hedge placed — locked in ${guaranteed_profit:.4f}")
        else:
            print(f"  ❌ Hedge order failed")
        print(f"{'='*55}")

# ============================================================
# TRADING LOOP
# ============================================================

async def trading_loop(session):
    global daily_pnl, peak_pnl, last_stats_print

    print("Fetching market token IDs...")
    market_infos = []
    while not market_infos:
        for market in MARKETS:
            info = await fetch_market_tokens(session, market)
            if info and info["up_token"]:
                market_infos.append(info)
                print(f"  + {info['asset']} | {info['slug']}")
        if not market_infos:
            print("  No markets fetched -- retrying in 5s...")
            await asyncio.sleep(5)

    print(f"\nWatching {len(market_infos)} markets...\n")

    last_refresh        = time.time()
    last_position_check = time.time()

    while True:
        if daily_pnl <= -MAX_DAILY_LOSS:
            print(f"🛑 Daily loss limit hit — stopping")
            break

        now = time.time()

        if now - last_stats_print > 900:
            print_cycle_stats()
            last_stats_print = now

        if now - last_position_check > 15:
            await check_resolved_positions(session)
            last_position_check = now

        if now - last_refresh > 30:
            fetch_tasks = [fetch_market_tokens(session, m) for m in MARKETS]
            results     = await asyncio.gather(*fetch_tasks, return_exceptions=True)
            new_infos     = []
            slugs_changed = False
            for info in results:
                if isinstance(info, Exception) or not info or not info["up_token"]:
                    continue
                new_infos.append(info)
                existing = next(
                    (m for m in market_infos if m["asset"] == info["asset"]), None
                )
                if existing and existing["slug"] != info["slug"]:
                    slugs_changed = True
            if new_infos:
                market_infos = new_infos
                if slugs_changed:
                    previous_ask_sizes.clear()
                    recent_sweeps.clear()
                    reprice_candidates.clear()
                    if PAPER_TRADE:
                        open_positions.clear()
                    print(f"[{ts()}] 🔄 New market cycle — sizes reset")
                else:
                    print(f"[{ts()}] ✅ Markets refreshed")
            last_refresh = now

        # ── Parallel orderbook fetch ───────────────────────────────────
        fetch_tasks = []
        fetch_meta  = []
        for info in market_infos:
            secs_left = seconds_until(info["end_date"])
            if secs_left is None or secs_left < MIN_TIME_LEFT or secs_left > MAX_TIME_LEFT:
                continue
            for direction, token_id in [("Up", info["up_token"]), ("Down", info["down_token"])]:
                if not token_id:
                    continue
                fetch_tasks.append(fetch_orderbook(session, token_id))
                fetch_meta.append((info, direction, f"{info['slug']}_{direction}"))

        orderbook_results = await asyncio.gather(*fetch_tasks, return_exceptions=True)

        market_orderbooks = {}
        for (info, direction, key), ob in zip(fetch_meta, orderbook_results):
            market_orderbooks[key] = None if isinstance(ob, Exception) else ob

        cycle_signals = []

        for info in market_infos:
            asset     = info["asset"]
            slug      = info["slug"]
            secs_left = seconds_until(info["end_date"])
            end_time  = info["end_time"]

            if secs_left is None or secs_left < MIN_TIME_LEFT or secs_left > MAX_TIME_LEFT:
                continue

            up_sweep      = None
            down_sweep    = None
            up_best_ask   = None
            down_best_ask = None

            sweep_threshold = SWEEP_THRESHOLD if asset == "BTC" else SWEEP_THRESHOLD_ALT

            for direction, token_id in [
                ("Up",   info["up_token"]),
                ("Down", info["down_token"])
            ]:
                if not token_id:
                    continue
                key          = f"{slug}_{direction}"
                orderbook    = market_orderbooks.get(key)
                current_size = get_ask_size_at_price(orderbook, "0.99")
                best_ask     = get_best_ask(orderbook)
                prev_size    = previous_ask_sizes.get(key, None)

                if direction == "Up":
                    up_best_ask = best_ask
                else:
                    down_best_ask = best_ask

                if prev_size is not None:
                    drop = prev_size - current_size
                    if drop >= sweep_threshold:
                        if direction == "Up":
                            up_sweep = drop
                        else:
                            down_sweep = drop

                previous_ask_sizes[key] = current_size

            now_ts = time.time()
            if up_sweep:
                recent_sweeps[f"{slug}_Up"] = now_ts
            if down_sweep:
                recent_sweeps[f"{slug}_Down"] = now_ts

            up_recent   = recent_sweeps.get(f"{slug}_Up",   0)
            down_recent = recent_sweeps.get(f"{slug}_Down", 0)

            asset_binance = next(
                (k for k, v in ASSETS.items() if v == asset), None
            )

            if (up_sweep or down_sweep):
                both_within_window = (
                    up_recent   > now_ts - BOTH_SIDES_WINDOW and
                    down_recent > now_ts - BOTH_SIDES_WINDOW
                )
                if both_within_window:
                    flow     = flow_signals.get(asset_binance, "NEUTRAL")
                    strength = flow_strength.get(asset_binance, "NEUTRAL")
                    strong_up   = (flow == "UP"   and strength in ("STRONG", "MODERATE"))
                    strong_down = (flow == "DOWN" and strength in ("STRONG", "MODERATE"))
                    if strong_up and up_recent > down_recent:
                        pass
                    elif strong_down and down_recent > up_recent:
                        pass
                    else:
                        print(f"[{ts()}] {asset} both sides swept within {BOTH_SIDES_WINDOW}s, no strong flow — skipping")
                        continue

            for k in list(recent_sweeps.keys()):
                if isinstance(recent_sweeps[k], float) and now_ts - recent_sweeps[k] > 90:
                    recent_sweeps.pop(k, None)

            for direction, sweep, best_ask in [
                ("Up",   up_sweep,   up_best_ask),
                ("Down", down_sweep, down_best_ask)
            ]:
                if not sweep:
                    continue
                flow     = flow_signals.get(asset_binance, "NEUTRAL")
                strength = flow_strength.get(asset_binance, "NEUTRAL")
                if not best_ask:
                    continue
                if not sweep_confirmed_by_price(asset_binance, direction):
                    print(f"[{ts()}] ⚠️  Sweep {asset} {direction} — no price confirmation, skipping")
                    continue

                cycle_signals.append({
                    "asset":         asset,
                    "slug":          slug,
                    "direction":     direction,
                    "drop":          sweep,
                    "secs_left":     secs_left,
                    "end_time":      end_time,
                    "token_id":      info[f"{direction.lower()}_token"],
                    "best_ask":      best_ask,
                    "flow_signal":   flow,
                    "flow_strength": strength,
                })

        # ── Process sweep signals ──────────────────────────────────────
        traded_this_cycle = set()
        for sig in cycle_signals:
            now_str   = ts()
            flow      = sig["flow_signal"]
            strength  = sig["flow_strength"]
            direction = sig["direction"]
            secs_left = sig["secs_left"]
            asset     = sig["asset"]

            trade_key = f"{sig['slug']}_{direction}"
            if trade_key in traded_this_cycle:
                continue
            opp_key = f"{sig['slug']}_{'Down' if direction == 'Up' else 'Up'}"
            if opp_key in traded_this_cycle:
                continue
            if sig["token_id"] in open_positions:
                continue
            if any(pos.get("slug") == sig["slug"] and not pos.get("is_hedge")
                   for pos in open_positions.values()):
                continue

            if not PAPER_TRADE and sig["best_ask"] > MAX_ENTRY:
                continue

            if time.time() < spoof_lockout_until:
                print(f"[{now_str}] ⚠️  Spoof lockout — skipping {asset} {direction}")
                continue

            flow_confirms = (
                (direction == "Up"   and flow == "UP") or
                (direction == "Down" and flow == "DOWN")
            )
            flow_conflicts = (
                (direction == "Up"   and flow == "DOWN") or
                (direction == "Down" and flow == "UP")
            )

            if flow_conflicts and strength == "STRONG":
                print(f"[{now_str}] ⚠️  Flow CONFLICTS strongly — skipping {asset} {direction}")
                continue

            asset_binance = next(
                (k for k, v in ASSETS.items() if v == asset), None
            )

            edge_cfg = get_edge_settings(asset)
            price_dir, price_edge, price_label = get_price_signal(asset_binance, secs_left, asset)
            path_score, path_label = get_path_resistance(asset_binance, direction, secs_left)

            price_confirms  = (price_dir == direction and price_edge >= edge_cfg["edge_confirm"])
            price_conflicts = (
                price_dir is not None and
                price_dir != direction and
                price_edge >= edge_cfg["edge_conflict"]
            )

            if price_conflicts:
                print(f"[{now_str}] ⚠️  Price CONFLICTS sweep — skipping {asset} {direction}")
                continue

            score = 0
            if flow_confirms and strength == "STRONG":  score += 3
            elif flow_confirms:                         score += 1
            if price_confirms and price_edge >= edge_cfg["edge_min"]: score += 3
            elif price_confirms:                        score += 1
            if path_score >= 2:    score += 2
            elif path_score >= 1:  score += 1
            elif path_score <= -2: score -= 2
            elif path_score <= -1: score -= 1

            if path_score <= -2 and price_conflicts:
                print(f"[{now_str}] ⚠️  Path + price both conflict — skipping {asset} {direction}")
                continue

            if score >= 5:
                confidence = "🔥 HIGH"
            elif score >= 2:
                confidence = "✅ MEDIUM"
            elif score >= 0:
                confidence = "⚪ LOW (no confirmation)"
            else:
                confidence = "⚠️  CONFLICTED"

            if score < 2:
                continue

            print(f"\n{'='*55}")
            print(f"🚨 SIGNAL [{now_str}]")
            print(f"  Asset:      {asset} {direction}")
            print(f"  Swept:      {sig['drop']:,.0f} shares at 0.99")
            print(f"  Time left:  {secs_left}s")
            print(f"  Entry:      {sig['best_ask']:.2f}")
            print(f"  Flow:       {flow} ({strength})")
            print(f"  Price:      {price_label}")
            print(f"  Path:       {path_label}")
            print(f"  Score:      {score}/9")
            print(f"  Confidence: {confidence}")
            print(f"  Est profit: ${TRADE_SIZE/sig['best_ask'] - TRADE_SIZE:.2f} if correct")
            print(f"  Stats:      {wins}W/{losses}L | P&L: ${daily_pnl:+.2f}")

            if "⚠️  CONFLICTED" in confidence:
                print(f"  → Skipping conflicted signal")
                print(f"{'='*55}")
                continue

            if len(open_positions) >= MAX_OPEN_POSITIONS:
                print(f"  → Max positions reached")
                print(f"{'='*55}")
                continue

            entry_price = sig["best_ask"]
            asset_name  = sig["asset"]

            if not should_take_trade(asset=asset_name, direction=direction,
                                     entry=entry_price, confidence=confidence):
                print(f"  ⏭️ Trade filtered out by rules\n{'='*55}")
                continue

            # ML filter
            sweep_td = build_trade_dict(
                asset=asset_name, direction=direction,
                entry=entry_price, market_ask=sig["best_ask"],
                confidence=confidence, size=TRADE_SIZE,
                secs_left=secs_left, score=score,
                flow=flow, strength=strength,
                price_edge=price_edge, path_score=path_score,
            )
            ml_take, ml_prob = ml_check(sweep_td, label=f"sweep {asset_name} {direction}")
            if not ml_take:
                print(f"  🤖 ML blocked — prob={ml_prob:.2f}\n{'='*55}")
                shadow_log(sig["token_id"], sweep_td, ml_prob)
                continue
            if ml_filter:
                print(f"  🤖 ML approved — prob={ml_prob:.2f}")

            if entry_price > REPRICE_MAX_ENTRY:
                sweep_reprice_key = f"{sig['slug']}_{direction}_sweep"
                if sweep_reprice_key not in reprice_candidates and sweep_reprice_key not in traded_this_cycle:
                    print(f"[{ts()}] ⏳ SWEEP {asset} {direction} ask {entry_price:.2f} > {REPRICE_MAX_ENTRY} — watching")
                    reprice_candidates[sweep_reprice_key] = {
                        "token_id":      sig["token_id"],
                        "direction":     direction,
                        "asset":         asset,
                        "slug":          sig["slug"],
                        "end_time":      sig["end_time"],
                        "expires":       time.time() + REPRICE_TIMEOUT,
                        "original_ask":  entry_price,
                        "confidence":    confidence,
                        "condition_id":  info.get("condition_id", ""),
                        "ml_trade_dict": sweep_td,
                    }
                print(f"{'='*55}")
                continue

            ob_fresh  = await fetch_orderbook(session, sig["token_id"])
            ask_fresh = get_best_ask(ob_fresh)
            if not ask_fresh:
                print(f"{'='*55}")
                continue
            if ask_fresh > entry_price + 0.05:
                if ask_fresh <= REPRICE_MAX_ENTRY:
                    print(f"  ⚠️  Ask moved {entry_price:.2f} → {ask_fresh:.2f} — still acceptable")
                    entry_price = ask_fresh
                else:
                    print(f"  ⚠️  Ask moved {entry_price:.2f} → {ask_fresh:.2f} — too high, skipping")
                    print(f"{'='*55}")
                    continue
            else:
                entry_price = ask_fresh

            success, shares_filled, actual_entry = await place_order(
                session, sig["token_id"], entry_price, TRADE_SIZE, direction, asset_name
            )

            if not success:
                print("  ❌ Order did NOT fill — skipping\n{'='*55}")
                continue

            traded_this_cycle.add(trade_key)
            try:
                log_trade(sig["asset"], direction, actual_entry, TRADE_SIZE,
                          confidence, sig["token_id"], quoted_ask=sig["best_ask"])
            except Exception as e:
                print(f"  ⚠️ Log failed: {e}")

            sweep_td["entry"]      = actual_entry
            sweep_td["market_ask"] = sig["best_ask"]

            open_positions[sig["token_id"]] = {
                "asset":         sig["asset"],
                "direction":     direction,
                "entry":         actual_entry,
                "size":          TRADE_SIZE,
                "shares":        shares_filled,
                "time":          time.time(),
                "end_time":      sig["end_time"],
                "slug":          sig["slug"],
                "condition_id":  info.get("condition_id", ""),
                "ml_trade_dict": sweep_td,
            }
            print(f"{'='*55}")

        # ── Standalone price signals + reprice ─────────────────────────
        if not (time.time() < spoof_lockout_until):
            positioned_slugs = {
                pos.get("slug", "") for pos in open_positions.values()
                if not pos.get("is_hedge")
            }

            # ── Reprice checker (CHANGE #3) ────────────────────────────
            now = time.time()
            for key, rc in list(reprice_candidates.items()):
                expired_buy = False
                expired_ask = None

                # CHANGE #3: only reprice within REPRICE_MAX_SECS_LEFT of expiry
                secs_left_now = int(rc["end_time"] - now)
                if secs_left_now > REPRICE_MAX_SECS_LEFT:
                    continue  # too early — wait until we're close to expiry

                if now > rc["expires"]:
                    if secs_left_now > MIN_TIME_LEFT:
                        ob  = await fetch_orderbook(session, rc["token_id"])
                        ask = get_best_ask(ob)
                        if ask and ask <= MAX_ENTRY:
                            print(f"[{ts()}] ⏳ Reprice expired — signal held, buying at {ask:.2f}")
                            reprice_candidates.pop(key, None)
                            expired_buy = True
                            expired_ask = ask
                        else:
                            print(f"[{ts()}] ⌛ Reprice expired: {rc['asset']} {rc['direction']}")
                            reprice_candidates.pop(key, None)
                            continue
                    else:
                        print(f"[{ts()}] ⌛ Reprice expired: {rc['asset']} {rc['direction']}")
                        reprice_candidates.pop(key, None)
                        continue

                if key in traded_this_cycle:
                    reprice_candidates.pop(key, None)
                    continue
                if rc["token_id"] in open_positions:
                    reprice_candidates.pop(key, None)
                    continue
                if rc["slug"] in positioned_slugs:
                    reprice_candidates.pop(key, None)
                    continue

                if secs_left_now < MIN_TIME_LEFT:
                    reprice_candidates.pop(key, None)
                    continue

                if expired_buy:
                    ask = expired_ask
                else:
                    ob  = await fetch_orderbook(session, rc["token_id"])
                    ask = get_best_ask(ob)
                    if not ask or ask > REPRICE_MAX_ENTRY:
                        continue

                if ask < REPRICE_MIN_ENTRY:
                    print(f"[{ts()}] ⬇️  Reprice ask {ask:.2f} below min {REPRICE_MIN_ENTRY} — skipping")
                    reprice_candidates.pop(key, None)
                    continue

                ob_fresh  = await fetch_orderbook(session, rc["token_id"])
                ask_fresh = get_best_ask(ob_fresh)
                if not ask_fresh:
                    reprice_candidates.pop(key, None)
                    continue
                if ask_fresh > ask + 0.05:
                    if ask_fresh <= REPRICE_MAX_ENTRY:
                        ask = ask_fresh
                    else:
                        reprice_candidates.pop(key, None)
                        continue
                else:
                    ask = ask_fresh

                print(f"\n{'='*55}")
                print(f"💰 REPRICED [{ts()}] {rc['asset']} {rc['direction']}")
                print(f"  Original ask: {rc['original_ask']:.2f} → Now: {ask:.2f}")
                print(f"  Time left: {secs_left_now}s")

                if not should_take_trade(rc["asset"], rc["direction"], ask, rc["confidence"]):
                    print(f"  ⏭️ Repriced trade filtered by rules\n{'='*55}")
                    reprice_candidates.pop(key, None)
                    continue

                reprice_td = rc.get("ml_trade_dict") or build_trade_dict(
                    asset=rc["asset"], direction=rc["direction"],
                    entry=ask, market_ask=rc["original_ask"],
                    confidence=rc["confidence"] + " (REPRICED)",
                    size=PRICE_SIGNAL_TRADE_SIZE, secs_left=secs_left_now,
                )
                reprice_td["entry"]      = ask
                reprice_td["market_ask"] = rc["original_ask"]
                reprice_td["confidence"] = rc["confidence"] + " (REPRICED)"

                ml_take, ml_prob = ml_check(reprice_td, label=f"reprice {rc['asset']} {rc['direction']}")
                if not ml_take:
                    print(f"  🤖 ML blocked reprice — prob={ml_prob:.2f}\n{'='*55}")
                    shadow_log(rc["token_id"], reprice_td, ml_prob)
                    reprice_candidates.pop(key, None)
                    continue
                if ml_filter:
                    print(f"  🤖 ML approved reprice — prob={ml_prob:.2f}")

                success, shares_filled, actual_entry = await place_order(
                    session, token_id=rc["token_id"], price=ask,
                    size=PRICE_SIGNAL_TRADE_SIZE, direction=rc["direction"], asset=rc["asset"],
                )

                if success:
                    actual_cost = max(PRICE_SIGNAL_TRADE_SIZE, 5.0)
                    traded_this_cycle.add(key)
                    positioned_slugs.add(rc["slug"])
                    reprice_td["entry"]      = actual_entry
                    reprice_td["market_ask"] = rc["original_ask"]
                    try:
                        log_trade(rc["asset"], rc["direction"], actual_entry, actual_cost,
                                  rc["confidence"] + " (REPRICED)", rc["token_id"], quoted_ask=ask)
                    except: pass
                    open_positions[rc["token_id"]] = {
                        "asset":         rc["asset"],
                        "direction":     rc["direction"],
                        "entry":         actual_entry,
                        "size":          actual_cost,
                        "shares":        shares_filled,
                        "time":          now,
                        "end_time":      rc["end_time"],
                        "slug":          rc["slug"],
                        "condition_id":  rc.get("condition_id", ""),
                        "ml_trade_dict": reprice_td,
                    }
                    print(f"{'='*55}")
                else:
                    print(f"  ❌ Repriced order did not fill\n{'='*55}")

                reprice_candidates.pop(key, None)

            # ── Arb scanner ────────────────────────────────────────────
            await check_arb_opportunities(session, market_infos)

            # ── New price signals (CHANGE #2, #5, #6) ─────────────────
            for info in market_infos:
                secs_left = seconds_until(info["end_date"])
                if secs_left is None or secs_left < MIN_TIME_LEFT or secs_left > MAX_TIME_LEFT:
                    continue

                # CHANGE #5: don't fire price signals too close to window boundary
                if secs_left < MIN_SECS_LEFT_PRICE:
                    continue

                asset         = info["asset"]
                asset_binance = next(
                    (k for k, v in ASSETS.items() if v == asset), None
                )

                edge_cfg  = get_edge_settings(asset)
                price_dir, price_edge, price_label = get_price_signal(asset_binance, secs_left, asset)

                if price_dir is None or price_edge < edge_cfg["edge_min"]:
                    continue

                # CHANGE #6: rolling win rate gate (price signals only)
                if not rolling_wr_allows_price_signal(asset):
                    continue

                path_score, path_label = get_path_resistance(asset_binance, price_dir, secs_left)
                if path_score <= -2:
                    continue

                price_token_id  = info["up_token"] if price_dir == "Up" else info["down_token"]
                price_trade_key = f"{info['slug']}_{price_dir}"
                price_opp_key   = f"{info['slug']}_{'Down' if price_dir == 'Up' else 'Up'}"

                if price_trade_key in traded_this_cycle:
                    continue
                if price_opp_key in traded_this_cycle:
                    continue
                if price_token_id in open_positions:
                    continue
                if info["slug"] in positioned_slugs:
                    continue

                ob       = await fetch_orderbook(session, price_token_id)
                best_ask = get_best_ask(ob)

                # CHANGE #2: apply MIN_SIGNAL_ENTRY floor here too
                if not best_ask or best_ask < MIN_SIGNAL_ENTRY:
                    continue

                if best_ask > REPRICE_MAX_ENTRY:
                    if price_trade_key not in reprice_candidates and price_trade_key not in traded_this_cycle:
                        print(f"[{ts()}] ⏳ {asset} {price_dir} ask {best_ask:.2f} > {REPRICE_MAX_ENTRY} — watching")
                        reprice_candidates[price_trade_key] = {
                            "token_id":      price_token_id,
                            "direction":     price_dir,
                            "asset":         asset,
                            "slug":          info["slug"],
                            "end_time":      info["end_time"],
                            "expires":       time.time() + REPRICE_TIMEOUT,
                            "original_ask":  best_ask,
                            "confidence":    f"PRICE {price_label}",
                            "condition_id":  info.get("condition_id", ""),
                            "ml_trade_dict": None,
                        }
                    continue

                if not PAPER_TRADE and best_ask > MAX_ENTRY:
                    continue

                flow     = flow_signals.get(asset_binance, "NEUTRAL")
                strength = flow_strength.get(asset_binance, "NEUTRAL")
                flow_conflicts = (
                    (price_dir == "Up"   and flow == "DOWN") or
                    (price_dir == "Down" and flow == "UP")
                )
                if flow_conflicts and strength == "STRONG":
                    continue

                end_time   = info["end_time"]
                trade_size = PRICE_SIGNAL_TRADE_SIZE

                print(f"\n{'='*55}")
                print(f"📈 PRICE SIGNAL [{ts()}]")
                print(f"  Asset:      {asset} {price_dir}")
                print(f"  Price:      {price_label}")
                print(f"  Path:       {path_label}")
                print(f"  Edge:       {price_edge:.3f} | PathScore: {path_score:.0f}")
                print(f"  Entry:      {best_ask:.2f}")
                print(f"  Time left:  {secs_left}s")
                print(f"  Flow:       {flow} ({strength})")
                print(f"  Size:       ${trade_size}")
                print(f"  Rolling WR: {get_rolling_wr(asset):.0%}" if get_rolling_wr(asset) else
                      f"  Rolling WR: building ({len(rolling_outcomes.get(asset, deque()))}/{ROLLING_WR_WINDOW})")
                print(f"  Est profit: ${trade_size/best_ask - trade_size:.2f} if correct")
                print(f"  Stats:      {wins}W/{losses}L | P&L: ${daily_pnl:+.2f}")

                if len(open_positions) >= MAX_OPEN_POSITIONS:
                    print(f"  → Max positions reached\n{'='*55}")
                    continue

                entry_price = best_ask
                asset_name  = info["asset"]

                if not should_take_trade(asset=asset_name, direction=price_dir,
                                         entry=entry_price,
                                         confidence=f"PRICE {price_label}"):
                    print(f"  ⏭️ Price signal filtered out by rules\n{'='*55}")
                    continue

                price_td = build_trade_dict(
                    asset=asset_name, direction=price_dir,
                    entry=entry_price, market_ask=entry_price,
                    confidence=f"PRICE {price_label}",
                    size=trade_size, secs_left=secs_left,
                    score=0, flow=flow, strength=strength,
                    price_edge=price_edge, path_score=path_score,
                )
                ml_take, ml_prob = ml_check(price_td, label=f"price signal {asset_name} {price_dir}")
                if not ml_take:
                    print(f"  🤖 ML blocked — prob={ml_prob:.2f}\n{'='*55}")
                    shadow_log(price_token_id, price_td, ml_prob)
                    continue
                if ml_filter:
                    print(f"  🤖 ML approved — prob={ml_prob:.2f}")

                ob_fresh  = await fetch_orderbook(session, price_token_id)
                ask_fresh = get_best_ask(ob_fresh)
                if not ask_fresh:
                    print(f"{'='*55}")
                    continue
                if ask_fresh > entry_price + 0.05:
                    if ask_fresh <= REPRICE_MAX_ENTRY:
                        print(f"  ⚠️  Ask moved {entry_price:.2f} → {ask_fresh:.2f} — still acceptable")
                        entry_price = ask_fresh
                    else:
                        print(f"  ⚠️  Ask moved {entry_price:.2f} → {ask_fresh:.2f} — too high, skipping")
                        print(f"{'='*55}")
                        continue
                else:
                    entry_price = ask_fresh

                success, shares_filled, actual_entry = await place_order(
                    session, token_id=price_token_id, price=entry_price,
                    size=trade_size, direction=price_dir, asset=asset_name
                )

                if not success:
                    print("  ❌ Price signal order did NOT fill — skipping\n{'='*55}")
                    continue

                actual_cost = max(trade_size, 5.0)
                traded_this_cycle.add(price_trade_key)

                price_td["entry"]      = actual_entry
                price_td["market_ask"] = best_ask

                try:
                    log_trade(info["asset"], price_dir, actual_entry, actual_cost,
                              f"PRICE {price_label}", price_token_id, quoted_ask=entry_price)
                except: pass

                open_positions[price_token_id] = {
                    "asset":         info["asset"],
                    "direction":     price_dir,
                    "entry":         actual_entry,
                    "size":          actual_cost,
                    "shares":        shares_filled,
                    "time":          time.time(),
                    "end_time":      end_time,
                    "slug":          info["slug"],
                    "condition_id":  info.get("condition_id", ""),
                    "ml_trade_dict": price_td,
                }

                print(f"{'='*55}")
                positioned_slugs.add(info["slug"])

        await asyncio.sleep(POLL_INTERVAL)

# ============================================================
# MAIN
# ============================================================

async def main():
    print("=== Unified Trading Bot V5.6 ===")
    print(f"Mode:        {'📝 PAPER TRADING' if PAPER_TRADE else '💰 LIVE TRADING'}")
    print(f"ML filter:   {'✅ active (threshold=' + str(ml_filter.threshold) + ')' if ml_filter else '⚠️  not loaded — rules-only'}")
    print(f"Shadow log:  {'✅ active' if _shadow_enabled else '⚠️  not loaded'}")
    print(f"Trade size:  ${TRADE_SIZE} sweep | ${PRICE_SIGNAL_TRADE_SIZE} price signal")
    print(f"Max daily loss: ${MAX_DAILY_LOSS} | Max entry: {MAX_ENTRY}")
    print(f"Min signal entry: {MIN_SIGNAL_ENTRY} (floor, data-backed)")
    print(f"Reprice target: {REPRICE_MAX_ENTRY} | Timeout: {REPRICE_TIMEOUT}s | Max secs: {REPRICE_MAX_SECS_LEFT}s")
    print(f"Rolling WR gate: last {ROLLING_WR_WINDOW} trades < {ROLLING_WR_MIN:.0%} → skip price signals")
    print(f"ARB scanner: ON | Min guaranteed: ${ARB_MIN_GUARANTEED} | Max hedge: ${ARB_MAX_HEDGE}")
    print(f"Assets: {list(ASSETS.values())} (BNB+DOGE removed — sub-55% WR)")
    print(f"Blocked hours: {sorted(BLOCKED_HOURS)}")
    print("=" * 50)

    bot_dir = os.path.dirname(os.path.abspath(__file__))
    order_server = await asyncio.create_subprocess_exec(
        "node", "place_order.js",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=bot_dir
    )
    try:
        ready_line = await asyncio.wait_for(order_server.stdout.readline(), timeout=10)
        print(f"  {ready_line.decode().strip()}")
    except asyncio.TimeoutError:
        err = await order_server.stderr.read(500)
        print(f"  ❌ Order server failed to start: {err.decode().strip()}")
        return

    connector = aiohttp.TCPConnector(force_close=False)
    timeout   = aiohttp.ClientTimeout(total=10, connect=5)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        tasks = [
            *[watch_trades(asset) for asset in ASSETS.keys()],
            *[watch_depth(asset)  for asset in ASSETS.keys()],
            flow_monitor(),
            spoof_detector(),
            trading_loop(session),
            send_heartbeat(),
            run_claimer(),
            passive_cycle_tracker(session),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for task, result in zip(tasks, results):
            if isinstance(result, Exception):
                print(f"❌ Task crashed: {task} — {result}")

if __name__ == "__main__":
    asyncio.run(main())