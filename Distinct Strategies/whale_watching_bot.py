import asyncio
import aiohttp
import json
import time
import websockets
from datetime import datetime
from collections import deque
from dotenv import load_dotenv
import os

load_dotenv()



'''
------------------------Hard rule ---------------------
The bot may not be turned off during its historically most profitable window. (3 PM-6 PM & 3am-7am)

'''

# ============================================================
# SETTINGS
# ============================================================

PAPER_TRADE        = True
PAPER_ENTRY_PRICE  = 0.97   # simulated fill price in paper modeCUR
MAX_ENTRY          = 0.97   # skip any trade where best ask is above this
TRADE_SIZE         = 6
MAX_DAILY_LOSS     = 40
MAX_OPEN_POSITIONS = 5000

WINDOW_SECONDS = 30
IMBALANCE_THRESHOLD = 0.65
MIN_VOLUME = 50000
PRICE_MOVE_THRESHOLD = 0.03

SWEEP_THRESHOLD     = 3000 # BTC / sometimes there are 15000-20000 shares swept
SWEEP_THRESHOLD_ALT =  2250 # ETH / SOL / XRP // 2150-2250 was getting .96-97 entries // spikes of 2-5k shares swept around 8pm-1am, 9am-1pm

MIN_TIME_LEFT = 20      # 20 sec might be okay 
MAX_TIME_LEFT = 200    # 200-240 sec might be better 
POLL_INTERVAL = 0.5

SPOOF_MIN_SIZE = 2.5
SPOOF_MAX_SIZE = 8.0
SPOOF_PROXIMITY = 25
SPOOF_MIN_HOLD = 15
SPOOF_MAX_HOLD = 90
SPOOF_BOUNCE = 15

CLOB_API  = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com/markets/slug"

ASSETS = {
    "btcusdt": "BTC",
    "ethusdt": "ETH",
    "solusdt": "SOL",
    "xrpusdt": "XRP",
}

MARKETS = [
    {"slug": "btc-updown-5m", "interval": 300, "asset": "BTC"},
    {"slug": "eth-updown-5m", "interval": 300, "asset": "ETH"},
    {"slug": "sol-updown-5m", "interval": 300, "asset": "SOL"},
    {"slug": "xrp-updown-5m", "interval": 300, "asset": "XRP"},
]

WHALE_WALLETS = {
    "examplewalletid1": "exampleusername1",
    "examplewalletid2": "exampleusername2",
}
WHALE_PAUSE_DURATION = 300  # 5 min pause after last whale trade
whale_last_active = 0.0
whale_active_name = ""

# ============================================================
# SHARED STATE
# ============================================================

trade_windows = {asset: [] for asset in ASSETS.keys()}
price_windows = {asset: deque(maxlen=100) for asset in ASSETS.keys()}
flow_signals  = {asset: "NEUTRAL" for asset in ASSETS.keys()}
flow_strength = {asset: "NEUTRAL" for asset in ASSETS.keys()}
spoof_lockout_until = 0
previous_ask_sizes  = {}
open_positions      = {}
recent_sweeps       = {}
daily_pnl = 0.0
wins      = 0
losses    = 0

# Price vs window open tracking
window_open_prices  = {asset: {} for asset in ASSETS.keys()}
window_open_times   = {asset: 0  for asset in ASSETS.keys()}
current_prices      = {asset: 0.0 for asset in ASSETS.keys()}

# Orderbook depth cache
depth_cache = {asset: {"bids": [], "asks": [], "ts": 0} for asset in ASSETS.keys()}

# Price signal settings
PRICE_EDGE_MIN           = 0.08 # 0.08 was default, produces around 89-91% win rate but alot of spoofs, late buys that get reversed
PRICE_EDGE_CONFIRM       = 0.03
PRICE_EDGE_CONFLICT      = 0.10
PRICE_SIGNAL_TRADE_SIZE  = 7       

# Path resistance settings
PATH_STRONG_THRESHOLD    = 3.0
PATH_WEAK_THRESHOLD      = 0.5
PATH_SCAN_RANGE_PCT      = 0.30

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

def log_trade(asset, direction, entry, size, confidence, token_id):
    # Use actual entry if it's already below our paper target, else simulate
    log_entry = (entry if entry < PAPER_ENTRY_PRICE else PAPER_ENTRY_PRICE) if PAPER_TRADE else entry
    with open("trades_log.txt", "a", encoding="utf-8") as f:
        f.write(f"\n{'='*50}\n")
        f.write(f"Time:       {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"Asset:      {asset} {direction}\n")
        f.write(f"Entry:      {log_entry}\n")
        f.write(f"Market ask: {entry}\n")
        f.write(f"Size:       ${size}\n")
        f.write(f"Confidence: {confidence}\n")
        f.write(f"Token ID:   {token_id}\n")
        f.write(f"Result:     PENDING\n")

# ============================================================
# PRICE TRADE TIME LOCK
# ============================================================
from datetime import datetime

def price_time_multiplier():
    now  = datetime.now()
    hour = now.hour
    minute = now.minute

    # Prime time block 1: 23:00 → 07:59 (overnight)
    if hour >= 23 or hour < 8:
        return 1.0

    # 08:00 → 08:59 — tapering into bad morning
    if hour == 8:
        return 0.5

    # Bad morning window: 09:00 → 13:59
    if 9 <= hour < 14:
        return 0.25

    # Prime time block 2: 14:00 → 18:29
    if hour == 14 or hour == 15 or hour == 16 or hour == 17:
        return 1.0
    if hour == 18 and minute < 30:
        return 1.0

    # 18:30 → 18:59 — tapering out of prime
    if hour == 18 and minute >= 30:
        return 0.5

    # 19:00 → 20:59 — worst evening hours
    if 19 <= hour < 21:
        return 0.1

    # 21:00 → 22:59 — recovering toward overnight prime
    return 0.33

# ============================================================
# WALLET CONNECTION
# ============================================================

def get_client():
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds
        from py_clob_client.constants import POLYGON

        creds = ApiCreds(
            api_key=os.getenv("POLY_API_KEY"),
            api_secret=os.getenv("POLY_SECRET"),
            api_passphrase=os.getenv("POLY_PASSPHRASE"),
        )
        client = ClobClient(
            host="https://clob.polymarket.com",
            key=os.getenv("PRIVATE_KEY"),
            chain_id=POLYGON,
            creds=creds,
            signature_type=2,
            funder=os.getenv("PROXY_WALLET")
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
    while True:
        try:
            if not PAPER_TRADE:
                client = get_client()
                if client:
                    resp = client.post_heartbeat(heartbeat_id)
                    heartbeat_id = resp.get("heartbeat_id", "")
        except:
            pass
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
    buy_vol  = sum(t["volume"] for t in trades if t["side"] == "BUY")
    sell_vol = sum(t["volume"] for t in trades if t["side"] == "SELL")
    total_vol = buy_vol + sell_vol
    if total_vol < MIN_VOLUME:
        return None, None, total_vol
    return buy_vol, sell_vol, total_vol

def calculate_price_trend(asset):
    now = time.time()
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
                    data = json.loads(await ws.recv())
                    price    = float(data.get("p", 0))
                    quantity = float(data.get("q", 0))
                    is_buyer_maker = data.get("m", False)
                    side = "SELL" if is_buyer_maker else "BUY"
                    trade_windows[asset].append({
                        "time": time.time(), "side": side,
                        "volume": price * quantity, "price": price
                    })
                    price_windows[asset].append({
                        "time": time.time(), "price": price
                    })
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
    """
    Measures how hard it is for price to reach the window open price.

    Logic:
    - opening_price = price at start of 5min window
    - current_price = latest Binance price
    - gap = distance price needs to travel to change resolution outcome

    If direction is Up:
      We need price to stay above (or get above) opening_price
      If current > opening -> already winning, check bid support below
      If current < opening -> needs to recover, check ask wall above

    Returns (score, label):
      score > 0 -> path is CLEAR (favours direction)
      score < 0 -> path is BLOCKED (resistance against direction)
      score = 0 -> neutral / no data
    """
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
            supporting_bids = sum(
                size * price for price, size in bids
                if price >= price_now - scan_dist
            )
            near_asks = sum(
                size * price for price, size in asks
                if price <= price_now + scan_range
            )
            ratio    = supporting_bids / near_asks if near_asks > 0 else 10.0
            dist_pct = (price_now - open_price) / open_price * 100
            label    = (f"UP winning +{dist_pct:.3f}% | "
                        f"bid support: ${supporting_bids:,.0f} "
                        f"vs ask pressure: ${near_asks:,.0f} "
                        f"(ratio {ratio:.1f}x)")
        else:
            asks_in_path = sum(
                size * price for price, size in asks
                if price_now < price <= price_now + scan_dist
            )
            bid_momentum = sum(
                size * price for price, size in bids
                if price >= price_now - scan_range
            )
            ratio    = bid_momentum / asks_in_path if asks_in_path > 0 else 0.0
            dist_pct = (open_price - price_now) / open_price * 100
            label    = (f"UP needs +{dist_pct:.3f}% | "
                        f"ask wall: ${asks_in_path:,.0f} "
                        f"bid momentum: ${bid_momentum:,.0f} "
                        f"(ratio {ratio:.1f}x)")
    else:
        if price_now <= open_price:
            supporting_asks = sum(
                size * price for price, size in asks
                if price <= price_now + scan_dist
            )
            near_bids = sum(
                size * price for price, size in bids
                if price >= price_now - scan_range
            )
            ratio    = supporting_asks / near_bids if near_bids > 0 else 10.0
            dist_pct = (open_price - price_now) / open_price * 100
            label    = (f"DOWN winning -{dist_pct:.3f}% | "
                        f"ask support: ${supporting_asks:,.0f} "
                        f"vs bid pressure: ${near_bids:,.0f} "
                        f"(ratio {ratio:.1f}x)")
        else:
            bids_in_path = sum(
                size * price for price, size in bids
                if price_now - scan_dist <= price < price_now
            )
            ask_momentum = sum(
                size * price for price, size in asks
                if price <= price_now + scan_range
            )
            ratio    = ask_momentum / bids_in_path if bids_in_path > 0 else 0.0
            dist_pct = (price_now - open_price) / open_price * 100
            label    = (f"DOWN needs -{dist_pct:.3f}% | "
                        f"bid wall: ${bids_in_path:,.0f} "
                        f"ask momentum: ${ask_momentum:,.0f} "
                        f"(ratio {ratio:.1f}x)")

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

def get_price_signal(asset_binance, secs_left):
    """
    Compare current price to window open price for any asset.
    Returns (direction, edge, label).
    edge = abs(pct_move) * time_weight — higher = stronger signal.
    """
    price_now  = current_prices.get(asset_binance, 0)
    window_ts  = window_open_times.get(asset_binance, 0)
    open_price = window_open_prices.get(asset_binance, {}).get(window_ts)

    if not price_now or not open_price:
        return None, 0.0, "NO DATA"

    pct_move     = (price_now - open_price) / open_price * 100
    time_elapsed = 300 - secs_left
    time_weight  = min(time_elapsed / 300, 1.0)
    edge         = abs(pct_move) * time_weight

    if pct_move > 0:
        direction = "Up"
    elif pct_move < 0:
        direction = "Down"
    else:
        return None, 0.0, "FLAT"

    asset_name = ASSETS.get(asset_binance, asset_binance.upper())

    if edge >= PRICE_EDGE_MIN:
        label = f"STRONG {'UP' if pct_move > 0 else 'DOWN'} {asset_name} ({pct_move:+.3f}% x {time_weight:.0%})"
    elif edge >= PRICE_EDGE_CONFIRM:
        label = f"WEAK {'UP' if pct_move > 0 else 'DOWN'} {asset_name} ({pct_move:+.3f}%)"
    else:
        label = f"FLAT {asset_name} ({pct_move:+.3f}%)"

    return direction, edge, label

# ============================================================
# SPOOF DETECTOR
# ============================================================

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
                                    tracked[key] = {
                                        "price": price, "size": size,
                                        "side": "BID", "first_seen": now,
                                        "price_at_appearance": current_price
                                    }

                    for price, size in current_asks.items():
                        if SPOOF_MIN_SIZE <= size <= SPOOF_MAX_SIZE:
                            if abs(price - current_price) <= SPOOF_PROXIMITY:
                                key = f"ASK_{price:.0f}"
                                if key not in tracked:
                                    tracked[key] = {
                                        "price": price, "size": size,
                                        "side": "ASK", "first_seen": now,
                                        "price_at_appearance": current_price
                                    }

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
                "asset":      market["asset"],
                "slug":       slug,
                "end_date":   end_date,
                "end_time":   parse_end_time(end_date),
                "condition_id": data.get("conditionId", ""),
                "up_token":   token_ids[0] if len(token_ids) > 0 else None,
                "down_token": token_ids[1] if len(token_ids) > 1 else None,
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
    """Return the lowest available ask price below 0.99."""
    if not orderbook:
        return None
    valid = [
        float(ask.get("price", 0))
        for ask in orderbook.get("asks", [])
        if float(ask.get("size", 0)) > 0 and float(ask.get("price", 0)) < 0.97
    ]
    return min(valid) if valid else None

def get_best_ask_price(orderbook):
    if not orderbook:
        return None
    for ask in orderbook.get("asks", []):
        price = float(ask.get("price", 0))
        size  = float(ask.get("size", 0))
        if size > 0:
            return price
    return None

async def whale_watcher(session):
    global whale_last_active, whale_active_name
    while True:
        try:
            for wallet, name in WHALE_WALLETS.items():
                async with session.get(
                    "https://data-api.polymarket.com/activity",
                    params={"user": wallet, "limit": 20},
                    timeout=aiohttp.ClientTimeout(total=8)
                ) as resp:
                    if resp.status != 200:
                        continue
                    data = await resp.json()

                trades = [d for d in data if d.get("type") == "TRADE"]
                if not trades:
                    continue

                now = time.time()
                recent = [t for t in trades if now - t.get("timestamp", 0) < 180]
                if not recent:
                    continue

                # Count how many different timeframes they're active in
                titles = set(t.get("title", "") for t in recent)
                has_5min  = any(any(x in t for x in ["11:50", "11:45", "12:05", "12:10", "AM-", "PM-"]) for t in titles)
                has_15min = any("15" in t for t in titles)
                has_hourly = any(t.endswith("ET") or t.endswith("AM ET") or t.endswith("PM ET") for t in titles)
                timeframes = sum([has_5min, has_15min, has_hourly])

                total_usdc = sum(t.get("usdcSize", 0) for t in recent)

                if len(recent) >= 2 or total_usdc >= 500:
                    was_inactive = now - whale_last_active >= WHALE_PAUSE_DURATION
                    still_active = not was_inactive
                    whale_last_active = now
                    whale_active_name = name
                    if was_inactive:
                        risk = "🚨 HIGH RISK" if timeframes >= 2 else "⚠️  ACTIVE"
                        print(f"[{ts()}] 🐋 {name} {risk} | {len(recent)} trades | ${total_usdc:,.0f} | {timeframes} timeframe(s) — pausing {WHALE_PAUSE_DURATION//60}min")
                    elif still_active:
                        print(f"[{ts()}] 🐋 {name} still trading — timer reset to {WHALE_PAUSE_DURATION//60}min")

            if whale_last_active > 0 and time.time() - whale_last_active >= WHALE_PAUSE_DURATION:
                if time.time() - whale_last_active < WHALE_PAUSE_DURATION + 30:
                    print(f"[{ts()}] ✅ {whale_active_name} gone quiet — resuming trading")

        except Exception:
            pass
        await asyncio.sleep(30)


def is_whale_active():
    if time.time() - whale_last_active < WHALE_PAUSE_DURATION:
        return whale_active_name
    return None

# ============================================================
# ORDER PLACEMENT
# ============================================================

def place_order(token_id, price, size, direction, asset):
    global daily_pnl

    if PAPER_TRADE:
        # If market ask is already below our paper entry target, use actual ask
        # Otherwise simulate getting filled at PAPER_ENTRY_PRICE
        fill_price = price if price < PAPER_ENTRY_PRICE else PAPER_ENTRY_PRICE
        shares     = size / fill_price
        print(f"  📝 PAPER TRADE: BUY {asset} {direction} | "
              f"${size} at {fill_price:.2f} (sim) = {shares:.1f} shares | "
              f"Est payout: ${shares:.2f} | Market ask: {price:.2f}")
        return True

    import subprocess
    try:
        result = subprocess.run(
            ["node", "place_order.js", token_id, str(price), str(max(size, 5.0))],
            capture_output=True, text=True, encoding="utf-8", timeout=10,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        stdout_lines = result.stdout.strip().split("\n")
        json_line    = next((l for l in stdout_lines if l.startswith("{")), None)
        output       = json.loads(json_line) if json_line else {"success": False, "error": "No output"}
        response     = output.get("response", {})
        if output.get("success") and not response.get("error"):
            amount = float(response.get("makingAmount", 0))
            print(f"  ✅ SHARES BOUGHT: {amount:.4f} | Breathe, nothing is 100%")
            return True
        else:
            print(f"  ❌ Order failed: {response.get('error') or output.get('error')}")
            return False
    except Exception as e:
        print(f"  ❌ Order failed: {e}")
        return False

# ============================================================
# REDEMPTION — handled by polymarket-claimer externally
# ============================================================

async def run_claimer():
    await asyncio.sleep(30)
    while True:
        try:
            proc = await asyncio.create_subprocess_exec(
                "py", "-3.12", "claimer.py", "--batch", "20",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=os.path.dirname(os.path.abspath(__file__))
            )
            stdout, stderr = await proc.communicate()
            if stdout:
                for line in stdout.decode('utf-8', errors='replace').strip().split("\n"):
                    if line.strip():
                        print(f"  💰 CLAIMER: {line.strip()}")
            if stderr:
                for line in stderr.decode('utf-8', errors='replace').strip().split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    if "No redeemable positions found" in line:
                        print(f"  💰 CLAIMER: nothing to claim")
                    elif "Batch done" in line:
                        print(f"  💰 CLAIMER: {line.split('INFO')[-1].strip()}")
                    elif "Claimed" in line or "Failed" in line:
                        print(f"  💰 CLAIMER: {line.split('INFO')[-1].strip()}")
                    elif "Found" in line and "redeemable" in line:
                        print(f"  💰 CLAIMER: {line.split('INFO')[-1].strip()}")
                    # skip HTTP requests, nonce calls, startup messages
        except Exception as e:
            print(f"[{ts()}] ⚠️  Claimer error: {e}")
        await asyncio.sleep(120)


# ============================================================
# P&L TRACKER  
# ============================================================

async def check_resolved_positions(session):
    global daily_pnl, open_positions, wins, losses

    if PAPER_TRADE:
        return

    resolved = []
    for token_id, pos in open_positions.items():
        if time.time() < pos.get("end_time", 0):
            continue

        try:
            async with session.get(
                f"{GAMMA_API}/{pos['slug']}",
                timeout=aiohttp.ClientTimeout(total=8)
            ) as resp:
                if resp.status != 200:
                    continue
                data = await resp.json()

            if not data.get("closed"):
                continue

            token_ids = json.loads(data.get("clobTokenIds", "[]"))
            prices    = json.loads(data.get("outcomePrices", "[]")) if isinstance(data.get("outcomePrices"), str) else data.get("outcomePrices", [])

            winning_token_id = None
            for i, price in enumerate(prices):
                if float(price) >= 0.99 and i < len(token_ids):
                    winning_token_id = token_ids[i]
                    break

            if winning_token_id is None:
                continue

            if token_id == winning_token_id:
                shares = pos["size"] / pos["entry"]
                profit = shares - pos["size"]
                daily_pnl += profit
                wins += 1
                print(f"\n✅ WIN [{ts()}] {pos['asset']} {pos['direction']} | "
                      f"+${profit:.2f} | "
                      f"Record: {wins}W/{losses}L | "
                      f"P&L: ${daily_pnl:+.2f}")
            else:
                daily_pnl -= pos["size"]
                losses += 1
                print(f"\n❌ LOSS [{ts()}] {pos['asset']} {pos['direction']} | "
                      f"-${pos['size']:.2f} | "
                      f"Record: {wins}W/{losses}L | "
                      f"P&L: ${daily_pnl:+.2f}")

            resolved.append(token_id)

        except Exception as e:
            print(f"  ⚠️  Position check error: {e}")
            continue

    for token_id in resolved:
        open_positions.pop(token_id, None)

# ============================================================
# TRADING LOOP
# ============================================================

async def trading_loop(session):
    global daily_pnl

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

        if now - last_position_check > 15:
            await check_resolved_positions(session)
            last_position_check = now

        # Parallel market token refresh every 30s
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
                    if PAPER_TRADE:
                        open_positions.clear()  # reset paper positions each cycle
                    print(f"[{ts()}] 🔄 New market cycle — sizes reset")
                else:
                    print(f"[{ts()}] ✅ Markets refreshed")
            last_refresh = now

        # Parallel orderbook fetch
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

        # Detect sweeps
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

            if up_sweep and down_sweep:
                print(f"[{ts()}] {asset} both sides swept simultaneously -- skipping")
                continue

            now_ts = time.time()
            if up_sweep:
                recent_sweeps[f"{slug}_Up"] = now_ts
            if down_sweep:
                recent_sweeps[f"{slug}_Down"] = now_ts

            up_recent   = recent_sweeps.get(f"{slug}_Up",   0)
            down_recent = recent_sweeps.get(f"{slug}_Down", 0)
            if up_recent and down_recent and abs(up_recent - down_recent) < 45:
                skip_key = f"{slug}_bothsides_printed"
                if now_ts - recent_sweeps.get(skip_key, 0) > 45:
                    print(f"[{ts()}] {asset} both sides swept within 45s -- skipping")
                    recent_sweeps[skip_key] = now_ts
                continue

            for direction, sweep, best_ask in [
                ("Up",   up_sweep,   up_best_ask),
                ("Down", down_sweep, down_best_ask)
            ]:
                if not sweep:
                    continue

                asset_binance = next(
                    (k for k, v in ASSETS.items() if v == asset), None
                )
                flow     = flow_signals.get(asset_binance, "NEUTRAL")
                strength = flow_strength.get(asset_binance, "NEUTRAL")

                if not best_ask:
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

        # Process signals
        # track traded slug+direction this poll cycle to prevent duplicates
        traded_this_cycle = set()

        for sig in cycle_signals:
            now_str   = ts()
            flow      = sig["flow_signal"]
            strength  = sig["flow_strength"]
            direction = sig["direction"]
            secs_left = sig["secs_left"]

            # ── FILTER 1: no duplicate same asset+direction this cycle ──
            trade_key = f"{sig['slug']}_{direction}"
            if trade_key in traded_this_cycle:
                continue
            # Also block opposite direction on same asset this cycle
            opp_key = f"{sig['slug']}_{'Down' if direction == 'Up' else 'Up'}"
            if opp_key in traded_this_cycle:
                continue
            if sig["token_id"] in open_positions:
                continue
            # Block if we already have any position on this market (either side)
            if any(pos.get("slug") == sig["slug"] for pos in open_positions.values()):
                continue

            # ── FILTER 2: max entry (live only — paper uses simulated price) ──
            if not PAPER_TRADE and sig["best_ask"] > MAX_ENTRY:
                continue

            if time.time() < spoof_lockout_until:
                print(f"[{now_str}] ⚠️  Spoof lockout — skipping {sig['asset']} {direction}")
                continue

            whale = is_whale_active()
            if whale:
                continue

            flow_confirms = (
                (direction == "Up"   and flow == "UP")   or
                (direction == "Down" and flow == "DOWN")
            )
            flow_conflicts = (
                (direction == "Up"   and flow == "DOWN") or
                (direction == "Down" and flow == "UP")
            )

            if flow_conflicts and strength == "STRONG":
                print(f"[{now_str}] ⚠️  Flow CONFLICTS strongly — skipping {sig['asset']} {direction}")
                continue

            asset_binance = next(
                (k for k, v in ASSETS.items() if v == sig["asset"]), None
            )
            price_dir, price_edge, price_label = get_price_signal(asset_binance, secs_left)
            path_score, path_label = get_path_resistance(asset_binance, direction, secs_left)

            price_confirms  = (price_dir == direction and price_edge >= PRICE_EDGE_CONFIRM)
            price_conflicts = (
                price_dir is not None and
                price_dir != direction and
                price_edge >= PRICE_EDGE_CONFLICT
            )

            if price_conflicts:
                print(f"[{now_str}] ⚠️  Price CONFLICTS sweep — skipping {sig['asset']} {direction}")
                continue

            score = 0
            if flow_confirms and strength == "STRONG":  score += 3
            elif flow_confirms:                         score += 1
            if price_confirms and price_edge >= PRICE_EDGE_MIN: score += 3
            elif price_confirms:                        score += 1
            if path_score >= 2:    score += 2
            elif path_score >= 1:  score += 1
            elif path_score <= -2: score -= 2
            elif path_score <= -1: score -= 1

            if path_score <= -2 and price_conflicts:
                print(f"[{now_str}] ⚠️  Path + price both conflict — skipping {sig['asset']} {direction}")
                continue

            if score >= 5:
                confidence = "🔥 HIGH"
            elif score >= 2:
                confidence = "✅ MEDIUM"
            elif score >= 0:
                confidence = "⚪ LOW (no confirmation)"
            else:
                confidence = "⚠️  CONFLICTED"

            # Block LOW confidence sweeps — 82% win rate doesn't justify the risk
            # Price signals are unaffected (separate code path)
            if score < 2:
                continue

            print(f"\n{'='*55}")
            print(f"🚨 SIGNAL [{now_str}]")
            print(f"  Asset:      {sig['asset']} {direction}")
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

            success = place_order(
                token_id=sig["token_id"],
                price=sig["best_ask"],
                size=TRADE_SIZE,
                direction=direction,
                asset=sig["asset"]
            )

            if success:
                traded_this_cycle.add(trade_key)
                try:
                    log_trade(
                        sig["asset"], direction, sig["best_ask"],
                        TRADE_SIZE, confidence, sig["token_id"]
                    )
                except Exception as e:
                    print(f"  ⚠️ Log failed: {e}")

                open_positions[sig["token_id"]] = {
                    "asset":     sig["asset"],
                    "direction": direction,
                    "entry":     PAPER_ENTRY_PRICE if PAPER_TRADE else sig["best_ask"],
                    "size":      TRADE_SIZE,
                    "time":      time.time(),
                    "end_time":  sig["end_time"],
                    "slug":      sig["slug"],
                    "condition_id": info.get("condition_id", ""), 
                }
                
            print(f"{'='*55}")

        # Standalone price signal
        if not (time.time() < spoof_lockout_until) and not is_whale_active():
            # Track which market slugs already have a position (either side)
            positioned_slugs = {
                pos.get("slug", "") for pos in open_positions.values()
            }

            for info in market_infos:
                secs_left = seconds_until(info["end_date"])
                if secs_left is None or secs_left < MIN_TIME_LEFT or secs_left > MAX_TIME_LEFT:
                    continue

                asset_binance = next(
                    (k for k, v in ASSETS.items() if v == info["asset"]), None
                )

                price_dir, price_edge, price_label = get_price_signal(asset_binance, secs_left)

                if price_dir is None or price_edge < PRICE_EDGE_MIN:
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

                ob = await fetch_orderbook(session, price_token_id)
                best_ask = get_best_ask(ob)

                if not best_ask or best_ask < 0.10 or (not PAPER_TRADE and best_ask > MAX_ENTRY):
                    continue

                flow     = flow_signals.get(asset_binance, "NEUTRAL")
                strength = flow_strength.get(asset_binance, "NEUTRAL")

                # Skip if flow strongly conflicts with price direction
                flow_conflicts = (
                    (price_dir == "Up"   and flow == "DOWN") or
                    (price_dir == "Down" and flow == "UP")
                )
                if flow_conflicts and strength == "STRONG":
                    continue

                end_time = info["end_time"]

                print(f"\n{'='*55}")
                print(f"📈 PRICE SIGNAL [{ts()}]")
                print(f"  Asset:      {info['asset']} {price_dir}")
                print(f"  Price:      {price_label}")
                print(f"  Path:       {path_label}")
                print(f"  Edge:       {price_edge:.3f} | PathScore: {path_score:.0f}")
                print(f"  Entry:      {best_ask:.2f}")
                print(f"  Time left:  {secs_left}s")
                print(f"  Flow:       {flow} ({strength})")
                print(f"  Est profit: ${PRICE_SIGNAL_TRADE_SIZE/best_ask - PRICE_SIGNAL_TRADE_SIZE:.2f} if correct")
                print(f"  Stats:      {wins}W/{losses}L | P&L: ${daily_pnl:+.2f}")

                if len(open_positions) >= MAX_OPEN_POSITIONS:
                    print(f"  → Max positions reached")
                    print(f"{'='*55}")
                    continue


                # ✅ PRICE TIME / REGIME GATE
                mult = price_time_multiplier()

                print(f"[{ts()}] Price Signal Size Multiplier: {mult:.2f}")

                adjusted_size = PRICE_SIGNAL_TRADE_SIZE * mult


                success = place_order(
                    token_id=price_token_id,
                    price=best_ask,
                    size=adjusted_size,
                    direction=price_dir,
                    asset=info["asset"]
                )

                if success:
                    traded_this_cycle.add(price_trade_key)
                    try:
                        log_trade(
                            info["asset"], price_dir, best_ask,
                            adjusted_size, f"PRICE {price_label}", price_token_id
                        )
                    except:
                        pass
                    open_positions[price_token_id] = {
                        "asset":     info["asset"],
                        "direction": price_dir,
                        "entry":     PAPER_ENTRY_PRICE if PAPER_TRADE else best_ask,
                        "size":      PRICE_SIGNAL_TRADE_SIZE,
                        "time":      time.time(),
                        "end_time":  end_time,
                        "slug":      info["slug"],
                        "condition_id": info.get("condition_id", ""), 
                    }

                print(f"{'='*55}")
        await asyncio.sleep(POLL_INTERVAL)

# ============================================================
# MAIN
# ============================================================

async def main():
    print("=== Just Breathe Filip ===")
    print(f"Mode: {'📝 PAPER TRADING' if PAPER_TRADE else '💰 LIVE TRADING'}")
    print(f"Trade size: ${TRADE_SIZE} | Max daily loss: ${MAX_DAILY_LOSS}")
    print(f"Max entry: {MAX_ENTRY} | Paper fill: {PAPER_ENTRY_PRICE}")
    print("=" * 40)

    connector = aiohttp.TCPConnector(force_close=True)
    timeout   = aiohttp.ClientTimeout(total=10, connect=5)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        await asyncio.gather(
            *[watch_trades(asset) for asset in ASSETS.keys()],
            *[watch_depth(asset)  for asset in ASSETS.keys()],
            flow_monitor(),
            spoof_detector(),
            trading_loop(session),
            send_heartbeat(),
            run_claimer(),
            whale_watcher(session),
        )

if __name__ == "__main__":
    asyncio.run(main())