import asyncio
import json
import time
import websockets
from datetime import datetime
from collections import deque

# ============================================================
# SETTINGS
# ============================================================

ASSETS = {
    "btcusdt": "BTC",
    "ethusdt": "ETH",
    "solusdt": "SOL",
    "xrpusdt": "XRP",
}

WINDOW_SECONDS = 15
IMBALANCE_THRESHOLD = 0.65
MIN_VOLUME = 50000
HIGH_VOLUME_THRESHOLD = 150000
LOW_VOLUME_THRESHOLD = 50000
PRICE_MOVE_THRESHOLD = 0.03  # 0.05% price move to confirm direction

# ============================================================
# STATE
# ============================================================

trade_windows = {asset: [] for asset in ASSETS.keys()}
price_windows = {asset: deque(maxlen=100) for asset in ASSETS.keys()}
current_signals = {asset: "NEUTRAL" for asset in ASSETS.keys()}

def ts():
    return datetime.now().strftime("%H:%M:%S")

def calculate_imbalance(asset):
    now = time.time()
    cutoff = now - WINDOW_SECONDS
    trade_windows[asset] = [t for t in trade_windows[asset] if t["time"] > cutoff]
    trades = trade_windows[asset]
    if not trades:
        return None, None, None
    buy_volume = sum(t["volume"] for t in trades if t["side"] == "BUY")
    sell_volume = sum(t["volume"] for t in trades if t["side"] == "SELL")
    total_volume = buy_volume + sell_volume
    if total_volume < MIN_VOLUME:
        return None, None, total_volume
    return buy_volume, sell_volume, total_volume

def calculate_price_trend(asset):
    """
    Compare price now vs 30 seconds ago.
    Returns direction and % change.
    """
    now = time.time()
    cutoff = now - WINDOW_SECONDS
    prices = price_windows[asset]

    if len(prices) < 2:
        return "FLAT", 0.0

    # get oldest price in window
    old_prices = [p for p in prices if p["time"] > cutoff]
    if not old_prices:
        return "FLAT", 0.0

    oldest_price = old_prices[0]["price"]
    newest_price = old_prices[-1]["price"]

    if oldest_price == 0:
        return "FLAT", 0.0

    pct_change = (newest_price - oldest_price) / oldest_price * 100

    if pct_change > PRICE_MOVE_THRESHOLD:
        return "UP", pct_change
    elif pct_change < -PRICE_MOVE_THRESHOLD:
        return "DOWN", pct_change
    else:
        return "FLAT", pct_change

def get_signal_strength(flow_signal, price_trend, buy_pct, sell_pct):
    """
    Combine flow signal and price trend into confidence level.
    """
    if flow_signal == "NEUTRAL":
        return "NEUTRAL", "⚪"

    if flow_signal == price_trend:
        # flow and price agree — strong signal
        dominant_pct = buy_pct if flow_signal == "UP" else sell_pct
        if dominant_pct > 80:
            return "STRONG", "💪"
        else:
            return "MODERATE", "✅"
    elif price_trend == "FLAT":
        # flow signal but price not moving yet
        return "EARLY", "⏳"
    else:
        # flow and price disagree — skip
        return "CONFLICTED", "⚠️"

# ============================================================
# WEBSOCKET FEEDS
# ============================================================

async def watch_trades(asset):
    """Stream real trades and track buy/sell volume."""
    url = f"wss://stream.binance.com:9443/ws/{asset}@aggTrade"

    while True:
        try:
            async with websockets.connect(url, ping_interval=None) as ws:
                while True:
                    data = json.loads(await ws.recv())
                    price = float(data.get("p", 0))
                    quantity = float(data.get("q", 0))
                    is_buyer_maker = data.get("m", False)
                    side = "SELL" if is_buyer_maker else "BUY"
                    volume_usd = price * quantity

                    trade_windows[asset].append({
                        "time": time.time(),
                        "side": side,
                        "volume": volume_usd,
                        "price": price
                    })

                    # track price
                    price_windows[asset].append({
                        "time": time.time(),
                        "price": price
                    })

        except Exception as e:
            await asyncio.sleep(1)

# ============================================================
# SIGNAL PRINTER
# ============================================================

async def signal_printer():
    prev_signals = {asset: "NEUTRAL" for asset in ASSETS.keys()}
    prev_volumes = {asset: 0 for asset in ASSETS.keys()}
    exhaustion_flags = {asset: False for asset in ASSETS.keys()}

    while True:
        await asyncio.sleep(5)
        now = ts()
        any_signal = False

        for asset, name in ASSETS.items():
            buy_vol, sell_vol, total_vol = calculate_imbalance(asset)
            price_trend, pct_change = calculate_price_trend(asset)

            if total_vol is None or buy_vol is None or sell_vol is None:
                prev_volumes[asset] = 0
                continue

            buy_pct = buy_vol / total_vol * 100
            sell_pct = sell_vol / total_vol * 100

            if buy_vol / total_vol >= IMBALANCE_THRESHOLD:
                signal = "UP"
            elif sell_vol / total_vol >= IMBALANCE_THRESHOLD:
                signal = "DOWN"
            else:
                signal = "NEUTRAL"

            strength, strength_emoji = get_signal_strength(
                signal, price_trend, buy_pct, sell_pct
            )

            # volume exhaustion detection
            prev_vol = prev_volumes[asset]
            exhaustion_msg = ""
            reversal_msg = ""

            if prev_vol > HIGH_VOLUME_THRESHOLD and total_vol < LOW_VOLUME_THRESHOLD:
                if prev_signals[asset] == "DOWN":
                    exhaustion_msg = "⚡ EXHAUSTION — reversal UP possible"
                    exhaustion_flags[asset] = True
                elif prev_signals[asset] == "UP":
                    exhaustion_msg = "⚡ EXHAUSTION — reversal DOWN possible"
                    exhaustion_flags[asset] = True

            # confirmed reversal after exhaustion
            if exhaustion_flags[asset] and signal != "NEUTRAL":
                if prev_signals[asset] == "DOWN" and signal == "UP":
                    reversal_msg = "🔄 REVERSAL CONFIRMED — DOWN → UP"
                    exhaustion_flags[asset] = False
                elif prev_signals[asset] == "UP" and signal == "DOWN":
                    reversal_msg = "🔄 REVERSAL CONFIRMED — UP → DOWN"
                    exhaustion_flags[asset] = False

            current_signals[asset] = signal

            # only print if something meaningful
            if signal != "NEUTRAL" or exhaustion_msg or reversal_msg:
                any_signal = True

                if signal == "UP":
                    flow_emoji = "🟢"
                elif signal == "DOWN":
                    flow_emoji = "🔴"
                else:
                    flow_emoji = "⚪"

                price_emoji = "📈" if price_trend == "UP" else "📉" if price_trend == "DOWN" else "➡️"

                print(f"[{now}] {flow_emoji} {name} | "
                      f"Buy: {buy_pct:.0f}% Sell: {sell_pct:.0f}% | "
                      f"Vol: ${total_vol/1000:.0f}k | "
                      f"Price: {price_emoji} {pct_change:+.3f}% | "
                      f"{strength_emoji} {strength}")

                if exhaustion_msg:
                    print(f"         {exhaustion_msg}")
                if reversal_msg:
                    print(f"         {reversal_msg}")

            prev_signals[asset] = signal
            prev_volumes[asset] = total_vol if total_vol else 0

        if not any_signal:
            print(f"[{now}] All neutral — no signal")

# ============================================================
# MAIN
# ============================================================

async def main():
    print("=== Order Flow + Price Confirmation Signal ===")
    print(f"Window: {WINDOW_SECONDS}s rolling")
    print(f"Flow threshold: {IMBALANCE_THRESHOLD*100:.0f}%+")
    print(f"Price move threshold: {PRICE_MOVE_THRESHOLD}%")
    print(f"Min volume: ${MIN_VOLUME/1000:.0f}k")
    print("=" * 40)

    await asyncio.gather(
        *[watch_trades(asset) for asset in ASSETS.keys()],
        signal_printer()
    )

if __name__ == "__main__":
    asyncio.run(main())