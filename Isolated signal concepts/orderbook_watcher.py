import asyncio
import aiohttp
import json
import time
from datetime import datetime

CLOB_API = "https://clob.polymarket.com"
GAMMA_API = "https://gamma-api.polymarket.com/markets/slug"

POLL_INTERVAL = 0.5
SWEEP_THRESHOLD = 4500
MIN_TIME_LEFT = 1
MAX_TIME_LEFT = 300

MARKETS = [
    {"slug": "btc-updown-5m",  "interval": 300, "asset": "BTC"},
    {"slug": "eth-updown-5m",  "interval": 300, "asset": "ETH"},
    {"slug": "sol-updown-5m",  "interval": 300, "asset": "SOL"},
    {"slug": "xrp-updown-5m",  "interval": 300, "asset": "XRP"},
]

def get_current_timestamp(interval):
    now = int(time.time())
    return (now // interval) * interval

def get_slug(base_slug, interval):
    ts = get_current_timestamp(interval)
    return f"{base_slug}-{ts}"

def ts_to_time():
    return datetime.fromtimestamp(time.time()).strftime("%H:%M:%S")

def seconds_until(end_date_str):
    try:
        end_ts = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        now_ts = datetime.now(end_ts.tzinfo)
        return int((end_ts - now_ts).total_seconds())
    except:
        return None

previous_ask_sizes = {}

async def fetch_market_tokens(session, market):
    slug = get_slug(market["slug"], market["interval"])
    try:
        async with session.get(f"{GAMMA_API}/{slug}") as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
            token_ids = json.loads(data.get("clobTokenIds", "[]"))
            end_date = data.get("endDate", "")
            return {
                "asset": market["asset"],
                "slug": slug,
                "token_ids": token_ids,
                "end_date": end_date,
                "up_token": token_ids[0] if len(token_ids) > 0 else None,
                "down_token": token_ids[1] if len(token_ids) > 1 else None,
            }
    except:
        return None

async def fetch_orderbook(session, token_id):
    try:
        async with session.get(f"{CLOB_API}/book", params={"token_id": token_id}) as resp:
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
    """Get the lowest available ask price."""
    if not orderbook:
        return None
    asks = orderbook.get("asks", [])
    if not asks:
        return None
    # asks are sorted lowest first
    for ask in asks:
        price = float(ask.get("price", 0))
        size = float(ask.get("size", 0))
        if size > 0 and price < 0.99:
            return price
    return None

async def watch_markets(session):
    print("Fetching market token IDs...")
    market_infos = []
    for market in MARKETS:
        info = await fetch_market_tokens(session, market)
        if info and info["up_token"]:
            market_infos.append(info)
            print(f"  ✓ {info['asset']} | {info['slug']}")

    print(f"\nWatching {len(market_infos)} markets...\n")

    last_token_refresh = time.time()

    while True:
         # refresh tokens every 30 seconds
        if time.time() - last_token_refresh > 30:
            new_infos = []
            slugs_changed = False
            for market in MARKETS:
                info = await fetch_market_tokens(session, market)
                if info and info["up_token"]:
                    new_infos.append(info)
                    # check if slug changed vs what we have
                    existing = next((m for m in market_infos if m["asset"] == info["asset"]), None)
                    if existing and existing["slug"] != info["slug"]:
                        slugs_changed = True
            if new_infos:
                market_infos = new_infos
                if slugs_changed:
                    previous_ask_sizes.clear()
                    print(f"[{ts_to_time()}] New market cycle — sizes reset")
                else:
                    print(f"[{ts_to_time()}] Markets refreshed — same slugs, keeping sizes")
            last_token_refresh = time.time()

        # collect all sweeps this poll cycle
        cycle_signals = []

        for info in market_infos:
            asset = info["asset"]
            slug = info["slug"]
            secs_left = seconds_until(info["end_date"])

            if secs_left is None or secs_left < MIN_TIME_LEFT or secs_left > MAX_TIME_LEFT:
                continue

            up_sweep = None
            down_sweep = None

            for direction, token_id in [("Up", info["up_token"]), ("Down", info["down_token"])]:
                if not token_id:
                    continue

                key = f"{slug}_{direction}"
                orderbook = await fetch_orderbook(session, token_id)
                current_size = get_ask_size_at_price(orderbook, "0.99")
                prev_size = previous_ask_sizes.get(key, None)

                if prev_size is not None:
                    drop = prev_size - current_size
                    if drop >= SWEEP_THRESHOLD:
                        if direction == "Up":
                            up_sweep = drop
                        else:
                            down_sweep = drop

                best_ask = get_best_ask(orderbook)
                previous_ask_sizes[key] = current_size

            # filter: if SAME market has both Up AND Down swept simultaneously
            # that's market maker rebalancing this specific market — skip
            if up_sweep and down_sweep:
                print(f"[{ts_to_time()}] {asset} both sides swept simultaneously — skipping (rebalancing)")
                continue

            if up_sweep:
                cycle_signals.append({
                    "asset": asset,
                    "slug": slug,
                    "direction": "Up",
                    "drop": up_sweep,
                    "remaining": previous_ask_sizes.get(f"{slug}_Up", 0),
                    "secs_left": secs_left,
                    "token_id": info["up_token"],
                    "best_ask": best_ask or 0.50,
                })

            if down_sweep:
                cycle_signals.append({
                    "asset": asset,
                    "slug": slug,
                    "direction": "Down",
                    "drop": down_sweep,
                    "remaining": previous_ask_sizes.get(f"{slug}_Down", 0),
                    "secs_left": secs_left,
                    "token_id": info["down_token"],
                    "best_ask": best_ask or 0.50,
                })

        for sig in cycle_signals:
            now = ts_to_time()
            print(f"{'='*55}")
            print(f"🚨 SWEEP DETECTED [{now}]")
            print(f"  Asset:        {sig['asset']} {sig['direction']}")
            print(f"  Market:       {sig['slug']}")
            print(f"  Shares swept: {sig['drop']:,.0f} at 0.99")
            print(f"  Remaining:    {sig['remaining']:,.0f} shares at 0.99")
            print(f"  Time left:    {sig['secs_left']}s")
            print(f"  Entry price:  ~{sig['best_ask']:.2f} (current best ask)")
            print(f"  Target exit:  0.99")
            print(f"  Est profit:   {(0.99 - sig['best_ask']):.2f} per share")
            print(f"  Token ID:     {sig['token_id']}")
            print(f"{'='*55}")

        await asyncio.sleep(POLL_INTERVAL)

async def main():
    print("=== Orderbook Sweep Watcher ===")
    print(f"Sweep threshold: {SWEEP_THRESHOLD:,} shares")
    print(f"Time window: {MIN_TIME_LEFT}-{MAX_TIME_LEFT} seconds left")
    print(f"Polling every {POLL_INTERVAL}s")
    print(f"Filtering: same-market Up+Down simultaneous sweeps")
    print("=" * 40)

    async with aiohttp.ClientSession() as session:
        await watch_markets(session)

if __name__ == "__main__":
    asyncio.run(main())