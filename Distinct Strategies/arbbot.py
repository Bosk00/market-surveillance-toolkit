import asyncio
import aiohttp
import json
import time
from datetime import datetime

MIN_EDGE = 0.01  # 1% minimum edge after fees
SCAN_INTERVAL = 2
FEE = 0.0025  # 0.25% taker fee

BASE_URL = "https://gamma-api.polymarket.com/markets/slug"

# asset slug prefixes and their intervals in seconds
MARKETS = [
    {"slug": "btc-updown-5m",  "interval": 300,  "asset": "BTC", "fees": True},
    {"slug": "btc-updown-15m", "interval": 900,  "asset": "BTC", "fees": True},
    {"slug": "eth-updown-5m",  "interval": 300,  "asset": "ETH", "fees": True},
    {"slug": "eth-updown-15m", "interval": 900,  "asset": "ETH", "fees": True},
    {"slug": "sol-updown-5m",  "interval": 300,  "asset": "SOL", "fees": True},
    {"slug": "sol-updown-15m", "interval": 900,  "asset": "SOL", "fees": True},
    {"slug": "xrp-updown-5m",  "interval": 300,  "asset": "XRP", "fees": True},
    {"slug": "xrp-updown-15m", "interval": 900,  "asset": "XRP", "fees": True},
]

def get_current_timestamp(interval):
    """Round current time up to next interval boundary."""
    now = int(time.time())
    return ((now // interval) + 1) * interval

def get_slug(base_slug, interval):
    ts = get_current_timestamp(interval)
    return f"{base_slug}-{ts}"

async def fetch_market(session, market):
    slug = get_slug(market["slug"], market["interval"])
    url = f"{BASE_URL}/{slug}"
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                return None
            return await resp.json()
    except:
        return None

def analyze_market(data, market):
    if not data:
        return None

    outcome_prices = data.get("outcomePrices")
    if not outcome_prices:
        return None

    try:
        prices = json.loads(outcome_prices)
        up = float(prices[0])
        down = float(prices[1])
    except:
        return None

    total = up + down
    edge = abs(1.0 - total)

    # account for fees
    fee_cost = FEE if market["fees"] else 0
    net_edge = edge - fee_cost

    end_date = data.get("endDate", "")
    time_left = ""
    if end_date:
        try:
            end_ts = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
            now_ts = datetime.now(end_ts.tzinfo)
            seconds_left = int((end_ts - now_ts).total_seconds())
            if seconds_left < 0:
                return None  # market already ended
            time_left = f"{seconds_left}s left"
        except:
            pass

    return {
        "asset": market["asset"],
        "timeframe": "5m" if market["interval"] == 300 else "15m",
        "question": data.get("question", ""),
        "up": up,
        "down": down,
        "total": total,
        "edge": edge,
        "net_edge": net_edge,
        "time_left": time_left,
        "fees": market["fees"],
        "condition_id": data.get("conditionId")
    }

async def main():
    print("=== Polymarket Arb Scanner ===")
    print(f"Min edge: {MIN_EDGE*100:.1f}% (after {FEE*100:.2f}% fees)")
    print("=" * 40)

    async with aiohttp.ClientSession() as session:
        while True:
            now = datetime.now().strftime("%H:%M:%S")
            results = []

            # fetch all markets simultaneously
            tasks = [fetch_market(session, m) for m in MARKETS]
            responses = await asyncio.gather(*tasks)

            for i, data in enumerate(responses):
                result = analyze_market(data, MARKETS[i])
                if result:
                    results.append(result)

            if not results:
                print(f"[{now}] No markets fetched — timestamps may be off")
            else:
                print(f"\n[{now}] === Market Snapshot ===")
                for r in results:
                    edge_str = f"{r['net_edge']*100:.2f}% net edge" if r['net_edge'] > 0 else "no edge"
                    flag = "⚡ EDGE" if r['net_edge'] >= MIN_EDGE else ""
                    print(f"  {r['asset']} {r['timeframe']} | "
                          f"Up: {r['up']*100:.1f}% Down: {r['down']*100:.1f}% | "
                          f"Total: {r['total']*100:.1f}% | "
                          f"{edge_str} | {r['time_left']} {flag}")

            await asyncio.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    asyncio.run(main())