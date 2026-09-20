"""
Builds a behavioral profile of a tracked wallet from its public trade
history: price levels, position sizing, timing, and market selection —
establishing what "normal" activity looks like for that entity.
"""
import asyncio
import aiohttp
import json
from datetime import datetime
from collections import defaultdict

WALLET = ""
DATA_API = "https://data-api.polymarket.com"

def ts_to_time(ts):
    return datetime.fromtimestamp(ts).strftime("%m/%d %H:%M:%S")

async def main():
    async with aiohttp.ClientSession() as session:

        print(f"Fetching trades for tracked wallet ({WALLET[:10]}...)\n")

        # Pull 500 most recent trades
        all_trades = []
        for offset in range(0, 500, 100):
            async with session.get(
                f"{DATA_API}/activity",
                params={"user": WALLET, "limit": 100, "offset": offset, "type": "TRADE"}
            ) as resp:
                data = await resp.json()
                if not data:
                    break
                all_trades.extend(data)
                if len(data) < 100:
                    break

        print(f"Total trades found: {len(all_trades)}\n")

        trades = [t for t in all_trades if t.get("side") == "BUY"]
        print(f"BUY trades: {len(trades)}\n")

        # ── Price distribution ────────────────────────────────────────
        print("=" * 60)
        print("PRICE DISTRIBUTION (what prices do they buy at?)")
        print("=" * 60)
        buckets = defaultdict(int)
        for t in trades:
            p = t.get("price", 0)
            if p < 0.01:   buckets["< 1¢"] += 1
            elif p < 0.05: buckets["1¢-5¢"] += 1
            elif p < 0.10: buckets["5¢-10¢"] += 1
            elif p < 0.30: buckets["10¢-30¢"] += 1
            elif p < 0.50: buckets["30¢-50¢"] += 1
            elif p < 0.70: buckets["50¢-70¢"] += 1
            elif p < 0.90: buckets["70¢-90¢"] += 1
            else:           buckets["> 90¢"] += 1

        for bucket, count in sorted(buckets.items()):
            bar = "█" * (count // 2)
            print(f"  {bucket:10} {count:4} trades  {bar}")

        # ── Big penny wins ────────────────────────────────────────────
        print("\n" + "=" * 60)
        print("PENNY BUYS (price < 5¢) sorted by USDC spent")
        print("=" * 60)
        penny = [t for t in trades if t.get("price", 1) < 0.05]
        penny.sort(key=lambda x: x.get("usdcSize", 0), reverse=True)
        print(f"{'Time':<18} {'Price':>8} {'USDC':>10} {'Shares':>12} {'Outcome':<6} {'Market'}")
        print("-" * 90)
        for t in penny[:30]:
            price  = t.get("price", 0)
            usdc   = t.get("usdcSize", 0)
            shares = usdc / price if price > 0 else 0
            time_s = ts_to_time(t.get("timestamp", 0))
            outcome = t.get("outcome", "")[:5]
            title  = t.get("title", "")[:40]
            print(f"{time_s:<18} {price:>8.4f} ${usdc:>9.2f} {shares:>12,.0f}  {outcome:<6} {title}")

        # ── Market categories ─────────────────────────────────────────
        print("\n" + "=" * 60)
        print("WHICH MARKETS? (event slugs for penny buys)")
        print("=" * 60)
        slug_counts = defaultdict(int)
        for t in penny:
            slug = t.get("eventSlug", t.get("slug", "unknown"))
            # simplify slug
            parts = slug.split("-")[:4]
            slug_counts["-".join(parts)] += 1

        for slug, count in sorted(slug_counts.items(), key=lambda x: -x[1])[:20]:
            print(f"  {count:4}x  {slug}")

        # ── Pattern: YES vs NO preference ────────────────────────────
        print("\n" + "=" * 60)
        print("YES vs NO for penny buys")
        print("=" * 60)
        yes_count = sum(1 for t in penny if t.get("outcome","").upper() == "YES")
        no_count  = sum(1 for t in penny if t.get("outcome","").upper() == "NO")
        print(f"  YES: {yes_count} trades")
        print(f"  NO:  {no_count} trades")

        # ── Time pattern: when in the market lifecycle do they buy? ──
        print("\n" + "=" * 60)
        print("RECENT 20 PENNY TRADES in full detail")
        print("=" * 60)
        for t in penny[:20]:
            price   = t.get("price", 0)
            usdc    = t.get("usdcSize", 0)
            shares  = usdc / price if price > 0 else 0
            time_s  = ts_to_time(t.get("timestamp", 0))
            outcome = t.get("outcome", "")
            title   = t.get("title", "")
            end     = t.get("endDate", "")
            print(f"\n  [{time_s}] {outcome} @ {price:.4f} ({price*100:.2f}¢)")
            print(f"  Paid: ${usdc:.2f} for {shares:,.0f} shares (payout if win: ${shares:.2f})")
            print(f"  Market: {title}")
            if end:
                print(f"  End: {end}")

        # ── What markets do they actually trade? ─────────────────────
        print("\n" + "=" * 60)
        print("TOP MARKETS by trade count (all trades)")
        print("=" * 60)
        slug_counts = defaultdict(int)
        slug_usdc   = defaultdict(float)
        for t in trades:
            slug = t.get("eventSlug", t.get("slug", "unknown"))
            parts = slug.split("-")[:5]
            key = "-".join(parts)
            slug_counts[key] += 1
            slug_usdc[key]   += t.get("usdcSize", 0)

        for slug, count in sorted(slug_counts.items(), key=lambda x: -x[1])[:20]:
            print(f"  {count:4}x  ${slug_usdc[slug]:8.2f}  {slug}")

        # ── Mid-range trades analysis (their real strategy) ───────────
        print("\n" + "=" * 60)
        print("MID-RANGE TRADES 30¢-70¢ — sample of 20")
        print("=" * 60)
        mid = [t for t in trades if 0.30 <= t.get("price", 0) <= 0.70]
        mid.sort(key=lambda x: x.get("usdcSize", 0), reverse=True)
        print(f"Total mid-range trades: {len(mid)}\n")
        print(f"{'Time':<18} {'Price':>7} {'USDC':>9} {'Out':<4} {'Market'}")
        print("-" * 90)
        for t in mid[:20]:
            price   = t.get("price", 0)
            usdc    = t.get("usdcSize", 0)
            time_s  = ts_to_time(t.get("timestamp", 0))
            outcome = t.get("outcome", "")[:4]
            title   = t.get("title", "")[:50]
            print(f"{time_s:<18} {price:>7.3f} ${usdc:>8.2f}  {outcome:<4} {title}")

        # ── Fetch SELL trades too ─────────────────────────────────────
        print("\n" + "=" * 60)
        print("FETCHING ALL TRADES (BUY + SELL)...")
        print("=" * 60)

        all_buys_sells = []
        for offset in range(0, 300, 100):
            async with session.get(
                f"{DATA_API}/activity",
                params={"user": WALLET, "limit": 100, "offset": offset, "type": "TRADE"}
            ) as resp:
                data = await resp.json()
                if not data:
                    break
                all_buys_sells.extend(data)
                if len(data) < 100:
                    break

        sells = [t for t in all_buys_sells if t.get("side") == "SELL"]
        buys  = [t for t in all_buys_sells if t.get("side") == "BUY"]
        print(f"BUY trades:  {len(buys)}")
        print(f"SELL trades: {len(sells)}")

        # ── Deep dive one busy market cycle ──────────────────────────
        # Find the market with most activity
        from collections import Counter
        cid_counts = Counter(t.get("conditionId","") for t in all_buys_sells)
        busiest_cid, busiest_count = cid_counts.most_common(1)[0]

        cycle_trades = [t for t in all_buys_sells if t.get("conditionId") == busiest_cid]
        cycle_trades.sort(key=lambda x: x.get("timestamp", 0))

        title = cycle_trades[0].get("title","") if cycle_trades else ""
        print(f"\n{'=' * 60}")
        print(f"BUSIEST MARKET CYCLE ({busiest_count} trades)")
        print(f"Market: {title}")
        print(f"{'=' * 60}")
        print(f"\n{'Time':<18} {'Side':<5} {'Out':<5} {'Price':>7} {'USDC':>9}  Running total Up/Down")
        print("-" * 70)

        up_cost = 0.0
        dn_cost = 0.0
        up_sold = 0.0
        dn_sold = 0.0

        for t in cycle_trades:
            time_s  = ts_to_time(t.get("timestamp", 0))
            side    = t.get("side", "")
            outcome = t.get("outcome", "")
            price   = t.get("price", 0)
            usdc    = t.get("usdcSize", 0)

            if side == "BUY" and outcome.upper() == "YES":
                up_cost += usdc
            elif side == "BUY" and outcome.upper() == "NO":
                dn_cost += usdc
            elif side == "SELL" and outcome.upper() == "YES":
                up_sold += usdc
            elif side == "SELL" and outcome.upper() == "NO":
                dn_sold += usdc

            print(f"{time_s:<18} {side:<5} {outcome:<5} {price:>7.3f} ${usdc:>8.2f}  "
                  f"Up: ${up_cost:.2f} Dn: ${dn_cost:.2f}")

        print(f"\n  Total spent Up:   ${up_cost:.2f}")
        print(f"  Total spent Down: ${dn_cost:.2f}")
        print(f"  Total sold Up:    ${up_sold:.2f}")
        print(f"  Total sold Down:  ${dn_sold:.2f}")
        print(f"  Combined spent:   ${up_cost + dn_cost:.2f}")
        if up_cost > 0 and dn_cost > 0:
            implied_avg_yes = up_cost / sum(1 for t in cycle_trades if t.get('side')=='BUY' and t.get('outcome','').upper()=='YES') if any(t.get('side')=='BUY' and t.get('outcome','').upper()=='YES' for t in cycle_trades) else 0
            implied_avg_no  = dn_cost / sum(1 for t in cycle_trades if t.get('side')=='BUY' and t.get('outcome','').upper()=='NO')  if any(t.get('side')=='BUY' and t.get('outcome','').upper()=='NO'  for t in cycle_trades) else 0
            print(f"\n  → This looks like {'MARKET MAKING' if len(sells) > 10 else 'DIRECTIONAL BOTH-SIDES'} strategy")
            print(f"  → Avg Up entry:   ${implied_avg_yes:.3f}")
            print(f"  → Avg Down entry: ${implied_avg_no:.3f}")

        print("\n" + "=" * 60)
        print("ARB PAIRS — YES+NO bought in same market within 10s")
        print("=" * 60)

        by_condition = defaultdict(list)
        for t in trades:
            cid = t.get("conditionId", "")
            if cid:
                by_condition[cid].append(t)

        pairs_found = []
        for cid, cid_trades in by_condition.items():
            if len(cid_trades) < 2:
                continue
            yes_trades = [t for t in cid_trades if t.get("outcome","").upper() == "YES"]
            no_trades  = [t for t in cid_trades if t.get("outcome","").upper() == "NO"]
            for y in yes_trades:
                for n in no_trades:
                    time_diff = abs(y.get("timestamp",0) - n.get("timestamp",0))
                    if time_diff <= 10:
                        combined = y.get("price",0) + n.get("price",0)
                        pairs_found.append({
                            "time":     ts_to_time(y.get("timestamp",0)),
                            "yes":      y.get("price",0),
                            "no":       n.get("price",0),
                            "combined": combined,
                            "gap":      1.0 - combined,
                            "title":    y.get("title","")[:45],
                        })

        pairs_found.sort(key=lambda x: x["combined"])
        print(f"Pairs found: {len(pairs_found)}\n")
        print(f"{'Time':<18} {'YES':>6} {'NO':>6} {'Combined':>10} {'Gap':>6}  {'Market'}")
        print("-" * 95)
        for p in pairs_found[:40]:
            flag = " ✅" if p["combined"] < 0.95 else ""
            print(f"{p['time']:<18} {p['yes']:>6.3f} {p['no']:>6.3f} "
                  f"{p['combined']:>10.3f} {p['gap']:>6.3f}{flag}  {p['title']}")

        if pairs_found:
            avg_combined = sum(p["combined"] for p in pairs_found) / len(pairs_found)
            min_combined = min(p["combined"] for p in pairs_found)
            under_95     = sum(1 for p in pairs_found if p["combined"] < 0.95)
            under_97     = sum(1 for p in pairs_found if p["combined"] < 0.97)
            under_99     = sum(1 for p in pairs_found if p["combined"] < 0.99)
            print(f"\n  Pairs found:  {len(pairs_found)}")
            print(f"  Avg combined: {avg_combined:.4f}")
            print(f"  Min combined: {min_combined:.4f}")
            print(f"  Under 0.95:   {under_95} pairs")
            print(f"  Under 0.97:   {under_97} pairs")
            print(f"  Under 0.99:   {under_99} pairs")
            print(f"\n  → Our ARB_MAX_COMBINED = 0.95 would catch {under_95}/{len(pairs_found)} of their trades")
            print(f"  → Setting to 0.97 would catch {under_97}/{len(pairs_found)}")
            print(f"  → Setting to 0.99 would catch {under_99}/{len(pairs_found)}")

if __name__ == "__main__":
    asyncio.run(main())
