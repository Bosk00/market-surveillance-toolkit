"""
ml_shadow_log.py
================
Tracks every trade the ML filter blocks, then resolves the outcome
when the market settles. Lets you answer: "Is the ML actually helping?"

Drop this file next to your bot. It's imported by unified_bot.py
via two calls:

    from ml_shadow_log import shadow_log, shadow_resolve

Usage pattern (already wired into unified_bot.py below):
    # When ML blocks a trade:
    shadow_log(token_id, trade_dict, ml_prob)

    # When market resolves (in check_resolved_positions, for ALL tokens
    # including ones you didn't trade):
    shadow_resolve(token_id, won)

Outputs two files:
    ml_shadow_log.jsonl   — one line per blocked trade, outcome filled in later
    ml_audit.txt          — human-readable running summary
"""

import json
import time
import threading
from pathlib import Path
from datetime import datetime

SHADOW_LOG_PATH = "ml_shadow_log.jsonl"
AUDIT_PATH      = "ml_audit.txt"

_lock      = threading.Lock()
_pending   = {}   # token_id -> record (blocked trades awaiting outcome)


def shadow_log(token_id: str, trade_dict: dict, ml_prob: float):
    """
    Call this when the ML blocks a trade. Records it as pending.
    """
    record = {
        "token_id":  token_id,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "asset":     trade_dict.get("asset", ""),
        "direction": trade_dict.get("direction", ""),
        "entry":     trade_dict.get("entry", 0),
        "confidence":trade_dict.get("confidence", ""),
        "ml_prob":   round(ml_prob, 4),
        "outcome":   None,   # filled in by shadow_resolve()
        "saved_loss":None,   # positive = ML saved you money (blocked a loser)
        "missed_win":None,   # positive = ML cost you money (blocked a winner)
    }
    with _lock:
        _pending[token_id] = record


def shadow_resolve(token_id: str, won: bool, size: float = 5.0, shares: float = 0.0):
    """
    Call this when a market closes, for every token_id — whether you
    traded it or not. If it was a blocked trade, records the outcome
    and writes it to the shadow log.

    size   — the $ size you would have spent (default TRADE_SIZE=5)
    shares — shares you would have received at that entry
             (used to compute hypothetical profit; 0 = estimate from size/entry)
    """
    with _lock:
        record = _pending.pop(token_id, None)

    if record is None:
        return   # wasn't a blocked trade — ignore

    entry  = record["entry"]
    if shares <= 0 and entry > 0:
        shares = size / entry

    if won:
        # ML blocked a winner — we missed profit
        hypothetical_profit = shares - size
        record["outcome"]    = "WIN"
        record["missed_win"] = round(hypothetical_profit, 4)
        record["saved_loss"] = 0
    else:
        # ML correctly blocked a loser — we saved the stake
        record["outcome"]    = "LOSS"
        record["saved_loss"] = round(size, 4)
        record["missed_win"] = 0

    # Append to JSONL log
    with _lock:
        with open(SHADOW_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    _write_audit_summary()


def _write_audit_summary():
    """Recompute and overwrite the audit summary from the full JSONL log."""
    path = Path(SHADOW_LOG_PATH)
    if not path.exists():
        return

    records = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    records.append(json.loads(line))
                except:
                    pass

    if not records:
        return

    total      = len(records)
    wins       = sum(1 for r in records if r["outcome"] == "WIN")
    losses     = sum(1 for r in records if r["outcome"] == "LOSS")
    total_saved = sum(r["saved_loss"] or 0 for r in records)
    total_missed= sum(r["missed_win"] or 0 for r in records)
    net_ml_value= total_saved - total_missed

    # Per-asset breakdown
    by_asset = {}
    for r in records:
        a = r["asset"]
        if a not in by_asset:
            by_asset[a] = {"blocked": 0, "wins": 0, "saved": 0.0, "missed": 0.0}
        by_asset[a]["blocked"] += 1
        if r["outcome"] == "WIN":
            by_asset[a]["wins"]   += 1
            by_asset[a]["missed"] += r["missed_win"] or 0
        else:
            by_asset[a]["saved"] += r["saved_loss"] or 0

    # Per-confidence-type breakdown
    by_type = {}
    for r in records:
        conf = r["confidence"].upper()
        if "PRICE" in conf:   t = "PRICE"
        elif "REPRIC" in conf: t = "REPRICED"
        elif "SWEEP" in conf or "HIGH" in conf or "MEDIUM" in conf: t = "SWEEP"
        else:                  t = "OTHER"
        if t not in by_type:
            by_type[t] = {"blocked": 0, "wins": 0, "saved": 0.0, "missed": 0.0}
        by_type[t]["blocked"] += 1
        if r["outcome"] == "WIN":
            by_type[t]["wins"]   += 1
            by_type[t]["missed"] += r["missed_win"] or 0
        else:
            by_type[t]["saved"] += r["saved_loss"] or 0

    # Recent 50 trades (most relevant for tuning)
    recent = records[-50:]
    recent_wins  = sum(1 for r in recent if r["outcome"] == "WIN")
    recent_saved = sum(r["saved_loss"] or 0 for r in recent)
    recent_missed= sum(r["missed_win"] or 0 for r in recent)

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [
        f"ML SHADOW AUDIT  [{now}]",
        "=" * 55,
        f"Blocked trades resolved : {total}",
        f"  Would have WON        : {wins}  ({wins/total:.1%})",
        f"  Would have LOST       : {losses}  ({losses/total:.1%})",
        f"",
        f"Net ML value            : ${net_ml_value:+.2f}",
        f"  Total saved (blocked losers) : ${total_saved:.2f}",
        f"  Total missed (blocked winners): ${total_missed:.2f}",
        f"",
        f"Verdict : {'✅ ML IS HELPING' if net_ml_value > 0 else '❌ ML IS HURTING'}"
        f"  (${abs(net_ml_value):.2f} {'saved' if net_ml_value > 0 else 'missed'})",
        f"",
        f"RECENT 50 blocked trades:",
        f"  Would-have WR : {recent_wins/len(recent):.1%}",
        f"  Saved         : ${recent_saved:.2f}",
        f"  Missed        : ${recent_missed:.2f}",
        f"  Net           : ${recent_saved - recent_missed:+.2f}",
        f"",
        f"BY ASSET:",
    ]
    for asset, s in sorted(by_asset.items()):
        wr    = s["wins"] / s["blocked"] if s["blocked"] else 0
        net   = s["saved"] - s["missed"]
        lines.append(
            f"  {asset:<6}  blocked={s['blocked']:>4}  "
            f"blocked-WR={wr:.0%}  "
            f"saved=${s['saved']:.2f}  "
            f"missed=${s['missed']:.2f}  "
            f"net=${net:+.2f}"
        )

    lines += ["", "BY SIGNAL TYPE:"]
    for t, s in sorted(by_type.items()):
        wr  = s["wins"] / s["blocked"] if s["blocked"] else 0
        net = s["saved"] - s["missed"]
        lines.append(
            f"  {t:<10}  blocked={s['blocked']:>4}  "
            f"blocked-WR={wr:.0%}  "
            f"saved=${s['saved']:.2f}  "
            f"missed=${s['missed']:.2f}  "
            f"net=${net:+.2f}"
        )

    lines += [
        "",
        "THRESHOLD SENSITIVITY:",
        f"  Current threshold : (see SKIP_THRESHOLD in pipeline)",
        f"  If ML is hurting  : raise threshold (block fewer trades)",
        f"  If ML is helping  : lower threshold slightly (block more losers)",
        "",
        f"NOTE: 'blocked-WR' is the win rate of trades the ML blocked.",
        f"  If blocked-WR < overall bot WR → ML is correctly filtering losers.",
        f"  If blocked-WR ≈ overall bot WR → ML is blocking indiscriminately.",
    ]

    with open(AUDIT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def print_shadow_summary():
    """Call this from your bot's stats printer to see ML audit inline."""
    path = Path(AUDIT_PATH)
    if path.exists():
        print(path.read_text(encoding="utf-8"))
    else:
        pending_count = len(_pending)
        print(f"  ML shadow log: no resolved blocked trades yet "
              f"({pending_count} pending resolution)")