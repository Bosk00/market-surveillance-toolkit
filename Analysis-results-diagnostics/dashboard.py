import streamlit as st
import pandas as pd
import json
import subprocess
import sys
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh

JSON_PATH = "results_cache.json"

st.set_page_config(
    page_title="BOT TERMINAL",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# ── Terminal styling ────────────────────────────────────────
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=IBM+Plex+Sans:wght@300;400;500&display=swap');

html, body, [class*="css"] {
    background-color: #0a0c0f !important;
    color: #c8d0db !important;
    font-family: 'IBM Plex Sans', monospace !important;
}

.stApp { background-color: #0a0c0f !important; }

h1, h2, h3 { font-family: 'IBM Plex Mono', monospace !important; }

/* Metric cards */
[data-testid="metric-container"] {
    background: #0f1318 !important;
    border: 1px solid #1e2530 !important;
    border-radius: 4px !important;
    padding: 16px !important;
}
[data-testid="stMetricValue"] {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 1.6rem !important;
    font-weight: 600 !important;
    color: #e8edf3 !important;
}
[data-testid="stMetricLabel"] {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.7rem !important;
    color: #4a5568 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.1em !important;
}
[data-testid="stMetricDelta"] {
    font-family: 'IBM Plex Mono', monospace !important;
    font-size: 0.75rem !important;
}

/* Hero banner */
.hero-banner {
    background: #0f1318;
    border: 1px solid #1e2530;
    border-left: 3px solid #00d4aa;
    border-radius: 4px;
    padding: 24px 32px;
    margin-bottom: 24px;
    font-family: 'IBM Plex Mono', monospace;
}
.hero-pnl {
    font-size: 3.2rem;
    font-weight: 600;
    letter-spacing: -0.02em;
    line-height: 1;
}
.hero-pnl.positive { color: #00d4aa; }
.hero-pnl.negative { color: #ff4d6d; }
.hero-sub {
    font-size: 0.8rem;
    color: #4a5568;
    text-transform: uppercase;
    letter-spacing: 0.12em;
    margin-top: 6px;
}
.hero-wr {
    font-size: 1.1rem;
    font-weight: 500;
    color: #7dd3fc;
    margin-top: 12px;
}

/* Streak badge */
.streak-box {
    background: #0f1318;
    border: 1px solid #1e2530;
    border-left: 3px solid #f59e0b;
    border-radius: 4px;
    padding: 16px 20px;
    font-family: 'IBM Plex Mono', monospace;
    text-align: center;
}
.streak-num {
    font-size: 2.4rem;
    font-weight: 600;
    color: #f59e0b;
}
.streak-label {
    font-size: 0.7rem;
    color: #4a5568;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

/* Section headers */
.section-header {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.7rem;
    color: #4a5568;
    text-transform: uppercase;
    letter-spacing: 0.15em;
    border-bottom: 1px solid #1e2530;
    padding-bottom: 8px;
    margin: 28px 0 16px 0;
}

/* Trade rows */
.trade-row {
    display: flex;
    align-items: center;
    padding: 8px 12px;
    border-bottom: 1px solid #0f1318;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.78rem;
    gap: 12px;
}
.trade-row:hover { background: #0f1318; }
.win-dot { color: #00d4aa; font-weight: 700; }
.loss-dot { color: #ff4d6d; font-weight: 700; }
.paper-dot { color: #7dd3fc; font-weight: 700; }
.trade-asset { color: #e8edf3; font-weight: 500; min-width: 80px; }
.trade-entry { color: #7dd3fc; min-width: 60px; }
.trade-pnl-pos { color: #00d4aa; }
.trade-pnl-neg { color: #ff4d6d; }
.trade-time { color: #2d3748; font-size: 0.72rem; }
.trade-conf { color: #4a5568; font-size: 0.72rem; }

/* Status bar */
.status-bar {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 0.7rem;
    color: #2d3748;
    text-align: right;
    padding: 4px 0;
    border-top: 1px solid #0f1318;
    margin-top: 32px;
}
.status-live { color: #00d4aa; }

/* Dataframe */
[data-testid="stDataFrame"] {
    background: #0f1318 !important;
}

/* Hide streamlit chrome */
#MainMenu, footer, header { visibility: hidden; }
.block-container { padding-top: 1.5rem !important; }

/* Win rate bar */
.wr-bar-bg {
    background: #1e2530;
    border-radius: 2px;
    height: 6px;
    width: 100%;
    margin-top: 6px;
}
.wr-bar-fill {
    background: #00d4aa;
    border-radius: 2px;
    height: 6px;
}
</style>
""", unsafe_allow_html=True)

# ── Auto-refresh every 120s ──────────────────────────────────
st_autorefresh(interval=120000, limit=None, key="terminal_refresh")

# ── Run check_results.py ────────────────────────────────────
@st.cache_data(ttl=14)
def refresh_results():
    try:
        result = subprocess.run(
            ["py", "-3.11", "check_results.py"],
            capture_output=True, text=True, timeout=30
        )
        return result.returncode, result.stdout[-200:] if result.stdout else ""
    except Exception as e:
        return -1, str(e)

rc, out = refresh_results()

# ── Load JSON ───────────────────────────────────────────────
def load_data():
    try:
        with open(JSON_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        st.error(f"Cannot read {JSON_PATH}: {e}")
        st.stop()

    trades = []
    for token_id, trade in raw.items():
        t = trade.copy()
        t["token_id"] = token_id
        trades.append(t)

    df = pd.DataFrame(trades)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df["entry"] = pd.to_numeric(df.get("entry", 0), errors="coerce").fillna(0)
    df["size"] = (
        df["size"].astype(str)
        .str.replace("$", "", regex=False)
        .astype(float).fillna(0)
    )
    df["result"] = df.get("result", "PENDING").fillna("PENDING").astype(str).str.upper()

    def conf_tier(c):
        c = (c or "").upper()
        if "HIGH" in c: return "HIGH"
        if "MED" in c: return "MED"
        if "PRICE" in c: return "PRICE"
        if "LOW" in c: return "LOW"
        if "REPRICED" in c: return "REPRICED"
        return "?"

    df["conf_tier"] = df.get("conf", "").apply(conf_tier)
    df["direction"] = df["asset"].apply(lambda x: "Up" if "Up" in str(x) else "Down")
    df["asset_name"] = (
        df["asset"].str.replace(" Up", "", regex=False)
                   .str.replace(" Down", "", regex=False)
    )

    def pnl(row):
        if row["result"] == "WIN" and row["entry"] > 0:
            return (row["size"] / row["entry"]) - row["size"]
        if row["result"] == "LOSS":
            return -row["size"]
        return 0.0

    df["pnl"] = df.apply(pnl, axis=1)
    df = df.sort_values("time", ascending=False).reset_index(drop=True)
    return df

df = load_data()
now = datetime.now()

# ── Compute stats ───────────────────────────────────────────
completed = df[df["result"].isin(["WIN", "LOSS"])]

# Hero uses last 3 days to exclude anomalies/pre-calibration data
hero_cutoff = now - timedelta(days=3)
hero = completed[completed["time"] >= hero_cutoff]
total_wins = (hero["result"] == "WIN").sum()
total_losses = (hero["result"] == "LOSS").sum()
total_trades = len(hero)
total_pnl = hero["pnl"].sum()
total_spent = hero["size"].sum()
overall_wr = (total_wins / total_trades * 100) if total_trades > 0 else 0
overall_roi = (total_pnl / total_spent * 100) if total_spent > 0 else 0

today = completed[completed["time"].dt.date == now.date()]
today_wins = (today["result"] == "WIN").sum()
today_losses = (today["result"] == "LOSS").sum()
today_pnl = today["pnl"].sum()
today_trades = len(today)
today_wr = (today_wins / today_trades * 100) if today_trades > 0 else 0

# Current streak
def current_streak(df_sorted):
    results = df_sorted.sort_values("time")["result"].tolist()
    if not results:
        return 0, "WIN"
    streak_type = results[-1]
    count = 0
    for r in reversed(results):
        if r == streak_type:
            count += 1
        else:
            break
    return count, streak_type

streak_count, streak_type = current_streak(completed)

# Best streak
def best_win_streak(df_sorted):
    results = df_sorted.sort_values("time")["result"].tolist()
    best = cur = 0
    for r in results:
        if r == "WIN":
            cur += 1
            best = max(best, cur)
        else:
            cur = 0
    return best

best_streak = best_win_streak(completed)

# Rolling windows
def window_stats(minutes):
    sub = completed[completed["time"] >= now - timedelta(minutes=minutes)]
    w = (sub["result"] == "WIN").sum()
    l = (sub["result"] == "LOSS").sum()
    p = sub["pnl"].sum()
    wr = (w / (w + l) * 100) if (w + l) > 0 else 0
    return w, l, p, wr

w5, l5, p5, wr5 = window_stats(5)
w15, l15, p15, wr15 = window_stats(15)
w30, l30, p30, wr30 = window_stats(30)
w60, l60, p60, wr60 = window_stats(60)

# ── HERO BANNER ─────────────────────────────────────────────
pnl_class = "positive" if total_pnl >= 0 else "negative"
pnl_sign = "+" if total_pnl >= 0 else ""
hero_start_label = hero_cutoff.strftime("%b %d")

st.markdown(f"""
<div class="hero-banner">
  <div style="display:flex; justify-content:space-between; align-items:flex-start; flex-wrap:wrap; gap:24px;">
    <div>
      <div class="hero-sub">p&l — {hero_start_label} to now</div>
      <div class="hero-pnl {pnl_class}">{pnl_sign}${total_pnl:.2f}</div>
      <div class="hero-wr">
        {overall_wr:.1f}% win rate &nbsp;·&nbsp; {total_wins}W / {total_losses}L &nbsp;·&nbsp; {total_trades:,} trades
      </div>
      <div class="wr-bar-bg"><div class="wr-bar-fill" style="width:{min(overall_wr,100):.1f}%"></div></div>
    </div>
    <div style="text-align:right;">
      <div class="hero-sub">today</div>
      <div class="hero-pnl {'positive' if today_pnl >= 0 else 'negative'}" style="font-size:2rem;">
        {'+'if today_pnl>=0 else ''}${today_pnl:.2f}
      </div>
      <div class="hero-wr" style="font-size:0.9rem;">
        {today_wr:.1f}% wr &nbsp;·&nbsp; {today_wins}W/{today_losses}L &nbsp;·&nbsp; {today_trades} trades
      </div>
    </div>
  </div>
</div>
""", unsafe_allow_html=True)

# ── STREAK + ROI ROW ────────────────────────────────────────
c1, c2, c3, c4 = st.columns(4)

streak_color = "#00d4aa" if streak_type == "WIN" else "#ff4d6d"
streak_emoji = "🔥" if streak_type == "WIN" and streak_count >= 5 else ("✅" if streak_type == "WIN" else "❌")

with c1:
    st.markdown(f"""
    <div class="streak-box" style="border-left-color:{streak_color};">
      <div class="streak-num" style="color:{streak_color};">{streak_emoji} {streak_count}</div>
      <div class="streak-label">current {streak_type.lower()} streak</div>
    </div>""", unsafe_allow_html=True)

with c2:
    st.markdown(f"""
    <div class="streak-box" style="border-left-color:#f59e0b;">
      <div class="streak-num" style="color:#f59e0b;">🏆 {best_streak}</div>
      <div class="streak-label">best win streak</div>
    </div>""", unsafe_allow_html=True)

with c3:
    roi_col = "#00d4aa" if overall_roi >= 0 else "#ff4d6d"
    st.markdown(f"""
    <div class="streak-box" style="border-left-color:{roi_col};">
      <div class="streak-num" style="color:{roi_col};">{'+'if overall_roi>=0 else ''}{overall_roi:.1f}%</div>
      <div class="streak-label">roi (3 days)</div>
    </div>""", unsafe_allow_html=True)

with c4:
    st.markdown(f"""
    <div class="streak-box" style="border-left-color:#7dd3fc;">
      <div class="streak-num" style="color:#7dd3fc;">${total_spent:,.0f}</div>
      <div class="streak-label">volume (3 days)</div>
    </div>""", unsafe_allow_html=True)

# ── ROLLING WINDOWS ─────────────────────────────────────────
st.markdown('<div class="section-header">rolling performance</div>', unsafe_allow_html=True)

cols = st.columns(4)
windows_data = [
    ("5 min", w5, l5, p5, wr5),
    ("15 min", w15, l15, p15, wr15),
    ("30 min", w30, l30, p30, wr30),
    ("60 min", w60, l60, p60, wr60),
]
for col, (label, w, l, p, wr) in zip(cols, windows_data):
    p_str = f"{'+'if p>=0 else ''}${p:.2f}"
    p_col = "#00d4aa" if p >= 0 else "#ff4d6d"
    with col:
        st.markdown(f"""
        <div style="background:#0f1318; border:1px solid #1e2530; border-radius:4px; padding:14px 16px;">
          <div style="font-family:'IBM Plex Mono',monospace; font-size:0.65rem; color:#4a5568; text-transform:uppercase; letter-spacing:0.12em; margin-bottom:8px;">{label}</div>
          <div style="font-family:'IBM Plex Mono',monospace; font-size:1.3rem; font-weight:600; color:{p_col};">{p_str}</div>
          <div style="font-family:'IBM Plex Mono',monospace; font-size:0.75rem; color:#7dd3fc; margin-top:4px;">{wr:.0f}% wr &nbsp; {w}W/{l}L</div>
          <div class="wr-bar-bg"><div class="wr-bar-fill" style="width:{min(wr,100):.0f}%; background:{'#00d4aa' if wr>=80 else '#f59e0b' if wr>=65 else '#ff4d6d'};"></div></div>
        </div>""", unsafe_allow_html=True)

# ── PER ASSET ───────────────────────────────────────────────
st.markdown('<div class="section-header">per asset — all time</div>', unsafe_allow_html=True)

asset_cols = st.columns(len(df["asset_name"].unique()) or 1)
for col, asset in zip(asset_cols, sorted(df["asset_name"].unique())):
    sub = completed[completed["asset_name"] == asset]
    aw = (sub["result"] == "WIN").sum()
    al = (sub["result"] == "LOSS").sum()
    ap = sub["pnl"].sum()
    awr = (aw / (aw + al) * 100) if (aw + al) > 0 else 0
    p_col = "#00d4aa" if ap >= 0 else "#ff4d6d"
    with col:
        st.markdown(f"""
        <div style="background:#0f1318; border:1px solid #1e2530; border-radius:4px; padding:14px 16px;">
          <div style="font-family:'IBM Plex Mono',monospace; font-size:0.85rem; font-weight:600; color:#e8edf3; margin-bottom:6px;">{asset}</div>
          <div style="font-family:'IBM Plex Mono',monospace; font-size:1.1rem; color:{p_col}; font-weight:600;">{'+'if ap>=0 else ''}${ap:.2f}</div>
          <div style="font-family:'IBM Plex Mono',monospace; font-size:0.72rem; color:#7dd3fc; margin-top:4px;">{awr:.0f}% &nbsp; {aw}W/{al}L</div>
          <div class="wr-bar-bg" style="margin-top:6px;"><div class="wr-bar-fill" style="width:{min(awr,100):.0f}%;"></div></div>
        </div>""", unsafe_allow_html=True)

# ── RECENT TRADES ───────────────────────────────────────────
st.markdown('<div class="section-header">recent trades</div>', unsafe_allow_html=True)

n_trades = st.slider("", 10, 50, 20, 5, label_visibility="collapsed")
recent = df.head(n_trades)

trade_html = '<div style="background:#0f1318; border:1px solid #1e2530; border-radius:4px; overflow:hidden;">'
for _, row in recent.iterrows():
    res = row["result"]
    if res == "WIN":
        dot = '<span class="win-dot">▲</span>'
        pnl_str = f'<span class="trade-pnl-pos">+${row["pnl"]:.2f}</span>'
    elif res == "LOSS":
        dot = '<span class="loss-dot">▼</span>'
        pnl_str = f'<span class="trade-pnl-neg">-${abs(row["pnl"]):.2f}</span>'
    else:
        dot = '<span class="paper-dot">○</span>'
        pnl_str = '<span style="color:#4a5568;">pending</span>'

    t = row["time"].strftime("%H:%M:%S") if pd.notna(row["time"]) else "—"
    conf = str(row.get("conf", "")).upper()[:12]
    conf_short = "PRICE" if "PRICE" in conf else ("HIGH" if "HIGH" in conf else ("MED" if "MED" in conf else ("LOW" if "LOW" in conf else "?")))

    trade_html += f"""
    <div class="trade-row">
      {dot}
      <span class="trade-asset">{row['asset']}</span>
      <span class="trade-entry">{row['entry']:.3f}</span>
      {pnl_str}
      <span class="trade-conf">{conf_short}</span>
      <span class="trade-time" style="margin-left:auto;">{t}</span>
    </div>"""

trade_html += "</div>"
st.markdown(trade_html, unsafe_allow_html=True)

# ── CONFIDENCE BREAKDOWN ────────────────────────────────────
st.markdown('<div class="section-header">confidence breakdown</div>', unsafe_allow_html=True)

conf_rows = []
for tier, g in completed.groupby("conf_tier"):
    cw = (g["result"] == "WIN").sum()
    cl = (g["result"] == "LOSS").sum()
    cp = g["pnl"].sum()
    cwr = (cw / (cw + cl) * 100) if (cw + cl) > 0 else 0
    conf_rows.append({"Tier": tier, "Trades": cw+cl, "W": cw, "L": cl,
                       "WR%": f"{cwr:.1f}%", "P&L": f"{'+'if cp>=0 else ''}${cp:.2f}"})

if conf_rows:
    cdf = pd.DataFrame(conf_rows).sort_values("Trades", ascending=False)
    st.dataframe(
        cdf,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Tier": st.column_config.TextColumn("TIER"),
            "Trades": st.column_config.NumberColumn("TRADES"),
            "W": st.column_config.NumberColumn("W"),
            "L": st.column_config.NumberColumn("L"),
            "WR%": st.column_config.TextColumn("WIN RATE"),
            "P&L": st.column_config.TextColumn("P&L"),
        }
    )

# ── STATUS BAR ──────────────────────────────────────────────
last_update = datetime.now().strftime("%H:%M:%S")
st.markdown(f"""
<div class="status-bar">
  <span class="status-live">● LIVE</span> &nbsp;·&nbsp;
  last sync {last_update} &nbsp;·&nbsp;
  check_results: {'ok' if rc == 0 else f'err({rc})'} &nbsp;·&nbsp;
  {total_trades:,} trades loaded
</div>
""", unsafe_allow_html=True)