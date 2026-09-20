
import re, json, warnings, pickle, time
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import xgboost as xgb
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import roc_auc_score, average_precision_score
from sklearn.preprocessing import LabelEncoder
from sklearn.calibration import CalibratedClassifierCV
from pathlib import Path

warnings.filterwarnings("ignore")

# ── CONFIG ────────────────────────────────────────────────────────────────────
XLSX_PATH      = "trades_analysis.xlsx"
JSON_PATH      = ""                        # same data as xlsx — leave blank
LOG_PATH       = "trades_log.txt"
MODEL_OUT      = "trade_filter_model.json"
CALIB_OUT      = "trade_filter_calib.pkl"
FEATURES_OUT   = "trade_filter_features.pkl"
PLOT_OUT       = "trade_analysis.png"
SKIP_THRESHOLD = 0.58
CYCLE_GAP_SEC  = 60    # trades within this window = same cycle
MIN_SAMPLES_PER_FOLD = 30
# ─────────────────────────────────────────────────────────────────────────────

SCHEMA = ["DateTime", "Asset", "Direction", "Confidence",
          "Entry", "MarketAsk", "Size", "PnL", "Result", "Source"]

# ── CLEANING HELPERS ──────────────────────────────────────────────────────────

def _clean_result(s):
    s = str(s).strip().upper()
    if s in ("WIN", "W"):   return "WIN"
    if s in ("LOSS", "L"):  return "LOSS"
    return None

def _clean_size(s):
    try:   return float(re.sub(r"[^\d.]", "", str(s)))
    except: return np.nan

def _parse_asset_dir(s):
    parts = str(s).strip().rsplit(" ", 1)
    if len(parts) == 2 and parts[1] in ("Up", "Down"):
        return parts[0].strip(), parts[1]
    return parts[0].strip(), ""

def _to_dt(date_s, time_s):
    return pd.to_datetime((str(date_s) + " " + str(time_s)).strip(), errors="coerce")

# ── LOADERS ───────────────────────────────────────────────────────────────────

def load_xlsx(path):
    raw = pd.read_excel(path, dtype=str)
    raw.columns = [c.strip() for c in raw.columns]

    def find(options, default=""):
        for o in options:
            if o in raw.columns: return raw[o]
        return pd.Series(default, index=raw.index)

    date_col  = find(["Date","date"])
    time_col  = find(["Time","time"])
    asset_col = find(["Asset","asset"])
    dir_col   = find(["Direction","direction"])
    conf_col  = find(["Confidence","confidence","conf"])
    entry_col = find(["Entry","entry"])
    ask_col   = find(["Market Ask","market_ask","MarketAsk","Quoted ask","quoted_ask"])
    size_col  = find(["Size ($)","Size","size"])
    pnl_col   = find(["P&L ($)","P&L","PnL","pnl"])
    res_col   = find(["Result","result"])

    rows = []
    for i in range(len(raw)):
        asset, direction = _parse_asset_dir(asset_col.iloc[i])
        if str(dir_col.iloc[i]).strip() not in ("", "nan"):
            direction = str(dir_col.iloc[i]).strip()
        rows.append({
            "DateTime":   _to_dt(date_col.iloc[i], time_col.iloc[i]),
            "Asset":      asset,
            "Direction":  direction,
            "Confidence": str(conf_col.iloc[i]),
            "Entry":      pd.to_numeric(entry_col.iloc[i], errors="coerce"),
            "MarketAsk":  pd.to_numeric(ask_col.iloc[i],   errors="coerce"),
            "Size":       _clean_size(size_col.iloc[i]),
            "PnL":        pd.to_numeric(pnl_col.iloc[i],   errors="coerce"),
            "Result":     _clean_result(res_col.iloc[i]),
            "Source":     "xlsx",
        })
    df = pd.DataFrame(rows, columns=SCHEMA)
    print(f"  xlsx:  {len(df):>6,} rows loaded")
    return df

def load_json(path):
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    rows = []
    for token_id, v in data.items():
        asset, direction = _parse_asset_dir(v.get("asset",""))
        rows.append({
            "DateTime":   pd.to_datetime(str(v.get("time","")), errors="coerce"),
            "Asset":      asset,
            "Direction":  direction,
            "Confidence": str(v.get("conf","")),
            "Entry":      float(v["entry"]) if v.get("entry") is not None else np.nan,
            "MarketAsk":  float(v["market_ask"]) if v.get("market_ask") is not None else np.nan,
            "Size":       _clean_size(v.get("size","")),
            "PnL":        np.nan,
            "Result":     _clean_result(v.get("result","")),
            "Source":     "json",
        })
    df = pd.DataFrame(rows, columns=SCHEMA)
    print(f"  json:  {len(df):>6,} rows loaded")
    return df

def load_log(path):
    blocks, cur = [], {}
    with open(path, encoding="utf-8", errors="ignore") as f:
        for line in f:
            line = line.strip()
            if line.startswith("====="):
                if cur: blocks.append(cur)
                cur = {}
            elif ":" in line:
                k, _, val = line.partition(":")
                cur[k.strip()] = val.strip()
    if cur: blocks.append(cur)

    rows = []
    for b in blocks:
        asset, direction = _parse_asset_dir(b.get("Asset",""))
        rows.append({
            "DateTime":   pd.to_datetime(b.get("Time",""), errors="coerce"),
            "Asset":      asset,
            "Direction":  direction,
            "Confidence": b.get("Confidence",""),
            "Entry":      pd.to_numeric(b.get("Entry",""),      errors="coerce"),
            "MarketAsk":  pd.to_numeric(b.get("Quoted ask", b.get("Market ask","")), errors="coerce"),
            "Size":       _clean_size(b.get("Size","")),
            "PnL":        np.nan,
            "Result":     _clean_result(b.get("Result","")),
            "Source":     "log",
        })
    df = pd.DataFrame(rows, columns=SCHEMA)
    print(f"  log:   {len(df):>6,} rows loaded")
    return df

def merge_all(xlsx_path, json_path, log_path):
    frames = []
    for path, loader in [(xlsx_path, load_xlsx), (json_path, load_json), (log_path, load_log)]:
        if path and Path(path).exists():
            frames.append(loader(path))
        elif path:
            print(f"  [WARN] {path} not found, skipping.")

    if not frames:
        raise FileNotFoundError("No data files found.")

    df = pd.concat(frames, ignore_index=True)
    df = df[df["Result"].notna()].copy()
    df["is_win"] = (df["Result"] == "WIN").astype(int)
    df = df.sort_values("DateTime").reset_index(drop=True)

    print(f"\n  Total resolved trades : {len(df):,}")
    print(f"  Date range            : {df['DateTime'].min()} → {df['DateTime'].max()}")
    print(f"  Overall WR            : {df['is_win'].mean():.2%}")
    print(f"  Assets                : {sorted(df['Asset'].dropna().unique().tolist())}")
    return df

# ── CYCLE HELPERS ─────────────────────────────────────────────────────────────

def assign_cycles(df: pd.DataFrame, gap_seconds: int = CYCLE_GAP_SEC) -> np.ndarray:
    """
    Trades within gap_seconds of each other = same cycle.
    Returns an integer array of cycle IDs (0, 1, 2, ...).
    """
    times = df["DateTime"].values
    cycle_ids = np.zeros(len(df), dtype=int)
    cid = 0
    for i in range(1, len(df)):
        dt = (pd.Timestamp(times[i]) - pd.Timestamp(times[i-1])).total_seconds()
        if dt > gap_seconds:
            cid += 1
        cycle_ids[i] = cid
    return cycle_ids


def cycle_loss_streak(df: pd.DataFrame) -> np.ndarray:
    """
    For each trade, how many consecutive losing cycles came BEFORE it.
    A cycle is a loss if its net PnL <= 0 (or majority of trades lost
    when PnL is unavailable).

    Multiple assets losing in one burst = ONE losing cycle, not N.
    """
    cids = df["_cycle_id"].values
    n    = len(df)

    # Net PnL per cycle — fall back to majority vote when PnL missing
    cycle_pnl   = {}
    cycle_wins  = {}
    cycle_total = {}
    for i in range(n):
        cid = int(cids[i])
        pnl = df["PnL"].iloc[i]
        w   = df["is_win"].iloc[i]
        if not np.isnan(pnl if pnl is not None else np.nan):
            cycle_pnl[cid]  = cycle_pnl.get(cid, 0.0) + float(pnl)
        cycle_wins[cid]  = cycle_wins.get(cid, 0)  + int(w)
        cycle_total[cid] = cycle_total.get(cid, 0) + 1

    def cycle_won(cid):
        if cid in cycle_pnl:
            return cycle_pnl[cid] > 0
        # Fallback: majority of trades won
        return cycle_wins.get(cid, 0) > cycle_total.get(cid, 1) / 2

    # Build streak_before[cycle_id] = consecutive losing cycles before this one
    sorted_cids = sorted(set(cids))
    streak_before = {}
    streak = 0
    for cid in sorted_cids:
        streak_before[cid] = streak
        streak = streak + 1 if not cycle_won(cid) else 0

    return np.array([streak_before[int(c)] for c in cids])


def last_cycle_streak(df: pd.DataFrame) -> int:
    """
    How many consecutive losing cycles are at the END of df.
    Used to initialise the test-fold consec_losses feature.
    """
    if len(df) == 0:
        return 0
    df = df.copy()
    df["_cycle_id"] = assign_cycles(df)

    # Build ordered list of (cycle_id, won)
    cids = sorted(set(df["_cycle_id"].values))
    results = []
    for cid in cids:
        mask = df["_cycle_id"] == cid
        pnl_vals = df.loc[mask, "PnL"].dropna()
        if len(pnl_vals):
            results.append((cid, pnl_vals.sum() > 0))
        else:
            wins  = df.loc[mask, "is_win"].sum()
            total = mask.sum()
            results.append((cid, wins > total / 2))

    streak = 0
    for _, won in reversed(results):
        if not won:
            streak += 1
        else:
            break
    return streak

# ── FEATURE ENGINEERING ───────────────────────────────────────────────────────

_PRICE_PCT = re.compile(r"\(([+-]?\d+\.\d+)%")
_CONF_PCT  = re.compile(r"x\s*(\d+(?:\.\d+)?)%\)")

def _parse_conf(s):
    s = str(s)
    price_pct = float(m.group(1)) if (m := _PRICE_PCT.search(s)) else np.nan
    conf_pct  = float(m.group(1)) if (m := _CONF_PCT.search(s))  else np.nan
    su = s.upper()
    is_arb      = int("ARB_HEDGE" in su or s.strip() == "?")
    is_strong   = int("STRONG"    in su)
    is_repriced = int("REPRICED"  in su)
    is_sweep    = int("SWEEP" in su or "HIGH" in su or "MEDIUM" in su)
    is_price_sig= int("PRICE" in su)
    if "STRONG" in su or "HIGH" in su:  level = 3
    elif "MED"  in su:                  level = 2
    elif "LOW"  in su:                  level = 1
    else:                               level = 0
    return price_pct, conf_pct, is_arb, is_strong, is_repriced, is_sweep, is_price_sig, level

def _session(h):
    if 3  <= h < 7:  return "early_am"
    if 7  <= h < 9:  return "am_open"
    if 9  <= h < 12: return "am_session"
    if 12 <= h < 14: return "midday"
    if 15 <= h < 18: return "pm_window"
    if 18 <= h < 21: return "pm_close"
    return "overnight"

SESSION_MAP = {s: i for i, s in enumerate(
    ["overnight","early_am","am_open","am_session","midday","pm_window","pm_close"]
)}

def engineer_base(df: pd.DataFrame) -> pd.DataFrame:
    """Static features only — no target used, no leakage."""
    df = df.copy()

    parsed = df["Confidence"].apply(_parse_conf)
    (df["price_move_pct"], df["signal_conf_pct"],
     df["is_arb"], df["is_strong"], df["is_repriced"],
     df["is_sweep"], df["is_price_sig"],
     df["conf_level"]) = zip(*parsed)

    df["implied_payout"] = 1.0 / df["Entry"].clip(lower=0.01)
    df["ev_proxy"]       = (df["signal_conf_pct"].fillna(50) / 100.0) * df["implied_payout"] - 1.0
    df["slippage_abs"]   = (df["Entry"] - df["MarketAsk"]).abs().fillna(0)
    df["has_slippage"]   = (df["slippage_abs"] > 0.001).astype(int)

    dt = df["DateTime"]
    df["hour"] = dt.dt.hour.fillna(-1).astype(int)
    df["dow"]  = dt.dt.dayofweek.fillna(-1).astype(int)

    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["dow_sin"]  = np.sin(2 * np.pi * df["dow"]  / 7)
    df["dow_cos"]  = np.cos(2 * np.pi * df["dow"]  / 7)

    df["is_weekend"] = (df["dow"] >= 5).astype(int)
    df["session"]    = df["hour"].apply(_session).map(SESSION_MAP).fillna(0).astype(int)

    le = LabelEncoder()
    df["asset_enc"] = le.fit_transform(df["Asset"].fillna("UNK"))
    df["is_up"]     = (df["Direction"].str.strip().str.upper() == "UP").astype(int)
    df["asset_dir"] = df["asset_enc"] * 2 + df["is_up"]

    df["entry_bucket"] = pd.cut(
        df["Entry"], bins=[0,.75,.80,.85,.90,.95,1.01], labels=[0,1,2,3,4,5]
    ).astype(float)

    df["log_size"] = np.log1p(df["Size"].clip(lower=0))

    df["dir_price_agree"] = (
        ((df["is_up"] == 1) & (df["price_move_pct"].fillna(0) > 0)) |
        ((df["is_up"] == 0) & (df["price_move_pct"].fillna(0) < 0))
    ).astype(int)

    # Assign cycle IDs — used by cycle_loss_streak
    df["_cycle_id"] = assign_cycles(df)

    return df


def add_fold_features(df_train: pd.DataFrame, df_test: pd.DataFrame):
    """
    Compute rolling win-rate and CYCLE-AWARE streak from training data only,
    then apply the end-of-training values to test rows.
    """
    df_train = df_train.copy()
    df_test  = df_test.copy()

    wins = df_train["is_win"].values

    # ── Rolling win rates for training rows (shift to avoid self-leakage) ──
    for w in [10, 25, 50]:
        df_train[f"rolling_wr_{w}"] = (
            df_train["is_win"].shift(1).rolling(w, min_periods=3).mean().fillna(0.5)
        )
        # Test rows all get the single scalar from end of training set
        val = wins[-w:].mean() if len(wins) >= w else (wins.mean() if len(wins) else 0.5)
        df_test[f"rolling_wr_{w}"] = val

    # ── Cycle-aware consecutive losing streak ──
    df_train["consec_losses"] = cycle_loss_streak(df_train)

    # End-of-training streak → applied as constant to all test rows
    end_streak = last_cycle_streak(df_train)
    df_test["consec_losses"] = end_streak

    return df_train, df_test


BASE_FEATURES = [
    "Entry", "entry_bucket", "implied_payout",
    "price_move_pct", "signal_conf_pct", "ev_proxy", "conf_level",
    "is_strong", "is_repriced", "is_arb", "is_sweep", "is_price_sig",
    "is_up", "asset_enc", "asset_dir",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "session", "is_weekend",
    "slippage_abs", "has_slippage", "log_size",
    "dir_price_agree",
]
FOLD_FEATURES = ["rolling_wr_10", "rolling_wr_25", "rolling_wr_50", "consec_losses"]
FEATURES = BASE_FEATURES + FOLD_FEATURES


def build_matrix(df):
    cols = [c for c in FEATURES if c in df.columns]
    X = df[cols].copy().fillna(df[cols].median(numeric_only=True))
    return X, df["is_win"].values, cols

# ── WALK-FORWARD VALIDATION ───────────────────────────────────────────────────

def get_xgb_params(n_samples):
    if n_samples < 300:
        n_est, depth, mcw = 100, 3, 5
    elif n_samples < 1000:
        n_est, depth, mcw = 200, 4, 8
    else:
        n_est, depth, mcw = 400, 4, 10
    return dict(
        n_estimators=n_est, learning_rate=0.05, max_depth=depth,
        subsample=0.8, colsample_bytree=0.8, min_child_weight=mcw, gamma=1,
        use_label_encoder=False, eval_metric="logloss",
        random_state=42, n_jobs=-1,
    )


def walk_forward(df, n_splits=5):
    tscv = TimeSeriesSplit(n_splits=n_splits)
    rows = []
    for fold, (tr_idx, te_idx) in enumerate(tscv.split(np.arange(len(df)))):
        df_tr = df.iloc[tr_idx].copy()
        df_te = df.iloc[te_idx].copy()

        if len(df_tr) < MIN_SAMPLES_PER_FOLD:
            print(f"  Fold {fold+1}: ⚠️  only {len(df_tr)} training samples — skipping")
            continue

        df_tr, df_te = add_fold_features(df_tr, df_te)

        X_tr = df_tr.reindex(columns=FEATURES, fill_value=0).fillna(0)
        y_tr = df_tr["is_win"].values
        X_te = df_te.reindex(columns=FEATURES, fill_value=0).fillna(0)
        y_te = df_te["is_win"].values

        pos = y_tr.sum(); neg = len(y_tr) - pos
        spw = neg / pos if pos > 0 else 1.0

        params = get_xgb_params(len(df_tr))
        params["scale_pos_weight"] = spw

        m = xgb.XGBClassifier(**params)
        m.fit(X_tr, y_tr, eval_set=[(X_te, y_te)], verbose=False)

        prob = m.predict_proba(X_te)[:, 1]
        kept = prob >= SKIP_THRESHOLD
        wr_all  = y_te.mean()
        wr_filt = y_te[kept].mean() if kept.any() else np.nan
        auc     = roc_auc_score(y_te, prob) if len(np.unique(y_te)) > 1 else 0.5
        ap      = average_precision_score(y_te, prob) if len(np.unique(y_te)) > 1 else 0.5

        rows.append(dict(
            fold=fold+1, auc=auc, ap=ap,
            wr_all=wr_all, wr_filt=wr_filt,
            kept_pct=kept.mean(), n_train=len(df_tr), n_test=len(df_te)
        ))
        print(f"  Fold {fold+1}  n={len(df_tr)}+{len(df_te)}  "
              f"AUC={auc:.3f}  AvgPrec={ap:.3f}  "
              f"WR {wr_all:.2%}→{wr_filt:.2%}  "
              f"Kept {kept.mean():.0%}  spw={spw:.1f}")

    fd = pd.DataFrame(rows)
    if len(fd):
        print(f"\n  Mean AUC : {fd['auc'].mean():.3f} ± {fd['auc'].std():.3f}")
    return fd

# ── PLOTS ─────────────────────────────────────────────────────────────────────

def make_plots(df, model, feat_cols, fold_df):
    BG, CARD = "#0f1117", "#1a1d2e"
    G, R, B  = "#2dd4a2", "#f87171", "#60a5fa"
    AMB, MUT = "#fbbf24", "#6b7280"
    TXT      = "#e2e8f0"

    fig = plt.figure(figsize=(18,14), facecolor=BG)
    gs  = gridspec.GridSpec(3, 3, figure=fig, hspace=0.48, wspace=0.35)

    def sa(ax, title=""):
        ax.set_facecolor(CARD)
        ax.tick_params(colors=TXT, labelsize=8)
        for sp in ax.spines.values():
            sp.set_edgecolor(MUT); sp.set_linewidth(0.5)
        ax.grid(axis="y", color=MUT, alpha=0.2, lw=0.5)
        if title: ax.set_title(title, color=TXT, fontsize=9, pad=6, fontweight="bold")

    ax = fig.add_subplot(gs[0,0])
    awr = df.groupby("Asset")["is_win"].agg(["mean","count"])
    awr = awr[awr["count"]>=5].sort_values("mean")
    ax.barh(awr.index, awr["mean"]*100,
            color=[G if v>.5 else R for v in awr["mean"]], alpha=.85)
    ax.axvline(50, color=AMB, lw=1, ls="--", alpha=.6)
    ax.set_xlabel("Win rate %", color=TXT, fontsize=8); sa(ax, "Win rate by asset")

    ax = fig.add_subplot(gs[0,1])
    lmap = {0:"other/unk",1:"low",2:"med",3:"strong/high"}
    df["_cl"] = df["conf_level"].map(lmap)
    cwr = df.groupby("_cl")["is_win"].mean().dropna()
    ax.bar(cwr.index, cwr.values*100,
           color=[G if v>.5 else R for v in cwr.values], alpha=.85)
    ax.axhline(50, color=AMB, lw=1, ls="--", alpha=.6)
    ax.set_ylabel("Win rate %", color=TXT, fontsize=8); sa(ax, "Win rate by confidence")

    ax = fig.add_subplot(gs[0,2])
    hwr = df.groupby("hour")["is_win"].mean()
    ax.bar(hwr.index, hwr.values*100,
           color=[G if v>.5 else R for v in hwr.values], alpha=.85, width=.7)
    ax.axhline(50, color=AMB, lw=1, ls="--", alpha=.6)
    for span in [(3,7),(15,18)]:
        ax.axvspan(span[0], span[1], color=G, alpha=0.08)
    ax.set_xlabel("Hour (UTC)", color=TXT, fontsize=8)
    ax.set_ylabel("Win rate %", color=TXT, fontsize=8); sa(ax, "Win rate by hour")

    ax = fig.add_subplot(gs[1,:2])
    if df["PnL"].notna().any():
        cum = df["PnL"].fillna(0).cumsum()
        col = G if cum.iloc[-1] > 0 else R
        ax.plot(cum.values, color=col, lw=1.2)
        ax.fill_between(range(len(cum)), cum.values, alpha=.1, color=col)
    ax.axhline(0, color=MUT, lw=.8)
    ax.set_xlabel("Trade #", color=TXT, fontsize=8)
    ax.set_ylabel("Cumulative P&L ($)", color=TXT, fontsize=8)
    sa(ax, "Cumulative P&L")

    ax = fig.add_subplot(gs[1,2])
    fi = pd.Series(model.feature_importances_, index=feat_cols).sort_values(ascending=True).tail(12)
    ax.barh(fi.index, fi.values, color=B, alpha=.8)
    ax.set_xlabel("Importance", color=TXT, fontsize=8); sa(ax, "Feature importance (top 12)")

    ax = fig.add_subplot(gs[2,0])
    if len(fold_df):
        ax.plot(fold_df["fold"], fold_df["auc"], marker="o", color=B, lw=1.5)
    ax.axhline(.5, color=MUT, ls="--", lw=.8, alpha=.6)
    ax.set_ylim(.4,1.); ax.set_xlabel("Fold", color=TXT, fontsize=8)
    ax.set_ylabel("AUC", color=TXT, fontsize=8); sa(ax, "Walk-forward AUC")

    ax = fig.add_subplot(gs[2,1])
    if len(fold_df):
        x = np.arange(len(fold_df)); w=.35
        ax.bar(x-w/2, fold_df["wr_all"]*100,  w, color=MUT, alpha=.8, label="All")
        ax.bar(x+w/2, fold_df["wr_filt"].fillna(0)*100, w, color=G, alpha=.8, label="Filtered")
        ax.axhline(50, color=AMB, lw=1, ls="--", alpha=.6)
        ax.set_xticks(x); ax.set_xticklabels(fold_df["fold"])
    ax.set_xlabel("Fold", color=TXT, fontsize=8)
    ax.set_ylabel("Win rate %", color=TXT, fontsize=8)
    ax.legend(fontsize=7, facecolor=CARD, edgecolor=MUT, labelcolor=TXT)
    sa(ax, "All vs filtered win rate")

    ax = fig.add_subplot(gs[2,2])
    blabels = {0:"<0.75",1:"0.75–0.80",2:"0.80–0.85",3:"0.85–0.90",4:"0.90–0.95",5:"0.95+"}
    bwr = df.groupby("entry_bucket")["is_win"].mean().dropna()
    lbls = [blabels.get(int(i),"?") for i in bwr.index]
    ax.bar(lbls, bwr.values*100,
           color=[G if v>.5 else R for v in bwr.values], alpha=.85)
    ax.axhline(50, color=AMB, lw=1, ls="--", alpha=.6)
    ax.set_ylabel("Win rate %", color=TXT, fontsize=8)
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=35, ha="right", fontsize=7)
    sa(ax, "Win rate by entry price")

    for a in fig.axes:
        a.tick_params(colors=TXT)

    fig.suptitle("Trading Bot ML Analysis v3", color=TXT, fontsize=14, fontweight="bold", y=0.98)
    plt.savefig(PLOT_OUT, facecolor=BG, dpi=150, bbox_inches="tight")
    print(f"\n  Plot saved → {PLOT_OUT}")
    plt.close()

# ── LIVE FILTER ───────────────────────────────────────────────────────────────

class TradeFilter:
    """
    Live trade filter with cycle-aware consecutive loss tracking.

    Usage:
        filt = TradeFilter.load()

        # Before placing a trade:
        take, prob = filt.should_take(trade_dict)
        if take:
            place_order()

        # After market resolves — pass the cycle timestamp so the filter
        # can group simultaneous trades into cycles correctly:
        filt.record_outcome(won=True, cycle_ts=trade_dict["datetime"])

    Circuit breaker (implement in your bot):
        if filt.cycle_loss_streak() >= 3:
            pause_trading(minutes=20)
    """

    def __init__(self, model, feature_cols, threshold=SKIP_THRESHOLD, calib=None):
        self.model        = model
        self.feature_cols = feature_cols
        self.threshold    = threshold
        self.calib        = calib
        # Each entry: {"ts": datetime, "won": bool}
        self._outcome_buf: list = []

    @classmethod
    def load(cls, model_path=MODEL_OUT, feat_path=FEATURES_OUT,
             calib_path=CALIB_OUT, threshold=SKIP_THRESHOLD):
        m = xgb.XGBClassifier()
        m.load_model(model_path)
        with open(feat_path, "rb") as f:
            cols = pickle.load(f)
        calib = None
        if Path(calib_path).exists():
            with open(calib_path, "rb") as f:
                calib = pickle.load(f)
        return cls(m, cols, threshold, calib)

    # ── Cycle-aware rolling stats from live outcome buffer ──────────────────

    def _get_cycles(self):
        """
        Group buffered outcomes into cycles by timestamp proximity.
        Returns list of (cycle_ts, won) where won = net positive cycle.
        """
        if not self._outcome_buf:
            return []
        buf = sorted(self._outcome_buf, key=lambda x: x["ts"])
        cycles = []
        cur_ts  = buf[0]["ts"]
        cur_wins, cur_total = 0, 0

        for entry in buf:
            gap = (entry["ts"] - cur_ts).total_seconds()
            if gap > CYCLE_GAP_SEC and cur_total > 0:
                cycles.append({"ts": cur_ts, "won": cur_wins > cur_total / 2})
                cur_ts, cur_wins, cur_total = entry["ts"], 0, 0
            cur_wins  += int(entry["won"])
            cur_total += 1
            cur_ts = entry["ts"]   # advance to latest ts in cycle

        if cur_total > 0:
            cycles.append({"ts": cur_ts, "won": cur_wins > cur_total / 2})
        return cycles

    def cycle_loss_streak(self) -> int:
        """Consecutive losing cycles at the current moment. Use for circuit breaker."""
        cycles = self._get_cycles()
        streak = 0
        for c in reversed(cycles):
            if not c["won"]:
                streak += 1
            else:
                break
        return streak

    def _rolling_wr(self, window: int) -> float:
        cycles = self._get_cycles()
        recent = cycles[-window:] if len(cycles) >= 3 else cycles
        if not recent:
            return 0.5
        return sum(c["won"] for c in recent) / len(recent)

    # ── Predict ─────────────────────────────────────────────────────────────

    def _to_row(self, trade: dict) -> pd.DataFrame:
        row = {
            "DateTime":   pd.to_datetime(trade.get("datetime",""), errors="coerce"),
            "Asset":      trade.get("asset",""),
            "Direction":  trade.get("direction",""),
            "Confidence": trade.get("confidence",""),
            "Entry":      trade.get("entry", np.nan),
            "MarketAsk":  trade.get("market_ask", trade.get("entry", np.nan)),
            "Size":       trade.get("size", 5),
            "PnL":        np.nan, "Result": None, "Source": "live",
        }
        df = pd.DataFrame([row], columns=SCHEMA)
        df["is_win"]   = 0
        df["_cycle_id"] = 0
        df = engineer_base(df)

        df["rolling_wr_10"]  = self._rolling_wr(10)
        df["rolling_wr_25"]  = self._rolling_wr(25)
        df["rolling_wr_50"]  = self._rolling_wr(50)
        df["consec_losses"]  = self.cycle_loss_streak()
        return df

    def predict_proba(self, trade: dict) -> float:
        try:
            df = self._to_row(trade)
            X  = df.reindex(columns=self.feature_cols, fill_value=0).fillna(0)
            src = self.calib if self.calib is not None else self.model
            return float(src.predict_proba(X)[0, 1])
        except Exception as e:
            print(f"  ⚠️  TradeFilter.predict_proba error: {e}")
            return 0.5

    def should_take(self, trade: dict) -> tuple:
        """
        Returns (take: bool, win_probability: float).
        ARB hedge trades always pass through.
        """
        conf = str(trade.get("confidence", "")).strip()
        if "ARB_HEDGE" in conf or conf == "?":
            return True, 1.0
        prob = self.predict_proba(trade)
        return prob >= self.threshold, round(prob, 4)

    def record_outcome(self, won: bool, cycle_ts=None):
        """
        Call after each trade resolves.
        cycle_ts should be the trade's datetime string or datetime object
        so simultaneous trades group into the same cycle.
        """
        ts = pd.to_datetime(cycle_ts, errors="coerce") if cycle_ts else pd.Timestamp.now()
        if pd.isna(ts):
            ts = pd.Timestamp.now()
        self._outcome_buf.append({"ts": ts, "won": won})
        # Keep last 200 outcomes (~50 cycles)
        if len(self._outcome_buf) > 200:
            self._outcome_buf = self._outcome_buf[-200:]

# ── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 55)
    print("  Trading Bot ML Pipeline  v3")
    print("=" * 55)

    print("\n[1/6] Loading & merging data...")
    df = merge_all(XLSX_PATH, JSON_PATH, LOG_PATH)

    print("\n[2/6] Engineering base features...")
    arb_mask = (
        df["Confidence"].str.contains("ARB_HEDGE", na=False) |
        (df["Confidence"].str.strip() == "?")
    )
    df_arb = df[arb_mask].copy()
    df     = df[~arb_mask].copy()
    print(f"  Main trades : {len(df):,}")
    print(f"  ARB hedges  : {len(df_arb):,}  (excluded from training)")

    df = engineer_base(df)

    # Log cycle stats
    n_cycles = df["_cycle_id"].nunique()
    print(f"  Cycles found: {n_cycles:,}  "
          f"(avg {len(df)/n_cycles:.1f} trades/cycle, gap={CYCLE_GAP_SEC}s)")

    print("\n[3/6] Walk-forward validation (leak-free)...")
    fold_df = walk_forward(df)

    print("\n[4/6] Training final model on full dataset...")
    df_final, _ = add_fold_features(df, df.head(1))
    feat_cols   = [c for c in FEATURES if c in df_final.columns]

    X_final = df_final.reindex(columns=feat_cols, fill_value=0).fillna(0)
    y_final = df_final["is_win"].values

    pos = y_final.sum(); neg = len(y_final) - pos
    spw = neg / pos if pos > 0 else 1.0
    print(f"  scale_pos_weight = {spw:.2f}  ({pos} wins / {neg} losses)")

    params = get_xgb_params(len(df_final))
    params["scale_pos_weight"] = spw

    final = xgb.XGBClassifier(**params)
    final.fit(X_final, y_final)
    final.save_model(MODEL_OUT)
    with open(FEATURES_OUT, "wb") as f:
        pickle.dump(feat_cols, f)
    print(f"  Model saved → {MODEL_OUT}")

    calib = None
    if len(df_final) >= 100:
        print("  Calibrating probabilities (isotonic)...")
        try:
            base = xgb.XGBClassifier(**params)
            calib = CalibratedClassifierCV(base, method="isotonic", cv=3)
            calib.fit(X_final, y_final)
            with open(CALIB_OUT, "wb") as f:
                pickle.dump(calib, f)
            print(f"  Calibrated model saved → {CALIB_OUT}")
        except Exception as e:
            print(f"  ⚠️  Calibration failed: {e}")

    fi = pd.Series(final.feature_importances_, index=feat_cols).sort_values(ascending=False)
    print("\n  Top 12 features:")
    for name, imp in fi.head(12).items():
        print(f"    {name:<30} {imp:.4f}")

    try:
        import shap
        explainer = shap.TreeExplainer(final)
        sv = explainer.shap_values(X_final)
        shap_imp = pd.Series(np.abs(sv).mean(axis=0), index=feat_cols).sort_values(ascending=False)
        print("\n  Top 12 SHAP values (mean |impact|):")
        for name, val in shap_imp.head(12).items():
            print(f"    {name:<30} {val:.4f}")
    except ImportError:
        pass

    print("\n[5/6] Generating plots...")
    make_plots(df_final, final, feat_cols, fold_df)

    print("\n[6/6] Summary...")
    if len(fold_df):
        last = fold_df.iloc[-1]
        gain = (last["wr_filt"] - last["wr_all"]) * 100
        print(f"""
{'='*55}
  FILTER RESULT (last validation fold, n={int(last['n_test'])} trades)
{'='*55}
  Trades kept after filter : {last['kept_pct']:.0%}
  Win rate — all trades    : {last['wr_all']:.2%}
  Win rate — filtered      : {last['wr_filt']:.2%}
  Edge improvement         : +{gain:.1f} pp
  AUC                      : {last['auc']:.3f}

  HOW TO USE IN YOUR BOT
  ───────────────────────────────────────
  from trade_ml_pipeline import TradeFilter
  filt = TradeFilter.load()

  # Before placing:
  take, prob = filt.should_take(trade_dict)
  if take:
      place_order()

  # After market resolves:
  filt.record_outcome(won=True, cycle_ts=trade_dict["datetime"])

  # Circuit breaker (losing CYCLES, not trades):
  if filt.cycle_loss_streak() >= 3:
      pause_trading(minutes=20)
""")


if __name__ == "__main__":
    main()