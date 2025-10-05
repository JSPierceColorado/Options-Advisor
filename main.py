# ==========================================================
# OptionExecutor v1.8 — Alpaca-fast Screener (All Symbols) + Tradier Options
# ==========================================================
# Flow:
# 1) Universe from Alpaca assets (ALL active us_equity symbols)
# 2) Screener via Alpaca daily bars (EMA20>EMA50 & RSI>50 rising) in batches
# 3) For PASSED tickers only:
#    - Tradier (throttled to ~1 req/sec) → expirations + option chains (greeks)
#    - Pick best call (30–60 DTE, Δ 0.6–0.8, OI/VOL min, spread<=15%)
#    - Place limit buy; manage exits via ATR trail / 25% drawdown / ≤7 DTE
#
# Notes:
# - Orders are sent to TRADIER when DRY_RUN=false.
# - DEBUG=true prints per-ticker analysis & reasons for skips.
# - Alpaca is used ONLY for data (universe + bars).
# ==========================================================

import os, json, sqlite3, time, requests, pandas as pd
from datetime import datetime, timedelta, UTC
from dataclasses import dataclass
from typing import Dict, List, Optional, Iterable, Tuple
from collections import defaultdict

# -------------------- Settings --------------------
def _b(name, d):
    v = os.getenv(name)
    return d if v is None else v.lower() in ("1", "true", "yes", "y", "on")

def _f(name, d):
    v = os.getenv(name)
    try:
        return float(v) if v else d
    except Exception:
        return d

def _i(name, d):
    v = os.getenv(name)
    try:
        return int(v) if v else d
    except Exception:
        return d

class S:
    # --- Alpaca (market data only) ---
    ALPACA_KEY   = os.getenv("APCA_API_KEY_ID", "")
    ALPACA_SECRET= os.getenv("APCA_API_SECRET_KEY", "")
    ALPACA_BASE  = os.getenv("ALPACA_BASE", "https://data.alpaca.markets")  # Market Data v2
    ALPACA_TRADE_API = os.getenv("ALPACA_TRADE_API", "https://api.alpaca.markets")  # for /v2/assets
    ALPACA_API_DELAY = _f("ALPACA_API_DELAY", 0.10)  # ~10 req/sec
    ALPACA_BAR_BATCH = _i("ALPACA_BAR_BATCH", 200)   # bars endpoint supports multi-symbol; tune batch size

    # --- Tradier (orders + options data) ---
    TRADIER_TOKEN   = os.getenv("TRADIER_TOKEN", "")
    TRADIER_BASE    = os.getenv("TRADIER_BASE", "https://sandbox.tradier.com")  # live: https://api.tradier.com
    TRADIER_ACCOUNT = os.getenv("TRADIER_ACCOUNT", "")
    TRADIER_API_DELAY = _f("TRADIER_API_DELAY", 1.00)  # 1 req/sec (safe)
    DRY_RUN   = _b("DRY_RUN", False)
    DURATION  = os.getenv("ORDER_DURATION", "day")

    # --- Option filters ---
    DTE_MIN, DTE_MAX = _i("DTE_MIN", 30), _i("DTE_MAX", 60)
    DELTA_MIN, DELTA_MAX = _f("DELTA_MIN", 0.6), _f("DELTA_MAX", 0.8)
    OI_MIN, VOL_MIN = _i("OI_MIN", 200), _i("VOL_MIN", 50)
    MAX_SPREAD = _f("MAX_SPREAD_PCT", 0.15)

    # --- Indicators ---
    EMA_FAST, EMA_SLOW, RSI_LEN = _i("EMA_FAST", 20), _i("EMA_SLOW", 50), _i("RSI_LEN", 14)
    ATR_LEN, TRAIL_K, OPT_DD, DTE_STOP = _i("ATR_LEN", 14), _f("TRAIL_K_ATR", 3), _f("OPT_DD_EXIT", 0.25), _i("DTE_STOP", 7)

    # --- Orders / persistence ---
    QTY = _i("ORDER_QTY", 1)
    DB  = os.getenv("DB_PATH", "./executor_tradier.db")

    # --- Logging ---
    VERBOSE = _b("VERBOSE", True)

DEBUG = os.getenv("DEBUG", "false").lower() in ("1","true","yes")

# -------------------- HTTP helpers --------------------
def _alpaca_headers():
    return {
        "APCA-API-KEY-ID": S.ALPACA_KEY,
        "APCA-API-SECRET-KEY": S.ALPACA_SECRET,
        "Accept": "application/json",
    }

def _alpaca_get(path, params=None, use_trade_api=False):
    base = S.ALPACA_TRADE_API if use_trade_api else S.ALPACA_BASE
    url = base.rstrip("/") + path
    try:
        r = requests.get(url, headers=_alpaca_headers(), params=params or {}, timeout=25)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[WARN] Alpaca GET {path}: {e}")
        data = {}
    time.sleep(S.ALPACA_API_DELAY)
    return data

def _tradier_headers():
    return {"Authorization": f"Bearer {S.TRADIER_TOKEN}", "Accept": "application/json"}

def _tradier_get(path, params=None):
    url = S.TRADIER_BASE.rstrip("/") + path
    try:
        r = requests.get(url, headers=_tradier_headers(), params=params or {}, timeout=25)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[WARN] Tradier GET {path}: {e}")
        data = {}
    time.sleep(S.TRADIER_API_DELAY)
    return data

def _tradier_post(path, data):
    url = S.TRADIER_BASE.rstrip("/") + path
    try:
        r = requests.post(url, headers={**_tradier_headers(), "Content-Type": "application/x-www-form-urlencoded"}, data=data, timeout=25)
        data = r.json()
    except Exception as e:
        print(f"[WARN] Tradier POST {path}: {e}")
        data = {}
    time.sleep(S.TRADIER_API_DELAY)
    return data

# -------------------- Indicators --------------------
def ema(s, n): return s.ewm(span=n, adjust=False).mean()
def rsi(s, n=14):
    d = s.diff()
    up, dn = d.clip(lower=0), -d.clip(upper=0)
    rs = up.ewm(alpha=1/n, adjust=False).mean() / (dn.ewm(alpha=1/n, adjust=False).mean() + 1e-9)
    return 100 - 100/(1+rs)
def atr(df, n=14):
    h, l, c = df["High"], df["Low"], df["Close"]
    p = c.shift(1)
    tr = pd.concat([(h - l).abs(), (h - p).abs(), (l - p).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()
def chandelier(high, atrv): return high - S.TRAIL_K * atrv
def trend_ok(df):
    if df.empty or len(df) < max(S.EMA_SLOW+5, S.RSI_LEN+5): return False
    c = df["Close"]
    ef, es = ema(c, S.EMA_FAST), ema(c, S.EMA_SLOW)
    r = rsi(c, S.RSI_LEN)
    return (ef.iloc[-1] > es.iloc[-1]) and (r.iloc[-1] > 50) and (r.iloc[-1] >= r.iloc[-2])

# -------------------- DB --------------------
def db_init():
    con = sqlite3.connect(S.DB)
    con.execute("""CREATE TABLE IF NOT EXISTS picks (
        ticker TEXT, contract TEXT PRIMARY KEY, expiry TEXT, strike REAL,
        delta REAL, entry_underlying REAL, entry_option REAL,
        highest_close REAL, trail REAL, peak_option REAL, created_at TEXT)""")
    con.commit(); con.close()

def db_all():
    con = sqlite3.connect(S.DB); con.row_factory = sqlite3.Row
    rows = [dict(r) for r in con.execute("SELECT * FROM picks")]
    con.close(); return rows

def db_add(row):
    con = sqlite3.connect(S.DB)
    con.execute("INSERT OR IGNORE INTO picks VALUES (?,?,?,?,?,?,?,?,?,?,?)", row)
    con.commit(); con.close()

def db_upd(c, h, t, p):
    con = sqlite3.connect(S.DB)
    con.execute("UPDATE picks SET highest_close=?, trail=?, peak_option=? WHERE contract=?", (h, t, p, c))
    con.commit(); con.close()

def db_del(c):
    con = sqlite3.connect(S.DB)
    con.execute("DELETE FROM picks WHERE contract=?", (c,))
    con.commit(); con.close()

# -------------------- Alpaca Universe + Bars --------------------
def alpaca_universe_all() -> List[str]:
    """
    All active US equities from Alpaca /v2/assets.
    Requires same API keys; returns several thousand symbols.
    """
    if not (S.ALPACA_KEY and S.ALPACA_SECRET):
        print("[WARN] Alpaca keys missing; universe will be empty.")
        return []
    params = {"status": "active", "asset_class": "us_equity"}
    data = _alpaca_get("/v2/assets", params=params, use_trade_api=True)
    if not isinstance(data, list):
        return []
    # Filter out weird/OTC if desired; by default include all active us_equity
    syms = []
    for a in data:
        sym = a.get("symbol")
        tradable = a.get("tradable", True)
        if sym and tradable:
            syms.append(sym.upper())
    # Dedup preserve order
    seen=set(); out=[]
    for s in syms:
        if s not in seen:
            out.append(s); seen.add(s)
    return out

def chunks(seq: List[str], n: int) -> Iterable[List[str]]:
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def alpaca_bars_batch(symbols: List[str], days: int = 420) -> Dict[str, pd.DataFrame]:
    """
    Fetch daily bars for many symbols at once using /v2/stocks/bars with 'symbols' param.
    Returns dict: symbol -> DataFrame(High, Low, Close, Volume)
    """
    out: Dict[str, pd.DataFrame] = {}
    if not symbols:
        return out
    start = (datetime.now(UTC) - timedelta(days=days+10)).isoformat()
    params = {
        "timeframe": "1Day",
        "start": start,
        "limit": 1500,
        "adjustment": "raw",
        "symbols": ",".join(symbols),
        "feed": "sip",  # change to "iex" if your plan requires
    }
    d = _alpaca_get("/v2/stocks/bars", params=params)
    bars = d.get("bars")
    if bars is None:
        return out

    # API may return dict(symbol -> [bars]) OR flat list with 'S' field
    if isinstance(bars, dict):
        for sym, blist in bars.items():
            df = pd.DataFrame(blist)
            if df.empty: 
                continue
            df["date"] = pd.to_datetime(df["t"])
            df = df.rename(columns={"h": "High", "l": "Low", "c": "Close", "v": "Volume"})
            for c in ("High","Low","Close","Volume"):
                df[c] = pd.to_numeric(df[c], errors="coerce")
            out[sym.upper()] = df[["date","High","Low","Close","Volume"]].set_index("date").sort_index()
    else:
        # assume list with S/t/o/h/l/c/v
        by_sym: Dict[str, list] = defaultdict(list)
        for b in bars:
            sym = (b.get("S") or "").upper()
            if not sym: 
                continue
            by_sym[sym].append(b)
        for sym, blist in by_sym.items():
            df = pd.DataFrame(blist)
            if df.empty:
                continue
            df["date"] = pd.to_datetime(df["t"])
            df = df.rename(columns={"h": "High", "l": "Low", "c": "Close", "v": "Volume"})
            for c in ("High","Low","Close","Volume"):
                df[c] = pd.to_numeric(df[c], errors="coerce")
            out[sym] = df[["date","High","Low","Close","Volume"]].set_index("date").sort_index()

    return out

def screen_candidates_via_alpaca(all_symbols: List[str]) -> List[str]:
    """
    Batch-load bars from Alpaca and return symbols that pass trend_ok().
    """
    passed: List[str] = []
    total = len(all_symbols)
    if DEBUG:
        print(f"[INFO] Alpaca universe size: {total} symbols")

    for batch in chunks(all_symbols, S.ALPACA_BAR_BATCH):
        bars_map = alpaca_bars_batch(batch, days=420)
        for sym in batch:
            df = bars_map.get(sym.upper())
            if df is None or df.empty:
                if DEBUG: print(f"[SKIP] {sym}: no bars")
                continue
            if trend_ok(df):
                if DEBUG:
                    r = rsi(df["Close"], S.RSI_LEN).iloc[-1]
                    ef = ema(df["Close"], S.EMA_FAST).iloc[-1]
                    es = ema(df["Close"], S.EMA_SLOW).iloc[-1]
                    print(f"[PASS] {sym}: EMA20={ef:.2f} > EMA50={es:.2f}, RSI={r:.1f}")
                passed.append(sym.upper())
            else:
                if DEBUG:
                    print(f"[FAIL] {sym}: trend not confirmed")
    if DEBUG:
        print(f"[INFO] Screener passed: {len(passed)} / {total}")
    return passed

# -------------------- Tradier market data & orders --------------------
def tradier_history_daily(symbol: str, days: int = 420) -> pd.DataFrame:
    start = (datetime.now(UTC) - timedelta(days=days+10)).date().isoformat()
    d = _tradier_get("/v1/markets/history", {"symbol": symbol, "interval": "daily", "start": start})
    h = (d.get("history") or {}).get("day")
    if not h: return pd.DataFrame()
    df = pd.DataFrame(h); df["date"] = pd.to_datetime(df["date"])
    for c in ["open","high","low","close","volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df.rename(columns={"high":"High","low":"Low","close":"Close","volume":"Volume"}).set_index("date").sort_index()

def tradier_expirations(sym: str) -> List[str]:
    d = _tradier_get("/v1/markets/options/expirations", {"symbol": sym, "includeAllRoots": "true", "strikes": "false"})
    e = (d.get("expirations") or {}).get("date")
    return e if isinstance(e, list) else ([e] if e else [])

def tradier_chain(sym: str, exp: str) -> pd.DataFrame:
    d = _tradier_get("/v1/markets/options/chains", {"symbol": sym, "expiration": exp, "greeks": "true"})
    o = (d.get("options") or {}).get("option")
    if not o: return pd.DataFrame()
    df = pd.DataFrame(o)
    for c in ["strike","bid","ask","volume","open_interest","delta"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def tradier_quote(sym_occ: str) -> Dict:
    d = _tradier_get("/v1/markets/quotes", {"symbols": sym_occ})
    return (d.get("quotes") or {}).get("quote") or {}

def tradier_positions() -> List[Dict]:
    if not S.TRADIER_ACCOUNT: return []
    d = _tradier_get(f"/v1/accounts/{S.TRADIER_ACCOUNT}/positions")
    p = (d.get("positions") or {}).get("position")
    return p if isinstance(p, list) else ([p] if p else [])

def tradier_order(sym_occ: str, side: str, qty: int, otype="limit", price=None):
    if S.DRY_RUN:
        return {"status":"dry_run","symbol":sym_occ,"side":side,"qty":qty,"type":otype,"price":price}
    if not S.TRADIER_ACCOUNT:
        return {"error":"no account"}
    data={"class":"option","symbol":sym_occ,"side":side,"quantity":str(qty),"type":otype,"duration":S.DURATION}
    if otype=="limit" and price is not None:
        data["price"]=f"{price:.2f}"
    return _tradier_post(f"/v1/accounts/{S.TRADIER_ACCOUNT}/orders", data)

# -------------------- Options selection --------------------
@dataclass
class Pick:
    ticker: str; contract: str; expiry: str; strike: float; bid: float; ask: float; mid: float; delta: float; dte: int

def pick_call_from_chain(t: str, exp: str, today: datetime) -> Optional[Pick]:
    df = tradier_chain(t, exp)
    if df.empty or "delta" not in df:
        if DEBUG: print(f"[INFO] {t} {exp}: no delta or empty chain")
        return None
    calls = df[df.get("option_type") == "call"].copy()
    if calls.empty:
        return None
    try:
        exp_dt = datetime.fromisoformat(exp)
    except Exception:
        exp_dt = datetime.strptime(exp, "%Y-%m-%d")
    dte = max(0, (exp_dt.date() - today.date()).days)
    if not (S.DTE_MIN <= dte <= S.DTE_MAX):
        return None
    for c in ["bid","ask","volume","open_interest","delta","strike"]:
        if c in calls.columns:
            calls[c] = pd.to_numeric(calls[c], errors="coerce")
    calls["mid"] = (calls["bid"] + calls["ask"]) / 2.0
    calls = calls[(calls["bid"] > 0) & (calls["ask"] > 0) & (calls["ask"] >= calls["bid"])]
    calls["spr"] = (calls["ask"] - calls["bid"]) / calls["mid"].clip(1e-9)
    calls = calls[
        (calls["open_interest"] >= S.OI_MIN) &
        (calls["volume"] >= S.VOL_MIN) &
        (calls["spr"] <= S.MAX_SPREAD) &
        (calls["delta"].between(S.DELTA_MIN, S.DELTA_MAX))
    ]
    if calls.empty:
        return None
    target = 0.5*(S.DELTA_MIN + S.DELTA_MAX)
    calls["drank"] = (calls["delta"] - target).abs()
    best = calls.sort_values(by=["drank","spr","mid"], ascending=[True,True,False]).iloc[0]
    return Pick(t, str(best["symbol"]), exp, float(best["strike"]), float(best["bid"]), float(best["ask"]), float(best["mid"]), float(best["delta"]), dte)

# -------------------- Exits / management --------------------
def manage_open_for_ticker(t: str, cnt: Dict[str,int]):
    # Get underlying history (for ATR trail) via Tradier (throttled)
    hist = tradier_history_daily(t, 420)
    if hist.empty:
        return
    spot = float(hist["Close"].iloc[-1])
    atr_val = float(atr(hist, S.ATR_LEN).iloc[-1])

    picks = [p for p in db_all() if p["ticker"] == t]
    for p in picks:
        hi = max(p["highest_close"], spot)
        trail = chandelier(hi, atr_val)
        q = tradier_quote(p["contract"]) or {}
        bid, ask = float(q.get("bid",0)), float(q.get("ask",0))
        mid = (bid + ask)/2 if bid>0 and ask>0 else p["entry_option"]
        peak = max(p["peak_option"], mid)

        reason = None
        if spot < trail: reason = "trail"
        elif mid <= peak*(1 - S.OPT_DD): reason = "drawdown"
        else:
            try:
                exp_dt = datetime.fromisoformat(p["expiry"])
            except Exception:
                exp_dt = datetime.strptime(p["expiry"], "%Y-%m-%d")
            if (exp_dt.date() - datetime.now(UTC).date()).days <= S.DTE_STOP:
                reason = "time"

        if reason:
            print(f"SELL {t} {p['contract']} reason={reason}")
            tradier_order(p["contract"], "sell_to_close", S.QTY, "market")
            db_del(p["contract"])
            cnt["sells"] += 1
        else:
            db_upd(p["contract"], hi, trail, peak)

# -------------------- Per-candidate flow --------------------
def process_candidate_tradier(t: str, cnt: Dict[str,int]):
    # Get underlying hist (for ATR trail calc on entry)
    hist = tradier_history_daily(t, 420)
    if hist.empty:
        if DEBUG: print(f"[SKIP] {t}: no Tradier history")
        return
    spot = float(hist["Close"].iloc[-1])
    atr_val = float(atr(hist, S.ATR_LEN).iloc[-1])

    # Expirations
    exps = tradier_expirations(t)
    if not exps:
        if DEBUG: print(f"[SKIP] {t}: no expirations")
        return

    today = datetime.now(UTC)
    best = None
    for exp in exps:
        cand = pick_call_from_chain(t, exp, today)
        if cand:
            best = cand
            break
    if not best:
        if DEBUG: print(f"[NO OPTIONS] {t}: no viable calls in window")
        return
    cnt["viable"] += 1

    # Place order (limit near mid/ask)
    limit = round(min(best.ask, best.mid * 1.02), 2)
    print(f"BUY {t} {best.contract} Δ={best.delta:.2f} @{limit}")
    tradier_order(best.contract, "buy_to_open", S.QTY, "limit", limit)
    db_add((t, best.contract, best.expiry, best.strike, best.delta, spot, best.mid,
            spot, chandelier(spot, atr_val), best.mid, datetime.now(UTC).isoformat()))
    cnt["buys"] += 1

# -------------------- Runner --------------------
def run_once():
    # Validate keys
    miss=[]
    if not S.ALPACA_KEY or not S.ALPACA_SECRET: miss.append("APCA_API_KEY_ID/APCA_API_SECRET_KEY")
    if not S.TRADIER_TOKEN: miss.append("TRADIER_TOKEN")
    if not S.TRADIER_ACCOUNT and not S.DRY_RUN: miss.append("TRADIER_ACCOUNT")
    if miss:
        print("[ERROR] missing env:", ", ".join(miss)); return

    db_init()

    # 1) Universe from Alpaca (all active US equities)
    syms = alpaca_universe_all()
    print(f"[INFO] Alpaca universe loaded: {len(syms)} symbols")

    # 2) Screen via Alpaca (EMA/RSI)
    candidates = screen_candidates_via_alpaca(syms)
    print(f"[INFO] Candidates after screen: {len(candidates)}")

    cnt={"viable":0,"buys":0,"sells":0}

    # 3) Options work via Tradier ONLY for candidates (throttled)
    for t in candidates:
        try:
            process_candidate_tradier(t, cnt)
        except Exception as e:
            print(f"[ERR] {t}: {e}")

    # 4) Always manage open picks even if their tickers didn’t pass today’s screen
    open_ticks = sorted(set([p["ticker"] for p in db_all()]))
    for t in open_ticks:
        try:
            manage_open_for_ticker(t, cnt)
        except Exception as e:
            print(f"[ERR][manage] {t}: {e}")

    print(f"[SUMMARY] candidates={len(candidates)} viable={cnt['viable']} buys={cnt['buys']} sells={cnt['sells']}")

if __name__ == "__main__":
    run_once()
