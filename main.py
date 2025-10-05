# ==========================================================
# OptionExecutor v1.9 — All Alpaca Symbols (simple) + Tradier Options
# - No batching; per-symbol Alpaca bars (slower but very reliable)
# - Full universe from Alpaca /v2/assets (active us_equity)
# - Options & orders via Tradier (1 req/sec throttle)
# ==========================================================

import os, json, sqlite3, time, requests, pandas as pd
from datetime import datetime, timedelta, UTC
from dataclasses import dataclass
from typing import Dict, List, Optional

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
    ALPACA_KEY       = os.getenv("APCA_API_KEY_ID", "")
    ALPACA_SECRET    = os.getenv("APCA_API_SECRET_KEY", "")
    # Use correct hosts for each API family:
    ALPACA_DATA_BASE = os.getenv("ALPACA_DATA_BASE", "https://data.alpaca.markets")  # stocks bars live here
    ALPACA_TRADE_API = os.getenv("ALPACA_TRADE_API", "https://api.alpaca.markets")   # /v2/assets lives here
    ALPACA_API_DELAY = _f("ALPACA_API_DELAY", 0.25)  # per request delay; 0.25s = ~240/hr
    ALPACA_FEED      = os.getenv("ALPACA_FEED", "iex")  # 'iex' (free) or 'sip' (paid)

    # --- Tradier (orders + options data) ---
    TRADIER_TOKEN     = os.getenv("TRADIER_TOKEN", "")
    TRADIER_BASE      = os.getenv("TRADIER_BASE", "https://sandbox.tradier.com")
    TRADIER_ACCOUNT   = os.getenv("TRADIER_ACCOUNT", "")
    TRADIER_API_DELAY = _f("TRADIER_API_DELAY", 1.00)  # 1 req/sec
    DRY_RUN   = _b("DRY_RUN", False)
    DURATION  = os.getenv("ORDER_DURATION", "day")

    # --- Option filters ---
    DTE_MIN, DTE_MAX           = _i("DTE_MIN", 30), _i("DTE_MAX", 60)
    DELTA_MIN, DELTA_MAX       = _f("DELTA_MIN", 0.60), _f("DELTA_MAX", 0.80)
    OI_MIN, VOL_MIN            = _i("OI_MIN", 200), _i("VOL_MIN", 50)
    MAX_SPREAD                 = _f("MAX_SPREAD_PCT", 0.15)

    # --- Indicators ---
    EMA_FAST, EMA_SLOW, RSI_LEN = _i("EMA_FAST", 20), _i("EMA_SLOW", 50), _i("RSI_LEN", 14)
    ATR_LEN, TRAIL_K, OPT_DD, DTE_STOP = _i("ATR_LEN", 14), _f("TRAIL_K_ATR", 3.0), _f("OPT_DD_EXIT", 0.25), _i("DTE_STOP", 7)

    # --- Orders / persistence ---
    QTY = _i("ORDER_QTY", 1)
    DB  = os.getenv("DB_PATH", "./executor_tradier.db")

    # --- Logging ---
    VERBOSE = _b("VERBOSE", True)

DEBUG = os.getenv("DEBUG", "false").lower() in ("1", "true", "yes")

# -------------------- HTTP helpers --------------------
def _alpaca_headers():
    return {
        "APCA-API-KEY-ID": S.ALPACA_KEY,
        "APCA-API-SECRET-KEY": S.ALPACA_SECRET,
        "Accept": "application/json",
    }

def _alpaca_get(base, path, params=None):
    url = base.rstrip("/") + path
    try:
        r = requests.get(url, headers=_alpaca_headers(), params=params or {}, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[WARN] Alpaca GET {url}: {e}")
        data = {}
    time.sleep(S.ALPACA_API_DELAY)
    return data

def _tradier_headers():
    return {"Authorization": f"Bearer {S.TRADIER_TOKEN}", "Accept": "application/json"}

def _tradier_get(path, params=None):
    url = S.TRADIER_BASE.rstrip("/") + path
    try:
        r = requests.get(url, headers=_tradier_headers(), params=params or {}, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[WARN] Tradier GET {url}: {e}")
        data = {}
    time.sleep(S.TRADIER_API_DELAY)
    return data

def _tradier_post(path, data):
    url = S.TRADIER_BASE.rstrip("/") + path
    try:
        r = requests.post(url, headers={**_tradier_headers(), "Content-Type": "application/x-www-form-urlencoded"}, data=data, timeout=30)
        data = r.json()
    except Exception as e:
        print(f"[WARN] Tradier POST {url}: {e}")
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
    if df.empty or len(df) < max(S.EMA_SLOW+5, S.RSI_LEN+5):
        return False
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

# -------------------- Alpaca Universe + Per-symbol Bars --------------------
def alpaca_universe_all() -> List[str]:
    """
    All active US equities from Alpaca /v2/assets. Requires API keys.
    """
    if not (S.ALPACA_KEY and S.ALPACA_SECRET):
        print("[ERROR] Alpaca keys missing (APCA_API_KEY_ID/APCA_API_SECRET_KEY).")
        return []
    params = {"status": "active", "asset_class": "us_equity"}
    data = _alpaca_get(S.ALPACA_TRADE_API, "/v2/assets", params=params)
    if not isinstance(data, list):
        return []
    syms = []
    for a in data:
        sym = a.get("symbol")
        tradable = a.get("tradable", True)
        if sym and tradable:
            syms.append(sym.upper())
    # dedup preserve order
    seen=set(); out=[]
    for s in syms:
        if s not in seen:
            out.append(s); seen.add(s)
    return out

def alpaca_history_daily_symbol(symbol: str, days: int = 420) -> pd.DataFrame:
    """
    Fetch 1Day bars for a single symbol via data.alpaca.markets.
    """
    start = (datetime.now(UTC) - timedelta(days=days+10)).isoformat()
    params = {
        "timeframe": "1Day",
        "start": start,
        "limit": 1500,
        "adjustment": "raw",
        "feed": S.ALPACA_FEED,  # 'iex' by default
    }
    # PER-SYMBOL bars endpoint:
    path = f"/v2/stocks/{symbol}/bars"
    d = _alpaca_get(S.ALPACA_DATA_BASE, path, params=params)
    bars = d.get("bars")
    if not bars:
        return pd.DataFrame()
    df = pd.DataFrame(bars)
    if df.empty:
        return pd.DataFrame()
    # expected columns: t, o, h, l, c, v, etc.
    df["date"] = pd.to_datetime(df["t"])
    df = df.rename(columns={"h":"High","l":"Low","c":"Close","v":"Volume"})
    for c in ("High","Low","Close","Volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["date","High","Low","Close","Volume"]].set_index("date").sort_index()

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
        if DEBUG: print(f"[INFO] {t} {exp}: empty chain or no delta")
        return None
    calls = df[df.get("option_type") == "call"].copy()
    if calls.empty: return None
    try:
        exp_dt = datetime.fromisoformat(exp)
    except Exception:
        exp_dt = datetime.strptime(exp, "%Y-%m-%d")
    dte = max(0, (exp_dt.date() - today.date()).days)
    if not (S.DTE_MIN <= dte <= S.DTE_MAX): return None
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
    if calls.empty: return None
    target = 0.5*(S.DELTA_MIN + S.DELTA_MAX)
    calls["drank"] = (calls["delta"] - target).abs()
    best = calls.sort_values(by=["drank","spr","mid"], ascending=[True,True,False]).iloc[0]
    return Pick(t, str(best["symbol"]), exp, float(best["strike"]), float(best["bid"]), float(best["ask"]),
                float(best["mid"]), float(best["delta"]), dte)

# -------------------- Exits / management --------------------
def manage_open_for_ticker(t: str, cnt: Dict[str,int]):
    hist = tradier_history_daily(t, 420)
    if hist.empty: return
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

# -------------------- Candidate processing --------------------
def process_candidate_tradier(t: str, cnt: Dict[str,int]):
    # underlying hist via Tradier (for ATR trail at entry)
    hist = tradier_history_daily(t, 420)
    if hist.empty:
        if DEBUG: print(f"[SKIP] {t}: no Tradier history")
        return
    spot = float(hist["Close"].iloc[-1])
    atr_val = float(atr(hist, S.ATR_LEN).iloc[-1])

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

    limit = round(min(best.ask, best.mid * 1.02), 2)
    print(f"BUY {t} {best.contract} Δ={best.delta:.2f} @{limit}")
    tradier_order(best.contract, "buy_to_open", S.QTY, "limit", limit)
    db_add((t, best.contract, best.expiry, best.strike, best.delta,
            spot, best.mid, spot, chandelier(spot, atr_val), best.mid, datetime.now(UTC).isoformat()))
    cnt["buys"] += 1

# -------------------- Runner --------------------
def run_once():
    miss=[]
    if not S.ALPACA_KEY or not S.ALPACA_SECRET: miss.append("APCA_API_KEY_ID/APCA_API_SECRET_KEY")
    if not S.TRADIER_TOKEN: miss.append("TRADIER_TOKEN")
    if not S.TRADIER_ACCOUNT and not S.DRY_RUN: miss.append("TRADIER_ACCOUNT")
    if miss:
        print("[ERROR] missing env:", ", ".join(miss)); return

    db_init()

    # 1) Universe (all active us_equity from Alpaca)
    syms = alpaca_universe_all()
    print(f"[INFO] Alpaca universe loaded: {len(syms)} symbols")

    # 2) Screen each symbol via Alpaca (EMA/RSI). No batching—slow but robust.
    candidates: List[str] = []
    for i, sym in enumerate(syms, 1):
        try:
            if DEBUG and i % 50 == 1:
                print(f"[PROGRESS] Screening {i}/{len(syms)}…")

            df = alpaca_history_daily_symbol(sym, 420)
            if df.empty:
                if DEBUG: print(f"[SKIP] {sym}: no bars")
                continue
            if trend_ok(df):
                if DEBUG:
                    r = rsi(df["Close"], S.RSI_LEN).iloc[-1]
                    ef = ema(df["Close"], S.EMA_FAST).iloc[-1]
                    es = ema(df["Close"], S.EMA_SLOW).iloc[-1]
                    print(f"[PASS] {sym}: EMA20={ef:.2f}>EMA50={es:.2f}, RSI={r:.1f}")
                candidates.append(sym)
            else:
                if DEBUG: print(f"[FAIL] {sym}: trend not confirmed")
        except Exception as e:
            print(f"[ERR][screen] {sym}: {e}")

    print(f"[INFO] Candidates after screen: {len(candidates)}")

    # 3) Options work via Tradier ONLY for candidates (throttled)
    cnt={"viable":0,"buys":0,"sells":0}
    for t in candidates:
        try:
            process_candidate_tradier(t, cnt)
        except Exception as e:
            print(f"[ERR] {t}: {e}")

    # 4) Manage existing picks regardless of today's screen
    open_ticks = sorted(set([p["ticker"] for p in db_all()]))
    for t in open_ticks:
        try:
            manage_open_for_ticker(t, cnt)
        except Exception as e:
            print(f"[ERR][manage] {t}: {e}")

    print(f"[SUMMARY] candidates={len(candidates)} viable={cnt['viable']} buys={cnt['buys']} sells={cnt['sells']}")

if __name__ == "__main__":
    run_once()
