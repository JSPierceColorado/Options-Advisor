# ==========================================================
# OptionExecutor v2.2 — S&P 500 Screener (Alpaca) → Options (Tradier)
# - Universe: S&P 500 only (scraped from Wikipedia with UA; small hardcoded fallback)
# - Screener: EMA20 > EMA50 and RSI(14) > 50 and rising (daily bars via Alpaca)
# - Options: Tradier chains (greeks flattened). Correct order fields:
#       symbol=<UNDERLYING>, option_symbol=<OCC>
# - Throttling: Alpaca delay & Tradier ~1 req/sec
# - Startup: sanity checks (market clock, profile, balances, env echo)
# - Logging: set DEBUG=true for verbose progress
# ==========================================================

import os, json, sqlite3, time, requests, pandas as pd
from datetime import datetime, timedelta, UTC
from dataclasses import dataclass
from typing import Dict, List, Optional

# -------------------- Settings --------------------
def _b(name, d):
    v = os.getenv(name)
    return d if v is None else v.lower() in ("1","true","yes","y","on")

def _f(name, d):
    v = os.getenv(name)
    try: return float(v) if v else d
    except Exception: return d

def _i(name, d):
    v = os.getenv(name)
    try: return int(v) if v else d
    except Exception: return d

class S:
    # --- Alpaca (market data only) ---
    ALPACA_KEY       = os.getenv("APCA_API_KEY_ID", "")
    ALPACA_SECRET    = os.getenv("APCA_API_SECRET_KEY", "")
    ALPACA_DATA_BASE = os.getenv("ALPACA_DATA_BASE", "https://data.alpaca.markets")  # stocks bars
    ALPACA_API_DELAY = _f("ALPACA_API_DELAY", 0.20)  # per request; tweak if you need more/less speed
    ALPACA_FEED      = os.getenv("ALPACA_FEED", "iex")  # 'iex' (free) or 'sip' (paid)

    # --- Tradier (orders + options data) ---
    TRADIER_TOKEN     = os.getenv("TRADIER_TOKEN", "")
    TRADIER_BASE      = os.getenv("TRADIER_BASE", "https://sandbox.tradier.com")  # live: https://api.tradier.com
    TRADIER_ACCOUNT   = os.getenv("TRADIER_ACCOUNT", "")
    TRADIER_API_DELAY = _f("TRADIER_API_DELAY", 1.00)  # 1 req/sec recommended
    DRY_RUN           = _b("DRY_RUN", False)
    DURATION          = os.getenv("ORDER_DURATION", "day")  # day / gtc

    # --- Strategy filters (options) ---
    DTE_MIN, DTE_MAX       = _i("DTE_MIN", 30), _i("DTE_MAX", 60)
    DELTA_MIN, DELTA_MAX   = _f("DELTA_MIN", 0.60), _f("DELTA_MAX", 0.80)
    OI_MIN, VOL_MIN        = _i("OI_MIN", 200), _i("VOL_MIN", 50)
    MAX_SPREAD             = _f("MAX_SPREAD_PCT", 0.15)

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
        "User-Agent": "OptionExecutor/2.2 (+S&P500 screener)"
    }

def _alpaca_get(path, params=None):
    url = S.ALPACA_DATA_BASE.rstrip("/") + path
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
    return {
        "Authorization": f"Bearer {S.TRADIER_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "OptionExecutor/2.2"
    }

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

# -------------------- S&P 500 universe --------------------
_FALLBACK_SP500 = [
    # Tiny fallback subset in case scraping fails hard (still functional)
    "AAPL","MSFT","AMZN","GOOGL","META","NVDA","BRK.B","JPM","JNJ","XOM",
    "V","PG","MA","AVGO","HD","KO","PEP","PFE","ABBV","BAC"
]

def sp500_symbols() -> List[str]:
    """
    Scrape the S&P 500 ticker list (Wikipedia).
    Uses a desktop User-Agent to reduce 403s.
    Falls back to a small hardcoded set if it cannot fetch.
    """
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    }
    try:
        html = requests.get(url, headers=headers, timeout=30).text
        tables = pd.read_html(html)
        # The first table usually contains the constituents
        df = tables[0]
        # Ticker column can be named differently; try common variants
        for col in ["Symbol", "Ticker symbol", "Ticker"]:
            if col in df.columns:
                syms = [str(x).strip().upper().replace(" ", "") for x in df[col].tolist()]
                # Wikipedia sometimes has "." vs "·" or notes; clean a bit
                syms = [s.replace("\u200a", "").replace("\u00b7",".") for s in syms]
                # Dedup while preserving order
                seen=set(); out=[]
                for s in syms:
                    if s and s not in seen:
                        out.append(s); seen.add(s)
                if DEBUG:
                    print(f"[INFO] S&P 500 scraped: {len(out)} symbols")
                return out
        print("[WARN] S&P 500: expected ticker column not found; using fallback")
    except Exception as e:
        print(f"[WARN] Could not scrape S&P 500 list: {e}; using fallback")
    return _FALLBACK_SP500[:]

# -------------------- Alpaca (per-symbol daily bars) --------------------
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
        "feed": S.ALPACA_FEED,
    }
    path = f"/v2/stocks/{symbol}/bars"
    d = _alpaca_get(path, params=params)
    bars = d.get("bars")
    if not bars: return pd.DataFrame()
    df = pd.DataFrame(bars)
    if df.empty: return pd.DataFrame()
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
    """
    Get chain with greeks and FLATTEN the greeks dict into top-level columns:
    - delta, gamma, theta, vega, iv
    """
    d = _tradier_get("/v1/markets/options/chains", {"symbol": sym, "expiration": exp, "greeks": "true"})
    o = (d.get("options") or {}).get("option")
    if not o: 
        return pd.DataFrame()
    df = pd.DataFrame(o)

    # numeric normals
    for c in ["strike","bid","ask","volume","open_interest","last","change"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Some payloads use 'type' instead of 'option_type'
    if "option_type" not in df.columns and "type" in df.columns:
        df["option_type"] = df["type"]

    # FLATTEN GREEKS
    if "greeks" in df.columns:
        g = df["greeks"].apply(lambda x: x if isinstance(x, dict) else {})
        df["delta"] = pd.to_numeric(g.apply(lambda x: x.get("delta")), errors="coerce")
        df["gamma"] = pd.to_numeric(g.apply(lambda x: x.get("gamma")), errors="coerce")
        df["theta"] = pd.to_numeric(g.apply(lambda x: x.get("theta")), errors="coerce")
        df["vega"]  = pd.to_numeric(g.apply(lambda x: x.get("vega")), errors="coerce")
        df["iv"]    = pd.to_numeric(g.apply(lambda x: x.get("mid_iv") if "mid_iv" in x else x.get("bid_iv") if "bid_iv" in x else x.get("ask_iv")), errors="coerce")

    return df

def tradier_quote(sym_occ: str) -> Dict:
    d = _tradier_get("/v1/markets/quotes", {"symbols": sym_occ})
    return (d.get("quotes") or {}).get("quote") or {}

def tradier_order(underlying: str, occ_symbol: str, side: str, qty: int, otype="limit", price=None, preview=False):
    """
    Places a single-leg OPTION order on Tradier.
    - underlying: e.g., "AAPL"
    - occ_symbol: e.g., "AAPL241220C00195000"
    """
    if S.DRY_RUN:
        print("[DRY_RUN] order", {"underlying": underlying, "occ": occ_symbol, "side": side, "qty": qty, "type": otype, "price": price})
        return {"status": "dry_run"}

    if not S.TRADIER_ACCOUNT:
        print("[ERROR] TRADIER_ACCOUNT is missing.")
        return {"error": "no account"}

    data = {
        "class": "option",
        "symbol": underlying,          # <-- REQUIRED: underlying here
        "option_symbol": occ_symbol,   # <-- REQUIRED: OCC symbol here
        "side": side,                  # buy_to_open / sell_to_close
        "quantity": str(qty),
        "type": otype,                 # market / limit
        "duration": S.DURATION,        # day / gtc
    }
    if otype == "limit" and price is not None:
        data["price"] = f"{price:.2f}"
    if preview:
        data["preview"] = "true"

    resp = _tradier_post(f"/v1/accounts/{S.TRADIER_ACCOUNT}/orders", data)

    # Surface any errors right in the logs
    if "errors" in resp:
        print("[TRADIER][ORDER][ERROR]", json.dumps(resp, indent=2))
    else:
        print("[TRADIER][ORDER][OK]", json.dumps(resp, indent=2))
    return resp

# -------------------- Connectivity / sanity checks --------------------
def sanity_check_tradier():
    print(f"[ENV] TRADIER_BASE={S.TRADIER_BASE} token_set={bool(S.TRADIER_TOKEN)} account={S.TRADIER_ACCOUNT}")
    try:
        clock = _tradier_get("/v1/markets/clock")
        print("[CHECK] market clock:", clock)
    except Exception as e:
        print("[CHECK] clock error:", e)
    try:
        profile = _tradier_get("/v1/user/profile")
        print("[CHECK] user profile:", profile)
    except Exception as e:
        print("[CHECK] profile error:", e)
    try:
        if S.TRADIER_ACCOUNT:
            bals = _tradier_get(f"/v1/accounts/{S.TRADIER_ACCOUNT}/balances")
            print("[CHECK] balances:", bals)
            ords = _tradier_get(f"/v1/accounts/{S.TRADIER_ACCOUNT}/orders")
            print("[CHECK] existing orders:", ords)
    except Exception as e:
        print("[CHECK] account error:", e)

# -------------------- Options selection --------------------
@dataclass
class Pick:
    ticker: str; contract: str; expiry: str; strike: float; bid: float; ask: float; mid: float; delta: float; dte: int

def pick_call_from_chain(t: str, exp: str, today: datetime) -> Optional[Pick]:
    df = tradier_chain(t, exp)
    if df.empty:
        if DEBUG: print(f"[INFO] {t} {exp}: empty chain")
        return None

    if "option_type" not in df.columns and "type" in df.columns:
        df["option_type"] = df["type"]

    calls = df[df.get("option_type") == "call"].copy()
    if calls.empty: return None

    # DTE filter
    try: exp_dt = datetime.fromisoformat(exp)
    except Exception: exp_dt = datetime.strptime(exp, "%Y-%m-%d")
    dte = max(0, (exp_dt.date() - today.date()).days)
    if not (S.DTE_MIN <= dte <= S.DTE_MAX):
        return None

    # Ensure numeric + mid + spread
    for c in ["bid","ask","volume","open_interest","delta","strike"]:
        if c in calls.columns:
            calls[c] = pd.to_numeric(calls[c], errors="coerce")
    calls = calls[(calls["bid"] > 0) & (calls["ask"] > 0) & (calls["ask"] >= calls["bid"])]
    if calls.empty:
        return None
    calls["mid"] = (calls["bid"] + calls["ask"]) / 2.0
    calls["spr"] = (calls["ask"] - calls["bid"]) / calls["mid"].clip(1e-9)

    # Liquidity + spread + DELTA band
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
    return Pick(t, str(best.get("symbol")), exp, float(best["strike"]), float(best["bid"]),
                float(best["ask"]), float(best["mid"]), float(best["delta"]), dte)

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
            try: exp_dt = datetime.fromisoformat(p["expiry"])
            except Exception: exp_dt = datetime.strptime(p["expiry"], "%Y-%m-%d")
            if (exp_dt.date() - datetime.now(UTC).date()).days <= S.DTE_STOP:
                reason = "time"

        if reason:
            print(f"SELL {t} {p['contract']} reason={reason}")
            tradier_order(t, p["contract"], "sell_to_close", S.QTY, "market")
            db_del(p["contract"])
            cnt["sells"] += 1
        else:
            db_upd(p["contract"], hi, trail, peak)

# -------------------- Candidate processing --------------------
def process_candidate_tradier(t: str, cnt: Dict[str,int]):
    # Underlying hist via Tradier (for ATR trail at entry)
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
    tradier_order(t, best.contract, "buy_to_open", S.QTY, "limit", limit)
    db_add((t, best.contract, best.expiry, best.strike, best.delta,
            spot, best.mid, spot, chandelier(spot, atr_val), best.mid, datetime.now(UTC).isoformat()))
    cnt["buys"] += 1

# -------------------- Screening (S&P 500 only) --------------------
def screen_symbol(sym: str) -> bool:
    df = alpaca_history_daily_symbol(sym, 420)
    if df.empty:
        if DEBUG: print(f"[SKIP] {sym}: no bars")
        return False
    passed = trend_ok(df)
    if DEBUG:
        if passed:
            r = rsi(df["Close"], S.RSI_LEN).iloc[-1]
            ef = ema(df["Close"], S.EMA_FAST).iloc[-1]
            es = ema(df["Close"], S.EMA_SLOW).iloc[-1]
            print(f"[PASS] {sym}: EMA{S.EMA_FAST}={ef:.2f}>EMA{S.EMA_SLOW}={es:.2f}, RSI={r:.1f}")
        else:
            print(f"[FAIL] {sym}: trend not confirmed")
    return passed

# -------------------- Runner --------------------
def run_once():
    # Validate env
    miss=[]
    if not S.ALPACA_KEY or not S.ALPACA_SECRET: miss.append("APCA_API_KEY_ID/APCA_API_SECRET_KEY")
    if not S.TRADIER_TOKEN: miss.append("TRADIER_TOKEN")
    if not S.TRADIER_ACCOUNT and not S.DRY_RUN: miss.append("TRADIER_ACCOUNT")
    if miss:
        print("[ERROR] missing env:", ", ".join(miss)); return

    print(f"[ENV] Alpaca feed={S.ALPACA_FEED} base={S.ALPACA_DATA_BASE}")
    db_init()
    sanity_check_tradier()

    # 1) Universe: S&P 500 only
    syms = sp500_symbols()
    print(f"[INFO] Universe: S&P 500 symbols={len(syms)}")

    # 2) Screen each symbol (EMA/RSI) — much smaller & faster than full US
    candidates: List[str] = []
    for i, sym in enumerate(syms, 1):
        try:
            if DEBUG and i % 50 == 1:
                print(f"[PROGRESS] Screening {i}/{len(syms)}…")
            if screen_symbol(sym):
                candidates.append(sym)
        except Exception as e:
            print(f"[ERR][screen] {sym}: {e}")

    print(f"[INFO] Candidates after screen: {len(candidates)}")

    # 3) Options via Tradier ONLY for candidates (throttled)
    cnt={"viable":0,"buys":0,"sells":0}
    for t in candidates:
        try:
            process_candidate_tradier(t, cnt)
        except Exception as e:
            print(f"[ERR] {t}: {e}")

    # 4) Manage existing picks
    open_ticks = sorted(set([p["ticker"] for p in db_all()]))
    for t in open_ticks:
        try:
            manage_open_for_ticker(t, cnt)
        except Exception as e:
            print(f"[ERR][manage] {t}: {e}")

    print(f"[SUMMARY] candidates={len(candidates)} viable={cnt['viable']} buys={cnt['buys']} sells={cnt['sells']}")

if __name__ == "__main__":
    run_once()