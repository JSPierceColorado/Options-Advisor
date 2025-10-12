# ==========================================================
# OptionsExecutor Unified v3.0 — S&P 500 (CSV) → Alpaca Screener → Tradier Options
# - Unifies CALL (bullish) and PUT (bearish) executors into one process
# - Universe: S&P 500 list via CSV mirrors (no lxml). Small fallback list if mirrors fail.
# - Screeners:
#     * Bullish (calls): EMA20>EMA50, RSI rising 50-70, price ≤ +5% of EMA20
#     * Bearish (puts):  EMA20<EMA50, RSI falling 30-50, price ≥ -5% below EMA20
# - Options (Tradier): greeks flattened; spread / OI / Vol / delta filters
# - Exits: fixed stop-loss & take-profit on option premium, ATR chandelier (bullish or bearish),
#          peak drawdown, and DTE-based time exit.
# - Duplicate protection: avoid duplicates via local DB + Tradier positions
# - Market-hours gating: skip new buys outside RTH by default (holiday-aware via Tradier)
# - Strategy selection via STRATEGY env: 'calls' | 'puts' | 'both' (default: both)
# - DB schema unified for both legs; safe to reuse if table exists (adds cols when missing)
# ==========================================================

import os, json, sqlite3, time, requests, pandas as pd
from datetime import datetime, timedelta, UTC
from dataclasses import dataclass
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo

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
    # --- Mode ---
    STRATEGY          = os.getenv("STRATEGY", "both").lower().strip()  # 'calls'|'puts'|'both'

    # --- Alpaca (market data only) ---
    ALPACA_KEY       = os.getenv("APCA_API_KEY_ID", "")
    ALPACA_SECRET    = os.getenv("APCA_API_SECRET_KEY", "")
    ALPACA_DATA_BASE = os.getenv("ALPACA_DATA_BASE", "https://data.alpaca.markets")
    ALPACA_API_DELAY = _f("ALPACA_API_DELAY", 0.20)
    ALPACA_FEED      = os.getenv("ALPACA_FEED", "iex")  # 'iex' (free) or 'sip'

    # --- Tradier (orders + options data) ---
    TRADIER_TOKEN     = os.getenv("TRADIER_TOKEN", "")
    TRADIER_BASE      = os.getenv("TRADIER_BASE", "https://sandbox.tradier.com")  # live: https://api.tradier.com
    TRADIER_ACCOUNT   = os.getenv("TRADIER_ACCOUNT", "")
    TRADIER_API_DELAY = _f("TRADIER_API_DELAY", 1.00)
    DRY_RUN           = _b("DRY_RUN", False)
    DURATION          = os.getenv("ORDER_DURATION", "day")  # day / gtc

    # --- Strategy filters (shared) ---
    DTE_MIN, DTE_MAX     = _i("DTE_MIN", 30), _i("DTE_MAX", 60)
    OI_MIN, VOL_MIN      = _i("OI_MIN", 200), _i("VOL_MIN", 50)
    MAX_SPREAD           = _f("MAX_SPREAD_PCT", 0.15)

    # Calls delta window (absolute +) and Puts delta window (negative -)
    CALL_DELTA_MIN, CALL_DELTA_MAX = _f("CALL_DELTA_MIN", 0.60), _f("CALL_DELTA_MAX", 0.80)
    PUT_DELTA_MIN,  PUT_DELTA_MAX  = _f("PUT_DELTA_MIN", -0.80), _f("PUT_DELTA_MAX", -0.60)

    # --- Indicators ---
    EMA_FAST, EMA_SLOW, RSI_LEN = _i("EMA_FAST", 20), _i("EMA_SLOW", 50), _i("RSI_LEN", 14)
    ATR_LEN, TRAIL_K, OPT_DD, DTE_STOP = _i("ATR_LEN", 14), _f("TRAIL_K_ATR", 3.0), _f("OPT_DD_EXIT", 0.25), _i("DTE_STOP", 7)

    # --- Exits (on premium) ---
    TAKE_PROFIT = _f("OPT_TAKE_PROFIT", 0.25)   # +25% TP
    STOP_LOSS   = _f("OPT_STOP_LOSS", 0.08)     # -8% SL

    # --- Orders / persistence ---
    QTY = _i("ORDER_QTY", 1)
    DB  = os.getenv("DB_PATH", "./options_unified.db")

    # --- Trading session guards ---
    ALLOW_AFTER_HOURS = _b("ALLOW_AFTER_HOURS", False)
    SKIP_OPEN_MIN     = _i("SKIP_OPEN_MIN", 5)
    SKIP_CLOSE_MIN    = _i("SKIP_CLOSE_MIN", 10)

    # --- Logging ---
    VERBOSE = _b("VERBOSE", True)

DEBUG = os.getenv("DEBUG", "false").lower() in ("1","true","yes")

# -------------------- HTTP helpers --------------------

def _alpaca_headers():
    return {
        "APCA-API-KEY-ID": S.ALPACA_KEY,
        "APCA-API-SECRET-KEY": S.ALPACA_SECRET,
        "Accept": "application/json",
        "User-Agent": "OptionsExecutor/3.0 (+Unified)"
    }

def _alpaca_get(path, params=None):
    url = S.ALPACA_DATA_BASE.rstrip("/") + path
    try:
        r = requests.get(url, headers=_alpaca_headers(), params=params or {}, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[WARN] Alpaca GET {url}: {e}"); data = {}
    time.sleep(S.ALPACA_API_DELAY)
    return data

def _tradier_headers():
    return {
        "Authorization": f"Bearer {S.TRADIER_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "OptionsExecutor/3.0"
    }

def _tradier_get(path, params=None):
    url = S.TRADIER_BASE.rstrip("/") + path
    try:
        r = requests.get(url, headers=_tradier_headers(), params=params or {}, timeout=30)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[WARN] Tradier GET {url}: {e}"); data = {}
    time.sleep(S.TRADIER_API_DELAY)
    return data

def _tradier_post(path, data):
    url = S.TRADIER_BASE.rstrip("/") + path
    try:
        r = requests.post(url, headers={**_tradier_headers(), "Content-Type": "application/x-www-form-urlencoded"}, data=data, timeout=30)
        data = r.json()
    except Exception as e:
        print(f"[WARN] Tradier POST {url}: {e}"); data = {}
    time.sleep(S.TRADIER_API_DELAY)
    return data

# -------------------- Time/session helpers --------------------

def _parse_iso(ts: Optional[str]) -> Optional[datetime]:
    if not ts: return None
    try:
        ts = ts.replace("Z", "+00:00")
        return datetime.fromisoformat(ts)
    except Exception:
        return None

def _et_now() -> datetime:
    try:
        return datetime.now(ZoneInfo("America/New_York"))
    except Exception:
        return datetime.now().astimezone()

def market_open_rth() -> bool:
    """True only during RTH (holiday-aware via Tradier) with buffers at open/close."""
    try:
        clk = _tradier_get("/v1/markets/clock") or {}
        c = clk.get("clock") or clk
        state = str(c.get("state", "")).lower()
        if state != "open":
            return False
        now = _parse_iso(c.get("timestamp")) or datetime.now(UTC)
        nxt = _parse_iso(c.get("next_change"))
        if nxt:
            mins_to_close = (nxt - now).total_seconds() / 60.0
            if mins_to_close < S.SKIP_CLOSE_MIN:
                return False
        et = _et_now()
        start = et.replace(hour=9, minute=30, second=0, microsecond=0)
        if et < start + timedelta(minutes=S.SKIP_OPEN_MIN):
            return False
        if et.weekday() >= 5:
            return False
        return True
    except Exception:
        et = _et_now()
        if et.weekday() >= 5: return False
        start = et.replace(hour=9, minute=30, second=0, microsecond=0)
        end   = et.replace(hour=16, minute=0,  second=0, microsecond=0)
        if et < start + timedelta(minutes=S.SKIP_OPEN_MIN): return False
        if et > end - timedelta(minutes=S.SKIP_CLOSE_MIN):  return False
        return start <= et <= end

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

def chandelier_bull(high, atrv):
    return high - S.TRAIL_K * atrv if atrv is not None else high

def chandelier_bear(low, atrv):
    return low + S.TRAIL_K * atrv if atrv is not None else low

# -------------------- Screeners --------------------

def trend_bull_ok(df):
    """Bullish health check: EMA20>EMA50; RSI rising 50-70; price not >5% above EMA20."""
    need = max(S.EMA_SLOW + 5, S.RSI_LEN + 5)
    if df.empty or len(df) < need: return False
    c = df["Close"].astype(float)
    ef, es = ema(c, S.EMA_FAST), ema(c, S.EMA_SLOW)
    r = rsi(c, S.RSI_LEN).dropna()
    if len(r) < 2: return False
    r_last, r_prev = float(r.iloc[-1]), float(r.iloc[-2])
    price = float(c.iloc[-1])
    if not (ef.iloc[-1] > es.iloc[-1]): return False
    if not (50.0 < r_last < 70.0): return False
    if r_last < r_prev: return False
    if price > float(ef.iloc[-1]) * 1.05: return False
    return True

def trend_bear_ok(df):
    """Bearish health check: EMA20<EMA50; RSI falling 30-50; price not <95% of EMA20."""
    need = max(S.EMA_SLOW + 5, S.RSI_LEN + 5)
    if df.empty or len(df) < need: return False
    c = df["Close"].astype(float)
    ef, es = ema(c, S.EMA_FAST), ema(c, S.EMA_SLOW)
    r = rsi(c, S.RSI_LEN).dropna()
    if len(r) < 2: return False
    r_last, r_prev = float(r.iloc[-1]), float(r.iloc[-2])
    price = float(c.iloc[-1])
    if not (ef.iloc[-1] < es.iloc[-1]): return False
    if not (30.0 <= r_last < 50.0): return False
    if r_last > r_prev: return False
    if price < float(ef.iloc[-1]) * 0.95: return False
    return True

# -------------------- DB --------------------

def db_init():
    con = sqlite3.connect(S.DB)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS picks (
            ticker TEXT,
            contract TEXT PRIMARY KEY,
            expiry TEXT,
            strike REAL,
            delta REAL,
            entry_underlying REAL,
            entry_option REAL,
            highest_close REAL,
            lowest_close REAL,
            trail_bull REAL,
            trail_bear REAL,
            peak_option REAL,
            created_at TEXT,
            option_type TEXT
        )
        """
    )
    # migrations for older tables
    for col in [
        ("option_type", "ALTER TABLE picks ADD COLUMN option_type TEXT"),
        ("highest_close", "ALTER TABLE picks ADD COLUMN highest_close REAL"),
        ("lowest_close", "ALTER TABLE picks ADD COLUMN lowest_close REAL"),
        ("trail_bull", "ALTER TABLE picks ADD COLUMN trail_bull REAL"),
        ("trail_bear", "ALTER TABLE picks ADD COLUMN trail_bear REAL"),
    ]:
        try:
            con.execute(col[1])
        except Exception:
            pass
    con.commit(); con.close()


def db_all():
    con = sqlite3.connect(S.DB); con.row_factory = sqlite3.Row
    rows = [dict(r) for r in con.execute("SELECT * FROM picks")] ; con.close(); return rows


def db_add(row):
    con = sqlite3.connect(S.DB)
    con.execute(
        """
        INSERT OR IGNORE INTO picks
        (ticker, contract, expiry, strike, delta, entry_underlying, entry_option,
         highest_close, lowest_close, trail_bull, trail_bear, peak_option, created_at, option_type)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        row,
    )
    con.commit(); con.close()


def db_upd(contract, highest_close, lowest_close, trail_bull, trail_bear, peak_option):
    con = sqlite3.connect(S.DB)
    con.execute(
        "UPDATE picks SET highest_close=?, lowest_close=?, trail_bull=?, trail_bear=?, peak_option=? WHERE contract=?",
        (highest_close, lowest_close, trail_bull, trail_bear, peak_option, contract),
    )
    con.commit(); con.close()


def db_del(contract):
    con = sqlite3.connect(S.DB)
    con.execute("DELETE FROM picks WHERE contract=?", (contract,))
    con.commit(); con.close()

# -------------------- Universe --------------------
_FALLBACK_SP500 = [
    "AAPL","MSFT","AMZN","GOOGL","META","NVDA","BRK.B","JPM","JNJ","XOM",
    "V","PG","MA","AVGO","HD","KO","PEP","PFE","ABBV","BAC"
]

_SP500_SOURCES = [
    "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv",
    "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv",
]

def sp500_symbols() -> List[str]:
    sess = requests.Session()
    sess.headers.update({"User-Agent": "Mozilla/5.0 OptionsExecutor/3.0"})
    for src in _SP500_SOURCES:
        try:
            r = sess.get(src, timeout=30); r.raise_for_status()
            from io import StringIO
            df = pd.read_csv(StringIO(r.text))
            for col in ["Symbol","Ticker","symbol","ticker"]:
                if col in df.columns:
                    syms=[str(x).strip().upper().replace(" ","") for x in df[col].tolist()]
                    syms=[s.replace("\u200a","").replace("\u00b7",".") for s in syms]
                    seen=set(); out=[]
                    for s in syms:
                        if s and s not in seen:
                            out.append(s); seen.add(s)
                    if len(out) >= 450:
                        if DEBUG: print(f"[INFO] S&P 500 fetched from {src}: {len(out)}")
                        return out
        except Exception as e:
            print(f"[WARN] S&P 500 fetch failed from {src}: {e}")
    print("[WARN] Using small S&P 500 fallback list")
    return _FALLBACK_SP500[:]

# -------------------- Data (Alpaca, Tradier) --------------------

def alpaca_history_daily_symbol(symbol: str, days: int = 420) -> pd.DataFrame:
    start = (datetime.now(UTC) - timedelta(days=days+10)).isoformat()
    params = {"timeframe":"1Day","start":start,"limit":1500,"adjustment":"raw","feed":S.ALPACA_FEED}
    d = _alpaca_get(f"/v2/stocks/{symbol}/bars", params=params)
    bars = d.get("bars")
    if not bars: return pd.DataFrame()
    df = pd.DataFrame(bars)
    if df.empty: return pd.DataFrame()
    df["date"] = pd.to_datetime(df["t"])
    df = df.rename(columns={"h":"High","l":"Low","c":"Close","v":"Volume"})
    for c in ("High","Low","Close","Volume"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    return df[["date","High","Low","Close","Volume"]].set_index("date").sort_index()


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
    d = _tradier_get("/v1/markets/options/expirations", {"symbol": sym, "includeAllRoots":"true", "strikes":"false"})
    e = (d.get("expirations") or {}).get("date")
    return e if isinstance(e, list) else ([e] if e else [])


def tradier_chain(sym: str, exp: str) -> pd.DataFrame:
    d = _tradier_get("/v1/markets/options/chains", {"symbol": sym, "expiration": exp, "greeks":"true"})
    o = (d.get("options") or {}).get("option")
    if not o: return pd.DataFrame()
    df = pd.DataFrame(o)
    for c in ["strike","bid","ask","volume","open_interest","last","change"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    if "option_type" not in df.columns and "type" in df.columns:
        df["option_type"] = df["type"]
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


def tradier_positions() -> List[Dict]:
    if not S.TRADIER_ACCOUNT: return []
    d = _tradier_get(f"/v1/accounts/{S.TRADIER_ACCOUNT}/positions")
    p = (d.get("positions") or {}).get("position")
    return p if isinstance(p, list) else ([p] if p else [])


def tradier_order(underlying: str, occ_symbol: str, side: str, qty: int, otype="limit", price=None, preview=False):
    if S.DRY_RUN:
        print("[DRY_RUN] order", {"underlying": underlying, "occ": occ_symbol, "side": side, "qty": qty, "type": otype, "price": price})
        return {"status": "dry_run"}
    if not S.TRADIER_ACCOUNT:
        print("[ERROR] TRADIER_ACCOUNT is missing.")
        return {"error": "no account"}
    data = {
        "class": "option",
        "symbol": underlying,
        "option_symbol": occ_symbol,
        "side": side,
        "quantity": str(qty),
        "type": otype,
        "duration": S.DURATION,
    }
    if otype == "limit" and price is not None:
        data["price"] = f"{price:.2f}"
    if preview:
        data["preview"] = "true"
    resp = _tradier_post(f"/v1/accounts/{S.TRADIER_ACCOUNT}/orders", data)
    if "errors" in resp:
        print("[TRADIER][ORDER][ERROR]", json.dumps(resp, indent=2))
    else:
        print("[TRADIER][ORDER][OK]", json.dumps(resp, indent=2))
    return resp

# -------------------- Duplicate-protection helpers --------------------

def has_open_pick_in_db(ticker: str, occ: Optional[str] = None) -> bool:
    con = sqlite3.connect(S.DB); con.row_factory = sqlite3.Row
    if occ:
        r = con.execute("SELECT 1 FROM picks WHERE contract=?", (occ,)).fetchone()
    else:
        r = con.execute("SELECT 1 FROM picks WHERE ticker=?", (ticker,)).fetchone()
    con.close(); return r is not None


def has_tradier_position(ticker: str, occ: Optional[str] = None) -> bool:
    try: poss = tradier_positions()
    except Exception: return False
    if not poss: return False
    items = poss if isinstance(poss, list) else [poss]
    for p in items:
        sym = str(p.get("symbol","")); und = str(p.get("underlying",""))
        if occ:
            if sym == occ: return True
        else:
            if und == ticker or sym == ticker or sym.split(' ')[0] == ticker: return True
    return False


def already_holding(ticker: str, occ: str) -> bool:
    return has_open_pick_in_db(ticker) or has_open_pick_in_db(ticker, occ) or \
           has_tradier_position(ticker) or has_tradier_position(ticker, occ)

# -------------------- Selection --------------------

@dataclass
class Pick:
    ticker: str; contract: str; expiry: str; strike: float; bid: float; ask: float; mid: float; delta: float; dte: int; option_type: str


def pick_from_chain(t: str, exp: str, today: datetime, option_type: str) -> Optional[Pick]:
    df = tradier_chain(t, exp)
    if df.empty:
        if DEBUG: print(f"[INFO] {t} {exp}: empty chain"); return None
    if "option_type" not in df.columns and "type" in df.columns:
        df["option_type"] = df["type"]
    leg = df[df.get("option_type") == option_type].copy()
    if leg.empty: return None

    try: exp_dt = datetime.fromisoformat(exp)
    except Exception: exp_dt = datetime.strptime(exp, "%Y-%m-%d")
    dte = max(0, (exp_dt.date() - today.date()).days)
    if not (S.DTE_MIN <= dte <= S.DTE_MAX): return None

    for c in ["bid","ask","volume","open_interest","delta","strike"]:
        if c in leg.columns: leg[c] = pd.to_numeric(leg[c], errors="coerce")
    leg = leg[(leg["bid"] > 0) & (leg["ask"] > 0) & (leg["ask"] >= leg["bid"]) ]
    if leg.empty: return None

    leg["mid"] = (leg["bid"] + leg["ask"]) / 2.0
    leg = leg[leg["mid"] > 0.01]
    leg["spr"] = (leg["ask"] - leg["bid"]) / leg["mid"].clip(1e-9)

    if option_type == "call":
        leg = leg[
            (leg["open_interest"] >= S.OI_MIN) &
            (leg["volume"] >= S.VOL_MIN) &
            (leg["spr"] <= S.MAX_SPREAD) &
            (leg["delta"].between(S.CALL_DELTA_MIN, S.CALL_DELTA_MAX))
        ]
        target = 0.5 * (S.CALL_DELTA_MIN + S.CALL_DELTA_MAX)  # ~ +0.70
    else:
        leg = leg[
            (leg["open_interest"] >= S.OI_MIN) &
            (leg["volume"] >= S.VOL_MIN) &
            (leg["spr"] <= S.MAX_SPREAD) &
            (leg["delta"].between(S.PUT_DELTA_MIN, S.PUT_DELTA_MAX))
        ]
        target = 0.5 * (S.PUT_DELTA_MIN + S.PUT_DELTA_MAX)   # ~ -0.70

    if leg.empty: return None

    leg["drank"] = (leg["delta"] - target) ** 2
    best = leg.sort_values(by=["drank","spr","mid","open_interest"], ascending=[True,True,False,False]).iloc[0]

    return Pick(t, str(best.get("symbol")), exp, float(best["strike"]), float(best["bid"]),
                float(best["ask"]), float(best["mid"]), float(best["delta"]), dte, option_type)

# -------------------- Exits / management --------------------

def manage_open_for_ticker(t: str, cnt: Dict[str,int]):
    hist = tradier_history_daily(t, 420)
    if hist.empty: return
    spot = float(hist["Close"].iloc[-1])
    hi = float(hist["Close"].rolling(2).max().iloc[-1])  # quick stab if highest_close missing
    lo = float(hist["Close"].rolling(2).min().iloc[-1])

    atr_series = atr(hist, S.ATR_LEN)
    atr_val = float(atr_series.dropna().iloc[-1]) if atr_series.notna().any() else None

    picks = [p for p in db_all() if p.get("ticker") == t]
    for p in picks:
        option_type = p.get("option_type", "call")
        q = tradier_quote(p["contract"]) or {}
        bid, ask = float(q.get("bid",0)), float(q.get("ask",0))
        mid = (bid + ask)/2 if bid>0 and ask>0 else p["entry_option"]
        peak = max(p.get("peak_option", mid), mid)

        highest_close = max(p.get("highest_close", hi), spot)
        lowest_close  = min(p.get("lowest_close", lo), spot)
        trail_bull = chandelier_bull(highest_close, atr_val) if atr_val is not None else p.get("trail_bull", spot)
        trail_bear = chandelier_bear(lowest_close,  atr_val) if atr_val is not None else p.get("trail_bear", spot)

        reason=None
        # hard stops / targets on premium
        if mid <= p["entry_option"] * (1 - S.STOP_LOSS):
            reason = "stoploss"
        elif mid >= p["entry_option"] * (1 + S.TAKE_PROFIT):
            reason = "target"
        else:
            # chandelier per leg
            if option_type == "call" and atr_val is not None and spot < trail_bull:
                reason = "trail_bull_break"
            elif option_type == "put" and atr_val is not None and spot > trail_bear:
                reason = "trail_bear_break"
            # peak drawdown on option
            elif mid <= peak*(1 - S.OPT_DD):
                reason = "drawdown"
            else:
                try: exp_dt = datetime.fromisoformat(p["expiry"])
                except Exception: exp_dt = datetime.strptime(p["expiry"], "%Y-%m-%d")
                if (exp_dt.date() - datetime.now(UTC).date()).days <= S.DTE_STOP:
                    reason = "time"

        if reason:
            side = "sell_to_close"
            print(f"SELL {option_type.upper()} {t} {p['contract']} reason={reason}")
            tradier_order(t, p["contract"], side, S.QTY, "market")
            db_del(p["contract"])
            cnt["sells"] += 1
        else:
            db_upd(p["contract"], highest_close, lowest_close, trail_bull, trail_bear, peak)

# -------------------- Candidate processing --------------------

def process_candidate_tradier(t: str, option_type: str, cnt: Dict[str,int]):
    if not S.ALLOW_AFTER_HOURS and not market_open_rth():
        if DEBUG: print(f"[SKIP] {t}: market not open for new buys")
        return

    hist = tradier_history_daily(t, 420)
    if hist.empty:
        if DEBUG: print(f"[SKIP] {t}: no Tradier history"); return
    spot = float(hist["Close"].iloc[-1])

    atr_series = atr(hist, S.ATR_LEN)
    atr_val = float(atr_series.dropna().iloc[-1]) if atr_series.notna().any() else None

    exps = tradier_expirations(t)
    if not exps:
        if DEBUG: print(f"[SKIP] {t}: no expirations"); return

    today = datetime.now(UTC)
    target_dte = (S.DTE_MIN + S.DTE_MAX) // 2
    scored: List[tuple] = []
    for exp in exps:
        cand = pick_from_chain(t, exp, today, option_type)
        if cand:
            scored.append((abs(cand.dte - target_dte), cand))
    if not scored:
        if DEBUG: print(f"[NO {option_type.upper()}] {t}: no viable options"); return

    best = min(scored, key=lambda x: x[0])[1]

    if already_holding(t, best.contract):
        print(f"[SKIP] {t}: already holding {t} / {best.contract}"); return

    cnt["viable"] += 1
    limit = round(min(best.ask, best.mid * 1.02), 2)
    side = "buy_to_open"
    print(f"BUY {option_type.upper()} {t} {best.contract} Δ={best.delta:.2f} @{limit}")
    tradier_order(t, best.contract, side, S.QTY, "limit", limit)

    # initial trails
    hi = float(hist["Close"].iloc[-1])
    lo = float(hist["Close"].iloc[-1])
    trail_bull = chandelier_bull(hi, atr_val) if atr_val is not None else hi
    trail_bear = chandelier_bear(lo, atr_val) if atr_val is not None else lo

    db_add((t, best.contract, best.expiry, best.strike, best.delta,
            spot, best.mid, hi, lo, trail_bull, trail_bear, best.mid, datetime.now(UTC).isoformat(), option_type))
    cnt["buys"] += 1

# -------------------- Screening --------------------

def screen_symbol(sym: str, leg: str) -> bool:
    df = alpaca_history_daily_symbol(sym, 420)
    if df.empty:
        if DEBUG: print(f"[SKIP] {sym}: no bars"); return False
    if leg == "call":
        passed = trend_bull_ok(df)
    else:
        passed = trend_bear_ok(df)
    if DEBUG:
        c = df["Close"]
        ef = float(ema(c, S.EMA_FAST).iloc[-1])
        es = float(ema(c, S.EMA_SLOW).iloc[-1])
        r = rsi(c, S.RSI_LEN).dropna()
        r_last = float(r.iloc[-1]) if len(r) else float('nan')
        price = float(c.iloc[-1])
        tag = "PASS" if passed else "FAIL"
        orient = ">" if leg=="call" else "<"
        print(f"[{tag}-{leg.upper()}] {sym}: price={price:.2f}, EMA{S.EMA_FAST}={ef:.2f} {orient} EMA{S.EMA_SLOW}={es:.2f}, RSI={r_last:.1f}")
    return passed

# -------------------- Runner --------------------

def sanity_check_tradier():
    print(f"[ENV] TRADIER_BASE={S.TRADIER_BASE} token_set={bool(S.TRADIER_TOKEN)} account={S.TRADIER_ACCOUNT}")
    try: print("[CHECK] market clock:", _tradier_get("/v1/markets/clock"))
    except Exception as e: print("[CHECK] clock error:", e)
    try: print("[CHECK] user profile:", _tradier_get("/v1/user/profile"))
    except Exception as e: print("[CHECK] profile error:", e)
    if S.TRADIER_ACCOUNT:
        try:
            print("[CHECK] balances:", _tradier_get(f"/v1/accounts/{S.TRADIER_ACCOUNT}/balances"))
            print("[CHECK] existing orders:", _tradier_get(f"/v1/accounts/{S.TRADIER_ACCOUNT}/orders"))
        except Exception as e:
            print("[CHECK] account error:", e)


def run_once():
    # Validate env
    miss=[]
    if not S.ALPACA_KEY or not S.ALPACA_SECRET: miss.append("APCA_API_KEY_ID/APCA_API_SECRET_KEY")
    if not S.TRADIER_TOKEN: miss.append("TRADIER_TOKEN")
    if not S.TRADIER_ACCOUNT and not S.DRY_RUN: miss.append("TRADIER_ACCOUNT")
    if miss:
        print("[ERROR] missing env:", ", ".join(miss)); return

    strat = S.STRATEGY if S.STRATEGY in ("calls","puts","both") else "both"
    print(f"[ENV] Strategy={strat} | Alpaca feed={S.ALPACA_FEED} base={S.ALPACA_DATA_BASE}")
    print(f"[ENV] Exits: STOP_LOSS={S.STOP_LOSS:.2%}, TAKE_PROFIT={S.TAKE_PROFIT:.2%}, PEAK_DD={S.OPT_DD:.2%}, DTE_STOP={S.DTE_STOP}d")

    db_init()
    sanity_check_tradier()

    # Universe
    syms = sp500_symbols()
    print(f"[INFO] Universe: S&P 500 symbols={len(syms)}")

    cnt={"viable":0,"buys":0,"sells":0}

    # Screen & buy only if market is open (or explicitly allowed)
    if not S.ALLOW_AFTER_HOURS and not market_open_rth():
        print("[MARKET CLOSED] Skipping screening & new buys; will still manage open positions.")
    else:
        legs = ["call","put"] if strat == "both" else (["call"] if strat=="calls" else ["put"])
        for leg in legs:
            candidates: List[str] = []
            for i, sym in enumerate(syms, 1):
                try:
                    if DEBUG and i % 50 == 1:
                        print(f"[PROGRESS] Screening {leg.upper()} {i}/{len(syms)}…")
                    if screen_symbol(sym, leg):
                        candidates.append(sym)
                except Exception as e:
                    print(f"[ERR][screen] {sym}: {e}")

            print(f"[INFO] {leg.upper()} candidates: {len(candidates)}")

            for t in candidates:
                try:
                    process_candidate_tradier(t, leg, cnt)
                except Exception as e:
                    print(f"[ERR] {t}: {e}")

    # Manage existing picks (always)
    open_ticks = sorted(set([p["ticker"] for p in db_all()]))
    for t in open_ticks:
        try:
            manage_open_for_ticker(t, cnt)
        except Exception as e:
            print(f"[ERR][manage] {t}: {e}")

    print(f"[SUMMARY] candidates={(cnt['viable']+0)} viable={cnt['viable']} buys={cnt['buys']} sells={cnt['sells']}")


if __name__ == "__main__":
    run_once()
