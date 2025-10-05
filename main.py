# =========================================
# OptionExecutor (Tradier-only, full S&P 500)
# - Scans all S&P 500 tickers each run (no rotation)
# - Uses GitHub CSV mirror for universe
# - Self-throttled to ~60 API calls/min (1 s delay)
# - Tradier for all data and execution
# =========================================

import os
import json
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from typing import Dict, List, Optional

import requests
import pandas as pd


# -------------------- Settings --------------------
def _get_bool(name, default):
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def _get_float(name, default):
    v = os.getenv(name)
    try:
        return float(v) if v is not None else default
    except Exception:
        return default


def _get_int(name, default):
    v = os.getenv(name)
    try:
        return int(v) if v is not None else default
    except Exception:
        return default


class SETTINGS:
    TRADIER_TOKEN = os.getenv("TRADIER_TOKEN", "")
    TRADIER_BASE = os.getenv("TRADIER_BASE", "https://sandbox.tradier.com")
    TRADIER_ACCOUNT = os.getenv("TRADIER_ACCOUNT", "")
    DRY_RUN = _get_bool("DRY_RUN", True)
    DURATION = os.getenv("ORDER_DURATION", "day")

    # Option filters
    DTE_MIN = _get_int("DTE_MIN", 30)
    DTE_MAX = _get_int("DTE_MAX", 60)
    DELTA_MIN = _get_float("DELTA_MIN", 0.60)
    DELTA_MAX = _get_float("DELTA_MAX", 0.80)
    OI_MIN = _get_int("OI_MIN", 200)
    VOL_MIN = _get_int("VOL_MIN", 50)
    MAX_SPREAD_PCT = _get_float("MAX_SPREAD_PCT", 0.15)

    # Indicators
    EMA_FAST = _get_int("EMA_FAST", 20)
    EMA_SLOW = _get_int("EMA_SLOW", 50)
    RSI_LEN = _get_int("RSI_LEN", 14)
    ATR_LEN = _get_int("ATR_LEN", 14)
    TRAIL_K_ATR = _get_float("TRAIL_K_ATR", 3.0)
    OPT_DD_EXIT = _get_float("OPT_DD_EXIT", 0.25)
    DTE_STOP = _get_int("DTE_STOP", 7)

    # Position sizing
    ORDER_QTY = _get_int("ORDER_QTY", 1)

    # Persistence
    DB_PATH = os.getenv("DB_PATH", "./executor_tradier.db")

    # Logging
    VERBOSE = _get_bool("VERBOSE", True)

    # API delay
    API_DELAY = _get_float("API_DELAY", 1.0)  # seconds between calls


# -------------------- HTTP / Tradier --------------------
def _auth_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {SETTINGS.TRADIER_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "OptionExecutor/1.4",
    }


def safe_get(path: str, params: Dict = None) -> Dict:
    url = SETTINGS.TRADIER_BASE.rstrip("/") + path
    try:
        r = requests.get(url, headers=_auth_headers(), params=params or {}, timeout=25)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"[WARN] GET failed {path}: {e}")
        data = {}
    time.sleep(SETTINGS.API_DELAY)
    return data


def safe_post(path: str, data: Dict) -> Dict:
    url = SETTINGS.TRADIER_BASE.rstrip("/") + path
    try:
        r = requests.post(
            url,
            headers={**_auth_headers(), "Content-Type": "application/x-www-form-urlencoded"},
            data=data,
            timeout=25,
        )
        data = r.json()
    except Exception as e:
        print(f"[WARN] POST failed {path}: {e}")
        data = {}
    time.sleep(SETTINGS.API_DELAY)
    return data


# ---- Market data ----
def tradier_quote(symbols: List[str]) -> Dict[str, Dict]:
    symlist = ",".join(symbols)
    data = safe_get("/v1/markets/quotes", {"symbols": symlist})
    quotes = {}
    q = (data.get("quotes") or {}).get("quote")
    if q is None:
        return quotes
    rows = q if isinstance(q, list) else [q]
    for row in rows:
        sym = str(row.get("symbol"))
        quotes[sym] = row
    return quotes


def tradier_history_daily(symbol: str, days: int = 420) -> pd.DataFrame:
    start = (datetime.now(UTC) - timedelta(days=days + 10)).date().isoformat()
    data = safe_get(
        "/v1/markets/history",
        {"symbol": symbol, "interval": "daily", "start": start},
    )
    hist = (data.get("history") or {}).get("day")
    if not hist:
        return pd.DataFrame()
    df = pd.DataFrame(hist)
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["date"] = pd.to_datetime(df["date"])
    df = df.rename(columns={"high": "High", "low": "Low", "close": "Close", "volume": "Volume"})
    return df.set_index("date").sort_index()


def tradier_expirations(symbol: str) -> List[str]:
    data = safe_get("/v1/markets/options/expirations", {"symbol": symbol, "includeAllRoots": "true", "strikes": "false"})
    exps = (data.get("expirations") or {}).get("date")
    if not exps:
        return []
    return exps if isinstance(exps, list) else [exps]


def tradier_chain(symbol: str, expiration: str) -> pd.DataFrame:
    data = safe_get("/v1/markets/options/chains", {"symbol": symbol, "expiration": expiration, "greeks": "true"})
    opts = (data.get("options") or {}).get("option")
    if not opts:
        return pd.DataFrame()
    df = pd.DataFrame(opts)
    for c in ["strike", "last", "bid", "ask", "volume", "open_interest", "delta"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def tradier_positions() -> List[Dict]:
    if not SETTINGS.TRADIER_ACCOUNT:
        return []
    data = safe_get(f"/v1/accounts/{SETTINGS.TRADIER_ACCOUNT}/positions")
    pos = (data.get("positions") or {}).get("position")
    if not pos:
        return []
    return pos if isinstance(pos, list) else [pos]


def tradier_place_option_order(symbol_occ: str, side: str, qty: int, order_type="limit", price=None, duration=None) -> Dict:
    if SETTINGS.DRY_RUN:
        return {"status": "dry_run", "symbol": symbol_occ, "side": side, "qty": qty, "type": order_type, "price": price}
    if not SETTINGS.TRADIER_ACCOUNT:
        return {"error": "TRADIER_ACCOUNT not set"}
    payload = {
        "class": "option",
        "symbol": symbol_occ,
        "side": side,
        "quantity": str(int(max(1, qty))),
        "type": order_type,
        "duration": duration or SETTINGS.DURATION,
    }
    if order_type == "limit" and price:
        payload["price"] = f"{price:.02f}"
    return safe_post(f"/v1/accounts/{SETTINGS.TRADIER_ACCOUNT}/orders", payload)


# -------------------- Indicators --------------------
def ema(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False).mean()


def rsi(series: pd.Series, n: int = 14) -> pd.Series:
    delta = series.diff()
    up, down = delta.clip(lower=0), -delta.clip(upper=0)
    roll_up = up.ewm(alpha=1 / n, adjust=False).mean()
    roll_down = down.ewm(alpha=1 / n, adjust=False).mean()
    rs = roll_up / (roll_down + 1e-12)
    return 100 - 100 / (1 + rs)


def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    hi, lo, cl = df["High"], df["Low"], df["Close"]
    prev = cl.shift(1)
    tr = pd.concat([(hi - lo).abs(), (hi - prev).abs(), (lo - prev).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()


def daily_trend_ok(df: pd.DataFrame) -> bool:
    if df.empty or len(df) < max(SETTINGS.EMA_SLOW + 5, SETTINGS.RSI_LEN + 5):
        return False
    close = df["Close"]
    e_fast, e_slow = ema(close, SETTINGS.EMA_FAST), ema(close, SETTINGS.EMA_SLOW)
    r = rsi(close, SETTINGS.RSI_LEN)
    return bool((e_fast.iloc[-1] > e_slow.iloc[-1]) and (r.iloc[-1] > 50) and (r.iloc[-1] >= r.iloc[-2]))


def chandelier_trail(highest_close: float, atr_val: float) -> float:
    return highest_close - SETTINGS.TRAIL_K_ATR * atr_val


# -------------------- SQLite --------------------
def db_init():
    con = sqlite3.connect(SETTINGS.DB_PATH)
    cur = con.cursor()
    cur.execute(
        """CREATE TABLE IF NOT EXISTS picks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT, contract TEXT UNIQUE, expiry TEXT, strike REAL,
            delta REAL, entry_underlying REAL, entry_option REAL,
            highest_close REAL, trail REAL, peak_option REAL, created_at TEXT)"""
    )
    con.commit()
    con.close()


def db_insert_pick(row: Dict):
    con = sqlite3.connect(SETTINGS.DB_PATH)
    cur = con.cursor()
    cur.execute(
        """INSERT OR IGNORE INTO picks
        (ticker, contract, expiry, strike, delta, entry_underlying, entry_option,
         highest_close, trail, peak_option, created_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (
            row["ticker"],
            row["contract"],
            row["expiry"],
            row["strike"],
            row["delta"],
            row["entry_underlying"],
            row["entry_option"],
            row["highest_close"],
            row["trail"],
            row["peak_option"],
            row["created_at"],
        ),
    )
    con.commit()
    con.close()


def db_get_picks() -> List[Dict]:
    con = sqlite3.connect(SETTINGS.DB_PATH)
    con.row_factory = sqlite3.Row
    rows = con.execute("SELECT * FROM picks;").fetchall()
    con.close()
    return [dict(r) for r in rows]


def db_delete_pick(contract: str):
    con = sqlite3.connect(SETTINGS.DB_PATH)
    con.execute("DELETE FROM picks WHERE contract = ?;", (contract,))
    con.commit()
    con.close()


def db_update_pick(contract: str, highest_close: float, trail: float, peak_option: float):
    con = sqlite3.connect(SETTINGS.DB_PATH)
    con.execute(
        "UPDATE picks SET highest_close=?, trail=?, peak_option=? WHERE contract=?;",
        (highest_close, trail, peak_option, contract),
    )
    con.commit()
    con.close()


# -------------------- Universe (GitHub CSV mirror) --------------------
def get_universe_symbols() -> List[str]:
    url = "https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
    try:
        df = pd.read_csv(url)
        return (
            df["Symbol"]
            .astype(str)
            .str.replace(".", "-", regex=False)
            .str.upper()
            .drop_duplicates()
            .tolist()
        )
    except Exception as e:
        print(f"[WARN] Could not load S&P 500 CSV: {e}")
        return ["AAPL", "MSFT", "NVDA", "AMZN", "META"]


# -------------------- Strategy / Execution --------------------
@dataclass
class OptionCandidate:
    ticker: str
    contract: str
    expiry: str
    strike: float
    bid: float
    ask: float
    mid: float
    delta: float
    dte: int


def pick_best_call_from_chain(ticker: str, expiration: str, today: datetime) -> Optional[OptionCandidate]:
    df = tradier_chain(ticker, expiration)
    if df.empty:
        return None
    calls = df[df.get("option_type") == "call"].copy()
    if calls.empty or "delta" not in calls.columns:
        return None

    exp_dt = datetime.fromisoformat(expiration)
    dte = max(0, (exp_dt.date() - today.date()).days)
    if not (SETTINGS.DTE_MIN <= dte <= SETTINGS.DTE_MAX):
        return None

    for c in ["bid", "ask", "volume", "open_interest", "delta", "strike"]:
        if c in calls.columns:
            calls[c] = pd.to_numeric(calls[c], errors="coerce")
    calls["mid"] = (calls["bid"] + calls["ask"]) / 2
    calls = calls[(calls["bid"] > 0) & (calls["ask"] > 0) & (calls["ask"] >= calls["bid"])]
    calls["spread_pct"] = (calls["ask"] - calls["bid"]) / calls["mid"].clip(lower=1e-9)
    calls = calls[
        (calls["open_interest"] >= SETTINGS.OI_MIN)
        & (calls["volume"] >= SETTINGS.VOL_MIN)
        & (calls["spread_pct"] <= SETTINGS.MAX_SPREAD_PCT)
        & (calls["delta"].between(SETTINGS.DELTA_MIN, SETTINGS.DELTA_MAX))
    ]
    if calls.empty:
        return None
    target = 0.5 * (SETTINGS.DELTA_MIN + SETTINGS.DELTA_MAX)
    calls["delta_rank"] = (calls["delta"] - target).abs()
    best = calls.sort_values(by=["delta_rank", "spread_pct"], ascending=[True, True]).iloc[0]
    return OptionCandidate(ticker, str(best["symbol"]), expiration, float(best["strike"]), float(best["bid"]),
                           float(best["ask"]), float(best["mid"]), float(best["delta"]), dte)


# -------------------- Main loop --------------------
def process_ticker(ticker: str, positions: List[Dict], counters: Dict[str, int]):
    hist = tradier_history_daily(ticker, 420)
    counters["scanned"] += 1
    if hist.empty or not daily_trend_ok(hist):
        return
    counters["trend_pass"] += 1
    spot = float(hist["Close"].iloc[-1])
    atr_val = float(atr(hist, SETTINGS.ATR_LEN).iloc[-1])
    today = datetime.now(UTC)
    exps = tradier_expirations(ticker)
    if not exps:
        return

    best = None
    for exp in exps:
        cand = pick_best_call_from_chain(ticker, exp, today)
        if cand:
            best = cand
            break

    if not best:
        return
    counters["viable"] += 1

    existing = any(p["contract"] == best.contract for p in db_get_picks())
    if not existing:
        limit_price = round(min(best.ask, best.mid * 1.02), 2)
        order = tradier_place_option_order(best.contract, "buy_to_open", SETTINGS.ORDER_QTY, "limit", limit_price)
        print(f"BUY {ticker} {best.contract} Δ={best.delta:.2f} @{limit_price:.2f} → {json.dumps(order)}")
        db_insert_pick({
            "ticker": ticker,
            "contract": best.contract,
            "expiry": best.expiry,
            "strike": best.strike,
            "delta": best.delta,
            "entry_underlying": spot,
            "entry_option": best.mid,
            "highest_close": spot,
            "trail": chandelier_trail(spot, atr_val),
            "peak_option": best.mid,
            "created_at": datetime.now(UTC).isoformat(),
        })
        counters["buys"] += 1


def run_once():
    db_init()
    if not SETTINGS.TRADIER_TOKEN:
        print("[ERROR] Missing TRADIER_TOKEN")
        return
    symbols = get_universe_symbols()
    print(f"[INFO] Loaded {len(symbols)} S&P 500 tickers")

    counters = {"scanned": 0, "trend_pass": 0, "viable": 0, "buys": 0}
    positions = tradier_positions() if not SETTINGS.DRY_RUN else []

    for t in symbols:
        try:
            process_ticker(t, positions, counters)
        except Exception as e:
            print(f"[ERR] {t}: {e}")

    print(f"[SUMMARY] scanned={counters['scanned']} trend_pass={counters['trend_pass']} "
          f"viable={counters['viable']} buys={counters['buys']}")


if __name__ == "__main__":
    run_once()
