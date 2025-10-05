# =========================================
# OptionExecutor (Tradier-only) – S&P 500 Scraper
# - Universe: live scrape S&P 500 symbols from Wikipedia
# - Rotation: MAX_TICKERS_PER_RUN + UNIVERSE_OFFSET
# - Data & Orders: Tradier only (history, chains+greeks, quotes, positions, orders)
# - Entries: daily trend OK; pick 30–60 DTE call, Δ 0.6–0.8, liquid, tight spread
# - Exits: Chandelier ATR trail (underlying), option peak drawdown, time stop
# - Logs: small summary per run
# =========================================

import os
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, UTC
from typing import Dict, List, Optional

import requests
import pandas as pd


# -------------------- Settings --------------------
def _get_bool(name, default):
    v = os.getenv(name)
    if v is None: return default
    return str(v).strip().lower() in ("1","true","yes","y","on")

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
    # Universe control
    MAX_TICKERS_PER_RUN = _get_int("MAX_TICKERS_PER_RUN", 75)   # how many to scan each run
    UNIVERSE_OFFSET     = _get_int("UNIVERSE_OFFSET", 0)        # rotate with cron

    # Tradier API
    TRADIER_TOKEN   = os.getenv("TRADIER_TOKEN", "")
    TRADIER_BASE    = os.getenv("TRADIER_BASE", "https://sandbox.tradier.com")  # sandbox | https://api.tradier.com
    TRADIER_ACCOUNT = os.getenv("TRADIER_ACCOUNT", "")  # required if DRY_RUN=false

    # Selection filters
    DTE_MIN = _get_int("DTE_MIN", 30)
    DTE_MAX = _get_int("DTE_MAX", 60)
    DELTA_MIN = _get_float("DELTA_MIN", 0.60)
    DELTA_MAX = _get_float("DELTA_MAX", 0.80)
    OI_MIN   = _get_int("OI_MIN", 200)
    VOL_MIN  = _get_int("VOL_MIN", 50)
    MAX_SPREAD_PCT = _get_float("MAX_SPREAD_PCT", 0.15)

    # Underlying daily trend (entry confirm)
    EMA_FAST = _get_int("EMA_FAST", 20)
    EMA_SLOW = _get_int("EMA_SLOW", 50)
    RSI_LEN  = _get_int("RSI_LEN", 14)

    # Exits (no fixed TP)
    ATR_LEN = _get_int("ATR_LEN", 14)
    TRAIL_K_ATR = _get_float("TRAIL_K_ATR", 3.0)     # chandelier multiple
    OPT_DD_EXIT = _get_float("OPT_DD_EXIT", 0.25)    # option drawdown from peak (25%)
    DTE_STOP    = _get_int("DTE_STOP", 7)            # force exit when ≤ 7 DTE

    # Position sizing (simple)
    ORDER_QTY = _get_int("ORDER_QTY", 1)             # number of contracts per buy

    # Execution
    DRY_RUN  = _get_bool("DRY_RUN", True)            # true = simulate orders
    DURATION = os.getenv("ORDER_DURATION", "day")    # day | gtc

    # Persistence
    DB_PATH = os.getenv("DB_PATH", "./executor_tradier.db")

    # Logging
    VERBOSE = _get_bool("VERBOSE", True)


# -------------------- HTTP / Tradier --------------------
def _auth_headers() -> Dict[str, str]:
    return {
        "Authorization": f"Bearer {SETTINGS.TRADIER_TOKEN}",
        "Accept": "application/json",
        "User-Agent": "OptionExecutor/1.3"
    }

def tget(path: str, params: Dict = None) -> Dict:
    url = SETTINGS.TRADIER_BASE.rstrip("/") + path
    r = requests.get(url, headers=_auth_headers(), params=params or {}, timeout=25)
    r.raise_for_status()
    try:
        return r.json()
    except Exception:
        return {}

def tpost(path: str, data: Dict) -> Dict:
    url = SETTINGS.TRADIER_BASE.rstrip("/") + path
    r = requests.post(
        url,
        headers={**_auth_headers(), "Content-Type": "application/x-www-form-urlencoded"},
        data=data,
        timeout=25
    )
    try:
        return r.json()
    except Exception:
        return {"raw": r.text, "status_code": r.status_code}

# ---- Market data ----
def tradier_quote(symbols: List[str]) -> Dict[str, Dict]:
    symlist = ",".join(symbols)
    data = tget("/v1/markets/quotes", {"symbols": symlist})
    quotes = {}
    q = (data.get("quotes") or {}).get("quote")
    if q is None: return quotes
    rows = q if isinstance(q, list) else [q]
    for row in rows:
        sym = str(row.get("symbol"))
        quotes[sym] = row
    return quotes

def tradier_history_daily(symbol: str, days: int = 420) -> pd.DataFrame:
    start = (datetime.now(UTC) - timedelta(days=days+10)).date().isoformat()
    data = tget("/v1/markets/history", {"symbol": symbol, "interval": "daily", "start": start})
    hist = (data.get("history") or {}).get("day")
    if not hist: return pd.DataFrame()
    df = pd.DataFrame(hist)
    for col in ("open","high","low","close","volume"):
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["date"] = pd.to_datetime(df["date"])
    # Use only High/Low/Close/Volume
    df = df.rename(columns={"high":"High","low":"Low","close":"Close","volume":"Volume"})
    df = df.set_index("date").sort_index()
    return df

def tradier_expirations(symbol: str) -> List[str]:
    data = tget("/v1/markets/options/expirations", {
        "symbol": symbol, "includeAllRoots": "true", "strikes": "false"
    })
    exps = (data.get("expirations") or {}).get("date")
    if not exps: return []
    return exps if isinstance(exps, list) else [exps]

def tradier_chain(symbol: str, expiration: str) -> pd.DataFrame:
    data = tget("/v1/markets/options/chains", {
        "symbol": symbol, "expiration": expiration, "greeks": "true"
    })
    opts = (data.get("options") or {}).get("option")
    if not opts: return pd.DataFrame()
    df = pd.DataFrame(opts)
    num_cols = ["strike","last","bid","ask","volume","open_interest","delta"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

# ---- Accounts / Positions / Orders ----
def tradier_positions() -> List[Dict]:
    if not SETTINGS.TRADIER_ACCOUNT:
        return []
    data = tget(f"/v1/accounts/{SETTINGS.TRADIER_ACCOUNT}/positions")
    pos = (data.get("positions") or {}).get("position")
    if not pos: return []
    return pos if isinstance(pos, list) else [pos]

def tradier_place_option_order(symbol_occ: str, side: str, qty: int, order_type: str = "limit", price: Optional[float] = None, duration: Optional[str] = None) -> Dict:
    if SETTINGS.DRY_RUN:
        return {"status": "dry_run", "symbol": symbol_occ, "side": side, "qty": qty, "type": order_type, "price": price, "duration": duration or SETTINGS.DURATION}
    if not SETTINGS.TRADIER_ACCOUNT:
        return {"error": "TRADIER_ACCOUNT not set"}
    payload = {
        "class": "option",
        "symbol": symbol_occ,
        "side": side,  # buy_to_open / sell_to_close
        "quantity": str(int(max(1, qty))),
        "type": order_type,
        "duration": duration or SETTINGS.DURATION,
    }
    if order_type == "limit":
        if price is None:
            return {"error": "limit order requires price"}
        payload["price"] = f"{price:.02f}"
    return tpost(f"/v1/accounts/{SETTINGS.TRADIER_ACCOUNT}/orders", payload)


# -------------------- Indicators --------------------
def ema(series: pd.Series, n: int) -> pd.Series:
    return series.ewm(span=n, adjust=False).mean()

def rsi(series: pd.Series, n: int = 14) -> pd.Series:
    delta = series.diff()
    up, down = delta.clip(lower=0), -delta.clip(upper=0)
    roll_up = up.ewm(alpha=1/n, adjust=False).mean()
    roll_down = down.ewm(alpha=1/n, adjust=False).mean()
    rs = roll_up / (roll_down + 1e-12)
    return 100 - 100/(1+rs)

def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    hi, lo, cl = df["High"], df["Low"], df["Close"]
    prev = cl.shift(1)
    tr = pd.concat([(hi - lo).abs(), (hi - prev).abs(), (lo - prev).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()

def daily_trend_ok(df: pd.DataFrame) -> bool:
    if df is None or df.empty:
        return False
    if len(df) < max(SETTINGS.EMA_SLOW+5, SETTINGS.RSI_LEN+5):
        return False
    close = df["Close"]
    e_fast = ema(close, SETTINGS.EMA_FAST)
    e_slow = ema(close, SETTINGS.EMA_SLOW)
    r = rsi(close, SETTINGS.RSI_LEN)
    return bool((e_fast.iloc[-1] > e_slow.iloc[-1]) and (r.iloc[-1] > 50) and (r.iloc[-1] >= r.iloc[-2]))

def chandelier_trail(highest_close: float, atr_val: float) -> float:
    return highest_close - SETTINGS.TRAIL_K_ATR * atr_val


# -------------------- SQLite State --------------------
def db_init():
    con = sqlite3.connect(SETTINGS.DB_PATH)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS picks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker TEXT NOT NULL,
        contract TEXT NOT NULL UNIQUE,
        expiry TEXT NOT NULL,
        strike REAL NOT NULL,
        delta REAL NOT NULL,
        entry_underlying REAL NOT NULL,
        entry_option REAL NOT NULL,
        highest_close REAL NOT NULL,
        trail REAL NOT NULL,
        peak_option REAL NOT NULL,
        created_at TEXT NOT NULL
    );
    """)
    con.commit()
    con.close()

def db_insert_pick(row: Dict):
    con = sqlite3.connect(SETTINGS.DB_PATH)
    cur = con.cursor()
    cur.execute("""
        INSERT OR IGNORE INTO picks
        (ticker, contract, expiry, strike, delta, entry_underlying, entry_option, highest_close, trail, peak_option, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """, (
        row['ticker'], row['contract'], row['expiry'], row['strike'], row['delta'],
        row['entry_underlying'], row['entry_option'], row['highest_close'],
        row['trail'], row['peak_option'], row['created_at']
    ))
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
    con.execute("UPDATE picks SET highest_close=?, trail=?, peak_option=? WHERE contract=?;", (highest_close, trail, peak_option, contract))
    con.commit()
    con.close()


# -------------------- Universe (Wikipedia scrape) --------------------
def get_universe_symbols() -> List[str]:
    """
    Scrape the official S&P 500 component list from Wikipedia.
    Replaces '.' with '-' for OCC/Tradier compatibility (e.g., BRK.B -> BRK-B).
    """
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        tables = pd.read_html(url)  # requires lxml
        df = tables[0]  # first table has the components
        symbols = (
            df["Symbol"]
            .astype(str)
            .str.replace(".", "-", regex=False)
            .str.upper()
            .tolist()
        )
        # Deduplicate while preserving order
        seen = set(); out = []
        for s in symbols:
            if s not in seen:
                out.append(s); seen.add(s)
        return out
    except Exception as e:
        print(f"[WARN] Could not scrape S&P 500 list: {e}")
        # minimal fallback to avoid crashing the run
        return ["AAPL","MSFT","NVDA","AMZN","META"]


# -------------------- Selection / Execution --------------------
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

def pick_best_call_from_chain(ticker: str, expiration: str, today_dt: datetime) -> Optional[OptionCandidate]:
    df = tradier_chain(ticker, expiration)
    if df is None or df.empty:
        return None
    calls = df[df.get("option_type") == "call"].copy()
    if calls.empty:
        return None

    # Some expiries may lack delta (esp. sandbox/illiquid)
    if "delta" not in calls.columns:
        if SETTINGS.VERBOSE:
            print(f"[INFO] skip {ticker} {expiration}: no delta in chain")
        return None

    # DTE window
    try:
        exp_dt = datetime.fromisoformat(expiration)
    except Exception:
        exp_dt = datetime.strptime(expiration, "%Y-%m-%d")
    dte = max(0, (exp_dt.date() - today_dt.date()).days)
    if dte < SETTINGS.DTE_MIN or dte > SETTINGS.DTE_MAX:
        return None

    # numeric + liquidity
    for c in ["bid","ask","volume","open_interest","delta","strike"]:
        if c in calls.columns:
            calls[c] = pd.to_numeric(calls[c], errors="coerce")
    calls["mid"] = (calls["bid"] + calls["ask"]) / 2.0
    calls = calls[(calls["bid"] > 0) & (calls["ask"] > 0) & (calls["ask"] >= calls["bid"])]
    calls["spread_pct"] = (calls["ask"] - calls["bid"]) / calls["mid"].clip(lower=1e-9)
    calls = calls[
        (calls["open_interest"] >= SETTINGS.OI_MIN) &
        (calls["volume"] >= SETTINGS.VOL_MIN) &
        (calls["spread_pct"] <= SETTINGS.MAX_SPREAD_PCT) &
        (calls["delta"] >= SETTINGS.DELTA_MIN) &
        (calls["delta"] <= SETTINGS.DELTA_MAX)
    ]
    if calls.empty:
        return None

    target = 0.5 * (SETTINGS.DELTA_MIN + SETTINGS.DELTA_MAX)
    calls["delta_rank"] = (calls["delta"] - target).abs()
    calls = calls.sort_values(by=["delta_rank", "spread_pct", "mid"], ascending=[True, True, False])
    best = calls.iloc[0]

    return OptionCandidate(
        ticker=ticker,
        contract=str(best.get("symbol")),
        expiry=expiration,
        strike=float(best.get("strike")),
        bid=float(best.get("bid")),
        ask=float(best.get("ask")),
        mid=float(best.get("mid")),
        delta=float(best.get("delta")),
        dte=dte
    )

def already_holding_contract(positions: List[Dict], occ_symbol: str) -> bool:
    for p in positions:
        sym = str(p.get("symbol") or "")
        if sym == occ_symbol and float(p.get("quantity") or 0) > 0:
            return True
    return False


# -------------------- Management (exits) --------------------
def manage_open_picks_for_ticker(ticker: str, spot_now: float, atr_val: float, positions: List[Dict], counters: Dict[str, int]):
    picks = [p for p in db_get_picks() if p["ticker"] == ticker]
    if not picks: return

    for p in picks:
        prev_high = float(p["highest_close"])
        new_high = max(prev_high, spot_now)
        new_trail = chandelier_trail(new_high, atr_val)

        # option mid from quotes
        q = tradier_quote([p["contract"]]).get(p["contract"], {})
        bid = float(q.get("bid") or 0.0)
        ask = float(q.get("ask") or 0.0)
        mid_now = (bid + ask) / 2.0 if (bid > 0 and ask > 0 and ask >= bid) else float(p["entry_option"])
        new_peak = max(float(p["peak_option"]), mid_now)

        # Exit logic
        reason = None
        if spot_now < new_trail:
            reason = "trail_stop"
        elif new_peak > 0 and mid_now <= new_peak * (1.0 - SETTINGS.OPT_DD_EXIT):
            reason = "option_peak_drawdown"
        else:
            # time stop
            try:
                exp_dt = datetime.fromisoformat(p["expiry"])
            except Exception:
                exp_dt = datetime.strptime(p["expiry"], "%Y-%m-%d")
            dte = max(0, (exp_dt.date() - datetime.now(UTC).date()).days)
            if dte <= SETTINGS.DTE_STOP:
                reason = "time_stop"

        if reason:
            if already_holding_contract(positions, p["contract"]):
                order = tradier_place_option_order(
                    symbol_occ=p["contract"],
                    side="sell_to_close",
                    qty=SETTINGS.ORDER_QTY,
                    order_type="market",
                    price=None
                )
                print(f"SELL {ticker} | {p['contract']} | reason={reason} | mid≈{mid_now:.2f} | resp={json.dumps(order)}")
                counters["sells"] += 1
            else:
                print(f"SELL SIGNAL (no position) {ticker} | {p['contract']} | reason={reason} | mid≈{mid_now:.2f}")
            db_delete_pick(p["contract"])
        else:
            db_update_pick(p["contract"], new_high, new_trail, new_peak)


# -------------------- Core per-ticker flow --------------------
def process_ticker(ticker: str, positions: List[Dict], counters: Dict[str, int]):
    # 1) daily history
    hist = tradier_history_daily(ticker, days=420)
    counters["scanned"] += 1
    if hist is None or hist.empty:
        return
    # trend confirm
    if not daily_trend_ok(hist):
        return
    counters["trend_pass"] += 1

    spot_now = float(hist["Close"].iloc[-1])
    atr_val = float(atr(hist, SETTINGS.ATR_LEN).iloc[-1])

    # 2) expirations
    exps = tradier_expirations(ticker)
    if not exps:
        return

    # 3) choose first good call
    today_utc = datetime.now(UTC)
    best_pick = None
    viable_chain = False
    for exp in exps:
        # quick DTE check before fetching
        try:
            exp_dt = datetime.fromisoformat(exp)
        except Exception:
            exp_dt = datetime.strptime(exp, "%Y-%m-%d")
        dte = max(0, (exp_dt.date() - today_utc.date()).days)
        if dte < SETTINGS.DTE_MIN or dte > SETTINGS.DTE_MAX:
            continue

        cand = pick_best_call_from_chain(ticker, exp, today_utc)
        if cand:
            viable_chain = True
            best_pick = cand
            break
    if viable_chain:
        counters["viable_options"] += 1

    # 4) BUY if candidate & not already held/tracked
    if best_pick:
        in_db = any(p["contract"] == best_pick.contract for p in db_get_picks())
        have_pos = already_holding_contract(positions, best_pick.contract)
        if not have_pos and not in_db:
            limit_price = round(min(best_pick.ask, best_pick.mid * 1.02), 2)
            order = tradier_place_option_order(
                symbol_occ=best_pick.contract,
                side="buy_to_open",
                qty=SETTINGS.ORDER_QTY,
                order_type="limit",
                price=limit_price,
                duration=SETTINGS.DURATION
            )
            print(f"BUY {ticker} | {best_pick.contract} | Δ≈{best_pick.delta:.2f} | "
                  f"limit≤{limit_price:.2f} | resp={json.dumps(order)}")
            counters["buys"] += 1

            highest_close = spot_now
            trail = chandelier_trail(highest_close, atr_val)
            db_insert_pick({
                "ticker": ticker,
                "contract": best_pick.contract,
                "expiry": best_pick.expiry,
                "strike": best_pick.strike,
                "delta": best_pick.delta,
                "entry_underlying": spot_now,
                "entry_option": best_pick.mid,
                "highest_close": highest_close,
                "trail": trail,
                "peak_option": best_pick.mid,
                "created_at": datetime.now(UTC).isoformat()
            })

    # 5) Manage exits
    manage_open_picks_for_ticker(ticker, spot_now, atr_val, positions, counters)


# -------------------- Runner --------------------
def run_once():
    # validate env
    missing = []
    if not SETTINGS.TRADIER_TOKEN: missing.append("TRADIER_TOKEN")
    if not SETTINGS.TRADIER_ACCOUNT and not SETTINGS.DRY_RUN: missing.append("TRADIER_ACCOUNT")
    if missing:
        print("[ERROR] missing env:", ", ".join(missing))
        return

    db_init()

    # Build universe (from Wikipedia)
    universe = get_universe_symbols()
    total_universe = len(universe)

    # Rotation window
    if SETTINGS.MAX_TICKERS_PER_RUN > 0:
        start = (SETTINGS.UNIVERSE_OFFSET % max(1, total_universe))
        end = min(start + SETTINGS.MAX_TICKERS_PER_RUN, total_universe)
        batch = universe[start:end]
    else:
        batch = universe

    counters = {
        "scanned": 0,
        "trend_pass": 0,
        "viable_options": 0,
        "buys": 0,
        "sells": 0,
        "universe": total_universe,
        "batch": len(batch),
        "offset": SETTINGS.UNIVERSE_OFFSET
    }

    positions = tradier_positions() if not SETTINGS.DRY_RUN else []

    for t in batch:
        try:
            process_ticker(t, positions, counters)
        except Exception as e:
            if SETTINGS.VERBOSE:
                print(f"[ERR] {t}: {e}")

    # Small summary log
    print(f"[SUMMARY] universe={counters['universe']} batch={counters['batch']} "
          f"offset={counters['offset']} scanned={counters['scanned']} "
          f"trend_pass={counters['trend_pass']} viable_options={counters['viable_options']} "
          f"buys={counters['buys']} sells={counters['sells']}")

if __name__ == "__main__":
    run_once()
    # For perpetual mode, uncomment:
    # import time; 
    # while True: run_once(); time.sleep(900)
