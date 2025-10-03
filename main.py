# =========================================
# OptionAdvisor Free (Yahoo/yfinance) - Railway Ready
# - Advisor-only: prints BUY and SELL signals, no auto-trading
# - Data source: yfinance (free). Chains + IV are scraped from Yahoo Finance.
# - Entry (per underlying): daily trend OK + 30-60 DTE calls + delta 0.6-0.8 + liquidity
# - Exits (no fixed TP): Chandelier ATR trailing stop (underlying) OR option peak drawdown
# - State: SQLite (open picks: track trail/peaks)
# =========================================

import os
import math
import time
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional, Tuple, Dict

import pandas as pd
import yfinance as yf

# ---------------- Settings ----------------
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

def _get_csv(name, default_list):
    v = os.getenv(name)
    if not v: return list(default_list)
    return [x.strip().upper() for x in v.split(",") if x.strip()]

class SETTINGS:
    # Universe of underlyings (liquid only!)
    TICKERS = _get_csv("TICKERS", ["SPY","QQQ","AAPL","MSFT","NVDA","META","AMZN"])

    # Contract selection
    DTE_MIN = _get_int("DTE_MIN", 30)
    DTE_MAX = _get_int("DTE_MAX", 60)
    DELTA_MIN = _get_float("DELTA_MIN", 0.60)
    DELTA_MAX = _get_float("DELTA_MAX", 0.80)
    OI_MIN   = _get_int("OI_MIN", 200)              # minimum open interest
    VOL_MIN  = _get_int("VOL_MIN", 50)              # minimum daily volume
    MAX_SPREAD_PCT = _get_float("MAX_SPREAD_PCT", 0.15)  # (ask-bid)/mid <= 15%

    # Entry confirmation on underlying (daily trend filter)
    EMA_FAST = _get_int("EMA_FAST", 20)
    EMA_SLOW = _get_int("EMA_SLOW", 50)
    RSI_LEN  = _get_int("RSI_LEN", 14)

    # Exits (no fixed TP)
    ATR_LEN = _get_int("ATR_LEN", 14)
    TRAIL_K_ATR = _get_float("TRAIL_K_ATR", 3.0)      # Chandelier ATR multiple
    OPT_DD_EXIT = _get_float("OPT_DD_EXIT", 0.25)     # option peak drawdown 25%
    DTE_TIME_STOP = _get_int("DTE_TIME_STOP", 7)      # exit if <= 7 days to expiry

    # Risk-free rate for Greeks (Black-Scholes)
    RISK_FREE = _get_float("RISK_FREE", 0.045)        # 4.5% annualized
    DIV_YIELD = _get_float("DIV_YIELD", 0.0)          # assume 0 unless set

    # State / persistence
    DB_PATH = os.getenv("DB_PATH", "./advisor.db")

    # Runtime
    VERBOSE = _get_bool("VERBOSE", True)

# ------------- Math/Greeks (BS) -------------
def _norm_cdf(x: float) -> float:
    # standard normal CDF via erf
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def bs_delta_call(S, K, T, r, q, sigma) -> float:
    """
    Black-Scholes call delta with continuous div yield q.
    S: spot, K: strike, T: time in years, r: risk-free, q: dividend yield, sigma: volatility
    """
    if S <= 0 or K <= 0 or T <= 0 or sigma <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    return math.exp(-q * T) * _norm_cdf(d1)

# ------------- Indicators -------------
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
    hi, lo, cl = df['High'], df['Low'], df['Close']
    prev = cl.shift(1)
    tr = pd.concat([
        (hi - lo).abs(),
        (hi - prev).abs(),
        (lo - prev).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(n).mean()

def daily_trend_ok(df: pd.DataFrame) -> bool:
    """
    Basic confirmation: EMA20 > EMA50 and RSI > 50 rising.
    """
    if len(df) < max(SETTINGS.EMA_SLOW+5, SETTINGS.RSI_LEN+5):
        return False
    close = df['Close']
    e20, e50 = ema(close, SETTINGS.EMA_FAST), ema(close, SETTINGS.EMA_SLOW)
    r = rsi(close, SETTINGS.RSI_LEN)
    cond1 = e20.iloc[-1] > e50.iloc[-1]
    cond2 = r.iloc[-1] > 50 and r.iloc[-1] >= r.iloc[-2]
    return bool(cond1 and cond2)

# ------------- SQLite state -------------
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

# ------------- Helpers -------------
@dataclass
class OptionCandidate:
    ticker: str
    contract: str
    expiry: str
    strike: float
    mid: float
    delta: float
    dte: int
    bid: float
    ask: float
    iv: float

def pick_best_call(ticker: str, price_now: float, chain_df: pd.DataFrame, today: pd.Timestamp) -> Optional[OptionCandidate]:
    """
    From a chain dataframe (calls), select the best contract per filters.
    Uses Yahoo-provided IV; computes delta via BS for consistency.
    """
    if chain_df is None or chain_df.empty:
        return None

    # required Yahoo columns (yfinance typically provides these)
    required = {'contractSymbol','strike','lastPrice','bid','ask','impliedVolatility','lastTradeDate','inTheMoney','volume','openInterest','expiration'}
    if not required.issubset(chain_df.columns):
        return None

    candidates: List[OptionCandidate] = []
    for _, row in chain_df.iterrows():
        exp = pd.to_datetime(row['expiration'])
        dte = max(0, (exp.normalize() - today.normalize()).days)
        if not (SETTINGS.DTE_MIN <= dte <= SETTINGS.DTE_MAX):
            continue

        bid = float(row['bid'] or 0.0)
        ask = float(row['ask'] or 0.0)
        if bid <= 0 or ask <= 0 or ask < bid:
            continue
        mid = (bid + ask) / 2.0
        spread_pct = (ask - bid) / max(mid, 1e-9)
        if spread_pct > SETTINGS.MAX_SPREAD_PCT:
            continue

        oi = int(row.get('openInterest') or 0)
        vol = int(row.get('volume') or 0)
        if oi < SETTINGS.OI_MIN or vol < SETTINGS.VOL_MIN:
            continue

        K = float(row['strike'])
        iv = float(row.get('impliedVolatility') or 0.0)
        # yfinance gives IV as 0.20 for 20% (already decimal). If it looks like 20.0, convert:
        if iv > 3.0:  # assume percentage entered as 20 rather than 0.20
            iv = iv / 100.0

        T = max(1/365, dte / 365.0)
        delta = bs_delta_call(price_now, K, T, SETTINGS.RISK_FREE, SETTINGS.DIV_YIELD, max(iv, 1e-6))

        if not (SETTINGS.DELTA_MIN <= delta <= SETTINGS.DELTA_MAX):
            continue

        candidates.append(OptionCandidate(
            ticker=ticker,
            contract=str(row['contractSymbol']),
            expiry=exp.date().isoformat(),
            strike=K,
            mid=mid,
            delta=delta,
            dte=dte,
            bid=bid,
            ask=ask,
            iv=iv
        ))

    if not candidates:
        return None

    # Rank: closest to mid-delta range center, then tighter spread, then higher OI (if available)
    target = (SETTINGS.DELTA_MIN + SETTINGS.DELTA_MAX) / 2.0
    candidates.sort(key=lambda c: (abs(c.delta - target), (c.ask - c.bid)/max(c.mid, 1e-9), -c.mid))
    return candidates[0]

def chandelier_trail(highest_close: float, atr_val: float) -> float:
    return highest_close - SETTINGS.TRAIL_K_ATR * atr_val

def update_trailing(highest_close: float, price_close: float, atr_val: float) -> Tuple[float, float]:
    new_high = max(highest_close, price_close)
    return new_high, chandelier_trail(new_high, atr_val)

# ------------- Core run -------------
def process_ticker(ticker: str):
    tk = yf.Ticker(ticker)
    today = pd.Timestamp.utcnow().tz_localize("UTC")

    # Daily history for trend and ATR
    try:
        hist = tk.history(period="1y", interval="1d", auto_adjust=False)
        if hist is None or hist.empty:
            print(f"[WARN] No daily data for {ticker}")
            return
    except Exception as e:
        print(f"[ERR] Daily fetch {ticker}: {e}")
        return

    if not daily_trend_ok(hist):
        return

    price_now = float(hist['Close'].iloc[-1])
    atr_val = float(atr(hist.rename(columns={"High":"High","Low":"Low","Close":"Close"}), SETTINGS.ATR_LEN).iloc[-1])

    # Options expirations
    try:
        exps = tk.options
    except Exception as e:
        print(f"[ERR] Expirations {ticker}: {e}")
        return
    if not exps:
        return

    best_pick: Optional[OptionCandidate] = None
    for exp in exps:
        # Only fetch relevant expiries lazily
        exp_dt = pd.to_datetime(exp)
        dte = max(0, (exp_dt.normalize() - today.normalize()).days)
        if not (SETTINGS.DTE_MIN <= dte <= SETTINGS.DTE_MAX):
            continue
        try:
            chain = tk.option_chain(exp)
            calls = chain.calls.copy()
            if 'expiration' not in calls.columns:
                calls['expiration'] = exp
            pick = pick_best_call(ticker, price_now, calls, today)
            if pick:
                best_pick = pick
                break  # take first valid expiry in window
        except Exception as e:
            if SETTINGS.VERBOSE:
                print(f"[WARN] Option chain fetch {ticker} {exp}: {e}")
            continue

    # If we found a candidate and it isn't already open, emit BUY signal + save state
    if best_pick:
        # Check if already open
        open_now = db_get_picks()
        if any(p['contract'] == best_pick.contract for p in open_now):
            # Already tracking; we'll manage it in the manage step below
            pass
        else:
            # initial trail from chandelier using current ATR/daily close
            highest_close = price_now
            trail = chandelier_trail(highest_close, atr_val)
            # Persist
            db_insert_pick({
                "ticker": ticker,
                "contract": best_pick.contract,
                "expiry": best_pick.expiry,
                "strike": best_pick.strike,
                "delta": best_pick.delta,
                "entry_underlying": price_now,
                "entry_option": best_pick.mid,
                "highest_close": highest_close,
                "trail": trail,
                "peak_option": best_pick.mid,
                "created_at": datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()
            })
            # Signal
            limit_hint = round(best_pick.mid * 1.02, 2)  # +2% over mid as a sanity limit suggestion
            print(f"BUY SIGNAL: {ticker}  |  {best_pick.contract}  exp {best_pick.expiry}  "
                  f"strike {best_pick.strike:.2f}  Δ≈{best_pick.delta:.2f}  IV≈{best_pick.iv:.2%}  "
                  f"mid≈{best_pick.mid:.2f}  suggest limit≤{limit_hint:.2f}")

    # Manage any open picks for this ticker (SELL signals)
    manage_open_picks_for_ticker(ticker, tk, price_now, atr_val)

def manage_open_picks_for_ticker(ticker: str, tk: yf.Ticker, price_now: float, atr_val: float):
    picks = [p for p in db_get_picks() if p['ticker'] == ticker]
    if not picks:
        return

    # Update trailing stop from underlying
    highest_close = None
    trail = None

    for p in picks:
        # refresh underlying trail
        prev_high = float(p['highest_close'])
        new_high, new_trail = update_trailing(prev_high, price_now, atr_val)

        # fetch current option quote (mid)
        try:
            # Extract from contract symbol: e.g., AAPL240118C00160000
            opt_tk = yf.Ticker(p['contract'])
            opt_hist = opt_tk.history(period="1d", interval="1m")  # intraday may be sparse; fallback to fast call
            # if no intraday, try fast .info; yfinance may have 'bid','ask' in .fast_info
            if opt_hist is not None and not opt_hist.empty:
                opt_last = float(opt_hist['Close'].iloc[-1])
                bid = ask = None
                mid_now = opt_last  # crude fallback
            else:
                fi = getattr(opt_tk, "fast_info", None)
                bid = float(getattr(fi, "bid", 0.0) or 0.0) if fi else 0.0
                ask = float(getattr(fi, "ask", 0.0) or 0.0) if fi else 0.0
                if bid > 0 and ask > 0 and ask >= bid:
                    mid_now = (bid + ask) / 2.0
                else:
                    # one more fallback: daily option data
                    daily_opt = opt_tk.history(period="5d", interval="1d")
                    mid_now = float(daily_opt['Close'].iloc[-1]) if daily_opt is not None and not daily_opt.empty else float(p['entry_option'])
        except Exception:
            mid_now = float(p['entry_option'])

        # update peak option price
        peak = max(float(p['peak_option']), mid_now)

        # Exit rules:
        reason = None
        # A) Underlying chandelier stop
        if price_now < new_trail:
            reason = "trail_stop"
        # B) Option peak drawdown
        elif peak > 0 and (mid_now <= peak * (1.0 - SETTINGS.OPT_DD_EXIT)):
            reason = "option_peak_drawdown"
        # C) Time stop (close to expiry)
        else:
            # Parse ISO expiry
            try:
                exp = pd.to_datetime(p['expiry'])
                dte = max(0, (exp.normalize() - pd.Timestamp.utcnow().tz_localize("UTC").normalize()).days)
                if dte <= SETTINGS.DTE_TIME_STOP:
                    reason = "time_stop"
            except Exception:
                pass

        if reason:
            print(f"SELL SIGNAL: {ticker}  |  {p['contract']}  reason={reason}  "
                  f"trail≈{new_trail:.2f}  underlying≈{price_now:.2f}  opt≈{mid_now:.2f}")
            db_delete_pick(p['contract'])
        else:
            # persist updated trail/high/peak
            con = sqlite3.connect(SETTINGS.DB_PATH)
            cur = con.cursor()
            cur.execute("""
                UPDATE picks
                SET highest_close=?, trail=?, peak_option=?
                WHERE contract=?;
            """, (new_high, new_trail, peak, p['contract']))
            con.commit()
            con.close()

# ------------- Runner -------------
def run_once():
    db_init()
    for t in SETTINGS.TICKERS:
        try:
            process_ticker(t)
        except Exception as e:
            if SETTINGS.VERBOSE:
                print(f"[ERR] process {t}: {e}")

if __name__ == "__main__":
    run_once()
    # For Railway Cron, just schedule: python main.py
