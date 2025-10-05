# ==========================================================
# OptionExecutor v1.5  —  Tradier Paper-Trading, Full Auto
# ==========================================================

import os, json, sqlite3, time, requests, pandas as pd
from datetime import datetime, timedelta, UTC
from dataclasses import dataclass
from typing import Dict, List, Optional

# -------------------- Settings --------------------
def _b(name, d): v=os.getenv(name);return d if v is None else v.lower() in ("1","true","yes","y","on")
def _f(name, d): v=os.getenv(name); 
    try: return float(v) if v else d
    except: return d
def _i(name, d): v=os.getenv(name);
    try: return int(v) if v else d
    except: return d

class S:
    TRADIER_TOKEN=os.getenv("TRADIER_TOKEN","")
    TRADIER_BASE=os.getenv("TRADIER_BASE","https://sandbox.tradier.com") # use https://api.tradier.com for live
    TRADIER_ACCOUNT=os.getenv("TRADIER_ACCOUNT","")
    DRY_RUN=_b("DRY_RUN",False)
    DURATION=os.getenv("ORDER_DURATION","day")
    DTE_MIN,DTE_MAX=_i("DTE_MIN",30),_i("DTE_MAX",60)
    DELTA_MIN,DELTA_MAX=_f("DELTA_MIN",0.6),_f("DELTA_MAX",0.8)
    OI_MIN,VOL_MIN=_i("OI_MIN",200),_i("VOL_MIN",50)
    MAX_SPREAD=_f("MAX_SPREAD_PCT",0.15)
    EMA_FAST,EMA_SLOW,RSI_LEN=_i("EMA_FAST",20),_i("EMA_SLOW",50),_i("RSI_LEN",14)
    ATR_LEN,TRAIL_K,OPT_DD,DTE_STOP=_i("ATR_LEN",14),_f("TRAIL_K_ATR",3),_f("OPT_DD_EXIT",0.25),_i("DTE_STOP",7)
    QTY=_i("ORDER_QTY",1)
    DB=os.getenv("DB_PATH","./executor_tradier.db")
    API_DELAY=_f("API_DELAY",1.0)
    VERBOSE=_b("VERBOSE",True)

# -------------------- HTTP wrappers --------------------
def _hdr():return{"Authorization":f"Bearer {S.TRADIER_TOKEN}","Accept":"application/json"}
def _get(p,params=None):
    u=S.TRADIER_BASE.rstrip("/")+p
    try:r=requests.get(u,headers=_hdr(),params=params or {},timeout=25);r.raise_for_status();j=r.json()
    except Exception as e: print(f"[WARN] GET {p}: {e}");j={}
    time.sleep(S.API_DELAY);return j
def _post(p,data):
    u=S.TRADIER_BASE.rstrip("/")+p
    try:r=requests.post(u,headers={**_hdr(),"Content-Type":"application/x-www-form-urlencoded"},data=data,timeout=25);j=r.json()
    except Exception as e: print(f"[WARN] POST {p}: {e}");j={}
    time.sleep(S.API_DELAY);return j

# -------------------- Tradier helpers --------------------
def quote(sym):d=_get("/v1/markets/quotes",{"symbols":sym});q=(d.get("quotes")or{}).get("quote");return q
def hist(sym,days=420):
    start=(datetime.now(UTC)-timedelta(days=days+10)).date().isoformat()
    d=_get("/v1/markets/history",{"symbol":sym,"interval":"daily","start":start})
    h=(d.get("history")or{}).get("day"); 
    if not h:return pd.DataFrame()
    df=pd.DataFrame(h);df["date"]=pd.to_datetime(df["date"])
    for c in["open","high","low","close","volume"]:df[c]=pd.to_numeric(df[c],errors="coerce")
    return df.rename(columns={"high":"High","low":"Low","close":"Close","volume":"Volume"}).set_index("date").sort_index()
def expirations(sym):
    d=_get("/v1/markets/options/expirations",{"symbol":sym,"includeAllRoots":"true","strikes":"false"})
    e=(d.get("expirations")or{}).get("date"); 
    return e if isinstance(e,list) else ([e] if e else [])
def chain(sym,exp):
    d=_get("/v1/markets/options/chains",{"symbol":sym,"expiration":exp,"greeks":"true"})
    o=(d.get("options")or{}).get("option"); 
    if not o:return pd.DataFrame()
    df=pd.DataFrame(o)
    for c in["strike","bid","ask","volume","open_interest","delta"]:
        if c in df.columns:df[c]=pd.to_numeric(df[c],errors="coerce")
    return df
def positions():
    if not S.TRADIER_ACCOUNT:return []
    d=_get(f"/v1/accounts/{S.TRADIER_ACCOUNT}/positions")
    p=(d.get("positions")or{}).get("position")
    return p if isinstance(p,list) else ([p] if p else [])
def order(sym,side,qty,otype="limit",price=None):
    if S.DRY_RUN:return{"status":"dry_run","sym":sym,"side":side,"qty":qty,"price":price}
    if not S.TRADIER_ACCOUNT:return{"error":"no account"}
    data={"class":"option","symbol":sym,"side":side,"quantity":str(qty),"type":otype,"duration":S.DURATION}
    if otype=="limit"and price:data["price"]=f"{price:.2f}"
    return _post(f"/v1/accounts/{S.TRADIER_ACCOUNT}/orders",data)

# -------------------- Indicators --------------------
def ema(s,n):return s.ewm(span=n,adjust=False).mean()
def rsi(s,n=14):
    d=s.diff();u,dn=d.clip(lower=0),-d.clip(upper=0)
    rs=u.ewm(alpha=1/n,adjust=False).mean()/ (dn.ewm(alpha=1/n,adjust=False).mean()+1e-9)
    return 100-100/(1+rs)
def atr(df,n=14):
    h,l,c=df["High"],df["Low"],df["Close"];p=c.shift(1)
    tr=pd.concat([(h-l).abs(),(h-p).abs(),(l-p).abs()],axis=1).max(axis=1)
    return tr.rolling(n).mean()
def chandelier(high,atrv):return high-S.TRAIL_K*atrv
def trend_ok(df):
    if df.empty or len(df)<max(S.EMA_SLOW+5,S.RSI_LEN+5):return False
    c=df["Close"];ef,es=ema(c,S.EMA_FAST),ema(c,S.EMA_SLOW);r=rsi(c,S.RSI_LEN)
    return (ef.iloc[-1]>es.iloc[-1])and(r.iloc[-1]>50)and(r.iloc[-1]>=r.iloc[-2])

# -------------------- DB --------------------
def db_init():
    con=sqlite3.connect(S.DB);c=con.cursor()
    c.execute("""CREATE TABLE IF NOT EXISTS picks(
      ticker TEXT,contract TEXT PRIMARY KEY,expiry TEXT,strike REAL,delta REAL,
      entry_underlying REAL,entry_option REAL,highest_close REAL,trail REAL,
      peak_option REAL,created_at TEXT)""");con.commit();con.close()
def db_all():
    con=sqlite3.connect(S.DB);con.row_factory=sqlite3.Row
    r=[dict(x) for x in con.execute("SELECT * FROM picks")]
    con.close();return r
def db_upd(c,h,t,p):
    con=sqlite3.connect(S.DB);con.execute("UPDATE picks SET highest_close=?,trail=?,peak_option=? WHERE contract=?",(h,t,p,c))
    con.commit();con.close()
def db_add(r):
    con=sqlite3.connect(S.DB);con.execute("INSERT OR IGNORE INTO picks VALUES(?,?,?,?,?,?,?,?,?,?,?)",r);con.commit();con.close()
def db_del(c):
    con=sqlite3.connect(S.DB);con.execute("DELETE FROM picks WHERE contract=?",(c,));con.commit();con.close()

# -------------------- Universe --------------------
def universe():
    u="https://raw.githubusercontent.com/datasets/s-and-p-500-companies/main/data/constituents.csv"
    try:
        df=pd.read_csv(u)
        return df["Symbol"].astype(str).str.replace(".","-",regex=False).str.upper().drop_duplicates().tolist()
    except Exception as e:
        print(f"[WARN] universe load failed {e}")
        return["AAPL","MSFT","NVDA"]

# -------------------- Strategy --------------------
@dataclass
class Pick: ticker:str;contract:str;expiry:str;strike:float;bid:float;ask:float;mid:float;delta:float;dte:int
def pick_call(t,e,today):
    df=chain(t,e)
    if df.empty or "delta"not in df:return None
    df=df[df.option_type=="call"].copy()
    exp=datetime.fromisoformat(e);dte=(exp.date()-today.date()).days
    if not(S.DTE_MIN<=dte<=S.DTE_MAX):return None
    for c in["bid","ask","volume","open_interest","delta","strike"]:df[c]=pd.to_numeric(df[c],errors="coerce")
    df["mid"]=(df["bid"]+df["ask"])/2;df["spr"]=(df["ask"]-df["bid"])/df["mid"].clip(1e-9)
    df=df[(df.open_interest>=S.OI_MIN)&(df.volume>=S.VOL_MIN)&(df.spr<=S.MAX_SPREAD)&df.delta.between(S.DELTA_MIN,S.DELTA_MAX)]
    if df.empty:return None
    target=0.5*(S.DELTA_MIN+S.DELTA_MAX);df["drank"]=(df["delta"]-target).abs()
    b=df.sort_values(["drank","spr"]).iloc[0]
    return Pick(t,str(b.symbol),e,float(b.strike),float(b.bid),float(b.ask),float(b.mid),float(b.delta),dte)

# -------------------- Core --------------------
def manage_open(t,spot,atrv,positions,cnt):
    for p in [x for x in db_all() if x["ticker"]==t]:
        hi=max(p["highest_close"],spot);trail=chandelier(hi,atrv)
        q=quote(p["contract"])or{};bid=float(q.get("bid",0));ask=float(q.get("ask",0))
        mid=(bid+ask)/2 if bid>0 and ask>0 else p["entry_option"]
        peak=max(p["peak_option"],mid)
        reason=None
        if spot<trail:reason="trail"
        elif mid<=peak*(1-S.OPT_DD):reason="drawdown"
        else:
            exp=datetime.fromisoformat(p["expiry"])
            if (exp.date()-datetime.now(UTC).date()).days<=S.DTE_STOP:reason="time"
        if reason:
            print(f"SELL {t} {p['contract']} reason={reason}")
            order(p["contract"],"sell_to_close",S.QTY,"market")
            db_del(p["contract"]);cnt["sells"]+=1
        else:db_upd(p["contract"],hi,trail,peak)

def process(t,positions,cnt):
    df=hist(t,420);cnt["scan"]+=1
    if df.empty or not trend_ok(df):return
    cnt["trend"]+=1;spot=float(df["Close"].iloc[-1]);a=float(atr(df,S.ATR_LEN).iloc[-1])
    today=datetime.now(UTC)
    for e in expirations(t):
        c=pick_call(t,e,today)
        if c:
            cnt["viable"]+=1
            limit=round(min(c.ask,c.mid*1.02),2)
            print(f"BUY {t} {c.contract} Δ={c.delta:.2f} @{limit}")
            order(c.contract,"buy_to_open",S.QTY,"limit",limit)
            db_add((t,c.contract,c.expiry,c.strike,c.delta,spot,c.mid,spot,chandelier(spot,a),c.mid,datetime.now(UTC).isoformat()))
            cnt["buys"]+=1;break
    manage_open(t,spot,a,positions,cnt)

# -------------------- Runner --------------------
def run_once():
    if not S.TRADIER_TOKEN:return print("[ERROR] Missing TRADIER_TOKEN")
    db_init();syms=universe();print(f"[INFO] universe {len(syms)} tickers")
    cnt={"scan":0,"trend":0,"viable":0,"buys":0,"sells":0}
    pos=positions() if not S.DRY_RUN else []
    for t in syms:
        try:process(t,pos,cnt)
        except Exception as e:print(f"[ERR] {t}: {e}")
    print(f"[SUMMARY] scanned={cnt['scan']} trend={cnt['trend']} viable={cnt['viable']} buys={cnt['buys']} sells={cnt['sells']}")

if __name__=="__main__": run_once()
