"""
compile_holdings.py — NEPSE Holdings Compiler (Memory-Efficient + Self-Contained HTML)
- Processes ONE file at a time to avoid memory issues
- Embeds JSON data directly into index.html so double-click works (no server needed)

Run: python compile_holdings.py
Place all daily Excel files inside ./data/ folder first.
"""

import os, json, glob, time
import pandas as pd
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────────────────
DATA_DIR     = "./data"
OUTPUT_JSON  = "holdings_summary.json"   # also kept as backup
OUTPUT_HTML  = "index.html"              # dashboard — open by double-click
IPO_QTY      = 10
# ─────────────────────────────────────────────────────────────────────────

API_COL_MAP = {
    "businessDate"    : "Date",
    "stockSymbol"     : "Stock Symbol",
    "buyerMemberId"   : "Buyer",
    "sellerMemberId"  : "Seller",
    "contractQuantity": "Quantity",
    "contractRate"    : "Rate (Rs)",
    "contractAmount"  : "Amount (Rs)",
    "buyerBrokerName" : "BuyerName",
    "sellerBrokerName": "SellerName",
    "securityName"    : "Security Name",
}

def clean_col(c):
    return str(c).strip().replace('\xa0','').replace('\u00a0','').replace('\u200b','').strip()

def read_and_normalise(filepath):
    fname = os.path.basename(filepath)
    try:
        try:
            df = pd.read_excel(filepath, sheet_name="MASTER ", engine="openpyxl")
        except Exception:
            df = pd.read_excel(filepath, sheet_name=0, engine="openpyxl")

        df.columns = [clean_col(c) for c in df.columns]

        if "contractQuantity" in df.columns:
            df = df.rename(columns=API_COL_MAP)
            fmt = "API"
        elif "Quantity" in df.columns and "Buyer" in df.columns:
            if "BROKER #" in df.columns:
                df = df[df["BROKER #"].isna()].copy()
            fmt = "MASTER"
        else:
            print(f"  SKIP {fname} — unknown format")
            return None, None

        required = ["Date","Stock Symbol","Buyer","Seller","Quantity","Amount (Rs)"]
        missing  = [c for c in required if c not in df.columns]
        if missing:
            print(f"  SKIP {fname} — missing: {missing}")
            return None, None

        df["Quantity"]     = pd.to_numeric(df["Quantity"],    errors="coerce").fillna(0)
        df["Amount (Rs)"]  = pd.to_numeric(df["Amount (Rs)"], errors="coerce").fillna(0)
        df["Buyer"]        = pd.to_numeric(df["Buyer"],       errors="coerce").fillna(0).astype(int)
        df["Seller"]       = pd.to_numeric(df["Seller"],      errors="coerce").fillna(0).astype(int)
        df["Date"]         = pd.to_datetime(df["Date"],       errors="coerce").dt.strftime("%Y-%m-%d")
        df["Stock Symbol"] = df["Stock Symbol"].astype(str).str.strip()

        meta = {}
        if "BuyerName" in df.columns:
            meta["broker_names"] = (df[["Buyer","BuyerName"]].drop_duplicates()
                                      .rename(columns={"Buyer":"broker","BuyerName":"broker_name"})
                                      .dropna(subset=["broker_name"]))
        if "SellerName" in df.columns:
            sn = (df[["Seller","SellerName"]].drop_duplicates()
                    .rename(columns={"Seller":"broker","SellerName":"broker_name"})
                    .dropna(subset=["broker_name"]))
            meta["broker_names"] = pd.concat(
                [meta.get("broker_names", pd.DataFrame(columns=["broker","broker_name"])), sn]
            ).drop_duplicates(subset=["broker"])
        if "Security Name" in df.columns:
            meta["security_names"] = df[["Stock Symbol","Security Name"]].drop_duplicates().dropna()

        return df, meta
    except Exception as e:
        print(f"  ERR  {fname} — {e}")
        return None, None


def aggregate_one_file(df):
    grp = ["Date","Stock Symbol"]
    buy = (df.groupby(grp+["Buyer"])
             .agg(buy_qty=("Quantity","sum"), buy_amt=("Amount (Rs)","sum"))
             .reset_index().rename(columns={"Buyer":"broker"}))
    sell_all = (df.groupby(grp+["Seller"])
                  .agg(total_sale_qty=("Quantity","sum"), sales_amt=("Amount (Rs)","sum"))
                  .reset_index().rename(columns={"Seller":"broker"}))
    sell_ipo = (df[df["Quantity"]==IPO_QTY]
                  .groupby(grp+["Seller"])
                  .agg(ipo_sale_qty=("Quantity","sum"))
                  .reset_index().rename(columns={"Seller":"broker"}))
    sell = sell_all.merge(sell_ipo, on=grp+["broker"], how="left")
    sell["ipo_sale_qty"]  = sell["ipo_sale_qty"].fillna(0)
    sell["bulk_sale_qty"] = sell["total_sale_qty"] - sell["ipo_sale_qty"]
    h = buy.merge(sell, on=grp+["broker"], how="outer").fillna(0)
    h["holding_qty"] = h["buy_qty"] - h["bulk_sale_qty"]
    h["avg_rate"]    = (h["buy_amt"]/h["buy_qty"]).where(h["buy_qty"]>0,0).round(2)
    h["broker"]      = h["broker"].astype(int)
    return h


def build_summary(h, cumul, broker_names, sec_names, all_symbols, all_dates):
    h     = h.merge(broker_names, on="broker", how="left")
    h["broker_name"]   = h.get("broker_name", pd.Series(dtype=str)).fillna("")
    h     = h.merge(sec_names, on="Stock Symbol", how="left")
    h["Security Name"] = h.get("Security Name", pd.Series(dtype=str)).fillna("")

    cumul = cumul.merge(broker_names, on="broker", how="left")
    cumul["broker_name"]   = cumul.get("broker_name",   pd.Series(dtype=str)).fillna("")
    cumul = cumul.merge(sec_names, on="Stock Symbol", how="left")
    cumul["Security Name"] = cumul.get("Security Name", pd.Series(dtype=str)).fillna("")
    cumul = cumul.sort_values("net_holding", ascending=False)

    top = {}
    for sym in sorted(all_symbols):
        rows = cumul[cumul["Stock Symbol"]==sym].nlargest(10,"net_holding")
        top[sym] = rows[["broker","broker_name","net_holding",
                          "total_buy_qty","total_bulk_qty","total_ipo_qty"]].to_dict(orient="records")

    recs = h[h["holding_qty"]!=0].copy()
    rec_cols = [c for c in ["Date","Stock Symbol","Security Name","broker","broker_name",
                             "buy_qty","total_sale_qty","ipo_sale_qty","bulk_sale_qty",
                             "holding_qty","avg_rate"] if c in recs.columns]
    cum_cols = [c for c in ["Stock Symbol","Security Name","broker","broker_name",
                             "total_buy_qty","total_sale_qty","total_ipo_qty",
                             "total_bulk_qty","net_holding","avg_rate"] if c in cumul.columns]
    return {
        "generated_at" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_records": len(recs),
        "symbols"      : sorted(all_symbols),
        "dates"        : sorted(all_dates),
        "top_brokers"  : top,
        "holdings"     : recs[rec_cols].to_dict(orient="records"),
        "cumulative"   : cumul[cum_cols].to_dict(orient="records"),
    }


def write_html(summary):
    """Write index.html with JSON data embedded directly — works with double-click."""
    json_str = json.dumps(summary, default=str)

    html = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NEPSE Holdings Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<style>
:root{--bg:#060a12;--s1:#0d1420;--s2:#111d2e;--border:#1e3048;--border2:#264060;--cyan:#00c8ff;--cyan2:#0090c8;--green:#00e5a0;--red:#ff4d6a;--amber:#ffb830;--purple:#a855f7;--text:#cdd9e5;--muted:#4a6480;--muted2:#2a3f55;--mono:'IBM Plex Mono',monospace;--sans:'Outfit',sans-serif}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh;overflow-x:hidden}
body::after{content:'';position:fixed;inset:0;background:repeating-linear-gradient(0deg,transparent,transparent 2px,rgba(0,0,0,.04) 2px,rgba(0,0,0,.04) 4px);pointer-events:none;z-index:9999}
.wrap{max-width:1440px;margin:0 auto;padding:0 28px}
header{padding:16px 0;border-bottom:1px solid var(--border);position:sticky;top:0;background:rgba(6,10,18,.94);backdrop-filter:blur(12px);z-index:100}
.hdr{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap}
.brand{display:flex;align-items:center;gap:14px}
.brand-icon{width:38px;height:38px;border-radius:8px;background:linear-gradient(135deg,var(--cyan),var(--purple));display:flex;align-items:center;justify-content:center;font-size:18px;flex-shrink:0}
.brand h1{font-family:var(--mono);font-size:14px;color:var(--cyan);letter-spacing:2px;font-weight:600}
.brand p{font-size:11px;color:var(--muted);margin-top:2px}
.hdr-meta{font-family:var(--mono);font-size:10px;color:var(--muted);text-align:right;line-height:1.9}
.hdr-meta b{color:var(--cyan)}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:20px 0}
.sc{background:var(--s1);border:1px solid var(--border);border-radius:10px;padding:16px 18px;position:relative;overflow:hidden;transition:border-color .2s,transform .2s}
.sc:hover{border-color:var(--border2);transform:translateY(-2px)}
.sc::after{content:'';position:absolute;bottom:0;left:0;right:0;height:2px;background:linear-gradient(90deg,var(--cyan),var(--purple));opacity:.5}
.sc-label{font-size:10px;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);margin-bottom:8px}
.sc-val{font-family:var(--mono);font-size:24px;color:var(--cyan);font-weight:600;line-height:1}
.sc-sub{font-size:10px;color:var(--muted);margin-top:5px}
.fp{background:var(--s1);border:1px solid var(--border);border-radius:10px;padding:16px 20px;margin-bottom:18px;display:flex;flex-wrap:wrap;gap:12px;align-items:flex-end}
.fg{display:flex;flex-direction:column;gap:5px;min-width:150px}
.fg label{font-size:10px;text-transform:uppercase;letter-spacing:1.2px;color:var(--muted)}
select,input[type=text]{background:var(--s2);border:1px solid var(--border);border-radius:7px;color:var(--text);font-family:var(--sans);font-size:13px;padding:7px 11px;outline:none;transition:border-color .2s;cursor:pointer;width:100%}
select:focus,input[type=text]:focus{border-color:var(--cyan)}
select option{background:var(--s2)}
.btn{padding:8px 18px;border-radius:7px;border:none;font-family:var(--sans);font-size:13px;font-weight:500;cursor:pointer;transition:all .2s;white-space:nowrap}
.btn-p{background:linear-gradient(135deg,var(--cyan2),var(--purple));color:#fff}
.btn-p:hover{opacity:.85;transform:translateY(-1px)}
.btn-g{background:transparent;border:1px solid var(--border);color:var(--muted)}
.btn-g:hover{border-color:var(--cyan);color:var(--cyan)}
.btns{display:flex;gap:8px;align-items:flex-end}
.tabs{display:flex;gap:2px;margin-bottom:16px;border-bottom:1px solid var(--border)}
.tab{padding:10px 20px;font-size:13px;font-weight:500;color:var(--muted);cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-1px;transition:all .2s;user-select:none}
.tab:hover{color:var(--text)}
.tab.active{color:var(--cyan);border-bottom-color:var(--cyan)}
.tw{background:var(--s1);border:1px solid var(--border);border-radius:10px;overflow:hidden;margin-bottom:18px}
.th2{display:flex;align-items:center;justify-content:space-between;padding:12px 18px;border-bottom:1px solid var(--border);flex-wrap:wrap;gap:8px}
.ttitle{font-size:13px;font-weight:600}
.tcnt{font-family:var(--mono);font-size:11px;color:var(--muted)}
.tscroll{overflow-x:auto}
table{width:100%;border-collapse:collapse;min-width:700px}
thead th{padding:9px 13px;font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);text-align:left;background:var(--s2);cursor:pointer;user-select:none;white-space:nowrap;border-bottom:1px solid var(--border)}
thead th:hover{color:var(--cyan)}
tbody tr{border-bottom:1px solid var(--border);transition:background .12s}
tbody tr:last-child{border-bottom:none}
tbody tr:hover{background:var(--s2)}
td{padding:9px 13px;font-size:13px;white-space:nowrap}
.m{font-family:var(--mono);font-size:12px}
.sym{font-family:var(--mono);font-weight:600;color:var(--cyan);font-size:13px}
.pos{color:var(--green);font-family:var(--mono)}
.neg{color:var(--red);font-family:var(--mono)}
.brk{display:inline-block;background:rgba(0,200,255,.08);border:1px solid rgba(0,200,255,.2);border-radius:4px;padding:2px 7px;font-family:var(--mono);font-size:11px;color:var(--cyan)}
.badge-ipo{display:inline-block;background:rgba(168,85,247,.1);border:1px solid rgba(168,85,247,.2);border-radius:4px;padding:2px 7px;font-family:var(--mono);font-size:11px;color:var(--purple)}
.bname{font-size:11px;color:var(--muted);max-width:140px;overflow:hidden;text-overflow:ellipsis}
.qcell{display:flex;align-items:center;gap:8px}
.qbar{flex:1;height:3px;background:var(--muted2);border-radius:2px;min-width:40px;max-width:80px}
.qfill{height:100%;border-radius:2px}
.qfill.p{background:linear-gradient(90deg,var(--cyan),var(--green))}
.qfill.n{background:linear-gradient(90deg,var(--red),#f97316)}
.pag{display:flex;align-items:center;justify-content:space-between;padding:11px 18px;border-top:1px solid var(--border);font-size:12px;color:var(--muted);flex-wrap:wrap;gap:8px}
.pbtns{display:flex;gap:6px}
.pb{padding:5px 12px;border-radius:6px;border:1px solid var(--border);background:transparent;color:var(--text);font-size:12px;cursor:pointer;transition:all .2s}
.pb:hover:not(:disabled){border-color:var(--cyan);color:var(--cyan)}
.pb:disabled{opacity:.3;cursor:not-allowed}
.cw{background:var(--s1);border:1px solid var(--border);border-radius:10px;padding:22px;margin-bottom:18px}
.ctitle{font-size:14px;font-weight:600;margin-bottom:18px}
.barchart{display:flex;flex-direction:column;gap:12px}
.brow{display:flex;align-items:center;gap:10px}
.brank{font-size:12px;width:26px;text-align:right;flex-shrink:0;color:var(--muted)}
.binfo{width:190px;flex-shrink:0}
.binfo .bn{font-family:var(--mono);font-size:11px;color:var(--cyan)}
.binfo .bnn{font-size:11px;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.btrack{flex:1;height:26px;background:var(--s2);border-radius:5px;overflow:hidden;border:1px solid var(--border)}
.bfill{height:100%;border-radius:4px;background:linear-gradient(90deg,var(--cyan2),var(--cyan));display:flex;align-items:center;padding-left:8px;font-family:var(--mono);font-size:11px;color:#fff;transition:width .7s cubic-bezier(.4,0,.2,1);min-width:3px}
.empty{text-align:center;padding:48px 20px;color:var(--muted)}
@media(max-width:768px){.stats{grid-template-columns:repeat(2,1fr)}.fp{flex-direction:column}.fg{min-width:100%}}
</style>
</head>
<body>
<header>
  <div class="wrap">
    <div class="hdr">
      <div class="brand">
        <div class="brand-icon">📊</div>
        <div><h1>NEPSE · HOLDINGS DASHBOARD</h1><p>Broker-level floorsheet analysis · open by double-click</p></div>
      </div>
      <div class="hdr-meta">Updated: <b id="gen-at">—</b><br>Range: <b id="date-range">—</b></div>
    </div>
  </div>
</header>
<div class="wrap" style="padding-top:22px;padding-bottom:40px">
  <div class="stats">
    <div class="sc"><div class="sc-label">Positions</div><div class="sc-val" id="s-pos">—</div><div class="sc-sub">non-zero holdings</div></div>
    <div class="sc"><div class="sc-label">Stocks</div><div class="sc-val" id="s-sym">—</div><div class="sc-sub">unique symbols</div></div>
    <div class="sc"><div class="sc-label">Trading Days</div><div class="sc-val" id="s-days">—</div><div class="sc-sub">days of data</div></div>
    <div class="sc"><div class="sc-label">Brokers</div><div class="sc-val" id="s-brk">—</div><div class="sc-sub">active in view</div></div>
    <div class="sc"><div class="sc-label">Net Holdings</div><div class="sc-val" id="s-net">—</div><div class="sc-sub">total qty (filtered)</div></div>
    <div class="sc"><div class="sc-label">Total Symbols</div><div class="sc-val" id="s-allsym">—</div><div class="sc-sub">in dataset</div></div>
  </div>
  <div class="fp">
    <div class="fg"><label>Stock Symbol</label><select id="f-sym"><option value="">All Stocks</option></select></div>
    <div class="fg"><label>Broker Number</label><input type="text" id="f-brk" placeholder="e.g. 58"></div>
    <div class="fg"><label>Broker Name</label><input type="text" id="f-bname" placeholder="e.g. Sunrise"></div>
    <div class="fg"><label>Date From</label><select id="f-dfrom"><option value="">Earliest</option></select></div>
    <div class="fg"><label>Date To</label><select id="f-dto"><option value="">Latest</option></select></div>
    <div class="fg"><label>Min Holding Qty</label><input type="text" id="f-minq" placeholder="e.g. 500"></div>
    <div class="fg"><label>Show</label>
      <select id="f-side">
        <option value="all">All positions</option>
        <option value="pos">Positive only</option>
        <option value="neg">Negative only</option>
      </select>
    </div>
    <div class="btns">
      <button class="btn btn-p" onclick="applyFilters()">Apply</button>
      <button class="btn btn-g" onclick="resetFilters()">Reset</button>
    </div>
  </div>
  <div class="tabs">
    <div class="tab active" onclick="showTab('daily')">Daily Holdings</div>
    <div class="tab" onclick="showTab('cumul')">Cumulative by Broker</div>
    <div class="tab" onclick="showTab('topb')">Top Brokers Chart</div>
  </div>
  <div id="tab-daily">
    <div class="tw">
      <div class="th2"><span class="ttitle">Daily Holdings per Broker per Stock</span><span class="tcnt" id="cnt-daily">— rows</span></div>
      <div class="tscroll"><table>
        <thead><tr>
          <th onclick="srt('Date')">Date ↕</th>
          <th onclick="srt('Stock Symbol')">Symbol ↕</th>
          <th onclick="srt('broker')">Broker # ↕</th>
          <th>Broker Name</th>
          <th onclick="srt('buy_qty')">Buy Qty ↕</th>
          <th onclick="srt('total_sale_qty')">Sale Qty ↕</th>
          <th onclick="srt('ipo_sale_qty')">IPO Sale ↕</th>
          <th onclick="srt('bulk_sale_qty')">Bulk Sale ↕</th>
          <th onclick="srt('holding_qty')">Net Holding ↕</th>
          <th onclick="srt('avg_rate')">Avg Rate ↕</th>
        </tr></thead>
        <tbody id="tbody-daily"><tr><td colspan="10"><div class="empty">Loading…</div></td></tr></tbody>
      </table></div>
      <div class="pag">
        <span id="pi-daily">Page 1 of 1</span>
        <div class="pbtns">
          <button class="pb" id="pp-daily" onclick="chpg(-1,'daily')">← Prev</button>
          <button class="pb" id="pn-daily" onclick="chpg(1,'daily')">Next →</button>
        </div>
      </div>
    </div>
  </div>
  <div id="tab-cumul" style="display:none">
    <div class="tw">
      <div class="th2"><span class="ttitle">Cumulative Net Holdings (All Dates)</span><span class="tcnt" id="cnt-cumul">— rows</span></div>
      <div class="tscroll"><table>
        <thead><tr>
          <th>Rank</th>
          <th onclick="csrt('Stock Symbol')">Symbol ↕</th>
          <th onclick="csrt('broker')">Broker # ↕</th>
          <th>Broker Name</th>
          <th onclick="csrt('total_buy_qty')">Total Buy ↕</th>
          <th onclick="csrt('total_sale_qty')">Total Sale ↕</th>
          <th onclick="csrt('total_ipo_qty')">IPO Sale ↕</th>
          <th onclick="csrt('total_bulk_qty')">Bulk Sale ↕</th>
          <th onclick="csrt('net_holding')">Net Holding ↕</th>
          <th onclick="csrt('avg_rate')">Avg Rate ↕</th>
        </tr></thead>
        <tbody id="tbody-cumul"></tbody>
      </table></div>
      <div class="pag">
        <span id="pi-cumul">Page 1 of 1</span>
        <div class="pbtns">
          <button class="pb" id="pp-cumul" onclick="chpg(-1,'cumul')">← Prev</button>
          <button class="pb" id="pn-cumul" onclick="chpg(1,'cumul')">Next →</button>
        </div>
      </div>
    </div>
  </div>
  <div id="tab-topb" style="display:none">
    <div class="cw">
      <div class="ctitle" id="chart-title">Select a stock symbol to see top brokers</div>
      <div class="barchart" id="barchart"><div class="empty">Use the Stock Symbol filter above and click Apply.</div></div>
    </div>
  </div>
</div>
<script>
// ── DATA EMBEDDED BY compile_holdings.py ──────────────────────────────────
const SUMMARY = """ + json_str + """;

// ── STATE ─────────────────────────────────────────────────────────────────
let DAILY=[],CUMUL=[],FD=[],FC=[];
let dCol='holding_qty',dAsc=false,cCol='net_holding',cAsc=false;
let pg={daily:1,cumul:1};
const PS=50;

function init(){
  const d=SUMMARY;
  document.getElementById('gen-at').textContent=d.generated_at||'—';
  const dates=d.dates||[];
  document.getElementById('date-range').textContent=dates.length?`${dates[0]} → ${dates[dates.length-1]}`:'—';
  document.getElementById('s-allsym').textContent=(d.symbols||[]).length;
  const ss=document.getElementById('f-sym');
  (d.symbols||[]).forEach(s=>{const o=new Option(s,s);ss.add(o)});
  ['f-dfrom','f-dto'].forEach(id=>{
    const el=document.getElementById(id);
    dates.forEach(dt=>{const o=new Option(dt,dt);el.add(o)});
  });
  DAILY=d.holdings||[];
  CUMUL=d.cumulative||[];
  FD=[...DAILY];FC=[...CUMUL];
  updStats(FD);renderDaily();renderCumul();
}

function updStats(fd){
  document.getElementById('s-pos').textContent  =fd.length.toLocaleString();
  document.getElementById('s-sym').textContent  =new Set(fd.map(r=>r['Stock Symbol'])).size;
  document.getElementById('s-days').textContent =new Set(fd.map(r=>r.Date)).size;
  document.getElementById('s-brk').textContent  =new Set(fd.map(r=>r.broker)).size;
  document.getElementById('s-net').textContent  =Math.round(fd.reduce((s,r)=>s+(r.holding_qty||0),0)).toLocaleString();
}

function applyFilters(){
  const sym  =document.getElementById('f-sym').value;
  const brk  =document.getElementById('f-brk').value.trim();
  const bname=document.getElementById('f-bname').value.trim().toLowerCase();
  const dfrom=document.getElementById('f-dfrom').value;
  const dto  =document.getElementById('f-dto').value;
  const minq =parseFloat(document.getElementById('f-minq').value)||null;
  const side =document.getElementById('f-side').value;
  FD=DAILY.filter(r=>{
    if(sym   && r['Stock Symbol']!==sym) return false;
    if(brk   && String(r.broker)!==brk) return false;
    if(bname && !(r.broker_name||'').toLowerCase().includes(bname)) return false;
    if(dfrom && r.Date<dfrom) return false;
    if(dto   && r.Date>dto)   return false;
    if(minq!==null && Math.abs(r.holding_qty)<minq) return false;
    if(side==='pos' && r.holding_qty<=0) return false;
    if(side==='neg' && r.holding_qty>=0) return false;
    return true;
  });
  FC=CUMUL.filter(r=>{
    if(sym   && r['Stock Symbol']!==sym) return false;
    if(brk   && String(r.broker)!==brk) return false;
    if(bname && !(r.broker_name||'').toLowerCase().includes(bname)) return false;
    return true;
  });
  pg.daily=1;pg.cumul=1;
  updStats(FD);renderDaily();renderCumul();
  renderChart(sym);
  if(sym) showTab('topb');
}

function resetFilters(){
  ['f-sym','f-dfrom','f-dto','f-side'].forEach(id=>document.getElementById(id).value='');
  ['f-brk','f-bname','f-minq'].forEach(id=>document.getElementById(id).value='');
  FD=[...DAILY];FC=[...CUMUL];pg.daily=1;pg.cumul=1;
  updStats(FD);renderDaily();renderCumul();renderChart('');
}

function doSort(arr,col,asc){
  return [...arr].sort((a,b)=>{
    let va=a[col],vb=b[col];
    if(typeof va==='number') return asc?va-vb:vb-va;
    return asc?String(va||'').localeCompare(String(vb||'')):String(vb||'').localeCompare(String(va||''));
  });
}
function srt(c){if(dCol===c)dAsc=!dAsc;else{dCol=c;dAsc=false;}pg.daily=1;renderDaily();}
function csrt(c){if(cCol===c)cAsc=!cAsc;else{cCol=c;cAsc=false;}pg.cumul=1;renderCumul();}
const fmt=n=>n==null?'—':Number(n).toLocaleString();
const fmtf=n=>n==null?'—':Number(n).toFixed(2);

function renderDaily(){
  const data=doSort(FD,dCol,dAsc);
  const tot=data.length,pages=Math.max(1,Math.ceil(tot/PS));
  pg.daily=Math.min(pg.daily,pages);
  const sl=data.slice((pg.daily-1)*PS,pg.daily*PS);
  const maxQ=Math.max(...FD.map(r=>Math.abs(r.holding_qty||0)),1);
  document.getElementById('cnt-daily').textContent=`${tot.toLocaleString()} rows`;
  document.getElementById('pi-daily').textContent=`Page ${pg.daily} of ${pages}`;
  document.getElementById('pp-daily').disabled=pg.daily<=1;
  document.getElementById('pn-daily').disabled=pg.daily>=pages;
  const tb=document.getElementById('tbody-daily');
  if(!sl.length){tb.innerHTML=`<tr><td colspan="10"><div class="empty">No data matches filters.</div></td></tr>`;return;}
  tb.innerHTML=sl.map(r=>{
    const hq=r.holding_qty||0,pct=Math.min(100,Math.abs(hq)/maxQ*100);
    const cls=hq>=0?'pos':'neg',fc=hq>=0?'p':'n';
    return`<tr>
      <td class="m">${r.Date||'—'}</td>
      <td class="sym">${r['Stock Symbol']||'—'}</td>
      <td><span class="brk">${r.broker||'—'}</span></td>
      <td class="bname">${r.broker_name||'—'}</td>
      <td class="m">${fmt(r.buy_qty)}</td>
      <td class="m">${fmt(r.total_sale_qty)}</td>
      <td><span class="badge-ipo">${fmt(r.ipo_sale_qty)}</span></td>
      <td class="m">${fmt(r.bulk_sale_qty)}</td>
      <td><div class="qcell"><span class="${cls}">${fmt(hq)}</span>
        <div class="qbar"><div class="qfill ${fc}" style="width:${pct}%"></div></div></div></td>
      <td class="m" style="color:var(--amber)">${fmtf(r.avg_rate)}</td>
    </tr>`;
  }).join('');
}

function renderCumul(){
  const data=doSort(FC,cCol,cAsc);
  const tot=data.length,pages=Math.max(1,Math.ceil(tot/PS));
  pg.cumul=Math.min(pg.cumul,pages);
  const sl=data.slice((pg.cumul-1)*PS,pg.cumul*PS);
  const off=(pg.cumul-1)*PS;
  document.getElementById('cnt-cumul').textContent=`${tot.toLocaleString()} rows`;
  document.getElementById('pi-cumul').textContent=`Page ${pg.cumul} of ${pages}`;
  document.getElementById('pp-cumul').disabled=pg.cumul<=1;
  document.getElementById('pn-cumul').disabled=pg.cumul>=pages;
  const tb=document.getElementById('tbody-cumul');
  if(!sl.length){tb.innerHTML=`<tr><td colspan="10"><div class="empty">No data.</div></td></tr>`;return;}
  const medals=['🥇','🥈','🥉'];
  tb.innerHTML=sl.map((r,i)=>{
    const rank=off+i+1,medal=medals[rank-1]||`#${rank}`;
    const nh=r.net_holding||0,cls=nh>=0?'pos':'neg';
    return`<tr>
      <td class="m" style="color:var(--muted)">${medal}</td>
      <td class="sym">${r['Stock Symbol']||'—'}</td>
      <td><span class="brk">${r.broker||'—'}</span></td>
      <td class="bname">${r.broker_name||'—'}</td>
      <td class="m">${fmt(r.total_buy_qty)}</td>
      <td class="m">${fmt(r.total_sale_qty)}</td>
      <td><span class="badge-ipo">${fmt(r.total_ipo_qty)}</span></td>
      <td class="m">${fmt(r.total_bulk_qty)}</td>
      <td class="${cls}">${fmt(nh)}</td>
      <td class="m" style="color:var(--amber)">${fmtf(r.avg_rate)}</td>
    </tr>`;
  }).join('');
}

function renderChart(sym){
  const chart=document.getElementById('barchart');
  const title=document.getElementById('chart-title');
  if(!sym){title.textContent='Select a stock symbol to see top brokers';
    chart.innerHTML=`<div class="empty">Use the Stock Symbol filter, click Apply.</div>`;return;}
  const rows=FC.filter(r=>r['Stock Symbol']===sym).sort((a,b)=>b.net_holding-a.net_holding).slice(0,10);
  if(!rows.length){chart.innerHTML=`<div class="empty">No data for ${sym}</div>`;return;}
  title.textContent=`Top Brokers — ${sym} (Cumulative Net Holdings)`;
  const maxV=Math.max(rows[0].net_holding,1);
  const medals=['🥇','🥈','🥉'];
  chart.innerHTML=rows.map((r,i)=>{
    const pct=Math.max(1,r.net_holding/maxV*100);
    return`<div class="brow">
      <div class="brank">${medals[i]||'#'+(i+1)}</div>
      <div class="binfo"><div class="bn">Broker ${r.broker}</div><div class="bnn">${r.broker_name||'—'}</div></div>
      <div class="btrack"><div class="bfill" style="width:${pct}%">${fmt(r.net_holding)}</div></div>
    </div>`;
  }).join('');
}

function showTab(name){
  ['daily','cumul','topb'].forEach(t=>{document.getElementById(`tab-${t}`).style.display=t===name?'':'none';});
  document.querySelectorAll('.tab').forEach((el,i)=>{el.classList.toggle('active',['daily','cumul','topb'][i]===name);});
  if(name==='topb') renderChart(document.getElementById('f-sym').value);
}
function chpg(dir,tbl){pg[tbl]+=dir;if(tbl==='daily')renderDaily();else renderCumul();window.scrollTo({top:0,behavior:'smooth'});}

init();
</script>
</body>
</html>"""

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)
    size_kb = os.path.getsize(OUTPUT_HTML) / 1024
    print(f"  Size: {size_kb:.1f} KB")


def main():
    print("="*60)
    print("NEPSE Holdings Compiler  (Memory-Efficient)")
    print("="*60)
    os.makedirs(DATA_DIR, exist_ok=True)

    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.xlsx")))
    if not files:
        raise FileNotFoundError(f"No .xlsx files found in '{DATA_DIR}/'")
    print(f"Found {len(files)} file(s)\n")

    agg_frames   = []
    broker_names = pd.DataFrame(columns=["broker","broker_name"])
    sec_names    = pd.DataFrame(columns=["Stock Symbol","Security Name"])
    all_symbols  = set()
    all_dates    = set()
    t0 = time.time()

    for i, fpath in enumerate(files, 1):
        fname = os.path.basename(fpath)
        t1 = time.time()
        print(f"[{i:>2}/{len(files)}] {fname}", end=" ... ", flush=True)
        df, meta = read_and_normalise(fpath)
        if df is None:
            continue
        agg = aggregate_one_file(df)
        if meta and "broker_names" in meta:
            broker_names = pd.concat([broker_names, meta["broker_names"]]).drop_duplicates(subset=["broker"])
        if meta and "security_names" in meta:
            sec_names = pd.concat([sec_names, meta["security_names"]]).drop_duplicates(subset=["Stock Symbol"])
        all_symbols.update(df["Stock Symbol"].dropna().unique())
        all_dates.update(df["Date"].dropna().unique())
        del df
        agg_frames.append(agg)
        print(f"{len(agg):,} positions  ({time.time()-t1:.1f}s)")

    if not agg_frames:
        raise ValueError("No files loaded successfully.")

    print(f"\nCombining {len(agg_frames)} aggregations...")
    h = pd.concat(agg_frames, ignore_index=True)
    del agg_frames

    print("Re-aggregating across all dates...")
    grp = ["Date","Stock Symbol","broker"]
    h = (h.groupby(grp)
          .agg(buy_qty       =("buy_qty",        "sum"),
               buy_amt       =("buy_amt",         "sum"),
               total_sale_qty=("total_sale_qty",  "sum"),
               ipo_sale_qty  =("ipo_sale_qty",    "sum"),
               bulk_sale_qty =("bulk_sale_qty",   "sum"))
          .reset_index())
    h["holding_qty"] = h["buy_qty"] - h["bulk_sale_qty"]
    h["avg_rate"]    = (h["buy_amt"]/h["buy_qty"]).where(h["buy_qty"]>0,0).round(2)
    h = h.sort_values(["Stock Symbol","Date","holding_qty"],
                      ascending=[True,True,False]).reset_index(drop=True)

    cumul = (h.groupby(["Stock Symbol","broker"])
              .agg(total_buy_qty  =("buy_qty",        "sum"),
                   total_sale_qty =("total_sale_qty",  "sum"),
                   total_ipo_qty  =("ipo_sale_qty",    "sum"),
                   total_bulk_qty =("bulk_sale_qty",   "sum"),
                   net_holding    =("holding_qty",     "sum"),
                   avg_rate       =("avg_rate",        "mean"))
              .reset_index())

    nz = h[h["holding_qty"]!=0]
    print(f"\nResults:")
    print(f"  Holdings : {len(h):,} rows  ({len(nz):,} non-zero)")
    print(f"  Symbols  : {sorted(all_symbols)}")
    print(f"  Brokers  : {h['broker'].nunique()}")
    print(f"  Dates    : {h['Date'].min()} → {h['Date'].max()}")

    summary = build_summary(h.copy(), cumul.copy(), broker_names, sec_names,
                            list(all_symbols), list(all_dates))

    print(f"\nSaving {OUTPUT_JSON} ...")
    with open(OUTPUT_JSON, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"  Size: {os.path.getsize(OUTPUT_JSON)/1024:.1f} KB")

    print(f"Generating {OUTPUT_HTML} (data embedded) ...")
    write_html(summary)

    print(f"\n✅ Done in {time.time()-t0:.1f}s")
    print(f"   → Double-click index.html to open the dashboard!")

if __name__ == "__main__":
    main()
