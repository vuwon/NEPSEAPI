"""
compile_holdings.py — NEPSE Holdings Compiler (Lightweight Output)
- Processes ONE file at a time (memory efficient)
- Outputs ONLY non-zero holdings to keep file sizes small
- index.html loads from holdings_summary.json (kept under 10MB)

Run: python compile_holdings.py
"""

import os, json, glob, time
import pandas as pd
from datetime import datetime

DATA_DIR     = "./data"
OUTPUT_JSON  = "holdings_summary.json"
OUTPUT_HTML  = "index.html"
IPO_QTY      = 10

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
        elif "Quantity" in df.columns and "Buyer" in df.columns:
            if "BROKER #" in df.columns:
                df = df[df["BROKER #"].isna()].copy()
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
        names = []
        if "BuyerName" in df.columns:
            names.append(df[["Buyer","BuyerName"]].rename(columns={"Buyer":"broker","BuyerName":"broker_name"}))
        if "SellerName" in df.columns:
            names.append(df[["Seller","SellerName"]].rename(columns={"Seller":"broker","SellerName":"broker_name"}))
        if names:
            meta["broker_names"] = pd.concat(names).drop_duplicates(subset=["broker"]).dropna(subset=["broker_name"])
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
                  .agg(total_sale_qty=("Quantity","sum"), total_sale_amt=("Amount (Rs)","sum"))
                  .reset_index().rename(columns={"Seller":"broker"}))
    sell_ipo = (df[df["Quantity"]==IPO_QTY]
                  .groupby(grp+["Seller"])
                  .agg(ipo_sale_qty=("Quantity","sum"), ipo_sale_amt=("Amount (Rs)","sum"))
                  .reset_index().rename(columns={"Seller":"broker"}))
    sell = sell_all.merge(sell_ipo, on=grp+["broker"], how="left")
    sell["ipo_sale_qty"]  = sell["ipo_sale_qty"].fillna(0)
    sell["ipo_sale_amt"]  = sell["ipo_sale_amt"].fillna(0)
    sell["bulk_sale_qty"] = sell["total_sale_qty"] - sell["ipo_sale_qty"]
    sell["bulk_sale_amt"] = sell["total_sale_amt"] - sell["ipo_sale_amt"]
    h = buy.merge(sell, on=grp+["broker"], how="outer").fillna(0)
    h["holding_qty"] = h["buy_qty"] - h["bulk_sale_qty"]
    # Avg rate = (buy amount - bulk sale amount) / holding qty
    h["avg_rate"]    = ((h["buy_amt"] - h["bulk_sale_amt"]) / h["holding_qty"]).where(h["holding_qty"]>0, 0).round(2)
    h["broker"]      = h["broker"].astype(int)
    # ✅ DROP zero holdings immediately — biggest size reduction
    h = h[h["holding_qty"] != 0].copy()
    return h


def main():
    print("="*60)
    print("NEPSE Holdings Compiler")
    print("="*60)
    os.makedirs(DATA_DIR, exist_ok=True)

    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.xlsx")))
    if not files:
        raise FileNotFoundError(f"No .xlsx files in '{DATA_DIR}/'")
    print(f"Found {len(files)} file(s)\n")

    agg_frames   = []
    broker_names = pd.DataFrame(columns=["broker","broker_name"])
    sec_names    = pd.DataFrame(columns=["Stock Symbol","Security Name"])
    all_symbols  = set()
    all_dates    = set()
    t0 = time.time()

    for i, fpath in enumerate(files, 1):
        t1 = time.time()
        print(f"[{i:>2}/{len(files)}] {os.path.basename(fpath)}", end=" ... ", flush=True)
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
        raise ValueError("No files loaded.")

    print(f"\nCombining {len(agg_frames)} aggregations...")
    h = pd.concat(agg_frames, ignore_index=True)
    del agg_frames

    # Re-aggregate (same broker+symbol+date may exist across files)
    grp = ["Date","Stock Symbol","broker"]
    h = (h.groupby(grp)
          .agg(buy_qty       =("buy_qty",        "sum"),
               buy_amt       =("buy_amt",         "sum"),
               total_sale_qty=("total_sale_qty",  "sum"),
               ipo_sale_qty  =("ipo_sale_qty",    "sum"),
               bulk_sale_qty =("bulk_sale_qty",   "sum"),
               bulk_sale_amt =("bulk_sale_amt",   "sum"))
          .reset_index())
    h["holding_qty"] = h["buy_qty"] - h["bulk_sale_qty"]
    # Avg rate = (buy amount - bulk sale amount) / holding qty
    h["avg_rate"]    = ((h["buy_amt"] - h["bulk_sale_amt"]) / h["holding_qty"]).where(h["holding_qty"]>0, 0).round(2)
    h = h[h["holding_qty"] != 0]  # ✅ keep only non-zero
    h = h.sort_values(["Stock Symbol","Date","holding_qty"],
                      ascending=[True,True,False]).reset_index(drop=True)

    # Attach names
    h = h.merge(broker_names, on="broker", how="left")
    h["broker_name"] = h.get("broker_name", pd.Series(dtype=str)).fillna("")
    h = h.merge(sec_names, on="Stock Symbol", how="left")
    h["Security Name"] = h.get("Security Name", pd.Series(dtype=str)).fillna("")

    # Cumulative
    cumul = (h.groupby(["Stock Symbol","broker"])
              .agg(broker_name      =("broker_name",    "first"),
                   security_name    =("Security Name",  "first"),
                   total_buy_qty    =("buy_qty",         "sum"),
                   total_sale_qty   =("total_sale_qty",  "sum"),
                   total_ipo_qty    =("ipo_sale_qty",    "sum"),
                   total_bulk_qty   =("bulk_sale_qty",   "sum"),
                   net_holding      =("holding_qty",     "sum"),
                   total_buy_amt    =("buy_amt",         "sum"),
                   total_bulk_amt   =("bulk_sale_amt",   "sum"))
              .reset_index()
              .sort_values("net_holding", ascending=False))
    # Avg rate = (total buy amount - total bulk sale amount) / net holding qty
    cumul["avg_rate"] = ((cumul["total_buy_amt"] - cumul["total_bulk_amt"]) / cumul["net_holding"]).where(cumul["net_holding"]>0, 0).round(2)

    symbols = sorted(all_symbols)
    dates   = sorted(all_dates)

    print(f"\nResults:")
    print(f"  Non-zero holdings : {len(h):,} rows")
    print(f"  Symbols  : {symbols}")
    print(f"  Brokers  : {h['broker'].nunique()}")
    print(f"  Dates    : {min(dates)} → {max(dates)}")

    # Top brokers per symbol
    top = {}
    for sym in symbols:
        rows = cumul[cumul["Stock Symbol"]==sym].nlargest(10,"net_holding")
        top[sym] = rows[["broker","broker_name","net_holding",
                          "total_buy_qty","total_bulk_qty","total_ipo_qty"]].to_dict(orient="records")

    # ✅ Use integers for numeric fields to reduce JSON size
    def compress(r):
        return {
            "d" : r.get("Date",""),
            "s" : r.get("Stock Symbol",""),
            "sn": r.get("Security Name",""),
            "b" : int(r.get("broker",0)),
            "bn": r.get("broker_name",""),
            "bq": int(r.get("buy_qty",0)),
            "sq": int(r.get("total_sale_qty",0)),
            "iq": int(r.get("ipo_sale_qty",0)),
            "lq": int(r.get("bulk_sale_qty",0)),
            "hq": int(r.get("holding_qty",0)),
            "ar": round(float(r.get("avg_rate",0)),2),
        }

    def compress_c(r):
        return {
            "s" : r.get("Stock Symbol",""),
            "sn": r.get("security_name",""),
            "b" : int(r.get("broker",0)),
            "bn": r.get("broker_name",""),
            "bq": int(r.get("total_buy_qty",0)),
            "sq": int(r.get("total_sale_qty",0)),
            "iq": int(r.get("total_ipo_qty",0)),
            "lq": int(r.get("total_bulk_qty",0)),
            "nh": int(r.get("net_holding",0)),
            "ar": round(float(r.get("avg_rate",0)),2),
        }

    summary = {
        "g" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "sy": symbols,
        "dt": dates,
        "tb": top,
        "h" : [compress(r) for r in h.to_dict(orient="records")],
        "c" : [compress_c(r) for r in cumul.to_dict(orient="records")],
    }

    # Save JSON
    print(f"\nSaving {OUTPUT_JSON} ...")
    with open(OUTPUT_JSON, "w") as f:
        json.dump(summary, f, separators=(',',':'), default=str)  # compact, no spaces
    size = os.path.getsize(OUTPUT_JSON)/1024/1024
    print(f"  Size: {size:.2f} MB")
    if size > 90:
        print(f"  ⚠️  Still large — consider filtering to specific symbols in Nepse_API.py")

    # Generate index.html (loads JSON via fetch — works on GitHub Pages)
    print(f"Generating {OUTPUT_HTML} ...")
    write_html()
    print(f"  Size: {os.path.getsize(OUTPUT_HTML)/1024:.1f} KB")

    print(f"\n✅ Done in {time.time()-t0:.1f}s")
    print(f"   Local: open index.html with  python -m http.server 8000  then visit http://localhost:8000")
    print(f"   GitHub Pages: push and visit your Pages URL")


def write_html():
    """Write a lightweight index.html that fetches holdings_summary.json."""
    html = r"""<!DOCTYPE html>
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
.wrap{max-width:1440px;margin:0 auto;padding:0 28px}
header{padding:16px 0;border-bottom:1px solid var(--border);position:sticky;top:0;background:rgba(6,10,18,.95);backdrop-filter:blur(12px);z-index:100}
.hdr{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap}
.brand{display:flex;align-items:center;gap:14px}
.brand-icon{width:38px;height:38px;border-radius:8px;background:linear-gradient(135deg,var(--cyan),var(--purple));display:flex;align-items:center;justify-content:center;font-size:18px}
.brand h1{font-family:var(--mono);font-size:14px;color:var(--cyan);letter-spacing:2px;font-weight:600}
.brand p{font-size:11px;color:var(--muted);margin-top:2px}
.hdr-meta{font-family:var(--mono);font-size:10px;color:var(--muted);text-align:right;line-height:1.9}
.hdr-meta b{color:var(--cyan)}
.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin:20px 0}
.sc{background:var(--s1);border:1px solid var(--border);border-radius:10px;padding:14px 16px;position:relative;overflow:hidden;transition:border-color .2s,transform .2s}
.sc:hover{border-color:var(--border2);transform:translateY(-2px)}
.sc::after{content:'';position:absolute;bottom:0;left:0;right:0;height:2px;background:linear-gradient(90deg,var(--cyan),var(--purple));opacity:.5}
.sc-label{font-size:10px;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);margin-bottom:6px}
.sc-val{font-family:var(--mono);font-size:22px;color:var(--cyan);font-weight:600}
.sc-sub{font-size:10px;color:var(--muted);margin-top:4px}
.fp{background:var(--s1);border:1px solid var(--border);border-radius:10px;padding:14px 18px;margin-bottom:16px;display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end}
.fg{display:flex;flex-direction:column;gap:4px;min-width:140px}
.fg label{font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--muted)}
select,input[type=text]{background:var(--s2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-family:var(--sans);font-size:13px;padding:6px 10px;outline:none;transition:border-color .2s;cursor:pointer;width:100%}
select:focus,input:focus{border-color:var(--cyan)}
select option{background:var(--s2)}
.btn{padding:7px 16px;border-radius:6px;border:none;font-family:var(--sans);font-size:13px;font-weight:500;cursor:pointer;transition:all .2s}
.btn-p{background:linear-gradient(135deg,var(--cyan2),var(--purple));color:#fff}
.btn-p:hover{opacity:.85}
.btn-g{background:transparent;border:1px solid var(--border);color:var(--muted)}
.btn-g:hover{border-color:var(--cyan);color:var(--cyan)}
.btns{display:flex;gap:8px;align-items:flex-end}
.tabs{display:flex;gap:2px;margin-bottom:14px;border-bottom:1px solid var(--border)}
.tab{padding:9px 18px;font-size:13px;font-weight:500;color:var(--muted);cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-1px;transition:all .2s;user-select:none}
.tab:hover{color:var(--text)}
.tab.active{color:var(--cyan);border-bottom-color:var(--cyan)}
.tw{background:var(--s1);border:1px solid var(--border);border-radius:10px;overflow:hidden;margin-bottom:16px}
.th2{display:flex;align-items:center;justify-content:space-between;padding:11px 16px;border-bottom:1px solid var(--border);flex-wrap:wrap;gap:8px}
.ttitle{font-size:13px;font-weight:600}
.tcnt{font-family:var(--mono);font-size:11px;color:var(--muted)}
.tscroll{overflow-x:auto}
table{width:100%;border-collapse:collapse;min-width:680px}
thead th{padding:8px 12px;font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);text-align:left;background:var(--s2);cursor:pointer;user-select:none;white-space:nowrap;border-bottom:1px solid var(--border)}
thead th:hover{color:var(--cyan)}
tbody tr{border-bottom:1px solid var(--border);transition:background .1s}
tbody tr:last-child{border-bottom:none}
tbody tr:hover{background:var(--s2)}
td{padding:8px 12px;font-size:13px;white-space:nowrap}
.m{font-family:var(--mono);font-size:12px}
.sym{font-family:var(--mono);font-weight:600;color:var(--cyan)}
.pos{color:var(--green);font-family:var(--mono)}
.neg{color:var(--red);font-family:var(--mono)}
.brk{display:inline-block;background:rgba(0,200,255,.08);border:1px solid rgba(0,200,255,.2);border-radius:4px;padding:2px 6px;font-family:var(--mono);font-size:11px;color:var(--cyan)}
.ipo{display:inline-block;background:rgba(168,85,247,.1);border:1px solid rgba(168,85,247,.2);border-radius:4px;padding:2px 6px;font-family:var(--mono);font-size:11px;color:var(--purple)}
.bname{font-size:11px;color:var(--muted);max-width:130px;overflow:hidden;text-overflow:ellipsis}
.qcell{display:flex;align-items:center;gap:6px}
.qbar{flex:1;height:3px;background:var(--muted2);border-radius:2px;min-width:30px;max-width:70px}
.qfill{height:100%;border-radius:2px}
.qfill.p{background:linear-gradient(90deg,var(--cyan),var(--green))}
.qfill.n{background:linear-gradient(90deg,var(--red),#f97316)}
.pag{display:flex;align-items:center;justify-content:space-between;padding:10px 16px;border-top:1px solid var(--border);font-size:12px;color:var(--muted);flex-wrap:wrap;gap:8px}
.pbtns{display:flex;gap:6px}
.pb{padding:4px 11px;border-radius:5px;border:1px solid var(--border);background:transparent;color:var(--text);font-size:12px;cursor:pointer;transition:all .2s}
.pb:hover:not(:disabled){border-color:var(--cyan);color:var(--cyan)}
.pb:disabled{opacity:.3;cursor:not-allowed}
.cw{background:var(--s1);border:1px solid var(--border);border-radius:10px;padding:20px;margin-bottom:16px}
.ctitle{font-size:14px;font-weight:600;margin-bottom:16px}
.barchart{display:flex;flex-direction:column;gap:11px}
.brow{display:flex;align-items:center;gap:10px}
.brank{font-size:12px;width:24px;text-align:right;flex-shrink:0;color:var(--muted)}
.binfo{width:180px;flex-shrink:0}
.binfo .bn{font-family:var(--mono);font-size:11px;color:var(--cyan)}
.binfo .bnn{font-size:11px;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.btrack{flex:1;height:24px;background:var(--s2);border-radius:5px;overflow:hidden;border:1px solid var(--border)}
.bfill{height:100%;background:linear-gradient(90deg,var(--cyan2),var(--cyan));display:flex;align-items:center;padding-left:8px;font-family:var(--mono);font-size:11px;color:#fff;transition:width .6s ease;min-width:2px}
.loading{text-align:center;padding:48px;color:var(--muted)}
.spinner{width:32px;height:32px;border:2px solid var(--border);border-top-color:var(--cyan);border-radius:50%;animation:spin .7s linear infinite;margin:0 auto 12px}
@keyframes spin{to{transform:rotate(360deg)}}
.empty{text-align:center;padding:40px;color:var(--muted)}
@media(max-width:768px){.stats{grid-template-columns:repeat(2,1fr)}.fp{flex-direction:column}.fg{min-width:100%}}
</style>
</head>
<body>
<header><div class="wrap"><div class="hdr">
  <div class="brand">
    <div class="brand-icon">📊</div>
    <div><h1>NEPSE · HOLDINGS</h1><p>Broker-level floorsheet dashboard · auto-updated daily</p></div>
  </div>
  <div class="hdr-meta">Updated: <b id="gen-at">—</b><br>Range: <b id="date-range">—</b></div>
</div></div></header>

<div class="wrap" style="padding-top:20px;padding-bottom:40px">
  <div class="stats">
    <div class="sc"><div class="sc-label">Positions</div><div class="sc-val" id="s-pos">—</div><div class="sc-sub">non-zero holdings</div></div>
    <div class="sc"><div class="sc-label">Stocks</div><div class="sc-val" id="s-sym">—</div><div class="sc-sub">unique symbols</div></div>
    <div class="sc"><div class="sc-label">Trading Days</div><div class="sc-val" id="s-days">—</div><div class="sc-sub">days of data</div></div>
    <div class="sc"><div class="sc-label">Brokers</div><div class="sc-val" id="s-brk">—</div><div class="sc-sub">active in view</div></div>
    <div class="sc"><div class="sc-label">Net Holdings</div><div class="sc-val" id="s-net">—</div><div class="sc-sub">filtered total</div></div>
    <div class="sc"><div class="sc-label">All Symbols</div><div class="sc-val" id="s-allsym">—</div><div class="sc-sub">in dataset</div></div>
  </div>

  <div class="fp">
    <div class="fg"><label>Stock Symbol</label><select id="f-sym"><option value="">All Stocks</option></select></div>
    <div class="fg"><label>Broker #</label><input type="text" id="f-brk" placeholder="e.g. 58"></div>
    <div class="fg"><label>Broker Name</label><input type="text" id="f-bname" placeholder="e.g. Sunrise"></div>
    <div class="fg"><label>Date From</label><select id="f-dfrom"><option value="">Earliest</option></select></div>
    <div class="fg"><label>Date To</label><select id="f-dto"><option value="">Latest</option></select></div>
    <div class="fg"><label>Min Holding</label><input type="text" id="f-minq" placeholder="e.g. 500"></div>
    <div class="fg"><label>Show</label><select id="f-side">
      <option value="all">All positions</option>
      <option value="pos">Positive only</option>
      <option value="neg">Negative only</option>
    </select></div>
    <div class="btns">
      <button class="btn btn-p" onclick="applyFilters()">Apply</button>
      <button class="btn btn-g" onclick="resetFilters()">Reset</button>
    </div>
  </div>

  <div class="tabs">
    <div class="tab active" onclick="showTab('daily')">Daily Holdings</div>
    <div class="tab" onclick="showTab('cumul')">Cumulative</div>
    <div class="tab" onclick="showTab('topb')">Top Brokers Chart</div>
  </div>

  <div id="tab-daily">
    <div class="tw">
      <div class="th2"><span class="ttitle">Daily Holdings per Broker per Stock</span><span class="tcnt" id="cnt-d">—</span></div>
      <div class="tscroll"><table>
        <thead><tr>
          <th onclick="srt('d')">Date ↕</th><th onclick="srt('s')">Symbol ↕</th>
          <th onclick="srt('b')">Broker # ↕</th><th>Broker Name</th>
          <th onclick="srt('bq')">Buy Qty ↕</th><th onclick="srt('sq')">Sale Qty ↕</th>
          <th onclick="srt('iq')">IPO Sale ↕</th><th onclick="srt('lq')">Bulk Sale ↕</th>
          <th onclick="srt('hq')">Net Holding ↕</th><th onclick="srt('ar')">Avg Rate ↕</th>
        </tr></thead>
        <tbody id="tbody-d"><tr><td colspan="10"><div class="loading"><div class="spinner"></div>Loading data…</div></td></tr></tbody>
      </table></div>
      <div class="pag">
        <span id="pi-d">Page 1 of 1</span>
        <div class="pbtns">
          <button class="pb" id="pp-d" onclick="chpg(-1,'d')">← Prev</button>
          <button class="pb" id="pn-d" onclick="chpg(1,'d')">Next →</button>
        </div>
      </div>
    </div>
  </div>

  <div id="tab-cumul" style="display:none">
    <div class="tw">
      <div class="th2"><span class="ttitle">Cumulative Net Holdings (All Dates)</span><span class="tcnt" id="cnt-c">—</span></div>
      <div class="tscroll"><table>
        <thead><tr>
          <th>Rank</th><th onclick="csrt('s')">Symbol ↕</th>
          <th onclick="csrt('b')">Broker # ↕</th><th>Broker Name</th>
          <th onclick="csrt('bq')">Total Buy ↕</th><th onclick="csrt('sq')">Total Sale ↕</th>
          <th onclick="csrt('iq')">IPO Sale ↕</th><th onclick="csrt('lq')">Bulk Sale ↕</th>
          <th onclick="csrt('nh')">Net Holding ↕</th><th onclick="csrt('ar')">Avg Rate ↕</th>
        </tr></thead>
        <tbody id="tbody-c"></tbody>
      </table></div>
      <div class="pag">
        <span id="pi-c">Page 1 of 1</span>
        <div class="pbtns">
          <button class="pb" id="pp-c" onclick="chpg(-1,'c')">← Prev</button>
          <button class="pb" id="pn-c" onclick="chpg(1,'c')">Next →</button>
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
let DAILY=[],CUMUL=[],FD=[],FC=[];
let dCol='hq',dAsc=false,cCol='nh',cAsc=false;
let pg={d:1,c:1};
const PS=50;

async function init(){
  try{
    const r=await fetch('holdings_summary.json?t='+Date.now());
    if(!r.ok) throw new Error('holdings_summary.json not found');
    const d=await r.json();
    document.getElementById('gen-at').textContent=d.g||'—';
    const dates=d.dt||[];
    document.getElementById('date-range').textContent=dates.length?`${dates[0]} → ${dates[dates.length-1]}`:'—';
    document.getElementById('s-allsym').textContent=(d.sy||[]).length;
    const ss=document.getElementById('f-sym');
    (d.sy||[]).forEach(s=>{ss.add(new Option(s,s))});
    ['f-dfrom','f-dto'].forEach(id=>{
      const el=document.getElementById(id);
      dates.forEach(dt=>{el.add(new Option(dt,dt))});
    });
    DAILY=d.h||[];CUMUL=d.c||[];FD=[...DAILY];FC=[...CUMUL];
    updStats(FD);renderD();renderC();
  }catch(e){
    document.getElementById('tbody-d').innerHTML=
      `<tr><td colspan="10"><div class="empty">⚠️ ${e.message}<br><br>
      <small>Run <code>python compile_holdings.py</code> to generate holdings_summary.json<br>
      Then push to GitHub — the dashboard will update automatically.</small></div></td></tr>`;
  }
}

function updStats(fd){
  document.getElementById('s-pos').textContent=fd.length.toLocaleString();
  document.getElementById('s-sym').textContent=new Set(fd.map(r=>r.s)).size;
  document.getElementById('s-days').textContent=new Set(fd.map(r=>r.d)).size;
  document.getElementById('s-brk').textContent=new Set(fd.map(r=>r.b)).size;
  document.getElementById('s-net').textContent=Math.round(fd.reduce((s,r)=>s+(r.hq||0),0)).toLocaleString();
}

function applyFilters(){
  const sym=document.getElementById('f-sym').value;
  const brk=document.getElementById('f-brk').value.trim();
  const bn =document.getElementById('f-bname').value.trim().toLowerCase();
  const df =document.getElementById('f-dfrom').value;
  const dt =document.getElementById('f-dto').value;
  const mq =parseFloat(document.getElementById('f-minq').value)||null;
  const sd =document.getElementById('f-side').value;
  FD=DAILY.filter(r=>{
    if(sym && r.s!==sym) return false;
    if(brk && String(r.b)!==brk) return false;
    if(bn  && !(r.bn||'').toLowerCase().includes(bn)) return false;
    if(df  && r.d<df) return false;
    if(dt  && r.d>dt) return false;
    if(mq!==null && Math.abs(r.hq)<mq) return false;
    if(sd==='pos' && r.hq<=0) return false;
    if(sd==='neg' && r.hq>=0) return false;
    return true;
  });
  FC=CUMUL.filter(r=>{
    if(sym && r.s!==sym) return false;
    if(brk && String(r.b)!==brk) return false;
    if(bn  && !(r.bn||'').toLowerCase().includes(bn)) return false;
    return true;
  });
  pg.d=1;pg.c=1;updStats(FD);renderD();renderC();
  renderChart(sym);
  if(sym) showTab('topb');
}

function resetFilters(){
  ['f-sym','f-dfrom','f-dto','f-side'].forEach(id=>document.getElementById(id).value='');
  ['f-brk','f-bname','f-minq'].forEach(id=>document.getElementById(id).value='');
  FD=[...DAILY];FC=[...CUMUL];pg.d=1;pg.c=1;
  updStats(FD);renderD();renderC();renderChart('');
}

function doSort(arr,col,asc){
  return [...arr].sort((a,b)=>{
    let va=a[col],vb=b[col];
    if(typeof va==='number') return asc?va-vb:vb-va;
    return asc?String(va||'').localeCompare(String(vb||'')):String(vb||'').localeCompare(String(va||''));
  });
}
function srt(c){if(dCol===c)dAsc=!dAsc;else{dCol=c;dAsc=false;}pg.d=1;renderD();}
function csrt(c){if(cCol===c)cAsc=!cAsc;else{cCol=c;cAsc=false;}pg.c=1;renderC();}
const fmt=n=>Number(n||0).toLocaleString();
const fmtf=n=>Number(n||0).toFixed(2);

function renderD(){
  const data=doSort(FD,dCol,dAsc);
  const tot=data.length,pages=Math.max(1,Math.ceil(tot/PS));
  pg.d=Math.min(pg.d,pages);
  const sl=data.slice((pg.d-1)*PS,pg.d*PS);
  const maxQ=Math.max(...FD.map(r=>Math.abs(r.hq||0)),1);
  document.getElementById('cnt-d').textContent=`${tot.toLocaleString()} rows`;
  document.getElementById('pi-d').textContent=`Page ${pg.d} of ${pages}`;
  document.getElementById('pp-d').disabled=pg.d<=1;
  document.getElementById('pn-d').disabled=pg.d>=pages;
  const tb=document.getElementById('tbody-d');
  if(!sl.length){tb.innerHTML=`<tr><td colspan="10"><div class="empty">No data matches filters.</div></td></tr>`;return;}
  tb.innerHTML=sl.map(r=>{
    const hq=r.hq||0,pct=Math.min(100,Math.abs(hq)/maxQ*100);
    const cls=hq>=0?'pos':'neg',fc=hq>=0?'p':'n';
    return`<tr>
      <td class="m">${r.d}</td>
      <td class="sym">${r.s}</td>
      <td><span class="brk">${r.b}</span></td>
      <td class="bname">${r.bn||'—'}</td>
      <td class="m">${fmt(r.bq)}</td>
      <td class="m">${fmt(r.sq)}</td>
      <td><span class="ipo">${fmt(r.iq)}</span></td>
      <td class="m">${fmt(r.lq)}</td>
      <td><div class="qcell"><span class="${cls}">${fmt(hq)}</span>
        <div class="qbar"><div class="qfill ${fc}" style="width:${pct}%"></div></div></div></td>
      <td class="m" style="color:var(--amber)">${fmtf(r.ar)}</td>
    </tr>`;
  }).join('');
}

function renderC(){
  const data=doSort(FC,cCol,cAsc);
  const tot=data.length,pages=Math.max(1,Math.ceil(tot/PS));
  pg.c=Math.min(pg.c,pages);
  const sl=data.slice((pg.c-1)*PS,pg.c*PS);
  const off=(pg.c-1)*PS;
  document.getElementById('cnt-c').textContent=`${tot.toLocaleString()} rows`;
  document.getElementById('pi-c').textContent=`Page ${pg.c} of ${pages}`;
  document.getElementById('pp-c').disabled=pg.c<=1;
  document.getElementById('pn-c').disabled=pg.c>=pages;
  const tb=document.getElementById('tbody-c');
  if(!sl.length){tb.innerHTML=`<tr><td colspan="10"><div class="empty">No data.</div></td></tr>`;return;}
  const medals=['🥇','🥈','🥉'];
  tb.innerHTML=sl.map((r,i)=>{
    const rank=off+i+1,medal=medals[rank-1]||`#${rank}`;
    const nh=r.nh||0,cls=nh>=0?'pos':'neg';
    return`<tr>
      <td class="m" style="color:var(--muted)">${medal}</td>
      <td class="sym">${r.s}</td>
      <td><span class="brk">${r.b}</span></td>
      <td class="bname">${r.bn||'—'}</td>
      <td class="m">${fmt(r.bq)}</td>
      <td class="m">${fmt(r.sq)}</td>
      <td><span class="ipo">${fmt(r.iq)}</span></td>
      <td class="m">${fmt(r.lq)}</td>
      <td class="${cls}">${fmt(nh)}</td>
      <td class="m" style="color:var(--amber)">${fmtf(r.ar)}</td>
    </tr>`;
  }).join('');
}

function renderChart(sym){
  const chart=document.getElementById('barchart');
  const title=document.getElementById('chart-title');
  if(!sym){title.textContent='Select a stock symbol to see top brokers';
    chart.innerHTML=`<div class="empty">Use the Stock Symbol filter above, then click Apply.</div>`;return;}
  const rows=FC.filter(r=>r.s===sym).sort((a,b)=>b.nh-a.nh).slice(0,10);
  if(!rows.length){chart.innerHTML=`<div class="empty">No data for ${sym}</div>`;return;}
  title.textContent=`Top Brokers — ${sym} (Cumulative Net Holdings)`;
  const maxV=Math.max(rows[0].nh,1);
  const medals=['🥇','🥈','🥉'];
  chart.innerHTML=rows.map((r,i)=>{
    const pct=Math.max(1,r.nh/maxV*100);
    return`<div class="brow">
      <div class="brank">${medals[i]||'#'+(i+1)}</div>
      <div class="binfo"><div class="bn">Broker ${r.b}</div><div class="bnn">${r.bn||'—'}</div></div>
      <div class="btrack"><div class="bfill" style="width:${pct}%">${fmt(r.nh)}</div></div>
    </div>`;
  }).join('');
}

function showTab(name){
  ['daily','cumul','topb'].forEach(t=>{document.getElementById(`tab-${t}`).style.display=t===name?'':'none';});
  document.querySelectorAll('.tab').forEach((el,i)=>{el.classList.toggle('active',['daily','cumul','topb'][i]===name);});
  if(name==='topb') renderChart(document.getElementById('f-sym').value);
}
function chpg(dir,t){pg[t]+=dir;if(t==='d')renderD();else renderC();window.scrollTo({top:0,behavior:'smooth'});}

init();
</script>
</body>
</html>"""
    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)


if __name__ == "__main__":
    main()
