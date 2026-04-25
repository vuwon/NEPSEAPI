"""
compile_holdings.py — NEPSE Holdings Compiler (Supabase Edition)

HOW IT WORKS:
  1. Reads today's Excel file(s) from ./data/ folder
  2. Aggregates holdings (buy/sell/IPO/bulk) per broker per stock
  3. Upserts rows into Supabase (INSERT or UPDATE if already exists)
  4. Generates lightweight index.html for GitHub Pages

REQUIREMENTS:
  pip install pandas openpyxl supabase

ENVIRONMENT VARIABLES NEEDED (set as GitHub Secrets):
  SUPABASE_URL  — e.g. https://abcdefgh.supabase.co
  SUPABASE_KEY  — service_role key (from Supabase → Settings → API)

Run locally:
  set SUPABASE_URL=https://your-project.supabase.co
  set SUPABASE_KEY=your-service-role-key
  python compile_holdings.py
"""

import os, glob, time, json
import pandas as pd
import httpx
from datetime import datetime

DATA_DIR = "./data"
OUTPUT_HTML = "index.html"
IPO_QTY = 10

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


class SupabaseClient:
    """Lightweight Supabase REST client using httpx — no C++ build tools needed."""
    def __init__(self, url: str, key: str):
        self.url = url.rstrip("/")
        self.headers = {
            "apikey"       : key,
            "Authorization": f"Bearer {key}",
            "Content-Type" : "application/json",
            "Prefer"       : "return=minimal",
        }

    def upsert(self, table: str, rows: list, on_conflict: str):
        """Upsert rows into a table."""
        headers = {**self.headers, "Prefer": f"resolution=merge-duplicates,return=minimal"}
        r = httpx.post(
            f"{self.url}/rest/v1/{table}",
            headers={**headers, "Prefer": f"resolution=merge-duplicates,return=minimal"},
            params={"on_conflict": on_conflict},
            content=json.dumps(rows),
            timeout=60,
        )
        if r.status_code not in (200, 201):
            raise Exception(f"Upsert failed [{r.status_code}]: {r.text[:300]}")

    def select(self, table: str, columns: str, filters: dict, limit: int = 10000) -> list:
        """Select rows from a table with simple equality filters."""
        params = {"select": columns, "limit": limit}
        for col, val in filters.items():
            params[f"{col}"] = f"eq.{val}"
        r = httpx.get(
            f"{self.url}/rest/v1/{table}",
            headers=self.headers,
            params=params,
            timeout=60,
        )
        if r.status_code != 200:
            raise Exception(f"Select failed [{r.status_code}]: {r.text[:300]}")
        return r.json()

    def count(self, table: str, filters: dict) -> int:
        """Count rows matching filters."""
        params = {"select": "id"}
        for col, val in filters.items():
            params[col] = f"eq.{val}"
        r = httpx.get(
            f"{self.url}/rest/v1/{table}",
            headers={**self.headers, "Prefer": "count=exact"},
            params=params,
            timeout=30,
        )
        count_val = r.headers.get("content-range","0").split("/")[-1]
        return int(count_val) if count_val.isdigit() else 0


def get_supabase() -> SupabaseClient:
    url = os.environ.get("SUPABASE_URL","")
    key = os.environ.get("SUPABASE_KEY","")
    if not url or not key:
        raise ValueError(
            "SUPABASE_URL and SUPABASE_KEY environment variables are required.\n"
            "Set them as GitHub Secrets or locally before running."
        )
    return SupabaseClient(url, key)


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
    h["avg_rate"]    = ((h["buy_amt"] - h["bulk_sale_amt"]) / h["holding_qty"]).where(h["holding_qty"]>0, 0).round(2)
    h["broker"]      = h["broker"].astype(int)
    h = h[h["holding_qty"] != 0].copy()
    return h


def compute_daily_volume(df):
    """
    Compute accurate daily volume per symbol from raw transactions.
    Called BEFORE dropping zero holdings — captures ALL brokers.
    Returns: DataFrame with one row per (Date, Stock Symbol)
    """
    grp = ["Date", "Stock Symbol"]

    # Total buy per symbol per date (sum across ALL brokers)
    buy_vol = (df.groupby(grp + ["Buyer"])["Quantity"].sum()
                 .reset_index().rename(columns={"Buyer":"broker","Quantity":"buy_qty"}))

    # Total sell per symbol per date
    sell_vol = (df.groupby(grp + ["Seller"])["Quantity"].sum()
                  .reset_index().rename(columns={"Seller":"broker","Quantity":"sel_qty"}))

    # Symbol-level totals
    sym_buy = buy_vol.groupby(grp)["buy_qty"].sum().reset_index()
    sym_sel = sell_vol.groupby(grp)["sel_qty"].sum().reset_index()
    vol = sym_buy.merge(sym_sel, on=grp, how="outer").fillna(0)
    vol["total_volume"] = vol["buy_qty"] + vol["sel_qty"]

    # Top buyer per symbol per date
    top_buy = (buy_vol.sort_values("buy_qty", ascending=False)
                      .groupby(grp).first().reset_index()
                      .rename(columns={"broker":"top_buyer","buy_qty":"top_buyer_qty"}))

    # Top seller per symbol per date
    top_sel = (sell_vol.sort_values("sel_qty", ascending=False)
                       .groupby(grp).first().reset_index()
                       .rename(columns={"broker":"top_seller","sel_qty":"top_seller_qty"}))

    vol = vol.merge(top_buy[grp+["top_buyer","top_buyer_qty"]], on=grp, how="left")
    vol = vol.merge(top_sel[grp+["top_seller","top_seller_qty"]], on=grp, how="left")

    # Add broker names if available
    if "BuyerName" in df.columns:
        bnames = df[["Buyer","BuyerName"]].drop_duplicates().rename(
            columns={"Buyer":"top_buyer","BuyerName":"top_buyer_name"})
        vol = vol.merge(bnames, on="top_buyer", how="left")
    if "SellerName" in df.columns:
        snames = df[["Seller","SellerName"]].drop_duplicates().rename(
            columns={"Seller":"top_seller","SellerName":"top_seller_name"})
        vol = vol.merge(snames, on="top_seller", how="left")
    if "Security Name" in df.columns:
        secnames = df[["Stock Symbol","Security Name"]].drop_duplicates()
        vol = vol.merge(secnames, on="Stock Symbol", how="left")

    for col in ["top_buyer_name","top_seller_name","Security Name"]:
        if col not in vol.columns:
            vol[col] = ""
        vol[col] = vol[col].fillna("")

    vol["top_buyer"]  = vol["top_buyer"].fillna(0).astype(int)
    vol["top_seller"] = vol["top_seller"].fillna(0).astype(int)

    return vol


def upsert_daily_volume(supabase, vol_df, h_df):
    """
    Upsert daily_volume table.
    vol_df = from compute_daily_volume (raw counts, all brokers)
    h_df   = aggregated holdings (for top holder — highest holding_qty)
    """
    CHUNK = 200

    # Get top holder per symbol per date from holdings
    if not h_df.empty:
        top_hold = (h_df.sort_values("holding_qty", ascending=False)
                        .groupby(["Date","Stock Symbol"]).first()
                        .reset_index()[["Date","Stock Symbol","broker",
                                        "broker_name","holding_qty","avg_rate"]]
                        .rename(columns={
                            "broker"      : "top_holder",
                            "broker_name" : "top_holder_name",
                            "holding_qty" : "top_holder_qty",
                            "avg_rate"    : "top_holder_rate",
                        }))
        vol_df = vol_df.merge(
            top_hold,
            left_on=["Date","Stock Symbol"],
            right_on=["Date","Stock Symbol"],
            how="left"
        )
    for col in ["top_holder","top_holder_qty","top_holder_rate"]:
        if col not in vol_df.columns:
            vol_df[col] = 0
    if "top_holder_name" not in vol_df.columns:
        vol_df["top_holder_name"] = ""
    vol_df["top_holder"]      = vol_df["top_holder"].fillna(0).astype(int)
    vol_df["top_holder_qty"]  = vol_df["top_holder_qty"].fillna(0)
    vol_df["top_holder_rate"] = vol_df["top_holder_rate"].fillna(0)
    vol_df["top_holder_name"] = vol_df["top_holder_name"].fillna("")

    rows = [{
        "date"            : str(r["Date"]),
        "symbol"          : str(r["Stock Symbol"]),
        "security_name"   : str(r.get("Security Name", "")),
        "total_buy_qty"   : int(r["buy_qty"]),
        "total_sel_qty"   : int(r["sel_qty"]),
        "total_volume"    : int(r["total_volume"]),
        "top_buyer"       : int(r["top_buyer"]),
        "top_buyer_name"  : str(r.get("top_buyer_name","")),
        "top_buyer_qty"   : int(r["top_buyer_qty"]),
        "top_seller"      : int(r["top_seller"]),
        "top_seller_name" : str(r.get("top_seller_name","")),
        "top_seller_qty"  : int(r["top_seller_qty"]),
        "top_holder"      : int(r["top_holder"]),
        "top_holder_name" : str(r.get("top_holder_name","")),
        "top_holder_qty"  : int(r["top_holder_qty"]),
        "top_holder_rate" : float(round(r["top_holder_rate"], 2)),
        "updated_at"      : datetime.now().isoformat(),
    } for _, r in vol_df.iterrows()]

    for i in range(0, len(rows), CHUNK):
        supabase.upsert("daily_volume", rows[i:i+CHUNK], on_conflict="date,symbol")

    print(f"      Volume: {len(rows):,} symbol-days upserted to daily_volume")


def upsert_file_to_supabase(supabase: SupabaseClient, h: pd.DataFrame,
                             broker_names: pd.DataFrame, sec_names: pd.DataFrame,
                             file_label: str):
    """
    Upsert ONE file's aggregated holdings to Supabase.
    Called per-file so we never hold all rows in memory at once.
    """
    CHUNK = 200  # smaller chunks = less memory per request

    # Attach names
    if not broker_names.empty:
        h = h.merge(broker_names, on="broker", how="left")
    if "broker_name" not in h.columns:
        h["broker_name"] = ""
    h["broker_name"] = h["broker_name"].fillna("")

    if not sec_names.empty:
        h = h.merge(sec_names, on="Stock Symbol", how="left")
    if "Security Name" not in h.columns:
        h["Security Name"] = ""
    h["Security Name"] = h["Security Name"].fillna("")

    # Only non-zero holdings
    h = h[h["holding_qty"] != 0].copy()
    if h.empty:
        print(f"    {file_label}: no non-zero holdings, skipping")
        return set()

    print(f"    {file_label}: upserting {len(h):,} rows...", end=" ", flush=True)

    # Stream to Supabase in chunks — build chunk list, not full rows list
    total = 0
    for start in range(0, len(h), CHUNK):
        chunk_df = h.iloc[start:start+CHUNK]
        chunk = []
        for _, r in chunk_df.iterrows():
            chunk.append({
                "date"          : str(r["Date"]),
                "symbol"        : str(r["Stock Symbol"]),
                "security_name" : str(r.get("Security Name","")),
                "broker"        : int(r["broker"]),
                "broker_name"   : str(r.get("broker_name","")),
                "buy_qty"       : int(r["buy_qty"]),
                "buy_amt"       : float(round(r["buy_amt"],2)),
                "total_sale_qty": int(r["total_sale_qty"]),
                "ipo_sale_qty"  : int(r["ipo_sale_qty"]),
                "bulk_sale_qty" : int(r["bulk_sale_qty"]),
                "bulk_sale_amt" : float(round(r["bulk_sale_amt"],2)),
                "holding_qty"   : int(r["holding_qty"]),
                "avg_rate"      : float(round(r["avg_rate"],2)),
            })
        supabase.upsert("holdings", chunk, on_conflict="date,symbol,broker")
        total += len(chunk)
        del chunk, chunk_df  # free immediately

    print(f"done ({total:,} rows)")
    return set(h["Stock Symbol"].unique())


def update_cumulative(supabase: SupabaseClient, symbols: set):
    """Recalculate cumulative table for given symbols by querying Supabase."""
    CHUNK = 200
    print(f"\n  Recalculating cumulative for {len(symbols)} symbol(s)...")


    for sym in sorted(symbols):
        result = supabase.select(
            "holdings",
            "broker,broker_name,buy_qty,buy_amt,total_sale_qty,ipo_sale_qty,bulk_sale_qty,bulk_sale_amt,holding_qty,security_name",
            {"symbol": sym},
        )
        if not result:
            continue

        sym_df = pd.DataFrame(result)
        for col in ["buy_qty","buy_amt","total_sale_qty","ipo_sale_qty",
                    "bulk_sale_qty","bulk_sale_amt","holding_qty"]:
            sym_df[col] = pd.to_numeric(sym_df[col], errors="coerce").fillna(0)

        cumul = (sym_df.groupby("broker")
                   .agg(broker_name   =("broker_name",    "first"),
                        security_name =("security_name",  "first"),
                        total_buy_qty =("buy_qty",         "sum"),
                        total_sale_qty=("total_sale_qty",  "sum"),
                        total_ipo_qty =("ipo_sale_qty",    "sum"),
                        total_bulk_qty=("bulk_sale_qty",   "sum"),
                        net_holding   =("holding_qty",     "sum"),
                        total_buy_amt =("buy_amt",         "sum"),
                        total_bulk_amt=("bulk_sale_amt",   "sum"))
                   .reset_index())
        cumul["avg_rate"] = ((cumul["total_buy_amt"] - cumul["total_bulk_amt"]) / cumul["net_holding"]).where(cumul["net_holding"]>0, 0).round(2)
        cumul = cumul[cumul["net_holding"] != 0]
        del sym_df

        cumul_rows = [{
            "symbol"        : sym,
            "security_name" : str(r.get("security_name","")),
            "broker"        : int(r["broker"]),
            "broker_name"   : str(r.get("broker_name","")),
            "total_buy_qty" : int(r["total_buy_qty"]),
            "total_sale_qty": int(r["total_sale_qty"]),
            "total_ipo_qty" : int(r["total_ipo_qty"]),
            "total_bulk_qty": int(r["total_bulk_qty"]),
            "net_holding"   : int(r["net_holding"]),
            "total_buy_amt" : float(round(r["total_buy_amt"],2)),
            "total_bulk_amt": float(round(r["total_bulk_amt"],2)),
            "avg_rate"      : float(round(r["avg_rate"],2)),
            "updated_at"    : datetime.now().isoformat(),
        } for _, r in cumul.iterrows()]

        for i in range(0, len(cumul_rows), CHUNK):
            supabase.upsert("cumulative", cumul_rows[i:i+CHUNK], on_conflict="symbol,broker")
        print(f"    {sym}: {len(cumul_rows)} broker positions")
        del cumul, cumul_rows

    print("  ✅ Cumulative update complete!")


def main():
    print("="*60)
    print("NEPSE Holdings Compiler  (Supabase Edition)")
    print("="*60)
    os.makedirs(DATA_DIR, exist_ok=True)
    t0 = time.time()

    # ── Connect to Supabase ───────────────────────────────────────────
    print("\nConnecting to Supabase...")
    supabase = get_supabase()
    print("  ✅ Connected!")

    # ── Process and upload ONE file at a time (avoids MemoryError) ──────
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.xlsx")))
    if not files:
        print(f"\n⚠️  No Excel files found in '{DATA_DIR}/'")
        return

    print(f"\nFound {len(files)} file(s) — processing one at a time:\n")
    broker_names = pd.DataFrame(columns=["broker","broker_name"])
    broker_names    = pd.DataFrame(columns=["broker","broker_name"])
    sec_names       = pd.DataFrame(columns=["Stock Symbol","Security Name"])
    all_symbols     = set()
    vol_frames      = []
    agg_frames_copy = []
    print("Uploading to Supabase...")
    for i, fpath in enumerate(files, 1):
        t1 = time.time()
        fname = os.path.basename(fpath)
        print(f"[{i:>2}/{len(files)}] {fname}")
        df, meta = read_and_normalise(fpath)
        if df is None:
            continue

        # Compute volume BEFORE aggregation — captures ALL brokers including zero-holding
        vol = compute_daily_volume(df)

        # Aggregate holdings
        agg = aggregate_one_file(df)

        # Collect name lookups
        if meta and "broker_names" in meta:
            broker_names = pd.concat([broker_names, meta["broker_names"]]).drop_duplicates(subset=["broker"])
        if meta and "security_names" in meta:
            sec_names = pd.concat([sec_names, meta["security_names"]]).drop_duplicates(subset=["Stock Symbol"])

        del df  # free raw data immediately

        # Keep small copy for top holder lookup in daily_volume
        # broker_name not yet attached to agg — save minimal cols only
        agg_copy = agg[["Date","Stock Symbol","broker","holding_qty","avg_rate"]].copy()
        agg_copy["broker_name"] = ""  # will be filled from broker_names lookup
        # Attach broker names if available
        if not broker_names.empty:
            agg_copy = agg_copy.merge(broker_names, on="broker", how="left")
            if "broker_name_y" in agg_copy.columns:
                agg_copy["broker_name"] = agg_copy["broker_name_y"].fillna("")
                agg_copy = agg_copy.drop(columns=["broker_name_x","broker_name_y"], errors="ignore")
            else:
                agg_copy["broker_name"] = agg_copy.get("broker_name", pd.Series([""]* len(agg_copy))).fillna("")
        agg_frames_copy.append(agg_copy)
        vol_frames.append(vol)

        # Upsert this file's holdings data to Supabase right away
        syms = upsert_file_to_supabase(supabase, agg, broker_names, sec_names, fname)
        all_symbols.update(syms)
        del agg  # free aggregated data immediately
        print(f"      Done in {time.time()-t1:.1f}s")

    # ── Upsert accurate daily volumes (skipped if table doesn't exist yet) ──
    print("\nUpserting daily_volume table (if exists)...")
    if vol_frames:
      try:
        all_vol = pd.concat(vol_frames, ignore_index=True)
        # Re-aggregate: pick top buyer/seller by max qty per date+symbol
        sym_buy = all_vol.groupby(["Date","Stock Symbol"])["buy_qty"].sum().reset_index()
        sym_sel = all_vol.groupby(["Date","Stock Symbol"])["sel_qty"].sum().reset_index()
        sym_vol = sym_buy.merge(sym_sel, on=["Date","Stock Symbol"], how="outer").fillna(0)
        sym_vol["total_volume"] = sym_vol["buy_qty"] + sym_vol["sel_qty"]
        # top buyer: broker with most buy_qty across all files for that date+symbol
        top_b = (all_vol.sort_values("top_buyer_qty",ascending=False)
                        .groupby(["Date","Stock Symbol"]).first()
                        .reset_index()[["Date","Stock Symbol","top_buyer","top_buyer_name","top_buyer_qty"]])
        top_s = (all_vol.sort_values("top_seller_qty",ascending=False)
                        .groupby(["Date","Stock Symbol"]).first()
                        .reset_index()[["Date","Stock Symbol","top_seller","top_seller_name","top_seller_qty"]])
        sym_vol = sym_vol.merge(top_b, on=["Date","Stock Symbol"], how="left")
        sym_vol = sym_vol.merge(top_s, on=["Date","Stock Symbol"], how="left")
        if "Security Name" in all_vol.columns:
            sn = all_vol[["Stock Symbol","Security Name"]].drop_duplicates()
            sym_vol = sym_vol.merge(sn, on="Stock Symbol", how="left")
        del all_vol
        # Build a combined agg df for top holder lookup
        all_agg = pd.concat(agg_frames_copy, ignore_index=True) if agg_frames_copy else pd.DataFrame()
        upsert_daily_volume(supabase, sym_vol, all_agg)
        del sym_vol, all_agg
      except Exception as e:
        if "daily_volume" in str(e) or "PGRST205" in str(e) or "404" in str(e):
            print(f"  ⚠️  daily_volume table not found — skipping. Run add_daily_volume_table.sql in Supabase to enable.")
        else:
            raise

    # ── Recalculate cumulative for all affected symbols ───────────────
    update_cumulative(supabase, all_symbols)

    # ── Generate index.html ───────────────────────────────────────────
    print(f"\nGenerating {OUTPUT_HTML}...")
    write_html()
    print(f"  Size: {os.path.getsize(OUTPUT_HTML)/1024:.1f} KB")

    print(f"\n✅ All done in {time.time()-t0:.1f}s")


def write_html():
    """Dashboard with 4 tabs — Market Summary as Tab 4."""

    SUPABASE_URL      = os.environ.get("SUPABASE_URL", "YOUR_SUPABASE_URL").rstrip("/")
    SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY", "YOUR_SUPABASE_ANON_KEY")

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>NEPSE Holdings Dashboard</title>
<link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=Outfit:wght@300;400;500;600;700&display=swap" rel="stylesheet">
<script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2/dist/umd/supabase.min.js"></script>
<style>
:root{{--bg:#060a12;--s1:#0d1420;--s2:#111d2e;--border:#1e3048;--border2:#264060;--cyan:#00c8ff;--cyan2:#0090c8;--green:#00e5a0;--red:#ff4d6a;--amber:#ffb830;--purple:#a855f7;--text:#cdd9e5;--muted:#4a6480;--muted2:#2a3f55;--mono:'IBM Plex Mono',monospace;--sans:'Outfit',sans-serif}}
*{{box-sizing:border-box;margin:0;padding:0}}
body{{background:var(--bg);color:var(--text);font-family:var(--sans);min-height:100vh;overflow-x:hidden}}
.wrap{{max-width:1440px;margin:0 auto;padding:0 28px}}
header{{padding:16px 0;border-bottom:1px solid var(--border);position:sticky;top:0;background:rgba(6,10,18,.95);backdrop-filter:blur(12px);z-index:100}}
.hdr{{display:flex;align-items:center;justify-content:space-between;gap:16px;flex-wrap:wrap}}
.brand{{display:flex;align-items:center;gap:14px}}
.brand-icon{{width:38px;height:38px;border-radius:8px;background:linear-gradient(135deg,var(--cyan),var(--purple));display:flex;align-items:center;justify-content:center;font-size:18px}}
.brand h1{{font-family:var(--mono);font-size:14px;color:var(--cyan);letter-spacing:2px;font-weight:600}}
.brand p{{font-size:11px;color:var(--muted);margin-top:2px}}
.hdr-meta{{font-family:var(--mono);font-size:10px;color:var(--muted);text-align:right;line-height:1.9}}
.hdr-meta b{{color:var(--cyan)}}
.stats{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:12px;margin:20px 0}}
.sc{{background:var(--s1);border:1px solid var(--border);border-radius:10px;padding:14px 16px;position:relative;overflow:hidden;transition:border-color .2s,transform .2s}}
.sc:hover{{border-color:var(--border2);transform:translateY(-2px)}}
.sc::after{{content:'';position:absolute;bottom:0;left:0;right:0;height:2px;background:linear-gradient(90deg,var(--cyan),var(--purple));opacity:.5}}
.sc-label{{font-size:10px;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);margin-bottom:6px}}
.sc-val{{font-family:var(--mono);font-size:22px;color:var(--cyan);font-weight:600}}
.sc-sub{{font-size:10px;color:var(--muted);margin-top:4px}}
.fp{{background:var(--s1);border:1px solid var(--border);border-radius:10px;padding:14px 18px;margin-bottom:16px;display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end}}
.fg{{display:flex;flex-direction:column;gap:4px;min-width:140px}}
.fg label{{font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--muted)}}
select,input[type=text],input[type=date]{{background:var(--s2);border:1px solid var(--border);border-radius:6px;color:var(--text);font-family:var(--sans);font-size:13px;padding:6px 10px;outline:none;transition:border-color .2s;cursor:pointer;width:100%}}
select:focus,input:focus{{border-color:var(--cyan)}}
select option{{background:var(--s2)}}
.btn{{padding:7px 16px;border-radius:6px;border:none;font-family:var(--sans);font-size:13px;font-weight:500;cursor:pointer;transition:all .2s}}
.btn-p{{background:linear-gradient(135deg,var(--cyan2),var(--purple));color:#fff}}
.btn-p:hover{{opacity:.85}}
.btn-g{{background:transparent;border:1px solid var(--border);color:var(--muted)}}
.btn-g:hover{{border-color:var(--cyan);color:var(--cyan)}}
.btns{{display:flex;gap:8px;align-items:flex-end}}
.tabs{{display:flex;gap:2px;margin-bottom:14px;border-bottom:1px solid var(--border);flex-wrap:wrap}}
.tab{{padding:9px 18px;font-size:13px;font-weight:500;color:var(--muted);cursor:pointer;border-bottom:2px solid transparent;margin-bottom:-1px;transition:all .2s;user-select:none}}
.tab:hover{{color:var(--text)}}
.tab.active{{color:var(--cyan);border-bottom-color:var(--cyan)}}
.tw{{background:var(--s1);border:1px solid var(--border);border-radius:10px;overflow:hidden;margin-bottom:16px}}
.th2{{display:flex;align-items:center;justify-content:space-between;padding:11px 16px;border-bottom:1px solid var(--border);flex-wrap:wrap;gap:8px}}
.ttitle{{font-size:13px;font-weight:600}}
.tcnt{{font-family:var(--mono);font-size:11px;color:var(--muted)}}
.tscroll{{overflow-x:auto}}
table{{width:100%;border-collapse:collapse}}
thead th{{padding:8px 12px;font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);text-align:left;background:var(--s2);cursor:pointer;user-select:none;white-space:nowrap;border-bottom:1px solid var(--border)}}
thead th:hover{{color:var(--cyan)}}
tbody tr{{border-bottom:1px solid var(--border);transition:background .1s}}
tbody tr:last-child{{border-bottom:none}}
tbody tr:hover{{background:var(--s2)}}
td{{padding:8px 12px;font-size:13px;white-space:nowrap}}
.m{{font-family:var(--mono);font-size:12px}}
.sym{{font-family:var(--mono);font-weight:700;color:var(--cyan)}}
.pos{{color:var(--green);font-family:var(--mono)}}
.neg{{color:var(--red);font-family:var(--mono)}}
.brk{{display:inline-block;background:rgba(0,200,255,.08);border:1px solid rgba(0,200,255,.2);border-radius:4px;padding:2px 6px;font-family:var(--mono);font-size:11px;color:var(--cyan)}}
.brk.sell{{background:rgba(255,77,106,.08);border-color:rgba(255,77,106,.2);color:var(--red)}}
.brk.hold{{background:rgba(0,229,160,.08);border-color:rgba(0,229,160,.2);color:var(--green)}}
.ipo{{display:inline-block;background:rgba(168,85,247,.1);border:1px solid rgba(168,85,247,.2);border-radius:4px;padding:2px 6px;font-family:var(--mono);font-size:11px;color:var(--purple)}}
.bname{{font-size:10px;color:var(--muted);max-width:120px;overflow:hidden;text-overflow:ellipsis}}
.qcell{{display:flex;align-items:center;gap:6px}}
.qbar{{flex:1;height:3px;background:var(--muted2);border-radius:2px;min-width:30px;max-width:70px}}
.qfill{{height:100%;border-radius:2px}}
.qfill.p{{background:linear-gradient(90deg,var(--cyan),var(--green))}}
.qfill.n{{background:linear-gradient(90deg,var(--red),#f97316)}}
.pag{{display:flex;align-items:center;justify-content:space-between;padding:10px 16px;border-top:1px solid var(--border);font-size:12px;color:var(--muted);flex-wrap:wrap;gap:8px}}
.pbtns{{display:flex;gap:6px}}
.pb{{padding:4px 11px;border-radius:5px;border:1px solid var(--border);background:transparent;color:var(--text);font-size:12px;cursor:pointer;transition:all .2s}}
.pb:hover:not(:disabled){{border-color:var(--cyan);color:var(--cyan)}}
.pb:disabled{{opacity:.3;cursor:not-allowed}}
.cw{{background:var(--s1);border:1px solid var(--border);border-radius:10px;padding:20px;margin-bottom:16px}}
.ctitle{{font-size:14px;font-weight:600;margin-bottom:16px}}
.barchart{{display:flex;flex-direction:column;gap:11px}}
.brow{{display:flex;align-items:center;gap:10px}}
.brank{{font-size:12px;width:24px;text-align:right;flex-shrink:0;color:var(--muted)}}
.binfo{{width:180px;flex-shrink:0}}
.binfo .bn{{font-family:var(--mono);font-size:11px;color:var(--cyan)}}
.binfo .bnn{{font-size:11px;color:var(--muted);overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.btrack{{flex:1;height:24px;background:var(--s2);border-radius:5px;overflow:hidden;border:1px solid var(--border)}}
.bfill{{height:100%;background:linear-gradient(90deg,var(--cyan2),var(--cyan));display:flex;align-items:center;padding-left:8px;font-family:var(--mono);font-size:11px;color:#fff;transition:width .6s ease;min-width:2px}}
.empty{{text-align:center;padding:40px;color:var(--muted)}}
.status-bar{{background:var(--s2);border:1px solid var(--border);border-radius:6px;padding:8px 14px;font-family:var(--mono);font-size:11px;color:var(--muted);margin-bottom:14px}}
.status-bar b{{color:var(--cyan)}}
.spinner{{width:28px;height:28px;border:2px solid var(--border);border-top-color:var(--cyan);border-radius:50%;animation:spin .7s linear infinite;margin:0 auto 10px}}
@keyframes spin{{to{{transform:rotate(360deg)}}}}
/* MARKET SUMMARY TAB */
.ms-section-title{{font-family:var(--mono);font-size:12px;color:var(--cyan);letter-spacing:1px;text-transform:uppercase;margin-bottom:12px;margin-top:20px}}
.spotlight-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:12px;margin-bottom:20px}}
.sp-card{{background:var(--s1);border:1px solid var(--border);border-radius:10px;padding:16px;position:relative;overflow:hidden;transition:border-color .2s,transform .2s}}
.sp-card:hover{{border-color:var(--border2);transform:translateY(-2px)}}
.sp-card::after{{content:'';position:absolute;bottom:0;left:0;right:0;height:2px}}
.sp-card.buy::after{{background:var(--cyan)}}
.sp-card.sell::after{{background:var(--red)}}
.sp-card.hold::after{{background:var(--green)}}
.sp-card.rate::after{{background:var(--amber)}}
.sp-label{{font-size:10px;text-transform:uppercase;letter-spacing:1.5px;color:var(--muted);margin-bottom:6px}}
.sp-broker{{font-family:var(--mono);font-size:20px;font-weight:700;margin-bottom:2px}}
.sp-broker.buy{{color:var(--cyan)}}
.sp-broker.sell{{color:var(--red)}}
.sp-broker.hold{{color:var(--green)}}
.sp-broker.rate{{color:var(--amber)}}
.sp-name{{font-size:11px;color:var(--muted);margin-bottom:4px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.sp-qty{{font-size:13px;color:var(--text);font-family:var(--mono)}}
.rank-badge{{display:inline-flex;align-items:center;justify-content:center;width:22px;height:22px;border-radius:50%;font-family:var(--mono);font-size:11px;font-weight:600}}
.rk1{{background:rgba(255,184,48,.15);color:var(--amber);border:1px solid rgba(255,184,48,.3)}}
.rk2{{background:rgba(180,178,169,.1);color:#b4b2a9;border:1px solid rgba(180,178,169,.2)}}
.rk3{{background:rgba(240,153,123,.1);color:#f0997b;border:1px solid rgba(240,153,123,.2)}}
.rkn{{background:var(--s2);color:var(--muted);border:1px solid var(--border)}}
.vol-wrap{{display:flex;align-items:center;gap:8px;min-width:120px}}
.vol-bar{{height:4px;border-radius:2px;background:var(--cyan)}}
.vol-track{{flex:1;height:4px;background:var(--muted2);border-radius:2px;max-width:60px}}
.dp-panel{{background:var(--s1);border:1px solid var(--cyan);border-radius:10px;padding:20px;margin-bottom:16px;display:none}}
.dp-panel.open{{display:block}}
.dp-header{{display:flex;align-items:center;justify-content:space-between;margin-bottom:14px;flex-wrap:wrap;gap:8px}}
.dp-sym{{font-family:var(--mono);font-size:20px;font-weight:700;color:var(--cyan)}}
.dp-close{{background:transparent;border:1px solid var(--border);color:var(--muted);padding:4px 12px;border-radius:5px;font-size:12px;cursor:pointer}}
.dp-close:hover{{border-color:var(--red);color:var(--red)}}
.dp-stats-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin-bottom:16px}}
.dp-stat{{background:var(--s2);border:1px solid var(--border);border-radius:8px;padding:12px}}
.dp-stat-label{{font-size:10px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:4px}}
.dp-stat-val{{font-family:var(--mono);font-size:18px;font-weight:600}}
.weekly-bars{{display:flex;flex-direction:column;gap:10px}}
.wrow{{display:flex;align-items:center;gap:10px}}
.wrank{{font-family:var(--mono);font-size:11px;color:var(--muted);width:20px;text-align:right;flex-shrink:0}}
.wsym{{font-family:var(--mono);font-size:12px;color:var(--cyan);width:70px;flex-shrink:0;cursor:pointer}}
.wsym:hover{{text-decoration:underline}}
.wtrack{{flex:1;height:22px;background:var(--s2);border-radius:4px;overflow:hidden;border:1px solid var(--border)}}
.wfill{{height:100%;background:linear-gradient(90deg,var(--cyan2),var(--cyan));display:flex;align-items:center;padding-left:8px;font-family:var(--mono);font-size:11px;color:#fff;min-width:2px}}
/* broker cell with name below */
.brk-cell{{display:flex;flex-direction:column;gap:2px}}
@media(max-width:768px){{.stats{{grid-template-columns:repeat(2,1fr)}}.fp{{flex-direction:column}}.fg{{min-width:100%}}.spotlight-grid{{grid-template-columns:repeat(2,1fr)}}}}
</style>
</head>
<body>
<header><div class="wrap"><div class="hdr">
  <div class="brand">
    <div class="brand-icon">📊</div>
    <div><h1>NEPSE · HOLDINGS</h1><p>Live broker-level floorsheet dashboard</p></div>
  </div>
  <div class="hdr-meta">Updated: <b id="gen-at">—</b><br>Today: <b id="today-lbl">—</b></div>
</div></div></header>

<div class="wrap" style="padding-top:20px;padding-bottom:40px">

  <!-- STAT CARDS -->
  <div class="stats">
    <div class="sc"><div class="sc-label">Positions</div><div class="sc-val" id="s-pos">—</div><div class="sc-sub">filtered results</div></div>
    <div class="sc"><div class="sc-label">Brokers</div><div class="sc-val" id="s-brk">—</div><div class="sc-sub">active in view</div></div>
    <div class="sc"><div class="sc-label">Net Holdings</div><div class="sc-val" id="s-net">—</div><div class="sc-sub">filtered total qty</div></div>
    <div class="sc"><div class="sc-label">Top Broker</div><div class="sc-val" id="s-top">—</div><div class="sc-sub" id="s-top-name">by net holding</div></div>
  </div>

  <!-- FILTERS -->
  <div class="fp">
    <div class="fg"><label>Stock Symbol ★</label>
      <select id="f-sym"><option value="">-- Select Symbol --</option></select></div>
    <div class="fg"><label>Broker #</label><input type="text" id="f-brk" placeholder="e.g. 58"></div>
    <div class="fg"><label>Broker Name</label><input type="text" id="f-bname" placeholder="e.g. Sunrise"></div>
    <div class="fg"><label>Date From</label><input type="date" id="f-dfrom"></div>
    <div class="fg"><label>Date To</label><input type="date" id="f-dto"></div>
    <div class="fg"><label>Min Holding</label><input type="text" id="f-minq" placeholder="e.g. 500"></div>
    <div class="fg"><label>Show</label>
      <select id="f-side">
        <option value="all">All positions</option>
        <option value="pos">Positive only</option>
        <option value="neg">Negative only</option>
      </select></div>
    <div class="btns">
      <button class="btn btn-p" onclick="applyFilters()">Search</button>
      <button class="btn btn-g" onclick="resetFilters()">Reset</button>
    </div>
  </div>

  <div class="status-bar" id="status">Select a stock symbol above and click Search.</div>

  <!-- TABS -->
  <div class="tabs">
    <div class="tab active" onclick="showTab('daily')">Daily Holdings</div>
    <div class="tab" onclick="showTab('cumul')">Cumulative</div>
    <div class="tab" onclick="showTab('topb')">Top Brokers Chart</div>
    <div class="tab" onclick="showTab('mkt')">Market Summary</div>
    <div class="tab" onclick="showTab('cmp')">Broker Comparison</div>
  </div>

  <!-- TAB 1: DAILY HOLDINGS -->
  <div id="tab-daily">
    <div class="tw">
      <div class="th2"><span class="ttitle">Daily Holdings per Broker per Stock</span><span class="tcnt" id="cnt-d">—</span></div>
      <div class="tscroll"><table>
        <thead><tr>
          <th onclick="srtD('date')">Date ↕</th><th onclick="srtD('symbol')">Symbol ↕</th>
          <th onclick="srtD('broker')">Broker # ↕</th><th>Broker Name</th>
          <th onclick="srtD('buy_qty')">Buy Qty ↕</th><th onclick="srtD('total_sale_qty')">Sale Qty ↕</th>
          <th onclick="srtD('ipo_sale_qty')">IPO Sale ↕</th><th onclick="srtD('bulk_sale_qty')">Bulk Sale ↕</th>
          <th onclick="srtD('holding_qty')">Net Holding ↕</th><th onclick="srtD('avg_rate')">Avg Rate ↕</th>
        </tr></thead>
        <tbody id="tbody-d"><tr><td colspan="10"><div class="empty">Select a symbol and click Search.</div></td></tr></tbody>
      </table></div>
      <div class="pag">
        <span id="pi-d">—</span>
        <div class="pbtns">
          <button class="pb" id="pp-d" onclick="chpg(-1,'d')" disabled>← Prev</button>
          <button class="pb" id="pn-d" onclick="chpg(1,'d')"  disabled>Next →</button>
        </div>
      </div>
    </div>
  </div>

  <!-- TAB 2: CUMULATIVE -->
  <div id="tab-cumul" style="display:none">
    <div class="tw">
      <div class="th2"><span class="ttitle">Cumulative Net Holdings (All Dates)</span><span class="tcnt" id="cnt-c">—</span></div>
      <div class="tscroll"><table>
        <thead><tr>
          <th>Rank</th><th onclick="csrt('symbol')">Symbol ↕</th>
          <th onclick="csrt('broker')">Broker # ↕</th><th>Broker Name</th>
          <th onclick="csrt('total_buy_qty')">Total Buy ↕</th><th onclick="csrt('total_sale_qty')">Total Sale ↕</th>
          <th onclick="csrt('total_ipo_qty')">IPO Sale ↕</th><th onclick="csrt('total_bulk_qty')">Bulk Sale ↕</th>
          <th onclick="csrt('net_holding')">Net Holding ↕</th><th onclick="csrt('avg_rate')">Avg Rate ↕</th>
        </tr></thead>
        <tbody id="tbody-c"></tbody>
      </table></div>
      <div class="pag">
        <span id="pi-c">—</span>
        <div class="pbtns">
          <button class="pb" id="pp-c" onclick="chpg(-1,'c')" disabled>← Prev</button>
          <button class="pb" id="pn-c" onclick="chpg(1,'c')"  disabled>Next →</button>
        </div>
      </div>
    </div>
  </div>

  <!-- TAB 3: TOP BROKERS CHART -->
  <div id="tab-topb" style="display:none">
    <div class="cw">
      <div class="ctitle" id="chart-title">Select a stock symbol to see top brokers</div>
      <div class="barchart" id="barchart"><div class="empty">Use the Stock Symbol filter above and click Search.</div></div>
    </div>
  </div>

  <!-- TAB 4: MARKET SUMMARY -->
  <div id="tab-mkt" style="display:none">

    <!-- Spotlight cards for highest vol script today -->
    <div class="ms-section-title" id="ms-title">Loading today's market data…</div>
    <div id="spotlight-wrap">
      <div class="empty"><div class="spinner"></div>Loading…</div>
    </div>

    <!-- High volume table today -->
    <div class="tw" style="margin-top:4px">
      <div class="th2">
        <span class="ttitle" id="vol-table-title">Top scripts by volume today</span>
        <span class="tcnt" id="vol-cnt">—</span>
      </div>
      <div class="tscroll"><table>
        <thead><tr>
          <th>#</th>
          <th>Symbol</th>
          <th onclick="sortVol('volume')">Volume (Buy Qty) ↕</th>
          <th onclick="sortVol('total_sale_qty')">Sell Qty ↕</th>
          <th onclick="sortVol('top_buyer_qty')">Top Buyer ↕</th>
          <th onclick="sortVol('top_seller_qty')">Top Seller ↕</th>
          <th onclick="sortVol('top_holder_qty')">Top Holder ↕</th>
        </tr></thead>
        <tbody id="vol-tbody"><tr><td colspan="9"><div class="empty"><div class="spinner"></div>Loading…</div></td></tr></tbody>
      </table></div>
    </div>

    <!-- Script detail panel -->
    <div class="dp-panel" id="dp-panel">
      <div class="dp-header">
        <span class="dp-sym" id="dp-sym">—</span>
        <button class="dp-close" onclick="closeDetail()">✕ Close</button>
      </div>
      <div class="dp-stats-grid" id="dp-stats"></div>
      <div class="tw">
        <div class="th2">
          <span class="ttitle">Daily breakdown — <span id="dp-sym2">—</span></span>
          <span class="tcnt" id="dp-cnt">—</span>
        </div>
        <div class="tscroll"><table>
          <thead><tr>
            <th onclick="sortDet('date')">Date ↕</th>
            <th onclick="sortDet('volume')">Volume ↕</th>
            <th onclick="sortDet('total_sale_qty')">Sell Qty ↕</th>
            <th onclick="sortDet('rank')">Rank ↕</th>
            <th onclick="sortDet('top_buyer_qty')">Top Buyer ↕</th>
            <th onclick="sortDet('top_seller_qty')">Top Seller ↕</th>
            <th onclick="sortDet('top_holder_qty')">Top Holder ↕</th>
            <th onclick="sortDet('avg_rate')">Avg Rate ↕</th>
          </tr></thead>
          <tbody id="dp-tbody"></tbody>
        </table></div>
      </div>
    </div>

    <!-- Weekly top 10 -->
    <div class="cw">
      <div class="ctitle">Top 10 scripts by volume — last 5 trading days</div>
      <div class="weekly-bars" id="weekly-bars"><div class="empty"><div class="spinner"></div>Loading…</div></div>
    </div>

  </div>

  <!-- TAB 5: BROKER COMPARISON -->
  <div id="tab-cmp" style="display:none">

    <!-- Controls -->
    <div class="tw" style="margin-bottom:14px">
      <div class="th2">
        <span class="ttitle">Broker Comparison</span>
        <span class="tcnt" id="cmp-cnt">—</span>
      </div>
      <div style="padding:14px 16px;display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end;border-bottom:1px solid var(--border)">
        <div class="fg"><label>Add Broker #</label>
          <input type="text" id="cmp-brk-input" placeholder="e.g. 42" style="width:110px"
            onkeydown="if(event.key==='Enter')addCmpBroker()"></div>
        <div class="fg"><label>Date From</label>
          <input type="date" id="cmp-dfrom"></div>
        <div class="fg"><label>Date To</label>
          <input type="date" id="cmp-dto"></div>
        <div class="btns">
          <button class="btn btn-p" onclick="addCmpBroker()">Add Broker</button>
          <button class="btn btn-p" onclick="loadCmp()">Compare</button>
          <button class="btn btn-g" onclick="clearCmpBrokers()">Clear All</button>
        </div>
      </div>
      <!-- Broker tags -->
      <div style="padding:10px 16px;min-height:42px;display:flex;flex-wrap:wrap;gap:6px;align-items:center" id="cmp-tags">
        <span style="font-size:12px;color:var(--muted)">No brokers added — type a broker number and click Add.</span>
      </div>
    </div>

    <!-- Bar chart -->
    <div class="cw" style="margin-bottom:14px">
      <div style="display:flex;flex-wrap:wrap;gap:14px;margin-bottom:12px;font-size:12px;color:var(--muted)">
        <span><span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:#185FA5;margin-right:4px"></span>Buy Qty</span>
        <span><span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:#A32D2D;margin-right:4px"></span>Sell Qty</span>
        <span><span style="display:inline-block;width:10px;height:10px;border-radius:2px;background:#3B6D11;margin-right:4px"></span>Net Holding</span>
      </div>
      <div style="position:relative;width:100%;height:300px">
        <canvas id="cmp-chart" role="img" aria-label="Grouped bar chart comparing buy sell and net holding per broker"></canvas>
      </div>
    </div>

    <!-- Combined table: qty + avg rate in same column -->
    <div class="tw">
      <div class="th2">
        <span class="ttitle" id="cmp-table-title">Comparison — select brokers and click Compare</span>
      </div>
      <div class="tscroll"><table>
        <thead><tr>
          <th onclick="sortCmp('broker')">Broker ↕</th>
          <th onclick="sortCmp('buy_qty')">Buy Qty ↕<br><span style="font-weight:400;font-size:10px;color:var(--muted)">Avg Rate</span></th>
          <th onclick="sortCmp('sell_qty')">Sell Qty ↕<br><span style="font-weight:400;font-size:10px;color:var(--muted)">Avg Rate</span></th>
          <th onclick="sortCmp('ipo_qty')">IPO Sale ↕</th>
          <th onclick="sortCmp('bulk_qty')">Bulk Sale ↕<br><span style="font-weight:400;font-size:10px;color:var(--muted)">Avg Rate</span></th>
          <th onclick="sortCmp('net_holding')">Net Holding ↕<br><span style="font-weight:400;font-size:10px;color:var(--muted)">Holding Rate</span></th>
        </tr></thead>
        <tbody id="cmp-tbody">
          <tr><td colspan="6"><div class="empty">Select a symbol, add brokers, then click Compare.</div></td></tr>
        </tbody>
      </table></div>
    </div>
  </div>

</div>

<script>
const SUPABASE_URL = "{SUPABASE_URL}";
const SUPABASE_ANON_KEY = "{SUPABASE_ANON_KEY}";
const sb = supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

let DAILY=[],CUMUL=[],FD=[],FC=[];
let dCol='holding_qty',dAsc=false,cCol='net_holding',cAsc=false;
let pg={{d:1,c:1}};
const PS=50;
let loading=false;
let VOL_DATA=[], volCol='volume', volAsc=false;
let DET_DATA=[],  detCol='date',   detAsc=false;  // default sort by date
let TODAY_STR='', mktLoaded=false;

const fmt  = n => Number(n||0).toLocaleString();
const fmtf = n => Number(n||0).toFixed(2);

function getToday(){{
  const d=new Date(), pad=n=>String(n).padStart(2,'0');
  return d.getFullYear()+'-'+pad(d.getMonth()+1)+'-'+pad(d.getDate());
}}
function getLast5(){{
  const days=[]; let d=new Date(), count=0;
  while(count<5){{
    const dow=d.getDay();
    if(dow>=1&&dow<=5){{days.push(d.toISOString().split('T')[0]);count++;}}
    d.setDate(d.getDate()-1);
  }}
  return days;
}}
function rankBadge(n){{
  const cls=n===1?'rk1':n===2?'rk2':n===3?'rk3':'rkn';
  const lbl=n===1?'🥇':n===2?'🥈':n===3?'🥉':n;
  return '<span class="rank-badge '+cls+'">'+lbl+'</span>';
}}
function brkCell(broker,name,qty,cls,rate){{
  cls=cls||'';
  const qcls = cls==='sell'?'neg':cls==='hold'?'pos':'';
  const rateHtml = (rate!==undefined && rate!==null)
    ? '<div class="m" style="color:var(--amber);font-size:10px">Rs '+fmtf(rate)+'</div>'
    : '';
  return '<div class="brk-cell"><span class="brk '+cls+'">'+broker+'</span>'
    +'<div class="bname">'+(name||'')+'</div>'
    +'<div class="m '+qcls+'">'+fmt(qty)+'</div>'
    +rateHtml+'</div>';
}}

// ── MARKET SUMMARY ─────────────────────────────────────────────────────────
async function loadMarketSummary(){{
  if(mktLoaded) return;
  TODAY_STR = getToday();
  document.getElementById('today-lbl').textContent = TODAY_STR;
  document.getElementById('gen-at').textContent    = new Date().toLocaleTimeString();

  try{{
    // ── Find the most recent date with actual data ─────────────────
    const {{data:ld, error:le}} = await sb.from('holdings')
      .select('date').order('date', {{ascending:false}}).limit(1);
    if(le) throw le;
    const LATEST_DATE = (ld && ld.length) ? ld[0].date : TODAY_STR;
    const isToday = LATEST_DATE === TODAY_STR;
    const dateLabel = isToday
      ? 'Today — ' + LATEST_DATE
      : 'Latest trading day — ' + LATEST_DATE + ' (market closed today)';
    document.getElementById('ms-title').textContent = dateLabel + ' · Loading…';
    TODAY_STR = LATEST_DATE;  // use latest date for all queries below

    // ── Fetch from holdings table directly (daily_volume table optional) ──
    const mktSym = document.getElementById('f-sym').value.trim();
    let allRows=[], offset=0, limit=1000;
    while(true){{
      let q=sb.from('holdings')
        .select('symbol,security_name,broker,broker_name,buy_qty,buy_amt,total_sale_qty,bulk_sale_qty,bulk_sale_amt,holding_qty,avg_rate')
        .eq('date',TODAY_STR).range(offset,offset+limit-1);
      if(mktSym) q=q.eq('symbol',mktSym);
      const {{data,error}}=await q;
      if(error) throw error;
      allRows.push(...(data||[]));
      if(!data||data.length<limit) break;
      offset+=limit;
    }}

    if(!allRows.length){{
      document.getElementById('spotlight-wrap').innerHTML='<div class="empty">No data for '+TODAY_STR+'.</div>';
      document.getElementById('vol-tbody').innerHTML='<tr><td colspan="7"><div class="empty">No data.</div></td></tr>';
      document.getElementById('weekly-bars').innerHTML='<div class="empty">No data.</div>';
      document.getElementById('ms-title').textContent='No data available.';
      return;
    }}

    // Aggregate per symbol from holdings rows
    const symMap={{}};
    for(const r of allRows){{
      const s=r.symbol;
      if(!symMap[s]) symMap[s]={{
        symbol:s, security_name:r.security_name||s,
        buy_qty:0, total_sale_qty:0, brokers:[]
      }};
      symMap[s].buy_qty        +=(r.buy_qty||0);
      symMap[s].total_sale_qty +=(r.total_sale_qty||0);
      symMap[s].brokers.push(r);
    }}

    VOL_DATA = Object.values(symMap).map(sm=>{{
      const B=sm.brokers;
      const tb=B.reduce((b,r)=>(r.buy_qty||0)>(b.buy_qty||0)?r:b, B[0]);
      const ts=B.reduce((b,r)=>(r.total_sale_qty||0)>(b.total_sale_qty||0)?r:b, B[0]);
      const th=B.reduce((b,r)=>(r.holding_qty||0)>(b.holding_qty||0)?r:b, B[0]);
      return {{
        symbol          : sm.symbol,
        security_name   : sm.security_name,
        volume          : sm.buy_qty,
        total_sale_qty  : sm.total_sale_qty,
        top_buyer       : tb.broker||'—',
        top_buyer_name  : tb.broker_name||'',
        top_buyer_qty   : tb.buy_qty||0,
        top_buyer_rate  : tb.avg_rate||0,
        top_seller      : ts.broker||'—',
        top_seller_name : ts.broker_name||'',
        top_seller_qty  : ts.total_sale_qty||0,
        top_seller_rate : (ts.bulk_sale_qty||0)>0 ? Math.round(((ts.bulk_sale_amt||0)/(ts.bulk_sale_qty||1))*100)/100 : (ts.avg_rate||0),
        top_holder      : th.broker||'—',
        top_holder_name : th.broker_name||'',
        top_holder_qty  : th.holding_qty||0,
        avg_rate        : th.avg_rate||0,
      }};
    }}).sort((a,b)=>b.volume-a.volume);

    document.getElementById('ms-title').textContent =
      dateLabel.replace(' · Loading…','') + ' · ' + VOL_DATA.length + ' symbols traded';
    renderSpotlight();
    renderVolTable();
    await loadWeekly();
    mktLoaded = true;

  }}catch(e){{
    console.error(e);
    document.getElementById('spotlight-wrap').innerHTML='<div class="empty">Error: '+e.message+'</div>';
  }}
}}

function renderSpotlight(){{
  if(!VOL_DATA.length) return;
  const top=VOL_DATA[0];
  document.getElementById('spotlight-wrap').innerHTML=`
    <div class="spotlight-grid">
      <div class="sp-card buy" onclick="openDetail('${{top.symbol}}')">
        <div class="sp-label">Top buyer — ${{top.symbol}}</div>
        <div class="sp-broker buy">Broker ${{top.top_buyer}}</div>
        <div class="sp-name">${{top.top_buyer_name||'—'}}</div>
        <div class="sp-qty">${{fmt(top.top_buyer_qty)}} shares bought</div>
      </div>
      <div class="sp-card sell" onclick="openDetail('${{top.symbol}}')">
        <div class="sp-label">Top seller — ${{top.symbol}}</div>
        <div class="sp-broker sell">Broker ${{top.top_seller}}</div>
        <div class="sp-name">${{top.top_seller_name||'—'}}</div>
        <div class="sp-qty">${{fmt(top.top_seller_qty)}} shares sold</div>
      </div>
      <div class="sp-card hold" onclick="openDetail('${{top.symbol}}')">
        <div class="sp-label">Top holder — ${{top.symbol}}</div>
        <div class="sp-broker hold">Broker ${{top.top_holder}}</div>
        <div class="sp-name">${{top.top_holder_name||'—'}}</div>
        <div class="sp-qty">${{fmt(top.top_holder_qty)}} shares held</div>
      </div>
      <div class="sp-card rate" onclick="openDetail('${{top.symbol}}')">
        <div class="sp-label">Avg rate of top holder</div>
        <div class="sp-broker rate">Rs ${{fmtf(top.avg_rate)}}</div>
        <div class="sp-name">Broker ${{top.top_holder}} · ${{top.top_holder_name||''}}</div>
        <div class="sp-qty">Highest vol script: ${{top.symbol}}</div>
      </div>
    </div>`;
}}

function doSort2(arr,col,asc){{
  return [...arr].sort((a,b)=>{{
    let va=a[col],vb=b[col];
    if(typeof va==='number') return asc?va-vb:vb-va;
    return asc?String(va||'').localeCompare(String(vb||'')):String(vb||'').localeCompare(String(va||''));
  }});
}}
function sortVol(col){{if(volCol===col)volAsc=!volAsc;else{{volCol=col;volAsc=false;}}renderVolTable();}}

function renderVolTable(){{
  const data=doSort2(VOL_DATA,volCol,volAsc);
  const maxV=data.length?data[0].volume:1;
  document.getElementById('vol-cnt').textContent=data.length+' symbols';
  document.getElementById('vol-table-title').textContent='Top scripts by volume — '+TODAY_STR+' (buy qty)';
  const tb=document.getElementById('vol-tbody');
  if(!data.length){{tb.innerHTML='<tr><td colspan="7"><div class="empty">No data.</div></td></tr>';return;}}
  tb.innerHTML=data.map((r,i)=>{{
    const pct=Math.max(2,r.volume/maxV*80);
    return `<tr onclick="openDetail('${{r.symbol}}')" style="cursor:pointer">
      <td>${{rankBadge(i+1)}}</td>
      <td class="sym">${{r.symbol}}</td>
      <td><div class="vol-wrap"><div class="vol-track"><div class="vol-bar" style="width:${{pct}}px"></div></div><span class="m pos">${{fmt(r.volume)}}</span></div></td>
      <td class="m neg">${{fmt(r.total_sale_qty)}}</td>
      <td>${{brkCell(r.top_buyer,r.top_buyer_name,r.top_buyer_qty,'',r.top_buyer_rate)}}</td>
      <td>${{brkCell(r.top_seller,r.top_seller_name,r.top_seller_qty,'sell',r.top_seller_rate)}}</td>
      <td>${{brkCell(r.top_holder,r.top_holder_name,r.top_holder_qty,'hold',r.avg_rate)}}</td>
    </tr>`;
  }}).join('');
}}

// ── SCRIPT DETAIL ──────────────────────────────────────────────────────────
async function openDetail(sym){{
  const panel=document.getElementById('dp-panel');
  document.getElementById('dp-sym').textContent =sym;
  document.getElementById('dp-sym2').textContent=sym;
  panel.classList.add('open');
  panel.scrollIntoView({{behavior:'smooth',block:'start'}});
  document.getElementById('dp-tbody').innerHTML=
    '<tr><td colspan="8"><div class="empty"><div class="spinner" style="margin:0 auto 8px"></div>Loading…</div></td></tr>';

  try{{
    let all=[], offset=0, limit=1000;
    while(true){{
      const {{data,error}}=await sb.from('holdings')
        .select('date,broker,broker_name,buy_qty,buy_amt,total_sale_qty,bulk_sale_qty,bulk_sale_amt,holding_qty,avg_rate')
        .eq('symbol',sym).range(offset,offset+limit-1);
      if(error) throw error;
      all.push(...(data||[]));
      if(!data||data.length<limit) break;
      offset+=limit;
    }}
    if(!all.length){{
      document.getElementById('dp-tbody').innerHTML=
        '<tr><td colspan="8"><div class="empty">No data for '+sym+'</div></td></tr>';
      return;
    }}

    // Aggregate by date
    const dm={{}};
    for(const r of all){{
      if(!dm[r.date]) dm[r.date]={{date:r.date,rows:[],buy_qty:0,total_sale_qty:0}};
      dm[r.date].buy_qty        +=(r.buy_qty||0);
      dm[r.date].total_sale_qty +=(r.total_sale_qty||0);
      dm[r.date].rows.push(r);
    }}

    DET_DATA = Object.values(dm).map(d=>{{
      const R=d.rows;
      const tb=R.reduce((b,r)=>(r.buy_qty||0)>(b.buy_qty||0)?r:b,R[0]);
      const ts=R.reduce((b,r)=>(r.total_sale_qty||0)>(b.total_sale_qty||0)?r:b,R[0]);
      const th=R.reduce((b,r)=>(r.holding_qty||0)>(b.holding_qty||0)?r:b,R[0]);
      return {{
        date           : d.date,
        volume         : d.buy_qty,
        total_sale_qty : d.total_sale_qty,
        top_buyer      : tb.broker||'—', top_buyer_name : tb.broker_name||'', top_buyer_qty  : tb.buy_qty||0,       top_buyer_rate : tb.avg_rate||0,
        top_seller     : ts.broker||'—', top_seller_name: ts.broker_name||'', top_seller_qty : ts.total_sale_qty||0, top_seller_rate: (ts.bulk_sale_qty||0)>0?Math.round(((ts.bulk_sale_amt||0)/(ts.bulk_sale_qty||1))*100)/100:(ts.avg_rate||0),
        top_holder     : th.broker||'—', top_holder_name: th.broker_name||'', top_holder_qty : th.holding_qty||0,
        avg_rate       : th.avg_rate||0,
      }};
    }});

    // Assign volume-based rank
    const ranked=[...DET_DATA].sort((a,b)=>b.volume-a.volume);
    ranked.forEach((r,i)=>r.rank=i+1);
    const rkMap={{}};
    ranked.forEach(r=>rkMap[r.date]=r.rank);
    DET_DATA.forEach(r=>r.rank=rkMap[r.date]);

    // Default sort: by date descending
    detCol='date'; detAsc=false;

    const tv=DET_DATA.reduce((s,r)=>s+r.volume,0);
    const tb2=DET_DATA.reduce((s,r)=>s+r.total_sale_qty,0);
    const peak=[...DET_DATA].sort((a,b)=>b.volume-a.volume)[0];
    document.getElementById('dp-stats').innerHTML=`
      <div class="dp-stat"><div class="dp-stat-label">Trading Days</div><div class="dp-stat-val" style="color:var(--cyan)">${{DET_DATA.length}}</div></div>
      <div class="dp-stat"><div class="dp-stat-label">Total Buy Vol</div><div class="dp-stat-val" style="color:var(--green)">${{fmt(tv)}}</div></div>
      <div class="dp-stat"><div class="dp-stat-label">Total Sell Vol</div><div class="dp-stat-val" style="color:var(--red)">${{fmt(tb2)}}</div></div>
      <div class="dp-stat"><div class="dp-stat-label">Peak Vol Date</div><div class="dp-stat-val" style="color:var(--amber);font-size:13px">${{peak?peak.date:'—'}}</div></div>
      <div class="dp-stat"><div class="dp-stat-label">Peak Volume</div><div class="dp-stat-val" style="color:var(--cyan)">${{peak?fmt(peak.volume):'—'}}</div></div>`;

    document.getElementById('dp-cnt').textContent=DET_DATA.length+' trading days';
    renderDetTable();

  }}catch(e){{
    document.getElementById('dp-tbody').innerHTML=
      '<tr><td colspan="8"><div class="empty">Error: '+e.message+'</div></td></tr>';
  }}
}}

function sortDet(col){{if(detCol===col)detAsc=!detAsc;else{{detCol=col;detAsc=false;}}renderDetTable();}}
function closeDetail(){{document.getElementById('dp-panel').classList.remove('open');}}

function renderDetTable(){{
  const data=doSort2(DET_DATA,detCol,detAsc);
  document.getElementById('dp-tbody').innerHTML=data.map(r=>
    `<tr>
      <td class="m">${{r.date}}</td>
      <td><div class="vol-wrap"><span class="m pos">${{fmt(r.volume)}}</span></div></td>
      <td class="m neg">${{fmt(r.total_sale_qty)}}</td>
      <td>${{rankBadge(r.rank)}}</td>
      <td>${{brkCell(r.top_buyer,r.top_buyer_name,r.top_buyer_qty,'',r.top_buyer_rate)}}</td>
      <td>${{brkCell(r.top_seller,r.top_seller_name,r.top_seller_qty,'sell',r.top_seller_rate)}}</td>
      <td>${{brkCell(r.top_holder,r.top_holder_name,r.top_holder_qty,'hold',r.avg_rate)}}</td>
    </tr>`).join('');
}}

// ── WEEKLY ─────────────────────────────────────────────────────────────────
async function loadWeekly(){{
  try {{
    // Step 1: Get last 5 distinct trading dates
    let dateSet=new Set(), off=0;
    while(dateSet.size<5){{
      const {{data,error}}=await sb.from('holdings')
        .select('date').order('date',{{ascending:false}})
        .range(off,off+499);
      if(error||!data||!data.length) break;
      data.forEach(r=>dateSet.add(r.date));
      off+=500;
      if(data.length<500) break;
    }}
    const last5=[...dateSet].sort().reverse().slice(0,5);
    if(!last5.length){{document.getElementById('weekly-bars').innerHTML='<div class="empty">No data.</div>';return;}}

    // Step 2: Fetch ALL buy_qty rows for those 5 dates (paginated)
    let all=[], offset=0, limit=1000;
    while(true){{
      const {{data,error}}=await sb.from('holdings')
        .select('symbol,buy_qty,date')
        .in('date',last5)
        .range(offset,offset+limit-1);
      if(error) throw error;
      all.push(...(data||[]));
      if(!data||data.length<limit) break;
      offset+=limit;
    }}

    // Step 3: Sum buy_qty per symbol across ALL 5 dates
    const sv={{}};
    for(const r of all){{if(!sv[r.symbol])sv[r.symbol]=0;sv[r.symbol]+=(r.buy_qty||0);}}
    const top10=Object.entries(sv).sort((a,b)=>b[1]-a[1]).slice(0,10);
    if(!top10.length){{document.getElementById('weekly-bars').innerHTML='<div class="empty">No data.</div>';return;}}
    const maxV=top10[0][1];
    const medals=['🥇','🥈','🥉'];
    const dateRange=last5[last5.length-1]+' → '+last5[0];
    const titleEl=document.querySelector('.weekly-section .ctitle');
    if(titleEl) titleEl.textContent='Top 10 scripts by volume — '+dateRange+' ('+last5.length+' trading days)';
    document.getElementById('weekly-bars').innerHTML=top10.map(([sym,vol],i)=>{{
      const pct=Math.max(2,vol/maxV*100);
      return `<div class="wrow">
        <div class="wrank">${{medals[i]||'#'+(i+1)}}</div>
        <div class="wsym" onclick="openDetail('${{sym}}')">${{sym}}</div>
        <div class="wtrack"><div class="wfill" style="width:${{pct}}%">${{fmt(vol)}}</div></div>
      </div>`;
    }}).join('');
  }}catch(e){{document.getElementById('weekly-bars').innerHTML='<div class="empty">Error: '+e.message+'</div>';}}
}}
function setStatus(msg,isError=false){{
  const el=document.getElementById('status');
  el.innerHTML=msg;
  el.style.color=isError?'var(--red)':'var(--muted)';
  el.style.borderColor=isError?'var(--red)':'var(--border)';
}}

async function applyFilters(){{
  if(loading) return;
  const sym=document.getElementById('f-sym').value.trim();
  const brk=document.getElementById('f-brk').value.trim();
  if(!sym && !brk){{setStatus('⚠ Please select a stock symbol or enter a broker number.',true);return;}}
  mktLoaded=false;  // reset so market summary reloads with new symbol
  const bname=document.getElementById('f-bname').value.trim().toLowerCase();
  const dfrom=document.getElementById('f-dfrom').value;
  const dto  =document.getElementById('f-dto').value;
  const minq =parseFloat(document.getElementById('f-minq').value)||null;
  const side =document.getElementById('f-side').value;
  loading=true; setStatus('Loading <b>'+sym+'</b>…');
  try{{
    // Paginate holdings fully — no row limit
    let dd=[], _off=0, _lim=1000;
    while(true){{
      let dq=sb.from('holdings').select('*')
        .order('date',{{ascending:false}}).order('holding_qty',{{ascending:false}})
        .range(_off,_off+_lim-1);
      if(sym)   dq=dq.eq('symbol',sym);
      if(brk)   dq=dq.eq('broker',parseInt(brk));
      if(dfrom) dq=dq.gte('date',dfrom);
      if(dto)   dq=dq.lte('date',dto);
      if(side==='pos') dq=dq.gt('holding_qty',0);
      if(side==='neg') dq=dq.lt('holding_qty',0);
      const {{data,error}}=await dq; if(error) throw error;
      dd.push(...(data||[]));
      if(!data||data.length<_lim) break;
      _off+=_lim;
    }}
    let cd=[];
    if(dfrom || dto) {{
      // Date filter active — aggregate cumulative from filtered holdings rows
      const filtered = (dd||[]).filter(r=>!bname||(r.broker_name||'').toLowerCase().includes(bname));
      const brkMap={{}};
      for(const r of filtered){{
        const key=r.broker;
        if(!brkMap[key]) brkMap[key]={{
          symbol:r.symbol, broker:r.broker, broker_name:r.broker_name||'',
          total_buy_qty:0, total_sale_qty:0, total_ipo_qty:0, total_bulk_qty:0,
          net_holding:0, total_buy_amt:0, total_bulk_amt:0
        }};
        brkMap[key].total_buy_qty   += (r.buy_qty||0);
        brkMap[key].total_sale_qty  += (r.total_sale_qty||0);
        brkMap[key].total_ipo_qty   += (r.ipo_sale_qty||0);
        brkMap[key].total_bulk_qty  += (r.bulk_sale_qty||0);
        brkMap[key].net_holding     += (r.holding_qty||0);
        brkMap[key].total_buy_amt   += (r.buy_amt||0);
        brkMap[key].total_bulk_amt  += (r.bulk_sale_amt||0);
      }}
      cd = Object.values(brkMap).map(b=>{{
        const net = b.net_holding;
        b.avg_rate = net>0 ? Math.round(((b.total_buy_amt - b.total_bulk_amt)/net)*100)/100 : 0;
        return b;
      }}).sort((a,b)=>b.net_holding-a.net_holding);
    }} else {{
      // No date filter — use all-time cumulative table
      let cq=sb.from('cumulative').select('*').order('net_holding',{{ascending:false}}).limit(200);
      if(sym) cq=cq.eq('symbol',sym);
      if(brk)   cq=cq.eq('broker',parseInt(brk));
      if(bname) cq=cq.ilike('broker_name','%'+bname+'%');
      const {{data:cdata,error:ce}}=await cq; if(ce) throw ce;
      cd = cdata||[];
    }}
    DAILY=(dd||[]).filter(r=>!bname||(r.broker_name||'').toLowerCase().includes(bname));
    CUMUL=cd; FD=[...DAILY]; FC=[...CUMUL];
    const net=DAILY.reduce((s,r)=>s+(r.holding_qty||0),0);
    const top=CUMUL.length?CUMUL[0]:null;
    document.getElementById('s-pos').textContent=DAILY.length.toLocaleString();
    document.getElementById('s-brk').textContent=new Set(DAILY.map(r=>r.broker)).size;
    document.getElementById('s-net').textContent=Math.round(net).toLocaleString();
    document.getElementById('s-top').textContent=top?top.broker:'—';
    document.getElementById('s-top-name').textContent=top?(top.broker_name||''):'by net holding';
    pg.d=1;pg.c=1;renderD();renderC();renderChart(sym);
    setStatus('<b>'+DAILY.length.toLocaleString()+'</b> daily positions · <b>'+CUMUL.length+'</b> cumulative'+(sym?' for <b>'+sym+'</b>':''));
  }}catch(e){{setStatus('Error: '+e.message,true);}}
  finally{{loading=false;}}
}}

function resetFilters(){{
  ['f-sym','f-side'].forEach(id=>document.getElementById(id).value='');
  ['f-brk','f-bname','f-minq'].forEach(id=>document.getElementById(id).value='');
  document.getElementById('f-dfrom').value='';
  document.getElementById('f-dto').value='';
  DAILY=[];CUMUL=[];FD=[];FC=[];pg.d=1;pg.c=1;mktLoaded=false;
  ['tbody-d','tbody-c'].forEach(id=>{{document.getElementById(id).innerHTML='<tr><td colspan="10"><div class="empty">Select a symbol and click Search.</div></td></tr>';}});
  document.getElementById('barchart').innerHTML='<div class="empty">Select a symbol and click Search.</div>';
  ['s-pos','s-brk','s-net','s-top'].forEach(id=>document.getElementById(id).textContent='—');
  setStatus('Select a stock symbol above and click Search.');
}}

function doSort(arr,col,asc){{
  return [...arr].sort((a,b)=>{{
    let va=a[col],vb=b[col];
    if(typeof va==='number') return asc?va-vb:vb-va;
    return asc?String(va||'').localeCompare(String(vb||'')):String(vb||'').localeCompare(String(va||''));
  }});
}}
function srtD(c){{if(dCol===c)dAsc=!dAsc;else{{dCol=c;dAsc=false;}}pg.d=1;renderD();}}
function csrt(c){{if(cCol===c)cAsc=!cAsc;else{{cCol=c;cAsc=false;}}pg.c=1;renderC();}}

function renderD(){{
  const data=doSort(FD,dCol,dAsc);
  const tot=data.length,pages=Math.max(1,Math.ceil(tot/PS));
  pg.d=Math.min(pg.d,pages);
  const sl=data.slice((pg.d-1)*PS,pg.d*PS);
  const maxQ=Math.max(...FD.map(r=>Math.abs(r.holding_qty||0)),1);
  document.getElementById('cnt-d').textContent=tot.toLocaleString()+' rows';
  document.getElementById('pi-d').textContent='Page '+pg.d+' of '+pages;
  document.getElementById('pp-d').disabled=pg.d<=1;
  document.getElementById('pn-d').disabled=pg.d>=pages;
  const tb=document.getElementById('tbody-d');
  if(!sl.length){{tb.innerHTML='<tr><td colspan="10"><div class="empty">No data.</div></td></tr>';return;}}
  tb.innerHTML=sl.map(r=>{{
    const hq=r.holding_qty||0,pct=Math.min(100,Math.abs(hq)/maxQ*100);
    const cls=hq>=0?'pos':'neg',fc=hq>=0?'p':'n';
    return '<tr><td class="m">'+r.date+'</td><td class="sym">'+r.symbol+'</td><td><span class="brk">'+r.broker+'</span></td><td class="bname">'+(r.broker_name||'—')+'</td><td class="m pos">'+fmt(r.buy_qty)+'</td><td class="m neg">'+fmt(r.total_sale_qty)+'</td><td><span class="ipo">'+fmt(r.ipo_sale_qty)+'</span></td><td class="m">'+fmt(r.bulk_sale_qty)+'</td><td><div class="qcell"><span class="'+cls+'">'+fmt(hq)+'</span><div class="qbar"><div class="qfill '+fc+'" style="width:'+pct+'%"></div></div></div></td><td class="m" style="color:var(--amber)">'+fmtf(r.avg_rate)+'</td></tr>';
  }}).join('');
}}

function renderC(){{
  const data=doSort(FC,cCol,cAsc);
  const tot=data.length,pages=Math.max(1,Math.ceil(tot/PS));
  pg.c=Math.min(pg.c,pages);
  const sl=data.slice((pg.c-1)*PS,pg.c*PS);
  const off=(pg.c-1)*PS;
  document.getElementById('cnt-c').textContent=tot.toLocaleString()+' rows';
  document.getElementById('pi-c').textContent='Page '+pg.c+' of '+pages;
  document.getElementById('pp-c').disabled=pg.c<=1;
  document.getElementById('pn-c').disabled=pg.c>=pages;
  const tb=document.getElementById('tbody-c');
  if(!sl.length){{tb.innerHTML='<tr><td colspan="10"><div class="empty">No data.</div></td></tr>';return;}}
  const medals=['🥇','🥈','🥉'];
  tb.innerHTML=sl.map((r,i)=>{{
    const rank=off+i+1,medal=medals[rank-1]||'#'+rank;
    const nh=r.net_holding||0,cls=nh>=0?'pos':'neg';
    return '<tr><td class="m" style="color:var(--muted)">'+medal+'</td><td class="sym">'+r.symbol+'</td><td><span class="brk">'+r.broker+'</span></td><td class="bname">'+(r.broker_name||'—')+'</td><td class="m pos">'+fmt(r.total_buy_qty)+'</td><td class="m neg">'+fmt(r.total_sale_qty)+'</td><td><span class="ipo">'+fmt(r.total_ipo_qty)+'</span></td><td class="m">'+fmt(r.total_bulk_qty)+'</td><td class="'+cls+'">'+fmt(nh)+'</td><td class="m" style="color:var(--amber)">'+fmtf(r.avg_rate)+'</td></tr>';
  }}).join('');
}}

function renderChart(sym){{
  const rows=FC.slice(0,10);
  const title=document.getElementById('chart-title');
  const chart=document.getElementById('barchart');
  if(!rows.length){{chart.innerHTML='<div class="empty">No data.</div>';return;}}
  title.textContent='Top Brokers — '+sym+' (Cumulative Net Holdings)';
  const maxV=Math.max(rows[0].net_holding,1);
  const medals=['🥇','🥈','🥉'];
  chart.innerHTML=rows.map((r,i)=>{{
    const pct=Math.max(1,r.net_holding/maxV*100);
    return '<div class="brow"><div class="brank">'+(medals[i]||'#'+(i+1))+'</div><div class="binfo"><div class="bn">Broker '+r.broker+'</div><div class="bnn">'+(r.broker_name||'—')+'</div></div><div class="btrack"><div class="bfill" style="width:'+pct+'%">'+fmt(r.net_holding)+'</div></div></div>';
  }}).join('');
}}

function showTab(name){{
  ['daily','cumul','topb','mkt','cmp'].forEach(t=>{{
    document.getElementById('tab-'+t).style.display=t===name?'':'none';
  }});
  document.querySelectorAll('.tab').forEach((el,i)=>{{
    el.classList.toggle('active',['daily','cumul','topb','mkt','cmp'][i]===name);
  }});
  if(name==='topb') renderChart(document.getElementById('f-sym').value);
  if(name==='mkt')  loadMarketSummary();
  if(name==='cmp')  initCmp();
}}
function chpg(dir,t){{pg[t]+=dir;if(t==='d')renderD();else renderC();window.scrollTo({{top:0,behavior:'smooth'}});}}

// ── INIT ──────────────────────────────────────────────────────────────────
async function init(){{
  TODAY_STR = getToday();
  document.getElementById('today-lbl').textContent = TODAY_STR;
  document.getElementById('gen-at').textContent    = new Date().toLocaleTimeString();

  try{{
    let allSymbols=[],offset=0,limit=1000;
    while(true){{
      const r=await fetch(SUPABASE_URL+'/rest/v1/cumulative?select=symbol&order=symbol.asc&limit='+limit+'&offset='+offset,
        {{headers:{{'apikey':SUPABASE_ANON_KEY,'Authorization':'Bearer '+SUPABASE_ANON_KEY}}}});
      if(!r.ok) break;
      const data=await r.json();
      if(!data.length) break;
      allSymbols.push(...data.map(d=>d.symbol));
      if(data.length<limit) break;
      offset+=limit;
    }}
    const symbols=[...new Set(allSymbols)].sort();
    const sel=document.getElementById('f-sym');
    while(sel.options.length>1) sel.remove(1);
    symbols.forEach(s=>{{sel.add(new Option(s,s));}});
  }}catch(e){{console.error('Symbol load error:',e);}}
}}

init();

// ── BROKER COMPARISON ───────────────────────────────────────────────────────
let CMP_BROKERS = [];
let cmpChart = null;

function initCmp(){{ renderCmpTags(); }}



function addCmpBroker(){{
  const val = document.getElementById('cmp-brk-input').value.trim();
  const brk = parseInt(val);
  if(!brk) return;
  if(CMP_BROKERS.find(b=>b.broker===brk)){{
    alert('Broker '+brk+' already added.');
    document.getElementById('cmp-brk-input').value='';
    return;
  }}
  CMP_BROKERS.push({{broker:brk, name:'Broker '+brk}});
  document.getElementById('cmp-brk-input').value='';
  renderCmpTags();
}}

function removeCmpBroker(brk){{
  CMP_BROKERS = CMP_BROKERS.filter(b=>b.broker!==brk);
  renderCmpTags();
}}

function clearCmpBrokers(){{
  CMP_BROKERS=[];
  renderCmpTags();
  document.getElementById('cmp-tbody').innerHTML='<tr><td colspan="10"><div class="empty">Add brokers and click Compare.</div></td></tr>';
  document.getElementById('cmp-rate-cards').innerHTML='<div class="empty">Add brokers and click Compare.</div>';
  if(cmpChartQty){{ cmpChartQty.destroy(); cmpChartQty=null; }}
  document.getElementById('cmp-cnt').textContent='—';
}}

function renderCmpTags(){{
  const wrap = document.getElementById('cmp-tags');
  if(!CMP_BROKERS.length){{
    wrap.innerHTML='<span style="font-size:12px;color:var(--muted)">No brokers added yet.</span>';
    return;
  }}
  wrap.innerHTML = CMP_BROKERS.map(b=>
    `<div style="display:inline-flex;align-items:center;gap:5px;background:var(--s2);border:1px solid var(--border);border-radius:6px;padding:3px 8px;font-size:12px">
      <span>Br ${{b.broker}}</span><span style="font-size:11px;color:var(--muted)">${{b.name}}</span>
      <span onclick="removeCmpBroker(${{b.broker}})" style="cursor:pointer;color:var(--muted);font-size:14px;line-height:1;margin-left:2px">×</span>
    </div>`
  ).join('');
}}

async function loadCmp(){{
  const sym = document.getElementById('f-sym').value.trim();
  if(!sym){{ alert('Please select a stock symbol first.'); return; }}
  if(!CMP_BROKERS.length){{ alert('Please add at least one broker.'); return; }}

  const dfrom = document.getElementById('cmp-dfrom').value;
  const dto   = document.getElementById('cmp-dto').value;
  if(!dfrom||!dto){{ alert('Please select both Date From and Date To.'); return; }}
  const mode = 'range';

  document.getElementById('cmp-cnt').textContent='Loading…';
  document.getElementById('cmp-tbody').innerHTML=
    '<tr><td colspan="6"><div class="loading"><div class="spinner"></div>Loading…</div></td></tr>';

  const brokerNums = CMP_BROKERS.map(b=>b.broker);
  let results=[];

  if(mode==='cumul'){{
    // All-time cumulative table
    const {{data,error}}=await sb.from('cumulative').select('*')
      .eq('symbol',sym).in('broker',brokerNums);
    if(error){{console.error(error);return;}}
    results=(data||[]).map(r=>{{
      const buyRate  = r.total_buy_qty>0  ? r.total_buy_amt /r.total_buy_qty  : 0;
      const sellRate = r.total_bulk_qty>0 ? r.total_bulk_amt/r.total_bulk_qty : 0;
      return {{
        broker:r.broker, name:r.broker_name||('Broker '+r.broker),
        buy_qty:r.total_buy_qty||0,       avg_buy_rate :Math.round(buyRate*100)/100,
        sell_qty:r.total_sale_qty||0,     avg_sell_rate:Math.round(sellRate*100)/100,
        ipo_qty:r.total_ipo_qty||0,
        bulk_qty:r.total_bulk_qty||0,     bulk_rate:Math.round(sellRate*100)/100,
        net_holding:r.net_holding||0,     holding_rate:r.avg_rate||0,
        label:'Cumulative',
      }};
    }});
  }} else {{
    // Date range — aggregate from holdings table
    let all=[], off=0, lim=1000;
    while(true){{
      let q=sb.from('holdings').select('*')
        .eq('symbol',sym).in('broker',brokerNums)
        .gte('date',dfrom).lte('date',dto)
        .range(off,off+lim-1);
      const {{data,error}}=await q;
      if(error){{console.error(error);break;}}
      all.push(...(data||[]));
      if(!data||data.length<lim) break;
      off+=lim;
    }}
    // Aggregate per broker
    const bmap={{}};
    for(const r of all){{
      const k=r.broker;
      if(!bmap[k]) bmap[k]={{
        broker:k,name:r.broker_name||('Broker '+k),
        buy_qty:0,buy_amt:0,sell_qty:0,ipo_qty:0,
        bulk_qty:0,bulk_amt:0,net_holding:0
      }};
      bmap[k].buy_qty     +=(r.buy_qty||0);
      bmap[k].buy_amt     +=(r.buy_amt||0);
      bmap[k].sell_qty    +=(r.total_sale_qty||0);
      bmap[k].ipo_qty     +=(r.ipo_sale_qty||0);
      bmap[k].bulk_qty    +=(r.bulk_sale_qty||0);
      bmap[k].bulk_amt    +=(r.bulk_sale_amt||0);
      bmap[k].net_holding +=(r.holding_qty||0);
    }}
    results=Object.values(bmap).map(b=>{{
      const buyRate  = b.buy_qty>0  ? b.buy_amt /b.buy_qty  : 0;
      const sellRate = b.bulk_qty>0 ? b.bulk_amt/b.bulk_qty : 0;
      const holdRate = b.net_holding>0 ? (b.buy_amt-b.bulk_amt)/b.net_holding : 0;
      return {{
        broker:b.broker, name:b.name,
        buy_qty:b.buy_qty,        avg_buy_rate :Math.round(buyRate*100)/100,
        sell_qty:b.sell_qty,      avg_sell_rate:Math.round(sellRate*100)/100,
        ipo_qty:b.ipo_qty,
        bulk_qty:b.bulk_qty,      bulk_rate:Math.round(sellRate*100)/100,
        net_holding:b.net_holding,holding_rate :Math.round(holdRate*100)/100,
        label:dfrom+' → '+dto,
      }};
    }});
  }}

  // Update known names
  results.forEach(r=>{{ const b=CMP_BROKERS.find(x=>x.broker===r.broker); if(b) b.name=r.name; }});
  renderCmpTags();

  // Fill in missing brokers as zero rows
  brokerNums.forEach(brk=>{{
    if(!results.find(r=>r.broker===brk)){{
      const b=CMP_BROKERS.find(x=>x.broker===brk)||{{broker:brk,name:'Broker '+brk}};
      results.push({{broker:brk,name:b.name,buy_qty:0,avg_buy_rate:0,
        sell_qty:0,avg_sell_rate:0,ipo_qty:0,bulk_qty:0,bulk_rate:0,
        net_holding:0,holding_rate:0,label:mode==='cumul'?'Cumulative':dfrom+' → '+dto}});
    }}
  }});
  results.sort((a,b)=>a.broker-b.broker);

  const lbl = mode==='cumul'?'Cumulative (all dates)':dfrom+' → '+dto;
  document.getElementById('cmp-cnt').textContent=results.length+' brokers · '+sym+' · '+lbl;
  document.getElementById('cmp-table-title').textContent='Broker Comparison — '+sym+' · '+lbl;

  renderCmpChart(results);
  renderCmpTable(results);
}}

function renderCmpChart(results){{
  if(cmpChart){{ cmpChart.destroy(); cmpChart=null; }}
  const labels = results.map(r=>'Br '+r.broker+' '+r.name.split(' ')[0]);
  cmpChart = new Chart(document.getElementById('cmp-chart'), {{
    type:'bar',
    data:{{
      labels,
      datasets:[
        {{label:'Buy Qty',     data:results.map(r=>r.buy_qty),     backgroundColor:'#185FA5',borderWidth:0}},
        {{label:'Sell Qty',    data:results.map(r=>r.sell_qty),    backgroundColor:'#A32D2D',borderWidth:0}},
        {{label:'Net Holding', data:results.map(r=>r.net_holding), backgroundColor:'#3B6D11',borderWidth:0}},
      ]
    }},
    options:{{
      responsive:true, maintainAspectRatio:false,
      plugins:{{legend:{{display:false}}}},
      scales:{{
        x:{{ticks:{{autoSkip:false,maxRotation:30,font:{{size:11}}}},grid:{{display:false}}}},
        y:{{ticks:{{callback:v=>v>=1000?(v/1000).toFixed(1)+'k':v,font:{{size:11}}}},
           grid:{{color:'rgba(128,128,128,0.1)'}}}}
      }}
    }}
  }});
}}

function renderCmpRateCards(results){{
  const container = document.getElementById('cmp-rate-cards');
  if(!results.length){{container.innerHTML='<div class="empty">No data.</div>';return;}}
  container.innerHTML = results.map(r=>
    `<div style="background:var(--s1);border:1px solid var(--border);border-radius:10px;padding:14px">
      <div style="font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:1px">Broker ${{r.broker}}</div>
      <div style="font-size:13px;font-weight:600;margin:4px 0 10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">${{r.name}}</div>
      <div style="font-size:12px;line-height:2;border-top:1px solid var(--border);padding-top:8px">
        <div style="display:flex;justify-content:space-between">
          <span style="color:var(--muted)">Buy qty</span>
          <span style="font-family:var(--mono);color:#00c8ff">${{fmt(r.buy_qty)}}</span>
        </div>
        <div style="display:flex;justify-content:space-between">
          <span style="color:var(--muted)">Sell qty</span>
          <span style="font-family:var(--mono);color:var(--red)">${{fmt(r.sell_qty)}}</span>
        </div>
        <div style="display:flex;justify-content:space-between">
          <span style="color:var(--muted)">Net holding</span>
          <span style="font-family:var(--mono);color:var(--green)">${{fmt(r.net_holding)}}</span>
        </div>
        <div style="border-top:1px solid var(--border);margin-top:6px;padding-top:6px">
        <div style="display:flex;justify-content:space-between">
          <span style="color:var(--muted)">Avg buy rate</span>
          <span style="font-family:var(--mono);color:var(--amber)">Rs ${{fmtf(r.avg_buy_rate)}}</span>
        </div>
        <div style="display:flex;justify-content:space-between">
          <span style="color:var(--muted)">Avg sell rate</span>
          <span style="font-family:var(--mono);color:var(--amber)">Rs ${{fmtf(r.avg_sell_rate)}}</span>
        </div>
        <div style="display:flex;justify-content:space-between">
          <span style="color:var(--muted)">Holding rate</span>
          <span style="font-family:var(--mono);color:var(--amber)">Rs ${{fmtf(r.holding_rate)}}</span>
        </div>
        </div>
      </div>
    </div>`
  ).join('');
}}

let CMP_DATA=[], cmpSortCol='broker', cmpSortAsc=true;

function sortCmp(col){{
  if(cmpSortCol===col) cmpSortAsc=!cmpSortAsc;
  else {{ cmpSortCol=col; cmpSortAsc=true; }}
  renderCmpTable(CMP_DATA);
}}

function renderCmpTable(results){{
  CMP_DATA=[...results];
  const tb=document.getElementById('cmp-tbody');
  if(!results.length){{tb.innerHTML='<tr><td colspan="6"><div class="empty">No data.</div></td></tr>';return;}}
  results=[...results].sort((a,b)=>{{
    let va=a[cmpSortCol], vb=b[cmpSortCol];
    if(typeof va==='number') return cmpSortAsc?va-vb:vb-va;
    return cmpSortAsc?String(va||'').localeCompare(String(vb||'')):String(vb||'').localeCompare(String(va||''));
  }});
  function qtyRate(qty,rate,cls){{
    return `<div class="brk-cell">
      <div class="m ${{cls}}">${{fmt(qty)}}</div>
      <div class="m" style="color:var(--amber);font-size:10px">Rs ${{fmtf(rate)}}</div>
    </div>`;
  }}
  tb.innerHTML=results.map(r=>`<tr>
    <td>
      <span class="brk">${{r.broker}}</span>
      <div class="bname">${{r.name}}</div>
    </td>
    <td>${{qtyRate(r.buy_qty,r.avg_buy_rate,'pos')}}</td>
    <td>${{qtyRate(r.sell_qty,r.avg_sell_rate,'neg')}}</td>
    <td><span class="ipo">${{fmt(r.ipo_qty)}}</span></td>
    <td>${{qtyRate(r.bulk_qty,r.bulk_rate,'')}}</td>
    <td>${{qtyRate(r.net_holding,r.holding_rate,r.net_holding>=0?'pos':'neg')}}</td>
  </tr>`).join('');
}}

function setCmpTab(tab){{
  cmpTab=tab;
  document.getElementById('ctab-qty').classList.toggle('active',tab==='qty');
  document.getElementById('ctab-rate').classList.toggle('active',tab==='rate');
  document.getElementById('cmp-qty-panel').style.display  = tab==='qty'  ? '' : 'none';
  document.getElementById('cmp-rate-panel').style.display = tab==='rate' ? '' : 'none';
}}
</script>
<script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.js"></script>
</script>
</body>
</html>"""

    html = html.replace("{SUPABASE_URL}", SUPABASE_URL)
    html = html.replace("{SUPABASE_ANON_KEY}", SUPABASE_ANON_KEY)

    with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
        f.write(html)


if __name__ == "__main__":
    main()
