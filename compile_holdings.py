import sys
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
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
    "contractId"      : "ContractId",   # used for LTP: highest contractId = last trade
    "tradeTime"       : "TradeTime",    # backup for LTP ordering
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
        """Upsert rows into a table with automatic retry on network errors."""
        import time
        headers = {**self.headers, "Prefer": "resolution=merge-duplicates,return=minimal"}
        for attempt in range(5):
            try:
                r = httpx.post(
                    f"{self.url}/rest/v1/{table}",
                    headers=headers,
                    params={"on_conflict": on_conflict},
                    content=json.dumps(rows),
                    timeout=60,
                )
                if r.status_code not in (200, 201):
                    raise Exception(f"Upsert failed [{r.status_code}]: {r.text[:300]}")
                return
            except Exception as e:
                if "status_code" not in str(type(e)) and attempt < 4:
                    wait = 15 * (attempt + 1)
                    print(f"      ⚠️  Network error (attempt {attempt+1}/5): {e}")
                    print(f"         Retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise

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


# ── Second Supabase project for broker_trades ────────────────────────────────
TRADES_URL  = "https://fmseizcubbieodvfutby.supabase.co"
TRADES_KEY  = os.environ.get("TRADES_KEY", "")

def get_trades_supabase() -> SupabaseClient:
    """Secondary Supabase client for broker_trades table."""
    return SupabaseClient(TRADES_URL, TRADES_KEY)

def get_supabase() -> SupabaseClient:
    url = os.environ.get("SUPABASE_URL","")
    key = os.environ.get("SUPABASE_KEY","")
    if not url or not key:
        raise ValueError(
            "SUPABASE_URL and SUPABASE_KEY environment variables are required.\n"
            "Set them as GitHub Secrets or locally before running."
        )
    if not url.startswith("http"):
        raise ValueError(
            f"SUPABASE_URL must start with https:// — got: {url!r}\n"
            "Check your GitHub Secrets or environment variables."
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
    h = h[(h["buy_qty"] > 0) | (h["total_sale_qty"] > 0)].copy()  # keep zero-holders with buy/sell activity
    return h


def compute_daily_volume(df):
    """
    Compute accurate daily volume per symbol from raw transactions.
    Called BEFORE dropping zero holdings — captures ALL brokers.
    Includes LTP = max(Rate) and VWAP = total_buy_amt / total_buy_qty.
    Returns: DataFrame with one row per (Date, Stock Symbol)
    """
    grp = ["Date", "Stock Symbol"]

    # Ensure Rate column exists (contractRate)
    rate_col = "Rate (Rs)" if "Rate (Rs)" in df.columns else None

    # Total buy per symbol per date
    buy_vol = (df.groupby(grp + ["Buyer"])
                 .agg(buy_qty=("Quantity","sum"),
                      buy_amt=("Amount (Rs)","sum"))
                 .reset_index()
                 .rename(columns={"Buyer":"broker"}))

    # Total sell per symbol per date
    sell_vol = (df.groupby(grp + ["Seller"])["Quantity"].sum()
                  .reset_index()
                  .rename(columns={"Seller":"broker","Quantity":"sel_qty"}))

    # Symbol-level totals
    sym_buy = buy_vol.groupby(grp).agg(
        buy_qty=("buy_qty","sum"),
        buy_amt=("buy_amt","sum")
    ).reset_index()
    sym_sel = sell_vol.groupby(grp)["sel_qty"].sum().reset_index()
    vol = sym_buy.merge(sym_sel, on=grp, how="outer").fillna(0)
    vol["total_volume"] = vol["buy_qty"] + vol["sel_qty"]

    # VWAP = total buy amount / total buy qty
    vol["vwap"] = (vol["buy_amt"] / vol["buy_qty"].replace(0, float("nan"))).round(2).fillna(0)

    # LTP = contractRate of the row with highest contractId per symbol per date
    # contractId is a sequential number — highest = last transaction of the day
    if "ContractId" in df.columns:
        # Get the rate of the last transaction (max contractId) per symbol per date
        df["ContractId"] = pd.to_numeric(df["ContractId"], errors="coerce").fillna(0)
        ltp_idx = df.groupby(grp)["ContractId"].idxmax()
        ltp = df.loc[ltp_idx, grp + ["Rate (Rs)"]].rename(columns={"Rate (Rs)": "ltp"})
        vol = vol.merge(ltp, on=grp, how="left")
        vol["ltp"] = vol["ltp"].fillna(vol["vwap"]).round(2)
        print(f"    LTP computed from contractId for {len(vol)} symbols")
    elif "TradeTime" in df.columns:
        # Fallback: use tradeTime to find last transaction
        df["TradeTime"] = pd.to_datetime(df["TradeTime"], errors="coerce")
        ltp_idx = df.groupby(grp)["TradeTime"].idxmax()
        ltp = df.loc[ltp_idx, grp + ["Rate (Rs)"]].rename(columns={"Rate (Rs)": "ltp"})
        vol = vol.merge(ltp, on=grp, how="left")
        vol["ltp"] = vol["ltp"].fillna(vol["vwap"]).round(2)
        print(f"    LTP computed from tradeTime for {len(vol)} symbols")
    else:
        # Final fallback: VWAP
        vol["ltp"] = vol["vwap"]
        print(f"    LTP fallback to VWAP (no contractId/tradeTime found)")

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

    # Broker names
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
        "total_buy_amt"   : float(round(r.get("buy_amt", 0), 2)),
        "ltp"             : float(round(r.get("ltp", 0), 2)),
        "vwap"            : float(round(r.get("vwap", 0), 2)),
        "top_buyer"       : int(r["top_buyer"]),
        "top_buyer_name"  : str(r.get("top_buyer_name","")),
        "top_buyer_qty"   : int(r["top_buyer_qty"]),
        "top_seller"      : int(r["top_seller"]),
        "top_seller_name" : str(r.get("top_seller_name","")),
        "top_seller_qty"  : int(r["top_seller_qty"]),
        "top_holder"      : int(r.get("top_holder", 0)),
        "top_holder_name" : str(r.get("top_holder_name","")),
        "top_holder_qty"  : int(r.get("top_holder_qty", 0)),
        "top_holder_rate" : float(round(r.get("top_holder_rate", 0), 2)),
        "updated_at"      : datetime.now().isoformat(),
    } for _, r in vol_df.iterrows()]

    for i in range(0, len(rows), CHUNK):
        supabase.upsert("daily_volume", rows[i:i+CHUNK], on_conflict="date,symbol")

    print(f"      Volume: {len(rows):,} symbol-days upserted to daily_volume")


def compute_broker_trades(df):
    """
    Compute buyer-seller pair aggregates from raw transactions.
    Returns DataFrame with (Date, Stock Symbol, Buyer, Seller, qty, amount).
    """
    grp = ["Date", "Stock Symbol", "Buyer", "Seller"]
    required = all(c in df.columns for c in ["Buyer", "Seller", "Quantity", "Amount (Rs)"])
    if not required:
        return pd.DataFrame()

    trades = (df.groupby(grp)
                .agg(qty   =("Quantity",    "sum"),
                     amount=("Amount (Rs)", "sum"))
                .reset_index())
    trades["Buyer"]  = trades["Buyer"].astype(int)
    trades["Seller"] = trades["Seller"].astype(int)
    trades = trades[trades["qty"] > 0]
    return trades


def upsert_broker_trades(supabase, trades_df):
    """Upsert broker-seller pairs to broker_trades table."""
    if trades_df.empty:
        return
    CHUNK = 300
    rows = [{
        "date"  : str(r["Date"]),
        "symbol": str(r["Stock Symbol"]),
        "buyer" : int(r["Buyer"]),
        "seller": int(r["Seller"]),
        "qty"   : int(r["qty"]),
        "amount": float(round(r["amount"], 2)),
    } for _, r in trades_df.iterrows()]

    for i in range(0, len(rows), CHUNK):
        supabase.upsert("broker_trades", rows[i:i+CHUNK],
                        on_conflict="date,symbol,buyer,seller")
    print(f"      Trades: {len(rows):,} buyer-seller pairs upserted")


def compute_and_upsert_accumulation(supabase, date_str, all_holdings_df):
    """
    Calls server-side SQL function compute_accumulation() which runs entirely
    in Postgres — no data fetching, instant execution.
    """
    try:
        df = all_holdings_df.copy()
        df["Date"] = df["Date"].astype(str)
        dates = sorted(df["Date"].unique(), reverse=True)
        if len(dates) < 2:
            print("    Accumulation: need at least 2 dates, skipping")
            return

        today_str = dates[0]
        prev_str  = dates[1]
        print(f"    Computing accumulation: {prev_str} -> {today_str}")

        import httpx

        # Call the server-side SQL function via RPC
        hdrs = {**supabase.headers, "Prefer": "return=minimal"}
        r = httpx.post(
            f"{supabase.url}/rest/v1/rpc/compute_accumulation",
            headers=hdrs,
            json={
                "p_today"   : today_str,
                "p_prev"    : prev_str,
                "p_min_pct" : 10
            },
            timeout=300,  # 5 minute timeout
        )

        if r.status_code in (200, 204):
            print(f"    Accumulation: computed for {today_str}")
        elif "compute_accumulation" in r.text or "42883" in r.text:
            print(f"    Accumulation: function not found -- run create_accumulation_function.sql in Supabase")
        else:
            print(f"    Accumulation error: [{r.status_code}] {r.text[:200]}")

    except Exception as e:
        if "accumulation" in str(e) or "42P01" in str(e):
            print(f"    Accumulation table not found -- run create_accumulation_table.sql")
        else:
            print(f"    Accumulation error: {e}")


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
    h = h[(h["buy_qty"] > 0) | (h["total_sale_qty"] > 0)].copy()  # keep zero-holders with buy/sell activity
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

        # Compute buyer-seller pairs from raw transactions
        # broker_trades disabled to save DB space
        # trades = compute_broker_trades(df)

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
        # Compute and upsert broker_trades to SECOND Supabase project
        try:
            trades = compute_broker_trades(df)
            trades_sb = get_trades_supabase()
            upsert_broker_trades(trades_sb, trades)
            del trades
        except Exception as e:
            print(f"      ⚠️  broker_trades (second project) skipped: {e}")


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
        grp = ["Date","Stock Symbol"]

        # Sum qty/amt across all files per date+symbol
        sym_agg = all_vol.groupby(grp).agg(
            buy_qty      =("buy_qty",       "sum"),
            buy_amt      =("buy_amt",        "sum"),
            sel_qty      =("sel_qty",        "sum"),
            total_volume =("total_volume",   "sum"),
            ltp          =("ltp",            "last"),  # last contractId's rate = true LTP
        ).reset_index()
        sym_agg["vwap"] = (sym_agg["buy_amt"] / sym_agg["buy_qty"].replace(0, float("nan"))).round(2).fillna(0)
        sym_agg["total_volume"] = sym_agg["buy_qty"] + sym_agg["sel_qty"]

        # Top buyer/seller: broker with most qty across all files
        top_b = (all_vol.sort_values("top_buyer_qty", ascending=False)
                        .drop_duplicates(subset=grp)
                        [grp+["top_buyer","top_buyer_name","top_buyer_qty"]])
        top_s = (all_vol.sort_values("top_seller_qty", ascending=False)
                        .drop_duplicates(subset=grp)
                        [grp+["top_seller","top_seller_name","top_seller_qty"]])

        sym_vol = sym_agg.merge(top_b, on=grp, how="left")
        sym_vol = sym_vol.merge(top_s, on=grp, how="left")

        if "Security Name" in all_vol.columns:
            sn = all_vol[["Stock Symbol","Security Name"]].drop_duplicates(subset=["Stock Symbol"])
            sym_vol = sym_vol.merge(sn, on="Stock Symbol", how="left")

        # Final dedup just in case
        sym_vol = sym_vol.drop_duplicates(subset=grp).reset_index(drop=True)
        del all_vol

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

    # Compute and store accumulation data for today
    if agg_frames_copy:
        try:
            all_h = pd.concat(agg_frames_copy, ignore_index=True)
            today_date = str(all_h["Date"].max()) if "Date" in all_h.columns else datetime.now().strftime("%Y-%m-%d")
            compute_and_upsert_accumulation(supabase, today_date, all_h)
            del all_h
        except Exception as e:
            print(f"  Accumulation error: {e}")

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
.vol-item{{margin-bottom:12px;border:1px solid var(--border);border-radius:8px;overflow:hidden}}
.vol-item-hdr{{display:flex;align-items:center;gap:10px;padding:8px 12px;background:var(--s2);cursor:pointer;user-select:none}}
.vol-item-hdr:hover{{background:var(--border)}}
.vol-rank{{font-family:var(--mono);font-size:12px;color:var(--muted);width:24px;flex-shrink:0}}
.vol-sym{{font-family:var(--mono);font-size:13px;font-weight:700;color:var(--cyan);width:70px;flex-shrink:0}}
.vol-bar-wrap{{flex:1;display:flex;align-items:center;gap:8px}}
.vol-holders{{padding:8px 12px;display:none;background:var(--s1)}}
.vol-holders.open{{display:block}}
.holder-table{{width:100%;border-collapse:collapse;font-size:12px}}
.holder-table th{{padding:5px 8px;color:var(--muted);font-size:10px;text-transform:uppercase;letter-spacing:1px;text-align:left;border-bottom:1px solid var(--border)}}
.holder-table td{{padding:5px 8px;border-bottom:1px solid var(--border);font-family:var(--mono)}}
.holder-table tr:last-child td{{border-bottom:none}}
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
    <div class="tab" onclick="showTab('mds')">⚠ Manipulation Detection</div>
  </div>

  <!-- TAB 1: DAILY HOLDINGS -->
  <div id="tab-daily">
    <div class="tw">
      <div class="th2"><span class="ttitle">Daily Holdings per Broker per Stock</span><span class="tcnt" id="cnt-d">—</span></div>
      <div class="tscroll"><table>
        <thead><tr>
          <th style="width:36px">#</th>
          <th onclick="srtD('date')">Date ↕</th><th onclick="srtD('symbol')">Symbol ↕</th>
          <th onclick="srtD('broker')">Broker # ↕</th><th>Broker Name</th>
          <th onclick="srtD('buy_qty')">Buy Qty ↕</th><th onclick="srtD('total_sale_qty')">Sale Qty ↕</th>
          <th onclick="srtD('ipo_sale_qty')">IPO Sale ↕</th><th onclick="srtD('bulk_sale_qty')">Bulk Sale ↕</th>
          <th onclick="srtD('holding_qty')">Net Holding ↕</th><th onclick="srtD('avg_rate')">Avg Rate ↕</th>
        </tr>
        <tr id="daily-summary" style="display:none;background:var(--s2);font-weight:600;border-top:2px solid var(--border2)">
          <td colspan="5" class="m" style="color:var(--muted);font-size:11px" id="daily-sum-label">∑ Filtered total</td>
          <td class="m pos" id="daily-sum-buy">—</td>
          <td class="m neg" id="daily-sum-sale">—</td>
          <td id="daily-sum-ipo" style="color:var(--purple)">—</td>
          <td class="m" id="daily-sum-bulk">—</td>
          <td class="m" id="daily-sum-hold">—</td>
          <td class="m" style="color:var(--amber)" id="daily-sum-rate">—</td>
        </tr>
        </thead>
        <tbody id="tbody-d"><tr><td colspan="11"><div class="empty">Select a symbol and click Search.</div></td></tr></tbody>
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
        </tr>
        <tr id="cumul-summary" style="display:none;background:var(--s2);font-weight:600;border-top:2px solid var(--border2)">
          <td colspan="4" class="m" style="color:var(--muted);font-size:11px">∑ Filtered total</td>
          <td class="m pos" id="cs-buy">—</td>
          <td class="m neg" id="cs-sale">—</td>
          <td id="cs-ipo" style="color:var(--purple)">—</td>
          <td class="m" id="cs-bulk">—</td>
          <td class="m" id="cs-hold">—</td>
          <td class="m" style="color:var(--amber)" id="cs-rate">—</td>
        </tr>
        </thead>
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

    <!-- Cumulative drill-down panel -->
    <div id="cumul-drill" style="display:none;margin-top:14px">
      <div class="tw" style="border-color:var(--cyan)">
        <div class="th2" style="border-bottom:1px solid var(--border)">
          <span class="ttitle" id="cumul-drill-title">Daily breakdown</span>
          <button class="btn btn-g" style="font-size:11px;padding:4px 10px"
            onclick="document.getElementById('cumul-drill').style.display='none'">✕ Close</button>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;padding:12px">
          <!-- Top 10 Holdings -->
          <div>
            <div style="font-size:11px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:8px;font-weight:500">
              Top 10 holding days
            </div>
            <div class="tscroll"><table>
              <thead><tr>
                <th>#</th>
                <th onclick="sortCumulDrill('h','date')">Date ↕</th>
                <th onclick="sortCumulDrill('h','holding_qty')">Holding ↕<br><span style="font-weight:400;font-size:10px;color:var(--muted)">Avg Rate</span></th>
                <th onclick="sortCumulDrill('h','buy_qty')">Buy Qty ↕</th>
              </tr></thead>
              <tbody id="cumul-drill-hold-tbody">
                <tr><td colspan="4"><div class="empty">Click a row above.</div></td></tr>
              </tbody>
            </table></div>
          </div>
          <!-- Top 10 Sales -->
          <div>
            <div style="font-size:11px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:8px;font-weight:500">
              Top 10 selling days
            </div>
            <div class="tscroll"><table>
              <thead><tr>
                <th>#</th>
                <th onclick="sortCumulDrill('s','date')">Date ↕</th>
                <th onclick="sortCumulDrill('s','total_sale_qty')">Sell Qty ↕<br><span style="font-weight:400;font-size:10px;color:var(--muted)">Avg Rate</span></th>
                <th onclick="sortCumulDrill('s','ipo_sale_qty')">IPO ↕</th>
              </tr></thead>
              <tbody id="cumul-drill-sale-tbody">
                <tr><td colspan="4"><div class="empty">Click a row above.</div></td></tr>
              </tbody>
            </table></div>
          </div>
        </div>
      </div>
    </div>
  </div>

  <!-- TAB 3: TOP BROKERS CHART + ALL BROKERS TABLE -->
  <div id="tab-topb" style="display:none">
    <div class="cw" style="margin-bottom:14px">
      <div class="ctitle" id="chart-title">Select a stock symbol to see top brokers</div>
      <div class="barchart" id="barchart"><div class="empty">Use the Stock Symbol filter above and click Search.</div></div>
    </div>
    <!-- All brokers total net holding table — loads automatically, no symbol needed -->
    <div class="tw">
      <div class="th2">
        <span class="ttitle">All brokers — total net holding (all scripts combined)</span>
        <span class="tcnt" id="topb-table-cnt"></span>
      </div>
      <div style="padding:6px 12px 10px;border-bottom:1px solid var(--border)">
        <input type="text" id="topb-filter-brk" placeholder="Filter by broker # or name..."
          style="width:240px" oninput="filterTopbTable()">
      </div>
      <div class="tscroll"><table>
        <thead><tr>
          <th>#</th>
          <th onclick="sortTopbTable('broker')">Broker ↕</th>
          <th onclick="sortTopbTable('broker_name')">Broker Name ↕</th>
          <th onclick="sortTopbTable('total_buy_qty')">Total Buy ↕</th>
          <th onclick="sortTopbTable('total_sale_qty')">Total Sale ↕</th>
          <th onclick="sortTopbTable('total_ipo_qty')">IPO Sale ↕</th>
          <th onclick="sortTopbTable('total_bulk_qty')">Bulk Sale ↕</th>
          <th onclick="sortTopbTable('net_holding')">Net Holding ↕</th>
          <th onclick="sortTopbTable('scripts')">Scripts ↕</th>
        </tr></thead>
        <tbody id="topb-table-tbody">
          <tr><td colspan="9"><div class="loading"><div class="spinner"></div>Loading…</div></td></tr>
        </tbody>
      </table></div>
    </div>
  </div>

  <!-- TAB 4: MARKET SUMMARY -->
  <div id="tab-mkt" style="display:none">

    <!-- Gainers / Losers -->
    <div id="gl-section" style="margin-bottom:16px">
      <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px">
        <div class="tw">
          <div class="th2" style="border-bottom:1px solid var(--border)">
            <span class="ttitle" style="color:var(--green)">▲ Top 10 Gainers</span>
            <span class="tcnt" id="gl-date-label">—</span>
          </div>
          <div class="tscroll"><table style="min-width:0">
            <thead><tr>
              <th style="width:28px">#</th>
              <th>Symbol</th>
              <th>LTP</th>
              <th>Change %</th>
              <th>Points</th>
            </tr></thead>
            <tbody id="gainers-tbody">
              <tr><td colspan="5"><div class="empty"><div class="spinner"></div></div></td></tr>
            </tbody>
          </table></div>
        </div>
        <div class="tw">
          <div class="th2" style="border-bottom:1px solid var(--border)">
            <span class="ttitle" style="color:var(--red)">▼ Top 10 Losers</span>
            <span class="tcnt" id="gl-date-label2">—</span>
          </div>
          <div class="tscroll"><table style="min-width:0">
            <thead><tr>
              <th style="width:28px">#</th>
              <th>Symbol</th>
              <th>LTP</th>
              <th>Change %</th>
              <th>Points</th>
            </tr></thead>
            <tbody id="losers-tbody">
              <tr><td colspan="5"><div class="empty"><div class="spinner"></div></div></td></tr>
            </tbody>
          </table></div>
        </div>
      </div>
    </div>

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
            <th onclick="sortDet('ltp')">LTP ↕</th>
          </tr></thead>
          <tbody id="dp-tbody"></tbody>
        </table></div>
      </div>
    </div>

    <!-- Accumulation detector -->
    <div class="cw" style="margin-bottom:14px">
      <div class="ctitle">Brokers accumulating — holding up ≥10% vs previous day
        <span id="accum-date-label" style="font-size:11px;color:var(--muted);margin-left:8px"></span>
      </div>
      <div style="display:flex;flex-wrap:wrap;gap:8px;padding:10px 0;align-items:center;border-bottom:1px solid var(--border);margin-bottom:8px">
        <input type="text" id="accum-filter-sym" placeholder="Symbol..." style="width:110px" oninput="filterAccumTable()">
        <input type="text" id="accum-filter-brk" placeholder="Broker #..." style="width:100px" oninput="filterAccumTable()">
        <select id="accum-filter-date" onchange="loadAccumByDate()" style="font-size:12px;width:130px">
          <option value="">All dates</option>
        </select>
        <select id="accum-filter-minhold" onchange="filterAccumTable()" style="font-size:12px">
          <option value="5000">Prev &gt; 5,000</option>
          <option value="0">All holdings</option>
          <option value="1000">Prev &gt; 1,000</option>
          <option value="10000">Prev &gt; 10,000</option>
          <option value="50000">Prev &gt; 50,000</option>
        </select>
        <button class="btn btn-g" style="font-size:11px;padding:4px 10px" onclick="clearAccumFilters()">Clear</button>
        <span id="accum-filter-cnt" style="font-size:11px;color:var(--muted)"></span>
      </div>
      <div id="accum-wrap">
        <div class="empty"><div class="spinner"></div>Loading…</div>
      </div>
    </div>

    <!-- Daily top 10 + top 5 holders -->
    <div class="cw" style="margin-bottom:14px">
      <div class="ctitle" id="daily-vol-title">Top 10 scripts by volume — today</div>
      <div id="daily-vol-list"><div class="empty"><div class="spinner"></div>Loading…</div></div>
    </div>

    <!-- Weekly top 10 + top 5 holders -->
    <div class="cw">
      <div class="ctitle">Top 10 scripts by volume — last 5 trading days</div>
      <div id="weekly-bars"><div class="empty"><div class="spinner"></div>Loading…</div></div>
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
          <th onclick="sortCmp('ltp')">LTP ↕<br><span style="font-weight:400;font-size:10px;color:var(--muted)">Last traded price</span></th>
        <tr id="cmp-summary" style="display:none;background:var(--s2);font-weight:600;border-top:2px solid var(--border2)">
          <td class="m" style="color:var(--muted);font-size:11px" id="cmp-sum-label">∑ Total</td>
          <td><div class="brk-cell"><div class="m pos" id="cmp-sum-buy">—</div><div style="color:var(--amber);font-size:10px" id="cmp-sum-buy-rate">—</div></div></td>
          <td><div class="brk-cell"><div class="m neg" id="cmp-sum-sell">—</div><div style="color:var(--amber);font-size:10px" id="cmp-sum-sell-rate">—</div></div></td>
          <td><span class="ipo" id="cmp-sum-ipo">—</span></td>
          <td><div class="brk-cell"><div class="m" id="cmp-sum-bulk">—</div><div style="color:var(--amber);font-size:10px" id="cmp-sum-bulk-rate">—</div></div></td>
          <td><div class="brk-cell"><div class="m" id="cmp-sum-hold">—</div><div style="color:var(--amber);font-size:10px" id="cmp-sum-hold-rate">—</div></div></td>
          <td class="m" style="color:var(--amber)" id="cmp-sum-ltp">—</td>
        </tr>
        </tr></thead>
        <tbody id="cmp-tbody">
          <tr><td colspan="6"><div class="empty">Select a symbol, add brokers, then click Compare.</div></td></tr>
        </tbody>
      </table></div>
    </div>

    <!-- Top brokers for selected symbol -->
    <div class="tw" style="margin-top:14px">
      <div class="th2">
        <span class="ttitle">Top buyers of selected symbol — all brokers</span>
        <span class="tcnt" id="tb-cnt">—</span>
      </div>
      <div class="tscroll"><table>
        <thead><tr>
          <th onclick="sortTb('rank')"># ↕</th>
          <th onclick="sortTb('broker')">Broker ↕</th>
          <th onclick="sortTb('buy_qty')">Buy Qty ↕<br><span style="font-weight:400;font-size:10px;color:var(--muted)">Avg Rate</span></th>
          <th onclick="sortTb('sale_qty')">Sell Qty ↕<br><span style="font-weight:400;font-size:10px;color:var(--muted)">Avg Rate</span></th>
          <th onclick="sortTb('ipo_qty')">IPO Sale ↕</th>
          <th onclick="sortTb('bulk_qty')">Bulk Sale ↕<br><span style="font-weight:400;font-size:10px;color:var(--muted)">Avg Rate</span></th>
          <th onclick="sortTb('holding_qty')">Net Holding ↕<br><span style="font-weight:400;font-size:10px;color:var(--muted)">Avg Rate</span></th>
          <th onclick="sortTb('market_pct')">Hold % ↕</th>
        </tr></thead>
        <tbody id="tb-tbody">
          <tr><td colspan="8"><div class="empty">Select a symbol, set date range and click Compare.</div></td></tr>
        </tbody>
      </table></div>
    </div>

    <!-- Seller drill-down panel (opens when clicking buy qty) -->
    <div id="seller-panel" style="display:none;margin-top:14px">
      <div class="tw" style="border-color:var(--cyan)">
        <div class="th2" style="border-bottom:1px solid var(--border)">
          <span class="ttitle" id="seller-title">Who sold to Broker — ?</span>
          <button class="btn btn-g" style="font-size:11px;padding:4px 10px"
            onclick="document.getElementById('seller-panel').style.display='none'">✕ Close</button>
        </div>
        <div class="tscroll"><table>
          <thead><tr>
            <th onclick="sortSeller('rank')"># ↕</th>
            <th onclick="sortSeller('seller')">Seller Broker ↕</th>
            <th onclick="sortSeller('qty')">Qty Sold ↕<br><span style="font-weight:400;font-size:10px;color:var(--muted)">Avg Rate</span></th>
            <th onclick="sortSeller('buyer_pct')" id="seller-pct-hdr">Sold to Buyer %<br><span style="font-weight:400;font-size:10px;color:var(--muted)">of buyer total</span></th>
          </tr></thead>
          <tbody id="seller-tbody">
            <tr><td colspan="4"><div class="empty">Click a Buy Qty cell in the table above.</div></td></tr>
          </tbody>
        </table></div>
      </div>
    </div>

    <!-- Common stocks table — per broker columns -->
    <div class="tw" style="margin-top:14px">
      <div class="th2">
        <span class="ttitle">Stocks traded — per broker breakdown</span>
        <span class="tcnt" id="cs-cnt">—</span>
      </div>
      <div class="tscroll" id="cs-table-wrap">
        <div class="empty">Add brokers, select date range and click Compare.</div>
      </div>
    </div>

  </div>

  <!-- TAB 6: MANIPULATION DETECTION -->
  <div id="tab-mds" style="display:none">

    <!-- Controls -->
    <div class="tw" style="margin-bottom:14px">
      <div class="th2">
        <span class="ttitle">⚠ NEPSE EOD Manipulation Detection</span>
        <span class="tcnt" id="mds-cnt">—</span>
      </div>
      <div style="padding:14px 16px;display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end;border-bottom:1px solid var(--border)">
        <div class="fg"><label>Analysis Window</label>
          <select id="mds-window">
            <option value="5">Last 5 trading days (default)</option>
            <option value="10">Last 10 trading days</option>
            <option value="today">Today only</option>
          </select></div>
        <div class="fg"><label>Min Risk Score</label>
          <select id="mds-minscore">
            <option value="0">All scripts</option>
            <option value="4">Watchlist (4+)</option>
            <option value="7">Suspicious (7+)</option>
            <option value="10">Highly Suspicious (10+)</option>
          </select></div>
        <div class="btns">
          <button class="btn btn-p" onclick="loadMDS()">🔍 Analyse</button>
        </div>
      </div>
      <!-- Legend -->
      <div style="padding:10px 16px;display:flex;flex-wrap:wrap;gap:14px;font-size:12px">
        <span style="color:var(--muted)">Risk level:</span>
        <span><span style="background:#2d5a27;color:#7fff6e;border-radius:4px;padding:1px 6px">0–3 Normal</span></span>
        <span><span style="background:#5a4a00;color:#ffd700;border-radius:4px;padding:1px 6px">4–6 Watchlist</span></span>
        <span><span style="background:#5a2000;color:#ff8c42;border-radius:4px;padding:1px 6px">7–9 Suspicious</span></span>
        <span><span style="background:#5a0000;color:#ff4444;border-radius:4px;padding:1px 6px">≥10 Highly Suspicious</span></span>
      </div>
    </div>

    <!-- Summary cards -->
    <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:14px" id="mds-cards">
    </div>

    <!-- Main detection table -->
    <div class="tw" style="margin-bottom:14px">
      <div class="th2"><span class="ttitle">Detection Results — all scripts scored</span></div>
      <div class="tscroll"><table>
        <thead><tr>
          <th onclick="sortMds('rank')"># ↕</th>
          <th onclick="sortMds('symbol')">Symbol ↕</th>
          <th onclick="sortMds('score')">Risk Score ↕</th>
          <th>Risk Level</th>
          <th onclick="sortMds('vol_spike')">Vol Spike ↕<br><span style="font-size:9px;font-weight:400;color:var(--muted)">ratio vs avg</span></th>
          <th onclick="sortMds('price_chg')">Price Chg ↕<br><span style="font-size:9px;font-weight:400;color:var(--muted)">% change</span></th>
          <th onclick="sortMds('rising_days')">Rising Days ↕<br><span style="font-size:9px;font-weight:400;color:var(--muted)">consecutive</span></th>
          <th onclick="sortMds('vwap_dev')">VWAP Dev ↕<br><span style="font-size:9px;font-weight:400;color:var(--muted)">% deviation</span></th>
          <th onclick="sortMds('vol_spike_flag')">Volatility<br><span style="font-size:9px;font-weight:400;color:var(--muted)">spike flag</span></th>
          <th onclick="sortMds('circuit_days')">Circuit Days ↕<br><span style="font-size:9px;font-weight:400;color:var(--muted)">upper circuit</span></th>
          <th onclick="sortMds('ltp')">LTP ↕</th>
        </tr></thead>
        <tbody id="mds-tbody">
          <tr><td colspan="11"><div class="empty">Click Analyse to run detection.</div></td></tr>
        </tbody>
      </table></div>
    </div>

    <!-- Radar chart panel — shown when clicking a script -->
    <div id="mds-radar-wrap" style="display:none;margin-bottom:14px">
      <div class="tw">
        <div class="th2">
          <span class="ttitle" id="mds-radar-title">Indicator radar — select a script above</span>
          <button class="btn btn-g" style="font-size:11px;padding:4px 10px"
            onclick="document.getElementById('mds-radar-wrap').style.display='none'">✕ Close</button>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;padding:16px;align-items:start">
          <div style="position:relative;height:300px">
            <canvas id="mds-radar" role="img" aria-label="Radar chart showing manipulation indicator scores"></canvas>
          </div>
          <div id="mds-indicator-detail" style="font-size:12px;line-height:2"></div>
        </div>
      </div>
    </div>

  </div>

</div>

<script>
const SUPABASE_URL = "{SUPABASE_URL}";
const SUPABASE_ANON_KEY = "{SUPABASE_ANON_KEY}";
const sb = supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
const TRADES_URL="https://fmseizcubbieodvfutby.supabase.co";
const TRADES_ANON="eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImZtc2VpemN1YmJpZW9kdmZ1dGJ5Iiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzkwNTI1NTksImV4cCI6MjA5NDYyODU1OX0.1n3_mr-wUUoOpHF2pcz-K503dNcvuI842aW0fTYOQn0";
const sbTrades=supabase.createClient(TRADES_URL,TRADES_ANON);

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

// ── GAINERS / LOSERS ─────────────────────────────────────────────────────────
async function loadGainersLosers(){{
  try{{
    // Get last 2 distinct trading dates from daily_volume
    let dateSet=new Set(), off=0;
    while(dateSet.size<2){{
      const {{data,error}}=await sb.from('daily_volume')
        .select('date').order('date',{{ascending:false}}).range(off,off+199);
      if(error||!data||!data.length) break;
      data.forEach(r=>dateSet.add(r.date));
      off+=200;
      if(data.length<200) break;
    }}
    const dates=[...dateSet].sort().reverse();
    if(dates.length<2){{
      document.getElementById('gainers-tbody').innerHTML=
        '<tr><td colspan="5"><div class="empty">Need at least 2 trading days of LTP data.</div></td></tr>';
      document.getElementById('losers-tbody').innerHTML=
        '<tr><td colspan="5"><div class="empty">Need at least 2 trading days of LTP data.</div></td></tr>';
      return;
    }}
    const todayDate=dates[0], prevDate=dates[1];
    const label=prevDate+' → '+todayDate;
    document.getElementById('gl-date-label').textContent=label;
    document.getElementById('gl-date-label2').textContent=label;

    // Fetch LTP for both dates
    const {{data:todayData,error:e1}}=await sb.from('daily_volume')
      .select('symbol,ltp').eq('date',todayDate).gt('ltp',0);
    const {{data:prevData, error:e2}}=await sb.from('daily_volume')
      .select('symbol,ltp').eq('date',prevDate).gt('ltp',0);
    if(e1||e2) throw e1||e2;

    // Build prev LTP lookup
    const prevMap={{}};
    (prevData||[]).forEach(r=>prevMap[r.symbol]=r.ltp);

    // Compute change for each symbol
    const changes=[];
    for(const r of (todayData||[])){{
      const prev=prevMap[r.symbol];
      if(!prev||prev<=0) continue;
      const pts=Math.round((r.ltp-prev)*100)/100;
      const pct=Math.round(((r.ltp-prev)/prev)*10000)/100;
      changes.push({{symbol:r.symbol, ltp:r.ltp, prev, pts, pct}});
    }}

    // Sort for gainers and losers
    const gainers=[...changes].sort((a,b)=>b.pct-a.pct).slice(0,10);
    const losers =[...changes].sort((a,b)=>a.pct-b.pct).slice(0,10);

    const medals=['🥇','🥈','🥉'];
    function glRow(r,i,isGain){{
      const cls=isGain?'pos':'neg';
      const arrow=isGain?'▲':'▼';
      return `<tr style="cursor:pointer" onclick="document.getElementById('f-sym').value='${{r.symbol}}';applyFilters()">
        <td class="m" style="color:var(--muted)">${{medals[i]||i+1}}</td>
        <td class="sym">${{r.symbol}}</td>
        <td class="m">Rs ${{fmtf(r.ltp)}}</td>
        <td class="m ${{cls}}">${{arrow}} ${{Math.abs(r.pct).toFixed(2)}}%</td>
        <td class="m ${{cls}}">${{r.pts>=0?'+':''}}${{fmtf(r.pts)}}</td>
      </tr>`;
    }}

    document.getElementById('gainers-tbody').innerHTML=
      gainers.length ? gainers.map((r,i)=>glRow(r,i,true)).join('') :
      '<tr><td colspan="5"><div class="empty">No gainers today.</div></td></tr>';

    document.getElementById('losers-tbody').innerHTML=
      losers.length ? losers.map((r,i)=>glRow(r,i,false)).join('') :
      '<tr><td colspan="5"><div class="empty">No losers today.</div></td></tr>';

  }}catch(e){{
    console.error('Gainers/losers error:',e);
    document.getElementById('gainers-tbody').innerHTML=
      '<tr><td colspan="5"><div class="empty">Error: '+e.message+'</div></td></tr>';
    document.getElementById('losers-tbody').innerHTML=
      '<tr><td colspan="5"><div class="empty">Error: '+e.message+'</div></td></tr>';
  }}
}}

// ── ACCUMULATION DETECTOR ───────────────────────────────────────────────────
async function loadAccumulation(){{
  document.getElementById('accum-wrap').innerHTML=
    '<div class="empty"><div class="spinner"></div>Loading…</div>';
  try{{
    // Always fetch latest date from accumulation table
    const {{data:dateRows,error:de}}=await sb.from('accumulation')
      .select('date').order('date',{{ascending:false}}).limit(1);
    if(de) throw de;
    if(!dateRows||!dateRows.length){{
      document.getElementById('accum-wrap').innerHTML=
        '<div class="empty">No accumulation data yet. Run compile_holdings.py to populate.</div>';
      return;
    }}
    const latestDate=dateRows[0].date;
    document.getElementById('accum-date-label').textContent=
      '(latest: '+latestDate+')';

    const {{data,error}}=await sb.from('accumulation')
      .select('*').eq('date',latestDate)
      .order('change_pct',{{ascending:false}});
    if(error) throw error;

    if(!data||!data.length){{
      document.getElementById('accum-wrap').innerHTML=
        '<div class="empty">No brokers with ≥10% cumulative holding increase on '+latestDate+'.</div>';
      return;
    }}

    ACCUM_DATA=data.map((r,i)=>({{ 
      symbol   : r.symbol,
      broker   : r.broker,
      name     : r.broker_name||'',
      cumToday : r.cum_today||0,
      cumPrev  : r.cum_prev||0,
      change   : r.change_qty||0,
      pct      : parseFloat(r.change_pct)||0,
      avg_rate : parseFloat(r.avg_rate)||0,
      date     : r.date||latestDate,
      rank     : i+1,
    }}));
    accumSortCol='pct'; accumSortAsc=false;

    // Populate date dropdown
    const {{data:allDates}}=await sb.from('accumulation')
      .select('date').order('date',{{ascending:false}});
    const dateSelect=document.getElementById('accum-filter-date');
    if(dateSelect && allDates){{
      const uniqueDates=[...new Set(allDates.map(r=>r.date))];
      dateSelect.innerHTML='<option value="">All dates</option>'+
        uniqueDates.map(d=>`<option value="${{d}}"${{d===latestDate?' selected':''}}>${{d}}</option>`).join('');
      dateSelect.onchange=async function(){{
        const selDate=this.value||latestDate;
        const {{data:nd}}=await sb.from('accumulation')
          .select('*').eq('date',selDate).order('change_pct',{{ascending:false}});
        ACCUM_DATA=(nd||[]).map((r,i)=>({{symbol:r.symbol,broker:r.broker,
          name:r.broker_name||'',cumToday:r.cum_today||0,cumPrev:r.cum_prev||0,
          change:r.change_qty||0,pct:parseFloat(r.change_pct)||0,
          avg_rate:parseFloat(r.avg_rate)||0,date:r.date||selDate,rank:i+1}}));
        filterAccumTable();
      }};
    }}

    const cnt=document.getElementById('accum-filter-cnt');
    if(cnt) cnt.textContent=ACCUM_DATA.length+' rows';
    filterAccumTable();
    renderAccumTable();}}catch(e){{
    console.error('Accumulation error:',e);
    document.getElementById('accum-wrap').innerHTML=
      '<div class="empty">Error: '+e.message+'</div>';
  }}
}}

// ── MARKET SUMMARY ─────────────────────────────────────────────────────────
async function loadMarketSummary(){{
  if(mktLoaded) return;
  TODAY_STR = getToday();
  document.getElementById('today-lbl').textContent = TODAY_STR;
  document.getElementById('gen-at').textContent    = new Date().toLocaleTimeString();
  loadGainersLosers();  // load gainers/losers in parallel
  loadAccumulation();   // always reload — shows latest date from DB

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
        top_buyer_rate  : (tb.buy_qty||0)>0 ? Math.round(((tb.buy_amt||0)/(tb.buy_qty||1))*100)/100 : 0,
        top_seller      : ts.broker||'—',
        top_seller_name : ts.broker_name||'',
        top_seller_qty  : ts.total_sale_qty||0,
        top_seller_rate : (ts.bulk_sale_qty||0)>0 ? Math.round(((ts.bulk_sale_amt||0)/(ts.bulk_sale_qty||1))*100)/100 : (ts.avg_rate||0),
        top_holder      : th.broker||'—',
        top_holder_name : th.broker_name||'',
        top_holder_qty  : th.holding_qty||0,
        avg_rate        : th.avg_rate||0,
        ltp             : 0,  // filled below from daily_volume
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
        top_buyer      : tb.broker||'—', top_buyer_name : tb.broker_name||'', top_buyer_qty  : tb.buy_qty||0,       top_buyer_rate : (tb.buy_qty||0)>0?Math.round(((tb.buy_amt||0)/(tb.buy_qty||1))*100)/100:0,
        top_seller     : ts.broker||'—', top_seller_name: ts.broker_name||'', top_seller_qty : ts.total_sale_qty||0, top_seller_rate: (ts.bulk_sale_qty||0)>0?Math.round(((ts.bulk_sale_amt||0)/(ts.bulk_sale_qty||1))*100)/100:(ts.avg_rate||0),
        top_holder     : th.broker||'—', top_holder_name: th.broker_name||'', top_holder_qty : th.holding_qty||0,
        avg_rate       : th.avg_rate||0,
        ltp            : 0,
      }};
    }});

    // Assign volume-based rank
    const ranked=[...DET_DATA].sort((a,b)=>b.volume-a.volume);
    ranked.forEach((r,i)=>r.rank=i+1);
    const rkMap={{}};
    ranked.forEach(r=>rkMap[r.date]=r.rank);
    DET_DATA.forEach(r=>r.rank=rkMap[r.date]);

    // Fetch LTP from daily_volume for each date
    try{{
      const allDates=DET_DATA.map(r=>r.date);
      const {{data:lvData}}=await sb.from('daily_volume')
        .select('date,ltp').eq('symbol',sym).in('date',allDates);
      if(lvData && lvData.length){{
        const ltpMap={{}};
        lvData.forEach(r=>ltpMap[r.date]=r.ltp||0);
        DET_DATA.forEach(r=>r.ltp=ltpMap[r.date]||0);
      }}
    }}catch(e){{console.error('LTP fetch error:',e);}}

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
       <td class="m" style="color:var(--amber);font-weight:600">Rs ${{fmtf(r.ltp||0)}}</td>
    </tr>`).join('');
}}

// ── WEEKLY ─────────────────────────────────────────────────────────────────
async function loadWeekly(){{
  try {{
    // ── Get last 5 distinct trading dates ─────────────────────────────────
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
    if(!last5.length){{
      document.getElementById('weekly-bars').innerHTML='<div class="empty">No data.</div>';
      document.getElementById('daily-vol-list').innerHTML='<div class="empty">No data.</div>';
      return;
    }}
    const today=last5[0];
    document.getElementById('daily-vol-title').textContent=
      'Top 10 scripts by volume — '+today;

    // ── Fetch buy_qty for last 5 dates ────────────────────────────────────
    let allW=[], off2=0, lim=1000;
    while(true){{
      const {{data,error}}=await sb.from('holdings')
        .select('symbol,buy_qty,date').in('date',last5)
        .range(off2,off2+lim-1);
      if(error) throw error;
      allW.push(...(data||[]));
      if(!data||data.length<lim) break;
      off2+=lim;
    }}

    // ── Aggregate weekly volumes ──────────────────────────────────────────
    const svW={{}};
    for(const r of allW){{if(!svW[r.symbol])svW[r.symbol]=0;svW[r.symbol]+=(r.buy_qty||0);}}
    const top10W=Object.entries(svW).sort((a,b)=>b[1]-a[1]).slice(0,10);

    // ── Aggregate daily volumes ───────────────────────────────────────────
    const svD={{}};
    for(const r of allW.filter(r=>r.date===today)){{
      if(!svD[r.symbol])svD[r.symbol]=0;svD[r.symbol]+=(r.buy_qty||0);
    }}
    const top10D=Object.entries(svD).sort((a,b)=>b[1]-a[1]).slice(0,10);

    // ── Fetch top 5 holders for each top symbol ───────────────────────────
    const weeklySyms = top10W.map(([s])=>s);
    const dailySyms  = top10D.map(([s])=>s);
    const allSyms    = [...new Set([...weeklySyms,...dailySyms])];

    // Weekly holders: from cumulative table (all-time net holding)
    // But filtered for the date range — aggregate from holdings table
    const weeklyHolders={{}};
    const dailyHolders={{}};

    for(const sym of allSyms){{
      // Weekly: sum holding_qty per broker across last 5 dates
      const wRows = allW.filter(r=>r.symbol===sym);
      // Need full holdings data for avg_rate — fetch separately
      const {{data:hFull}} = await sb.from('holdings')
        .select('broker,broker_name,buy_qty,buy_amt,total_sale_qty,ipo_sale_qty,bulk_sale_qty,bulk_sale_amt,holding_qty,avg_rate,date')
        .eq('symbol',sym).in('date',last5)
        .order('holding_qty',{{ascending:false}});

      if(hFull && hFull.length){{
        // Weekly: aggregate per broker across 5 days
        const bmap={{}};
        for(const r of hFull){{
          if(!bmap[r.broker]) bmap[r.broker]={{broker:r.broker,name:r.broker_name||'',
            buy_qty:0,buy_amt:0,sale_qty:0,bulk_sale_qty:0,bulk_sale_amt:0,net_holding:0}};
          bmap[r.broker].buy_qty       +=(r.buy_qty||0);
          bmap[r.broker].buy_amt       +=(r.buy_amt||0);
          bmap[r.broker].sale_qty      +=(r.total_sale_qty||0);
          bmap[r.broker].bulk_sale_qty +=(r.bulk_sale_qty||0);
          bmap[r.broker].bulk_sale_amt +=(r.bulk_sale_amt||0);
          bmap[r.broker].net_holding   +=(r.holding_qty||0);
        }}
        weeklyHolders[sym] = Object.values(bmap)
          .map(b=>{{
            const holdRate = b.net_holding>0
              ? Math.round(((b.buy_amt-b.bulk_sale_amt)/b.net_holding)*100)/100 : 0;
            return {{
              broker:b.broker,
              buy_qty:b.buy_qty, buy_amt:b.buy_amt,
              sale_qty:b.sale_qty||0,
              bulk_qty:b.bulk_sale_qty||0, bulk_sale_amt:b.bulk_sale_amt||0,
              net_holding:b.net_holding, avg_rate:holdRate
            }};
          }})
          .sort((a,b)=>b.net_holding-a.net_holding).slice(0,5);

        // Daily: only today's date
        const dRows = hFull.filter(r=>r.date===today);
        dailyHolders[sym] = dRows
          .sort((a,b)=>(b.holding_qty||0)-(a.holding_qty||0))
          .slice(0,5)
          .map(r=>{{return {{
            broker:r.broker,
            buy_qty:r.buy_qty||0, buy_amt:r.buy_amt||0,
            sale_qty:r.total_sale_qty||0,
            bulk_qty:r.bulk_sale_qty||0, bulk_sale_amt:r.bulk_sale_amt||0,
            net_holding:r.holding_qty||0, avg_rate:r.avg_rate||0
          }};}});
      }}
    }}

    // ── Render helper ─────────────────────────────────────────────────────
    const medals=['🥇','🥈','🥉'];
    function renderVolList(top10, holders, containerId, maxV){{
      document.getElementById(containerId).innerHTML = top10.map(([sym,vol],i)=>{{
        const pct=Math.max(2,vol/maxV*100);
        const hList = holders[sym]||[];
        const holderRows = hList.length
          ? hList.map((h,j)=>{{
              const buyRate  = h.buy_qty>0      ? Math.round(((h.buy_amt||0)/h.buy_qty)*100)/100          : 0;
              const sellRate = h.bulk_qty>0     ? Math.round(((h.bulk_sale_amt||0)/h.bulk_qty)*100)/100   : 0;
              const holdRate = h.net_holding>0  ? Math.round(((h.buy_amt-h.bulk_sale_amt)/h.net_holding)*100)/100 : 0;
              const nhcls    = h.net_holding>=0 ? 'pos' : 'neg';
              return `<tr>
                <td style="color:var(--muted);padding:4px 8px">${{medals[j]||'#'+(j+1)}}</td>
                <td style="padding:4px 8px"><span class="brk">${{h.broker}}</span></td>
                <td style="padding:4px 8px">
                  <div class="m pos" style="font-size:11px">${{fmt(h.buy_qty||0)}}</div>
                  <div style="color:var(--amber);font-size:9px">Rs ${{fmtf(buyRate)}}</div>
                </td>
                <td style="padding:4px 8px">
                  <div class="m neg" style="font-size:11px">${{fmt(h.sale_qty||0)}}</div>
                  <div style="color:var(--amber);font-size:9px">Rs ${{fmtf(sellRate)}}</div>
                </td>
                <td style="padding:4px 8px">
                  <div class="m ${{nhcls}}" style="font-size:11px">${{fmt(h.net_holding||0)}}</div>
                  <div style="color:var(--amber);font-size:9px">Rs ${{fmtf(holdRate)}}</div>
                </td>
              </tr>`;
            }}).join('')
          : '<tr><td colspan="5" class="empty" style="padding:8px">No data</td></tr>';
        return `<div class="vol-item">
          <div class="vol-item-hdr" onclick="toggleHolder(this)">
            <div class="vol-rank">${{medals[i]||'#'+(i+1)}}</div>
            <div class="vol-sym">${{sym}}</div>
            <div class="vol-bar-wrap">
              <div class="wtrack"><div class="wfill" style="width:${{pct}}%"></div></div>
              <span class="m pos">${{fmt(vol)}}</span>
            </div>
            <span style="font-size:11px;color:var(--muted);margin-left:8px">▼ Top 5 holders</span>
          </div>
          <div class="vol-holders">
            <table class="holder-table">
              <thead><tr>
                <th>#</th>
                <th>Broker</th>
                <th>Buy Qty<br><span style="font-weight:400;color:var(--muted)">Avg Rate</span></th>
                <th>Sell Qty<br><span style="font-weight:400;color:var(--muted)">Avg Rate</span></th>
                <th>Net Holding<br><span style="font-weight:400;color:var(--muted)">Avg Rate</span></th>
              </tr></thead>
              <tbody>${{holderRows}}</tbody>
            </table>
          </div>
        </div>`;
      }}).join('');
    }}

    const maxW = top10W.length ? top10W[0][1] : 1;
    const maxD = top10D.length ? top10D[0][1] : 1;
    const dateRange=last5[last5.length-1]+' → '+last5[0];
    const titleEl=document.querySelector('.weekly-section .ctitle');
    if(titleEl) titleEl.textContent='Top 10 scripts by volume — '+dateRange+' ('+last5.length+' trading days)';

    renderVolList(top10D, dailyHolders, 'daily-vol-list',  maxD);
    renderVolList(top10W, weeklyHolders, 'weekly-bars', maxW);

  }}catch(e){{
    document.getElementById('weekly-bars').innerHTML='<div class="empty">Error: '+e.message+'</div>';
    document.getElementById('daily-vol-list').innerHTML='<div class="empty">Error: '+e.message+'</div>';
    console.error(e);
  }}
}}

function sortAccum(col){{
  if(accumSortCol===col) accumSortAsc=!accumSortAsc;
  else{{accumSortCol=col; accumSortAsc=false;}}
  renderAccumTable();
}}

function clearAccumFilters(){{
  document.getElementById('accum-filter-sym').value='';
  document.getElementById('accum-filter-brk').value='';
  document.getElementById('accum-filter-minhold').value='5000';
  filterAccumTable();
}}

async function loadAccumByDate(){{
  const selDate=document.getElementById('accum-filter-date').value;
  if(!selDate) return;
  const {{data}}=await sb.from('accumulation')
    .select('*').eq('date',selDate).order('change_pct',{{ascending:false}});
  ACCUM_DATA=(data||[]).map((r,i)=>({{
    symbol:r.symbol, broker:r.broker, name:r.broker_name||'',
    cumToday:r.cum_today||0, cumPrev:r.cum_prev||0,
    change:r.change_qty||0, pct:parseFloat(r.change_pct)||0,
    avg_rate:parseFloat(r.avg_rate)||0, date:r.date||selDate, rank:i+1
  }}));
  filterAccumTable();
}}

function filterAccumTable(){{
  const sym      = (document.getElementById('accum-filter-sym')?.value||'').trim().toUpperCase();
  const brk      = (document.getElementById('accum-filter-brk')?.value||'').trim();
  const minHold  = parseInt(document.getElementById('accum-filter-minhold')?.value||'5000')||0;
  const filtered = ACCUM_DATA.filter(r=>{{
    if(sym && !r.symbol.toUpperCase().includes(sym)) return false;
    if(brk && String(r.broker).indexOf(brk) === -1) return false;
    if(r.cumPrev < minHold) return false;
    return true;
  }});
  const cnt = document.getElementById('accum-filter-cnt');
  if(cnt) cnt.textContent = filtered.length + ' of ' + ACCUM_DATA.length + ' rows';
  renderAccumTable(filtered);
}}


function renderAccumTable(data){{
  // Always use provided data (already filtered by filterAccumTable)
  renderAccumTableData(data||ACCUM_DATA);
}}

function renderAccumTableData(data){{
  const sorted=[...data].sort((a,b)=>{{
    let va=a[accumSortCol],vb=b[accumSortCol];
    if(typeof va==='number') return accumSortAsc?va-vb:vb-va;
    return accumSortAsc?String(va||'').localeCompare(String(vb||'')):String(vb||'').localeCompare(String(va||''));
  }});
  const medals=['🥇','🥈','🥉'];
  function thSort(col,label){{
    const arrow=accumSortCol===col?(accumSortAsc?'↑':'↓'):'↕';
    return '<th data-col="'+col+'" onclick="sortAccum(this.dataset.col)" style="cursor:pointer;white-space:nowrap">'+label+' '+arrow+'</th>';
  }}
  if(!sorted.length){{
    document.getElementById('accum-wrap').innerHTML='<div class="empty">No results match the filter.</div>';
    return;
  }}
  document.getElementById('accum-wrap').innerHTML=
    '<div class="tscroll"><table>'
    +'<thead><tr>'
    +thSort('rank','#')
    +thSort('symbol','Symbol')
    +thSort('broker','Broker')
    +thSort('cumToday','Holding Today<br><span style="font-size:10px;font-weight:400;color:var(--muted)">Avg Rate</span>')
    +thSort('cumPrev','Holding Prev Day')
    +thSort('change','Change')
    +thSort('pct','Change %')
    +'</tr></thead><tbody>'
    +sorted.map((r,i)=>{{
      const pctColor=r.pct>=50?'var(--cyan)':r.pct>=25?'var(--amber)':'var(--green)';
      return '<tr data-sym="'+r.symbol+'" onclick="accumClick(this)" style="cursor:pointer">'
        +'<td class="m" style="color:var(--muted)">'+( medals[r.rank-1]||r.rank)+'</td>'
        +'<td class="sym">'+r.symbol+'</td>'
        +'<td><span class="brk">'+r.broker+'</span>'
          +(r.name?'<div class="bname">'+r.name+'</div>':'')+'</td>'
        +'<td><div class="m pos">'+fmt(r.cumToday)+'</div>'
          +'<div style="color:var(--amber);font-size:10px">Rs '+fmtf(r.avg_rate)+'</div></td>'
        +'<td class="m" style="color:var(--muted)">'+fmt(r.cumPrev)+'</td>'
        +'<td class="m pos">+'+fmt(r.change)+'</td>'
        +'<td><span style="background:var(--s2);color:'+pctColor
          +';border:1px solid '+pctColor
          +';border-radius:6px;padding:2px 8px;font-weight:600;font-family:var(--mono)">'
          +'+'+r.pct.toFixed(1)+'%</span></td>'
        +'</tr>';
    }}).join('')
    +'</tbody></table></div>'
    +'<div style="font-size:11px;color:var(--muted);padding:8px 12px">'
    +sorted.length+' broker-script pairs · click any row to view in Daily Holdings</div>';
}}

function accumClick(tr){{const sym=tr.dataset.sym;if(sym){{document.getElementById('f-sym').value=sym;applyFilters();}}}}

function toggleHolder(hdr){{
  const panel=hdr.nextElementSibling;
  panel.classList.toggle('open');
  const arrow=hdr.querySelector('span:last-child');
  arrow.textContent=panel.classList.contains('open')?'▲ Top holders':'▼ Top holders';
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
        // Group by symbol+broker so each script shows separately
        const key=(r.symbol||'')+'|'+r.broker;
        if(!brkMap[key]) brkMap[key]={{
          symbol:r.symbol, broker:r.broker, broker_name:r.broker_name||'',
          security_name:r.security_name||'',
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
        if(r.security_name) brkMap[key].security_name=r.security_name;
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
    // Populate all-brokers table in Tab 3 using FC (full cumulative data for symbol)
    TOPB_DATA=FC.map(r=>({{symbol:r.symbol,broker:r.broker,broker_name:r.broker_name,
      total_buy_qty:r.total_buy_qty||0,total_buy_amt:r.total_buy_amt||0,
      total_sale_qty:r.total_sale_qty||0,total_ipo_qty:r.total_ipo_qty||0,
      total_bulk_qty:r.total_bulk_qty||0,total_bulk_amt:r.total_bulk_amt||0,
      net_holding:r.net_holding||0,avg_rate:r.avg_rate||0}}));
    topbSortCol='net_holding'; topbSortAsc=false;
    const ttl=document.getElementById('topb-table-title');
    if(ttl) ttl.textContent='All brokers — '+sym+' net holdings'+(dfrom?' ('+dfrom+' → '+dto+')':'');
    renderTopbTable();
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
  document.getElementById('daily-summary').style.display='none';
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

function updateDailySummary(data){{
  if(!data||!data.length){{
    document.getElementById('daily-summary').style.display='none';
    return;
  }}
  const sumBuy  = data.reduce((s,r)=>s+(r.buy_qty||0),0);
  const sumSale = data.reduce((s,r)=>s+(r.total_sale_qty||0),0);
  const sumIpo  = data.reduce((s,r)=>s+(r.ipo_sale_qty||0),0);
  const sumBulk = data.reduce((s,r)=>s+(r.bulk_sale_qty||0),0);
  const sumHold = data.reduce((s,r)=>s+(r.holding_qty||0),0);
  // Weighted avg rate = (sum buy_amt - sum bulk_sale_amt) / sum holding_qty
  const sumBuyAmt  = data.reduce((s,r)=>s+(r.buy_amt||0),0);
  const sumBulkAmt = data.reduce((s,r)=>s+(r.bulk_sale_amt||0),0);
  const avgRate = sumHold>0 ? Math.round(((sumBuyAmt-sumBulkAmt)/sumHold)*100)/100 : 0;

  const row=document.getElementById('daily-summary');
  row.style.display='';
  document.getElementById('daily-sum-label').textContent='∑ '+data.length.toLocaleString()+' rows (all pages)';
  document.getElementById('daily-sum-buy').textContent  = fmt(sumBuy);
  document.getElementById('daily-sum-sale').textContent = fmt(sumSale);
  document.getElementById('daily-sum-ipo').innerHTML    = '<span class="ipo">'+fmt(sumIpo)+'</span>';
  document.getElementById('daily-sum-bulk').textContent = fmt(sumBulk);
  const nhcls=sumHold>=0?'pos':'neg';
  document.getElementById('daily-sum-hold').className   = 'm '+nhcls;
  document.getElementById('daily-sum-hold').textContent = fmt(sumHold);
  document.getElementById('daily-sum-rate').textContent = sumHold>0 ? 'Rs '+fmtf(avgRate) : '—';
}}

function renderD(){{
  const data=doSort(FD,dCol,dAsc);
  const tot=data.length,pages=Math.max(1,Math.ceil(tot/PS));
  pg.d=Math.min(pg.d,pages);
  const sl=data.slice((pg.d-1)*PS,pg.d*PS);
  const maxQ=Math.max(...FD.map(r=>Math.abs(r.holding_qty||0)),1);
  document.getElementById('cnt-d').textContent=tot.toLocaleString()+' rows';
  document.getElementById('pi-d').textContent='Page '+pg.d+' of '+pages;
  updateDailySummary(data);  // compute totals across ALL filtered rows
  document.getElementById('pp-d').disabled=pg.d<=1;
  document.getElementById('pn-d').disabled=pg.d>=pages;
  const tb=document.getElementById('tbody-d');
  if(!sl.length){{tb.innerHTML='<tr><td colspan="10"><div class="empty">No data.</div></td></tr>';return;}}
  const off=(pg.d-1)*PS;
  tb.innerHTML=sl.map((r,i)=>{{
    const hq=r.holding_qty||0,pct=Math.min(100,Math.abs(hq)/maxQ*100);
    const cls=hq>=0?'pos':'neg',fc=hq>=0?'p':'n';
    return '<tr><td class="m" style="color:var(--muted)">'+(off+i+1)+'</td><td class="m">'+r.date+'</td><td class="sym">'+r.symbol+'</td><td><span class="brk">'+r.broker+'</span></td><td class="bname">'+(r.broker_name||'—')+'</td><td class="m pos">'+fmt(r.buy_qty)+'</td><td class="m neg">'+fmt(r.total_sale_qty)+'</td><td><span class="ipo">'+fmt(r.ipo_sale_qty)+'</span></td><td class="m">'+fmt(r.bulk_sale_qty)+'</td><td><div class="qcell"><span class="'+cls+'">'+fmt(hq)+'</span><div class="qbar"><div class="qfill '+fc+'" style="width:'+pct+'%"></div></div></div></td><td class="m" style="color:var(--amber)">'+fmtf(r.avg_rate)+'</td></tr>';
  }}).join('');
}}

let CUMUL_DRILL=[];
let cumulDrillHCol='holding_qty', cumulDrillHAsc=false;
let cumulDrillSCol='total_sale_qty', cumulDrillSAsc=false;

function sortCumulDrill(side, col){{
  if(side==='h'){{
    if(cumulDrillHCol===col) cumulDrillHAsc=!cumulDrillHAsc;
    else{{cumulDrillHCol=col; cumulDrillHAsc=false;}}
    renderCumulDrillHold(CUMUL_DRILL);
  }} else {{
    if(cumulDrillSCol===col) cumulDrillSAsc=!cumulDrillSAsc;
    else{{cumulDrillSCol=col; cumulDrillSAsc=false;}}
    renderCumulDrillSale(CUMUL_DRILL);
  }}
}}

function cumulDrillClick(tr){{
  openCumulDrill(tr.dataset.sym, parseInt(tr.dataset.brk), tr.dataset.name||'');
}}

async function openCumulDrill(symbol, broker, brokerName){{
  const dfrom = document.getElementById('f-dfrom').value;
  const dto   = document.getElementById('f-dto').value;
  const panel = document.getElementById('cumul-drill');
  panel.style.display='';
  panel.scrollIntoView({{behavior:'smooth',block:'start'}});
  document.getElementById('cumul-drill-title').textContent=
    'Daily breakdown — '+symbol+' · Broker '+broker+' ('+brokerName+')'+
    (dfrom?' · '+dfrom+' → '+dto:' · all dates')+' (top 10 holding days)';
  document.getElementById('cumul-drill-hold-tbody').innerHTML=
    '<tr><td colspan="4"><div class="loading"><div class="spinner"></div>Loading…</div></td></tr>';
  document.getElementById('cumul-drill-sale-tbody').innerHTML=
    '<tr><td colspan="4"><div class="loading"><div class="spinner"></div>Loading…</div></td></tr>';

  try{{
    // Fetch all data in date range for this broker+symbol (no limit — need both top holds and top sales)
    let q=sb.from('holdings').select(
      'date,buy_qty,buy_amt,total_sale_qty,ipo_sale_qty,bulk_sale_qty,bulk_sale_amt,holding_qty,avg_rate'
    ).eq('symbol',symbol).eq('broker',broker).order('date',{{ascending:false}}).limit(500);
    if(dfrom) q=q.gte('date',dfrom);
    if(dto)   q=q.lte('date',dto);
    const {{data,error}}=await q;
    if(error) throw error;
    console.log('Drill data:', data?.length, 'rows for', symbol, broker);
    CUMUL_DRILL=(data||[]);
    cumulDrillHCol='holding_qty'; cumulDrillHAsc=false;
    cumulDrillSCol='total_sale_qty'; cumulDrillSAsc=false;
    renderCumulDrillHold(CUMUL_DRILL);
    renderCumulDrillSale(CUMUL_DRILL);
  }}catch(e){{
    const msg='<tr><td colspan="4"><div class="empty">Error: '+e.message+'</div></td></tr>';
    document.getElementById('cumul-drill-hold-tbody').innerHTML=msg;
    document.getElementById('cumul-drill-sale-tbody').innerHTML=msg;
  }}
}}

function doSort(arr, col, asc){{
  return [...arr].sort((a,b)=>{{
    let va=a[col],vb=b[col];
    if(typeof va==='number') return asc?va-vb:vb-va;
    return asc?String(va||'').localeCompare(String(vb||'')):String(vb||'').localeCompare(String(va||''));
  }});
}}

function renderCumulDrillHold(data){{
  const tb=document.getElementById('cumul-drill-hold-tbody');
  if(!data||!data.length){{tb.innerHTML='<tr><td colspan="4"><div class="empty">No data.</div></td></tr>';return;}}
  const sorted=doSort(data,cumulDrillHCol,cumulDrillHAsc).slice(0,10);
  const medals=['🥇','🥈','🥉'];
  tb.innerHTML=sorted.map((r,i)=>{{
    const nhcls=(r.holding_qty||0)>=0?'pos':'neg';
    return '<tr>'
      +'<td class="m" style="color:var(--muted)">'+(medals[i]||i+1)+'</td>'
      +'<td class="m">'+r.date+'</td>'
      +'<td><div class="m '+nhcls+'" style="font-size:12px">'+fmt(r.holding_qty||0)+'</div>'
        +'<div style="color:var(--amber);font-size:10px">Rs '+fmtf(r.avg_rate||0)+'</div></td>'
      +'<td class="m pos">'+fmt(r.buy_qty||0)+'</td>'
      +'</tr>';
  }}).join('');
}}

function renderCumulDrillSale(data){{
  const tb=document.getElementById('cumul-drill-sale-tbody');
  if(!data||!data.length){{tb.innerHTML='<tr><td colspan="4"><div class="empty">No data.</div></td></tr>';return;}}
  const sorted=doSort(data,cumulDrillSCol,cumulDrillSAsc).slice(0,10);
  const medals=['🥇','🥈','🥉'];
  tb.innerHTML=sorted.map((r,i)=>{{
    const sellRate=r.bulk_sale_qty>0?Math.round((r.bulk_sale_amt/r.bulk_sale_qty)*100)/100:0;
    return '<tr>'
      +'<td class="m" style="color:var(--muted)">'+(medals[i]||i+1)+'</td>'
      +'<td class="m">'+r.date+'</td>'
      +'<td><div class="m neg" style="font-size:12px">'+fmt(r.total_sale_qty||0)+'</div>'
        +'<div style="color:var(--amber);font-size:10px">Rs '+fmtf(sellRate)+'</div></td>'
      +'<td><span class="ipo">'+fmt(r.ipo_sale_qty||0)+'</span></td>'
      +'</tr>';
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
  if(!sl.length){{tb.innerHTML='<tr><td colspan="10"><div class="empty">No data.</div></td></tr>';
    document.getElementById('cumul-summary').style.display='none'; return;}}

  // ── Compute totals across ALL filtered rows (not just current page) ───────
  const sumBuy  = data.reduce((s,r)=>s+(r.total_buy_qty||0),0);
  const sumSale = data.reduce((s,r)=>s+(r.total_sale_qty||0),0);
  const sumIpo  = data.reduce((s,r)=>s+(r.total_ipo_qty||0),0);
  const sumBulk = data.reduce((s,r)=>s+(r.total_bulk_qty||0),0);
  const sumHold = data.reduce((s,r)=>s+(r.net_holding||0),0);
  // Weighted avg rate = (sum buy_amt - sum bulk_amt) / sum net_holding
  const sumBuyAmt  = data.reduce((s,r)=>s+(r.total_buy_amt||0),0);
  const sumBulkAmt = data.reduce((s,r)=>s+(r.total_bulk_amt||0),0);
  const avgRate = sumHold>0 ? Math.round(((sumBuyAmt-sumBulkAmt)/sumHold)*100)/100 : 0;

  const summary=document.getElementById('cumul-summary');
  summary.style.display='';
  document.getElementById('cs-buy').textContent  = fmt(sumBuy);
  document.getElementById('cs-sale').textContent = fmt(sumSale);
  document.getElementById('cs-ipo').innerHTML    = '<span class="ipo">'+fmt(sumIpo)+'</span>';
  document.getElementById('cs-bulk').textContent = fmt(sumBulk);
  const nhcls=sumHold>=0?'pos':'neg';
  document.getElementById('cs-hold').className  = 'm '+nhcls;
  document.getElementById('cs-hold').textContent = fmt(sumHold);
  document.getElementById('cs-rate').textContent = sumHold>0?'Rs '+fmtf(avgRate):'—';
  // Label shows count
  summary.querySelector('td').textContent = '∑ '+tot.toLocaleString()+' rows';

  const medals=['🥇','🥈','🥉'];
  tb.innerHTML=sl.map((r,i)=>{{
    const rank=off+i+1,medal=medals[rank-1]||'#'+rank;
    const nh=r.net_holding||0,cls=nh>=0?'pos':'neg';
    return '<tr style="cursor:pointer" data-sym="'+r.symbol+'" data-brk="'+r.broker+'" data-name="'+(r.broker_name||'').replace(/"/g,'&quot;')+'" onclick="cumulDrillClick(this)">'+'<td class="m" style="color:var(--muted)">'+medal+'</td><td class="sym">'+r.symbol+'</td><td><span class="brk">'+r.broker+'</span></td><td class="bname">'+(r.broker_name||'—')+'</td><td class="m pos">'+fmt(r.total_buy_qty)+'</td><td class="m neg">'+fmt(r.total_sale_qty)+'</td><td><span class="ipo">'+fmt(r.total_ipo_qty)+'</span></td><td class="m">'+fmt(r.total_bulk_qty)+'</td><td class="'+cls+'">'+fmt(nh)+'</td><td class="m" style="color:var(--amber)">'+fmtf(r.avg_rate)+'</td></tr>';
  }}).join('');
}}

let TOPB_DATA=[], TOPB_FILTERED=[], topbSortCol='net_holding', topbSortAsc=false;

function sortTopbTable(col){{
  if(topbSortCol===col) topbSortAsc=!topbSortAsc;
  else{{topbSortCol=col; topbSortAsc=false;}}
  renderTopbTable();
}}

function filterTopbTable(){{
  const q=(document.getElementById('topb-filter-brk')?.value||'').trim().toLowerCase();
  TOPB_FILTERED=q
    ? TOPB_DATA.filter(r=>String(r.broker).includes(q)||(r.broker_name||'').toLowerCase().includes(q))
    : [...TOPB_DATA];
  renderTopbTable();
}}

function renderTopbTable(){{
  const tb=document.getElementById('topb-table-tbody');
  const data=TOPB_FILTERED.length||document.getElementById('topb-filter-brk')?.value
    ? TOPB_FILTERED : TOPB_DATA;
  if(!data.length){{tb.innerHTML='<tr><td colspan="9"><div class="empty">No data.</div></td></tr>';return;}}
  const sorted=[...data].sort((a,b)=>{{
    let va=a[topbSortCol],vb=b[topbSortCol];
    if(typeof va==='number') return topbSortAsc?va-vb:vb-va;
    return topbSortAsc?String(va||'').localeCompare(String(vb||'')):String(vb||'').localeCompare(String(va||''));
  }});
  const medals=['🥇','🥈','🥉'];
  tb.innerHTML=sorted.map((r,i)=>{{
    const nh=r.net_holding||0, cls=nh>=0?'pos':'neg';
    return '<tr>'
      +'<td class="m" style="color:var(--muted)">'+(medals[i]||i+1)+'</td>'
      +'<td><span class="brk">'+r.broker+'</span></td>'
      +'<td class="bname">'+(r.broker_name||'—')+'</td>'
      +'<td class="m pos">'+fmt(r.total_buy_qty||0)+'</td>'
      +'<td class="m neg">'+fmt(r.total_sale_qty||0)+'</td>'
      +'<td><span class="ipo">'+fmt(r.total_ipo_qty||0)+'</span></td>'
      +'<td class="m">'+fmt(r.total_bulk_qty||0)+'</td>'
      +'<td class="m '+cls+'">'+fmt(nh)+'</td>'
      +'<td class="m" style="color:var(--muted)">'+r.scripts+'</td>'
      +'</tr>';
  }}).join('');
  document.getElementById('topb-table-cnt').textContent=
    sorted.length+' brokers'+(TOPB_DATA.length!==sorted.length?' (filtered from '+TOPB_DATA.length+')':'');
}}

async function loadTopbTable(){{
  try{{
    let rows=[], off=0, lim=1000;
    while(true){{
      const {{data,error}}=await sb.from('cumulative')
        .select('broker,broker_name,total_buy_qty,total_sale_qty,total_ipo_qty,total_bulk_qty,net_holding')
        .range(off,off+lim-1);
      if(error) throw error;
      rows.push(...(data||[]));
      if(!data||data.length<lim) break;
      off+=lim;
    }}
    const brkMap={{}};
    for(const r of rows){{
      const k=r.broker;
      if(!brkMap[k]) brkMap[k]={{broker:r.broker,broker_name:r.broker_name||'',
        total_buy_qty:0,total_sale_qty:0,total_ipo_qty:0,total_bulk_qty:0,net_holding:0,scripts:0}};
      brkMap[k].total_buy_qty  +=(r.total_buy_qty||0);
      brkMap[k].total_sale_qty +=(r.total_sale_qty||0);
      brkMap[k].total_ipo_qty  +=(r.total_ipo_qty||0);
      brkMap[k].total_bulk_qty +=(r.total_bulk_qty||0);
      brkMap[k].net_holding    +=(r.net_holding||0);
      brkMap[k].scripts        +=1;
      if(r.broker_name) brkMap[k].broker_name=r.broker_name;
    }}
    TOPB_DATA=Object.values(brkMap).sort((a,b)=>b.net_holding-a.net_holding);
    TOPB_FILTERED=[...TOPB_DATA];
    renderTopbTable();
  }}catch(e){{
    document.getElementById('topb-table-tbody').innerHTML=
      '<tr><td colspan="9"><div class="empty">Error: '+e.message+'</div></td></tr>';
  }}
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
  ['daily','cumul','topb','mkt','cmp','mds'].forEach(t=>{{
    document.getElementById('tab-'+t).style.display=t===name?'':'none';
  }});
  document.querySelectorAll('.tab').forEach((el,i)=>{{
    el.classList.toggle('active',['daily','cumul','topb','mkt','cmp'][i]===name);
  }});
  if(name==='topb'){{renderChart(document.getElementById('f-sym').value);loadTopbTable();}}
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

// ── MANIPULATION DETECTION SYSTEM ───────────────────────────────────────────
let MDS_DATA = [], mdsSortCol = 'score', mdsSortAsc = false;
let ACCUM_DATA=[], accumSortCol='pct', accumSortAsc=false;
let mdsChart = null;

const MDS_THRESHOLDS = {{
  vol_spike   : {{ warn:2, alert:3 }},
  price_chg   : {{ warn:5, alert:7 }},
  rising_days : {{ warn:3, alert:4 }},
  vwap_dev    : {{ warn:4, alert:6 }},
  circuit_days: {{ warn:2, alert:3 }},
}};

function scoreMDS(d){{
  let score = 0;
  const flags = {{}};
  // Volume Spike: ratio today vs window avg
  if(d.vol_spike >= 3)      {{ score+=3; flags.vol_spike='🔴'; }}
  else if(d.vol_spike >= 2) {{ score+=1; flags.vol_spike='🟡'; }}
  else flags.vol_spike='🟢';
  // Price Change %
  if(Math.abs(d.price_chg) >= 7)      {{ score+=2; flags.price_chg='🔴'; }}
  else if(Math.abs(d.price_chg) >= 5) {{ score+=1; flags.price_chg='🟡'; }}
  else flags.price_chg='🟢';
  // Rising Days (consecutive up days)
  if(d.rising_days >= 4)      {{ score+=2; flags.rising_days='🔴'; }}
  else if(d.rising_days >= 3) {{ score+=1; flags.rising_days='🟡'; }}
  else flags.rising_days='🟢';
  // VWAP Deviation
  if(Math.abs(d.vwap_dev) >= 6)      {{ score+=2; flags.vwap_dev='🔴'; }}
  else if(Math.abs(d.vwap_dev) >= 4) {{ score+=1; flags.vwap_dev='🟡'; }}
  else flags.vwap_dev='🟢';
  // Volatility Spike (price range > 5% in window)
  if(d.price_range_pct >= 10)     {{ score+=2; flags.vol_spike_flag='🔴'; }}
  else if(d.price_range_pct >= 5) {{ score+=1; flags.vol_spike_flag='🟡'; }}
  else flags.vol_spike_flag='🟢';
  // Upper Circuit (consecutive days at +X%)
  if(d.circuit_days >= 3)      {{ score+=3; flags.circuit_days='🔴'; }}
  else if(d.circuit_days >= 2) {{ score+=1; flags.circuit_days='🟡'; }}
  else flags.circuit_days='🟢';
  return {{ score, flags }};
}}

function riskLabel(score){{
  if(score >= 10) return {{label:'Highly Suspicious', color:'#ff4444', bg:'rgba(255,68,68,0.12)'}};
  if(score >= 7)  return {{label:'Suspicious',        color:'#ff8c42', bg:'rgba(255,140,66,0.12)'}};
  if(score >= 4)  return {{label:'Watchlist',         color:'#ffd700', bg:'rgba(255,215,0,0.10)'}};
  return               {{label:'Normal',              color:'#7fff6e', bg:'rgba(127,255,110,0.08)'}};
}}

function sortMds(col){{
  if(mdsSortCol===col) mdsSortAsc=!mdsSortAsc;
  else{{mdsSortCol=col; mdsSortAsc=col==='symbol';}}
  renderMdsTable(MDS_DATA);
}}

async function loadMDS(){{
  const win    = document.getElementById('mds-window').value;
  const minSc  = parseInt(document.getElementById('mds-minscore').value)||0;
  document.getElementById('mds-cnt').textContent='Loading…';
  document.getElementById('mds-tbody').innerHTML=
    '<tr><td colspan="11"><div class="loading"><div class="spinner"></div>Analysing market data…</div></td></tr>';
  document.getElementById('mds-cards').innerHTML='';
  document.getElementById('mds-radar-wrap').style.display='none';

  try{{
    // ── Step 1: Get last N trading dates ─────────────────────────────────
    let dateSet=new Set(), off=0;
    const need = win==='today' ? 2 : (parseInt(win)||5)+1;
    while(dateSet.size < need){{
      const {{data,error}}=await sb.from('daily_volume')
        .select('date').order('date',{{ascending:false}}).range(off,off+299);
      if(error||!data||!data.length) break;
      data.forEach(r=>dateSet.add(r.date));
      off+=300; if(data.length<300) break;
    }}
    const dates=[...dateSet].sort().reverse();
    const today=dates[0];
    const prev=dates[1]||today;
    const windowDates = win==='today' ? [today] : dates.slice(0, parseInt(win)||5);

    // ── Step 2: Fetch daily_volume for all dates in window ────────────────
    let dvAll=[], dvOff=0;
    while(true){{
      const {{data,error}}=await sb.from('daily_volume')
        .select('date,symbol,total_buy_qty,total_sel_qty,ltp,vwap')
        .in('date', windowDates).range(dvOff,dvOff+999);
      if(error) throw error;
      dvAll.push(...(data||[]));
      if(!data||data.length<1000) break;
      dvOff+=1000;
    }}
    if(!dvAll.length){{
      document.getElementById('mds-tbody').innerHTML=
        '<tr><td colspan="11"><div class="empty">No daily_volume data. Run compile first.</div></td></tr>';
      return;
    }}

    // ── Step 3: Compute indicators per symbol ─────────────────────────────
    const symMap={{}};
    for(const r of dvAll){{
      const s=r.symbol;
      if(!symMap[s]) symMap[s]={{symbol:s, rows:[]}};
      symMap[s].rows.push(r);
    }}

    MDS_DATA=[];
    for(const [sym, sd] of Object.entries(symMap)){{
      const rows=sd.rows.sort((a,b)=>b.date.localeCompare(a.date));
      const todayRow=rows[0];
      if(!todayRow||todayRow.date!==today) continue; // must have today's data

      const ltps   = rows.map(r=>r.ltp||0).filter(x=>x>0);
      const vols   = rows.map(r=>r.total_buy_qty||0);
      const ltp    = todayRow.ltp||0;
      const vwap   = todayRow.vwap||0;
      const prevLtp= rows[1]?.ltp||ltp;

      // Volume spike ratio: today vs avg of rest
      const restVols=vols.slice(1);
      const avgVol=restVols.length ? restVols.reduce((a,b)=>a+b,0)/restVols.length : vols[0];
      const vol_spike=avgVol>0 ? Math.round((vols[0]/avgVol)*100)/100 : 1;

      // Price change % (today vs previous day)
      const price_chg=prevLtp>0 ? Math.round(((ltp-prevLtp)/prevLtp)*10000)/100 : 0;

      // Consecutive rising days
      let rising_days=0;
      for(let i=0;i<ltps.length-1;i++){{
        if(ltps[i]>ltps[i+1]) rising_days++;
        else break;
      }}

      // VWAP deviation: (LTP - VWAP) / VWAP * 100
      const vwap_dev=vwap>0 ? Math.round(((ltp-vwap)/vwap)*10000)/100 : 0;

      // Price range % in window (high-low / low)
      const maxLtp=Math.max(...ltps), minLtp=Math.min(...ltps);
      const price_range_pct=minLtp>0 ? Math.round(((maxLtp-minLtp)/minLtp)*10000)/100 : 0;

      // Consecutive upper circuit days (price_chg >= 9.9% per day)
      let circuit_days=0;
      for(let i=0;i<ltps.length-1;i++){{
        const chg=ltps[i+1]>0?(ltps[i]-ltps[i+1])/ltps[i+1]*100:0;
        if(chg>=9.9) circuit_days++;
        else break;
      }}

      const d={{symbol:sym, ltp, vwap, vol_spike, price_chg, rising_days,
               vwap_dev, price_range_pct, circuit_days, vol_today:vols[0], rank:0}};
      const {{score,flags}}=scoreMDS(d);
      MDS_DATA.push({{...d, score, flags}});
    }}

    // Sort by score desc, assign rank
    MDS_DATA.sort((a,b)=>b.score-a.score||b.vol_spike-a.vol_spike);
    MDS_DATA.forEach((r,i)=>r.rank=i+1);

    // Filter by min score
    const filtered=MDS_DATA.filter(r=>r.score>=minSc);

    // Summary cards
    const counts={{normal:0,watchlist:0,suspicious:0,high:0}};
    MDS_DATA.forEach(r=>{{
      if(r.score>=10) counts.high++;
      else if(r.score>=7) counts.suspicious++;
      else if(r.score>=4) counts.watchlist++;
      else counts.normal++;
    }});
    document.getElementById('mds-cards').innerHTML=[
      {{label:'Highly Suspicious', count:counts.high,      color:'#ff4444', icon:'🚨'}},
      {{label:'Suspicious',        count:counts.suspicious, color:'#ff8c42', icon:'⚠️'}},
      {{label:'Watchlist',         count:counts.watchlist,  color:'#ffd700', icon:'👁'}},
      {{label:'Normal',            count:counts.normal,     color:'#7fff6e', icon:'✅'}},
    ].map(c=>`<div style="background:var(--s1);border:1px solid var(--border);border-radius:10px;padding:12px;text-align:center">
      <div style="font-size:20px">${{c.icon}}</div>
      <div style="font-size:22px;font-weight:700;color:${{c.color}};font-family:var(--mono)">${{c.count}}</div>
      <div style="font-size:11px;color:var(--muted)">${{c.label}}</div>
    </div>`).join('');

    document.getElementById('mds-cnt').textContent=
      filtered.length+' scripts · window: '+windowDates[windowDates.length-1]+' → '+today;
    renderMdsTable(filtered);

  }}catch(e){{
    console.error('MDS error:',e);
    document.getElementById('mds-tbody').innerHTML=
      '<tr><td colspan="11"><div class="empty">Error: '+e.message+'</div></td></tr>';
  }}
}}

function renderMdsTable(data){{
  const sorted=[...data].sort((a,b)=>{{
    let va=a[mdsSortCol],vb=b[mdsSortCol];
    if(typeof va==='number') return mdsSortAsc?va-vb:vb-va;
    return mdsSortAsc?String(va||'').localeCompare(String(vb||'')):String(vb||'').localeCompare(String(va||''));
  }});
  const tb=document.getElementById('mds-tbody');
  if(!sorted.length){{tb.innerHTML='<tr><td colspan="11"><div class="empty">No scripts meet the filter criteria.</div></td></tr>';return;}}
  const medals=['🥇','🥈','🥉'];
  tb.innerHTML=sorted.map(r=>{{
    const rl=riskLabel(r.score);
    const scoreBadge=`<span style="background:${{rl.bg}};color:${{rl.color}};border:1px solid ${{rl.color}};border-radius:6px;padding:2px 8px;font-weight:700;font-family:var(--mono)">${{r.score}}</span>`;
    const levelBadge=`<span style="color:${{rl.color}};font-size:11px">${{rl.label}}</span>`;
    const pctColor=r.price_chg>=0?'var(--green)':'var(--red)';
    return `<tr style="cursor:pointer" onclick="openMdsRadar('${{r.symbol}}')">
      <td class="m" style="color:var(--muted)">${{medals[r.rank-1]||r.rank}}</td>
      <td class="sym">${{r.symbol}}</td>
      <td style="text-align:center">${{scoreBadge}}</td>
      <td>${{levelBadge}}</td>
      <td class="m" style="color:${{r.vol_spike>=3?'var(--red)':r.vol_spike>=2?'var(--amber)':'var(--muted)'}}">${{r.flags.vol_spike}} ${{r.vol_spike.toFixed(2)}}x</td>
      <td class="m" style="color:${{pctColor}}">${{r.price_chg>=0?'+':''}}${{r.price_chg.toFixed(2)}}%</td>
      <td class="m" style="color:${{r.rising_days>=4?'var(--red)':r.rising_days>=3?'var(--amber)':'var(--muted)'}}">${{r.flags.rising_days}} ${{r.rising_days}}</td>
      <td class="m" style="color:${{Math.abs(r.vwap_dev)>=6?'var(--red)':Math.abs(r.vwap_dev)>=4?'var(--amber)':'var(--muted)'}}">${{r.flags.vwap_dev}} ${{r.vwap_dev.toFixed(2)}}%</td>
      <td class="m" style="text-align:center">${{r.flags.vol_spike_flag}} ${{r.price_range_pct.toFixed(1)}}%</td>
      <td class="m" style="color:${{r.circuit_days>=3?'var(--red)':r.circuit_days>=2?'var(--amber)':'var(--muted)'}}">${{r.flags.circuit_days}} ${{r.circuit_days}}</td>
      <td class="m" style="color:var(--amber)">Rs ${{fmtf(r.ltp)}}</td>
    </tr>`;
  }}).join('');
}}

function openMdsRadar(sym){{
  const r=MDS_DATA.find(x=>x.symbol===sym);
  if(!r) return;
  document.getElementById('mds-radar-wrap').style.display='';
  document.getElementById('mds-radar-title').textContent=sym+' — Indicator Radar (Score: '+r.score+'/14)';

  // Radar chart — normalize each indicator to 0-100
  const indicators=[
    {{label:'Vol Spike',    val:Math.min(r.vol_spike/4*100,100),     raw:r.vol_spike.toFixed(2)+'x',    threshold:'≥3x = high risk'}},
    {{label:'Price Chg %',  val:Math.min(Math.abs(r.price_chg)/10*100,100), raw:r.price_chg.toFixed(2)+'%', threshold:'≥7% = high risk'}},
    {{label:'Rising Days',  val:Math.min(r.rising_days/5*100,100),   raw:r.rising_days+' days',         threshold:'≥4 = high risk'}},
    {{label:'VWAP Dev',     val:Math.min(Math.abs(r.vwap_dev)/10*100,100), raw:r.vwap_dev.toFixed(2)+'%', threshold:'≥6% = high risk'}},
    {{label:'Volatility',   val:Math.min(r.price_range_pct/15*100,100), raw:r.price_range_pct.toFixed(1)+'%', threshold:'≥10% = high risk'}},
    {{label:'Circuit Days', val:Math.min(r.circuit_days/4*100,100),  raw:r.circuit_days+' days',        threshold:'≥3 = high risk'}},
  ];

  if(mdsChart){{mdsChart.destroy(); mdsChart=null;}}
  const rl=riskLabel(r.score);
  mdsChart=new Chart(document.getElementById('mds-radar'),{{
    type:'radar',
    data:{{
      labels:indicators.map(x=>x.label),
      datasets:[{{
        label:sym,
        data:indicators.map(x=>x.val),
        backgroundColor:rl.color+'33',
        borderColor:rl.color,
        borderWidth:2,
        pointBackgroundColor:rl.color,
        pointRadius:4,
      }}]
    }},
    options:{{
      responsive:true, maintainAspectRatio:false,
      plugins:{{legend:{{display:false}}}},
      scales:{{r:{{
        beginAtZero:true, max:100,
        ticks:{{display:false}},
        grid:{{color:'rgba(128,128,128,0.2)'}},
        pointLabels:{{color:'var(--text)',font:{{size:11}}}},
      }}}},
    }}
  }});

  // Detail panel
  document.getElementById('mds-indicator-detail').innerHTML=
    `<div style="font-weight:600;margin-bottom:8px;color:${{rl.color}}">${{rl.label}} — Score: ${{r.score}}/14</div>`+
    indicators.map(ind=>
      `<div style="display:flex;justify-content:space-between;border-bottom:1px solid var(--border);padding:3px 0">
        <span style="color:var(--muted)">${{ind.label}}</span>
        <span><strong style="color:var(--text)">${{ind.raw}}</strong>
          <span style="font-size:10px;color:var(--muted);margin-left:6px">${{ind.threshold}}</span></span>
      </div>`
    ).join('')+
    `<div style="margin-top:10px;font-size:11px;color:var(--muted)">
      LTP: Rs ${{fmtf(r.ltp)}} · VWAP: Rs ${{fmtf(r.vwap)}} · Vol today: ${{fmt(r.vol_today)}}
    </div>
    <div style="margin-top:6px">
      <button class="btn btn-g" style="font-size:11px" onclick="document.getElementById('f-sym').value='${{r.symbol}}';showTab('mkt')">
        → View in Market Summary
      </button>
    </div>`;
}}

// ── BROKER COMPARISON ───────────────────────────────────────────────────────
let CMP_BROKERS = [];
let cmpChart = null;
let CS_DATA = [], csSortCol = 'total_buy_qty', csSortAsc = false;
let TB_DATA = [], tbSortCol = 'buy_qty', tbSortAsc = false;
let SELLER_DATA = [], sellerSortCol = 'qty', sellerSortAsc = false;
let SELLER_SYM = '', SELLER_BRK = 0, SELLER_DATE_FROM = '', SELLER_DATE_TO = '';

function initCmp(){{ renderCmpTags(); }}



function addCmpBroker(){{
  const val = document.getElementById('cmp-brk-input').value.trim();
  const brk = parseInt(val);
  if(!brk) return;
  if(CMP_BROKERS.length>=5){{
    alert('Maximum 5 brokers allowed for comparison.');
    document.getElementById('cmp-brk-input').value='';
    return;
  }}
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
  document.getElementById('cmp-tbody').innerHTML='<tr><td colspan="7"><div class="empty">Add brokers and click Compare.</div></td></tr>';
  document.getElementById('cmp-summary').style.display='none';
  document.getElementById('cmp-rate-cards').innerHTML='<div class="empty">Add brokers and click Compare.</div>';
  if(cmpChartQty){{ cmpChartQty.destroy(); cmpChartQty=null; }}
  document.getElementById('cmp-cnt').textContent='—';
  document.getElementById('cs-cnt').textContent='—';
  document.getElementById('cs-table-wrap').innerHTML='<div class="empty">Add brokers and click Compare.</div>';
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

  // ── Fetch LTP from daily_volume table (max contractRate of the day) ────────
  // Falls back to VWAP from holdings if daily_volume not yet populated
  let ltpVal = 0, ltpDate = dto, ltpLabel = 'LTP';
  try{{
    // Try daily_volume table first (has true LTP = max contractRate)
    const {{data:dvData, error:dvErr}} = await sb.from('daily_volume')
      .select('ltp,vwap,date')
      .eq('symbol', sym)
      .lte('date', dto)
      .gte('date', dfrom)
      .order('date', {{ascending:false}})
      .limit(1);
    if(!dvErr && dvData && dvData.length && dvData[0].ltp > 0){{
      ltpVal   = dvData[0].ltp;
      ltpDate  = dvData[0].date;
      ltpLabel = 'LTP';
    }} else {{
      // Fallback: compute VWAP from holdings table
      const {{data:hData}} = await sb.from('holdings')
        .select('buy_qty,buy_amt,date')
        .eq('symbol', sym)
        .lte('date', dto)
        .gte('date', dfrom)
        .order('date', {{ascending:false}})
        .limit(500);
      if(hData && hData.length){{
        ltpDate = hData[0].date;
        const dayRows = hData.filter(r=>r.date===ltpDate);
        const totalAmt = dayRows.reduce((s,r)=>s+(r.buy_amt||0),0);
        const totalQty = dayRows.reduce((s,r)=>s+(r.buy_qty||0),0);
        ltpVal   = totalQty>0 ? Math.round((totalAmt/totalQty)*100)/100 : 0;
        ltpLabel = 'LTP';
      }}
    }}
  }}catch(e){{console.error('LTP fetch error:',e);}}

  // Attach to every row
  results.forEach(r=>{{ r.ltp=ltpVal; r.ltp_date=ltpDate; r.ltp_label=ltpLabel; }});

  renderCmpChart(results);
  renderCmpTable(results);
  await Promise.all([
    buildTopBrokers(sym, dfrom, dto),
    buildCommonStocks(sym, brokerNums, dfrom, dto),
  ]);
}}

function sortTb(col){{
  if(tbSortCol===col) tbSortAsc=!tbSortAsc; else{{tbSortCol=col;tbSortAsc=false;}}
  renderTbTable(TB_DATA);
}}

async function buildTopBrokers(sym, dfrom, dto){{
  if(!sym){{ 
    document.getElementById('tb-cnt').textContent='—';
    return; 
  }}
  document.getElementById('tb-cnt').textContent='Loading…';
  document.getElementById('tb-tbody').innerHTML=
    '<tr><td colspan="8"><div class="loading"><div class="spinner"></div>Loading…</div></td></tr>';
  try{{
    console.log('Top buyers query:', sym, dfrom, dto);
    // Fetch all broker rows for this symbol in date range
    let all=[], off=0, lim=1000;
    while(true){{
      let tbq=sb.from('holdings').select(
        'broker,broker_name,buy_qty,buy_amt,total_sale_qty,ipo_sale_qty,bulk_sale_qty,bulk_sale_amt,holding_qty'
      ).eq('symbol',sym);
      if(dfrom) tbq=tbq.gte('date',dfrom);
      if(dto)   tbq=tbq.lte('date',dto);
      tbq=tbq.range(off,off+lim-1);
      const {{data,error}}=await tbq;
      if(error) throw error;
      all.push(...(data||[]));
      if(!data||data.length<lim) break;
      off+=lim;
    }}
    if(!all.length){{
      document.getElementById('tb-cnt').textContent='No data';
      document.getElementById('tb-tbody').innerHTML=
        '<tr><td colspan="8"><div class="empty">No data for '+sym+' in this date range.</div></td></tr>';
      return;
    }}

    // Aggregate per broker across all dates in range
    const bmap={{}};
    for(const r of all){{
      const b=r.broker;
      if(!bmap[b]) bmap[b]={{
        broker:b, name:r.broker_name||'',
        buy_qty:0,buy_amt:0,sale_qty:0,
        ipo_qty:0,bulk_qty:0,bulk_amt:0,holding_qty:0
      }};
      bmap[b].buy_qty    +=(r.buy_qty||0);
      bmap[b].buy_amt    +=(r.buy_amt||0);
      bmap[b].sale_qty   +=(r.total_sale_qty||0);
      bmap[b].ipo_qty    +=(r.ipo_sale_qty||0);
      bmap[b].bulk_qty   +=(r.bulk_sale_qty||0);
      bmap[b].bulk_amt   +=(r.bulk_sale_amt||0);
      bmap[b].holding_qty+=(r.holding_qty||0);
    }}

    // Fetch total market volume for market share %
    let marketVol=0;
    try{{
      const {{data:dv}}=await sb.from('daily_volume')
        .select('total_buy_qty').eq('symbol',sym)
        .gte('date',dfrom).lte('date',dto);
      if(dv) marketVol=dv.reduce((s,r)=>s+(r.total_buy_qty||0),0);
    }}catch(e){{}}

    const totalHoldingAll = Object.values(bmap).reduce((s,b)=>s+(b.holding_qty||0),0);
    const allBrkData = Object.values(bmap).map((b,i)=>{{
      const buyRate  = b.buy_qty>0   ? Math.round((b.buy_amt/b.buy_qty)*100)/100        : 0;
      const sellRate = b.bulk_qty>0  ? Math.round((b.bulk_amt/b.bulk_qty)*100)/100      : 0;
      const holdRate = b.holding_qty>0 ? Math.round(((b.buy_amt-b.bulk_amt)/b.holding_qty)*100)/100 : 0;
      const mktPct   = totalHoldingAll>0 ? Math.round((b.holding_qty/totalHoldingAll)*10000)/100 : 0;
      return {{
        broker:b.broker, name:b.name,
        buy_qty:b.buy_qty, avg_buy_rate:buyRate,
        sale_qty:b.sale_qty,
        ipo_qty:b.ipo_qty,
        bulk_qty:b.bulk_qty, avg_sell_rate:sellRate,
        holding_qty:b.holding_qty, avg_hold_rate:holdRate,
        market_pct:mktPct, rank:0
      }};
    }}).sort((a,b)=>b.buy_qty-a.buy_qty);
    // Top 10 by buy qty
    TB_DATA = allBrkData.slice(0,10);
    TB_DATA.forEach((r,i)=>r.rank=i+1);

    document.getElementById('tb-cnt').textContent=
      'Top 10 of '+allBrkData.length+' brokers · '+sym+' · '+dfrom+' → '+dto+
      (marketVol>0?' · Mkt Vol: '+fmt(marketVol):'');
    renderTbTable(TB_DATA);

  }}catch(e){{
    console.error('Top brokers error:',e);
    document.getElementById('tb-tbody').innerHTML=
      '<tr><td colspan="8"><div class="empty">Error: '+e.message+'</div></td></tr>';
  }}
}}

function renderTbTable(data){{
  const tb=document.getElementById('tb-tbody');
  if(!data||!data.length){{tb.innerHTML='<tr><td colspan="8"><div class="empty">No data.</div></td></tr>';return;}}
  const sorted=[...data].sort((a,b)=>{{
    let va=a[tbSortCol], vb=b[tbSortCol];
    if(typeof va==='number') return tbSortAsc?va-vb:vb-va;
    return tbSortAsc?String(va||'').localeCompare(String(vb||'')):String(vb||'').localeCompare(String(va||''));
  }});
  const medals=['🥇','🥈','🥉'];
  function qr(qty,rate,cls){{
    return '<div class="brk-cell">'
      +'<div class="m '+cls+'" style="font-size:12px">'+fmt(qty)+'</div>'
      +'<div style="color:var(--amber);font-size:10px">Rs '+fmtf(rate)+'</div>'
      +'</div>';
  }}
  tb.innerHTML=sorted.map(r=>{{
    const rk=r.rank; const medal=medals[rk-1]||rk;
    const nhcls=r.holding_qty>=0?'pos':'neg';
    const isSelected=CMP_BROKERS.find(b=>b.broker===r.broker);
    return '<tr style="'+(isSelected?'background:rgba(0,200,255,0.05);border-left:2px solid var(--cyan)':'')+'">'
      +'<td class="m" style="color:var(--muted)">'+medal+'</td>'
      +'<td><span class="brk" style="'+(isSelected?'border-color:var(--cyan)':'')+'">'
        +r.broker+'</span>'
        +(isSelected?'<div style="font-size:9px;color:var(--cyan)">selected</div>':'')+'</td>'
      +'<td style="cursor:pointer" title="Click to see who sold to this broker" '
        +'data-broker="'+r.broker+'" data-name="'+r.name.replace(/"/g,'&quot;')+'" '
+'onclick="handleBuyClick(this)">'
        +'<div class="brk-cell">'
        +'<div class="m pos" style="font-size:12px;text-decoration:underline dotted;color:var(--cyan)">'+fmt(r.buy_qty)+'</div>'
        +'<div style="color:var(--amber);font-size:10px">Rs '+fmtf(r.avg_buy_rate)+'</div>'
        +'<div style="font-size:9px;color:var(--muted);margin-top:1px">🔍 click to drill down</div>'
        +'</div></td>'
      +'<td>'+qr(r.sale_qty,r.avg_sell_rate,'neg')+'</td>'
      +'<td><span class="ipo">'+fmt(r.ipo_qty)+'</span></td>'
      +'<td>'+qr(r.bulk_qty,r.avg_sell_rate,'')+'</td>'
      +'<td>'+qr(r.holding_qty,r.avg_hold_rate,nhcls)+'</td>'
      +'<td class="m" style="color:'+(r.market_pct>=10?'var(--cyan)':r.market_pct>=5?'var(--amber)':'var(--muted)')+'">'+r.market_pct.toFixed(2)+'%</td>'
      +'</tr>';
  }}).join('');
}}

function sortSeller(col){{
  if(sellerSortCol===col) sellerSortAsc=!sellerSortAsc;
  else{{sellerSortCol=col; sellerSortAsc=false;}}
  renderSellerTable(SELLER_DATA);
}}

function handleBuyClick(td){{
  const broker = parseInt(td.dataset.broker);
  const name   = td.dataset.name||'';
  openSellerDrilldown(broker, name);
}}

async function openSellerDrilldown(buyerBroker, buyerName){{
  const sym   = document.getElementById('f-sym').value.trim();
  const dfrom = document.getElementById('cmp-dfrom').value;
  const dto   = document.getElementById('cmp-dto').value;
  if(!sym||!dfrom||!dto){{
    alert('Please select a symbol and date range first.');
    return;
  }}

  SELLER_SYM=sym; SELLER_BRK=buyerBroker;
  SELLER_DATE_FROM=dfrom; SELLER_DATE_TO=dto;

  const panel=document.getElementById('seller-panel');
  panel.style.display='block';
  panel.scrollIntoView({{behavior:'smooth',block:'start'}});
  document.getElementById('seller-title').textContent=
    'Who sold '+sym+' to Broker '+buyerBroker+' ('+buyerName+') · '+dfrom+' → '+dto;
  document.getElementById('seller-tbody').innerHTML=
    '<tr><td colspan="4"><div class="loading"><div class="spinner"></div>Loading sellers…</div></td></tr>';

  try{{
    // Fetch raw floorsheet: all transactions where buyerMemberId = selected broker for this symbol
    // We need holdings data — but holdings is aggregated. We can infer sellers from
    // the fact that each sell transaction matches a buy transaction.
    // Best approach: query holdings for this symbol in date range for ALL brokers (sellers)
    // and cross-reference with how much the buyer bought on each date.

    // Step 1: Get total buy qty of selected broker per date
    const {{data:buyerRows, error:be}} = await sb.from('holdings')
      .select('date,buy_qty,buy_amt')
      .eq('symbol', sym).eq('broker', buyerBroker)
      .gte('date', dfrom).lte('date', dto);
    if(be) throw be;
    if(!buyerRows||!buyerRows.length){{
      document.getElementById('seller-tbody').innerHTML=
        '<tr><td colspan="4"><div class="empty">No purchase data for Broker '+buyerBroker+' in this period.</div></td></tr>';
      return;
    }}

    const totalBuyerQty = buyerRows.reduce((s,r)=>s+(r.buy_qty||0),0);
    const avgBuyRate = totalBuyerQty>0
      ? Math.round((buyerRows.reduce((s,r)=>s+(r.buy_amt||0),0)/totalBuyerQty)*100)/100 : 0;

    // Step 2: Query broker_trades — exact qty sold by each seller TO this buyer
    let btRows=[], off2=0, lim2=1000;
    try{{
      while(true){{
        const {{data,error}}=await sbTrades.from('broker_trades').select(
          'seller,qty,amount,date'
        ).eq('symbol',sym).eq('buyer',buyerBroker)
         .gte('date',dfrom).lte('date',dto)
         .range(off2,off2+lim2-1);
        if(error) throw error;
        btRows.push(...(data||[]));
        if(!data||data.length<lim2) break;
        off2+=lim2;
      }}
    }}catch(e){{
      // broker_trades table may not exist yet — fall back to holdings
      console.warn('broker_trades not available, using holdings fallback:', e.message);
      btRows=[];
    }}

    // Step 3: Total market volume for % calculation
    let totalMarketQty=0;
    try{{
      const {{data:dv}}=await sb.from('daily_volume')
        .select('total_buy_qty').eq('symbol',sym)
        .gte('date',dfrom).lte('date',dto);
      if(dv) totalMarketQty=dv.reduce((s,r)=>s+(r.total_buy_qty||0),0);
    }}catch(e){{}}

    if(btRows.length){{
      // ── EXACT: from broker_trades table ──────────────────────────────────
      // Fetch seller names from holdings
      const sellerNums=[...new Set(btRows.map(r=>r.seller))];
      const nameMap={{}};
      try{{
        const {{data:nh}}=await sb.from('holdings').select('broker,broker_name')
          .eq('symbol',sym).in('broker',sellerNums).limit(200);
        (nh||[]).forEach(r=>{{if(r.broker_name) nameMap[r.broker]=r.broker_name;}});
      }}catch(e){{}}

      // Aggregate by seller
      const sellerMap={{}};
      for(const r of btRows){{
        const s=r.seller;
        if(!sellerMap[s]) sellerMap[s]={{seller:s,qty:0,amount:0}};
        sellerMap[s].qty    +=(r.qty||0);
        sellerMap[s].amount +=(r.amount||0);
      }}

      SELLER_DATA=Object.values(sellerMap).map(s=>{{
        const avgRate  = s.qty>0 ? Math.round((s.amount/s.qty)*100)/100 : 0;
        const buyerPct = totalBuyerQty>0 ? Math.round((s.qty/totalBuyerQty)*10000)/100 : 0;
        const mktPct   = totalMarketQty>0 ? Math.round((s.qty/totalMarketQty)*10000)/100 : 0;
        return {{
          seller:s.seller, name:nameMap[s.seller]||'',
          qty:s.qty, avg_rate:avgRate,
          buyer_pct:buyerPct, day_pct:mktPct, rank:0
        }};
      }}).sort((a,b)=>b.qty-a.qty);
      SELLER_DATA.forEach((r,i)=>r.rank=i+1);

    }}else{{
      // ── FALLBACK: estimate sellers proportionally per date ─────────
      const buyerByDate={{}};
      buyerRows.forEach(r=>buyerByDate[r.date]=(r.buy_qty||0));

      let allSellers=[], off3=0;
      while(true){{
        const {{data,error}}=await sb.from('holdings').select(
          'date,broker,broker_name,bulk_sale_qty,bulk_sale_amt'
        ).eq('symbol',sym).gte('date',dfrom).lte('date',dto)
         .gt('bulk_sale_qty',0).range(off3,off3+999);
        if(error) break;
        allSellers.push(...(data||[]));
        if(!data||data.length<1000) break;
        off3+=1000;
      }}

      // Get market volume per date for proportional estimation
      const mktByDate={{}};
      try{{
        const {{data:dv2}}=await sb.from('daily_volume')
          .select('date,total_buy_qty').eq('symbol',sym)
          .gte('date',dfrom).lte('date',dto);
        (dv2||[]).forEach(r=>mktByDate[r.date]=(r.total_buy_qty||0));
      }}catch(e){{}}

      // Estimate: each seller sold to this buyer proportionally
      // (buyer_qty / total_market_qty) * seller_qty
      const sellerMap={{}};
      for(const r of allSellers){{
        const mktVol   = mktByDate[r.date]||1;
        const buyerQty = buyerByDate[r.date]||0;
        if(buyerQty<=0) continue;
        const ratio  = buyerQty/mktVol;
        const estQty = Math.round((r.bulk_sale_qty||0)*ratio);
        if(estQty<=0) continue;
        const b=r.broker;
        if(!sellerMap[b]) sellerMap[b]={{seller:b,name:r.broker_name||'',qty:0,bulk_amt:0}};
        sellerMap[b].qty     += estQty;
        sellerMap[b].bulk_amt+= Math.round((r.bulk_sale_amt||0)*ratio);
      }}
      SELLER_DATA=Object.values(sellerMap).map(s=>{{
        const avgRate  = s.qty>0 ? Math.round((s.bulk_amt/s.qty)*100)/100 : 0;
        const buyerPct = totalBuyerQty>0 ? Math.round((s.qty/totalBuyerQty)*10000)/100 : 0;
        const mktPct   = totalMarketQty>0 ? Math.round((s.qty/totalMarketQty)*10000)/100 : 0;
        return {{seller:s.seller,name:s.name,qty:s.qty,avg_rate:avgRate,
                 buyer_pct:buyerPct,day_pct:mktPct,rank:0}};
      }}).sort((a,b)=>b.qty-a.qty);
      SELLER_DATA.forEach((r,i)=>r.rank=i+1);
    }}

    document.getElementById('seller-title').textContent=
      'Who sold '+sym+' to Broker '+buyerBroker+' ('+buyerName+') · '+dfrom+' → '+dto+
      ' · Buyer total: '+fmt(totalBuyerQty)+' @ Rs '+fmtf(avgBuyRate)+
      (btRows.length?' (exact)':' (approximate — run compile to get exact data)');

    sellerSortCol='qty'; sellerSortAsc=false;
    renderSellerTable(SELLER_DATA);

  }}catch(e){{
    console.error('Seller drill-down error:',e);
    document.getElementById('seller-tbody').innerHTML=
      '<tr><td colspan="4"><div class="empty">Error: '+e.message+'</div></td></tr>';
  }}
}}

function renderSellerTable(data){{
  const tb=document.getElementById('seller-tbody');
  if(!data||!data.length){{tb.innerHTML='<tr><td colspan="4"><div class="empty">No sellers found.</div></td></tr>';return;}}
  const sorted=[...data].sort((a,b)=>{{
    let va=a[sellerSortCol],vb=b[sellerSortCol];
    if(va===null||va===undefined) va=0;
    if(vb===null||vb===undefined) vb=0;
    if(typeof va==='number') return sellerSortAsc?va-vb:vb-va;
    return sellerSortAsc?String(va||'').localeCompare(String(vb||'')):String(vb||'').localeCompare(String(va||''));
  }});
  const medals=['🥇','🥈','🥉'];
  const hasExact=sorted.some(r=>r.buyer_pct!==null&&r.buyer_pct!==undefined);
  const hdrEl=document.getElementById('seller-pct-hdr');
  if(hdrEl) hdrEl.innerHTML=hasExact
    ? '% of Buyer Total ↕<br><span style="font-weight:400;font-size:9px;color:var(--muted)">exact from trades</span>'
    : 'Mkt Share % ↕<br><span style="font-weight:400;font-size:9px;color:var(--muted)">approx</span>';
  const maxPct=Math.max(...sorted.map(r=>(r.buyer_pct!=null?r.buyer_pct:r.day_pct)||0),1);
  tb.innerHTML=sorted.map(r=>{{
    const rk=r.rank;
    const pct=(r.buyer_pct!=null?r.buyer_pct:r.day_pct)||0;
    const pctBar=Math.max(2,pct/maxPct*80);
    const pctColor=pct>30?'var(--cyan)':pct>10?'var(--amber)':'var(--muted)';
    return '<tr>'
      +'<td class="m" style="color:var(--muted)">'+( medals[rk-1]||rk)+'</td>'
      +'<td><span class="brk sell">'+r.seller+'</span>'
        +(r.name?'<div class="bname">'+r.name+'</div>':'')+'</td>'
      +'<td><div class="brk-cell">'
        +'<div class="m neg">'+fmt(r.qty)+'</div>'
        +'<div style="color:var(--amber);font-size:10px">Rs '+fmtf(r.avg_rate)+'</div>'
        +'</div></td>'
      +'<td><div style="display:flex;align-items:center;gap:6px">'
        +'<div style="flex:1;height:4px;background:var(--muted2);border-radius:2px;max-width:60px">'
        +'<div style="width:'+pctBar+'px;height:100%;background:var(--red);border-radius:2px"></div></div>'
        +'<span class="m" style="color:'+pctColor+'">'+pct.toFixed(2)+'%</span>'
        +'</div></td>'
      +'</tr>';
  }}).join('');
}}

function sortCs(col){{
  if(csSortCol===col) csSortAsc=!csSortAsc; else{{csSortCol=col;csSortAsc=false;}}
  renderCsTable(CS_DATA, CS_BROKERS_META);
}}

async function buildCommonStocks(sym, brokerNums, dfrom, dto){{
  document.getElementById('cs-cnt').textContent='Loading…';
  document.getElementById('cs-table-wrap').innerHTML=
    '<div class="loading"><div class="spinner"></div>Loading…</div>';
  try{{
    // Fetch holdings for ALL selected brokers in date range
    let all=[], off=0, lim=1000;
    while(true){{
      const {{data,error}}=await sb.from('holdings').select(
        'symbol,broker,broker_name,buy_qty,buy_amt,total_sale_qty,ipo_sale_qty,bulk_sale_qty,bulk_sale_amt,holding_qty'
      ).in('broker',brokerNums).gte('date',dfrom).lte('date',dto)
       .range(off,off+lim-1);
      if(error) throw error;
      all.push(...(data||[]));
      if(!data||data.length<lim) break;
      off+=lim;
    }}
    if(!all.length){{
      document.getElementById('cs-cnt').textContent='No data';
      document.getElementById('cs-table-wrap').innerHTML=
        '<div class="empty">No data for selected brokers in this date range.</div>';
      return;
    }}

    // Build broker name lookup
    CS_BROKERS_META={{}};
    for(const r of all) if(r.broker_name) CS_BROKERS_META[r.broker]=r.broker_name;

    // Aggregate per symbol per broker
    const symBrkMap={{}};
    for(const r of all){{
      const s=r.symbol, b=r.broker;
      if(!symBrkMap[s]) symBrkMap[s]={{}};
      if(!symBrkMap[s][b]) symBrkMap[s][b]={{
        buy_qty:0,buy_amt:0,sale_qty:0,ipo_qty:0,
        bulk_qty:0,bulk_amt:0,holding_qty:0
      }};
      const d=symBrkMap[s][b];
      d.buy_qty    +=(r.buy_qty||0);
      d.buy_amt    +=(r.buy_amt||0);
      d.sale_qty   +=(r.total_sale_qty||0);
      d.ipo_qty    +=(r.ipo_sale_qty||0);
      d.bulk_qty   +=(r.bulk_sale_qty||0);
      d.bulk_amt   +=(r.bulk_sale_amt||0);
      d.holding_qty+=(r.holding_qty||0);
    }}

    // ── ONLY include symbols traded by ALL selected brokers ───────────────
    const commonSyms = Object.entries(symBrkMap).filter(([sym, brkData])=>{{
      return brokerNums.every(b=> brkData[b] && (brkData[b].buy_qty||0)>0);
    }});

    if(!commonSyms.length){{
      document.getElementById('cs-cnt').textContent='No common stocks found';
      document.getElementById('cs-table-wrap').innerHTML=
        '<div class="empty">No symbols were traded by ALL '+brokerNums.length+' selected brokers in this period.</div>';
      return;
    }}

    // Build CS_DATA — fetch LTP + market volume, filter positive, sort by holding
    const commonSymList = commonSyms.map(([s])=>s);

    // Fetch LTP + total market volume for common symbols
    const mktMap={{}};
    try{{
      let dvRows=[], dvOff=0;
      while(true){{
        const {{data:dv}}=await sb.from('daily_volume')
          .select('symbol,ltp,vwap,total_buy_qty,date')
          .in('symbol',commonSymList)
          .gte('date',dfrom).lte('date',dto)
          .range(dvOff,dvOff+999);
        if(!dv||!dv.length) break;
        dv.forEach(r=>{{
          if(!mktMap[r.symbol]) mktMap[r.symbol]={{ltp:0,vwap:0,mktVol:0,latestDate:''}};
          mktMap[r.symbol].mktVol += (r.total_buy_qty||0);
          if(r.date > mktMap[r.symbol].latestDate){{
            mktMap[r.symbol].ltp  = r.ltp||0;
            mktMap[r.symbol].vwap = r.vwap||0;
            mktMap[r.symbol].latestDate = r.date;
          }}
        }});
        if(dv.length<1000) break;
        dvOff+=1000;
      }}
    }}catch(e){{console.warn('daily_volume fetch failed:',e);}}

    const allCsData = commonSyms.map(([sym, brkData])=>{{
      const totalHolding = Object.values(brkData).reduce((s,b)=>s+(b.holding_qty||0),0);
      const totalBuy     = Object.values(brkData).reduce((s,b)=>s+(b.buy_qty||0),0);
      const mkt          = mktMap[sym]||{{ltp:0,vwap:0,mktVol:0}};
      // Holding % = combined holding / market volume * 100
      const holdPct      = mkt.mktVol>0 ? Math.round((Math.abs(totalHolding)/mkt.mktVol)*10000)/100 : 0;
      return {{ symbol:sym, totalHolding, totalBuy, ltp:mkt.ltp, vwap:mkt.vwap,
               mktVol:mkt.mktVol, holdPct, brkData }};
    }})
    .filter(r=>{{
      // ALL selected brokers must have positive holding for this script
      return brokerNums.every(b=> (r.brkData[b]?.holding_qty||0) > 0);
    }})
    .sort((a,b)=>b.totalHolding-a.totalHolding);

    if(!allCsData.length){{
      document.getElementById('cs-cnt').textContent='No common symbols with positive holdings for all brokers';
      document.getElementById('cs-table-wrap').innerHTML=
        '<div class="empty">No scripts where ALL selected brokers have positive holdings. Try a wider date range.</div>';
      return;
    }}

    CS_DATA = allCsData.slice(0,10);
    CS_DATA.forEach((r,i)=>r.rank=i+1);

    document.getElementById('cs-cnt').textContent=
      'Top '+CS_DATA.length+' of '+allCsData.length+' common symbols · sorted by net holding · '+
      brokerNums.length+' brokers · '+dfrom+' → '+dto;
    renderCsTable(CS_DATA, CS_BROKERS_META);

  }}catch(e){{
    console.error('Common stocks error:',e);
    document.getElementById('cs-table-wrap').innerHTML=
      '<div class="empty">Error: '+e.message+'</div>';
  }}
}}

function renderCsTable(data, brkMeta){{
  if(!data||!data.length){{
    document.getElementById('cs-table-wrap').innerHTML='<div class="empty">No data.</div>';
    return;
  }}
  brkMeta=brkMeta||CS_BROKERS_META||{{}};

  // Sorted broker list
  const allBrokers=Object.keys(data[0].brkData||{{}}).map(Number).sort((a,b)=>a-b);
  const nBrk=allBrokers.length;

  // Sort data
  const sorted=[...data].sort((a,b)=>{{
    if(csSortCol==='rank')   return csSortAsc?a.rank-b.rank:b.rank-a.rank;
    if(csSortCol==='symbol') return csSortAsc?a.symbol.localeCompare(b.symbol):b.symbol.localeCompare(a.symbol);
    return (()=>{{
    if(csSortCol==='ltp')     return csSortAsc?a.ltp-b.ltp:b.ltp-a.ltp;
    if(csSortCol==='mktVol')  return csSortAsc?a.mktVol-b.mktVol:b.mktVol-a.mktVol;
    if(csSortCol==='holdPct') return csSortAsc?a.holdPct-b.holdPct:b.holdPct-a.holdPct;
    return csSortAsc?a.totalHolding-b.totalHolding:b.totalHolding-a.totalHolding;
  }})();
  }});

  const medals=['🥇','🥈','🥉'];

  // Compact broker name (first word only)
  function shortName(b){{ return (brkMeta[b]||'Broker '+b).split(' ')[0]; }}

  // Header row — broker names
  const thBrk = allBrokers.map(b=>
    `<th colspan="4" style="text-align:center;border-left:2px solid var(--border2);background:var(--s2);padding:6px 4px">
      <div><span class="brk" style="font-size:11px">${{b}}</span></div>
      <div style="font-size:10px;color:var(--muted);margin-top:2px;white-space:nowrap;overflow:hidden;max-width:100px">${{shortName(b)}}</div>
    </th>`
  ).join('');

  // Sub-header
  const thSub = allBrokers.map(()=>
    `<th style="border-left:2px solid var(--border2);font-size:9px">Buy<br>Rate</th>`+
    `<th style="font-size:9px">Sell<br>Rate</th>`+
    `<th style="font-size:9px">IPO</th>`+
    `<th style="font-size:9px">Hold<br>Hold%</th>`
  ).join('');

  // Rows
  const rows=sorted.map(r=>{{
    const rk=r.rank;
    const cells=allBrokers.map(b=>{{
      const d=r.brkData[b]||{{}};
      const buyRate  = d.buy_qty>0   ? Math.round((d.buy_amt/d.buy_qty)*100)/100   : 0;
      const sellRate = d.bulk_qty>0  ? Math.round((d.bulk_amt/d.bulk_qty)*100)/100 : 0;
      const holdRate = d.holding_qty>0 ? Math.round(((d.buy_amt-d.bulk_amt)/d.holding_qty)*100)/100 : 0;
      const nhcls    = (d.holding_qty||0)>=0?'pos':'neg';
      // Broker holding % = broker holding / market volume * 100
      const bHoldPct = r.mktVol>0 ? ((d.holding_qty||0)/r.mktVol*100).toFixed(2) : '—';
      const pctColor = parseFloat(bHoldPct)>=10?'var(--cyan)':parseFloat(bHoldPct)>=5?'var(--amber)':'var(--green)';
      return `<td style="border-left:2px solid var(--border2);padding:4px 6px">
          <div class="m pos" style="font-size:11px">${{fmt(d.buy_qty||0)}}</div>
          <div style="color:var(--amber);font-size:9px">Rs ${{fmtf(buyRate)}}</div>
        </td>
        <td style="padding:4px 6px">
          <div class="m neg" style="font-size:11px">${{fmt(d.sale_qty||0)}}</div>
          <div style="color:var(--amber);font-size:9px">Rs ${{fmtf(sellRate)}}</div>
        </td>
        <td style="padding:4px 6px">
          <span class="ipo" style="font-size:10px">${{fmt(d.ipo_qty||0)}}</span>
        </td>
        <td style="padding:4px 6px">
          <div class="m ${{nhcls}}" style="font-size:11px">${{fmt(d.holding_qty||0)}}</div>
          <div style="color:${{pctColor}};font-size:9px;font-weight:600">${{bHoldPct}}%</div>
        </td>`;
    }}).join('');
    return `<tr>
      <td style="padding:4px 8px;font-size:11px;color:var(--muted)">${{medals[rk-1]||rk}}</td>
      <td class="sym" style="padding:4px 8px">${{r.symbol}}</td>
      <td class="m" style="padding:4px 8px;font-size:11px;color:var(--amber)">${{r.ltp ? 'Rs '+fmtf(r.ltp) : '—'}}</td>
      <td class="m" style="padding:4px 8px;font-size:11px;color:var(--muted)">${{fmt(r.mktVol||0)}}</td>
      <td class="m" style="padding:4px 8px;font-size:11px;color:${{r.holdPct>=10?'var(--cyan)':r.holdPct>=5?'var(--amber)':'var(--green)'}}">${{r.holdPct.toFixed(2)}}%</td>
      ${{cells}}
    </tr>`;
  }}).join('');

  // Compute approx width: 120px fixed + 160px per broker
  const minW = 280 + nBrk * 180;

  document.getElementById('cs-table-wrap').innerHTML=
    `<table style="min-width:${{minW}}px;width:100%;border-collapse:collapse">
      <thead>
        <tr>
          <th rowspan="2" onclick="sortCs('rank')" style="padding:6px 8px;min-width:36px"># ↕</th>
          <th rowspan="2" onclick="sortCs('symbol')" style="padding:6px 8px;min-width:60px">Symbol ↕</th>
          <th rowspan="2" onclick="sortCs('ltp')" style="padding:6px 8px;min-width:70px">LTP ↕<div style="font-weight:400;font-size:9px;color:var(--muted)">Last price</div></th>
          <th rowspan="2" onclick="sortCs('mktVol')" style="padding:6px 8px;min-width:80px">Mkt Vol ↕<div style="font-weight:400;font-size:9px;color:var(--muted)">Period total</div></th>
          <th rowspan="2" onclick="sortCs('holdPct')" style="padding:6px 8px;min-width:70px">Hold% ↕<div style="font-weight:400;font-size:9px;color:var(--muted)">vs mkt vol</div></th>
          ${{thBrk}}
        </tr>
        <tr>${{thSub}}</tr>
      </thead>
      <tbody>${{rows}}</tbody>
    </table>`;
}}

let CS_BROKERS_META={{}};

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
  // ── Compute summary totals ─────────────────────────────────────────────
  if(results.length){{
    const sumBuy  = results.reduce((s,r)=>s+(r.buy_qty||0),0);
    const sumSell = results.reduce((s,r)=>s+(r.sell_qty||0),0);
    const sumIpo  = results.reduce((s,r)=>s+(r.ipo_qty||0),0);
    const sumBulk = results.reduce((s,r)=>s+(r.bulk_qty||0),0);
    const sumHold = results.reduce((s,r)=>s+(r.net_holding||0),0);
    // Weighted avg rates
    const totBuyAmt  = results.reduce((s,r)=>s+(r.buy_qty||0)*(r.avg_buy_rate||0),0);
    const totSellAmt = results.reduce((s,r)=>s+(r.sell_qty||0)*(r.avg_sell_rate||0),0);
    const totBulkAmt = results.reduce((s,r)=>s+(r.bulk_qty||0)*(r.bulk_rate||0),0);
    const avgBuyRate  = sumBuy>0  ? Math.round((totBuyAmt/sumBuy)*100)/100   : 0;
    const avgSellRate = sumSell>0 ? Math.round((totSellAmt/sumSell)*100)/100 : 0;
    const avgBulkRate = sumBulk>0 ? Math.round((totBulkAmt/sumBulk)*100)/100 : 0;
    const holdRate    = sumHold>0
      ? Math.round(((results.reduce((s,r)=>s+(r.buy_qty||0)*(r.avg_buy_rate||0),0)
                   - results.reduce((s,r)=>s+(r.bulk_qty||0)*(r.bulk_rate||0),0))
                   / sumHold)*100)/100 : 0;
    // LTP same for all (script-level)
    const ltp = results[0]?.ltp||0;
    const nhcls = sumHold>=0?'pos':'neg';
    document.getElementById('cmp-summary').style.display='';
    document.getElementById('cmp-sum-label').textContent='∑ '+results.length+' brokers';
    document.getElementById('cmp-sum-buy').textContent      = fmt(sumBuy);
    document.getElementById('cmp-sum-buy-rate').textContent = avgBuyRate?'Rs '+fmtf(avgBuyRate):'—';
    document.getElementById('cmp-sum-sell').textContent     = fmt(sumSell);
    document.getElementById('cmp-sum-sell-rate').textContent= avgSellRate?'Rs '+fmtf(avgSellRate):'—';
    document.getElementById('cmp-sum-ipo').textContent      = fmt(sumIpo);
    document.getElementById('cmp-sum-bulk').textContent     = fmt(sumBulk);
    document.getElementById('cmp-sum-bulk-rate').textContent= avgBulkRate?'Rs '+fmtf(avgBulkRate):'—';
    document.getElementById('cmp-sum-hold').className       = 'm '+nhcls;
    document.getElementById('cmp-sum-hold').textContent     = fmt(sumHold);
    document.getElementById('cmp-sum-hold-rate').textContent= holdRate?'Rs '+fmtf(holdRate):'—';
    document.getElementById('cmp-sum-ltp').textContent      = ltp?'Rs '+fmtf(ltp):'—';
  }} else {{
    document.getElementById('cmp-summary').style.display='none';
  }}
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
    <td><div class="m" style="color:var(--amber);font-size:14px;font-weight:600">Rs ${{fmtf(r.ltp||0)}}</div>
        <div style="font-size:10px;color:var(--muted)">${{r.ltp_label||'LTP'}} · ${{r.ltp_date||'—'}}</div></td>
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
