"""
compile_holdings.py
───────────────────
Reads all daily NEPSE floorsheet Excel files from ./data/ folder.
Each file has a sheet named "MASTER " with these columns:

  RAW TRANSACTION COLUMNS (used for computation):
    Date, Stock Symbol, Buyer, Seller, Quantity, Rate (Rs), Amount (Rs)

  PRE-COMPUTED SUMMARY COLUMNS (ignored — we recompute from raw):
    BROKER #, BUY_QTY, SELL_QTY, IPO SALES, BULK SALES_QTY, HOLDINGS, etc.

LOGIC:
  Raw rows     = rows where BROKER # is null (the transaction-level data)
  IPO sale     = Quantity == 10 on the SELL side
  bulk_sale    = total_sale_qty - ipo_sale_qty
  holding_qty  = buy_qty - bulk_sale_qty

OUTPUT:
  holdings_summary.json  → dashboard data file (loaded by index.html)
  holdings_full.xlsx     → full export with Daily + Summary sheets

USAGE:
  python compile_holdings.py
"""

import os, json, glob
import pandas as pd
from datetime import datetime

# ── CONFIG ────────────────────────────────────────────────────────────────
DATA_DIR     = "./data"
OUTPUT_JSON  = "holdings_summary.json"
OUTPUT_EXCEL = "holdings_full.xlsx"
SHEET_NAME   = "MASTER "    # note trailing space — matches your file
IPO_QTY      = 10           # sale qty == 10 → IPO sale
# ─────────────────────────────────────────────────────────────────────────


def load_all_files():
    """Load and combine all daily Excel files from DATA_DIR."""
    files = sorted(glob.glob(os.path.join(DATA_DIR, "*.xlsx")))
    if not files:
        raise FileNotFoundError(
            f"No .xlsx files found in '{DATA_DIR}/'. "
            "Upload your daily floorsheet files there first."
        )
    print(f"Found {len(files)} file(s) in {DATA_DIR}/")
    frames = []
    for f in files:
        print(f"  Loading: {os.path.basename(f)}")
        try:
            # Try the known sheet name first, fall back to first sheet
            try:
                df = pd.read_excel(f, sheet_name=SHEET_NAME, engine="openpyxl")
            except Exception:
                df = pd.read_excel(f, sheet_name=0, engine="openpyxl")

            # Standardise column names (strip whitespace)
            df.columns = [str(c).strip() for c in df.columns]
            frames.append(df)
        except Exception as e:
            print(f"  WARNING: Skipping {f} — {e}")

    combined = pd.concat(frames, ignore_index=True)
    print(f"Total rows loaded: {len(combined):,}")
    return combined


def extract_raw_transactions(df):
    """
    Keep only raw transaction rows.
    Your file has two kinds of rows:
      - Raw transactions : BROKER # is NaN
      - Summary rows     : BROKER # has a value  (pre-computed, we ignore these)
    """
    # Detect summary column — try 'BROKER #' first
    if "BROKER #" in df.columns:
        raw = df[df["BROKER #"].isna()].copy()
    else:
        raw = df.copy()  # no summary rows mixed in — use everything

    # Ensure numeric types
    for col in ["Quantity", "Rate (Rs)", "Amount (Rs)"]:
        if col in raw.columns:
            raw[col] = pd.to_numeric(raw[col], errors="coerce").fillna(0)

    # Ensure integer broker numbers
    for col in ["Buyer", "Seller"]:
        if col in raw.columns:
            raw[col] = pd.to_numeric(raw[col], errors="coerce").fillna(0).astype(int)

    # Standardise date
    raw["Date"] = pd.to_datetime(raw["Date"], errors="coerce").dt.strftime("%Y-%m-%d")

    # Standardise symbol
    if "Stock Symbol" in raw.columns:
        raw["Stock Symbol"] = raw["Stock Symbol"].astype(str).str.strip()

    print(f"Raw transaction rows: {len(raw):,}")
    return raw


def compute_holdings(raw):
    """
    Compute per-broker, per-stock, per-date holdings.

      buy_qty       = sum of Quantity where broker == Buyer
      total_sale    = sum of Quantity where broker == Seller
      ipo_sale_qty  = sum of Quantity where broker == Seller AND Quantity == 10
      bulk_sale_qty = total_sale - ipo_sale_qty
      holding_qty   = buy_qty - bulk_sale_qty
    """
    grp_cols = ["Date", "Stock Symbol"]

    # ── BUY SIDE ──────────────────────────────────────────────────────────
    buy = (
        raw.groupby(grp_cols + ["Buyer"])
        .agg(buy_qty=("Quantity", "sum"), buy_amt=("Amount (Rs)", "sum"))
        .reset_index()
        .rename(columns={"Buyer": "broker"})
    )

    # ── SELL SIDE — all ───────────────────────────────────────────────────
    sell_all = (
        raw.groupby(grp_cols + ["Seller"])
        .agg(total_sale_qty=("Quantity", "sum"), sales_amt=("Amount (Rs)", "sum"))
        .reset_index()
        .rename(columns={"Seller": "broker"})
    )

    # ── SELL SIDE — IPO only (Quantity == 10) ─────────────────────────────
    ipo_mask = raw["Quantity"] == IPO_QTY
    sell_ipo = (
        raw[ipo_mask]
        .groupby(grp_cols + ["Seller"])
        .agg(ipo_sale_qty=("Quantity", "sum"), ipo_sale_amt=("Amount (Rs)", "sum"))
        .reset_index()
        .rename(columns={"Seller": "broker"})
    )

    # ── MERGE SELL ─────────────────────────────────────────────────────────
    sell = sell_all.merge(sell_ipo, on=grp_cols + ["broker"], how="left")
    sell["ipo_sale_qty"]  = sell["ipo_sale_qty"].fillna(0)
    sell["ipo_sale_amt"]  = sell["ipo_sale_amt"].fillna(0)
    sell["bulk_sale_qty"] = sell["total_sale_qty"] - sell["ipo_sale_qty"]
    sell["bulk_sale_amt"] = sell["sales_amt"]      - sell["ipo_sale_amt"]

    # ── COMBINE BUY + SELL ────────────────────────────────────────────────
    h = buy.merge(sell, on=grp_cols + ["broker"], how="outer").fillna(0)
    h["holding_qty"] = h["buy_qty"] - h["bulk_sale_qty"]
    h["avg_rate"]    = (h["buy_amt"] / h["buy_qty"]).where(h["buy_qty"] > 0, 0).round(2)
    h["broker"]      = h["broker"].astype(int)

    h = h.sort_values(
        ["Stock Symbol", "Date", "holding_qty"],
        ascending=[True, True, False]
    ).reset_index(drop=True)

    return h


def build_cumulative(h):
    """Cumulative net holding per broker per stock across all dates."""
    cumul = (
        h.groupby(["Stock Symbol", "broker"])
        .agg(
            total_buy_qty   =("buy_qty",        "sum"),
            total_sale_qty  =("total_sale_qty",  "sum"),
            total_ipo_qty   =("ipo_sale_qty",    "sum"),
            total_bulk_qty  =("bulk_sale_qty",   "sum"),
            net_holding     =("holding_qty",     "sum"),
            avg_rate        =("avg_rate",        "mean"),
        )
        .reset_index()
        .sort_values("net_holding", ascending=False)
    )
    return cumul


def build_json(h, cumul):
    """Build holdings_summary.json for the dashboard."""
    symbols = sorted(h["Stock Symbol"].dropna().unique().tolist())
    dates   = sorted(h["Date"].dropna().unique().tolist())

    # Top 10 brokers per stock (cumulative)
    top_brokers = {}
    for sym in symbols:
        top_brokers[sym] = (
            cumul[cumul["Stock Symbol"] == sym]
            .nlargest(10, "net_holding")
            [["broker", "net_holding", "total_buy_qty", "total_bulk_qty", "total_ipo_qty"]]
            .to_dict(orient="records")
        )

    # Daily holdings records (non-zero only)
    records = h[h["holding_qty"] != 0].copy()

    summary = {
        "generated_at"       : datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "total_records"      : len(records),
        "symbols"            : symbols,
        "dates"              : dates,
        "top_brokers"        : top_brokers,
        "holdings"           : records[[
            "Date", "Stock Symbol", "broker",
            "buy_qty", "total_sale_qty", "ipo_sale_qty",
            "bulk_sale_qty", "holding_qty", "avg_rate"
        ]].to_dict(orient="records"),
        "cumulative"         : cumul[[
            "Stock Symbol", "broker",
            "total_buy_qty", "total_sale_qty",
            "total_ipo_qty", "total_bulk_qty",
            "net_holding", "avg_rate"
        ]].to_dict(orient="records"),
    }
    return summary


def main():
    print("=" * 60)
    print("NEPSE Holdings Compiler")
    print("=" * 60)

    os.makedirs(DATA_DIR, exist_ok=True)

    df  = load_all_files()
    raw = extract_raw_transactions(df)

    print("\nComputing holdings...")
    h     = compute_holdings(raw)
    cumul = build_cumulative(h)

    non_zero = h[h["holding_qty"] != 0]
    print(f"Holdings computed : {len(h):,} rows ({len(non_zero):,} non-zero)")
    print(f"Unique stocks     : {h['Stock Symbol'].nunique()}")
    print(f"Unique brokers    : {h['broker'].nunique()}")
    print(f"Date range        : {h['Date'].min()} → {h['Date'].max()}")

    # Top 5 preview
    print("\nTop 5 brokers (cumulative net holding):")
    print(cumul.head(5)[["Stock Symbol","broker","net_holding","total_buy_qty","total_bulk_qty"]].to_string(index=False))

    # Save JSON
    print(f"\nSaving {OUTPUT_JSON}...")
    summary = build_json(h, cumul)
    with open(OUTPUT_JSON, "w") as f:
        json.dump(summary, f, indent=2, default=str)
    print(f"  Size: {os.path.getsize(OUTPUT_JSON)/1024:.1f} KB")

    # Save Excel
    print(f"Saving {OUTPUT_EXCEL}...")
    with pd.ExcelWriter(OUTPUT_EXCEL, engine="openpyxl") as writer:
        h.to_excel(writer, sheet_name="Daily Holdings", index=False)
        cumul.to_excel(writer, sheet_name="Cumulative by Broker", index=False)
        non_zero.to_excel(writer, sheet_name="Non-Zero Positions", index=False)

    print(f"  Rows: {len(h):,}")
    print(f"\n✅ Done! Open index.html in your browser to view the dashboard.")


if __name__ == "__main__":
    main()
