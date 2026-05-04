"""
Helper script to export Merolagani floorsheet data to CSV.
Usage: python export_data.py --from 2025/01/01 --to 2026/01/28 --out data.csv
"""

import argparse
import csv
import sqlite3
from datetime import datetime
from pathlib import Path

def parse_date(s: str) -> str:
    """Parse date string and return ISO format."""
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"Invalid date: {s}")

def export_csv(db_path: str, out_path: str, from_date: str = None, to_date: str = None, symbol: str = None):
    """Export transactions to CSV file.

    Streams rows directly from the cursor to disk so memory usage stays
    constant regardless of result-set size.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Build query
    where = []
    params = []

    if from_date:
        where.append("trade_date >= ?")
        params.append(parse_date(from_date))

    if to_date:
        where.append("trade_date <= ?")
        params.append(parse_date(to_date))

    if symbol:
        where.append("symbol = ?")
        params.append(symbol.upper())

    sql = "SELECT * FROM transactions"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY trade_date DESC, transaction_no"

    # Execute query — do NOT call fetchall() here; we stream below.
    cur = conn.execute(sql, params)
    cols = [desc[0] for desc in cur.description]

    # Prepare output path
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    # Stream rows directly from cursor to CSV. Constant memory regardless of
    # how many rows the query returns.
    n = 0
    with out.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=cols)
        writer.writeheader()
        for row in cur:
            writer.writerow(dict(row))
            n += 1

    if n == 0:
        # File was created with just a header — that's fine, but let the
        # user know nothing matched.
        print("No data found for the specified criteria.")
        conn.close()
        return 0

    print(f"[OK] Exported {n:,} rows to {out}")
    print(f"     Columns: {', '.join(cols)}")

    conn.close()
    return n

def show_available_dates(db_path: str):
    """Show what date range is available in the database."""
    conn = sqlite3.connect(db_path)

    result = conn.execute("""
        SELECT MIN(trade_date), MAX(trade_date), COUNT(DISTINCT trade_date) as days, COUNT(*) as rows
        FROM transactions
    """).fetchone()

    if result[0] is None:
        print("Database is empty.")
    else:
        print(f"Available data:")
        print(f"  Date range:  {result[0]} to {result[1]}")
        print(f"  Days:        {result[2]}")
        print(f"  Total rows:  {result[3]:,}")

        # Show symbols
        symbols = conn.execute("SELECT COUNT(DISTINCT symbol) FROM transactions").fetchone()[0]
        print(f"  Symbols:     {symbols}")

    conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export Merolagani floorsheet data to CSV")
    parser.add_argument("--db", default="floorsheet.db", help="Database path")
    parser.add_argument("--from", dest="from_date", help="Start date (YYYY/MM/DD)")
    parser.add_argument("--to", dest="to_date", help="End date (YYYY/MM/DD)")
    parser.add_argument("--symbol", help="Filter by symbol")
    parser.add_argument("--out", help="Output CSV file")
    parser.add_argument("--show", action="store_true", help="Show available data and exit")

    args = parser.parse_args()

    if args.show:
        show_available_dates(args.db)
    elif args.out:
        export_csv(args.db, args.out, args.from_date, args.to_date, args.symbol)
    else:
        print("Usage: python export_data.py --from 2025/01/01 --to 2026/01/28 --out data.csv")
        print("       python export_data.py --show")