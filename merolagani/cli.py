"""
CLI interface and subcommand dispatch for the Merolagani scraper.
"""

import argparse
import csv
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

from merolagani.constants import DEFAULT_DB, DEFAULT_DELAY
from merolagani.db import open_db
from merolagani.scraper import (
    get_symbols_from_nepse, scrape_day, scrape_range
)


def parse_user_date(s: str) -> date:
    """Accept 2024/05/15, 2024-05-15, or 2024.05.15."""
    for fmt in ("%Y/%m/%d", "%Y-%m-%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    raise argparse.ArgumentTypeError(f"Bad date {s!r}; use YYYY/MM/DD.")


def cmd_one_day(args) -> None:
    """Scrape a single trading day."""
    conn = open_db(args.db)
    d = parse_user_date(args.date)
    from merolagani.db import already_scraped
    if already_scraped(conn, d.isoformat()) and not args.force:
        print(f"{d} already scraped (use --force to re-scrape).")
        return
    n = scrape_day(conn, d, delay=args.delay, verify=args.verify)
    print(f"{d}: {n} rows.")


def cmd_historical(args) -> None:
    """Scrape a date range."""
    conn = open_db(args.db)
    start = parse_user_date(args.from_date)
    end = parse_user_date(args.to_date)
    scrape_range(conn, start, end, delay=args.delay, skip_existing=not args.force, verify=args.verify)


def cmd_update(args) -> None:
    """Scrape from the most recent date in the DB to today."""
    conn = open_db(args.db)
    row = conn.execute("SELECT MAX(trade_date) FROM scraped_days").fetchone()
    last = row[0]
    if last is None:
        print("DB is empty. Run `historical` first or pick a start date.")
        return
    start = datetime.strptime(last, "%Y-%m-%d").date() + timedelta(days=1)
    end = date.today()
    if start > end:
        print(f"Already up to date (last scraped {last}).")
        return
    print(f"Updating from {start} to {end}.")
    scrape_range(conn, start, end, delay=args.delay, skip_existing=True, verify=args.verify)


def cmd_export(args) -> None:
    """Export filtered transactions to CSV."""
    conn = open_db(args.db)
    where = []
    params: list = []
    if args.symbol:
        where.append("symbol = ?")
        params.append(args.symbol.upper())
    if args.from_date:
        where.append("trade_date >= ?")
        params.append(parse_user_date(args.from_date).isoformat())
    if args.to_date:
        where.append("trade_date <= ?")
        params.append(parse_user_date(args.to_date).isoformat())
    sql = "SELECT * FROM transactions"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY trade_date, transaction_no"

    cur = conn.execute(sql, params)
    cols = [d[0] for d in cur.description]
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with out.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for row in cur:
            w.writerow(row)
            n += 1
    print(f"Exported {n} rows → {out}")


def cmd_stats(args) -> None:
    """Print database statistics."""
    conn = open_db(args.db)
    total_rows = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    days = conn.execute(
        "SELECT MIN(trade_date), MAX(trade_date), COUNT(*) FROM scraped_days"
    ).fetchone()
    symbols = conn.execute("SELECT COUNT(DISTINCT symbol) FROM transactions").fetchone()[0]
    print(f"Database:        {args.db}")
    print(f"Total rows:      {total_rows:,}")
    print(f"Distinct symbols:{symbols:,}")
    if days[2] > 0:
        print(f"Days scraped:    {days[2]} ({days[0]} → {days[1]})")
    else:
        print(f"Days scraped:    0")


def main() -> None:
    """Main entry point for the CLI."""
    p = argparse.ArgumentParser(description="Merolagani floorsheet historical scraper.")
    p.add_argument(
        "--db", default=DEFAULT_DB,
        help=f"SQLite path (default: {DEFAULT_DB})"
    )
    p.add_argument(
        "--no-verify", action="store_false", dest="verify",
        help="Disable TLS certificate verification (default: enabled)"
    )
    sub = p.add_subparsers(dest="cmd", required=True)

    p1 = sub.add_parser("one-day", help="Scrape a single date.")
    p1.add_argument("--date", required=True, help="YYYY/MM/DD")
    p1.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    p1.add_argument("--force", action="store_true", help="Re-scrape even if already in DB.")
    p1.set_defaults(func=cmd_one_day)

    p2 = sub.add_parser("historical", help="Scrape a date range.")
    p2.add_argument("--from", dest="from_date", required=True, help="YYYY/MM/DD")
    p2.add_argument("--to", dest="to_date", required=True, help="YYYY/MM/DD")
    p2.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    p2.add_argument(
        "--force", action="store_true",
        help="Re-scrape days already in DB."
    )
    p2.set_defaults(func=cmd_historical)

    p3 = sub.add_parser("update", help="Scrape from latest stored date through today.")
    p3.add_argument("--delay", type=float, default=DEFAULT_DELAY)
    p3.set_defaults(func=cmd_update)

    p4 = sub.add_parser("export", help="Export filtered rows to CSV.")
    p4.add_argument("--out", required=True, help="Output CSV path")
    p4.add_argument("--symbol", default=None, help="Filter by symbol")
    p4.add_argument("--from", dest="from_date", default=None, help="Start date (YYYY/MM/DD)")
    p4.add_argument("--to", dest="to_date", default=None, help="End date (YYYY/MM/DD)")
    p4.set_defaults(func=cmd_export)

    p5 = sub.add_parser("stats", help="Show DB stats.")
    p5.set_defaults(func=cmd_stats)

    args = p.parse_args()
    try:
        args.func(args)
    except KeyboardInterrupt:
        print("\nInterrupted. Re-run the same command to resume.", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
