"""
Core scrape logic for Merolagani floorsheet — no CLI, just the machinery.
"""

import sqlite3
import time
from datetime import date, datetime, timedelta
from typing import Iterator, Optional

import httpx
from bs4 import BeautifulSoup
from tqdm import tqdm

from merolagani.constants import (
    BASE_URL, DATE_FIELD, DEFAULT_DELAY, HEADERS, NEXT_PAGE_EVENT_TARGET,
    PAGER_BUTTON_TARGET, PAGER_PAGE_FIELD, SEARCH_EVENT_TARGET
)
from merolagani.db import already_scraped, mark_scraped
from merolagani.parser import (
    extract_form_state, get_total_pages, get_total_records, parse_rows
)


def scrape_day(
    conn: sqlite3.Connection,
    target_date: date,
    delay: float = DEFAULT_DELAY,
    max_retries: int = 3,
    show_progress: bool = True,
    verify: bool = True,
) -> int:
    """Scrape a single day's full floorsheet into the DB. Returns row count.

    Idempotent: writes use INSERT OR REPLACE on transaction_no, so re-running
    is safe.

    Args:
        conn: SQLite connection
        target_date: date to scrape
        delay: seconds between page requests
        max_retries: retries per failed page
        show_progress: show tqdm progress bar
        verify: verify TLS certificates (default True; set False for MITM scenarios)
    """
    iso = target_date.strftime("%Y-%m-%d")
    site_format = target_date.strftime("%Y/%m/%d")  # what the page expects

    client = httpx.Client(
        headers=HEADERS,
        verify=verify,
        timeout=30.0,
        http2=True,
    )

    try:
        # 1. GET the page for an initial viewstate.
        resp = client.get(BASE_URL)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        state = extract_form_state(soup)

        # 2. POST the date filter.
        form: dict[str, str] = dict(state)
        form["__EVENTTARGET"] = SEARCH_EVENT_TARGET
        form["__EVENTARGUMENT"] = ""
        form[DATE_FIELD] = site_format
        resp = client.post(BASE_URL, data=form, timeout=60.0)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        state = extract_form_state(soup)

        total_pages = get_total_pages(soup)
        expected_rows = get_total_records(soup)
        if total_pages is None or expected_rows == 0:
            # No trading on this day (weekend / holiday / market closed).
            return 0

        pbar = tqdm(
            total=total_pages, desc=f"  {iso}", unit="pg", leave=False,
            disable=not show_progress,
        )

        all_rows: list[dict] = []
        page = 1
        while page <= total_pages:
            rows = parse_rows(soup, iso)
            all_rows.extend(rows)
            pbar.update(1)
            pbar.set_postfix(rows=len(all_rows))

            if page >= total_pages:
                break
            page += 1   # we're about to request the NEXT page; advance counter first
            form = dict(state)
            form["__EVENTTARGET"] = PAGER_BUTTON_TARGET
            form["__EVENTARGUMENT"] = ""
            form[PAGER_PAGE_FIELD] = str(page)
            form[DATE_FIELD] = site_format   # date filter must persist across page jumps

            time.sleep(delay)

            for attempt in range(max_retries):
                try:
                    resp = client.post(BASE_URL, data=form, timeout=60.0)
                    resp.raise_for_status()
                    break
                except httpx.RequestError as e:
                    if attempt == max_retries - 1:
                        pbar.close()
                        raise
                    wait = 2 ** attempt
                    tqdm.write(f"  [retry {attempt+1}] page {page+1} failed ({e}); sleeping {wait}s")
                    time.sleep(wait)

            soup = BeautifulSoup(resp.text, "html.parser")
            state = extract_form_state(soup)

        pbar.close()

        # Bulk insert.
        conn.executemany(
            """INSERT OR REPLACE INTO transactions
               (transaction_no, trade_date, symbol, company_name,
                buyer_broker_code, buyer_broker_name,
                seller_broker_code, seller_broker_name,
                quantity, rate, amount)
               VALUES
               (:transaction_no, :trade_date, :symbol, :company_name,
                :buyer_broker_code, :buyer_broker_name,
                :seller_broker_code, :seller_broker_name,
                :quantity, :rate, :amount)""",
            all_rows,
        )
        mark_scraped(conn, iso, len(all_rows))

        if expected_rows is not None and len(all_rows) != expected_rows:
            tqdm.write(
                f"  [warn] {iso}: parsed {len(all_rows)} rows, "
                f"page reported {expected_rows}"
            )
        return len(all_rows)

    finally:
        client.close()


def trading_days(start: date, end: date) -> Iterator[date]:
    """Yield weekdays (Sun–Thu in Nepal) from start to end inclusive.

    NEPSE is closed on Friday and Saturday. We can't easily filter out public
    holidays without a calendar — those will just come back as 0-row days,
    get marked scraped, and not be re-tried.
    """
    cur = start
    while cur <= end:
        # weekday(): Mon=0 ... Sun=6. NEPSE trades Sun-Thu => weekdays 6,0,1,2,3.
        if cur.weekday() not in (4, 5):  # skip Fri (4) and Sat (5)
            yield cur
        cur += timedelta(days=1)


def scrape_range(
    conn: sqlite3.Connection,
    start: date,
    end: date,
    delay: float = DEFAULT_DELAY,
    skip_existing: bool = True,
    verify: bool = True,
) -> None:
    """Scrape all trading days in a range, skipping already-scraped dates.

    Args:
        conn: SQLite connection
        start: start date (inclusive)
        end: end date (inclusive)
        delay: seconds between page requests
        skip_existing: skip dates already in scraped_days table
        verify: verify TLS certificates
    """
    days = list(trading_days(start, end))
    print(f"Range: {start} → {end} ({len(days)} candidate trading days)")

    todo = [d for d in days if not (skip_existing and already_scraped(conn, d.isoformat()))]
    skipped = len(days) - len(todo)
    print(f"Already scraped: {skipped}. To do: {len(todo)}")

    grand_total = 0
    outer = tqdm(todo, desc="Days", unit="day")
    for d in outer:
        try:
            n = scrape_day(conn, d, delay=delay, show_progress=True, verify=verify)
            grand_total += n
            outer.set_postfix(rows=grand_total, last=d.isoformat())
        except Exception as e:
            tqdm.write(f"[error] {d}: {e} — skipping (will retry on next run)")
            time.sleep(5)
    print(f"\nFinished. Inserted/updated ~{grand_total} rows total.")


def get_symbols_from_nepse() -> list[str]:
    """Pull the master symbol list from the local nepse package."""
    try:
        from nepse import Nepse
        nepse = Nepse()
        companies = nepse.getCompanyList()
        if companies is None:
            return []
        return [c["symbol"] for c in companies if c.get("symbol")]
    except Exception as e:
        tqdm.write(f"[warn] Could not load NEPSE symbols: {e}")
        return []
