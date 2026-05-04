"""
SQLite database schema, initialization, and checkpoint utilities.
"""

import sqlite3
from datetime import datetime, timezone

SCHEMA = """
CREATE TABLE IF NOT EXISTS transactions (
    transaction_no       TEXT PRIMARY KEY,
    trade_date           TEXT NOT NULL,           -- YYYY-MM-DD
    symbol               TEXT NOT NULL,
    company_name         TEXT,
    buyer_broker_code    TEXT,
    buyer_broker_name    TEXT,
    seller_broker_code   TEXT,
    seller_broker_name   TEXT,
    quantity             REAL,
    rate                 REAL,
    amount               REAL
);

CREATE INDEX IF NOT EXISTS idx_tx_date         ON transactions(trade_date);
CREATE INDEX IF NOT EXISTS idx_tx_symbol       ON transactions(symbol);
CREATE INDEX IF NOT EXISTS idx_tx_date_symbol  ON transactions(trade_date, symbol);
CREATE INDEX IF NOT EXISTS idx_tx_buyer        ON transactions(buyer_broker_code);
CREATE INDEX IF NOT EXISTS idx_tx_seller       ON transactions(seller_broker_code);

-- One row per (date) we've fully ingested. Lets us resume safely.
CREATE TABLE IF NOT EXISTS scraped_days (
    trade_date    TEXT PRIMARY KEY,    -- YYYY-MM-DD
    row_count     INTEGER NOT NULL,
    scraped_at    TEXT NOT NULL
);
"""


def open_db(path: str) -> sqlite3.Connection:
    """Open or create the floorsheet database with optimal pragmas."""
    conn = sqlite3.connect(path)
    conn.executescript(SCHEMA)
    # Big inserts go faster with these. Safe enough for a scraper — at
    # worst you re-scrape one day if the OS dies mid-write.
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous  = NORMAL;")
    return conn


def already_scraped(conn: sqlite3.Connection, iso_date: str) -> bool:
    """Check if a date has already been fully scraped."""
    cur = conn.execute(
        "SELECT 1 FROM scraped_days WHERE trade_date = ?", (iso_date,)
    )
    return cur.fetchone() is not None


def mark_scraped(conn: sqlite3.Connection, iso_date: str, row_count: int) -> None:
    """Mark a date as fully scraped with its row count."""
    conn.execute(
        "INSERT OR REPLACE INTO scraped_days (trade_date, row_count, scraped_at) "
        "VALUES (?, ?, ?)",
        (iso_date, row_count, datetime.now(timezone.utc).isoformat(timespec="seconds")),
    )
    conn.commit()
