"""Tests for merolagani.db module."""

import pytest
import sqlite3

from merolagani.db import open_db, already_scraped, mark_scraped


@pytest.fixture
def in_memory_db():
    """Create an in-memory SQLite database for testing."""
    conn = open_db(":memory:")
    yield conn
    conn.close()


def test_open_db_creates_schema(in_memory_db):
    """Test that open_db creates the required schema."""
    # Check that both tables exist
    cur = in_memory_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='transactions'"
    )
    assert cur.fetchone() is not None
    
    cur = in_memory_db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='scraped_days'"
    )
    assert cur.fetchone() is not None


def test_already_scraped_false(in_memory_db):
    """Test already_scraped returns False for unscraped date."""
    result = already_scraped(in_memory_db, "2024-05-15")
    assert result is False


def test_mark_and_already_scraped(in_memory_db):
    """Test mark_scraped and already_scraped round-trip."""
    iso_date = "2024-05-15"
    
    # Initially not scraped
    assert already_scraped(in_memory_db, iso_date) is False
    
    # Mark as scraped
    mark_scraped(in_memory_db, iso_date, 42)
    
    # Now it should be scraped
    assert already_scraped(in_memory_db, iso_date) is True
    
    # Check the recorded row count
    cur = in_memory_db.execute(
        "SELECT row_count FROM scraped_days WHERE trade_date = ?",
        (iso_date,)
    )
    row = cur.fetchone()
    assert row is not None
    assert row[0] == 42


def test_mark_scraped_replace(in_memory_db):
    """Test INSERT OR REPLACE behavior of mark_scraped."""
    iso_date = "2024-05-15"
    
    # Mark with one count
    mark_scraped(in_memory_db, iso_date, 10)
    cur = in_memory_db.execute(
        "SELECT COUNT(*) FROM scraped_days WHERE trade_date = ?",
        (iso_date,)
    )
    assert cur.fetchone()[0] == 1
    
    # Mark again with different count (should replace, not insert)
    mark_scraped(in_memory_db, iso_date, 20)
    cur = in_memory_db.execute(
        "SELECT COUNT(*) FROM scraped_days WHERE trade_date = ?",
        (iso_date,)
    )
    assert cur.fetchone()[0] == 1
    
    # Check new row count
    cur = in_memory_db.execute(
        "SELECT row_count FROM scraped_days WHERE trade_date = ?",
        (iso_date,)
    )
    assert cur.fetchone()[0] == 20


def test_transaction_table_insert_or_replace(in_memory_db):
    """Test INSERT OR REPLACE on transaction_no (PK)."""
    # Insert a transaction
    in_memory_db.execute(
        """INSERT INTO transactions
           (transaction_no, trade_date, symbol, quantity, rate, amount)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("TXN001", "2024-05-15", "NABIL", 100, 3200.0, 320000.0)
    )
    in_memory_db.commit()
    
    # Verify it's there
    cur = in_memory_db.execute("SELECT COUNT(*) FROM transactions")
    assert cur.fetchone()[0] == 1
    
    # Replace it with different data
    in_memory_db.execute(
        """INSERT OR REPLACE INTO transactions
           (transaction_no, trade_date, symbol, quantity, rate, amount)
           VALUES (?, ?, ?, ?, ?, ?)""",
        ("TXN001", "2024-05-15", "NTC", 50, 150.0, 7500.0)
    )
    in_memory_db.commit()
    
    # Should still be 1 row
    cur = in_memory_db.execute("SELECT COUNT(*) FROM transactions")
    assert cur.fetchone()[0] == 1
    
    # Check updated data
    cur = in_memory_db.execute("SELECT symbol, quantity FROM transactions WHERE transaction_no = ?", ("TXN001",))
    row = cur.fetchone()
    assert row[0] == "NTC"
    assert row[1] == 50
