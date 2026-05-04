"""Tests for merolagani.cli module."""

import pytest
from unittest.mock import patch, MagicMock
from datetime import date
from argparse import Namespace

from merolagani.cli import parse_user_date, cmd_stats


def test_parse_user_date_slash_format():
    """Test parsing YYYY/MM/DD format."""
    d = parse_user_date("2024/05/15")
    assert d == date(2024, 5, 15)


def test_parse_user_date_dash_format():
    """Test parsing YYYY-MM-DD format."""
    d = parse_user_date("2024-05-15")
    assert d == date(2024, 5, 15)


def test_parse_user_date_dot_format():
    """Test parsing YYYY.MM.DD format."""
    d = parse_user_date("2024.05.15")
    assert d == date(2024, 5, 15)


def test_parse_user_date_invalid():
    """Test that invalid dates raise ArgumentTypeError."""
    import argparse
    with pytest.raises(argparse.ArgumentTypeError):
        parse_user_date("invalid-date")


@patch("merolagani.cli.open_db")
def test_cmd_stats(mock_open_db, capsys):
    """Test stats command output."""
    mock_conn = MagicMock()
    mock_open_db.return_value = mock_conn
    
    # Mock database queries
    mock_conn.execute.side_effect = [
        MagicMock(fetchone=MagicMock(return_value=(1000,))),  # total rows
        MagicMock(fetchone=MagicMock(return_value=("2024-01-01", "2024-05-15", 100))),  # days stats
        MagicMock(fetchone=MagicMock(return_value=(50,))),  # distinct symbols
    ]
    
    args = Namespace(db="test.db")
    cmd_stats(args)
    
    captured = capsys.readouterr()
    assert "Database:        test.db" in captured.out
    assert "Total rows:      1,000" in captured.out
    assert "Distinct symbols:50" in captured.out
    assert "Days scraped:    100 (2024-01-01 → 2024-05-15)" in captured.out
