"""Tests for merolagani.parser module."""

import pytest
from bs4 import BeautifulSoup

from merolagani.parser import (
    extract_form_state, get_total_pages, get_total_records, parse_rows
)


@pytest.fixture
def sample_html():
    """Load the sample Merolagani floorsheet HTML."""
    from pathlib import Path
    fixture_path = Path(__file__).parent.parent / "fixtures" / "floorsheet_sample.html"
    return fixture_path.read_text(encoding="utf-8")


def test_extract_form_state(sample_html):
    """Test extraction of ASP.NET hidden state fields."""
    soup = BeautifulSoup(sample_html, "html.parser")
    state = extract_form_state(soup)
    
    # Should extract all five hidden fields
    assert "__VIEWSTATE" in state
    assert "__VIEWSTATEGENERATOR" in state
    assert "__EVENTVALIDATION" in state
    assert state["__VIEWSTATE"] == "/wEPDwULLTEwODI0OTU0MTAPZBYCZg9kFgICAQ9kFgQCAQ8WAh4HVmlzaWJsZWhkAgMPZBYCAgEPZBYGAgEPZBYIAgEPFgIeB0NvbnRyb2xJZAUDcGFnZWQCBQ8WAh8AhQECAQPD"


def test_get_total_pages(sample_html):
    """Test parsing of total pages."""
    soup = BeautifulSoup(sample_html, "html.parser")
    pages = get_total_pages(soup)
    assert pages == 2


def test_get_total_records(sample_html):
    """Test parsing of total record count."""
    soup = BeautifulSoup(sample_html, "html.parser")
    records = get_total_records(soup)
    assert records == 20


def test_parse_rows(sample_html):
    """Test extraction of transaction rows."""
    soup = BeautifulSoup(sample_html, "html.parser")
    rows = parse_rows(soup, "2024/05/15")
    
    # Should extract 5 rows from the sample
    assert len(rows) == 5
    
    # Check first row fields
    row = rows[0]
    assert row["transaction_no"] == "TXN001"
    assert row["trade_date"] == "2024/05/15"
    assert row["symbol"] == "NABIL"
    assert row["company_name"] == "Nepal SBI Bank Limited"
    assert row["buyer_broker_code"] == "11101"
    assert row["buyer_broker_name"] == "Meroshare Limited"
    assert row["seller_broker_code"] == "11300"
    assert row["seller_broker_name"] == "Prabhu Securities"
    assert row["quantity"] == 100.0
    assert row["rate"] == 3200.0
    assert row["amount"] == 320000.0
    
    # Check another row
    row3 = rows[2]
    assert row3["symbol"] == "NTC"
    assert row3["quantity"] == 500.0
    assert row3["amount"] == 75000.0


def test_parse_rows_no_table():
    """Test parse_rows with HTML that has no floorsheet table."""
    html = "<html><body><p>No table here</p></body></html>"
    soup = BeautifulSoup(html, "html.parser")
    rows = parse_rows(soup, "2024/05/15")
    assert rows == []


def test_extract_form_state_missing_viewstate():
    """Test that extract_form_state raises if __VIEWSTATE is missing."""
    html = '<input type="hidden" name="__VIEWSTATEGENERATOR" value="test" />'
    soup = BeautifulSoup(html, "html.parser")
    with pytest.raises(RuntimeError, match="No __VIEWSTATE"):
        extract_form_state(soup)
