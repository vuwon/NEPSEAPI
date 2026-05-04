"""
HTML parsing helpers for Merolagani floorsheet pages.
"""

import re
from typing import Optional

from bs4 import BeautifulSoup


def extract_form_state(soup: BeautifulSoup) -> dict[str, str]:
    """Grab every hidden ASP.NET state field. These must be echoed on each POST."""
    state: dict[str, str] = {}
    for fname in (
        "__VIEWSTATE", "__VIEWSTATEGENERATOR", "__EVENTVALIDATION",
        "__VIEWSTATEENCRYPTED", "__PREVIOUSPAGE",
    ):
        tag = soup.find("input", {"name": fname})
        if tag is not None and tag.get("value") is not None:
            state[fname] = tag["value"]
    if "__VIEWSTATE" not in state:
        raise RuntimeError(
            "No __VIEWSTATE on page — layout may have changed or request was blocked."
        )
    return state


def get_total_pages(soup: BeautifulSoup) -> Optional[int]:
    """Parse 'Total pages: N' from the page text."""
    m = re.search(r"Total pages:\s*(\d+)", soup.get_text(" ", strip=True))
    return int(m.group(1)) if m else None


def get_total_records(soup: BeautifulSoup) -> Optional[int]:
    """Read 'Showing 1 - 500 of NNNNN records' so we can verify after the fact."""
    m = re.search(
        r"of\s+([\d,]+)\s+records", soup.get_text(" ", strip=True), re.IGNORECASE
    )
    return int(m.group(1).replace(",", "")) if m else None


def parse_rows(soup: BeautifulSoup, trade_date_iso: str) -> list[dict]:
    """Pull every transaction row out of the floorsheet table."""
    target = None
    for tbl in soup.find_all("table"):
        hdrs = {th.get_text(strip=True).lower() for th in tbl.find_all("th")}
        if {"symbol", "buyer", "seller", "quantity", "rate", "amount"} <= hdrs:
            target = tbl
            break
    if target is None:
        return []

    rows: list[dict] = []
    for tr in target.find_all("tr"):
        tds = tr.find_all("td")
        if len(tds) < 8:
            continue  # header row or malformed
        _sn, txn_no, sym_c, buy_c, sel_c, qty_c, rate_c, amt_c = tds[:8]

        sym_a = sym_c.find("a")
        buy_a = buy_c.find("a")
        sel_a = sel_c.find("a")

        def num(s: str) -> Optional[float]:
            s = s.replace(",", "").strip()
            if not s:
                return None
            try:
                return float(s)
            except ValueError:
                return None

        rows.append({
            "transaction_no": txn_no.get_text(strip=True),
            "trade_date": trade_date_iso,
            "symbol": (sym_a.get_text(strip=True) if sym_a else sym_c.get_text(strip=True)),
            "company_name": (sym_a.get("title", "").strip() if sym_a else ""),
            "buyer_broker_code": (buy_a.get_text(strip=True) if buy_a else buy_c.get_text(strip=True)),
            "buyer_broker_name": (buy_a.get("title", "").strip() if buy_a else ""),
            "seller_broker_code": (sel_a.get_text(strip=True) if sel_a else sel_c.get_text(strip=True)),
            "seller_broker_name": (sel_a.get("title", "").strip() if sel_a else ""),
            "quantity": num(qty_c.get_text()),
            "rate": num(rate_c.get_text()),
            "amount": num(amt_c.get_text()),
        })
    return rows
