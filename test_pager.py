"""Test pagination WITH date filter persisted across page jumps."""
import httpx
from bs4 import BeautifulSoup
from merolagani.constants import BASE_URL, HEADERS, SEARCH_EVENT_TARGET, DATE_FIELD
from merolagani.parser import extract_form_state, parse_rows

PAGER_BUTTON = "ctl00$ContentPlaceHolder1$PagerControl1$btnPaging"
PAGER_FIELD  = "ctl00$ContentPlaceHolder1$PagerControl1$hdnCurrentPage"

TARGET_DATE = "2025/01/01"

with httpx.Client(headers=HEADERS, timeout=60, verify=True, http2=True) as c:
    # 1. Initial GET
    r = c.get(BASE_URL)
    soup = BeautifulSoup(r.text, "html.parser")
    state = extract_form_state(soup)

    # 2. Submit search with date filter
    form = dict(state)
    form["__EVENTTARGET"] = SEARCH_EVENT_TARGET
    form["__EVENTARGUMENT"] = ""
    form[DATE_FIELD] = TARGET_DATE
    r = c.post(BASE_URL, data=form, timeout=60)
    soup = BeautifulSoup(r.text, "html.parser")
    state = extract_form_state(soup)

    rows1 = parse_rows(soup, "2025-01-01")
    print(f"PAGE 1: {len(rows1)} rows. First txn: {rows1[0]['transaction_no'] if rows1 else 'none'}")

    # 3. Jump to page 2 — INCLUDE the date filter again
    form = dict(state)
    form["__EVENTTARGET"] = PAGER_BUTTON
    form["__EVENTARGUMENT"] = ""
    form[PAGER_FIELD] = "2"
    form[DATE_FIELD] = TARGET_DATE   # ← key change: keep the date filter
    r = c.post(BASE_URL, data=form, timeout=60)
    soup = BeautifulSoup(r.text, "html.parser")
    state = extract_form_state(soup)

    rows2 = parse_rows(soup, "2025-01-01")
    print(f"PAGE 2: {len(rows2)} rows. First txn: {rows2[0]['transaction_no'] if rows2 else 'none'}")

    # 4. Jump to page 50
    form = dict(state)
    form["__EVENTTARGET"] = PAGER_BUTTON
    form["__EVENTARGUMENT"] = ""
    form[PAGER_FIELD] = "50"
    form[DATE_FIELD] = TARGET_DATE
    r = c.post(BASE_URL, data=form, timeout=60)
    soup = BeautifulSoup(r.text, "html.parser")
    rows50 = parse_rows(soup, "2025-01-01")
    print(f"PAGE 50: {len(rows50)} rows. First txn: {rows50[0]['transaction_no'] if rows50 else 'none'}")

    # 5. Verify all pages have 2025-01-01 data
    p1_ok = rows1 and rows1[0]['transaction_no'].startswith('20250101')
    p2_ok = rows2 and rows2[0]['transaction_no'].startswith('20250101')
    p50_ok = rows50 and rows50[0]['transaction_no'].startswith('20250101')

    print()
    print(f"Page 1  has 2025-01-01 data: {p1_ok}")
    print(f"Page 2  has 2025-01-01 data: {p2_ok}")
    print(f"Page 50 has 2025-01-01 data: {p50_ok}")

    if p1_ok and p2_ok and p50_ok:
        print(">>> SUCCESS: Date filter persists across page jumps.")
    else:
        print(">>> FAIL: Date filter is being lost. Need different approach.")