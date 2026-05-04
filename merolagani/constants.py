"""
Merolagani scraper constants and configuration.
"""

BASE_URL = "https://merolagani.com/Floorsheet.aspx"

# ASP.NET event targets — these IDs come from the rendered page. If Merolagani
# rebuilds the page and these stop working, open DevTools, submit the form,
# inspect the POST body, and update.
NEXT_PAGE_EVENT_TARGET = "ctl00$ContentPlaceHolder1$lbtnNextPage"
PAGER_BUTTON_TARGET = "ctl00$ContentPlaceHolder1$PagerControl1$btnPaging"
PAGER_PAGE_FIELD = "ctl00$ContentPlaceHolder1$PagerControl1$hdnCurrentPage"
SEARCH_EVENT_TARGET = "ctl00$ContentPlaceHolder1$lbtnSearchFloorsheet"

# Form field names for the date and symbol filters. These are best-effort
# guesses confirmed by inspecting the search form. If filtering doesn't
# work, override via DevTools.
DATE_FIELD = "ctl00$ContentPlaceHolder1$txtFloorsheetDateFilter"
SYMBOL_FIELD = "ctl00$ContentPlaceHolder1$ASCompanyFilter$txtAutoSuggest"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://merolagani.com",
    "Referer": BASE_URL,
}

DEFAULT_DELAY = 0.5     # seconds between page requests
DEFAULT_DB = "floorsheet.db"
