## Scraper Integration Summary

### Original scraper.py → merolagani/ package structure

The monolithic `scraper.py` has been split into the following modules:

#### **merolagani/constants.py**
- `BASE_URL` 
- `NEXT_PAGE_EVENT_TARGET`, `SEARCH_EVENT_TARGET`
- `DATE_FIELD`, `SYMBOL_FIELD`
- `HEADERS`
- `DEFAULT_DELAY`, `DEFAULT_DB`

#### **merolagani/db.py**
- `SCHEMA` (database schema definition)
- `open_db(path)` — Opens/creates SQLite with WAL mode and PRAGMA settings
- `already_scraped(conn, iso_date)` — Checkpoint query
- `mark_scraped(conn, iso_date, row_count)` — Checkpoint recording

#### **merolagani/parser.py**
- `extract_form_state(soup)` — Extract all hidden ASP.NET fields
- `get_total_pages(soup)` — Parse page count
- `get_total_records(soup)` — Parse total record count
- `parse_rows(soup, trade_date_iso)` — Parse transaction rows into dicts

#### **merolagani/scraper.py**
- `scrape_day(conn, target_date, ...)` — Core single-day scrape (now uses httpx instead of requests)
- `trading_days(start, end)` — Iterator for weekday filtering
- `scrape_range(conn, start, end, ...)` — Multi-day orchestration
- `get_symbols_from_nepse()` — Helper to pull symbol list from nepse package

#### **merolagani/cli.py**
- `parse_user_date(s)` — Date format parser
- `cmd_one_day(args)`, `cmd_historical(args)`, `cmd_update(args)`, `cmd_export(args)`, `cmd_stats(args)`
- `main()` — argparse subcommand dispatch

#### **merolagani/__init__.py**
- Public API exports: `open_db`, `scrape_day`, `scrape_range`, `get_symbols_from_nepse`, etc.

### Key changes from original:

1. **requests → httpx**: Migrated to `httpx.Client(http2=True)` with `verify` parameter
2. **TLS verification**: Added `--no-verify` CLI flag; defaults to ON (Merolagani cert is valid)
3. **Console entry point**: `pyproject.toml` now has `merolagani-scrape = "merolagani.cli:main"`
4. **New dependency**: Added `beautifulsoup4` to pyproject.toml and requirements.txt

### File structure:
```
merolagani/
├── __init__.py          # Public API
├── constants.py         # URLs, field names, headers, defaults
├── db.py               # SQLite schema & helpers
├── parser.py           # HTML parsing functions
├── scraper.py          # Core scrape logic
└── cli.py              # CLI interface

tests/
├── test_merolagani/
│   ├── __init__.py
│   ├── test_parser.py      # Parser + fixture tests
│   ├── test_db.py          # DB schema & idempotency tests
│   └── test_cli.py         # CLI argument parsing tests
└── fixtures/
    └── floorsheet_sample.html  # Real sample response

```

### To verify:
```bash
pip install -e .
merolagani-scrape stats                           # Should print "Total rows: 0"
merolagani-scrape one-day --date 2024/05/15      # Quick smoke test (no network)
pytest tests/test_merolagani/                    # All tests pass
```
