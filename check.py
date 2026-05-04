import sqlite3
c = sqlite3.connect('floorsheet.db')
r = c.execute("SELECT trade_date, COUNT(*), COUNT(DISTINCT transaction_no), COUNT(DISTINCT symbol) FROM transactions WHERE trade_date='2025-01-01' GROUP BY trade_date").fetchone()
print(f"Date: {r[0]} | Total rows: {r[1]} | Unique txns: {r[2]} | Distinct symbols: {r[3]}")