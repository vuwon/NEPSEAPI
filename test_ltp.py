import pandas as pd
import glob

files = sorted(glob.glob('data/*.xlsx'))
if not files:
    print("No Excel files found in data/")
    exit()

# Use the most recent file
f = files[-1]
print(f"File: {f}\n")

df = pd.read_excel(f, sheet_name=0, engine='openpyxl')
df.columns = [str(c).strip().replace('\xa0','') for c in df.columns]
df['contractId'] = pd.to_numeric(df['contractId'], errors='coerce')
df['contractRate'] = pd.to_numeric(df['contractRate'], errors='coerce')

# Pick one symbol that has many transactions
sym_counts = df.groupby('stockSymbol').size().sort_values(ascending=False)
test_sym = sym_counts.index[0]
print(f"Testing symbol: {test_sym} ({sym_counts[test_sym]} transactions)\n")

s = df[df['stockSymbol'] == test_sym].copy()

print("contractId range:")
print(f"  Min: {s['contractId'].min()}")
print(f"  Max: {s['contractId'].max()}")

print("\nFirst 5 rows (sorted by contractId asc):")
print(s.sort_values('contractId')[['contractId','contractRate','tradeTime' if 'tradeTime' in s.columns else 'contractId']].head(5).to_string(index=False))

print("\nLast 5 rows (sorted by contractId asc):")
print(s.sort_values('contractId')[['contractId','contractRate','tradeTime' if 'tradeTime' in s.columns else 'contractId']].tail(5).to_string(index=False))

print("\nLast 5 rows (sorted by tradeTime asc):")
if 'tradeTime' in s.columns:
    s['tradeTime'] = pd.to_datetime(s['tradeTime'], errors='coerce')
    print(s.sort_values('tradeTime')[['contractId','contractRate','tradeTime']].tail(5).to_string(index=False))

print(f"\nMax contractRate: {s['contractRate'].max()}")
print(f"Rate at max contractId: {s.loc[s['contractId'].idxmax(), 'contractRate']}")
if 'tradeTime' in s.columns:
    s['tradeTime'] = pd.to_datetime(s['tradeTime'], errors='coerce')
    print(f"Rate at max tradeTime:  {s.loc[s['tradeTime'].idxmax(), 'contractRate']}")
    print(f"Max tradeTime: {s['tradeTime'].max()}")
