import pandas as pd

# Load event log (ensure sorted by symbol, then by time if available)
df = pd.read_csv("event_log_labeled.csv")

# Parameters
PCT_THRESHOLD = 0.20  # 20% gain
LOOKAHEAD_ROWS = 10   # How many rows to look ahead for a "runner"

# Reset is_runner if you want to overwrite previous labels
df['is_runner'] = 0

# For each event, look ahead for a runner within LOOKAHEAD_ROWS for the same symbol
for idx, row in df.iterrows():
    symbol = row['symbol']
    price = row['price']

    # Find all future rows for the same symbol within the lookahead window
    future_rows = df[(df.index > idx) & (df.index <= idx + LOOKAHEAD_ROWS) & (df['symbol'] == symbol)]
    if not future_rows.empty:
        max_future_price = future_rows['price'].max()
        if max_future_price >= price * (1 + PCT_THRESHOLD):
            df.at[idx, 'is_runner'] = 1

df.to_csv("event_log_labeled.csv", index=False)
print("Labeling complete. Check event_log_labeled.csv.")
