import pandas as pd

# Load your event log
df = pd.read_csv("event_log_labeled.csv")

# Reset is_runner labels
df['is_runner'] = 0

# For each symbol, process in order to label runners:
for symbol in df['symbol'].unique():
    symbol_df = df[df['symbol'] == symbol].copy()
    prev_price = None
    prev_size = None
    for idx in symbol_df.index:
        price = df.loc[idx, 'price']
        size = df.loc[idx, 'size']
        if prev_price is not None and prev_size is not None:
            # Calculate percent change from previous price for this symbol
            price_pct_up = (price - prev_price) / prev_price if prev_price > 0 else 0
            size_increasing = size >= prev_size
            if price_pct_up >= 0.20 and size_increasing:
                df.loc[idx, 'is_runner'] = 1
        prev_price = price
        prev_size = size

# Save the labeled file
df.to_csv("event_log_labeled.csv", index=False)
print("Labeling complete. Check event_log_labeled.csv.")
