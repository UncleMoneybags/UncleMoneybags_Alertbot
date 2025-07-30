import pandas as pd

# Load your trade log and alert log
trades = pd.read_csv('event_log_labeled.csv', parse_dates=['timestamp'])
alerts = pd.read_csv('label_trades_with_alerts.py', parse_dates=['timestamp'])

trades['is_runner'] = 0

for _, alert in alerts.iterrows():
    mask = (
        (trades['symbol'] == alert['symbol']) &
        (abs(trades['timestamp'] - alert['timestamp']) <= pd.Timedelta('3min'))
    )
    trades.loc[mask, 'is_runner'] = 1

trades.to_csv('trade_log_labeled.csv', index=False)
print("Done! Labeled file saved as trade_log_labeled.csv")