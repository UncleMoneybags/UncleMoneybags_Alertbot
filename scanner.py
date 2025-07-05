import requests
import time
import pytz
from datetime import datetime

# --- CONFIG ---
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
TELEGRAM_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"

VOLUME_SPIKE_THRESHOLD = 2.0  # 2x last 10min avg
PRICE_CUTOFF = 5.00
CHECK_INTERVAL = 60  # seconds
PRICE_SPIKE_AMOUNT = 0.10

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        print(f"Telegram send error: {e}")

def is_scan_window():
    eastern = pytz.timezone("US/Eastern")
    now_est = datetime.now(eastern)
    weekday = now_est.weekday()  # Monday=0, Sunday=6
    hour = now_est.hour
    minute = now_est.minute

    # Only Monday-Friday (0-4)
    if 0 <= weekday <= 4:
        # 4:00am to 8:00pm inclusive
        if (hour > 4 and hour < 20):
            return True
        if hour == 4 and minute >= 0:
            return True
        if hour == 20 and minute == 0:
            return True
    return False

def get_all_us_tickers():
    tickers = []
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit=1000&apiKey={POLYGON_API_KEY}"
    while url:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            page = data.get("results", [])
            tickers += [item["ticker"] for item in page if not item["ticker"].endswith("W")]  # skip warrants
            url = data.get("next_url")
            if url and "apiKey" not in url:
                url += f"&apiKey={POLYGON_API_KEY}"
        else:
            print(f"Ticker list fetch error: {resp.status_code}")
            break
    return tickers

def fetch_minute_candles(symbol, lookback=11):
    today = datetime.now().strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{today}/{today}?adjusted=true&sort=desc&limit={lookback}&apiKey={POLYGON_API_KEY}"
    resp = requests.get(url, timeout=10)
    if resp.status_code == 200:
        data = resp.json()
        if data.get("results"):
            return data["results"]
    return []

class PennySpikeBot:
    def __init__(self):
        self.tickers = get_all_us_tickers()
        self.last_vol_alert = {}

    def scan_volume_spikes(self):
        for symbol in self.tickers:
            try:
                candles = fetch_minute_candles(symbol, lookback=11)
                if len(candles) < 11:
                    continue
                curr = candles[0]
                prev = candles[1]
                curr_price = curr['c']
                prev_price = prev['c']
                if curr_price is None or prev_price is None or curr_price > PRICE_CUTOFF or curr_price == 0:
                    continue
                # Require a price spike up (not down) AND at least $0.10 higher than previous
                price_diff = curr_price - prev_price
                if price_diff < PRICE_SPIKE_AMOUNT:
                    continue
                curr_vol = curr['v']
                avg_vol = sum(c['v'] for c in candles[1:]) / 10
                if avg_vol == 0:
                    continue
                rel_vol = curr_vol / avg_vol
                last_ts = self.last_vol_alert.get(symbol, 0)
                if rel_vol > VOLUME_SPIKE_THRESHOLD and curr['t'] != last_ts and curr_vol > 500:
                    msg = (
                        f"💥 <b>{symbol}</b> penny stock spike!\n"
                        f"Price ${curr_price:.2f} (+${price_diff:.2f}) Vol {curr_vol} ({rel_vol:.1f}x rel vol)"
                    )
                    send_telegram(msg)
                    self.last_vol_alert[symbol] = curr['t']
            except Exception as e:
                print(f"vol+price spike err {symbol}: {e}")

    def run(self):
        while True:
            if is_scan_window():
                self.scan_volume_spikes()
            else:
                print("Outside scan window, waiting...")
            time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    print("Starting Real-Time Penny Stock Spike Bot!")
    bot = PennySpikeBot()
    bot.run()
