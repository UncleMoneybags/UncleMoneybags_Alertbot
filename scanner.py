import requests
import time
from datetime import datetime, timedelta
import os

# === CONFIGURATION ===
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
TELEGRAM_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"

FLOAT_MAX = 20_000_000
PRICE_MAX = 10.0
VOLUME_SPIKE_MULT = 1.1         # Lowered for easy testing!
MIN_AVG_VOLUME = 1              # Lowered for easy testing!
NEWS_LOOKBACK_MINUTES = 60      # Back 1 hour for testing

SEEN_NEWS_FILE = "seen_news.txt"

def fetch_low_float_tickers():
    # For debugging, use a static list!
    tickers = ["SNTG", "GNS", "TBLT", "TOP", "HUBC", "HUDI", "CYN", "PEGY", "RAYA", "COMS"]
    print(f"{datetime.now()} | [DEBUG] Using static tickers: {tickers}")
    return tickers

def fetch_minute_candles(ticker):
    today = datetime.now().strftime("%Y-%m-%d")
    url = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{today}/{today}?apiKey={POLYGON_API_KEY}&limit=6'
    try:
        resp = requests.get(url, timeout=8)
        resp.raise_for_status()
        data = resp.json().get('results', [])
        print(f"[DEBUG] {ticker} minute candles fetched: {data}")
        return data
    except Exception as e:
        print(f"[ERROR] Error fetching {ticker} candles: {e}")
        return []

def send_telegram_alert(message):
    url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
    try:
        resp = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': message})
        print(f"[DEBUG] Telegram sent: {message} | Response: {resp.status_code}")
    except Exception as e:
        print(f"[ERROR] Error sending Telegram message: {e}")

def scan_for_volume(tickers, last_alerts):
    for ticker in tickers:
        candles = fetch_minute_candles(ticker)
        if len(candles) < 4:
            print(f"[DEBUG] {ticker} has less than 4 candles, skipping.")
            continue
        last = candles[-1]
        prev = candles[-4:-1]
        price = last['c']
        curr_vol = last['v']
        avg_prev_vol = sum(x['v'] for x in prev) / 3
        print(f"[DEBUG] {ticker}: price={price}, curr_vol={curr_vol}, avg_prev_vol={avg_prev_vol}")
        if price > PRICE_MAX:
            print(f"[DEBUG] {ticker}: price {price} > {PRICE_MAX}, skipping.")
            continue
        if curr_vol > avg_prev_vol * VOLUME_SPIKE_MULT and avg_prev_vol > MIN_AVG_VOLUME:
            # Only alert if price or volume changed since last alert for this ticker
            last_key = (price, curr_vol)
            if last_alerts.get(ticker) != last_key:
                msg = (f"🚨 {ticker}: Price ${price:.2f} | Float <20M | {curr_vol:,} 1min vol (>1.1x avg {int(avg_prev_vol):,})\n"
                       f"https://finance.yahoo.com/quote/{ticker}")
                print(f"[ALERT] {msg}")
                send_telegram_alert(msg)
                last_alerts[ticker] = last_key
            else:
                print(f"[DEBUG] {ticker}: Alert already sent for price {price} and vol {curr_vol}.")
        else:
            print(f"[DEBUG] {ticker}: No spike detected.")

def load_seen_news_ids(filename=SEEN_NEWS_FILE):
    if not os.path.exists(filename):
        return set()
    with open(filename, "r") as f:
        return set(line.strip() for line in f)

def save_seen_news_id(news_id, filename=SEEN_NEWS_FILE):
    with open(filename, "a") as f:
        f.write(f"{news_id}\n")

def scan_for_news(tickers, seen_news_ids):
    since = (datetime.utcnow() - timedelta(minutes=NEWS_LOOKBACK_MINUTES)).isoformat()[:16]
    for ticker in tickers:
        url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&published_utc.gte={since}&limit=5&apiKey={POLYGON_API_KEY}"
        try:
            resp = requests.get(url, timeout=8)
            news_items = resp.json().get("results", [])
            print(f"[DEBUG] News for {ticker}: {news_items}")
            for news in news_items:
                if news["id"] not in seen_news_ids:
                    seen_news_ids.add(news["id"])
                    save_seen_news_id(news["id"])  # Save to file!
                    headline = news["title"]
                    url_link = news.get("article_url", "")
                    alert = f"📰 NEWS: {ticker} - {headline}\n{url_link}"
                    print(f"[ALERT] {alert}")
                    send_telegram_alert(alert)
        except Exception as e:
            print(f"[ERROR] News error {ticker}: {e}")

if __name__ == "__main__":
    lowfloat_tickers = fetch_low_float_tickers()
    seen_news_ids = load_seen_news_ids()
    last_alerts = {}  # ticker -> (price, vol)
    while True:
        print(f"{datetime.now()} | [DEBUG] Scanning {len(lowfloat_tickers)} tickers for volume/news...")
        scan_for_volume(lowfloat_tickers, last_alerts)
        scan_for_news(lowfloat_tickers, seen_news_ids)
        print(f"{datetime.now()} | [DEBUG] Scan loop complete. Sleeping 60s.")
        time.sleep(60)
