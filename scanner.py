import requests
import time
from datetime import datetime, timezone
import pytz

# --- CONFIG ---
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
TELEGRAM_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"
CHECK_INTERVAL = 60  # seconds
PRICE_FILTER = 10.00  # Only alert stocks under this price
HIGH_LOW_ALERT_THRESHOLD = 0.05  # 5% threshold for high/low alert

POPULAR_NEWS_KEYWORDS = [
    "offering", "merger", "acquisition", "FDA", "earnings", "guidance", "spike", "halt",
    "lawsuit", "contract", "partnership", "approval", "phase", "buyout", "appoints",
    "delist", "split", "dividend", "bankruptcy", "IPO", "agreement", "collaboration",
    "settlement", "investigation", "grant", "license", "expansion", "recall", "patent",
    "sec", "upgrade", "downgrade", "initiates", "target", "price target", "surge", "plunge"
]

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

def is_market_hours():
    eastern = pytz.timezone("US/Eastern")
    now_eastern = datetime.now(eastern)
    weekday = now_eastern.weekday()  # Monday=0, Sunday=6
    hour = now_eastern.hour
    minute = now_eastern.minute
    # Only Monday-Friday, 4:00am-8:00pm
    if 0 <= weekday <= 4:
        if 4 <= hour < 20:
            return True
        if hour == 20 and minute == 0:
            return True
    return False

def contains_popular_keyword(title):
    title_l = title.lower()
    return any(kw in title_l for kw in POPULAR_NEWS_KEYWORDS)

def format_time(ts_ms):
    dt = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc)
    return dt.strftime('%H:%M')

def get_all_us_equity_tickers():
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit=1000&apiKey={POLYGON_API_KEY}"
    tickers = []
    next_url = url
    while next_url:
        resp = requests.get(next_url, timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            page = data.get("results", [])
            tickers += [item["ticker"] for item in page if item.get("primary_exchange") in ("NYSE", "NASDAQ", "AMEX")]
            next_url = data.get("next_url")
            if next_url:
                if "apiKey" not in next_url:
                    next_url += f"&apiKey={POLYGON_API_KEY}"
            else:
                break
        else:
            print(f"Ticker list fetch error: {resp.status_code}")
            break
    return tickers

def get_latest_price(symbol):
    # Use the previous day's close as a fallback, since "last trade" may be spotty premarket/afterhours for thin stocks
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev?adjusted=true&apiKey={POLYGON_API_KEY}"
    resp = requests.get(url, timeout=10)
    if resp.status_code == 200:
        results = resp.json().get("results", [])
        if results:
            return results[0].get("c")
    return None

def fetch_polygon_minute_candles(symbol):
    today = datetime.now().strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{today}/{today}?adjusted=true&sort=asc&limit=50000&apiKey={POLYGON_API_KEY}"
    resp = requests.get(url, timeout=10)
    if resp.status_code == 200:
        data = resp.json()
        if data.get("results"):
            return data["results"]
    return []

def fetch_polygon_news(symbol, limit=5):
    url = f"https://api.polygon.io/v2/reference/news?ticker={symbol}&limit={limit}&apiKey={POLYGON_API_KEY}"
    resp = requests.get(url, timeout=10)
    if resp.status_code == 200:
        news = resp.json().get("results", [])
        return news
    return []

class StockAlertBot:
    def __init__(self):
        self.highs = {}  # symbol -> today's high
        self.lows = {}   # symbol -> today's low
        self.last_alerted = {}  # (symbol, type) -> (price, ts)
        self.sent_news_ids = set()  # news id set to prevent duplicate alerts
        self.tickers = []

    def update_market_tickers(self):
        print("Refreshing ticker list...")
        self.tickers = get_all_us_equity_tickers()
        print(f"Found {len(self.tickers)} tickers.")

    def check_volume_spike(self, candles):
        if len(candles) < 5:
            return False, None, None, None
        curr = candles[-1]
        curr_vol = curr['v']
        curr_price = curr['c']
        avg_vol = sum(c['v'] for c in candles[-5:-1]) / 4
        if avg_vol == 0:
            return False, None, None, None
        if curr_vol > avg_vol * 1.2 and curr_vol > 500:
            return True, curr_price, curr_vol, curr['t']
        return False, None, None, None

    def check_new_high(self, symbol, price):
        prev_high = self.highs.get(symbol, float('-inf'))
        if price > prev_high * (1 + HIGH_LOW_ALERT_THRESHOLD):
            self.highs[symbol] = price
            return True
        return False

    def check_new_low(self, symbol, price):
        prev_low = self.lows.get(symbol, float('inf'))
        if price < prev_low * (1 - HIGH_LOW_ALERT_THRESHOLD):
            self.lows[symbol] = price
            return True
        return False

    def should_alert(self, symbol, price, alert_type):
        now = time.time()
        last = self.last_alerted.get((symbol, alert_type))
        if last and last[0] == price and now - last[1] < 300:
            return False
        self.last_alerted[(symbol, alert_type)] = (price, now)
        return True

    def scan(self):
        for symbol in self.tickers:
            try:
                price = get_latest_price(symbol)
                if price is None or price >= PRICE_FILTER or price <= 0:
                    continue

                candles = fetch_polygon_minute_candles(symbol)
                if not candles or len(candles) < 2:
                    continue
                last = candles[-1]
                ts = last['t']

                # Volume spike
                spike, spike_price, spike_vol, spike_ts = self.check_volume_spike(candles)
                if spike and self.should_alert(symbol, spike_price, "volume"):
                    msg = (f"<b>{symbol}</b> volume spike at <b>${spike_price:.2f}</b> (vol={spike_vol}) "
                           f"@ {format_time(spike_ts)} UTC")
                    send_telegram(msg)
                # New high (only if 5% above previous high)
                if self.check_new_high(symbol, price) and self.should_alert(symbol, price, "high"):
                    msg = (f"<b>{symbol}</b> new high of the day <b>${price:.2f}</b> 🤑 "
                           f"@ {format_time(ts)} UTC")
                    send_telegram(msg)
                # New low (only if 5% below previous low)
                if self.check_new_low(symbol, price) and self.should_alert(symbol, price, "low"):
                    msg = (f"<b>{symbol}</b> new low of the day <b>${price:.2f}</b> 😬 "
                           f"@ {format_time(ts)} UTC")
                    send_telegram(msg)
                # News headlines (no duplicates, only if keyword)
                news = fetch_polygon_news(symbol, limit=5)
                for item in news:
                    nid = item.get("id")
                    title = item.get("title", "")
                    url = item.get("article_url", "")
                    if nid and nid not in self.sent_news_ids and contains_popular_keyword(title):
                        self.sent_news_ids.add(nid)
                        newsmsg = f"📰 <b>{symbol} News:</b> <b>{title}</b>\n{url}"
                        send_telegram(newsmsg)
            except Exception as e:
                print(f"Error scanning {symbol}: {e}")

    def run(self):
        # Refresh tickers at launch and every hour
        last_update = 0
        while True:
            if is_market_hours():
                now = time.time()
                if now - last_update > 3600 or not self.tickers:
                    self.update_market_tickers()
                    last_update = now
                self.scan()
            else:
                print("Outside scan window, waiting...")
            time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    print("Starting Stock Alert Telegram Bot (scanning entire market under $10, high/low threshold 5%)...")
    bot = StockAlertBot()
    bot.run()
