import requests
import time
from datetime import datetime, timezone
import pytz

# --- CONFIG ---
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
TELEGRAM_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"
WATCHLIST = [
    'SVRE', 'INMB', 'RCT', 'CTOR', 'UGRO', 'AMIX', 'OGEN', 'PMNT'
]
CHECK_INTERVAL = 60  # seconds

POPULAR_NEWS_KEYWORDS = [
    "offering", "merger", "acquisition", "FDA", "earnings", "guidance", "spike", "halt",
    "lawsuit", "contract", "partnership", "approval", "phase", "buyout", "appoints",
    "delist", "split", "dividend", "bankruptcy", "IPO", "agreement", "collaboration",
    "settlement", "investigation", "grant", "license", "expansion", "recall", "patent",
    "sec", "upgrade", "downgrade", "initiates", "target", "price target", "surge", "plunge"
]

PRICE_FILTER = 10.00  # Only alert stocks under this price

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

def fetch_polygon_minute_candles(symbol):
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/2025-06-27/2025-06-27?adjusted=true&sort=asc&limit=50000&apiKey={POLYGON_API_KEY}"
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

def format_time(ts_ms):
    dt = datetime.fromtimestamp(ts_ms/1000, tz=timezone.utc)
    return dt.strftime('%H:%M')

def contains_popular_keyword(title):
    title_l = title.lower()
    return any(kw in title_l for kw in POPULAR_NEWS_KEYWORDS)

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

class StockAlertBot:
    def __init__(self, watchlist):
        self.watchlist = watchlist
        self.highs = {}  # symbol -> today's high
        self.lows = {}   # symbol -> today's low
        self.last_alerted = {}  # (symbol, type) -> (price, ts)
        self.sent_news_ids = set()  # news id set to prevent duplicate alerts

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
        if price > prev_high:
            self.highs[symbol] = price
            return True
        return False

    def check_new_low(self, symbol, price):
        prev_low = self.lows.get(symbol, float('inf'))
        if price < prev_low:
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
        for symbol in self.watchlist:
            try:
                candles = fetch_polygon_minute_candles(symbol)
                if not candles or len(candles) < 2:
                    continue
                last = candles[-1]
                price = last['c']
                ts = last['t']

                # Only scan/alert for stocks under PRICE_FILTER
                if price >= PRICE_FILTER:
                    continue

                # Volume spike
                spike, spike_price, spike_vol, spike_ts = self.check_volume_spike(candles)
                if spike and self.should_alert(symbol, spike_price, "volume"):
                    msg = (f"<b>{symbol}</b> volume spike at <b>${spike_price:.2f}</b> (vol={spike_vol}) "
                           f"@ {format_time(spike_ts)} UTC")
                    send_telegram(msg)
                # New high
                if self.check_new_high(symbol, price) and self.should_alert(symbol, price, "high"):
                    msg = (f"<b>{symbol}</b> new high of the day <b>${price:.2f}</b> 🤑 "
                           f"@ {format_time(ts)} UTC")
                    send_telegram(msg)
                # New low
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
        while True:
            if is_market_hours():
                self.scan()
            else:
                print("Outside scan window, waiting...")
            time.sleep(CHECK_INTERVAL)

if __name__ == "__main__":
    print("Starting Stock Alert Telegram Bot with news keyword filter and market hours restriction (stocks under $10 only)...")
    bot = StockAlertBot(WATCHLIST)
    bot.run()
