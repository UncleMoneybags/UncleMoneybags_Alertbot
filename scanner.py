import time
import requests
from datetime import datetime, timedelta
import pytz
from telegram import Bot
import threading
import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from collections import defaultdict
import warnings

# === LOGGING SETUP ===
from logging.handlers import RotatingFileHandler

log_handler = RotatingFileHandler('scanner.log', maxBytes=10*1024*1024, backupCount=5)
log_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logging.getLogger('').handlers = []  # Remove default handlers
logging.getLogger('').addHandler(log_handler)
logging.getLogger('').addHandler(logging.StreamHandler())  # Also log to console
logging.getLogger('').setLevel(logging.INFO)

error_counts = defaultdict(int)

# === SUPPRESS CONNECTION POOL WARNINGS ===
warnings.filterwarnings("ignore", message="Connection pool is full, discarding connection")

# === CUSTOM REQUESTS SESSION WITH BIGGER POOL ===
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
session.mount('https://', adapter)
session.mount('http://', adapter)

# === CONFIG ===
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"

from telegram.utils.request import Request
telegram_request = Request(con_pool_size=40)  # Increase Telegram pool!
bot = Bot(token=TELEGRAM_BOT_TOKEN, request=telegram_request)

EASTERN = pytz.timezone('US/Eastern')
SCAN_START_HOUR = 4
SCAN_END_HOUR = 20

last_alert_time = {}
alerted_tickers = set()
hod_tracker = {}

# === IMPACTFUL NEWS ONLY ===
KEYWORDS = [
    "fda approval", "acquisition", "merger", "guidance raised", "record revenue", "breakthrough therapy",
    "phase 3", "nda approval", "buyback", "uplisting", "contract", "strategic partnership",
    "emergency use authorization"
]

last_news_ids = set()
news_lock = threading.Lock()

def log_error_summary():
    for err, count in error_counts.items():
        if count > 0:
            logging.error(f"{err}: occurred {count} times")
    error_counts.clear()

def is_market_hours():
    now = datetime.now(EASTERN)
    return now.weekday() < 5 and SCAN_START_HOUR <= now.hour < SCAN_END_HOUR

def send_news_telegram_alert(symbol, headline, keyword, price=None):
    if price is not None and price < 2.0:
        return
    message = f"📰 ${symbol} — {headline}\n(keyword: {keyword})"
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
        alerted_tickers.add(symbol)
    except Exception as e:
        logging.error(f"Telegram News error: {e}")

def send_volume_telegram_alert(symbol, rel_vol, total_vol, avg_vol, price, price_change):
    if rel_vol < 4.0 or total_vol < 100000 or price < 2.0 or abs(price_change) < 2.0:
        return
    message = f"🚨 ${symbol} — Volume Spike\nRel: {rel_vol:.2f} Tot: {total_vol} Avg: {avg_vol} Price: {price:.2f} ({price_change:.2f}%)"
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
        alerted_tickers.add(symbol)
    except Exception as e:
        logging.error(f"Telegram volume alert error: {e}")

def send_hod_telegram_alert(symbol, price, open_price):
    percent = (price - open_price) / open_price * 100 if open_price else 0
    message = f"💰🚀 ${symbol} — NEW HIGH OF DAY: {price:.2f} ({percent:.2f}%)"
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
    except Exception as e:
        logging.error(f"Telegram HOD alert error: {e}")

def send_ema_stack_alert(symbol, price, timeframe, confidence, ema8, ema13, ema21):
    message = (
        f"📈🔥 ${symbol} — EMA STACK ({timeframe}): 8 > 13 > 21\n"
        f"Current Price: {price:.2f}\n"
        f"EMAs: 8={ema8:.4f}, 13={ema13:.4f}, 21={ema21:.4f}\n"
        f"Confidence: {confidence}/10"
    )
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
    except Exception as e:
        logging.error(f"Telegram EMA stack alert error: {e}")

def send_breakout_alert(symbol, price, high, volume, price_change, avg_vol):
    message = (
        f"💥 BREAKOUT: ${symbol} New HOD {high:.2f} "
        f"Vol {volume:.0f} ({price_change:.1f}% from open, avg vol: {avg_vol:.0f})"
    )
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
        alerted_tickers.add(symbol)
    except Exception as e:
        logging.error(f"Telegram Breakout alert error: {e}")

def fetch_all_tickers():
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&type=CS&active=true&limit=1000&apiKey={POLYGON_API_KEY}"
    tickers = []
    seen = set()
    page = 0
    while url:
        try:
            resp = session.get(url, timeout=15)
            data = resp.json()
            results = data.get('results', [])
            logging.info(f"Polygon page {page} returned {len(results)} raw tickers")
            logging.info("Sample raw ticker result: %s", results[:2])
            for item in results:
                symbol = item.get('ticker')
                if not symbol or symbol in seen:
                    continue
                seen.add(symbol)
                if item.get('primary_exchange') == 'OTC':
                    continue
                name = item.get('name', '').lower()
                if any(x in name for x in [
                    'etf', 'fund', 'trust', 'depositary', 'unit', 'warrant',
                    'preferred', 'adr', 'note', 'bond', 'income'
                ]):
                    continue
                tickers.append(symbol)
            url = data.get('next_url')
            if url:
                if url.startswith("/"):
                    url = f"https://api.polygon.io{url}&apiKey={POLYGON_API_KEY}"
                else:
                    url += f"&apiKey={POLYGON_API_KEY}"
            page += 1
        except Exception as e:
            error_counts[str(e)] += 1
            break
    logging.info(f"Fetched {len(tickers)} filtered tickers")
    logging.info("Sample filtered tickers: %s", tickers[:10])
    return tickers

def get_aggs(symbol, timespan, multiplier, from_ts, to_ts, limit=1000):
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_ts}/{to_ts}?adjusted=true&sort=asc&limit={limit}&apiKey={POLYGON_API_KEY}"
    try:
        resp = session.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])
    except Exception as e:
        error_counts[str(e)] += 1
        return []

def get_last_price(symbol):
    url = f"https://api.polygon.io/v2/last/trade/stock/{symbol}?apiKey={POLYGON_API_KEY}"
    try:
        resp = session.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", {}).get("p", 0)
    except Exception as e:
        error_counts[str(e)] += 1
        return 0

def ema(values, period):
    if len(values) < period:
        return []
    emas = []
    k = 2 / (period + 1)
    sma = sum(values[:period]) / period
    emas = [None] * (period - 1) + [sma]
    for price in values[period:]:
        prev_ema = emas[-1]
        emas.append((price - prev_ema) * k + prev_ema)
    return emas

def check_breakout_worker(symbol, now):
    try:
        # Get today's minute candles
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start_ts = int(start.timestamp() * 1000)
        end_ts = int(now.timestamp() * 1000)
        candles = get_aggs(symbol, "minute", 1, start_ts, end_ts, limit=1000)
        if not candles or len(candles) < 15:
            return
        prev_high = max(c["h"] for c in candles[:-1])
        last_candle = candles[-1]
        if last_candle["h"] > prev_high and last_candle["c"] > 2.0:
            avg_vol = sum(c["v"] for c in candles[-11:-1]) / 10
            if last_candle["v"] > 2 * avg_vol:
                open_price = candles[0]["o"]
                price_change = (last_candle["c"] - open_price) / open_price * 100
                if price_change > 3:
                    logging.info(f"Breakout: {symbol} HOD {last_candle['h']}, Vol {last_candle['v']}, Change {price_change:.1f}%, avg_vol {avg_vol}")
                    send_breakout_alert(symbol, last_candle["c"], last_candle["h"], last_candle["v"], price_change, avg_vol)
    except Exception as e:
        error_counts[str(e)] += 1

def breakout_scanner():
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers()
            now = datetime.utcnow()
            with ThreadPoolExecutor(max_workers=4) as executor:
                for symbol in tickers:
                    executor.submit(check_breakout_worker, symbol, now)
        time.sleep(2)

# (The rest of your previously discussed scanners: volume_spike_scanner, ema_stack_scanner, news_polling_scanner, etc, go here...)

def error_summary_thread():
    while True:
        time.sleep(300)
        log_error_summary()

if __name__ == "__main__":
    bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="🚨 THIS IS A TEST ALERT. If you see this, Telegram is working.")
    threading.Thread(target=breakout_scanner, daemon=True).start()
    # Add your other scanner threads below as needed
    threading.Thread(target=error_summary_thread, daemon=True).start()
    while True:
        time.sleep(60)
