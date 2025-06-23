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
    # Only alert for stocks with price > $2
    if price is not None and price < 2.0:
        return
    message = f"📰 ${symbol} — {headline}\n(keyword: {keyword})"
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
        alerted_tickers.add(symbol)
    except Exception as e:
        logging.error(f"Telegram News error: {e}")

def send_volume_telegram_alert(symbol, rel_vol, total_vol, avg_vol, price, price_change):
    # Only alert if rel_vol >= 4.0, total_vol > 100000, price > $2, and |price_change| > 2%
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

def send_ema_stack_alert(symbol, price, timeframe, confidence):
    message = f"📈🔥 ${symbol} — EMA STACK ({timeframe}): 8 > 13 > 21\nCurrent Price: {price:.2f}\nConfidence: {confidence}/10"
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
    except Exception as e:
        logging.error(f"Telegram EMA stack alert error: {e}")

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
                # Exclude OTC
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

def check_volume_spike_worker(symbol, now_utc, cooldown, now_ts):
    try:
        end_time = int(now_utc.timestamp() * 1000)
        start_time = int((now_utc - timedelta(minutes=5)).timestamp() * 1000)
        candles = get_aggs(symbol, "minute", 1, start_time, end_time, limit=5)
        if not candles or len(candles) < 5:
            return
        avg_vol = sum(candle["v"] for candle in candles[:-1]) / len(candles[:-1])
        if avg_vol < 50000:
            return
        total_vol = sum(candle["v"] for candle in candles)
        price = candles[-1]["c"]
        price_change = (candles[-1]["c"] - candles[0]["o"]) / candles[0]["o"] * 100 if candles[0]["o"] != 0 else 0
        rel_vol = total_vol / avg_vol if avg_vol > 0 else 0
        if now_ts - last_alert_time.get(symbol, 0) > cooldown:
            send_volume_telegram_alert(symbol, rel_vol, total_vol, avg_vol, price, price_change)
            last_alert_time[symbol] = now_ts
    except Exception as e:
        error_counts[str(e)] += 1

def volume_spike_scanner():
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers()
            logging.info(f"Fetched {len(tickers)} tickers [Volume Scanner]")
            logging.info("Sample tickers: %s", tickers[:10])
            now_utc = datetime.utcnow()
            cooldown = 60
            now_ts = time.time()
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(check_volume_spike_worker, symbol, now_utc, cooldown, now_ts) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(2)

def check_ema_stack_worker(symbol, timespan="minute", label_5min=False):
    try:
        now = datetime.utcnow()
        if timespan == "minute":
            end_time = int(now.timestamp() * 1000)
            start_time = int((now - timedelta(minutes=60)).timestamp() * 1000)
            candles = get_aggs(symbol, "minute", 1, start_time, end_time, limit=30)
        else:
            end_time = int(now.timestamp() * 1000)
            start_time = int((now - timedelta(minutes=300)).timestamp() * 1000)
            candles = get_aggs(symbol, "minute", 5, start_time, end_time, limit=30)
        if not candles or len(candles) < 21:
            return
        closes = [candle["c"] for candle in candles]
        def ema(prices, period):
            k = 2 / (period + 1)
            ema_values = [prices[0]]
            for price in prices[1:]:
                ema_values.append(price * k + ema_values[-1] * (1 - k))
            return ema_values
        ema_8 = ema(closes, 8)[-1]
        ema_13 = ema(closes, 13)[-1]
        ema_21 = ema(closes, 21)[-1]
        if not (ema_8 > ema_13 > ema_21):
            return
        last_candle = candles[-1]
        avg_vol = sum(candle["v"] for candle in candles[-10:]) / 10
        if last_candle["v"] < avg_vol or last_candle["v"] < 50000 or last_candle["c"] < 2.0:
            return
        price_change = (closes[-1] - closes[0]) / closes[0] * 100
        if abs(price_change) < 3:
            return
        confidence = 10 if price_change > 5 else 8 if price_change > 3 else 6
        label = "5-min" if label_5min else ("1-min" if timespan=="minute" else timespan)
        send_ema_stack_alert(symbol, closes[-1], label, confidence)
    except Exception as e:
        error_counts[str(e)] += 1

def ema_stack_scanner():
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers()
            logging.info(f"Fetched {len(tickers)} tickers [EMA Scanner]")
            logging.info("Sample tickers: %s", tickers[:10])
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(check_ema_stack_worker, symbol, "minute", False) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
                futures = [executor.submit(check_ema_stack_worker, symbol, "5minute", True) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(3)

def check_hod_worker(symbol):
    try:
        if symbol not in alerted_tickers:
            return
        now = datetime.utcnow()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start_ts = int(start.timestamp() * 1000)
        end_ts = int(now.timestamp() * 1000)
        candles = get_aggs(symbol, "minute", 1, start_ts, end_ts, limit=1000)
        if not candles:
            return
        hod = max(candle["h"] for candle in candles)
        prev_hod = hod_tracker.get(symbol, None)
        open_price = candles[0]["o"] if candles else 0
        if prev_hod is None or hod > prev_hod:
            if prev_hod is not None:
                send_hod_telegram_alert(symbol, hod, open_price)
            hod_tracker[symbol] = hod
    except Exception as e:
        error_counts[str(e)] += 1

def hod_scanner():
    while True:
        if is_market_hours():
            tickers = list(alerted_tickers)
            logging.info(f"Fetched {len(tickers)} tickers [HOD Scanner]")
            logging.info("Sample tickers: %s", tickers[:10])
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(check_hod_worker, symbol) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(2)

async def async_scan_news_and_alert_parallel(tickers, keywords):
    import aiohttp
    semaphore = asyncio.Semaphore(5)  # Lower concurrency for stability

    async def fetch_news(session, symbol):
        async with semaphore:
            try:
                url = f"https://api.polygon.io/v2/reference/news?ticker={symbol}&limit=3&apiKey={POLYGON_API_KEY}"
                async with session.get(url, timeout=8) as r:
                    data = await r.json()
                if "error" in data or (data.get("status") == "ERROR"):
                    logging.error(f"News API error for {symbol}: {data}")
                return symbol, data.get("results", [])
            except Exception as e:
                error_counts[str(e)] += 1
                return symbol, []

    tasks = []
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=5)) as session:
        for symbol in tickers[:100]:  # Only scan 100 for news for stability
            tasks.append(fetch_news(session, symbol))
        for future in asyncio.as_completed(tasks):
            symbol, news_items = await future
            # Get price for news filter
            price = get_last_price(symbol)
            for news in news_items:
                headline = news.get("title", "").lower()
                news_id = news.get("id", "")
                with news_lock:
                    if news_id in last_news_ids:
                        continue
                    matched = [kw for kw in keywords if kw.lower() in headline]
                    if matched and price > 2.0:
                        send_news_telegram_alert(symbol, news.get("title", ""), matched[0], price=price)
                        last_news_ids.add(news_id)

def news_polling_scanner():
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers()
            logging.info(f"Fetched {len(tickers)} tickers [News Scanner]")
            logging.info("Sample tickers: %s", tickers[:10])
            asyncio.run(async_scan_news_and_alert_parallel(tickers, KEYWORDS))
        time.sleep(15)

def check_gap_worker(symbol, seen_today):
    try:
        today = datetime.utcnow().date()
        yesterday = today - timedelta(days=1)
        yest = get_aggs(symbol, "day", 1, str(yesterday), str(yesterday), limit=1)
        today_agg = get_aggs(symbol, "day", 1, str(today), str(today), limit=1)
        if not yest or not today_agg:
            return
        prev = yest[0]
        curr = today_agg[0]
        if curr["o"] < 2.0:
            return
        gap = (curr["o"] - prev["c"]) / prev["c"] * 100
        key = f"{symbol}|{curr['o']}|{prev['c']}"
        if abs(gap) >= 5 and key not in seen_today:
            direction = "Gap Up" if gap > 0 else "Gap Down"
            message = f"🚀 {direction}: ${symbol} opened {gap:.1f}% {'higher' if gap > 0 else 'lower'}"
            bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
            seen_today.add(key)
    except Exception as e:
        error_counts[str(e)] += 1

def gap_scanner():
    seen_today = set()
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers()
            logging.info(f"Fetched {len(tickers)} tickers [Gap Scanner]")
            logging.info("Sample tickers: %s", tickers[:10])
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(check_gap_worker, symbol, seen_today) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(15)

def check_pm_ah_worker(symbol, seen, now_et, in_premarket, in_ah):
    try:
        today = datetime.utcnow().date()
        yesterday = today - timedelta(days=1)
        yest = get_aggs(symbol, "day", 1, str(yesterday), str(yesterday), limit=1)
        if not yest:
            return
        prev_close = yest[0]["c"]
        now = datetime.utcnow()
        end_time = int(now.timestamp() * 1000)
        start_time = int((now - timedelta(minutes=60)).timestamp() * 1000)
        trades = get_aggs(symbol, "minute", 1, start_time, end_time, limit=60)
        if not trades:
            return
        last = trades[-1]["c"]
        if last < 2.0:
            return
        move = (last - prev_close) / prev_close * 100
        key = f"{symbol}|{now_et.date()}|{last}"
        if abs(move) >= 5 and key not in seen:
            session_desc = "Premarket" if in_premarket else "After-hours"
            message = f"⚡ {session_desc} Mover: ${symbol} is up {move:.1f}%"
            bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
            seen.add(key)
    except Exception as e:
        error_counts[str(e)] += 1

def premarket_ah_mover_scanner():
    seen = set()
    while True:
        now_et = datetime.now(EASTERN)
        in_premarket = 4 <= now_et.hour < 9 or (now_et.hour == 9 and now_et.minute < 30)
        in_ah = 16 <= now_et.hour < 20
        if in_premarket or in_ah:
            tickers = fetch_all_tickers()
            logging.info(f"Fetched {len(tickers)} tickers [Premarket/AH Scanner]")
            logging.info("Sample tickers: %s", tickers[:10])
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(check_pm_ah_worker, symbol, seen, now_et, in_premarket, in_ah) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(15)

def error_summary_thread():
    while True:
        time.sleep(300)  # every 5 minutes
        log_error_summary()

if __name__ == "__main__":
    bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="🚨 THIS IS A TEST ALERT. If you see this, Telegram is working.")
    threading.Thread(target=volume_spike_scanner, daemon=True).start()
    threading.Thread(target=ema_stack_scanner, daemon=True).start()
    threading.Thread(target=hod_scanner, daemon=True).start()
    threading.Thread(target=news_polling_scanner, daemon=True).start()
    threading.Thread(target=gap_scanner, daemon=True).start()
    threading.Thread(target=premarket_ah_mover_scanner, daemon=True).start()
    threading.Thread(target=error_summary_thread, daemon=True).start()
    while True:
        time.sleep(60)
