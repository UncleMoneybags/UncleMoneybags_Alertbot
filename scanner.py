import time
import requests
import asyncio
import pickle
import os
from datetime import datetime, timedelta
import pytz
from telegram import Bot
import threading
from queue import Queue
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from collections import defaultdict
import warnings

# === CONFIG ===
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
EASTERN = pytz.timezone('US/Eastern')
SCAN_START_HOUR = 4
SCAN_END_HOUR = 20
MAX_FLOAT = 10_000_000  # 10 million
MAX_CANDLE_REQS_PER_SEC = 5  # Polygon Advanced plan per-endpoint limit

from logging.handlers import RotatingFileHandler
log_handler = RotatingFileHandler('scanner.log', maxBytes=10*1024*1024, backupCount=5)
log_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logging.getLogger('').handlers = []
logging.getLogger('').addHandler(log_handler)
logging.getLogger('').addHandler(logging.StreamHandler())
logging.getLogger('').setLevel(logging.INFO)
error_counts = defaultdict(int)

warnings.filterwarnings("ignore", message="Connection pool is full, discarding connection")

session = requests.Session()
adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
session.mount('https://', adapter)
session.mount('http://', adapter)

from telegram.utils.request import Request
telegram_request = Request(con_pool_size=40)
bot = Bot(token=TELEGRAM_BOT_TOKEN, request=telegram_request)

last_alert_time = {}
alerted_tickers = set()
hod_tracker = {}

KEYWORDS = [
    "fda approval", "acquisition", "merger", "guidance raised", "record revenue", "breakthrough therapy",
    "phase 3", "nda approval", "buyback", "uplisting", "contract", "strategic partnership",
    "emergency use authorization"
]
news_lock = threading.Lock()

NEWS_ALERTED_FILE = "news_alerted_ids.pkl"
news_alerted_ids = set()

def load_news_alerted_ids():
    global news_alerted_ids
    if os.path.exists(NEWS_ALERTED_FILE):
        try:
            with open(NEWS_ALERTED_FILE, "rb") as f:
                news_alerted_ids = pickle.load(f)
        except Exception:
            news_alerted_ids = set()
    else:
        news_alerted_ids = set()

def save_news_alerted_ids():
    try:
        with open(NEWS_ALERTED_FILE, "wb") as f:
            pickle.dump(news_alerted_ids, f)
    except Exception:
        pass

def log_error_summary():
    for err, count in error_counts.items():
        if count > 0:
            logging.error(f"{err}: occurred {count} times")
    error_counts.clear()

def is_market_hours():
    now = datetime.now(EASTERN)
    return now.weekday() < 5 and SCAN_START_HOUR <= now.hour < SCAN_END_HOUR

def send_news_telegram_alert(symbol, headline, keyword, price=None):
    if price is not None and price < 0.05:
        return
    message = f"📰 ${symbol} — {headline}\n(keyword: {keyword})"
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
        alerted_tickers.add(symbol)
    except Exception as e:
        logging.error(f"Telegram News error: {e}")

def send_volume_telegram_alert(symbol, rel_vol, price, price_change):
    message = (
        f"🚨 ${symbol} — Volume Spike\n"
        f"Spike: {rel_vol:.2f}x\n"
        f"Price: {price:.2f} ({price_change:.2f}%)"
    )
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

def send_pm_ah_alert(symbol, pct, session_desc):
    message = f"⚡ {session_desc} Mover: ${symbol} is up {pct:.1f}%"
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
        alerted_tickers.add(symbol)
    except Exception as e:
        logging.error(f"Telegram PM/AH alert error: {e}")

def send_breakout_alert(symbol, price, resistance, volume):
    message = f"💥 BREAKOUT: ${symbol} broke resistance {resistance:.2f} now {price:.2f} with vol {volume}"
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
    except Exception as e:
        logging.error(f"Telegram breakout alert error: {e}")

def fetch_all_tickers_with_float(max_float=MAX_FLOAT):
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&type=CS&active=true&limit=1000&apiKey={POLYGON_API_KEY}"
    tickers = []
    seen = set()
    while url:
        try:
            resp = session.get(url, timeout=15)
            data = resp.json()
            results = data.get('results', [])
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
                float_shares = item.get("share_class_shares_outstanding", None)
                if float_shares is None:
                    try:
                        f_url = f"https://api.polygon.io/v3/reference/financials?ticker={symbol}&limit=1&apiKey={POLYGON_API_KEY}"
                        f_resp = session.get(f_url, timeout=10)
                        f_data = f_resp.json()
                        fin = f_data.get("results", [{}])[0]
                        float_shares = fin.get("float_shares_outstanding", None)
                    except Exception:
                        float_shares = None
                if float_shares is not None and float_shares <= max_float:
                    tickers.append(symbol)
            url = data.get('next_url')
            if url:
                if url.startswith("/"):
                    url = f"https://api.polygon.io{url}&apiKey={POLYGON_API_KEY}"
                else:
                    url += f"&apiKey={POLYGON_API_KEY}"
        except Exception as e:
            error_counts[str(e)] += 1
            break
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

# --- Volume Spike Scanner with Polygon Rate Limiting ---
def check_volume_spike_worker(symbol, now_utc, cooldown, now_ts):
    try:
        end_time = int(now_utc.timestamp() * 1000)
        start_time = int((now_utc - timedelta(minutes=3)).timestamp() * 1000)
        candles = get_aggs(symbol, "minute", 1, start_time, end_time, limit=4)
        if not candles or len(candles) < 4:
            return
        avg_vol = sum(candle["v"] for candle in candles[:-1]) / 3
        last_vol = candles[-1]["v"]
        if avg_vol <= 0:
            return
        rel_vol = last_vol / avg_vol
        if 2 <= rel_vol < 3:
            price = candles[-1]["c"]
            price_change = (candles[-1]["c"] - candles[0]["o"]) / candles[0]["o"] * 100 if candles[0]["o"] != 0 else 0
            if 0.05 <= price <= 10 and abs(price_change) >= 5:
                if now_ts - last_alert_time.get(symbol, 0) > cooldown:
                    send_volume_telegram_alert(symbol, rel_vol, price, price_change)
                    last_alert_time[symbol] = now_ts
    except Exception as e:
        error_counts[str(e)] += 1

def rate_limited_worker(ticker_queue, *scanner_args):
    while True:
        ticker = ticker_queue.get()
        if ticker is None:
            break
        check_volume_spike_worker(ticker, *scanner_args)
        ticker_queue.task_done()

def volume_spike_scanner():
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers_with_float(MAX_FLOAT)
            ticker_queue = Queue()
            for symbol in tickers:
                ticker_queue.put(symbol)
            workers = []
            now_utc = datetime.utcnow()
            cooldown = 60
            now_ts = time.time()
            for _ in range(MAX_CANDLE_REQS_PER_SEC):
                t = threading.Thread(target=rate_limited_worker, args=(ticker_queue, now_utc, cooldown, now_ts))
                t.daemon = True
                t.start()
                workers.append(t)
            for _ in range(len(tickers)):
                time.sleep(1.0 / MAX_CANDLE_REQS_PER_SEC)
            ticker_queue.join()
            for _ in range(MAX_CANDLE_REQS_PER_SEC):
                ticker_queue.put(None)
            for t in workers:
                t.join()
        time.sleep(1)

# --- EMA STACK SCANNER ---
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
        ema8s = ema(closes, 8)
        ema13s = ema(closes, 13)
        ema21s = ema(closes, 21)
        if not ema8s or not ema13s or not ema21s:
            return
        ema_8 = ema8s[-1]
        ema_13 = ema13s[-1]
        ema_21 = ema21s[-1]
        if not (ema_8 > ema_13 > ema_21):
            return
        last_candle = candles[-1]
        avg_vol = sum(candle["v"] for candle in candles[-10:]) / 10
        if last_candle["v"] < avg_vol or last_candle["v"] < 50000 or last_candle["c"] < 0.05 or last_candle["c"] > 10:
            return
        price_change = (closes[-1] - closes[0]) / closes[0] * 100
        if abs(price_change) < 3:
            return
        confidence = 10 if price_change > 5 else 8 if price_change > 3 else 6
        label = "5-min" if label_5min else ("1-min" if timespan=="minute" else timespan)
        send_ema_stack_alert(symbol, closes[-1], label, confidence, ema_8, ema_13, ema_21)
    except Exception as e:
        error_counts[str(e)] += 1

def ema_stack_scanner():
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers_with_float(MAX_FLOAT)
            with ThreadPoolExecutor(max_workers=MAX_CANDLE_REQS_PER_SEC) as executor:
                futures = [executor.submit(check_ema_stack_worker, symbol, "minute", False) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
                futures = [executor.submit(check_ema_stack_worker, symbol, "5minute", True) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(3)

# --- HOD SCANNER ---
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
            with ThreadPoolExecutor(max_workers=2) as executor:
                futures = [executor.submit(check_hod_worker, symbol) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(2)

# --- BREAKOUT SCANNER ---
def find_resistance_level(candles, lookback=100, tolerance=0.002, min_touches=2):
    highs = [c['h'] for c in candles[-lookback:]]
    levels = []
    for i, high in enumerate(highs):
        touches = sum(1 for h in highs if abs(h - high)/high < tolerance)
        if touches >= min_touches:
            levels.append((high, touches))
    if not levels:
        return None
    levels.sort(key=lambda x: (x[1], x[0]), reverse=True)
    return levels[0][0]

def breakout_scanner():
    already_alerted = set()
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers_with_float(MAX_FLOAT)
            for symbol in tickers:
                now = datetime.utcnow()
                end_time = int(now.timestamp() * 1000)
                start_time = int((now - timedelta(minutes=120)).timestamp() * 1000)
                candles = get_aggs(symbol, "minute", 1, start_time, end_time, limit=120)
                if not candles or len(candles) < 30:
                    continue
                resistance = find_resistance_level(candles[:-5], lookback=100)
                if not resistance:
                    continue
                price = candles[-1]["c"]
                if price < 0.05 or price > 10:
                    continue
                prev_close = candles[-2]["c"]
                key = f"{symbol}:{resistance:.2f}"
                if prev_close < resistance and price > resistance and key not in already_alerted:
                    avg_vol = sum(c["v"] for c in candles[-11:-1]) / 10
                    if candles[-1]["v"] > 1.5 * avg_vol:
                        send_breakout_alert(symbol, price, resistance, candles[-1]["v"])
                        already_alerted.add(key)
        time.sleep(10)

# --- NEWS SCANNER ---
async def async_scan_news_and_alert_parallel(tickers, keywords):
    import aiohttp
    semaphore = asyncio.Semaphore(5)
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
        for symbol in tickers[:100]:
            tasks.append(fetch_news(session, symbol))
        for future in asyncio.as_completed(tasks):
            symbol, news_items = await future
            for news in news_items:
                news_id = news.get("id", "")
                if not news_id:
                    continue
                with news_lock:
                    if news_id in news_alerted_ids:
                        continue
                    headline = news.get("title", "").lower()
                    matched = [kw for kw in keywords if kw.lower() in headline]
                    if matched:
                        send_news_telegram_alert(symbol, news.get("title", ""), matched[0])
                        news_alerted_ids.add(news_id)
                        save_news_alerted_ids()

def news_polling_scanner():
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers_with_float(MAX_FLOAT)
            asyncio.run(async_scan_news_and_alert_parallel(tickers, KEYWORDS))
        time.sleep(5)

# --- PREMARKET/AFTERHOURS SCANNER ---
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
        if last < 0.05 or last > 10:
            return
        move = (last - prev_close) / prev_close * 100
        key = f"{symbol}|{now_et.date()}|{last}"
        if abs(move) >= 30 and key not in seen:
            session_desc = "Premarket" if in_premarket else "After-hours"
            send_pm_ah_alert(symbol, move, session_desc)
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
            tickers = fetch_all_tickers_with_float(MAX_FLOAT)
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(check_pm_ah_worker, symbol, seen, now_et, in_premarket, in_ah) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(15)

def error_summary_thread():
    while True:
        time.sleep(300)
        log_error_summary()

def scheduled_time_alerts():
    last_alerts = {"premarket": None, "open": None, "close": None}
    while True:
        now_et = datetime.now(EASTERN)
        weekday = now_et.weekday()
        today = now_et.date()
        if weekday < 5 and now_et.hour == 3 and now_et.minute == 55:
            if last_alerts["premarket"] != today:
                try:
                    bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID,
                        text="⏰ Premarket in 5 mins...be there!",
                        parse_mode="HTML"
                    )
                    last_alerts["premarket"] = today
                except Exception as e:
                    logging.error(f"Premarket alert error: {e}")
        if weekday < 5 and now_et.hour == 9 and now_et.minute == 25:
            if last_alerts["open"] != today:
                try:
                    bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID,
                        text="🚀 Market opens in 5 mins....secure the damn bag!",
                        parse_mode="HTML"
                    )
                    last_alerts["open"] = today
                except Exception as e:
                    logging.error(f"Open alert error: {e}")
        if weekday < 5 and now_et.hour == 20 and now_et.minute == 0:
            if last_alerts["close"] != today:
                try:
                    bot.send_message(
                        chat_id=TELEGRAM_CHAT_ID,
                        text="🔔 Market Closed....recovene in pre market.",
                        parse_mode="HTML"
                    )
                    last_alerts["close"] = today
                except Exception as e:
                    logging.error(f"Close alert error: {e}")
        time.sleep(20)

if __name__ == "__main__":
    load_news_alerted_ids()
    bot.send_message(
        chat_id=TELEGRAM_CHAT_ID,
        text="🚨 SCANNER RESTARTED. Volume, EMA, HOD, news, breakout, PM/AH, and scheduled time alerts active. Volume spike = 2–3x (3min), ≥5% move, $0.05–$10, float ≤10M. Max speed without exceeding Polygon Advanced plan limits."
    )
    threading.Thread(target=volume_spike_scanner, daemon=True).start()
    threading.Thread(target=ema_stack_scanner, daemon=True).start()
    threading.Thread(target=hod_scanner, daemon=True).start()
    threading.Thread(target=news_polling_scanner, daemon=True).start()
    threading.Thread(target=premarket_ah_mover_scanner, daemon=True).start()
    threading.Thread(target=breakout_scanner, daemon=True).start()
    threading.Thread(target=scheduled_time_alerts, daemon=True).start()
    threading.Thread(target=error_summary_thread, daemon=True).start()
    while True:
        time.sleep(60)
