import time
import requests
from datetime import datetime, timedelta
import pytz
from telegram import Bot
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from collections import defaultdict

# --- Config ---
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
EASTERN = pytz.timezone('US/Eastern')
SCAN_START_HOUR = 4
SCAN_END_HOUR = 20

# --- Logging ---
logging.basicConfig(level=logging.INFO)

# --- Telegram setup ---
from telegram.utils.request import Request
telegram_request = Request(con_pool_size=20)
bot = Bot(token=TELEGRAM_BOT_TOKEN, request=telegram_request)

# --- Breakout alert strict dedupe ---
breakout_alerted_hod = set()  # (symbol, hod, date)
last_reset_date = None

def daily_reset():
    global breakout_alerted_hod, last_reset_date
    today_str = datetime.now(EASTERN).strftime('%Y-%m-%d')
    if last_reset_date != today_str:
        breakout_alerted_hod.clear()
        last_reset_date = today_str

def is_market_hours():
    now = datetime.now(EASTERN)
    return now.weekday() < 5 and SCAN_START_HOUR <= now.hour < SCAN_END_HOUR

def send_breakout_alert(symbol, price, high, volume, price_change, avg_vol):
    message = (
        f"💥 BREAKOUT: ${symbol} New HOD {high:.2f} "
        f"Vol {volume:.0f} ({price_change:.1f}% from open, avg vol: {avg_vol:.0f})"
    )
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
    except Exception as e:
        logging.error(f"Telegram Breakout alert error: {e}")

def fetch_all_tickers():
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&type=CS&active=true&limit=1000&apiKey={POLYGON_API_KEY}"
    tickers = []
    seen = set()
    page = 0
    session = requests.Session()
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
                tickers.append(symbol)
            url = data.get('next_url')
            if url:
                if url.startswith("/"):
                    url = f"https://api.polygon.io{url}&apiKey={POLYGON_API_KEY}"
                else:
                    url += f"&apiKey={POLYGON_API_KEY}"
            page += 1
        except Exception as e:
            logging.error(f"Ticker fetch error: {e}")
            break
    return tickers

def get_aggs(symbol, timespan, multiplier, from_ts, to_ts, limit=1000):
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/{multiplier}/{timespan}/{from_ts}/{to_ts}?adjusted=true&sort=asc&limit={limit}&apiKey={POLYGON_API_KEY}"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        return data.get("results", [])
    except Exception as e:
        logging.error(f"get_aggs error: {e}")
        return []

def check_breakout_worker(symbol, now):
    global breakout_alerted_hod
    try:
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start_ts = int(start.timestamp() * 1000)
        end_ts = int(now.timestamp() * 1000)
        candles = get_aggs(symbol, "minute", 1, start_ts, end_ts, limit=1000)
        if not candles or len(candles) < 15:
            return
        prev_high = max(c["h"] for c in candles[:-1])
        last_candle = candles[-1]
        this_hod = round(float(last_candle["h"]), 4)
        today_str = now.strftime('%Y-%m-%d')
        key = (symbol, this_hod, today_str)
        if key in breakout_alerted_hod:
            return  # Already alerted for this HOD today

        if this_hod > prev_high and last_candle["c"] > 2.0:
            avg_vol = sum(c["v"] for c in candles[-11:-1]) / 10
            if last_candle["v"] > 2 * avg_vol:
                open_price = candles[0]["o"]
                price_change = (last_candle["c"] - open_price) / open_price * 100
                if price_change > 3:
                    send_breakout_alert(symbol, last_candle["c"], this_hod, last_candle["v"], price_change, avg_vol)
                    breakout_alerted_hod.add(key)
    except Exception as e:
        logging.error(f"Breakout worker error: {e}")

def breakout_scanner():
    while True:
        daily_reset()
        if is_market_hours():
            tickers = fetch_all_tickers()
            now = datetime.utcnow()
            with ThreadPoolExecutor(max_workers=4) as executor:
                for symbol in tickers:
                    executor.submit(check_breakout_worker, symbol, now)
        time.sleep(2)

if __name__ == "__main__":
    bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="🚨 THIS IS A TEST ALERT. If you see this, Telegram is working.")
    threading.Thread(target=breakout_scanner, daemon=True).start()
    while True:
        time.sleep(60)
