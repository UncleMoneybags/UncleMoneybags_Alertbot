import time
import requests
from datetime import datetime, timedelta
import pytz
from telegram import Bot
from polygon import RESTClient
import threading
import asyncio
import websockets
import json
import random
from concurrent.futures import ThreadPoolExecutor, as_completed

# === CONFIG ===
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
bot = Bot(token=TELEGRAM_BOT_TOKEN)
client = RESTClient(api_key=POLYGON_API_KEY)

EASTERN = pytz.timezone('US/Eastern')
SCAN_START_HOUR = 4
SCAN_END_HOUR = 20

last_alert_time = {}
last_message_time = time.time()
alerted_tickers = set()
hod_tracker = {}

KEYWORDS = [
    # ... (omitted for brevity, keep all your keywords here)
    "top line", "significant", "demonstrates", "treatment", "cancer", "primary",
    "positive", "laucnhes", "completes", "beneficial", "breakout", "signs"
]

last_news_ids = set()
news_lock = threading.Lock()

startup_messages = [
    # ... (omitted for brevity)
]

market_closed_messages = [
    # ... (omitted for brevity)
]

def scheduled_startup_and_close_messages():
    # ... (as before)
    pass

def is_market_hours():
    now = datetime.now(EASTERN)
    return now.weekday() < 5 and SCAN_START_HOUR <= now.hour < SCAN_END_HOUR

def send_news_telegram_alert(symbol, headline, keyword):
    # ... (as before)
    pass

def send_volume_telegram_alert(symbol, rel_vol, total_vol, avg_vol):
    # ... (as before)
    pass

def send_hod_telegram_alert(symbol, price, open_price):
    # ... (as before)
    pass

def send_ema_stack_alert(symbol, price, timeframe, confidence):
    # ... (as before)
    pass

def scheduler_saturday_ebook():
    # ... (as before)
    pass

def fetch_all_tickers():
    try:
        url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit=1000&apiKey={POLYGON_API_KEY}"
        r = requests.get(url)
        data = r.json()
        if "results" not in data:
            raise ValueError("No results key in Polygon response")
        return [item["ticker"] for item in data["results"] if item.get("primary_exchange") in ["XNYS", "XNAS"]]
    except Exception as e:
        print("Error fetching tickers:", e)
        return []

def check_volume_spike_worker(symbol, now_utc, cooldown, now_ts):
    try:
        end_time = int(now_utc.timestamp() * 1000)
        start_time = int((now_utc - timedelta(minutes=5)).timestamp() * 1000)
        try:
            candles = client.get_aggs(
                symbol,
                1,
                "minute",
                from_=start_time,
                to=end_time,
                limit=5
            )
        except Exception:
            candles = []
        if not candles or not isinstance(candles, list) or len(candles) < 5:
            return
        avg_vol = sum(c.v for c in candles[:-1]) / len(candles[:-1])
        if avg_vol < 100000:
            return
        total_vol = sum(c.v for c in candles)
        if total_vol < 500000:
            return
        rel_vol = total_vol / avg_vol if avg_vol > 0 else 0
        if (rel_vol >= 2.0 or total_vol >= 2 * avg_vol) and now_ts - last_alert_time.get(symbol, 0) > cooldown:
            send_volume_telegram_alert(symbol, rel_vol, total_vol, avg_vol)
            last_alert_time[symbol] = now_ts
    except Exception as e:
        print(f"Volume spike error for {symbol}: {e}")

def volume_spike_scanner():
    # ... (as before)
    pass

def check_ema_stack_worker(symbol, timeframe="minute", label_5min=False):
    try:
        now = datetime.utcnow()
        if timeframe == "minute":
            end_time = int(now.timestamp() * 1000)
            start_time = int((now - timedelta(minutes=60)).timestamp() * 1000)
            try:
                candles = client.get_aggs(symbol, 1, "minute", from_=start_time, to=end_time, limit=30)
            except Exception:
                candles = []
        else:
            end_time = int(now.timestamp() * 1000)
            start_time = int((now - timedelta(minutes=300)).timestamp() * 1000)
            try:
                candles = client.get_aggs(symbol, 5, "minute", from_=start_time, to=end_time, limit=30)
            except Exception:
                candles = []
        if not candles or not isinstance(candles, list) or len(candles) < 21:
            return
        closes = [c.c for c in candles]
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
        avg_vol = sum(c.v for c in candles[-10:]) / 10
        if last_candle.v < avg_vol or last_candle.v < 100000:
            return
        price_change = (closes[-1] - closes[0]) / closes[0] * 100
        if abs(price_change) < 2:
            return
        confidence = 10 if price_change > 5 else 8 if price_change > 3 else 6
        label = "5-min" if label_5min else ("1-min" if timeframe=="minute" else timeframe)
        send_ema_stack_alert(symbol, closes[-1], label, confidence)
    except Exception as e:
        print(f"EMA stack error for {symbol}: {e}")

def ema_stack_scanner():
    # ... (as before)
    pass

def check_hod_worker(symbol):
    try:
        if symbol not in alerted_tickers:
            return
        now = datetime.utcnow()
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start_ts = int(start.timestamp() * 1000)
        end_ts = int(now.timestamp() * 1000)
        try:
            candles = client.get_aggs(
                symbol,
                1,
                "minute",
                from_=start_ts,
                to=end_ts,
                limit=1000
            )
        except Exception:
            candles = []
        if not candles or not isinstance(candles, list) or not candles:
            return
        hod = max(c.h for c in candles)
        prev_hod = hod_tracker.get(symbol, None)
        open_price = candles[0].o if candles else 0
        if prev_hod is None or hod > prev_hod:
            if prev_hod is not None:
                send_hod_telegram_alert(symbol, hod, open_price)
            hod_tracker[symbol] = hod
    except Exception as e:
        print(f"HOD check error for {symbol}: {e}")

def hod_scanner():
    # ... (as before)
    pass

async def async_scan_news_and_alert_parallel(tickers, keywords):
    import aiohttp
    async def fetch_news(session, symbol):
        try:
            url = f"https://api.polygon.io/v2/reference/news?ticker={symbol}&limit=3&apiKey={POLYGON_API_KEY}"
            async with session.get(url, timeout=8) as r:
                data = await r.json()
            return symbol, data.get("results", [])
        except Exception as e:
            print(f"News error {symbol}: {e}")
            return symbol, []
    # ... (as before)
    pass

def news_polling_scanner():
    # ... (as before)
    pass

def check_gap_worker(symbol, seen_today):
    try:
        today = datetime.utcnow().date()
        yesterday = today - timedelta(days=1)
        try:
            yest = client.get_aggs(symbol, 1, "day", from_=str(yesterday), to=str(yesterday), limit=1)
        except Exception:
            yest = []
        try:
            today_agg = client.get_aggs(symbol, 1, "day", from_=str(today), to=str(today), limit=1)
        except Exception:
            today_agg = []
        if not yest or not isinstance(yest, list) or not yest:
            return
        if not today_agg or not isinstance(today_agg, list) or not today_agg:
            return
        prev = yest[0]
        curr = today_agg[0]
        gap = (curr.o - prev.c) / prev.c * 100
        key = f"{symbol}|{curr.o}|{prev.c}"
        if abs(gap) >= 5 and key not in seen_today:
            direction = "Gap Up" if gap > 0 else "Gap Down"
            message = f"🚀 {direction}: ${symbol} opened {gap:.1f}% {'higher' if gap > 0 else 'lower'}"
            bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
            seen_today.add(key)
    except Exception as e:
        print(f"Gap scanner error for {symbol}: {e}")

def gap_scanner():
    # ... (as before)
    pass

def check_pm_ah_worker(symbol, seen, now_et, in_premarket, in_ah):
    try:
        today = datetime.utcnow().date()
        yesterday = today - timedelta(days=1)
        try:
            yest = client.get_aggs(symbol, 1, "day", from_=str(yesterday), to=str(yesterday), limit=1)
        except Exception:
            yest = []
        if not yest or not isinstance(yest, list) or not yest:
            return
        prev_close = yest[0].c
        now = datetime.utcnow()
        end_time = int(now.timestamp() * 1000)
        start_time = int((now - timedelta(minutes=60)).timestamp() * 1000)
        try:
            trades = client.get_aggs(symbol, 1, "minute", from_=start_time, to=end_time, limit=60)
        except Exception:
            trades = []
        if not trades or not isinstance(trades, list) or not trades:
            return
        last = trades[-1].c
        move = (last - prev_close) / prev_close * 100
        key = f"{symbol}|{now_et.date()}|{last}"
        if abs(move) >= 5 and key not in seen:
            session = "Premarket" if in_premarket else "After-hours"
            message = f"⚡ {session} Mover: ${symbol} is up {move:.1f}%"
            bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
            seen.add(key)
    except Exception as e:
        print(f"Premarket/AH scanner error for {symbol}: {e}")

def premarket_ah_mover_scanner():
    # ... (as before)
    pass

def run_polygon_news_websocket(keywords):
    # ... (as before)
    pass

if __name__ == "__main__":
    threading.Thread(target=scheduled_startup_and_close_messages, daemon=True).start()
    threading.Thread(target=scheduler_saturday_ebook, daemon=True).start()
    threading.Thread(target=volume_spike_scanner, daemon=True).start()
    threading.Thread(target=ema_stack_scanner, daemon=True).start()
    threading.Thread(target=hod_scanner, daemon=True).start()
    threading.Thread(target=news_polling_scanner, daemon=True).start()
    threading.Thread(target=gap_scanner, daemon=True).start()
    threading.Thread(target=premarket_ah_mover_scanner, daemon=True).start()
    run_polygon_news_websocket(KEYWORDS)
    while True:
        time.sleep(60)
