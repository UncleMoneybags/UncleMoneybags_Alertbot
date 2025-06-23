import time
import requests
from datetime import datetime, timedelta
import pytz
from telegram import Bot
from polygon import RESTClient
import threading
import asyncio
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
alerted_tickers = set()
hod_tracker = {}

KEYWORDS = [
    "fda approval", "fast track", "phase 1", "phase 2", "phase 3", "orphan drug", "IND submission", "IND clearance",
    "breakthrough therapy", "AI", "AI integration", "blockchain", "cloud contract", "cybersecurity", "machine learning",
    "acquisition", "merger", "SPAC merger", "strategic partnership", "investment from", "contract", "expansion into",
    "guidance raised", "record revenue", "divestiture", "signs deal", "launches platform", "awarded", "accepting crypto",
    "bitcoin", "viral", "buyback", "oral treatment", "preclinical trial", "DOD contract", "china approval", "plans to acquire",
    "partnership", "emergency use authorization", "CRL", "NDA submission", "NDA approval", "BLA submission", "BLA approval",
    "PDUFA", "clinical hold", "data readout", "positive topline data", "statistically significant", "beats estimates",
    "misses estimates", "EPS", "profit warning", "short squeeze", "analyst upgrade", "analyst downgrade",
    "price target increased", "price target lowered", "joint venture", "spin-off", "reverse split", "IPO", "uplisting",
    "downlisting", "NFT", "web3", "data breach", "ransomware", "AI chip", "quantum computing", "SEC investigation",
    "settlement", "class action", "momentum", "record volume", "record high", "ATH", "halted", "resumes trading",
    "top line", "significant", "demonstrates", "treatment", "cancer", "primary",
    "positive", "laucnhes", "completes", "beneficial", "breakout", "signs"
]

last_news_ids = set()
news_lock = threading.Lock()

def is_market_hours():
    now = datetime.now(EASTERN)
    return now.weekday() < 5 and SCAN_START_HOUR <= now.hour < SCAN_END_HOUR

def send_news_telegram_alert(symbol, headline, keyword):
    message = f"📰 ${symbol} — {headline}\n(keyword: {keyword})"
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
        alerted_tickers.add(symbol)
    except Exception as e:
        print("Telegram News error:", e)

def send_volume_telegram_alert(symbol, rel_vol, total_vol, avg_vol):
    message = f"🚨 ${symbol} — Volume Spike\nRel: {rel_vol:.2f} Tot: {total_vol} Avg: {avg_vol}"
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
        alerted_tickers.add(symbol)
    except Exception as e:
        print("Telegram volume alert error:", e)

def send_hod_telegram_alert(symbol, price, open_price):
    percent = (price - open_price) / open_price * 100 if open_price else 0
    message = f"💰🚀 ${symbol} — NEW HIGH OF DAY: {price:.2f} ({percent:.2f}%)"
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
    except Exception as e:
        print("Telegram HOD alert error:", e)

def send_ema_stack_alert(symbol, price, timeframe, confidence):
    message = f"📈🔥 ${symbol} — EMA STACK ({timeframe}): 8 > 13 > 21\nCurrent Price: {price:.2f}\nConfidence: {confidence}/10"
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
    except Exception as e:
        print("Telegram EMA stack alert error:", e)

def fetch_all_tickers():
    # Scan all US listed stocks for price < $5
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit=1000&apiKey={POLYGON_API_KEY}"
    tickers = []
    next_url = url
    while next_url:
        resp = requests.get(next_url)
        data = resp.json()
        results = data.get('results', [])
        for item in results:
            symbol = item.get('ticker')
            if symbol:
                # Get last price for the symbol
                try:
                    aggs = client.get_aggs(symbol, 1, "day", limit=1)
                    if aggs and isinstance(aggs, list):
                        last_price = aggs[-1].close
                        if last_price is not None and last_price < 5:
                            tickers.append(symbol)
                except Exception:
                    continue
        next_url = data.get('next_url')
        if next_url:
            # Polygon returns a partial next_url, so add the domain if needed
            if next_url.startswith("/"):
                next_url = f"https://api.polygon.io{next_url}&apiKey={POLYGON_API_KEY}"
            else:
                next_url += f"&apiKey={POLYGON_API_KEY}"
    return tickers

def check_volume_spike_worker(symbol, now_utc, cooldown, now_ts):
    try:
        end_time = int(now_utc.timestamp() * 1000)
        start_time = int((now_utc - timedelta(minutes=5)).timestamp() * 1000)
        try:
            candles = client.get_aggs(symbol, 1, "minute", from_=start_time, to=end_time, limit=5)
        except Exception:
            candles = []
        if not candles or not isinstance(candles, list) or len(candles) < 5:
            return
        avg_vol = sum(candle.volume for candle in candles[:-1]) / len(candles[:-1])
        if avg_vol < 20000:     # Lowered for pennies
            return
        total_vol = sum(candle.volume for candle in candles)
        if total_vol < 50000:  # Lowered for pennies
            return
        rel_vol = total_vol / avg_vol if avg_vol > 0 else 0
        if (rel_vol >= 1.5 or total_vol >= 1.5 * avg_vol) and now_ts - last_alert_time.get(symbol, 0) > cooldown:
            send_volume_telegram_alert(symbol, rel_vol, total_vol, avg_vol)
            last_alert_time[symbol] = now_ts
    except Exception as e:
        print(f"Volume spike error for {symbol}: {e}")

def volume_spike_scanner():
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers()
            now_utc = datetime.utcnow()
            cooldown = 60
            now_ts = time.time()
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(check_volume_spike_worker, symbol, now_utc, cooldown, now_ts) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(2)

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
        closes = [candle.close for candle in candles]
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
        avg_vol = sum(candle.volume for candle in candles[-10:]) / 10
        if last_candle.volume < avg_vol or last_candle.volume < 20000:  # Lowered
            return
        price_change = (closes[-1] - closes[0]) / closes[0] * 100
        if abs(price_change) < 2:  # Lowered for pennies
            return
        confidence = 10 if price_change > 5 else 8 if price_change > 3 else 6
        label = "5-min" if label_5min else ("1-min" if timeframe=="minute" else timeframe)
        send_ema_stack_alert(symbol, closes[-1], label, confidence)
    except Exception as e:
        print(f"EMA stack error for {symbol}: {e}")

def ema_stack_scanner():
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers()
            with ThreadPoolExecutor(max_workers=8) as executor:
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
        try:
            candles = client.get_aggs(symbol, 1, "minute", from_=start_ts, to=end_ts, limit=1000)
        except Exception:
            candles = []
        if not candles or not isinstance(candles, list) or not candles:
            return
        hod = max(candle.high for candle in candles)
        prev_hod = hod_tracker.get(symbol, None)
        open_price = candles[0].open if candles else 0
        if prev_hod is None or hod > prev_hod:
            if prev_hod is not None:
                send_hod_telegram_alert(symbol, hod, open_price)
            hod_tracker[symbol] = hod
    except Exception as e:
        print(f"HOD check error for {symbol}: {e}")

def hod_scanner():
    while True:
        if is_market_hours():
            tickers = list(alerted_tickers)
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(check_hod_worker, symbol) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(2)

async def async_scan_news_and_alert_parallel(tickers, keywords):
    import aiohttp
    async def fetch_news(session, symbol):
        try:
            url = f"https://api.polygon.io/v2/reference/news?ticker={symbol}&limit=3&apiKey={POLYGON_API_KEY}"
            async with session.get(url, timeout=8) as r:
                data = await r.json()
            if "error" in data or (data.get("status") == "ERROR"):
                print(f"News API error for {symbol}: {data}")
            return symbol, data.get("results", [])
        except Exception as e:
            print(f"News error {symbol}: {repr(e)}")
            return symbol, []
    tasks = []
    async with aiohttp.ClientSession() as session:
        for symbol in tickers:
            tasks.append(fetch_news(session, symbol))
        for future in asyncio.as_completed(tasks):
            symbol, news_items = await future
            for news in news_items:
                headline = news.get("title", "").lower()
                news_id = news.get("id", "")
                with news_lock:
                    if news_id in last_news_ids:
                        continue
                    matched = [kw for kw in keywords if kw.lower() in headline]
                    if matched:
                        send_news_telegram_alert(symbol, news.get("title", ""), matched[0])
                        last_news_ids.add(news_id)

def news_polling_scanner():
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers()
            asyncio.run(async_scan_news_and_alert_parallel(tickers, KEYWORDS))
        time.sleep(15)

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
        gap = (curr.open - prev.close) / prev.close * 100
        key = f"{symbol}|{curr.open}|{prev.close}"
        if abs(gap) >= 5 and key not in seen_today:  # Penny stocks can gap big
            direction = "Gap Up" if gap > 0 else "Gap Down"
            message = f"🚀 {direction}: ${symbol} opened {gap:.1f}% {'higher' if gap > 0 else 'lower'}"
            bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)
            seen_today.add(key)
    except Exception as e:
        print(f"Gap scanner error for {symbol}: {e}")

def gap_scanner():
    seen_today = set()
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers()
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(check_gap_worker, symbol, seen_today) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(15)

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
        prev_close = yest[0].close
        now = datetime.utcnow()
        end_time = int(now.timestamp() * 1000)
        start_time = int((now - timedelta(minutes=60)).timestamp() * 1000)
        try:
            trades = client.get_aggs(symbol, 1, "minute", from_=start_time, to=end_time, limit=60)
        except Exception:
            trades = []
        if not trades or not isinstance(trades, list) or not trades:
            return
        last = trades[-1].close
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
    seen = set()
    while True:
        now_et = datetime.now(EASTERN)
        in_premarket = 4 <= now_et.hour < 9 or (now_et.hour == 9 and now_et.minute < 30)
        in_ah = 16 <= now_et.hour < 20
        if in_premarket or in_ah:
            tickers = fetch_all_tickers()
            with ThreadPoolExecutor(max_workers=8) as executor:
                futures = [executor.submit(check_pm_ah_worker, symbol, seen, now_et, in_premarket, in_ah) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(15)

if __name__ == "__main__":
    bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="🚨 THIS IS A TEST ALERT. If you see this, Telegram is working.")
    threading.Thread(target=volume_spike_scanner, daemon=True).start()
    threading.Thread(target=ema_stack_scanner, daemon=True).start()
    threading.Thread(target=hod_scanner, daemon=True).start()
    threading.Thread(target=news_polling_scanner, daemon=True).start()
    threading.Thread(target=gap_scanner, daemon=True).start()
    threading.Thread(target=premarket_ah_mover_scanner, daemon=True).start()
    while True:
        time.sleep(60)
