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

    # User-added keywords
    "top line", "significant", "demonstrates", "treatment", "cancer", "primary",
    "positive", "laucnhes", "completes", "beneficial", "breakout", "signs"
]

last_news_ids = set()
news_lock = threading.Lock()

startup_messages = [
    "Lets find some price imbalamce liquidity before the poors even wake up",
    "SECURE THE DAMN BAG",
    "Get in. Get rich. Get out.",
    "Remember, find the setup and ride the wave! Don't let your emotions make you the liquidity.",
    "SCALE OUT OR BAIL OUT!",
    "Make sure you're trading and not donating! NEVER chase, NEVER let FOMO get to you!"
]

market_closed_messages = [
    "🚨 The market is officially closed. Rest up and get ready to secure the bag tomorrow!",
    "🔒 The bell has rung and the casino is closed. See you bright and early!",
    "🛑 Trading day is over! Review your plays, manage your risk, and come back stronger.",
    "📉 Market closed. Don’t let FOMO get you after hours—patience pays!",
    "💤 Closing time! Recharge, reflect, and prepare for tomorrow’s action."
]

def scheduled_startup_and_close_messages():
    last_startup_sent = None
    last_close_sent = None

    closed_rotation = market_closed_messages.copy()
    random.shuffle(closed_rotation)
    closed_index = 0

    while True:
        now_et = datetime.now(EASTERN)
        # Startup message at 3:55am Mon–Fri
        if (now_et.weekday() < 5 and now_et.hour == 3 and now_et.minute == 55 and
                (last_startup_sent is None or last_startup_sent != now_et.date())):
            bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=random.choice(startup_messages))
            last_startup_sent = now_et.date()
        # Market closed message at 8:00pm Mon–Fri, rotating through all messages
        if (now_et.weekday() < 5 and now_et.hour == 20 and now_et.minute == 0 and
                (last_close_sent is None or last_close_sent != now_et.date())):
            bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=closed_rotation[closed_index])
            closed_index += 1
            if closed_index >= len(closed_rotation):
                random.shuffle(closed_rotation)
                closed_index = 0
            last_close_sent = now_et.date()
        time.sleep(5)  # keep this a little higher to avoid 2 messages by accident

def is_market_hours():
    now = datetime.now(EASTERN)
    return now.weekday() < 5 and SCAN_START_HOUR <= now.hour < SCAN_END_HOUR

def send_news_telegram_alert(symbol, headline, keyword):
    confidence = 5
    high_conf_keywords = [
        "fda approval", "acquisition", "merger", "halted", "resumes trading",
        "record high", "guidance raised", "beats estimates"
    ]
    if any(hk in keyword.lower() for hk in high_conf_keywords):
        confidence = 9
    elif keyword.lower() in ["ai", "blockchain", "web3"]:
        confidence = 7
    message = f"📰 ${symbol} — {headline}\n(keyword: {keyword})\nConfidence: {confidence}/10"
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
        alerted_tickers.add(symbol)
    except Exception as e:
        print("Telegram News error:", e)

def send_volume_telegram_alert(symbol, rel_vol, total_vol, avg_vol):
    if rel_vol > 4 or total_vol > 4*avg_vol:
        confidence = 10
    elif rel_vol > 3 or total_vol > 3*avg_vol:
        confidence = 8
    elif rel_vol > 2 or total_vol > 2*avg_vol:
        confidence = 6
    else:
        confidence = 4
    message = f"🚨 ${symbol} — Volume Spike\nConfidence: {confidence}/10"
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
        alerted_tickers.add(symbol)
    except Exception as e:
        print("Telegram volume alert error:", e)

def send_hod_telegram_alert(symbol, price, open_price):
    percent = (price - open_price) / open_price * 100 if open_price else 0
    if percent >= 10:
        confidence = 10
    elif percent >= 5:
        confidence = 8
    else:
        confidence = 6
    message = f"💰🚀 ${symbol} — NEW HIGH OF DAY: {price:.2f}\nConfidence: {confidence}/10"
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

def scheduler_saturday_ebook():
    last_sent_date = None
    while True:
        now = datetime.now(EASTERN)
        if now.weekday() == 5 and now.hour == 12 and last_sent_date != now.date():
            bot.send_message(chat_id=TELEGRAM_CHAT_ID, text='📚 Need more guidance? Grab my ebook "Stock Guide For The Poors."', parse_mode="HTML")
            last_sent_date = now.date()
        time.sleep(60)

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
        start_time = int((now_utc - timedelta(minutes=5)).timestamp() * 1000)
        end_time = int(now_utc.timestamp() * 1000)
        candles = client.get_aggs(
            symbol,
            1,
            "minute",
            from_=start_time,
            to=end_time,
            limit=5
        )
        if not candles or len(candles) < 5:
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
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers()
            now_utc = datetime.utcnow()
            cooldown = 30
            now_ts = time.time()
            with ThreadPoolExecutor(max_workers=512) as executor:  # maximize workers
                futures = [executor.submit(check_volume_spike_worker, symbol, now_utc, cooldown, now_ts) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(0.05)  # scan as fast as feasible

def check_ema_stack_worker(symbol, timeframe="minute", label_5min=False):
    try:
        candles = client.get_aggs(
            symbol,
            1 if timeframe=="minute" else 5,
            "minute",
            from_="now-60m" if timeframe=="minute" else "now-300m",
            to="now",
            limit=30
        )
        if not candles or len(candles) < 21:
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
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers()
            with ThreadPoolExecutor(max_workers=512) as executor:
                futures = [executor.submit(check_ema_stack_worker, symbol, "minute", False) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
                futures = [executor.submit(check_ema_stack_worker, symbol, "5minute", True) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(0.05)

def check_hod_worker(symbol):
    try:
        if symbol not in alerted_tickers:
            return
        now = datetime.now(pytz.UTC)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        start_ts = int(start.timestamp() * 1000)
        end_ts = int(now.timestamp() * 1000)
        candles = client.get_aggs(
            symbol,
            1,
            "minute",
            from_=start_ts,
            to=end_ts,
            limit=1000
        )
        if not candles:
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
    while True:
        if is_market_hours():
            tickers = list(alerted_tickers)
            with ThreadPoolExecutor(max_workers=256) as executor:
                futures = [executor.submit(check_hod_worker, symbol) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(0.05)

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
        time.sleep(0.05)

def check_gap_worker(symbol, seen_today):
    try:
        yest = client.get_aggs(symbol, 1, "day", from_="now-2d", to="now-1d", limit=1)
        today = client.get_aggs(symbol, 1, "day", from_="now-1d", to="now", limit=1)
        if not yest or not today:
            return
        prev = yest[0]
        curr = today[0]
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
    seen_today = set()
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers()
            with ThreadPoolExecutor(max_workers=512) as executor:
                futures = [executor.submit(check_gap_worker, symbol, seen_today) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(0.05)

def check_pm_ah_worker(symbol, seen, now_et, in_premarket, in_ah):
    try:
        yest = client.get_aggs(symbol, 1, "day", from_="now-2d", to="now-1d", limit=1)
        if not yest:
            return
        prev_close = yest[0].c
        trades = client.get_aggs(symbol, 1, "minute", from_="now-60m", to="now", limit=60)
        if not trades:
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
    seen = set()
    while True:
        now_et = datetime.now(EASTERN)
        in_premarket = 4 <= now_et.hour < 9 or (now_et.hour == 9 and now_et.minute < 30)
        in_ah = 16 <= now_et.hour < 20
        if in_premarket or in_ah:
            tickers = fetch_all_tickers()
            with ThreadPoolExecutor(max_workers=512) as executor:
                futures = [executor.submit(check_pm_ah_worker, symbol, seen, now_et, in_premarket, in_ah) for symbol in tickers]
                for _ in as_completed(futures):
                    pass
        time.sleep(0.05)

def run_polygon_news_websocket(keywords):
    url = f"wss://socket.polygon.io/stocks"
    ALLOWED_HALT_CODES = ["T1", "T2", "T3", "H10", "LUDP", "MCB"]
    EXCLUDE_WORDS = ["TEST", "DEMO", "AUCTION"]
    def ws_runner():
        async def listen():
            global last_news_ids
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps({"action":"auth","params":POLYGON_API_KEY}))
                await ws.send(json.dumps({
                    "action":"subscribe",
                    "params":"A.NEWS,A.HALT,A.RESUME"
                }))
                print("Polygon WebSocket news & halts connected.")
                while True:
                    try:
                        resp = await ws.recv()
                        msgs = json.loads(resp)
                        if not isinstance(msgs, list):
                            msgs = [msgs]
                        for msg in msgs:
                            if msg.get("ev") == "A.NEWS":
                                symbol = msg.get("sym", "")
                                headline = msg.get("headline", "").lower()
                                news_id = msg.get("id", "")
                                with news_lock:
                                    if news_id in last_news_ids:
                                        continue
                                    matched = [kw for kw in keywords if kw.lower() in headline]
                                    if matched:
                                        send_news_telegram_alert(
                                            symbol, msg.get("headline", ""), matched[0]
                                        )
                                        last_news_ids.add(news_id)
                                        alerted_tickers.add(symbol)
                            if msg.get("ev") == "A.HALT":
                                symbol = msg.get("sym", "")
                                reason = msg.get("reason", "") or msg.get("tape", "")
                                reason_upper = (reason or "").upper()
                                halt_code = None
                                for code in ALLOWED_HALT_CODES:
                                    if code in reason_upper:
                                        halt_code = code
                                        break
                                confidence = 6
                                if halt_code == "H10":
                                    confidence = 10
                                elif halt_code in ("T1", "T2"):
                                    confidence = 9
                                elif halt_code in ("LUDP", "MCB"):
                                    confidence = 8
                                elif halt_code == "T3":
                                    confidence = 7
                                if halt_code and not any(ex in reason_upper for ex in EXCLUDE_WORDS):
                                    message = f"⏸️ ${symbol} HALTED\nType: {halt_code}\nReason: {reason or 'Unknown'}\nConfidence: {confidence}/10"
                                    bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
                                    alerted_tickers.add(symbol)
                            if msg.get("ev") == "A.RESUME":
                                symbol = msg.get("sym", "")
                                message = f"▶️ ${symbol} RESUMED TRADING\nConfidence: 7/10"
                                bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
                                alerted_tickers.add(symbol)
                    except Exception as e:
                        print("WebSocket err:", e)
                        time.sleep(1)
                        break
        asyncio.run(listen())
    thread = threading.Thread(target=ws_runner, daemon=True)
    thread.start()

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
