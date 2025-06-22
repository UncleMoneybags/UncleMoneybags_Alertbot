import time
import requests
from datetime import datetime, timedelta
import pytz
from telegram import Bot
from polygon import RESTClient  # Requires: pip install polygon-api-client
import threading
import asyncio
import websockets
import json

# === CONFIG ===
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
bot = Bot(token=TELEGRAM_BOT_TOKEN)
client = RESTClient(api_key=POLYGON_API_KEY)

EASTERN = pytz.timezone('US/Eastern')
SCAN_START_HOUR = 4  # 4:00 AM ET
SCAN_END_HOUR = 20   # 8:00 PM ET

last_alert_time = {}
last_message_time = time.time()

# Track tickers that have been alerted (for HOD alerts)
alerted_tickers = set()
hod_tracker = {}

KEYWORDS = [
    # User-supplied keywords
    "fda approval",
    "fast track",
    "phase 1",
    "phase 2",
    "phase 3",
    "orphan drug",
    "IND submission",
    "IND clearance",
    "breakthrough therapy",
    "AI",
    "AI integration",
    "blockchain",
    "cloud contract",
    "cybersecurity",
    "machine learning",
    "acquisition",
    "merger",
    "SPAC merger",
    "strategic partnership",
    "investment from",
    "contract",
    "expansion into",
    "guidance raised",
    "record revenue",
    "divestiture",
    "signs deal",
    "launches platform",
    "awarded",
    "accepting crypto",
    "bitcoin",
    "viral",
    "buyback",
    "oral treatment",
    "preclinical trial",
    "DOD contract",
    "china approval",
    "plans to acquire",
    "partnership",
    "emergency use authorization",
    # Additional hot stock keywords (biotech/healthcare)
    "CRL",
    "NDA submission",
    "NDA approval",
    "BLA submission",
    "BLA approval",
    "PDUFA",
    "clinical hold",
    "data readout",
    "positive topline data",
    "statistically significant",
    # Earnings & guidance
    "beats estimates",
    "misses estimates",
    "EPS",
    "profit warning",
    # Market activity
    "short squeeze",
    "analyst upgrade",
    "analyst downgrade",
    "price target increased",
    "price target lowered",
    # Partnerships/corporate
    "joint venture",
    "spin-off",
    "reverse split",
    "IPO",
    "uplisting",
    "downlisting",
    # Crypto/AI/tech
    "NFT",
    "web3",
    "data breach",
    "ransomware",
    "AI chip",
    "quantum computing",
    # Regulatory
    "SEC investigation",
    "settlement",
    "class action",
    # Other momentum
    "momentum",
    "record volume",
    "record high",
    "ATH",
    "halted",
    "resumes trading"
]

last_news_ids = set()
news_lock = threading.Lock()  # For thread-safe news id tracking

def is_market_hours():
    # Only scan between 4:00 AM and 8:00 PM Eastern Time, Monday–Friday
    now = datetime.now(EASTERN)
    return now.weekday() < 5 and SCAN_START_HOUR <= now.hour < SCAN_END_HOUR

def sleep_until_market_open():
    now = datetime.now(EASTERN)
    if now.hour >= SCAN_END_HOUR or now.weekday() >= 5:
        days_ahead = (7 - now.weekday()) % 7
        if now.hour >= SCAN_END_HOUR or now.weekday() == 4:
            days_ahead = (7 - now.weekday() + 0) % 7
            if days_ahead == 0:
                days_ahead = 7
        else:
            days_ahead = 1
        next_open = (now + timedelta(days=days_ahead)).replace(hour=SCAN_START_HOUR, minute=0, second=0, microsecond=0)
    else:
        next_open = now.replace(hour=SCAN_START_HOUR, minute=0, second=0, microsecond=0)
        if now > next_open:
            next_open += timedelta(days=1)
    sleep_seconds = (next_open - now).total_seconds()
    print(f"Sleeping {int(sleep_seconds)} seconds until next market open at {next_open}")
    time.sleep(sleep_seconds)

def get_float_from_polygon(symbol, retries=3):
    url = f"https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={POLYGON_API_KEY}"
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url)
            data = r.json()
            return data["results"].get("share_class_shares_outstanding", 5_000_000)
        except Exception as e:
            print(f"Float lookup failed for {symbol}, attempt {attempt}: {e}")
            if hasattr(r, 'text'):
                print("Polygon response:", r.text)
            if attempt < retries:
                time.sleep(2 ** attempt)
    return 5_000_000

def send_news_telegram_alert(symbol, headline, keyword):
    confidence = 5  # default
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

def send_saturday_ebook_alert():
    message = '📚 Need more guidance? Grab my ebook "Stock Guide For The Poors."'
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
    except Exception as e:
        print("Telegram Saturday ebook alert error:", e)

def scheduler_saturday_ebook():
    last_sent_date = None
    while True:
        now = datetime.now(EASTERN)
        if now.weekday() == 5 and now.hour == 12 and last_sent_date != now.date():
            send_saturday_ebook_alert()
            last_sent_date = now.date()
        time.sleep(60)  # Check once a minute

def check_ema_stack(tickers, timeframe="minute", label_5min=False):
    for symbol in tickers:
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
                continue

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
                continue

            last_candle = candles[-1]
            avg_vol = sum(c.v for c in candles[-10:]) / 10
            if last_candle.v < avg_vol or last_candle.v < 100000:
                continue

            price_change = (closes[-1] - closes[0]) / closes[0] * 100
            if abs(price_change) < 2:
                continue

            confidence = 10 if price_change > 5 else 8 if price_change > 3 else 6
            label = "5-min" if label_5min else ("1-min" if timeframe=="minute" else timeframe)
            send_ema_stack_alert(symbol, closes[-1], label, confidence)
        except Exception as e:
            print(f"EMA stack error for {symbol}: {e}")

def check_high_of_day(symbols):
    global hod_tracker, alerted_tickers
    for symbol in symbols:
        if symbol not in alerted_tickers:
            continue
        try:
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
                continue
            hod = max(c.h for c in candles)
            prev_hod = hod_tracker.get(symbol, None)
            open_price = candles[0].o if candles else 0
            if prev_hod is None or hod > prev_hod:
                if prev_hod is not None:
                    send_hod_telegram_alert(symbol, hod, open_price)
                hod_tracker[symbol] = hod
        except Exception as e:
            print(f"HOD check error for {symbol}: {e}")

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

def check_volume_spikes(tickers):
    global last_message_time
    now_utc = datetime.utcnow()
    start_time = int((now_utc - timedelta(minutes=5)).timestamp() * 1000)
    end_time = int(now_utc.timestamp() * 1000)
    cooldown = 300  # 5 minutes
    now_ts = time.time()

    scanned = 0
    max_scans = 1  # Only scan 1 ticker per loop

    global last_alert_time
    if not isinstance(last_alert_time, dict):
        last_alert_time = {}

    for symbol in tickers:
        if scanned >= max_scans:
            break

        candles = []
        for attempt in range(1, 4):
            try:
                time.sleep(15)
                candles = client.get_aggs(
                    symbol,
                    1,
                    "minute",
                    from_=start_time,
                    to=end_time,
                    limit=5
                )
                break
            except Exception as e:
                err_msg = str(e)
                if 'NOT_AUTHORIZED' in err_msg or 'Your plan doesn\'t include this data timeframe' in err_msg:
                    print(f"Skipping {symbol}: {err_msg}")
                    candles = []
                    break
                print(f"Error scanning {symbol}, attempt {attempt}: {err_msg}")
                if attempt < 3:
                    time.sleep(2 ** attempt)
                else:
                    candles = []
        if not candles or len(candles) < 5:
            continue

        avg_vol = sum(c.v for c in candles[:-1]) / len(candles[:-1])
        if avg_vol < 100000:
            continue

        total_vol = sum(c.v for c in candles)
        if total_vol < 500000:
            continue

        rel_vol = total_vol / avg_vol if avg_vol > 0 else 0

        if (rel_vol >= 2.0 or total_vol >= 2 * avg_vol) and now_ts - last_alert_time.get(symbol, 0) > cooldown:
            send_volume_telegram_alert(symbol, rel_vol, total_vol, avg_vol)
            last_alert_time[symbol] = now_ts
            last_message_time = now_ts

        scanned += 1

async def async_scan_news_and_alert(tickers, keywords):
    global last_news_ids
    async def fetch_news(session, symbol):
        try:
            url = f"https://api.polygon.io/v2/reference/news?ticker={symbol}&limit=3&apiKey={POLYGON_API_KEY}"
            async with session.get(url, timeout=8) as r:
                data = await r.json()
            return symbol, data.get("results", [])
        except Exception as e:
            print(f"News error {symbol}: {e}")
            return symbol, []

    import aiohttp
    tasks = []
    async with aiohttp.ClientSession() as session:
        for symbol in tickers:
            tasks.append(fetch_news(session, symbol))
        results = await asyncio.gather(*tasks)
        for symbol, news_items in results:
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
                        time.sleep(5)
                        break
        asyncio.run(listen())
    thread = threading.Thread(target=ws_runner, daemon=True)
    thread.start()

# --- Independent scanner threads ---

def volume_spike_scanner():
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers()
            check_volume_spikes(tickers)
        time.sleep(90)

def ema_stack_scanner():
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers()
            # 1-min EMA stack
            check_ema_stack(tickers, timeframe="minute")
            # 5-min EMA stack with label
            check_ema_stack(tickers, timeframe="5minute", label_5min=True)
        time.sleep(90)

def hod_scanner():
    while True:
        if is_market_hours():
            check_high_of_day(list(alerted_tickers))
        time.sleep(90)

def news_polling_scanner():
    while True:
        if is_market_hours():
            tickers = fetch_all_tickers()
            asyncio.run(async_scan_news_and_alert(tickers, KEYWORDS))
        time.sleep(180)

if __name__ == "__main__":
    bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="lets find some bangers!")
    threading.Thread(target=scheduler_saturday_ebook, daemon=True).start()
    threading.Thread(target=volume_spike_scanner, daemon=True).start()
    threading.Thread(target=ema_stack_scanner, daemon=True).start()
    threading.Thread(target=hod_scanner, daemon=True).start()
    threading.Thread(target=news_polling_scanner, daemon=True).start()
    run_polygon_news_websocket(KEYWORDS)
    while True:
        time.sleep(60)
