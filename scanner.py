import asyncio
import websockets
import aiohttp
import json
import html
from collections import defaultdict, deque
from datetime import datetime, timedelta, time
import pytz
import signal

# --- CONFIG ---
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"

# Only alert for stocks $10 or less
PRICE_THRESHOLD = 10.00

# Broadened news keywords
NEWS_KEYWORDS = [
    "AI", "artificial intelligence", "Federal Reserve", "interest rate", "Big Tech", "earnings",
    "Apple", "Microsoft", "Google", "Amazon", "Nvidia", "Meta", "EV", "electric vehicle", "Tesla",
    "semiconductor", "chip", "biotech", "FDA", "approval", "IPO", "merger", "acquisition", "China",
    "trade", "oil", "energy", "renewable", "meme stock", "retail investor", "cybersecurity", "hack",
    "breach", "REIT", "real estate", "crypto", "Bitcoin", "ETF", "bank", "financial", "airline",
    "travel", "healthcare", "innovation", "stimulus", "infrastructure", "ESG", "green energy",
    "consumer spending", "retail", "inflation", "cost of living", "strike", "union", "antitrust",
    "regulation", "streaming", "Netflix", "Disney", "election",
    # Biotech/clinical/health-related
    "trial", "results", "patients", "clinical", "top-line", "data", "study", "endpoint", "significance",
    "phase", "enrollment", "dose", "primary", "secondary", "summit", "approval", "efficacy", "safety",
    "treatment", "response", "cohort", "interim", "complete response", "partial response", "disease",
    "progression", "biomarker", "mutation", "oncology", "cancer", "therapy"
]

recent_news = defaultdict(lambda: deque(maxlen=20))
NEWS_WINDOW = timedelta(minutes=10)

def is_market_scan_time():
    ny = pytz.timezone("America/New_York")
    now_ny = datetime.now(ny)
    if now_ny.weekday() >= 5:
        return False  # Saturday or Sunday
    scan_start = time(4, 0)
    scan_end = time(20, 0)
    return scan_start <= now_ny.time() <= scan_end

async def send_telegram_async(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": False
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(url, data=payload, timeout=10) as resp:
                if resp.status != 200:
                    print("Telegram send error:", await resp.text())
        except Exception as e:
            print(f"Telegram send error: {e}")

def escape_html(s):
    return html.escape(s or "")

def news_pretty_alert(symbol, headline, summary, url):
    symbol = escape_html(symbol)
    headline = escape_html(headline)
    summary = escape_html(summary)
    url = escape_html(url)
    msg = (
        f"📰 <b>${symbol}</b> News Alert\n"
        f"<b>{headline}</b>\n"
        f"{summary}\n"
        f"<a href='{url}'>Read more</a>"
    )
    return msg

def price_pretty_alert(symbol, event_type, price, event_time):
    symbol = escape_html(symbol)
    event_type = escape_html(event_type)
    msg = (
        f"🚨 <b>{event_type}</b>\n"
        f"Symbol: <b>${symbol}</b>\n"
        f"Price: <b>{price}</b>\n"
        f"Time: {event_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )
    return msg

def news_contains_keywords(headline, summary):
    text = (headline or "") + " " + (summary or "")
    text = text.lower()
    for kw in NEWS_KEYWORDS:
        if kw.lower() in text:
            return True
    return False

def on_news_event(symbol, headline, summary, url, news_time):
    recent_news[symbol].append((news_time, headline, summary, url))

def scan_recent_news(symbol, event_time):
    alerts = []
    for news_time, headline, summary, url in recent_news[symbol]:
        if abs((event_time - news_time).total_seconds()) <= NEWS_WINDOW.total_seconds():
            if news_contains_keywords(headline, summary):
                alerts.append((headline, summary, url))
    return alerts

async def on_price_alert(symbol, event_type, price, event_time):
    if not is_market_scan_time():
        return
    await send_telegram_async(price_pretty_alert(symbol, event_type, price, event_time))
    news_found = scan_recent_news(symbol, event_time)
    for headline, summary, url in news_found:
        msg = news_pretty_alert(symbol, headline, summary, url)
        await send_telegram_async(msg)

class Candle:
    def __init__(self, open_, high, low, close, volume, start_time):
        self.open = open_
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.start_time = start_time

candles = defaultdict(lambda: deque(maxlen=4))

async def on_new_candle(symbol, open_, high, low, close, volume, start_time):
    if not is_market_scan_time():
        return

    # Only alert on price/volume spikes for stocks <= $10
    if close > PRICE_THRESHOLD:
        return

    candles[symbol].append(Candle(open_, high, low, close, volume, start_time))

    if len(candles[symbol]) < 4:
        return

    c = candles[symbol]
    price_3ago = c[0].close
    price_now = c[-1].close
    price_diff = price_now - price_3ago
    if price_diff >= 0.20:
        await send_telegram_async(
            f"🚨 <b>{escape_html(symbol)}</b> stock price up ${price_diff:.2f} over last 3 min candles.\n"
            f"From ${price_3ago:.2f} to ${price_now:.2f}."
        )
        news_found = scan_recent_news(symbol, c[-1].start_time)
        for headline, summary, url in news_found:
            await send_telegram_async(news_pretty_alert(symbol, headline, summary, url))

    if c[0].volume < c[1].volume < c[2].volume:
        await send_telegram_async(
            f"🚨 <b>{escape_html(symbol)}</b> stock volume spike!\n"
            f"Volumes: {c[0].volume:,} < {c[1].volume:,} < {c[2].volume:,} (last 3 mins)\n"
            f"Current Price: ${c[2].close:.2f}"
        )
        news_found = scan_recent_news(symbol, c[-1].start_time)
        for headline, summary, url in news_found:
            await send_telegram_async(news_pretty_alert(symbol, headline, summary, url))

async def ws_connect_loop(ws_handler, name):
    delay = 1
    while True:
        try:
            await ws_handler()
            print(f"{name} WebSocket ended, reconnecting...")
        except Exception as e:
            print(f"{name} WebSocket error: {e}. Reconnecting in {delay}s...")
        await asyncio.sleep(delay)
        delay = min(delay * 2, 60)  # Exponential backoff up to 60s

# --- ASYNC Polygon News Polling (REST API, not WebSocket) ---
async def news_rest_poll():
    seen_ids = set()
    url = f"https://api.polygon.io/v2/reference/news?limit=50&apiKey={POLYGON_API_KEY}"
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    data = await resp.json()
                    results = data.get("results", [])
                    for news in results:
                        news_id = news["id"]
                        if news_id not in seen_ids:
                            seen_ids.add(news_id)
                            symbols = news.get("symbols") or []
                            headline = news.get("title", "")
                            summary = news.get("description", "")
                            url_ = news.get("article_url", "")
                            news_time = None
                            try:
                                news_time = datetime.strptime(news["published_utc"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=pytz.UTC)
                            except Exception:
                                news_time = datetime.utcnow().replace(tzinfo=pytz.UTC)
                            # If no symbols, still send alert with "MARKET"
                            if not symbols:
                                symbols = ["MARKET"]
                            for symbol in symbols:
                                on_news_event(symbol, headline, summary, url_, news_time)
                                msg = news_pretty_alert(symbol, headline, summary, url_)
                                await send_telegram_async(msg)
            await asyncio.sleep(30)
        except Exception as e:
            print("News polling error:", e)
            await asyncio.sleep(60)

TRADE_CANDLE_INTERVAL = timedelta(minutes=1)
trade_candle_builders = defaultdict(list)
trade_candle_last_time = {}

async def on_trade_event(symbol, price, size, trade_time):
    candle_time = trade_time.replace(second=0, microsecond=0)
    last_time = trade_candle_last_time.get(symbol)
    if last_time and candle_time != last_time:
        trades = trade_candle_builders[symbol]
        if trades:
            prices = [t[0] for t in trades]
            volumes = [t[1] for t in trades]
            open_ = prices[0]
            close = prices[-1]
            high = max(prices)
            low = min(prices)
            volume = sum(volumes)
            await on_new_candle(symbol, open_, high, low, close, volume, last_time)
        trade_candle_builders[symbol] = []
    trade_candle_builders[symbol].append((price, size))
    trade_candle_last_time[symbol] = candle_time

async def trade_ws():
    uri = "wss://socket.polygon.io/stocks"
    async with websockets.connect(uri, ping_interval=30, ping_timeout=10) as ws:
        await ws.send(json.dumps({"action": "auth", "params": POLYGON_API_KEY}))
        await ws.send(json.dumps({"action": "subscribe", "params": "T.*"}))
        print("Trade WebSocket opened and subscribing to ALL stocks...")
        async for message in ws:
            try:
                payload = json.loads(message)
                if not isinstance(payload, list):
                    payload = [payload]
                for item in payload:
                    if item.get("ev") != "T":
                        continue
                    symbol = item.get("sym")
                    price = float(item.get("p"))
                    size = float(item.get("s", 0))
                    trade_time = datetime.utcfromtimestamp(item.get("t") / 1000).replace(tzinfo=pytz.UTC)
                    await on_trade_event(symbol, price, size, trade_time)
            except Exception as e:
                print("Trade message error:", e)

def setup_signal_handlers(loop):
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(shutdown(loop, sig)))

async def shutdown(loop, sig):
    print(f"Received exit signal {sig.name}...")
    tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

async def main():
    loop = asyncio.get_event_loop()
    setup_signal_handlers(loop)
    await asyncio.gather(
        ws_connect_loop(trade_ws, "TRADE"),
        news_rest_poll(),  # use REST polling for news
    )

if __name__ == "__main__":
    print("Starting Polygon news + spike alert bot for stocks $10 and under (price/volume spikes), all news, robust reconnects...")
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
        print("Bot stopped gracefully.")
