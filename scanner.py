import websocket
import json
import threading
import requests
from collections import defaultdict, deque
from datetime import datetime, timedelta
import pytz

# --- CONFIG ---
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"

NEWS_KEYWORDS = [
    "AI", "artificial intelligence", "Federal Reserve", "interest rate", "Big Tech", "earnings",
    "Apple", "Microsoft", "Google", "Amazon", "Nvidia", "Meta", "EV", "electric vehicle", "Tesla",
    "semiconductor", "chip", "biotech", "FDA", "approval", "IPO", "merger", "acquisition", "China",
    "trade", "oil", "energy", "renewable", "meme stock", "retail investor", "cybersecurity", "hack",
    "breach", "REIT", "real estate", "crypto", "Bitcoin", "ETF", "bank", "financial", "airline",
    "travel", "healthcare", "innovation", "stimulus", "infrastructure", "ESG", "green energy",
    "consumer spending", "retail", "inflation", "cost of living", "strike", "union", "antitrust",
    "regulation", "streaming", "Netflix", "Disney", "election"
]

recent_news = defaultdict(lambda: deque(maxlen=20))
NEWS_WINDOW = timedelta(minutes=10)

def send_telegram_async(message):
    def _send():
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": False
        }
        try:
            requests.post(url, data=payload, timeout=10)
        except Exception as e:
            print(f"Telegram send error: {e}")
    threading.Thread(target=_send).start()

def news_pretty_alert(symbol, headline, summary, url):
    msg = (
        f"📰 <b>${symbol}</b> News Alert\n"
        f"<b>{headline}</b>\n"
        f"{summary}\n"
        f"<a href='{url}'>Read more</a>"
    )
    return msg

def price_pretty_alert(symbol, event_type, price, event_time):
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

def on_price_alert(symbol, event_type, price, event_time):
    send_telegram_async(price_pretty_alert(symbol, event_type, price, event_time))
    news_found = scan_recent_news(symbol, event_time)
    for headline, summary, url in news_found:
        msg = news_pretty_alert(symbol, headline, summary, url)
        send_telegram_async(msg)

# --- Candle structure and logic for price/volume alerts ---
class Candle:
    def __init__(self, open_, high, low, close, volume, start_time):
        self.open = open_
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.start_time = start_time

# Store last 4 candles for each symbol (to check last 3 completed candles)
candles = defaultdict(lambda: deque(maxlen=4))

def on_new_candle(symbol, open_, high, low, close, volume, start_time):
    # Only consider penny stocks
    if max(open_, close, high, low) > 5.00:
        return

    # Add new candle
    candles[symbol].append(Candle(open_, high, low, close, volume, start_time))

    # Need at least 4 candles (so last 3 completed)
    if len(candles[symbol]) < 4:
        return

    c = candles[symbol]
    # 1. PRICE INCREASE ALERT: last 3 candle close vs 1st
    price_3ago = c[0].close
    price_now = c[-1].close
    price_diff = price_now - price_3ago
    if price_diff >= 0.20:
        send_telegram_async(
            f"🚨 <b>{symbol}</b> penny stock price up ${price_diff:.2f} over last 3 min candles.\n"
            f"From ${price_3ago:.2f} to ${price_now:.2f}."
        )
        # Optionally scan for news here:
        news_found = scan_recent_news(symbol, c[-1].start_time)
        for headline, summary, url in news_found:
            send_telegram_async(news_pretty_alert(symbol, headline, summary, url))

    # 2. VOLUME SPIKE ALERT: volumes strictly increasing over last 3
    if c[0].volume < c[1].volume < c[2].volume:
        send_telegram_async(
            f"🚨 <b>{symbol}</b> penny stock volume spike!\n"
            f"Volumes: {c[0].volume:,} < {c[1].volume:,} < {c[2].volume:,} (last 3 mins)\n"
            f"Current Price: ${c[2].close:.2f}"
        )
        # Optionally scan for news here:
        news_found = scan_recent_news(symbol, c[-1].start_time)
        for headline, summary, url in news_found:
            send_telegram_async(news_pretty_alert(symbol, headline, summary, url))

# --- Polygon News WebSocket ---
def on_news_open(ws):
    print("News WebSocket opened and subscribing...")
    ws.send(json.dumps({"action": "auth", "params": POLYGON_API_KEY}))
    ws.send(json.dumps({"action": "subscribe", "params": "A.NEWS"}))

def on_news_message(ws, message):
    try:
        payload = json.loads(message)
        if not isinstance(payload, list):
            payload = [payload]
        for news in payload:
            if news.get("ev") != "A":
                continue
            symbol = news.get("sym", "")
            headline = news.get("title", "")
            summary = news.get("summary", "")
            url = news.get("url", "")
            ntime = news.get("dt", None)
            if ntime:
                news_time = datetime.utcfromtimestamp(ntime / 1000).replace(tzinfo=pytz.UTC)
            else:
                news_time = datetime.utcnow().replace(tzinfo=pytz.UTC)
            if not symbol or not headline:
                continue
            on_news_event(symbol, headline, summary, url, news_time)
    except Exception as e:
        print("News processing error:", e)

def on_news_error(ws, error):
    print("News WebSocket error:", error)

def on_news_close(ws, close_status_code, close_msg):
    print("News WebSocket closed:", close_status_code, close_msg)

def run_news_ws():
    ws = websocket.WebSocketApp(
        "wss://socket.polygon.io/news",
        on_open=on_news_open,
        on_message=on_news_message,
        on_error=on_news_error,
        on_close=on_news_close
    )
    ws.run_forever()

# --- Polygon Trade Spike Detection WebSocket ---
# Candle aggregation logic for 1-min candles
TRADE_CANDLE_INTERVAL = timedelta(minutes=1)
trade_candle_builders = defaultdict(list)
trade_candle_last_time = {}

def on_trade_event(symbol, price, size, trade_time):
    # Only aggregate penny stocks
    if price > 5.0:
        return

    # Find the current candle start time for 1-min bins
    candle_time = trade_time.replace(second=0, microsecond=0)
    # If we have a previous candle and new minute started, finalize and push
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
            on_new_candle(symbol, open_, high, low, close, volume, last_time)
        trade_candle_builders[symbol] = []
    # Add trade to current candle
    trade_candle_builders[symbol].append((price, size))
    trade_candle_last_time[symbol] = candle_time

def on_trade_open(ws):
    print("Trade WebSocket opened and subscribing to ALL stocks...")
    ws.send(json.dumps({"action": "auth", "params": POLYGON_API_KEY}))
    ws.send(json.dumps({"action": "subscribe", "params": "T.*"}))

def on_trade_message(ws, message):
    try:
        payload = json.loads(message)
        if not isinstance(payload, list):
            payload = [payload]
        for item in payload:
            if item.get("ev") != "T":
                continue
            symbol = item.get("sym")
            price = float(item.get("p"))
            size = float(item.get("s", 0))  # trade size, may be 0 if not in message
            trade_time = datetime.utcfromtimestamp(item.get("t") / 1000).replace(tzinfo=pytz.UTC)
            on_trade_event(symbol, price, size, trade_time)
    except Exception as e:
        print("Trade message error:", e)

def on_trade_error(ws, error):
    print("Trade WebSocket error:", error)

def on_trade_close(ws, close_status_code, close_msg):
    print("Trade WebSocket closed:", close_status_code, close_msg)

def run_trade_ws():
    ws = websocket.WebSocketApp(
        "wss://socket.polygon.io/stocks",
        on_open=on_trade_open,
        on_message=on_trade_message,
        on_error=on_trade_error,
        on_close=on_trade_close
    )
    ws.run_forever()

if __name__ == "__main__":
    print("Starting Polygon news + spike alert bot for penny stocks under $5...")
    threading.Thread(target=run_news_ws, daemon=True).start()
    run_trade_ws()
