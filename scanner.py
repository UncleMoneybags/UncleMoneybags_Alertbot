import requests
import time
from collections import deque
import datetime
import threading

# === USER CONFIGURATION ===
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"  # Can be user or group
DISCORD_WEBHOOK_URL = "YOUR_DISCORD_WEBHOOK_URL"

PRICE_MAX = 10.00
MIN_VOLUME = 100_000
VOLUME_SPIKE_MULT = 3.0
PRICE_SPIKE_PCT = 3.0  # % change vs rolling average to be considered a spike
NEWS_LOOKBACK_MINUTES = 5
TICKERS_LIMIT = 100

volume_history = {}
price_history = {}
alerted_news_ids = set()
alerted_halts = set()
alerted_ipos = set()

TRADING_START_HOUR = 4  # 4:00 AM ET
TRADING_END_HOUR = 20   # 8:00 PM ET

def is_market_time():
    """Return True if now is Monday-Friday and between 4am and 8pm Eastern Time."""
    now_utc = datetime.datetime.utcnow()
    # New York is UTC-4 during daylight saving (EDT, Mar-Nov), UTC-5 in standard (EST, Nov-Mar)
    # We'll use UTC-4 (EDT) for typical US trading months. If you want to be exact, use pytz.
    NY_OFFSET = -4
    now_ny = now_utc + datetime.timedelta(hours=NY_OFFSET)
    weekday = now_ny.weekday()  # 0 = Monday, 6 = Sunday
    hour = now_ny.hour
    return (weekday < 5) and (TRADING_START_HOUR <= hour < TRADING_END_HOUR)

# === ALERT SENDING ===

def send_telegram_alert(message):
    url = (
        f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    )
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown",
        "disable_web_page_preview": True
    }
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        print(f"[WARN] Telegram alert failed: {e}")

def send_discord_alert(message):
    payload = {"content": message}
    try:
        requests.post(DISCORD_WEBHOOK_URL, json=payload, timeout=5)
    except Exception as e:
        print(f"[WARN] Discord alert failed: {e}")

def alert(message):
    print(message)
    threading.Thread(target=send_telegram_alert, args=(message,)).start()
    threading.Thread(target=send_discord_alert, args=(message,)).start()

# === DATA FUNCTIONS ===

def get_active_tickers():
    """Pulls most active tickers under price cap from Polygon snapshot."""
    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/active?apiKey={POLYGON_API_KEY}"
    r = requests.get(url)
    data = r.json().get("tickers", [])
    tickers = [
        t["ticker"]
        for t in data
        if t.get("lastTrade", {}).get("p", 0) <= PRICE_MAX and t.get("day", {}).get("v", 0) >= MIN_VOLUME
    ]
    return tickers[:TICKERS_LIMIT]

def get_minute_bar(ticker):
    """Get the last 1-min bar for this ticker."""
    now = datetime.datetime.utcnow()
    start = now - datetime.timedelta(minutes=2)
    start_str = start.strftime("%Y-%m-%d")
    end_str = now.strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/minute/{start_str}/{end_str}?sort=desc&limit=1&apiKey={POLYGON_API_KEY}"
    r = requests.get(url)
    results = r.json().get("results", [])
    if not results:
        return None
    return results[0]  # Most recent bar

def detect_volume_spike(ticker, bar):
    """Detects if this minute's volume is a spike vs prior avg."""
    vol = bar.get("v", 0)
    hist = volume_history.setdefault(ticker, deque(maxlen=10))
    if len(hist) >= 3:
        avg = sum(hist) / len(hist)
        if avg > 0 and vol > avg * VOLUME_SPIKE_MULT:
            alert(f"🚨 *{ticker}* volume spike {vol/1000:.2f}K vs avg {avg/1000:.2f}K\nhttps://finance.yahoo.com/quote/{ticker}")
    hist.append(vol)

def detect_price_spike(ticker, bar):
    """Detects if this minute's price is a spike vs prior avg."""
    this_close = bar.get("c", 0)
    hist = price_history.setdefault(ticker, deque(maxlen=10))
    if len(hist) >= 3:
        avg = sum(hist) / len(hist)
        if avg > 0:
            pct_change = (this_close - avg) / avg * 100
            if abs(pct_change) >= PRICE_SPIKE_PCT:
                alert(f"⚡️ *{ticker}* price spike {pct_change:.2f}% to ${this_close:.2f}\nhttps://finance.yahoo.com/quote/{ticker}")
    hist.append(this_close)

def get_recent_news(ticker):
    """Returns news headlines for ticker in last N minutes."""
    now = datetime.datetime.utcnow()
    start = now - datetime.timedelta(minutes=NEWS_LOOKBACK_MINUTES)
    start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    url = (
        f"https://api.polygon.io/v2/reference/news?"
        f"ticker={ticker}&published_utc.gte={start_str}&apiKey={POLYGON_API_KEY}"
    )
    r = requests.get(url)
    articles = r.json().get("results", [])
    new_alerts = []
    for a in articles:
        news_id = a.get("id")
        if news_id not in alerted_news_ids:
            alerted_news_ids.add(news_id)
            headline = a.get("title","")
            url_link = a.get("article_url","")
            new_alerts.append(f"📰 *{ticker}* - {headline}\n{url_link}")
    return new_alerts

def get_halts():
    """Checks for new halts using Polygon's market status endpoint."""
    url = f"https://api.polygon.io/v3/reference/market-status/halts?apiKey={POLYGON_API_KEY}"
    try:
        r = requests.get(url)
        data = r.json().get("results", [])
        new_halts = []
        for h in data:
            key = (h.get("ticker"), h.get("halt_time"))
            if key not in alerted_halts:
                alerted_halts.add(key)
                reason = h.get("reason_code")
                new_halts.append(f"⏸️ HALT: *{h['ticker']}* - Reason: {reason} @ {h.get('halt_time')}")
        return new_halts
    except Exception as e:
        print(f"[WARN] Halt check failed: {e}")
        return []

def get_ipos():
    """Checks for new IPOs in last 3 days using Polygon's IPO calendar."""
    now = datetime.datetime.utcnow()
    start = now - datetime.timedelta(days=3)
    start_str = start.strftime("%Y-%m-%d")
    end_str = now.strftime("%Y-%m-%d")
    url = (
        f"https://api.polygon.io/v3/reference/market-activity/ipos?from={start_str}&to={end_str}&apiKey={POLYGON_API_KEY}"
    )
    try:
        r = requests.get(url)
        ipos = r.json().get("results", [])
        new_ipos = []
        for ipo in ipos:
            symbol = ipo.get("ticker")
            if symbol and symbol not in alerted_ipos:
                alerted_ipos.add(symbol)
                name = ipo.get("name", "")
                date = ipo.get("expected_date", "")
                new_ipos.append(f"🆕 IPO: *{symbol}* ({name}) - Expected: {date}")
        return new_ipos
    except Exception as e:
        print(f"[WARN] IPO check failed: {e}")
        return []

# === MAIN LOOP ===

def main_loop():
    print("Starting Stock Alert Service...")
    while True:
        if not is_market_time():
            print("Not market hours (4am-8pm ET, Mon-Fri). Sleeping 60s...")
            time.sleep(60)
            continue
        try:
            tickers = get_active_tickers()
            for ticker in tickers:
                bar = get_minute_bar(ticker)
                if bar:
                    detect_volume_spike(ticker, bar)
                    detect_price_spike(ticker, bar)
                news_alerts = get_recent_news(ticker)
                for alert_msg in news_alerts:
                    alert(alert_msg)
            # Halt and IPO checks (less frequent, every 2 min)
            if int(time.time()) % 120 < 60:
                for halt_msg in get_halts():
                    alert(halt_msg)
                for ipo_msg in get_ipos():
                    alert(ipo_msg)
        except Exception as e:
            print(f"[ERROR] Loop error: {e}")
        time.sleep(60)

if __name__ == "__main__":
    main_loop()
