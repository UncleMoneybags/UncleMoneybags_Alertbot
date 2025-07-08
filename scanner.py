import requests
import time
import datetime
import threading
from collections import deque
from zoneinfo import ZoneInfo

POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"

PRICE_MAX = 10.0
VOLUME_SPIKE_MULT = 2.0
PRICE_SPIKE_PCT = 3.0
NEWS_LOOKBACK_MINUTES = 5
TICKER_BATCH_SIZE = 50

volume_history = {}
price_history = {}
alerted_news_ids = set()
alerted_halts = set()
alerted_ipos = set()

TRADING_START_HOUR = 4
TRADING_END_HOUR = 20

def is_market_time():
    now_ny = datetime.datetime.now(ZoneInfo("America/New_York"))
    weekday = now_ny.weekday()
    hour = now_ny.hour
    return (weekday < 5) and (TRADING_START_HOUR <= hour < TRADING_END_HOUR)

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

def alert(message):
    print(message)
    threading.Thread(target=send_telegram_alert, args=(message,)).start()

def fetch_all_us_tickers():
    """
    Returns a list of all active, common stock US tickers.
    """
    print("Fetching all tickers...")
    tickers = []
    url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit=1000&apiKey={POLYGON_API_KEY}"
    while url:
        r = requests.get(url)
        data = r.json()
        for t in data.get("results", []):
            if t.get("type") != "CS":
                continue
            symbol = t["ticker"]
            if symbol.startswith("C:") or symbol.startswith("W:") or "." in symbol:
                continue
            tickers.append(symbol)
        url = data.get("next_url")
        if url:
            url += f"&apiKey={POLYGON_API_KEY}"
    print(f"Total US common stock tickers fetched: {len(tickers)}")
    return tickers

def get_last_prices_batch(batch):
    """
    Fetches last trade price for a batch of tickers.
    Returns dict {ticker: last_price}
    """
    symbols = ",".join(batch)
    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?tickers={symbols}&apiKey={POLYGON_API_KEY}"
    prices = {}
    try:
        r = requests.get(url)
        for t in r.json().get("tickers", []):
            price = t.get("lastTrade", {}).get("p", 0)
            prices[t["ticker"]] = price
    except Exception as e:
        print(f"[WARN] Price batch failed: {e}")
    return prices

def get_minute_bar_batch(tickers):
    """
    Fetches the most recent 1-min bars for a batch of tickers.
    Returns dict {ticker: bar}
    """
    url = f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{datetime.datetime.utcnow().strftime('%Y-%m-%d')}?adjusted=true&apiKey={POLYGON_API_KEY}"
    try:
        r = requests.get(url)
        bars = {}
        for bar in r.json().get("results", []):
            sym = bar["T"]
            if sym in tickers:
                bars[sym] = bar
        return bars
    except Exception as e:
        print(f"[WARN] Minute bar batch failed: {e}")
        return {}

def detect_volume_spike(ticker, bar):
    vol = bar.get("v", 0)
    hist = volume_history.setdefault(ticker, deque(maxlen=10))
    if len(hist) == hist.maxlen:
        avg = sum(hist) / len(hist)
        if avg > 0 and vol >= avg * VOLUME_SPIKE_MULT:
            alert(f"🚨 *{ticker}* volume spike {vol/1000:.2f}K vs avg {avg/1000:.2f}K\nhttps://finance.yahoo.com/quote/{ticker}")
    hist.append(vol)

def detect_price_spike(ticker, bar):
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
    now = datetime.datetime.utcnow()
    start = now - datetime.timedelta(minutes=NEWS_LOOKBACK_MINUTES)
    start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    url = (
        f"https://api.polygon.io/v2/reference/news?"
        f"ticker={ticker}&published_utc.gte={start_str}&apiKey={POLYGON_API_KEY}"
    )
    try:
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
    except Exception as e:
        print(f"[WARN] News check failed for {ticker}: {e}")
        return []

def get_halts():
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

def main_loop():
    print("Starting Real-Time Stock Alert Service...")
    # Cache the full US ticker list in memory (refresh at startup or if empty)
    all_tickers = []
    while True:
        if not is_market_time():
            print("Not market hours. Sleeping 60s...")
            time.sleep(60)
            continue
        if not all_tickers:
            all_tickers = fetch_all_us_tickers()
        under10_tickers = []
        # Batch fetch last prices for all tickers
        for i in range(0, len(all_tickers), TICKER_BATCH_SIZE):
            batch = all_tickers[i:i + TICKER_BATCH_SIZE]
            prices = get_last_prices_batch(batch)
            for t, p in prices.items():
                if p > 0 and p <= PRICE_MAX:
                    under10_tickers.append(t)
            time.sleep(0.1)  # avoid API burst
        print(f"Tickers under ${PRICE_MAX} right now: {len(under10_tickers)}")
        # Batch process for volume/price/news
        for i in range(0, len(under10_tickers), TICKER_BATCH_SIZE):
            batch = under10_tickers[i:i + TICKER_BATCH_SIZE]
            bars = get_minute_bar_batch(batch)
            for ticker, bar in bars.items():
                detect_volume_spike(ticker, bar)
                detect_price_spike(ticker, bar)
                for alert_msg in get_recent_news(ticker):
                    alert(alert_msg)
            # Halt and IPO checks every batch
            for halt_msg in get_halts():
                alert(halt_msg)
            for ipo_msg in get_ipos():
                alert(ipo_msg)
            time.sleep(1)  # avoid API burst
        # Full loop every ~len(under10_tickers)/BATCH_SIZE minutes
        # Reset all_tickers every loop for up-to-date coverage
        all_tickers = []
        # Optionally, sleep a short while before next loop (e.g., 10s)
        time.sleep(10)

if __name__ == "__main__":
    main_loop()
