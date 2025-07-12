print("scanner.py is running!!! --- If you see this, your file is found and started.")

import asyncio
import websockets
import aiohttp
import json
import html
import re
from collections import deque, defaultdict
from datetime import datetime, time, timezone, timedelta
import pytz
import signal

# ==== ML & Logging Imports (INJECTED) ====
import csv
import os
import joblib
import numpy as np

print("Imports completed successfully.")

# --- CONFIG ---
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"
PRICE_THRESHOLD = 10.00
MAX_SYMBOLS = 400
SCREENER_REFRESH_SEC = 60
MIN_ALERT_MOVE = 0.15
MIN_3MIN_VOLUME = 10000
MIN_PER_CANDLE_VOL = 1000
MIN_IPO_DAYS = 30
ALERT_PRICE_DELTA = 0.25

# --- 1m volume spike config (NEW for less noise) ---
MIN_1M_PRICE_MOVE_PCT = 0.02   # 2%
MIN_1M_PRICE_MOVE_ABS = 0.05   # $0.05

# For running VWAP calculation (INJECTED)
vwap_cum_vol = defaultdict(float)
vwap_cum_pv = defaultdict(float)

# Relative Volume config
rvol_history = defaultdict(lambda: deque(maxlen=20))
RVOL_MIN = 3.0

# ==== ML Event Logging and Scoring Functions (INJECTED) ====
EVENT_LOG_FILE = "event_log.csv"

def log_event(event_type, symbol, price, volume, event_time, extra_features=None):
    extra_features = extra_features or {}
    row = {
        "event_type": event_type,
        "symbol": symbol,
        "price": price,
        "volume": volume,
        "event_time_utc": event_time.isoformat(),
        **extra_features
    }
    header = list(row.keys())
    write_header = not os.path.exists(EVENT_LOG_FILE) or os.path.getsize(EVENT_LOG_FILE) == 0
    with open(EVENT_LOG_FILE, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)
        if write_header:
            writer.writeheader()
        writer.writerow(row)

try:
    runner_clf = joblib.load("runner_model.joblib")
    print("Loaded ML runner model.")
except Exception as e:
    print("WARNING: Could not load runner model:", e)
    runner_clf = None

def score_event_ml(event_type, symbol, price, volume, rvol, prepost):
    if runner_clf is None:
        return 0.0
    event_type_code = 0 if event_type == "spike" else 1
    prepost_f = float(prepost)
    X = np.array([[price, volume if volume is not None else 0, rvol if rvol is not None else 1.0, prepost_f, event_type_code]])
    prob = runner_clf.predict_proba(X)[0,1]
    return prob

# --- News Keyword Filter ---
KEYWORDS = [
    "offering", "FDA", "approval", "acquisition", "merger", "bankruptcy", "delisting", "reverse split", "split",
    "halt", "investigation", "lawsuit", "earnings", "guidance", "clinical", "phase 1", "phase 2", "phase 3",
    "partnership", "contract", "dividend", "buyback", "sec", "subpoena", "settlement", "short squeeze", "recall",
    "resigns", "appoints", "collaboration", "sec filing", "patent", "discontinued", "withdraw", "spike", "upsize",
    "pricing", "withdraws", "grants", "fires", "director", "ceo", "cfo"
]

def is_market_scan_time():
    ny = pytz.timezone("America/New_York")
    now_utc = datetime.now(pytz.UTC)
    now_ny = now_utc.astimezone(ny)
    if now_ny.weekday() >= 5:
        return False
    scan_start = time(4, 0)
    scan_end = time(20, 0)
    return scan_start <= now_ny.time() <= scan_end

def is_news_alert_time():
    ny = pytz.timezone("America/New_York")
    now_utc = datetime.now(pytz.UTC)
    now_ny = now_utc.astimezone(ny)
    if now_ny.weekday() >= 5:
        return False
    start = time(7, 0)
    end = time(20, 0)
    return start <= now_ny.time() <= end

async def send_telegram_async(message):
    print("send_telegram_async called.")
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

candles = defaultdict(lambda: deque(maxlen=3))
trade_candle_builders = defaultdict(list)
trade_candle_last_time = {}
last_alerted_price = {}
last_halt_alert = {}
news_seen = set()
latest_news_time = None  # For live news only

# 💥 1-minute volume spike state
last_volume_spike_time = defaultdict(lambda: datetime.min.replace(tzinfo=timezone.utc))
# 👀 Dual alert state (for runner warming up)
last_runner_alert_time = defaultdict(lambda: datetime.min.replace(tzinfo=timezone.utc))

def news_matches_keywords(headline, summary):
    text_block = f"{headline} {summary}".lower()
    return any(word.lower() in text_block for word in KEYWORDS)

def bold_keywords(text, keywords):
    def replacer(match):
        return f"<b>{match.group(0)}</b>"
    for kw in keywords:
        regex = re.compile(rf'\b({re.escape(kw)})\b', re.IGNORECASE)
        text = regex.sub(replacer, text)
    return text

async def is_under_10(tickers):
    print(f"is_under_10 called for tickers: {tickers}")
    for ticker in tickers:
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}?apiKey={POLYGON_API_KEY}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    data = await resp.json()
                    price = None
                    last_trade = data.get("ticker", {}).get("lastTrade", {})
                    day = data.get("ticker", {}).get("day", {})
                    if last_trade and last_trade.get("p", 0) > 0:
                        price = last_trade["p"]
                    elif day and day.get("c", 0) > 0:
                        price = day["c"]
                    if price is not None and 0 < price <= 10.00:
                        print(f"Ticker {ticker} is under $10: {price}")
                        return True
        except Exception as e:
            print(f"Price check failed for {ticker}: {e}")
    return False

async def send_news_telegram_async(message):
    print("send_news_telegram_async called.")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": False
    }
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.post(url, data=payload, timeout=10) as resp:
                    if resp.status == 429:
                        data = await resp.json()
                        retry = data.get("parameters", {}).get("retry_after", 30)
                        print(f"Rate limited. Waiting {retry} seconds before retrying...")
                        await asyncio.sleep(retry)
                        continue
                    if resp.status != 200:
                        print("Telegram send error (news):", await resp.text())
                    return
            except Exception as e:
                print(f"Telegram send error (news): {e}")
                return

# --- Price move filter for news (REMOVED as requested) ---

async def news_alerts_task():
    print("news_alerts_task started")
    global news_seen, latest_news_time
    url = f"https://api.polygon.io/v2/reference/news?apiKey={POLYGON_API_KEY}&limit=50"
    while True:
        print("news_alerts_task loop")
        if not is_news_alert_time():
            await asyncio.sleep(30)
            continue
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    data = await resp.json()
                    news_batch = data.get('results', [])
                    # Find the most recent news publish time in batch
                    if news_batch:
                        times = [datetime.fromisoformat(item.get('published_utc').replace('Z', '+00:00')) for item in news_batch if item.get('published_utc')]
                        most_recent = max(times) if times else None
                    else:
                        most_recent = None

                    # On first run after 7am, just set latest_news_time and don't send any news
                    if latest_news_time is None:
                        latest_news_time = most_recent
                        await asyncio.sleep(30)
                        continue

                    for item in sorted(news_batch, key=lambda x: x.get('published_utc')):
                        ntime = item.get('published_utc')
                        if not ntime:
                            continue
                        ntime_dt = datetime.fromisoformat(ntime.replace('Z', '+00:00'))
                        if ntime_dt <= latest_news_time:
                            continue
                        news_id = item['id']
                        if news_id in news_seen:
                            continue
                        headline = item.get('title', '')
                        summary = item.get('description', '')
                        tickers = item.get('tickers', [])
                        if not news_matches_keywords(headline, summary):
                            continue
                        if not await is_under_10(tickers):
                            continue

                        # ----------- FORMATTED NEWS ALERT START -----------
                        # Format tickers as bold, linked to Yahoo Finance
                        if tickers:
                            tickers_html = ", ".join([
                                f'<a href="https://finance.yahoo.com/quote/{escape_html(t)}">{escape_html(t)}</a>'
                                for t in tickers
                            ])
                            tickers_str = f"<b>{tickers_html}</b>"
                        else:
                            tickers_str = ""

                        headline_clean = escape_html(headline)
                        summary_clean = escape_html(summary)

                        msg = f"📰 {tickers_str}\n{headline_clean}"
                        if summary_clean:
                            msg += f"\n\n<small>{summary_clean}</small>"
                        url_ = item.get("article_url") or item.get("url")
                        if url_:
                            msg += f"\n<a href=\"{escape_html(url_)}\">Read more</a>"
                        # ----------- FORMATTED NEWS ALERT END -----------

                        news_seen.add(news_id)
                        await send_news_telegram_async(msg)
                        await asyncio.sleep(3)  # Throttle to avoid hitting Telegram rate limit
                    # After sending, update latest_news_time
                    if most_recent:
                        latest_news_time = most_recent
            if len(news_seen) > 500:
                news_seen = set(list(news_seen)[-500:])
        except Exception as e:
            print(f"News fetch error: {e}")
        await asyncio.sleep(30)

def is_equity_symbol(ticker):
    if re.search(r'(\.|\bW$|\bWS$|\bU$|\bR$|\bP$)', ticker):
        return False
    if ticker.endswith(('W', 'U', 'R', 'P', 'WS')) or '.' in ticker:
        return False
    if any(x in ticker for x in ['WS', 'W', 'U', 'R', 'P', '.']):
        return False
    return True

async def is_recent_ipo(ticker):
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={POLYGON_API_KEY}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                data = await resp.json()
                list_date_str = data.get("results", {}).get("list_date")
                if list_date_str:
                    list_date = datetime.strptime(list_date_str, "%Y-%m-%d").date()
                    days_since_ipo = (datetime.utcnow().date() - list_date).days
                    if days_since_ipo < MIN_IPO_DAYS:
                        print(f"{ticker} is a recent IPO ({days_since_ipo} days).")
                    return days_since_ipo < MIN_IPO_DAYS
        except Exception as e:
            print(f"IPO check failed for {ticker}: {e}")
    return False

async def fetch_top_penny_symbols():
    print("fetch_top_penny_symbols called")
    penny_symbols = []
    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey={POLYGON_API_KEY}&limit=1000"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                data = await resp.json()
                if "tickers" not in data:
                    print("Tickers snapshot API response:", data)
                    return []
                for stock in data.get("tickers", []):
                    ticker = stock.get("ticker")
                    if not is_equity_symbol(ticker):
                        continue
                    if await is_recent_ipo(ticker):
                        continue
                    last_trade = stock.get("lastTrade", {})
                    day = stock.get("day", {})
                    price = None
                    price_time = 0
                    if last_trade and last_trade.get("p", 0) > 0:
                        price = last_trade["p"]
                        price_time = last_trade.get("t", 0)
                    if day and day.get("c", 0) > 0 and (not price or day.get("t", 0) > price_time):
                        price = day["c"]
                    volume = day.get("v", 0)
                    if price is not None and 0 < price <= PRICE_THRESHOLD:
                        penny_symbols.append(ticker)
                        if len(penny_symbols) >= MAX_SYMBOLS:
                            break
        except Exception as e:
            print("Tickers snapshot fetch error:", e)
    print(f"fetch_top_penny_symbols: found {len(penny_symbols)} symbols")
    return penny_symbols

current_symbols = set()

async def dynamic_symbol_manager(symbol_queue):
    print("dynamic_symbol_manager started")
    global current_symbols
    while True:
        print("dynamic_symbol_manager loop")
        if is_market_scan_time():
            symbols = await fetch_top_penny_symbols()
            if symbols:
                if set(symbols) != current_symbols:
                    print(f"dynamic_symbol_manager: updating symbol_queue with {len(symbols)} symbols")
                    await symbol_queue.put(symbols)
                    current_symbols = set(symbols)
        await asyncio.sleep(SCREENER_REFRESH_SEC)

async def on_trade_event(symbol, price, size, trade_time):
    print(f"on_trade_event: {symbol}, price: {price}, size: {size}")
    candle_time = trade_time.replace(second=0, microsecond=0)
    last_time = trade_candle_last_time.get(symbol)
    if last_time is not None and candle_time != last_time:
        trades = trade_candle_builders[symbol]
        if trades:
            trades.sort(key=lambda t: t[2])
            prices = [t[0] for t in trades]
            volumes = [t[1] for t in trades]
            open_ = prices[0]
            close = prices[-1]
            high = max(prices)
            low = min(prices)
            volume = sum(volumes)
            start_time = last_time
            await on_new_candle(symbol, open_, high, low, close, volume, start_time)
        trade_candle_builders[symbol] = []
    trade_candle_builders[symbol].append((price, size, trade_time))
    trade_candle_last_time[symbol] = candle_time

async def on_new_candle(symbol, open_, high, low, close, volume, start_time):
    print(f"on_new_candle: {symbol} - open:{open_}, close:{close}, volume:{volume}")
    if not is_market_scan_time() or close > PRICE_THRESHOLD:
        return
    candles[symbol].append({
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "start_time": start_time,
    })

    # --- VWAP Calculation (INJECTED) ---
    vwap_cum_vol[symbol] += volume
    vwap_cum_pv[symbol] += close * volume
    vwap = vwap_cum_pv[symbol] / vwap_cum_vol[symbol] if vwap_cum_vol[symbol] > 0 else None

    # === DUAL 1-MIN VOLUME SPIKE ALERT SYSTEM (FIXED!) ===
    DUAL_MIN_1M_PRICE_MOVE_PCT = 0.02
    DUAL_MIN_1M_PRICE_MOVE_ABS = 0.05
    DUAL_MIN_1M_PRICE_MOVE_ABS_2PLUS = 0.10
    PRE_BREAKOUT_DIST_PCT = 0.02
    PRE_BREAKOUT_DIST_ABS = 0.05
    VOLUME_SPIKE_MULTIPLIER = 2.5
    VOLUME_SPIKE_MIN = 5000

    prev_vols = [c["volume"] for c in list(candles[symbol])[:-1]]
    if len(prev_vols) >= 2 and vwap is not None:
        avg_prev = sum(prev_vols) / len(prev_vols)
        price_move = close - open_
        price_move_pct = price_move / open_ if open_ > 0 else 0
        min_abs = DUAL_MIN_1M_PRICE_MOVE_ABS if close < 2 else DUAL_MIN_1M_PRICE_MOVE_ABS_2PLUS
        session_high = max([c["high"] for c in candles[symbol]])
        dist_from_high = session_high - close
        dist_pct = dist_from_high / session_high if session_high > 0 else 0
        now = datetime.now(timezone.utc)

        # --- Breakout Alert: new high on volume/price ---
        if (
            volume > VOLUME_SPIKE_MIN
            and volume >= VOLUME_SPIKE_MULTIPLIER * avg_prev
            and close > open_
            and close > vwap
            and (price_move >= min_abs or price_move_pct >= DUAL_MIN_1M_PRICE_MOVE_PCT)
            and abs(close - session_high) < 1e-6
            and (now - last_volume_spike_time[symbol]).total_seconds() > 600
        ):
            last_volume_spike_time[symbol] = now
            msg = f"🚀 {symbol} BREAKOUT! ${close:.2f} (NEW HIGH)"
            await send_telegram_async(msg)

        # --- Runner Warming Up Alert: near high, but not at high ---
        elif (
            volume > VOLUME_SPIKE_MIN
            and volume >= VOLUME_SPIKE_MULTIPLIER * avg_prev
            and close > open_
            and close > vwap
            and (price_move >= min_abs or price_move_pct >= DUAL_MIN_1M_PRICE_MOVE_PCT)
            and close < session_high
            and (0 < dist_from_high <= PRE_BREAKOUT_DIST_ABS or 0 < dist_pct <= PRE_BREAKOUT_DIST_PCT)
            and (now - last_runner_alert_time[symbol]).total_seconds() > 900
        ):
            last_runner_alert_time[symbol] = now
            msg = f"👀 {symbol} runner warming up: ${close:.2f} (within {dist_from_high:.2f} of high)"
            await send_telegram_async(msg)
    # === END DUAL 1-MIN VOLUME SPIKE ALERT SYSTEM (FIXED!) ===

    if len(candles[symbol]) == 3:
        c0, c1, c2 = candles[symbol]
        total_volume = c0["volume"] + c1["volume"] + c2["volume"]

        # RVOL logic
        rvol_history[symbol].append(total_volume)
        if len(rvol_history[symbol]) >= 5:
            trailing_vols = list(rvol_history[symbol])[:-1]
            if trailing_vols:
                avg_trailing = sum(trailing_vols) / len(trailing_vols)
                if avg_trailing > 0:
                    rvol = total_volume / avg_trailing
                    print(f"{symbol} RVOL: {rvol}")
                    if rvol < RVOL_MIN:
                        return
                else:
                    return
            else:
                return
        else:
            return

        if (
            c0["close"] < c1["close"] < c2["close"]
            and (c2["close"] - c0["close"]) >= MIN_ALERT_MOVE
            and total_volume >= MIN_3MIN_VOLUME
            and c0["volume"] >= MIN_PER_CANDLE_VOL
            and c1["volume"] >= MIN_PER_CANDLE_VOL
            and c2["volume"] >= MIN_PER_CANDLE_VOL
        ):
            move = c2["close"] - c0["close"]
            prev_price = last_alerted_price.get(symbol)
            if prev_price is not None and (c2["close"] - prev_price) < ALERT_PRICE_DELTA:
                return
            emoji = "🚨" if prev_price is None else "💰"
            last_alerted_price[symbol] = c2["close"]

            pct_move = (c2["close"] - c0["close"]) / c0["close"] if c0["close"] > 0 else 0

            conf = 1
            if 'rvol' in locals():
                if rvol >= 5:
                    conf += 2
                elif rvol >= 3:
                    conf += 1
            if pct_move >= 0.10:
                conf += 2
            elif pct_move >= 0.05:
                conf += 1
            if move >= 0.50:
                conf += 2
            elif move >= 0.30:
                conf += 1
            if min(c0["volume"], c1["volume"], c2["volume"]) >= 10000:
                conf += 1
            conf = min(conf, 10)

            msg = (
                f"{emoji} {escape_html(symbol)} up ${move:.2f} ({pct_move*100:.1f}%) in 3 minutes.\n"
                f"Now ${c2['close']:.2f}. "
                f"<b>Confidence: {conf}/10</b>"
            )
            await send_telegram_async(msg)

            log_event(
                event_type="spike",
                symbol=symbol,
                price=c2["close"],
                volume=total_volume,
                event_time=start_time,
                extra_features={"rvol": rvol if 'rvol' in locals() else None, "prepost": 0}
            )
            ml_prob = score_event_ml("spike", symbol, c2["close"], total_volume, rvol if 'rvol' in locals() else None, 0)
            if ml_prob > 0.7:
                await send_telegram_async(
                    f"🔥 <b>HIGH POTENTIAL RUNNER</b> {escape_html(symbol)} Rocket Fuel: {ml_prob:.2f} 🚀"
                )

async def handle_halt_event(item):
    print(f"handle_halt_event called: {item}")
    symbol = item.get("sym")
    if not symbol or not is_equity_symbol(symbol):
        return
    price = None
    if "p" in item and item["p"] is not None:
        price = float(item["p"])
    if price is None:
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}?apiKey={POLYGON_API_KEY}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    data = await resp.json()
                    snap = data.get("ticker", {})
                    price = snap.get("lastTrade", {}).get("p") or snap.get("day", {}).get("c")
        except Exception as e:
            print(f"[HALT ALERT] Could not get price for {symbol}: {e}")
    if price is None or price > PRICE_THRESHOLD or price <= 0:
        return
    if await is_recent_ipo(symbol):
        return
    halt_ts = item.get("t") if "t" in item else None
    global last_halt_alert
    if halt_ts is not None:
        if symbol in last_halt_alert and last_halt_alert[symbol] == halt_ts:
            return
        last_halt_alert[symbol] = halt_ts
    msg = f"🚦 {escape_html(symbol)} (${price:.2f}) just got halted!"
    await send_telegram_async(msg)

    log_event(
        event_type="halt",
        symbol=symbol,
        price=price,
        volume=None,
        event_time=datetime.now(timezone.utc),
        extra_features={"rvol": None, "prepost": 0}
    )
    ml_prob = score_event_ml("halt", symbol, price, 0, 1.0, 0)
    if ml_prob > 0.7:
        await send_telegram_async(
            f"🔥 <b>HIGH POTENTIAL HALT</b> {escape_html(symbol)} Rocket Fuel: {ml_prob:.2f} 🚀"
        )

async def send_scheduled_alerts():
    print("send_scheduled_alerts started")
    sent_open_msg = False
    sent_close_msg = False
    sent_ebook_msg = False
    while True:
        print("send_scheduled_alerts loop")
        now_utc = datetime.now(pytz.UTC)
        ny = pytz.timezone("America/New_York")
        now_ny = now_utc.astimezone(ny)
        weekday = now_ny.weekday()
        if weekday < 5 and now_ny.hour == 9 and now_ny.minute == 25:
            if not sent_open_msg:
                await send_telegram_async("Market opens in 5 mins...secure the damn bag!")
                sent_open_msg = True
        else:
            sent_open_msg = False
        if weekday < 4 and now_ny.hour == 20 and now_ny.minute == 1:
            if not sent_close_msg:
                await send_telegram_async("Market closed, reconvene in pre market tomorrow morning.")
                sent_close_msg = True
        else:
            sent_close_msg = False
        if weekday == 5 and now_ny.hour == 12 and now_ny.minute == 0:
            if not sent_ebook_msg:
                await send_telegram_async("📚 Need help trading? My ebook has everything you need to know!")
                sent_ebook_msg = True
        else:
            sent_ebook_msg = False
        await asyncio.sleep(30)

def setup_signal_handlers(loop):
    print("setup_signal_handlers called")
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(shutdown(loop, sig)))

async def shutdown(loop, sig):
    print(f"Received exit signal {sig.name}...")
    tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

async def trade_ws(symbol_queue):
    print("trade_ws started")
    subscribed_symbols = set()
    halt_subscribed = False
    while True:
        print("trade_ws loop")
        try:
            uri = "wss://socket.polygon.io/stocks"
            async with websockets.connect(uri, ping_interval=30, ping_timeout=10) as ws:
                await ws.send(json.dumps({"action": "auth", "params": POLYGON_API_KEY}))
                symbols = await symbol_queue.get()
                if not symbols:
                    await asyncio.sleep(SCREENER_REFRESH_SEC)
                    continue
                BATCH_SIZE = 100
                params_batches = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
                for batch in params_batches:
                    params = ",".join([f"T.{s}" for s in batch])
                    await ws.send(json.dumps({"action": "subscribe", "params": params}))
                    await asyncio.sleep(0.2)
                subscribed_symbols = set(symbols)

                # --- NEW: Subscribe to all halts (for all US stocks) ---
                if not halt_subscribed:
                    await ws.send(json.dumps({"action": "subscribe", "params": "H.*"}))
                    halt_subscribed = True

                async def ws_recv():
                    print("ws_recv started")
                    async for message in ws:
                        payload = json.loads(message)
                        if not isinstance(payload, list):
                            payload = [payload]
                        for item in payload:
                            if item.get("ev") == "T":
                                symbol = item.get("sym")
                                price = float(item.get("p"))
                                size = float(item.get("s", 0))
                                ts = item.get("t") / 1000
                                trade_time = datetime.utcfromtimestamp(ts).replace(tzinfo=pytz.UTC)
                                await on_trade_event(symbol, price, size, trade_time)
                            elif item.get("ev") == "H":
                                await handle_halt_event(item)

                async def ws_symbols():
                    print("ws_symbols started")
                    while True:
                        new_symbols = await symbol_queue.get()
                        to_unsubscribe = [s for s in subscribed_symbols if s not in new_symbols]
                        to_subscribe = [s for s in new_symbols if s not in subscribed_symbols]
                        if not to_unsubscribe and not to_subscribe:
                            continue
                        BATCH_SIZE = 100
                        if to_unsubscribe:
                            for i in range(0, len(to_unsubscribe), BATCH_SIZE):
                                remove_params = ",".join([f"T.{s}" for s in to_unsubscribe[i:i+BATCH_SIZE]])
                                await ws.send(json.dumps({"action": "unsubscribe", "params": remove_params}))
                                await asyncio.sleep(0.2)
                        if to_subscribe:
                            for i in range(0, len(to_subscribe), BATCH_SIZE):
                                add_params = ",".join([f"T.{s}" for s in to_subscribe[i:i+BATCH_SIZE]])
                                await ws.send(json.dumps({"action": "subscribe", "params": add_params}))
                                await asyncio.sleep(0.2)
                        subscribed_symbols.clear()
                        subscribed_symbols.update(new_symbols)

                await asyncio.gather(ws_recv(), ws_symbols())
        except Exception as e:
            print(f"Trade WebSocket error: {e}. Reconnecting soon...")
            await asyncio.sleep(5)

async def main():
    print("main() started.")
    loop = asyncio.get_event_loop()
    setup_signal_handlers(loop)
    symbol_queue = asyncio.Queue()
    init_syms = await fetch_top_penny_symbols()
    print(f"Initial symbols: {init_syms[:10]}... total: {len(init_syms)}")
    await symbol_queue.put(init_syms)
    await asyncio.gather(
        dynamic_symbol_manager(symbol_queue),
        trade_ws(symbol_queue),
        send_scheduled_alerts(),
        news_alerts_task()
    )

if __name__ == "__main__":
    print("Starting real-time penny stock spike scanner ($10 & under, 4am–8pm ET, Mon–Fri) with RVOL, confidence scoring, and keyword-filtered news alerts as SEPARATE alerts (now only for price-moving stocks UP 5%+)...")
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit, asyncio.CancelledError) as e:
        print(f"Bot stopped gracefully. Reason: {e}")
    except Exception as e:
        print(f"Top-level exception: {e}")
