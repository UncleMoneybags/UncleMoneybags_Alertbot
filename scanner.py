import asyncio
import websockets
import aiohttp
import json
import html
import re
from collections import deque, defaultdict
from datetime import datetime, timedelta, time
import pytz
import signal

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

# Relative Volume config
rvol_history = defaultdict(lambda: deque(maxlen=20))
RVOL_MIN = 3.0

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

candles = defaultdict(lambda: deque(maxlen=3))
trade_candle_builders = defaultdict(list)
trade_candle_last_time = {}
last_alerted_price = {}
last_halt_alert = {}
news_seen = set()

# News keyword filter check
def news_matches_keywords(headline, summary):
    text_block = f"{headline} {summary}".lower()
    return any(word.lower() in text_block for word in KEYWORDS)

# Separate news alert
async def send_news_telegram_async(message):
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
                    print("Telegram send error (news):", await resp.text())
        except Exception as e:
            print(f"Telegram send error (news): {e}")

async def news_alerts_task():
    url = f"https://api.polygon.io/v2/reference/news?apiKey={POLYGON_API_KEY}&limit=50"
    global news_seen
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    data = await resp.json()
                    for item in data.get('results', []):
                        news_id = item['id']
                        if news_id in news_seen:
                            continue
                        headline = item.get('title', '')
                        summary = item.get('description', '')
                        if not news_matches_keywords(headline, summary):
                            continue
                        news_seen.add(news_id)
                        url_link = item.get('article_url', '')
                        msg = (
                            f"📰 <b>NEWS ALERT</b>\n"
                            f"<b>{escape_html(headline)}</b>\n"
                            f"{escape_html(summary)}\n"
                            f"<a href=\"{url_link}\">Read more</a>"
                        )
                        await send_news_telegram_async(msg)
            if len(news_seen) > 500:
                news_seen = set(list(news_seen)[-500:])
        except Exception as e:
            print(f"News fetch error: {e}")
        await asyncio.sleep(30)

async def on_trade_event(symbol, price, size, trade_time):
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
                    return days_since_ipo < MIN_IPO_DAYS
        except Exception as e:
            print(f"IPO check failed for {ticker}: {e}")
    return False

async def fetch_top_penny_symbols():
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
    return penny_symbols

current_symbols = set()

async def dynamic_symbol_manager(symbol_queue):
    global current_symbols
    while True:
        if is_market_scan_time():
            symbols = await fetch_top_penny_symbols()
            if symbols:
                if set(symbols) != current_symbols:
                    await symbol_queue.put(symbols)
                    current_symbols = set(symbols)
        await asyncio.sleep(SCREENER_REFRESH_SEC)

async def handle_halt_event(item):
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

async def trade_ws(symbol_queue):
    subscribed_symbols = set()
    while True:
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

                async def ws_recv():
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

def setup_signal_handlers(loop):
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(shutdown(loop, sig)))

async def shutdown(loop, sig):
    print(f"Received exit signal {sig.name}...")
    tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

async def send_scheduled_alerts():
    sent_open_msg = False
    sent_close_msg = False
    sent_ebook_msg = False
    while True:
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

async def main():
    loop = asyncio.get_event_loop()
    setup_signal_handlers(loop)
    symbol_queue = asyncio.Queue()
    init_syms = await fetch_top_penny_symbols()
    await symbol_queue.put(init_syms)
    await asyncio.gather(
        dynamic_symbol_manager(symbol_queue),
        trade_ws(symbol_queue),
        send_scheduled_alerts(),
        news_alerts_task()
    )

if __name__ == "__main__":
    print("Starting real-time penny stock spike scanner ($10 & under, 4am–8pm ET, Mon–Fri) with RVOL, confidence scoring, and keyword-filtered news alerts as SEPARATE alerts...")
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
        print("Bot stopped gracefully.")
