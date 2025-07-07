import asyncio
import websockets
import aiohttp
import json
import html
from collections import deque, defaultdict
from datetime import datetime, timedelta, time
import pytz
import signal

# --- CONFIG ---
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"
PRICE_THRESHOLD = 5.00
MAX_SYMBOLS = 500  # Polygon Advanced Plan max per connection
SCREENER_REFRESH_SEC = 60

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

class Candle:
    def __init__(self, open_, high, low, close, volume, start_time):
        self.open = open_
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.start_time = start_time

candles = defaultdict(lambda: deque(maxlen=4))  # 4 to check previous 3 and current
first_alert_sent = set()
hod_tracker = {}  # symbol -> (high of day, hod_date)

async def on_new_candle(symbol, open_, high, low, close, volume, start_time):
    if not is_market_scan_time() or close > PRICE_THRESHOLD:
        return

    # Track high of day per symbol (reset at day start)
    ny = pytz.timezone("America/New_York")
    now_ny = datetime.now(ny).date()
    hod, hod_date = hod_tracker.get(symbol, (None, None))
    if hod_date != now_ny:
        hod = None  # reset high of day for new day
    if hod is None or high > hod:
        hod = high
        hod_date = now_ny
    hod_tracker[symbol] = (hod, hod_date)

    candles[symbol].append(Candle(open_, high, low, close, volume, start_time))

    if len(candles[symbol]) < 4:
        return

    c = candles[symbol]
    # Check 3 consecutive green candles with rising volume
    if (c[0].close < c[1].close < c[2].close and
        c[0].volume < c[1].volume < c[2].volume):
        # Price spike: latest close >= prev close + $0.10
        price_spike = c[3].close >= c[2].close + 0.10
        # Volume spike: latest volume >= 2x average of previous 3 green candles
        avg_vol = (c[0].volume + c[1].volume + c[2].volume) / 3
        volume_spike = c[3].volume >= 2 * avg_vol
        if price_spike and volume_spike:
            if symbol not in first_alert_sent:
                msg = (
                    f"🚨 {escape_html(symbol)} stock price up ${c[3].close - c[0].close:.2f} over last 3 min candles.\n"
                    f"From ${c[0].close:.2f} to ${c[3].close:.2f}."
                )
                await send_telegram_async(msg)
                first_alert_sent.add(symbol)
                hod_tracker[symbol] = (max(hod, c[3].close), hod_date)
            else:
                # Only alert on new high of day (💰 emoji)
                hod, _ = hod_tracker[symbol]
                if c[3].close > hod:
                    msg = f"💰 {escape_html(symbol)} new high of day: ${c[3].close:.2f}"
                    await send_telegram_async(msg)
                    hod_tracker[symbol] = (c[3].close, hod_date)

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

async def fetch_top_penny_symbols():
    penny_symbols = set()
    url_gainers = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers?apiKey={POLYGON_API_KEY}"
    url_losers = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/losers?apiKey={POLYGON_API_KEY}"
    async with aiohttp.ClientSession() as session:
        for url in [url_gainers, url_losers]:
            try:
                async with session.get(url) as resp:
                    if resp.status != 200:
                        print("Polygon snapshot error:", await resp.text())
                        continue
                    data = await resp.json()
                    for stock in data.get("tickers", []):
                        last = stock.get("last", {}).get("price")
                        ticker = stock.get("ticker")
                        if last is not None and last <= PRICE_THRESHOLD:
                            penny_symbols.add(ticker)
                        if len(penny_symbols) >= MAX_SYMBOLS:
                            break
            except Exception as e:
                print("Snapshot fetch error:", e)
    print(f"Fetched {len(penny_symbols)} penny stock symbols to scan.")
    return list(penny_symbols)[:MAX_SYMBOLS]

async def dynamic_symbol_manager(symbol_queue):
    current_symbols = set()
    while True:
        if is_market_scan_time():
            symbols = await fetch_top_penny_symbols()
            if symbols:
                if set(symbols) != current_symbols:
                    await symbol_queue.put(symbols)
                    current_symbols = set(symbols)
        await asyncio.sleep(SCREENER_REFRESH_SEC)

async def trade_ws(symbol_queue):
    ws = None
    subscribed_symbols = set()
    async def subscribe_symbols(ws, symbols):
        if not symbols:
            return
        params = ",".join([f"T.{s}" for s in symbols])
        await ws.send(json.dumps({"action": "subscribe", "params": params}))
        print(f"Subscribed to {len(symbols)} symbols.")

    while True:
        try:
            uri = "wss://socket.polygon.io/stocks"
            async with websockets.connect(uri, ping_interval=30, ping_timeout=10) as ws:
                await ws.send(json.dumps({"action": "auth", "params": POLYGON_API_KEY}))
                print("Trade WebSocket opened.")
                symbols = await symbol_queue.get()
                await subscribe_symbols(ws, symbols)
                subscribed_symbols = set(symbols)
                async def ws_recv():
                    async for message in ws:
                        payload = json.loads(message)
                        if not isinstance(payload, list):
                            payload = [payload]
                        for item in payload:
                            if item.get("ev") != "T":
                                continue
                            symbol = item.get("sym")
                            price = float(item.get("p"))
                            size = float(item.get("s", 0))
                            ts = item.get("t") / 1000
                            trade_time = datetime.utcfromtimestamp(ts).replace(tzinfo=pytz.UTC)
                            await on_trade_event(symbol, price, size, trade_time)
                async def ws_symbols():
                    while True:
                        new_symbols = await symbol_queue.get()
                        remove_syms = [s for s in subscribed_symbols if s not in new_symbols]
                        if remove_syms:
                            remove_params = ",".join([f"T.{s}" for s in remove_syms])
                            await ws.send(json.dumps({"action": "unsubscribe", "params": remove_params}))
                        add_syms = [s for s in new_symbols if s not in subscribed_symbols]
                        if add_syms:
                            add_params = ",".join([f"T.{s}" for s in add_syms])
                            await ws.send(json.dumps({"action": "subscribe", "params": add_params}))
                        subscribed_symbols.clear()
                        subscribed_symbols.update(new_symbols)
                        print(f"Dynamically updated to {len(new_symbols)} symbols.")
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

async def main():
    loop = asyncio.get_event_loop()
    setup_signal_handlers(loop)
    symbol_queue = asyncio.Queue()
    init_syms = await fetch_top_penny_symbols()
    await symbol_queue.put(init_syms)
    await asyncio.gather(
        dynamic_symbol_manager(symbol_queue),
        trade_ws(symbol_queue)
    )

if __name__ == "__main__":
    print("Starting real-time penny stock spike scanner ($5 & under, 4am–8pm ET, Mon–Fri)...")
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
        print("Bot stopped gracefully.")
