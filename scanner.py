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
MAX_SYMBOLS = 500
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

async def on_new_candle(symbol, open_, high, low, close, volume, start_time):
    if not is_market_scan_time() or close > PRICE_THRESHOLD:
        return

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
            msg = (
                f"🚨 {escape_html(symbol)} stock price up ${c[3].close - c[0].close:.2f} over last 3 min candles.\n"
                f"From ${c[0].close:.2f} to ${c[3].close:.2f}."
            )
            print(f"ALERT: {symbol} triggers spike condition!")  # Debug print
            await send_telegram_async(msg)

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
    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey={POLYGON_API_KEY}&limit=1000"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                data = await resp.json()
                print("Raw Polygon snapshot response:", json.dumps(data)[:2000])  # Debug output
                if "tickers" not in data:
                    print("Tickers snapshot API response:", data)
                    return []
                for stock in data.get("tickers", []):
                    ticker = stock.get("ticker")
                    last_trade = stock.get("lastTrade", {})
                    day = stock.get("day", {})
                    price = None
                    price_time = 0

                    # Use latest valid price between last trade and day close
                    if last_trade and last_trade.get("p", 0) > 0:
                        price = last_trade["p"]
                        price_time = last_trade.get("t", 0)
                    if day and day.get("c", 0) > 0 and (not price or day.get("t", 0) > price_time):
                        price = day["c"]

                    # Only add actual penny stocks: price > 0 and <= 5
                    if price is not None and 0 < price <= PRICE_THRESHOLD:
                        penny_symbols.add(ticker)
                        print(f"Adding {ticker} at ${price:.2f} to scan list")  # Debug
                        if len(penny_symbols) >= MAX_SYMBOLS:
                            break
        except Exception as e:
            print("Tickers snapshot fetch error:", e)
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
            else:
                print("No penny stock symbols found. Retrying...")
        else:
            print("Market not open for scanning. Waiting...")
        await asyncio.sleep(SCREENER_REFRESH_SEC)

async def trade_ws(symbol_queue):
    subscribed_symbols = set()
    while True:
        try:
            uri = "wss://socket.polygon.io/stocks"
            async with websockets.connect(uri, ping_interval=30, ping_timeout=10) as ws:
                await ws.send(json.dumps({"action": "auth", "params": POLYGON_API_KEY}))
                print("Trade WebSocket opened.")
                symbols = await symbol_queue.get()
                if not symbols:
                    print("No symbols to subscribe to, skipping websocket subscription.")
                    await asyncio.sleep(SCREENER_REFRESH_SEC)
                    continue
                params = ",".join([f"T.{s}" for s in symbols])
                await ws.send(json.dumps({"action": "subscribe", "params": params}))
                subscribed_symbols = set(symbols)
                print(f"Subscribed to {len(symbols)} symbols.")

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
                            print(f"Trade: {symbol}, price={price}, size={size}, time={trade_time}")  # Debug
                            await on_trade_event(symbol, price, size, trade_time)

                async def ws_symbols():
                    while True:
                        new_symbols = await symbol_queue.get()
                        to_unsubscribe = [s for s in subscribed_symbols if s not in new_symbols]
                        to_subscribe = [s for s in new_symbols if s not in subscribed_symbols]
                        # Only resubscribe if there's a change
                        if not to_unsubscribe and not to_subscribe:
                            print("No symbol changes; skipping resubscription.")
                            continue
                        if to_unsubscribe:
                            remove_params = ",".join([f"T.{s}" for s in to_unsubscribe])
                            await ws.send(json.dumps({"action": "unsubscribe", "params": remove_params}))
                            await asyncio.sleep(0.5)  # Give WS time to process
                        if to_subscribe:
                            add_params = ",".join([f"T.{s}" for s in to_subscribe])
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
    # TEST YOUR TELEGRAM ALERTS: Uncomment the next line to force a test message!
    # asyncio.run(send_telegram_async("Test alert from penny scanner"))
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
        print("Bot stopped gracefully.")
