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
    # Check green candles with increasing volume for 3 previous candles (c[0], c[1], c[2])
    if (c[0].close < c[1].close < c[2].close and
        c[0].volume < c[1].volume < c[2].volume):
        # Check price spike: latest close >= prev close + $0.10
        price_spike = c[3].close >= c[2].close + 0.10
        # Volume spike: latest volume >= 2x average of previous 3 green candles
        avg_vol = (c[0].volume + c[1].volume + c[2].volume) / 3
        volume_spike = c[3].volume >= 2 * avg_vol
        if price_spike and volume_spike:
            msg = (
                f"🚨 <b>${escape_html(symbol)}</b> SPIKE ALERT!\n"
                f"Time: {c[3].start_time.strftime('%Y-%m-%d %H:%M')}\n"
                f"Open: ${c[3].open:.2f}, High: ${c[3].high:.2f}, Low: ${c[3].low:.2f}, Close: ${c[3].close:.2f}\n"
                f"Volume: {c[3].volume:,}\n"
                f"Prev close: ${c[2].close:.2f}, Prev avg vol: {int(avg_vol):,}\n"
                f"3 consecutive green candles with rising volume, then spike!\n"
            )
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
    await ws_connect_loop(trade_ws, "TRADE")

if __name__ == "__main__":
    print("Starting penny stock spike alert scanner ($5 & under, 4am–8pm ET, Mon–Fri)...")
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
        print("Bot stopped gracefully.")
