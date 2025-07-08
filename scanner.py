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
MAX_SYMBOLS = 100  # Batch size for Polygon WebSocket stability
SCREENER_REFRESH_SEC = 60
MIN_ALERT_MOVE = 0.15  # Only alert if move is at least 15 cents
MIN_3MIN_VOLUME = 10000  # Alert only if at least this much total volume in last 3 min
MIN_PER_CANDLE_VOL = 1000  # Optional: per candle minimum volume

def is_market_scan_time():
    ny = pytz.timezone("America/New_York")
    now_utc = datetime.now(pytz.UTC)
    now_ny = now_utc.astimezone(ny)
    print(f"[DEBUG] NY time: {now_ny}, NY weekday: {now_ny.weekday()}")
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

# Robust candle and trade logic
candles = defaultdict(lambda: deque(maxlen=3))  # Store 3 finalized 1-min candles per symbol
trade_candle_builders = defaultdict(list)  # Store trades for current minute per symbol
trade_candle_last_time = {}  # Track last finalized minute per symbol

async def on_trade_event(symbol, price, size, trade_time):
    # Always round down to minute
    candle_time = trade_time.replace(second=0, microsecond=0)
    last_time = trade_candle_last_time.get(symbol)
    if last_time is not None and candle_time != last_time:
        # Finalize previous minute's candle
        trades = trade_candle_builders[symbol]
        if trades:
            # Sort trades by their timestamp!
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
    # Save every trade with its timestamp
    trade_candle_builders[symbol].append((price, size, trade_time))
    trade_candle_last_time[symbol] = candle_time

async def on_new_candle(symbol, open_, high, low, close, volume, start_time):
    if not is_market_scan_time() or close > PRICE_THRESHOLD:
        return
    # Store candle as dict for clarity
    candles[symbol].append({
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "start_time": start_time,
    })
    print(f"[DEBUG] {symbol} new candle: open={open_}, close={close}, volume={volume}, time={start_time}")
    if len(candles[symbol]) == 3:
        c0, c1, c2 = candles[symbol]
        total_volume = c0["volume"] + c1["volume"] + c2["volume"]
        print(f"[DEBUG] {symbol} 3m closes: {c0['close']}, {c1['close']}, {c2['close']} | volumes: {c0['volume']}, {c1['volume']}, {c2['volume']} | total_vol: {total_volume}")
        # True three green candles, min move, and enough total volume
        if (
            c0["close"] < c1["close"] < c2["close"]
            and (c2["close"] - c0["close"]) >= MIN_ALERT_MOVE
            and total_volume >= MIN_3MIN_VOLUME
            and c0["volume"] >= MIN_PER_CANDLE_VOL
            and c1["volume"] >= MIN_PER_CANDLE_VOL
            and c2["volume"] >= MIN_PER_CANDLE_VOL
        ):
            move = c2["close"] - c0["close"]
            msg = (
                f"🚨 {escape_html(symbol)} up ${move:.2f} ({total_volume:,} shares/3min).\n"
                f"Now ${c2['close']:.2f}."
            )
            print(f"ALERT: {symbol} 3 green candles, {total_volume} shares!")  # Debug print
            await send_telegram_async(msg)

def is_equity_symbol(ticker):
    # Exclude symbols ending with W, WS, U, R, P, or containing a dot (.)
    if re.search(r'(\.|\bW$|\bWS$|\bU$|\bR$|\bP$)', ticker):
        return False
    if ticker.endswith(('W', 'U', 'R', 'P', 'WS')) or '.' in ticker:
        return False
    # Exclude some known penny warrant/rights patterns
    if any(x in ticker for x in ['WS', 'W', 'U', 'R', 'P', '.']):
        return False
    return True

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
                    volume = day.get("v", 0)
                    # Only add actual penny stocks: price > 0 and <= 10
                    if price is not None and 0 < price <= PRICE_THRESHOLD:
                        penny_symbols.append(ticker)
                        print(f"Adding {ticker} at ${price:.2f}, vol={volume} to scan list")
                        if len(penny_symbols) >= MAX_SYMBOLS:
                            break
        except Exception as e:
            print("Tickers snapshot fetch error:", e)
    print(f"Fetched {len(penny_symbols)} penny stock symbols to scan.")
    return penny_symbols

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

                # Batch subscribe to avoid policy violation
                BATCH_SIZE = 100
                params_batches = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
                for batch in params_batches:
                    params = ",".join([f"T.{s}" for s in batch])
                    await ws.send(json.dumps({"action": "subscribe", "params": params}))
                    await asyncio.sleep(0.2)  # Prevent rate limit

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
                        # Unsubscribe and subscribe in batches
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
    print("Starting real-time penny stock spike scanner ($10 & under, 4am–8pm ET, Mon–Fri)...")
    # Uncomment this line to test Telegram delivery
    # asyncio.run(send_telegram_async("Test alert from penny scanner"))
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
        print("Bot stopped gracefully.")
