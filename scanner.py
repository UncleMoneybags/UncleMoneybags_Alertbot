import logging
import asyncio
import websockets
import aiohttp
import json
import html
import re
from collections import deque, defaultdict
from datetime import datetime, time, timezone, timedelta, date
import pytz
import signal
import pickle
import csv
import os
import joblib
import numpy as np
import atexit
import sys  # for platform check
import requests
from bs4 import BeautifulSoup

# === INDICATORS: EMA & VWAP & RSI & Bollinger Bands ===
def ema(prices, period):
    prices = np.asarray(prices, dtype=float)
    ema = np.zeros_like(prices)
    alpha = 2 / (period + 1)
    ema[0] = prices[0]
    for i in range(1, len(prices)):
        ema[i] = alpha * prices[i] + (1 - alpha) * ema[i - 1]
    return ema

def vwap_numpy(prices, volumes):
    prices = np.asarray(prices, dtype=float)
    volumes = np.asarray(volumes, dtype=float)
    return np.sum(prices * volumes) / np.sum(volumes) if np.sum(volumes) > 0 else 0.0

def vwap_candles_numpy(candles):
    prices = [(c['high'] + c['low'] + c['close']) / 3 for c in candles]
    volumes = [c['volume'] for c in candles]
    return vwap_numpy(prices, volumes)

def rsi(prices, period=14):
    prices = np.asarray(prices, dtype=float)
    deltas = np.diff(prices)
    seed = deltas[:period]
    up = seed[seed > 0].sum() / period
    down = -seed[seed < 0].sum() / period
    rs = up / down if down != 0 else 0
    rsi = np.zeros_like(prices)
    rsi[:period] = 100. - 100. / (1. + rs)
    for i in range(period, len(prices)):
        delta = deltas[i - 1]
        upval = delta if delta > 0 else 0
        downval = -delta if delta < 0 else 0
        up = (up * (period - 1) + upval) / period
        down = (down * (period - 1) + downval) / period
        rs = up / down if down != 0 else 0
        rsi[i] = 100. - 100. / (1. + rs)
    return rsi

def bollinger_bands(prices, period=20, num_std=2):
    prices = np.asarray(prices, dtype=float)
    if len(prices) < period:
        return None, None, None
    sma = np.convolve(prices, np.ones(period)/period, mode='valid')
    std = np.array([np.std(prices[i-period:i]) for i in range(period, len(prices)+1)])
    upper_band = sma + num_std * std
    lower_band = sma - num_std * std
    pad = [None] * (len(prices) - len(lower_band))
    lower_band = pad + list(lower_band)
    upper_band = pad + list(upper_band)
    sma = pad + list(sma)
    return lower_band, sma, upper_band

float_cache = {}

def save_float_cache():
    with open("float_cache.pkl", "wb") as f:
        pickle.dump(float_cache, f)
    print(f"[DEBUG] Saved float cache, entries: {len(float_cache)}")

def load_float_cache():
    global float_cache
    if os.path.exists("float_cache.pkl"):
        with open("float_cache.pkl", "rb") as f:
            float_cache = pickle.load(f)
        print(f"[DEBUG] Loaded float cache, entries: {len(float_cache)}")
    else:
        float_cache = {}
        print(f"[DEBUG] No float cache found, starting new.")

def get_float_shares(ticker):
    if ticker in float_cache:
        print(f"[DEBUG] Cache HIT for {ticker}: {float_cache[ticker]}")
        return float_cache[ticker]
    print(f"[DEBUG] Cache MISS for {ticker}")
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        float_shares = info.get('floatShares', None)
        float_cache[ticker] = float_shares
        save_float_cache()
        print(f"[DEBUG] Cached float for {ticker}: {float_shares}")
        return float_shares
    except Exception as e:
        float_cache[ticker] = None
        save_float_cache()
        print(f"[DEBUG] Yahoo float error for {ticker}: {e}")
        return None

load_float_cache()

NEWS_SEEN_FILE = "news_seen.txt"

def load_news_seen():
    try:
        with open(NEWS_SEEN_FILE, "r") as f:
            return set(line.strip() for line in f if line.strip())
    except FileNotFoundError:
        return set()

def save_news_id(news_id):
    with open(NEWS_SEEN_FILE, "a") as f:
        f.write(news_id + "\n")

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s:%(name)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("scanner")

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    logger.warning("yfinance not installed. Run 'pip install yfinance' for float filtering.")
    YFINANCE_AVAILABLE = False

logger.info("scanner.py is running!!! --- If you see this, your file is found and started.")
logger.info("Imports completed successfully.")

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "VmF1boger0pp2M7gV5HboHheRbplmLi5")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "-1002266463234")
PRICE_THRESHOLD = 20.00
MAX_SYMBOLS = 1000
SCREENER_REFRESH_SEC = 30
MIN_ALERT_MOVE = 0.15
MIN_3MIN_VOLUME = 25000
MIN_PER_CANDLE_VOL = 25000
MIN_IPO_DAYS = 30
ALERT_PRICE_DELTA = 0.25
RVOL_SPIKE_THRESHOLD = 2.5
RVOL_SPIKE_MIN_VOLUME = 25000

MIN_FLOAT_SHARES = 500_000
MAX_FLOAT_SHARES = 10_000_000

vwap_cum_vol = defaultdict(float)
vwap_cum_pv = defaultdict(float)
rvol_history = defaultdict(lambda: deque(maxlen=20))
RVOL_MIN = 2.0

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
    logger.info("Loaded ML runner model.")
except Exception as e:
    logger.warning(f"Could not load runner model: {e}")
    runner_clf = None

def score_event_ml(event_type, symbol, price, volume, rvol, prepost):
    if runner_clf is None:
        return 0.0
    event_type_code = 0 if event_type == "spike" else 1
    prepost_f = float(prepost)
    X = np.array([[price, volume if volume is not None else 0, rvol if rvol is not None else 1.0, prepost_f, event_type_code]])
    prob = runner_clf.predict_proba(X)[0,1]
    return prob

KEYWORDS = [
    "offering", "FDA", "approval", "acquisition", "merger", "bankruptcy", "delisting", "reverse split", "split",
    "halt", "investigation", "lawsuit", "earnings", "guidance", "clinical", "phase 1", "phase 2", "phase 3",
    "partnership", "contract", "dividend", "buyback", "sec", "subpoena", "settlement", "short squeeze", "recall",
    "resigns", "appoints", "collaboration", "sec filing", "patent", "discontinued", "withdraw", "spike", "upsize",
    "pricing", "withdraws", "grants", "fires", "director", "ceo", "cfo"
]

def is_market_scan_time():
    ny = pytz.timezone("America/New_York")
    now_utc = datetime.now(timezone.utc)
    now_ny = now_utc.astimezone(ny)
    if now_ny.weekday() >= 5:
        return False
    scan_start = time(4, 0)
    scan_end = time(20, 0)
    return scan_start <= now_ny.time() <= scan_end

def is_news_alert_time():
    ny = pytz.timezone("America/New_York")
    now_utc = datetime.now(timezone.utc)
    now_ny = now_utc.astimezone(ny)
    if now_ny.weekday() >= 5:
        return False
    start = time(7, 0)
    end = time(20, 0)
    return start <= now_ny.time() <= end

async def send_telegram_async(message):
    logger.debug(f"[DEBUG] send_telegram_async called. Message: {message}")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": False
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, data=payload, timeout=10) as resp:
                result_text = await resp.text()
                logger.debug(f"[DEBUG] Telegram API status: {resp.status}, response: {result_text}")
                if resp.status != 200:
                    logger.error(f"[DEBUG] Telegram send error: {result_text}")
    except Exception as e:
        logger.error(f"[DEBUG] Telegram send error: {e}")

def escape_html(s):
    return html.escape(s or "")

candles = defaultdict(lambda: deque(maxlen=20))
trade_candle_builders = defaultdict(list)
trade_candle_last_time = {}
last_alerted_price = {}
last_halt_alert = {}
news_seen = load_news_seen()
latest_news_time = None

last_volume_spike_time = defaultdict(lambda: datetime.min.replace(tzinfo=timezone.utc))
last_runner_alert_time = defaultdict(lambda: datetime.min.replace(tzinfo=timezone.utc))
runner_alerted_today = set()

pending_runner_alert = {}
HALT_LOG_FILE = "halt_event_log.csv"

alerted_symbols = {}
below_vwap_streak = defaultdict(int)
vwap_reclaimed_once = defaultdict(bool)
dip_play_seen = set()
recent_high = defaultdict(float)

volume_spike_alerted = set()  # not used
rvol_spike_alerted = set()

async def check_ema_stack_alert(symbol, candles, ema5, ema8, ema13, vwap, float_shares):
    try:
        if (
            float_shares is not None and float_shares <= 10_000_000 and
            ema5 > ema8 > ema13 and
            (ema5 / ema13) >= 1.01 and
            candles[-1]['volume'] >= 20_000
        ):
            alert_msg = (
                f"⚡️ <b>{escape_html(symbol)}</b> EMA STACK ALERT\n"
                f"EMA5: {ema5:.2f}, EMA8: {ema8:.2f}, EMA13: {ema13:.2f}, VWAP: {vwap:.2f}\n"
                f"Ratio EMA5/EMA13: {ema5/ema13:.2f}"
            )
            await send_telegram_async(alert_msg)
            alerted_symbols[symbol] = datetime.now(timezone.utc).date()
            logger.info(f"EMA STACK ALERT sent for {symbol}")
    except Exception as e:
        logger.error(f"EMA STACK ALERT error for {symbol}: {e}")

RUG_PULL_DROP_PCT = -0.10
RUG_PULL_BOUNCE_PCT = 0.05

async def on_new_candle(symbol, open_, high, low, close, volume, start_time):
    float_shares = get_float_shares(symbol)
    if float_shares is None or not (MIN_FLOAT_SHARES <= float_shares <= MAX_FLOAT_SHARES):
        logger.debug(f"Skipping {symbol} due to float {float_shares}")
        return

    logger.debug(f"on_new_candle: {symbol} - open:{open_}, close:{close}, volume:{volume}")
    if not is_market_scan_time() or close > 20.00:
        return

    today = datetime.now(timezone.utc).date()

    # --- WARMING UP LOGIC PATCH (STRICT) ---
    if len(candles_seq := candles[symbol]) >= 6:
        last_6 = list(candles_seq)[-6:]
        volumes_5 = [c['volume'] for c in last_6[:-1]]
        avg_vol_5 = sum(volumes_5) / 5
        last_candle = last_6[-1]
        open_wu = last_candle['open']
        close_wu = last_candle['close']
        volume_wu = last_candle['volume']
        price_move_wu = (close_wu - open_wu) / open_wu if open_wu > 0 else 0
        vwap_wu = vwap_cum_pv[symbol] / vwap_cum_vol[symbol] if vwap_cum_vol[symbol] > 0 else 0
        dollar_volume_wu = close_wu * volume_wu

        if (
            volume_wu >= 1.5 * avg_vol_5 and
            price_move_wu >= 0.02 and
            close_wu >= 0.50 and
            close_wu > vwap_wu and
            dollar_volume_wu >= 100_000
        ):
            msg = f"🌡️ <b>{escape_html(symbol)}</b> Warming Up ${close_wu:.2f}"
            await send_telegram_async(msg)
            alerted_symbols[symbol] = today

    # --- RUNNER ALERT LOGIC (STRICT, ONLY AFTER WARMING UP) ---
    if len(candles_seq) >= 6 and symbol in alerted_symbols and alerted_symbols[symbol] == today:
        last_6 = list(candles_seq)[-6:]
        volumes_5 = [c['volume'] for c in last_6[:-1]]
        avg_vol_5 = sum(volumes_5) / 5
        last_candle = last_6[-1]
        open_rn = last_candle['open']
        close_rn = last_candle['close']
        volume_rn = last_candle['volume']
        price_move_rn = (close_rn - open_rn) / open_rn if open_rn > 0 else 0

        if (
            volume_rn >= 2 * avg_vol_5 and
            price_move_rn >= 0.08 and
            close_rn >= 0.50 and
            (not last_runner_alert_time[symbol] or last_runner_alert_time[symbol] < today)
        ):
            msg = f"🏃‍♂️ <b>{escape_html(symbol)}</b> Runner Alert ${close_rn:.2f}"
            await send_telegram_async(msg)
            last_runner_alert_time[symbol] = today

    # --- OVERSOLD BOUNCE ALERT (RSI < 30, price <= lower Bollinger Band, float <= 10M, green candle) ---
    if (
        float_shares is not None and
        float_shares <= 10_000_000 and
        len(candles_seq) >= 20
    ):
        closes = [c['close'] for c in candles_seq]
        rsi_val = rsi(closes)[-1]
        lower_band, sma, upper_band = bollinger_bands(closes, period=20, num_std=2)
        last_candle = candles_seq[-1]
        if (
            rsi_val < 30 and
            lower_band[-1] is not None and
            closes[-1] <= lower_band[-1] and
            last_candle['close'] > last_candle['open']
        ):
            msg = f"🏀 <b>{escape_html(symbol)}</b> Oversold ${close:.2f}"
            await send_telegram_async(msg)
            alerted_symbols[symbol] = today

    # Dip Play Alert
    MIN_DIP_PCT = 0.10
    DIP_LOOKBACK = 10
    if len(candles_seq) >= DIP_LOOKBACK:
        highs = [c["high"] for c in list(candles_seq)[-DIP_LOOKBACK:]]
        rhigh = max(highs)
        recent_high[symbol] = rhigh
    if recent_high[symbol] > 0:
        dip_pct = (recent_high[symbol] - close) / recent_high[symbol]
        if dip_pct >= MIN_DIP_PCT and symbol not in dip_play_seen and close <= 20.00:
            if len(candles_seq) >= 3:
                c1, c2, c3 = list(candles_seq)[-3:]
                higher_lows = c2["low"] > c1["low"] and c3["low"] > c2["low"]
                rising_volume = c2["volume"] > c1["volume"] and c3["volume"] > c2["volume"]
                logger.info(f"[DIP PLAY DEBUG] {symbol}: dip_pct={dip_pct*100:.2f}% higher_lows={higher_lows} rising_volume={rising_volume}")
                if higher_lows and rising_volume:
                    msg = f"📉 <b>{escape_html(symbol)}</b> Dip Play Alert! ${close:.2f}"
                    await send_telegram_async(msg)
                    dip_play_seen.add(symbol)
                    alerted_symbols[symbol] = today

    # --- RUG PULL WARNING (ONLY FOR SYMBOLS ALERTED TODAY) ---
    if len(candles_seq) >= 3:
        c0, c1, c2 = list(candles_seq)[-3:]
        drop_pct = (c1["close"] - c0["close"]) / c0["close"]
        if drop_pct <= RUG_PULL_DROP_PCT:
            bounce_pct = (c2["close"] - c1["close"]) / c1["close"]
            if bounce_pct < RUG_PULL_BOUNCE_PCT:
                if symbol in alerted_symbols and alerted_symbols[symbol] == today:
                    rug_msg = f"⚠️ <b>{escape_html(symbol)}</b> Rug Pull Warning: Now ${c2['close']:.2f}."
                    await send_telegram_async(rug_msg)

    # VWAP Reclaim Alert
    if len(candles_seq) >= 2:
        prev_candle = list(candles_seq)[-2]
        prev_close = prev_candle['close']
        prev_vwap_numerator = vwap_cum_pv[symbol] - close * volume
        prev_vwap_denominator = vwap_cum_vol[symbol] - volume
        prev_vwap = prev_vwap_numerator / prev_vwap_denominator if prev_vwap_denominator > 0 else None

        trailing_vols = [c['volume'] for c in list(candles_seq)[:-1]]
        rvol = 0
        if trailing_vols:
            avg_trailing = sum(trailing_vols[-20:]) / min(len(trailing_vols), 20)
            rvol = volume / avg_trailing if avg_trailing > 0 else 0

        if (
            prev_close < (prev_vwap if prev_vwap is not None else prev_close) and
            close > (vwap_cum_pv[symbol] / vwap_cum_vol[symbol] if vwap_cum_vol[symbol] > 0 else close) and
            volume >= 100_000 and
            rvol >= 2.0 and
            not vwap_reclaimed_once[symbol]
        ):
            msg = (
                f"🔄 <b>{escape_html(symbol)}</b> VWAP Reclaim!\n"
                f"Price: ${close:.2f} | VWAP: ${(vwap_cum_pv[symbol] / vwap_cum_vol[symbol]) if vwap_cum_vol[symbol] > 0 else 0:.2f}\n"
                f"1-min Vol: {volume:,}\n"
                f"RVOL: {rvol:.2f} (float {float_shares:,})"
            )
            await send_telegram_async(msg)
            vwap_reclaimed_once[symbol] = True
            alerted_symbols[symbol] = today

    # --- RVOL SPIKE ALERT (ONCE PER TICKER PER SESSION, LABELLED AS VOLUME SPIKE WITH FIRE EMOJI) ---
    MIN_PRICE_MOVE_PCT = 0.08
    if symbol not in rvol_spike_alerted and len(candles_seq) == 3:
        c0, c1, c2 = list(candles_seq)
        total_volume = c0["volume"] + c1["volume"] + c2["volume"]
        rvol_history[symbol].append(total_volume)
        rvol_hist_seq = rvol_history[symbol]
        if not isinstance(rvol_hist_seq, (list, deque)):
            rvol_hist_seq = list(rvol_hist_seq)
        if len(rvol_hist_seq) >= 5:
            trailing_vols = list(rvol_hist_seq)[:-1]
            if trailing_vols:
                avg_trailing = sum(trailing_vols) / len(trailing_vols)
                if avg_trailing > 0:
                    rvol = total_volume / avg_trailing
                    logger.info(f"{symbol} RVOL: {rvol}")

                    price_move_pct = (c2["close"] - c0["close"]) / c0["close"] if c0["close"] > 0 else 0
                    if (
                        rvol >= RVOL_SPIKE_THRESHOLD and
                        total_volume >= RVOL_SPIKE_MIN_VOLUME and
                        price_move_pct >= MIN_PRICE_MOVE_PCT
                    ):
                        msg = f"🔥 <b>{escape_html(symbol)}</b> Volume Spike ${c2['close']:.2f}"
                        await send_telegram_async(msg)
                        rvol_spike_alerted.add(symbol)
                        alerted_symbols[symbol] = today
                    if rvol < RVOL_MIN:
                        return

    logger.debug(f"ALERT DEBUG: {symbol} c0={candles_seq[0]['close'] if len(candles_seq)>2 else ''} ...")

    # --- EMA STACK ALERT (relaxed, float <= 10M) ---
    if (
        float_shares is not None and
        float_shares <= 10_000_000 and
        len(candles_seq) >= 13
    ):
        closes = [c['close'] for c in candles_seq]
        ema5 = ema(closes, 5)[-1]
        ema8 = ema(closes, 8)[-1]
        ema13 = ema(closes, 13)[-1]
        vwap_value = vwap_numpy(closes, [c['volume'] for c in candles_seq])
        await check_ema_stack_alert(symbol, candles_seq, ema5, ema8, ema13, vwap_value, float_shares)

def polygon_time_to_utc(ts):
    return datetime.utcfromtimestamp(ts / 1000).replace(tzinfo=timezone.utc)

# --- Yahoo Finance Premarket Gainers Scraper ---
async def get_premarket_gainers_yahoo():
    url = "https://finance.yahoo.com/premarket/"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                html_text = await resp.text()
                soup = BeautifulSoup(html_text, "html.parser")
                table = soup.find("table")
                gainers = []
                if table and table.find("tbody"):
                    for row in table.find("tbody").find_all("tr")[:10]:  # Top 10 gainers
                        cols = row.find_all("td")
                        if len(cols) >= 5:
                            ticker = cols[0].text.strip()
                            last_price = cols[2].text.strip()
                            percent = cols[4].text.strip()
                            gainers.append(f"<b>{ticker}</b>: {last_price} ({percent})")
                return gainers if gainers else ["No premarket gainers found."]
    except Exception as e:
        logger.error(f"Premarket gainers fetch error: {e}")
        return ["Error fetching gainers."]

# --- Premarket Gainers Alert Loop ---
async def premarket_gainers_alert_loop():
    eastern = pytz.timezone("America/New_York")
    sent_today = False
    while True:
        now_utc = datetime.now(timezone.utc)
        now_est = now_utc.astimezone(eastern)
        # Monday=0, ..., Friday=4
        if now_est.weekday() in range(0, 5):
            if now_est.time().hour == 9 and now_est.time().minute >= 25 and not sent_today:
                gainers = await get_premarket_gainers_yahoo()
                gainers_text = "\n".join(gainers)
                msg = (
                    "Market opens in 5 mins...secure the damn bag!\n"
                    "Heres premarket top gainers list.\n"
                    f"{gainers_text}"
                )
                await send_telegram_async(msg)
                sent_today = True
        else:
            sent_today = False
        # Reset for new day
        if now_est.time() < time(9, 20):
            sent_today = False
        await asyncio.sleep(30)

# --- Market Close Alert Loop ---
async def market_close_alert_loop():
    eastern = pytz.timezone("America/New_York")
    sent_today = False
    while True:
        now_utc = datetime.now(timezone.utc)
        now_est = now_utc.astimezone(eastern)
        # Monday=0, ..., Thursday=3
        if now_est.weekday() in (0, 1, 2, 3):
            if now_est.time() >= time(20, 1) and not sent_today:
                await send_telegram_async("Market Closed. Reconvene in pre market tomorrow.")
                sent_today = True
        else:
            sent_today = False
        # Reset for new day
        if now_est.time() < time(20, 0):
            sent_today = False
        await asyncio.sleep(30)

async def ingest_polygon_minute_bars():
    url = "wss://socket.polygon.io/stocks"
    async with websockets.connect(url) as ws:
        await ws.send(json.dumps({"action": "auth", "params": POLYGON_API_KEY}))
        await ws.send(json.dumps({"action": "subscribe", "params": "AM.*"}))

        print("Subscribed to: AM.* (all tickers)")
        while True:
            msg = await ws.recv()
            try:
                data = json.loads(msg)
                if isinstance(data, dict) and data.get("ev") == "status" and data.get("status") == "auth_success":
                    print("Polygon authentication successful.")
                if not isinstance(data, list):
                    continue
                for event in data:
                    if event.get("ev") == "AM":
                        symbol = event["sym"]
                        open_ = event["o"]
                        high = event["h"]
                        low = event["l"]
                        close = event["c"]
                        volume = event["v"]
                        start_time = polygon_time_to_utc(event["s"])
                        await on_new_candle(symbol, open_, high, low, close, volume, start_time)
            except Exception as e:
                print(f"Error processing message: {e}\nRaw: {msg}")

async def main():
    print("Main event loop running. Press Ctrl+C to exit.")
    ingest_task = asyncio.create_task(ingest_polygon_minute_bars())
    close_alert_task = asyncio.create_task(market_close_alert_loop())
    premarket_alert_task = asyncio.create_task(premarket_gainers_alert_loop())
    try:
        while True:
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        print("Main loop cancelled.")
    finally:
        ingest_task.cancel()
        close_alert_task.cancel()
        premarket_alert_task.cancel()
        await ingest_task
        await close_alert_task
        await premarket_alert_task

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
