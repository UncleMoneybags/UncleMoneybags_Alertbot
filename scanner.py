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

# === INDICATORS: EMA & VWAP ===
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

# Load cache at the start!
load_float_cache()

# ==== NEWS SEEN PERSISTENCE ====
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

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s:%(name)s:%(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("scanner")

# ==== PATCH: Yahoo Finance Imports for Float Filtering ====
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    logger.warning("yfinance not installed. Run 'pip install yfinance' for float filtering.")
    YFINANCE_AVAILABLE = False

logger.info("scanner.py is running!!! --- If you see this, your file is found and started.")
logger.info("Imports completed successfully.")

# --- CONFIG ---
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

candles = defaultdict(lambda: deque(maxlen=3))
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

alerted_symbols = set()
below_vwap_streak = defaultdict(int)
vwap_reclaimed_once = defaultdict(bool)
dip_play_seen = set()
recent_high = defaultdict(float)

# ---- PATCH: Volume & RVOL spike alert once-per-ticker-per-session ----
volume_spike_alerted = set()
rvol_spike_alerted = set()

# === EMA STACK ALERT ===
async def check_ema_stack_alert(symbol, candles, ema5, ema8, ema13, vwap):
    try:
        if (
            ema5 > ema8 > ema13 and
            ema5 > vwap and ema8 > vwap and ema13 > vwap and
            (ema5 / ema13) >= 1.03 and
            candles[-1]['volume'] >= 100_000
        ):
            alert_msg = (
                f"⚡️ <b>{escape_html(symbol)}</b> EMA STACK ALERT\n"
                f"EMA5: {ema5:.2f}, EMA8: {ema8:.2f}, EMA13: {ema13:.2f}, VWAP: {vwap:.2f}\n"
                f"Ratio EMA5/EMA13: {ema5/ema13:.2f}\n"
                f"1-min Vol: {candles[-1]['volume']:,}"
            )
            await send_telegram_async(alert_msg)
            logger.info(f"EMA STACK ALERT sent for {symbol}")
    except Exception as e:
        logger.error(f"EMA STACK ALERT error for {symbol}: {e}")

RUG_PULL_DROP_PCT = -0.10
RUG_PULL_BOUNCE_PCT = 0.05

async def on_new_candle(symbol, open_, high, low, close, volume, start_time):
    logger.debug(f"on_new_candle: {symbol} - open:{open_}, close:{close}, volume:{volume}")
    if not is_market_scan_time() or close > 20.00:
        return

    today = datetime.now(timezone.utc).date()

    # --- VOLUME SPIKE ALERT (ONCE PER TICKER PER SESSION) ---
    if symbol not in volume_spike_alerted:
        if len(candles[symbol]) >= 5:
            prev_vols = [c['volume'] for c in list(candles[symbol])[-5:]]
            avg_prev_5 = sum(prev_vols) / 5
            if volume >= 150_000 and volume >= 2 * avg_prev_5:
                msg = f"🔊 <b>{escape_html(symbol)}</b> Volume Spike! ${close:.2f}"
                await send_telegram_async(msg)
                volume_spike_alerted.add(symbol)

    if not isinstance(candles[symbol], deque):
        logger.error(f"[DEBUG] ERROR: candles[{symbol}] not deque. Found type: {type(candles[symbol])}. Value: {candles[symbol]}")
        candles[symbol] = deque([candles[symbol]], maxlen=3)

    candles[symbol].append({
        "open": open_,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
        "start_time": start_time,
    })

    vwap_cum_vol[symbol] += volume
    vwap_cum_pv[symbol] += close * volume
    vwap = vwap_cum_pv[symbol] / vwap_cum_vol[symbol] if vwap_cum_vol[symbol] > 0 else None
    candles_seq = candles[symbol]

    # Dip Play Alert (unchanged)
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

    # Rug Pull Warning (unchanged)
    if len(candles_seq) >= 3:
        c0, c1, c2 = list(candles_seq)[-3:]
        drop_pct = (c1["close"] - c0["close"]) / c0["close"]
        if drop_pct <= RUG_PULL_DROP_PCT:
            bounce_pct = (c2["close"] - c1["close"]) / c1["close"]
            if bounce_pct < RUG_PULL_BOUNCE_PCT:
                if symbol in alerted_symbols:
                    rug_msg = f"⚠️ <b>{escape_html(symbol)}</b> Rug Pull Warning: Now ${c2['close']:.2f}."
                    await send_telegram_async(rug_msg)

    # VWAP Reclaim Alert (unchanged)
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
            close > (vwap if vwap is not None else close) and
            volume >= 100_000 and
            rvol >= 2.0 and
            not vwap_reclaimed_once[symbol]
        ):
            msg = (
                f"🔄 <b>{escape_html(symbol)}</b> VWAP Reclaim!\n"
                f"Price: ${close:.2f} | VWAP: ${vwap:.2f}\n"
                f"1-min Vol: {volume:,}\n"
                f"RVOL: {rvol:.2f}"
            )
            await send_telegram_async(msg)
            vwap_reclaimed_once[symbol] = True

    # --- RVOL SPIKE ALERT (ONCE PER TICKER PER SESSION) ---
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
                        msg = f"🔥 <b>{escape_html(symbol)}</b> RVOL Volume Spike ${c2['close']:.2f}"
                        await send_telegram_async(msg)
                        rvol_spike_alerted.add(symbol)
                    if rvol < RVOL_MIN:
                        return

    logger.debug(f"ALERT DEBUG: {symbol} c0={candles_seq[0]['close'] if len(candles_seq)>2 else ''} ...")

    # (Other alert blocks unchanged...)

    # EMA STACK ALERT (unchanged except bolded ticker)
    if len(candles_seq) >= 13:
        closes = [c['close'] for c in candles_seq]
        ema5 = ema(closes, 5)[-1]
        ema8 = ema(closes, 8)[-1]
        ema13 = ema(closes, 13)[-1]
        vwap_value = vwap_numpy(closes, [c['volume'] for c in candles_seq])
        if (
            ema5 > ema8 > ema13 and
            ema5 > vwap_value and ema8 > vwap_value and ema13 > vwap_value and
            (ema5 / ema13) >= 1.03 and
            candles_seq[-1]['volume'] >= 100_000
        ):
            alert_msg = (
                f"⚡️ <b>{escape_html(symbol)}</b> EMA STACK ALERT\n"
                f"EMA5: {ema5:.2f}, EMA8: {ema8:.2f}, EMA13: {ema13:.2f}, VWAP: {vwap_value:.2f}\n"
                f"Ratio EMA5/EMA13: {ema5/ema13:.2f}\n"
                f"1-min Vol: {candles_seq[-1]['volume']:,}"
            )
            await send_telegram_async(alert_msg)

# --- Main event loop to keep script alive ---
async def main():
    print("Main event loop running. Press Ctrl+C to exit.")
    while True:
        await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
