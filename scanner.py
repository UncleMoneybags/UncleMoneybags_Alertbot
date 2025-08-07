import logging
import asyncio
import websockets
import aiohttp
import json
import html
import re
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta, date, time as dt_time
import pytz
import signal
import pickle
import csv
import os
import joblib
import numpy as np
import atexit
import sys
import requests
from bs4 import BeautifulSoup
import pandas as pd  
import time

# --- Time-of-Day Volume Profile for RVOL Spike Alerts ---
PROFILE_FILE = "volume_profile.json"
DAYS_TO_KEEP = 20
MINUTES_PER_SESSION = 390  # 9:30-16:00

def get_minute_of_day(dt):
    return (dt.hour - 9) * 60 + (dt.minute - 30)

class VolumeProfile:
    def __init__(self):
        self.profile = defaultdict(lambda: defaultdict(list))
        self._load_profile()

    def _load_profile(self):
        if os.path.exists(PROFILE_FILE):
            with open(PROFILE_FILE, "r") as f:
                self.profile = json.load(f)
            for sym, by_min in self.profile.items():
                self.profile[sym] = {int(minidx): vols for minidx, vols in by_min.items()}
        else:
            self.profile = defaultdict(lambda: defaultdict(list))

    def _save_profile(self):
        serializable = {sym: {str(minidx): vols for minidx, vols in by_min.items()} for sym, by_min in self.profile.items()}
        with open(PROFILE_FILE, "w") as f:
            json.dump(serializable, f)

    def add_day(self, symbol, daily_candles):
        for candle in daily_candles:
            minute_idx = get_minute_of_day(candle['start_time'])
            vol = candle['volume']
            if minute_idx < 0 or minute_idx >= MINUTES_PER_SESSION:
                continue
            self.profile.setdefault(symbol, {}).setdefault(minute_idx, []).append(vol)
            if len(self.profile[symbol][minute_idx]) > DAYS_TO_KEEP:
                self.profile[symbol][minute_idx] = self.profile[symbol][minute_idx][-DAYS_TO_KEEP:]
        self._save_profile()

    def get_avg(self, symbol, minute_idx):
        vols = self.profile.get(symbol, {}).get(minute_idx, [])
        if vols:
            return sum(vols) / len(vols)
        return 1

    def get_rvol(self, symbol, minute_idx, curr_volume):
        avg_volume = self.get_avg(symbol, minute_idx)
        return curr_volume / avg_volume if avg_volume > 0 else 0

vol_profile = VolumeProfile()

EMA_PERIODS = [5, 8, 13]

def calculate_emas(prices, periods=[5, 8, 13], window=30):
    """
    Returns a dict with EMA values for specified periods, calculated on the last `window` prices.
    """
    prices = list(prices)[-window:]  # Use only last `window` candles
    s = pd.Series(prices)
    emas = {}
    for p in periods:
        emas[f"ema{p}"] = s.ewm(span=p, adjust=False).mean().to_numpy()
    return emas

def vwap_numpy(prices, volumes):
    prices = np.asarray(prices, dtype=float)
    volumes = np.asarray(volumes, dtype=float)
    total_vol = np.sum(volumes)
    return np.sum(prices * volumes) / total_vol if total_vol > 0 else 0.0

def vwap_candles_numpy(candles):
    if not candles:
        return 0.0
    prices = [(c['high'] + c['low'] + c['close']) / 3 for c in candles]
    volumes = [c['volume'] for c in candles]
    return vwap_numpy(prices, volumes)

def rsi(prices, period=14):
    prices = np.asarray(prices, dtype=float)
    if len(prices) < period + 1:
        return np.full_like(prices, np.nan)
    deltas = np.diff(prices)
    seed = deltas[:period]
    up = seed[seed > 0].sum() / period
    down = -seed[seed < 0].sum() / period
    rs = up / down if down != 0 else 0
    rsi_arr = np.zeros_like(prices)
    rsi_arr[:period] = 100. - 100. / (1. + rs)
    for i in range(period, len(prices)):
        delta = deltas[i - 1]
        upval = delta if delta > 0 else 0
        downval = -delta if delta < 0 else 0
        up = (up * (period - 1) + upval) / period
        down = (down * (period - 1) + downval) / period
        rs = up / down if down != 0 else 0
        rsi_arr[i] = 100. - 100. / (1. + rs)
    return rsi_arr

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

def polygon_time_to_utc(ts):
    return datetime.utcfromtimestamp(ts / 1000).replace(tzinfo=timezone.utc)

# --- FLOAT CACHE patch: cache negative results with retry cooldown ---
float_cache = {}
float_cache_none_retry = {}
FLOAT_CACHE_NONE_RETRY_MIN = 10  # minutes

def save_float_cache():
    with open("float_cache.pkl", "wb") as f:
        pickle.dump(float_cache, f)
    with open("float_cache_none.pkl", "wb") as f:
        pickle.dump(float_cache_none_retry, f)
    print(f"[DEBUG] Saved float cache, entries: {len(float_cache)}, none cache: {len(float_cache_none_retry)}")

def load_float_cache():
    global float_cache, float_cache_none_retry
    if os.path.exists("float_cache.pkl"):
        with open("float_cache.pkl", "rb") as f:
            float_cache = pickle.load(f)
        print(f"[DEBUG] Loaded float cache, entries: {len(float_cache)}")
    else:
        float_cache = {}
        print(f"[DEBUG] No float cache found, starting new.")
    if os.path.exists("float_cache_none.pkl"):
        with open("float_cache_none.pkl", "rb") as f:
            float_cache_none_retry = pickle.load(f)
        print(f"[DEBUG] Loaded float_cache_none_retry, entries: {len(float_cache_none_retry)}")
    else:
        float_cache_none_retry = {}
        print(f"[DEBUG] No float_cache_none_retry found, starting new.")

def get_float_shares(ticker):
    now = datetime.now(timezone.utc)
    # Check positive/real float first
    if ticker in float_cache and float_cache[ticker] is not None:
        return float_cache[ticker]
    # Check negative/None cache, only retry every N minutes
    if ticker in float_cache_none_retry:
        last_none = float_cache_none_retry[ticker]
        if (now - last_none).total_seconds() < FLOAT_CACHE_NONE_RETRY_MIN * 60:
            return None
    try:
        import yfinance as yf
        info = yf.Ticker(ticker).info
        float_shares = info.get('floatShares', None)
        if float_shares is not None:
            float_cache[ticker] = float_shares
            save_float_cache()
            if ticker in float_cache_none_retry:
                del float_cache_none_retry[ticker]
        else:
            float_cache_none_retry[ticker] = now
            save_float_cache()
        time.sleep(0.5)
        return float_shares
    except Exception as e:
        float_cache_none_retry[ticker] = now
        save_float_cache()
        if "Rate limited" in str(e):
            time.sleep(10)
        return float_cache.get(ticker, None)

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

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
if not POLYGON_API_KEY or not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    logger.critical("API keys or chat id missing in environment variables! Exiting.")
    sys.exit(1)

PRICE_THRESHOLD = 20.00
MAX_SYMBOLS = 4000
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
    scan_start = dt_time(4, 0)
    scan_end = dt_time(20, 0)
    return scan_start <= now_ny.time() <= scan_end

def is_news_alert_time():
    ny = pytz.timezone("America/New_York")
    now_utc = datetime.now(timezone.utc)
    now_ny = now_utc.astimezone(ny)
    if now_ny.weekday() >= 5:
        return False
    start = dt_time(7, 0)
    end = dt_time(20, 0)
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
runner_alerted_today = {}
below_vwap_streak = defaultdict(int)
vwap_reclaimed_once = defaultdict(bool)
dip_play_seen = set()
recent_high = defaultdict(float)

volume_spike_alerted = set()
rvol_spike_alerted = set()
halted_symbols = set()

ALERT_COOLDOWN_MINUTES = 10
last_alert_time = defaultdict(lambda: datetime.min.replace(tzinfo=timezone.utc))

warming_up_was_true = defaultdict(bool)
runner_was_true = defaultdict(bool)
dip_play_was_true = defaultdict(bool)
rug_pull_was_true = defaultdict(bool)
vwap_reclaim_was_true = defaultdict(bool)
volume_spike_was_true = defaultdict(bool)
ema_stack_was_true = defaultdict(bool)

def get_scanned_tickers():
    return set(candles.keys())

RUG_PULL_DROP_PCT = -0.10
RUG_PULL_BOUNCE_PCT = 0.05

vwap_candles = defaultdict(list)
vwap_session_date = defaultdict(lambda: None)

def get_session_date(dt):
    ny = pytz.timezone("America/New_York")
    dt_ny = dt.astimezone(ny)
    if dt_ny.time() < dt_time(4, 0):
        return dt_ny.date() - timedelta(days=1)
    return dt_ny.date()

def check_volume_spike(candles_seq, vwap_value):
    if len(candles_seq) < 4:
        return False
    curr_candle = list(candles_seq)[-1]
    curr_volume = curr_candle['volume']
    trailing_volumes = [c['volume'] for c in list(candles_seq)[-4:-1]]
    trailing_avg = sum(trailing_volumes) / 3 if len(trailing_volumes) == 3 else 1
    rvol = curr_volume / trailing_avg if trailing_avg > 0 else 0
    above_vwap = curr_candle['close'] > vwap_value
    if (
        curr_volume >= 125000 and
        rvol >= 2.0 and
        above_vwap
    ):
        return True
    return False

current_session_date = None

def get_ny_date():
    ny = pytz.timezone("America/New_York")
    now_utc = datetime.now(timezone.utc)
    now_ny = now_utc.astimezone(ny)
    return now_ny.date()

# --- PERFECT SETUP LOGIC START ---

def is_bullish_engulfing(candles_seq):
    if len(candles_seq) < 2:
        return False
    prev = candles_seq[-2]
    curr = candles_seq[-1]
    return (
        (prev["close"] < prev["open"]) and
        (curr["close"] > curr["open"]) and
        (curr["close"] > prev["open"]) and
        (curr["open"] < prev["close"])
    )

def calc_macd_hist(closes):
    # MACD(12,26,9)
    s = pd.Series(closes)
    ema12 = s.ewm(span=12, adjust=False).mean()
    ema26 = s.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    macd_hist = macd - signal
    return macd_hist.values

async def alert_perfect_setup(symbol, closes, volumes, highs, lows, candles_seq, vwap_value):
    # Use last 30 closes for indicators
    closes_np = np.array(closes)
    volumes_np = np.array(volumes)
    highs_np = np.array(highs)
    lows_np = np.array(lows)

    emas = calculate_emas(closes, periods=[5, 8, 13], window=30)
    ema5 = emas["ema5"][-1]
    ema8 = emas["ema8"][-1]
    ema13 = emas["ema13"][-1]

    # VWAP (already passed as vwap_value)
    last_close = closes[-1]
    last_volume = volumes[-1]

    # RVOL (relative to trailing 20)
    if len(volumes) >= 20:
        avg_vol20 = np.mean(volumes[-20:])
        rvol = last_volume / avg_vol20 if avg_vol20 > 0 else 0
    else:
        rvol = 0

    # RSI (14)
    rsi_vals = rsi(closes, period=14)
    last_rsi = rsi_vals[-1] if not np.isnan(rsi_vals[-1]) else 0

    # MACD Histogram
    macd_hist_vals = calc_macd_hist(closes)
    last_macd_hist = macd_hist_vals[-1]

    # Bullish Engulfing
    bullish_engulf = is_bullish_engulfing(candles_seq)

    # Perfect setup criteria
    perfect = (
        (ema5 > ema8 > ema13)
        and (ema5 >= 1.015 * ema13)
        and (last_close > vwap_value)
        and (ema5 > vwap_value)
        and (last_volume >= 175000)
        and (rvol > 2.5)
        and (last_rsi < 70)
        and (last_macd_hist > 0)
        and bullish_engulf
    )

    if perfect:
        # CLEAN, MINIMAL ALERT
        alert_text = (
            f"🚨 <b>PERFECT SETUP</b> 🚨\n"
            f"<b>{escape_html(symbol)}</b> | ${last_close:.2f} | Vol: {int(last_volume/1000)}K | RVOL: {rvol:.1f}\n\n"
            f"Trend: EMA5 > EMA8 > EMA13\n"
            f"{'Above VWAP' if last_close > vwap_value else 'Below VWAP'}"
            f" | MACD↑"
            f" | RSI: {int(round(last_rsi))}"
        )
        await send_telegram_async(alert_text)
        # Log as event
        now = datetime.now(timezone.utc)
        log_event(
            "perfect_setup",
            symbol,
            last_close,
            last_volume,
            now,
            {
                "ema5": ema5,
                "ema8": ema8,
                "ema13": ema13,
                "vwap": vwap_value,
                "rvol": rvol,
                "rsi14": last_rsi,
                "macd_hist": last_macd_hist,
                "bullish_engulfing": bullish_engulf
            }
        )
        # Avoid duplicate alerts (reuse EMA STACK logic flag)
        ema_stack_was_true[symbol] = True

# --- PERFECT SETUP LOGIC END ---

# --- PREMARKET GAINERS TRACKING (no float filter, tracks ALL symbols) ---
premarket_open_prices = {}  # symbol -> 4am price
premarket_last_prices = {}  # symbol -> latest price up to 9:25am
premarket_volumes = {}      # symbol -> cumulative volume since 4am

def is_premarket(dt):
    ny = pytz.timezone("America/New_York")
    dt_ny = dt.astimezone(ny)
    return dt_time(4, 0) <= dt_ny.time() < dt_time(9, 30)

async def on_new_candle(symbol, open_, high, low, close, volume, start_time):
    global current_session_date
    today_ny = get_ny_date()
    now = datetime.now(timezone.utc)
    if current_session_date != today_ny:
        current_session_date = today_ny
        alerted_symbols.clear()
        runner_alerted_today.clear()
        recent_high.clear()
        dip_play_seen.clear()
        halted_symbols.clear()
        print(f"[DEBUG] Reset alert state for new trading day: {today_ny}")

    # --- Track premarket prices/volumes for gainers list (NO FILTER) ---
    if is_premarket(start_time):
        if symbol not in premarket_open_prices:
            premarket_open_prices[symbol] = open_
            premarket_volumes[symbol] = 0
        premarket_last_prices[symbol] = close
        premarket_volumes[symbol] += volume

    # Regular alerting logic follows (float filter applies only to alerts, not to gainers list)
    if not isinstance(candles[symbol], deque):
        candles[symbol] = deque(candles[symbol], maxlen=20)
    if not isinstance(vwap_candles[symbol], list):
        vwap_candles[symbol] = list(vwap_candles[symbol])
    float_shares = get_float_shares(symbol)
    if float_shares is None or not (MIN_FLOAT_SHARES <= float_shares <= MAX_FLOAT_SHARES):
        logger.debug(f"Skipping {symbol} due to float {float_shares}")
        return
    if not is_market_scan_time() or close > 20.00:
        return
    today = datetime.now(timezone.utc).date()
    candles_seq = candles[symbol]
    event_time = datetime.now(timezone.utc)
    candles_seq.append({
        'open': open_,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume,
        'start_time': start_time
    })
    vwap_candles[symbol].append({
        'open': open_,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume,
        'start_time': start_time
    })

    # --- PERFECT SETUP SCANNER ---
    if len(candles_seq) >= 30:
        closes = [c['close'] for c in list(candles_seq)[-30:]]
        highs = [c['high'] for c in list(candles_seq)[-30:]]
        lows = [c['low'] for c in list(candles_seq)[-30:]]
        volumes = [c['volume'] for c in list(candles_seq)[-30:]]
        vwap_value = vwap_candles_numpy(list(vwap_candles[symbol]))
        # Only alert if not previously alerted this session
        if not ema_stack_was_true[symbol]:
            await alert_perfect_setup(symbol, closes, volumes, highs, lows, list(candles_seq)[-30:], vwap_value)

    # --- RVOL spike alert removed ---

    # Warming Up Logic
    if len(candles_seq) >= 6:
        last_6 = list(candles_seq)[-6:]
        volumes_5 = [c['volume'] for c in last_6[:-1]]
        avg_vol_5 = sum(volumes_5) / 5
        last_candle = last_6[-1]
        open_wu = last_candle['open']
        close_wu = last_candle['close']
        volume_wu = last_candle['volume']
        price_move_wu = (close_wu - open_wu) / open_wu if open_wu > 0 else 0
        vwap_wu = vwap_candles_numpy(vwap_candles[symbol]) if vwap_candles[symbol] else 0
        dollar_volume_wu = close_wu * volume_wu
        warming_up_criteria = (
            volume_wu >= 1.5 * avg_vol_5 and
            price_move_wu >= 0.03 and
            0.20 <= close_wu <= 20.00 and
            close_wu > vwap_wu and
            dollar_volume_wu >= 100_000
        )
        if warming_up_criteria and not warming_up_was_true[symbol]:
            if (now - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                return
            log_event("warming_up", symbol, close_wu, volume_wu, event_time, {
                "price_move": price_move_wu,
                "dollar_volume": dollar_volume_wu
            })
            price_str = f"{close_wu:.2f}"
            alert_text = (
                f"🌡️ <b>{escape_html(symbol)}</b> Warming Up\n"
                f"Current Price: ${price_str}"
            )
            await send_telegram_async(alert_text)
            warming_up_was_true[symbol] = True
            alerted_symbols[symbol] = today
            last_alert_time[symbol] = now

    # Runner Logic
    if len(candles_seq) >= 6:
        last_6 = list(candles_seq)[-6:]
        volumes_5 = [c['volume'] for c in last_6[:-1]]
        avg_vol_5 = sum(volumes_5) / 5
        last_candle = last_6[-1]
        open_rn = last_candle['open']
        close_rn = last_candle['close']
        volume_rn = last_candle['volume']
        price_move_rn = (close_rn - open_rn) / open_rn if open_rn > 0 else 0
        vwap_rn = vwap_candles_numpy(vwap_candles[symbol]) if vwap_candles[symbol] else 0
        runner_criteria = (
            volume_rn >= 2 * avg_vol_5 and
            price_move_rn >= 0.06 and
            close_rn >= 0.10 and
            close_rn > vwap_rn
        )
        if runner_criteria and not runner_was_true[symbol]:
            if (now - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                return
            log_event("runner", symbol, close_rn, volume_rn, event_time, {
                "price_move": price_move_rn
            })
            price_str = f"{close_rn:.2f}"
            alert_text = (
                f"🏃‍♂️ <b>{escape_html(symbol)}</b> Runner\n"
                f"Current Price: ${price_str}"
            )
            await send_telegram_async(alert_text)
            runner_was_true[symbol] = True
            runner_alerted_today[symbol] = today
            last_alert_time[symbol] = now

    # DIP PLAY LOGIC
    MIN_DIP_PCT = 0.10
    DIP_LOOKBACK = 10
    if len(candles_seq) >= DIP_LOOKBACK:
        highs = [c["high"] for c in list(candles_seq)[-DIP_LOOKBACK:]]
        rhigh = max(highs)
        recent_high[symbol] = rhigh
    if recent_high[symbol] > 0:
        dip_pct = (recent_high[symbol] - close) / recent_high[symbol]
        dip_play_criteria = (
            dip_pct >= MIN_DIP_PCT and close <= 20.00
        )
        if dip_play_criteria and len(candles_seq) >= 3:
            c1, c2, c3 = list(candles_seq)[-3:]
            higher_lows = c2["low"] > c1["low"] and c3["low"] > c2["low"]
            rising_volume = c2["volume"] > c1["volume"] and c3["volume"] > c2["volume"]
            dip_play_criteria = dip_play_criteria and higher_lows and rising_volume
            logger.info(f"[DIP PLAY DEBUG] {symbol}: dip_pct={dip_pct*100:.2f}% higher_lows={higher_lows} rising_volume={rising_volume}")
            if dip_play_criteria and not dip_play_was_true[symbol]:
                if (now - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                    return
                log_event("dip_play", symbol, close, volume, event_time, {
                    "dip_pct": dip_pct
                })
                price_str = f"{close:.2f}"
                alert_text = (
                    f"📉 <b>{escape_html(symbol)}</b> Dip Play\n"
                    f"Current Price: ${price_str}"
                )
                await send_telegram_async(alert_text)
                dip_play_was_true[symbol] = True
                dip_play_seen.add(symbol)
                alerted_symbols[symbol] = today
                last_alert_time[symbol] = now

    # Rug Pull Logic
    if len(candles_seq) >= 3:
        c0, c1, c2 = list(candles_seq)[-3:]
        drop_pct = (c1["close"] - c0["close"]) / c0["close"]
        bounce_pct = (c2["close"] - c1["close"]) / c1["close"]
        rug_pull_criteria = (
            drop_pct <= RUG_PULL_DROP_PCT and bounce_pct < RUG_PULL_BOUNCE_PCT and symbol in alerted_symbols and alerted_symbols[symbol] == today
        )
        if rug_pull_criteria and not rug_pull_was_true[symbol]:
            if (now - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                return
            log_event("rug_pull", symbol, c2['close'], c2['volume'], event_time, {
                "drop_pct": drop_pct,
                "bounce_pct": bounce_pct
            })
            price_str = f"{c2['close']:.2f}"
            alert_text = (
                f"⚠️ <b>{escape_html(symbol)}</b> Rug Pull\n"
                f"Current Price: ${price_str}"
            )
            await send_telegram_async(alert_text)
            rug_pull_was_true[symbol] = True
            last_alert_time[symbol] = now

    # VWAP Reclaim Logic
    if len(candles_seq) >= 2:
        prev_candle = list(candles_seq)[-2]
        curr_candle = list(candles_seq)[-1]
        prev_vwap = vwap_candles_numpy(list(vwap_candles[symbol])[:-1]) if len(vwap_candles[symbol]) >= 2 else None
        curr_vwap = vwap_candles_numpy(list(vwap_candles[symbol]))
        trailing_vols = [c['volume'] for c in list(candles_seq)[:-1]]
        rvol = 0
        if trailing_vols:
            avg_trailing = sum(trailing_vols[-20:]) / min(len(trailing_vols), 20)
            rvol = curr_candle['volume'] / avg_trailing if avg_trailing > 0 else 0
        vwap_reclaim_criteria = (
            prev_candle['close'] < (prev_vwap if prev_vwap is not None else prev_candle['close']) and
            curr_candle['close'] > (curr_vwap if curr_vwap is not None else curr_candle['close']) and
            curr_candle['volume'] >= 100_000 and
            rvol >= 2.0
        )
        if vwap_reclaim_criteria and not vwap_reclaim_was_true[symbol]:
            if (now - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                return
            log_event("vwap_reclaim", symbol, curr_candle['close'], curr_candle['volume'], event_time, {
                "rvol": rvol
            })
            price_str = f"{curr_candle['close']:.2f}"
            vwap_str = f"{curr_vwap:.2f}" if curr_vwap is not None else "?"
            vol_str = f"{curr_candle['volume']:,}"
            rvol_str = f"{rvol:.2f}"
            alert_text = (
                f"📈 <b>{escape_html(symbol)}</b> VWAP Reclaim!\n"
                f"Price: ${price_str} | VWAP: ${vwap_str}\n"
                f"1-min Vol: {vol_str}\n"
                f"RVOL: {rvol_str}"
            )
            await send_telegram_async(alert_text)
            vwap_reclaim_was_true[symbol] = True
            alerted_symbols[symbol] = today
            last_alert_time[symbol] = now

    # Volume Spike Logic
    vwap_value = vwap_candles_numpy(vwap_candles[symbol]) if vwap_candles[symbol] else 0
    if check_volume_spike(candles_seq, vwap_value) and not volume_spike_was_true[symbol]:
        if (now - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
            return
        price_str = f"{close:.2f}"
        alert_text = (
            f"🔥 <b>{escape_html(symbol)}</b> Volume Spike\n"
            f"Current Price: ${price_str}"
        )
        await send_telegram_async(alert_text)
        event_time = now
        log_event("volume_spike", symbol, close, volume, event_time, {
            "rvol": volume / (sum([c['volume'] for c in list(candles_seq)[-4:-1]]) / 3 if len(candles_seq) >= 4 else 1),
            "vwap": vwap_value
        })
        volume_spike_was_true[symbol] = True
        alerted_symbols[symbol] = today
        last_alert_time[symbol] = now

    # EMA STACK LOGIC
    if (
        float_shares is not None and
        float_shares <= 10_000_000 and
        len(candles_seq) >= max(EMA_PERIODS)
    ):
        closes = [c['close'] for c in list(candles_seq)[-30:]]  # last 30 closes for reliable EMAs
        emas = calculate_emas(closes, periods=[5, 8, 13], window=30)
        ema5 = emas['ema5'][-1]
        ema8 = emas['ema8'][-1]
        ema13 = emas['ema13'][-1]
        vwap_value = vwap_candles_numpy(vwap_candles[symbol])
        ema_stack_criteria = (
            ema5 > ema8 > ema13 and
            ema5 >= 1.015 * ema13 and
            closes[-1] > vwap_value and
            ema5 > vwap_value and
            list(candles_seq)[-1]['volume'] >= 175000
        )
        logger.info(f"[EMA STACK DEBUG] {symbol}: ema5={ema5:.2f}, ema8={ema8:.2f}, ema13={ema13:.2f}, vwap={vwap_value:.2f}, close={closes[-1]:.2f}, volume={list(candles_seq)[-1]['volume']}, criteria={ema_stack_criteria}")
        if ema_stack_criteria and not ema_stack_was_true[symbol]:
            if (now - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                return
            log_event("ema_stack", symbol, closes[-1], list(candles_seq)[-1]['volume'], event_time, {
                "ema5": ema5,
                "ema8": ema8,
                "ema13": ema13,
                "vwap": vwap_value
            })
            price_str = f"{closes[-1]:.2f}"
            alert_text = (
                f"⚡️ <b>{escape_html(symbol)}</b> EMA Stack\n"
                f"Current Price: ${price_str}\n"
                f"EMA5: {ema5:.2f}, EMA8: {ema8:.2f}, EMA13: {ema13:.2f}, VWAP: {vwap_value:.2f}"
            )
            await send_telegram_async(alert_text)
            ema_stack_was_true[symbol] = True
            alerted_symbols[symbol] = today
            last_alert_time[symbol] = now

# --- At end of each day, call this to update the time-of-day profile ---
def update_profile_for_day(symbol, day_candles):
    vol_profile.add_day(symbol, day_candles)

async def catalyst_news_alert_loop():
    global news_seen
    while True:
        tickers = list(get_scanned_tickers())
        for symbol in tickers:
            news_items = await get_ticker_news_yahoo(symbol)
            for title, link in news_items:
                if any(kw.lower() in title.lower() for kw in KEYWORDS):
                    news_id = f"{symbol}:{title}"
                    if news_id not in news_seen:
                        headline_fmt = highlight_keywords(title, KEYWORDS)
                        msg = (
                            f"📰 <b>{escape_html(symbol)}</b> {headline_fmt}\n"
                            f"{link}"
                        )
                        await send_telegram_async(msg)
                        event_time = datetime.now(timezone.utc)
                        log_event("news_alert", symbol, 0, 0, event_time, {
                            "headline": title,
                            "link": link
                        })
                        news_seen.add(news_id)
                        save_news_id(news_id)
        await asyncio.sleep(60)

# --- PREMARKET GAINERS ALERT LOOP (NO FLOAT FILTER) ---
async def premarket_gainers_alert_loop():
    eastern = pytz.timezone("America/New_York")
    sent_today = False
    while True:
        now_utc = datetime.now(timezone.utc)
        now_est = now_utc.astimezone(eastern)
        # Alert at 9:25am ET (use 9:24:55 to ensure delivery before open)
        if now_est.weekday() in range(0, 5):
            if (now_est.time().hour == 9 and 
                now_est.time().minute == 24 and 
                now_est.time().second >= 55 and not sent_today):
                logger.info("Sending premarket gainers alert at 9:24:55am ET")
                # Compute top 5 gainers from 4am open (NO FLOAT FILTER)
                gainers = []
                for sym in premarket_open_prices:
                    if sym in premarket_last_prices and premarket_open_prices[sym] > 0:
                        pct_gain = (premarket_last_prices[sym] - premarket_open_prices[sym]) / premarket_open_prices[sym] * 100
                        last_price = premarket_last_prices[sym]
                        total_vol = premarket_volumes.get(sym, 0)
                        if last_price <= 20 and total_vol >= 25000:
                            float_val = float_cache.get(sym)
                            float_str = f", Float: {float_val/1e6:.1f}M" if float_val else ""
                            gainers.append((sym, pct_gain, last_price, total_vol, float_str))
                gainers.sort(key=lambda x: x[1], reverse=True)
                top5 = gainers[:5]
                if top5:
                    gainers_text = "\n".join(
                        f"<b>{sym}</b>: {last_price:.2f} ({pct_gain:+.1f}%) Vol:{int(total_vol/1000)}K{float_str}"
                        for sym, pct_gain, last_price, total_vol, float_str in top5
                    )
                else:
                    gainers_text = "No premarket gainers found."
                msg = (
                    "Market opens in 5 mins...secure the damn bag!\n"
                    "Here are the top 5 premarket gainers (since 4am):\n"
                    f"{gainers_text}"
                )
                await send_telegram_async(msg)
                event_time = datetime.now(timezone.utc)
                log_event("premarket_gainers", "PREMARKET", 0, 0, event_time, {"gainers": gainers_text})
                sent_today = True
            if not (now_est.time().hour == 9 and now_est.time().minute == 24):
                sent_today = False
        else:
            sent_today = False
        await asyncio.sleep(1)

async def market_close_alert_loop():
    eastern = pytz.timezone("America/New_York")
    sent_today = False
    while True:
        now_utc = datetime.now(timezone.utc)
        now_est = now_utc.astimezone(eastern)
        if now_est.weekday() in (0, 1, 2, 3):
            if now_est.time() >= dt_time(20, 1) and not sent_today:
                await send_telegram_async("Market Closed. Reconvene in pre market tomorrow.")
                event_time = datetime.now(timezone.utc)
                log_event("market_close", "CLOSE", 0, 0, event_time)
                sent_today = True
        else:
            sent_today = False
        if now_est.time() < dt_time(20, 0):
            sent_today = False
        await asyncio.sleep(30)

async def handle_halt_event(event):
    symbol = event.get("sym")
    status = event.get("status")
    reason = event.get("reason", "")
    scanned = get_scanned_tickers()
    if symbol in scanned:
        msg = f"🛑 <b>{escape_html(symbol)}</b> HALTED\nReason: {escape_html(reason)}"
        await send_telegram_async(msg)
        event_time = datetime.now(timezone.utc)
        log_event("halt", symbol, 0, 0, event_time, {"status": status, "reason": reason})
        halted_symbols.add(symbol)
        with open(HALT_LOG_FILE, "a") as f:
            f.write(f"{datetime.now(timezone.utc).isoformat()},{symbol},{status},{reason}\n")
        logger.info(f"HALT ALERT sent for {symbol}")

async def handle_resume_event(event):
    symbol = event.get("sym")
    reason = event.get("reason", "")
    if symbol in halted_symbols:
        msg = f"🟢 <b>{escape_html(symbol)}</b> RESUMED\nReason: {escape_html(reason)}"
        await send_telegram_async(msg)
        event_time = datetime.now(timezone.utc)
        log_event("resume", symbol, 0, 0, event_time, {"reason": reason})
        halted_symbols.remove(symbol)

def highlight_keywords(title, keywords):
    words = set(kw.lower() for kw in keywords)
    def bold_match(word):
        for kw in words:
            pattern = r'\b(' + re.escape(kw) + r')\b'
            word = re.sub(pattern, r"<b>\1</b>", word, flags=re.IGNORECASE)
        return word
    return bold_match(title)

async def get_ticker_news_yahoo(ticker):
    url = f"https://finance.yahoo.com/quote/{ticker}/news?p={ticker}"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as resp:
                html_text = await resp.text()
        soup = BeautifulSoup(html_text, "html.parser")
        news_items = []
        for item in soup.find_all('li', class_='js-stream-content'):
            headline_tag = item.find('h3')
            if not headline_tag:
                continue
            title = headline_tag.text.strip()
            link_tag = headline_tag.find('a')
            if not link_tag or not link_tag.get('href'):
                continue
            link = link_tag['href']
            if not link.startswith('http'):
                link = f"https://finance.yahoo.com{link}"
            news_items.append((title, link))
        return news_items
    except Exception as e:
        logger.error(f"[NEWS SCRAPER ERROR] {ticker}: {e}")
        return []

async def ingest_polygon_events():
    url = "wss://socket.polygon.io/stocks"
    while True:
        try:
            async with websockets.connect(url) as ws:
                await ws.send(json.dumps({"action": "auth", "params": POLYGON_API_KEY}))
                await ws.send(json.dumps({"action": "subscribe", "params": "AM.*,status"}))
                print("Subscribed to: AM.* (all tickers) and status (halts/resumes)")
                while True:
                    msg = await ws.recv()
                    try:
                        data = json.loads(msg)
                        if isinstance(data, dict) and data.get("ev") == "status":
                            if data.get("status") == "halt":
                                await handle_halt_event(data)
                            elif data.get("status") == "resume":
                                await handle_resume_event(data)
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
                                print(f"[POLYGON] {symbol} {start_time} o:{open_} h:{high} l:{low} c:{close} v:{volume}")
                                candle = {
                                    "open": open_,
                                    "high": high,
                                    "low": low,
                                    "close": close,
                                    "volume": volume,
                                    "start_time": start_time,
                                }
                                if not isinstance(candles[symbol], deque):
                                    candles[symbol] = deque(candles[symbol], maxlen=20)
                                if not isinstance(vwap_candles[symbol], list):
                                    vwap_candles[symbol] = list(vwap_candles[symbol])
                                candles[symbol].append(candle)
                                session_date = get_session_date(candle['start_time'])
                                last_session = vwap_session_date[symbol]
                                if last_session != session_date:
                                    vwap_candles[symbol] = []
                                    vwap_session_date[symbol] = session_date
                                vwap_candles[symbol].append(candle)
                                vwap_cum_vol[symbol] += volume
                                vwap_cum_pv[symbol] += ((high + low + close) / 3) * volume
                                await on_new_candle(symbol, open_, high, low, close, volume, start_time)
                            elif event.get("ev") == "status" and event.get("status") == "halt":
                                await handle_halt_event(event)
                            elif event.get("ev") == "status" and event.get("status") == "resume":
                                await handle_resume_event(event)
                    except Exception as e:
                        print(f"Error processing message: {e}\nRaw: {msg}")
        except Exception as e:
            print(f"Websocket error: {e} — reconnecting in 10 seconds...")
            await asyncio.sleep(10)

async def main():
    print("Main event loop running. Press Ctrl+C to exit.")
    ingest_task = asyncio.create_task(ingest_polygon_events())
    close_alert_task = asyncio.create_task(market_close_alert_loop())
    premarket_alert_task = asyncio.create_task(premarket_gainers_alert_loop())
    catalyst_news_task = asyncio.create_task(catalyst_news_alert_loop())
    try:
        while True:
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        print("Main loop cancelled.")
    finally:
        ingest_task.cancel()
        close_alert_task.cancel()
        premarket_alert_task.cancel()
        catalyst_news_task.cancel()
        await ingest_task
        await close_alert_task
        await premarket_alert_task
        await catalyst_news_task

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
