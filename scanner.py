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
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
print("POLYGON_API_KEY:", POLYGON_API_KEY)
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
import joblib
import numpy as np
import atexit
import sys
import requests
from bs4 import BeautifulSoup
import pandas as pd  
import time

# --- PATCH: Track the latest real trade price and size for each symbol ---
last_trade_price = defaultdict(lambda: None)
last_trade_volume = defaultdict(lambda: 0)
last_trade_time = defaultdict(lambda: None)

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

def calculate_emas(prices, periods=[5, 8, 13], window=30, symbol=None):
    prices = list(prices)[-window:]  # Use only last `window` candles
    s = pd.Series(prices)
    emas = {}
    logger.info(f"[EMA DEBUG] {symbol if symbol else ''} | Input closes: {prices}")
    for p in periods:
        emas[f"ema{p}"] = s.ewm(span=p, adjust=False).mean().to_numpy()
        logger.info(f"[EMA DEBUG] {symbol if symbol else ''} | EMA{p} array: {emas[f'ema{p}']}")
        logger.info(f"[EMA DEBUG] {symbol if symbol else ''} | EMA{p} latest: {emas[f'ema{p}'][-1]}")
    return emas

def vwap_numpy(prices, volumes):
    prices = np.asarray(prices, dtype=float)
    volumes = np.asarray(volumes, dtype=float)
    total_vol = np.sum(volumes)
    return np.sum(prices * volumes) / total_vol if total_vol > 0 else 0.0

def vwap_candles_numpy(candles):
    if not candles:
        logger.info("[VWAP DEBUG] No candles, returning 0")
        return 0.0
    prices = [(c['high'] + c['low'] + c['close']) / 3 for c in candles]
    volumes = [c['volume'] for c in candles]
    logger.info(f"[VWAP DEBUG] Prices used: {prices}")
    logger.info(f"[VWAP DEBUG] Volumes used: {volumes}")
    vwap_val = vwap_numpy(prices, volumes)
    logger.info(f"[VWAP DEBUG] VWAP result: {vwap_val}")
    return vwap_val

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

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY","VmF1boger0pp2M7gV5HboHheRbplmLi5") 
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN","8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc") 
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID","-1002266463234") 
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
    logger.info(
        f"[DEBUG] {curr_candle.get('symbol', '?')} | Volume Spike Check | "
        f"Curr Volume={curr_volume}, Trailing Avg={trailing_avg}, RVOL={rvol:.2f}, Above VWAP={above_vwap}, VWAP={vwap_value:.2f}, Candle Close={curr_candle['close']}"
    )
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

def get_display_price(symbol, fallback):
    price = last_trade_price[symbol]
    logger.info(
        f"[PRICE DEBUG] {symbol} | Using trade price={price} | Fallback price={fallback}"
    )
    return price if price is not None else fallback

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
    s = pd.Series(closes)
    ema12 = s.ewm(span=12, adjust=False).mean()
    ema26 = s.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    macd_hist = macd - signal
    return macd_hist.values

async def alert_perfect_setup(symbol, closes, volumes, highs, lows, candles_seq, vwap_value):
    closes_np = np.array(closes)
    volumes_np = np.array(volumes)
    highs_np = np.array(highs)
    lows_np = np.array(lows)

    emas = calculate_emas(closes, periods=[5, 8, 13], window=30, symbol=symbol)
    ema5 = emas["ema5"][-1]
    ema8 = emas["ema8"][-1]
    ema13 = emas["ema13"][-1]

    last_close = closes[-1]
    last_volume = volumes[-1]

    if len(volumes) >= 20:
        avg_vol20 = np.mean(volumes[-20:])
        rvol = last_volume / avg_vol20 if avg_vol20 > 0 else 0
    else:
        rvol = 0

    rsi_vals = rsi(closes, period=14)
    last_rsi = rsi_vals[-1] if not np.isnan(rsi_vals[-1]) else 0
    macd_hist_vals = calc_macd_hist(closes)
    last_macd_hist = macd_hist_vals[-1]

    bullish_engulf = is_bullish_engulfing(candles_seq)

    logger.info(f"[ALERT DEBUG] {symbol} | Perfect Setup Check | EMA5={ema5}, EMA8={ema8}, EMA13={ema13}, VWAP={vwap_value}, Last Close={last_close}, Last Volume={last_volume}, RVOL={rvol}, RSI={last_rsi}, MACD Hist={last_macd_hist}, Bullish Engulf={bullish_engulf}")
    logger.info(f"[DEBUG] {symbol} | Session candles: {len(candles_seq)} | Candle times: {candles_seq[0]['start_time'] if candles_seq else 'n/a'} - {candles_seq[-1]['start_time'] if candles_seq else 'n/a'}")
    logger.info(f"[DEBUG] {symbol} | All candle closes: {closes}")
    logger.info(f"[DEBUG] {symbol} | All candle volumes: {volumes}")

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

    # ---- PATCH: enforce ratio at alert time! ----
    if perfect:
        if ema5 / ema13 < 1.015:
            logger.error(
                f"[BUG] Perfect Setup alert would have fired but ratio invalid! {symbol}: ema5={ema5:.4f}, ema13={ema13:.4f}, ratio={ema5/ema13:.4f} (should be >= 1.015)"
            )
            return
        alert_text = (
            f"🚨 <b>PERFECT SETUP</b> 🚨\n"
            f"<b>{escape_html(symbol)}</b> | ${get_display_price(symbol, last_close):.2f} | Vol: {int(last_volume/1000)}K | RVOL: {rvol:.1f}\n\n"
            f"Trend: EMA5 > EMA8 > EMA13\n"
            f"{'Above VWAP' if last_close > vwap_value else 'Below VWAP'}"
            f" | MACD↑"
            f" | RSI: {int(round(last_rsi))}"
        )
        await send_telegram_async(alert_text)
        now = datetime.now(timezone.utc)
        log_event(
            "perfect_setup",
            symbol,
            get_display_price(symbol, last_close),
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
        ema_stack_was_true[symbol] = True

# --- EMA STACK LOGIC PATCH (WITH RATIO ENFORCEMENT + DEBUG PRINT SANITY CHECK) ---
def ema_stack_alert_logic(symbol, candles_seq, float_shares, vwap_candles, last_trade_volume, last_trade_price, ema_stack_was_true, last_alert_time, ALERT_COOLDOWN_MINUTES, event_time, alerted_symbols):
    if (
        float_shares is not None and
        float_shares <= 10_000_000 and
        len(candles_seq) >= max(EMA_PERIODS)
    ):
        closes = [c['close'] for c in list(candles_seq)[-30:]]  # last 30 closes for reliable EMAs
        emas = calculate_emas(closes, periods=[5, 8, 13], window=30, symbol=symbol)
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
        logger.info(f"[EMA STACK DEBUG] {symbol}: ema5={ema5:.2f}, ema8={ema8:.2f}, ema13={ema13:.2f}, vwap={vwap_value:.2f}, close={closes[-1]:.2f}, volume={list(candles_seq)[-1]['volume']}, ratio={ema5/ema13:.4f}, criteria={ema_stack_criteria}")
        logger.info(
            f"[ALERT DEBUG] {symbol} | Alert Type: ema_stack | VWAP={vwap_value:.4f} | Last Trade={last_trade_price[symbol]} | Candle Close={closes[-1]} | Candle Volume={list(candles_seq)[-1]['volume']}"
        )
        if ema_stack_criteria and not ema_stack_was_true[symbol]:
            # SANITY CHECK PRINT
            print(
                f"EMA STACK ALERT SANITY CHECK: symbol={symbol} | ema5={ema5:.6f} | ema8={ema8:.6f} | ema13={ema13:.6f} | vwap={vwap_value:.6f} | close={closes[-1]:.6f} | volume={list(candles_seq)[-1]['volume']} | last_trade_price={last_trade_price[symbol]}"
            )
            if ema5 / ema13 < 1.015:
                logger.error(
                    f"[BUG] EMA stack alert would have fired but ratio invalid! {symbol}: ema5={ema5:.4f}, ema13={ema13:.4f}, ratio={ema5/ema13:.4f} (should be >= 1.015)"
                )
                return
            if last_trade_volume[symbol] < 100:
                logger.info(f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})")
                return
            if (datetime.now(timezone.utc) - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                return
            log_event("ema_stack", symbol, get_display_price(symbol, closes[-1]), list(candles_seq)[-1]['volume'], event_time, {
                "ema5": ema5,
                "ema8": ema8,
                "ema13": ema13,
                "vwap": vwap_value
            })
            price_str = f"{get_display_price(symbol, closes[-1]):.2f}"
            alert_text = (
                f"⚡️ <b>{escape_html(symbol)}</b> EMA Stack\n"
                f"Current Price: ${price_str}\n"
                f"EMA5: {ema5:.2f}, EMA8: {ema8:.2f}, EMA13: {ema13:.2f}, VWAP: {vwap_value:.2f}"
            )
            asyncio.create_task(send_telegram_async(alert_text))
            ema_stack_was_true[symbol] = True
            alerted_symbols[symbol] = datetime.now(timezone.utc).date()
            last_alert_time[symbol] = datetime.now(timezone.utc)

# --- Your main event loop and websocket ingest logic would call ema_stack_alert_logic(...) in the right place ---


# ... rest of your code unchanged ...
