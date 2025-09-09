import logging
import asyncio
import websockets
import aiohttp
import json
import html
import re
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta, date, time as dt_time
from email.utils import parsedate_to_datetime
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

MARKET_OPEN = dt_time(4, 0)
MARKET_CLOSE = dt_time(20, 0)
eastern = pytz.timezone("America/New_York")  
logger = logging.getLogger(__name__)

last_trade_price = defaultdict(lambda: None)
last_trade_volume = defaultdict(lambda: 0)
last_trade_time = defaultdict(lambda: None)

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
# --- END FLOAT PATCH ---

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

# --- STORED EMA SYSTEM ---
class RunningEMA:
    def __init__(self, period):
        self.period = period
        self.multiplier = 2 / (period + 1)
        self.ema = None
        self.initialized = False
    
    def update(self, price):
        if not self.initialized:
            self.ema = price
            self.initialized = True
        else:
            self.ema = (price * self.multiplier) + (self.ema * (1 - self.multiplier))
        return self.ema
    
    def get_value(self):
        return self.ema if self.initialized else None
    
    def reset(self):
        self.ema = None
        self.initialized = False

# Storage for running EMAs by symbol
stored_emas = defaultdict(lambda: {
    5: RunningEMA(5),
    8: RunningEMA(8),
    13: RunningEMA(13),
    200: RunningEMA(200)
})

EMA_PERIODS = [5, 8, 13]

def update_stored_emas(symbol, price):
    """Update stored EMAs with new price and return current values"""
    emas = {}
    for period in [5, 8, 13, 200]:
        ema_value = stored_emas[symbol][period].update(price)
        emas[f"ema{period}"] = ema_value
    
    logger.info(f"[STORED EMA] {symbol} | Price: {price:.4f} | EMA5: {emas['ema5']:.4f} | EMA8: {emas['ema8']:.4f} | EMA13: {emas['ema13']:.4f} | EMA200: {emas['ema200']:.4f}")
    return emas

def get_stored_emas(symbol, periods=[5, 8, 13]):
    """Get current stored EMA values without updating"""
    emas = {}
    for period in periods:
        ema_obj = stored_emas[symbol][period]
        if ema_obj.initialized:
            emas[f"ema{period}"] = ema_obj.get_value()
        else:
            emas[f"ema{period}"] = None
    return emas

def reset_stored_emas(symbol):
    """Reset stored EMAs for new session"""
    for period in [5, 8, 13, 200]:
        stored_emas[symbol][period].reset()
    logger.info(f"[STORED EMA] {symbol} | Reset all EMAs for new session")

# Legacy function for backwards compatibility
def calculate_emas(prices, periods=[5, 8, 13], window=30, symbol=None, latest_trade_price=None):
    """Legacy EMA calculation - kept for fallback"""
    prices = list(prices)[-window:]
    if latest_trade_price is not None and len(prices) > 0:
        prices[-1] = latest_trade_price
    s = pd.Series(prices)
    emas = {}
    logger.info(f"[LEGACY EMA] {symbol if symbol else ''} | Using legacy calculation")
    for p in periods:
        emas[f"ema{p}"] = s.ewm(span=p, adjust=False).mean().to_numpy()
        logger.info(f"[LEGACY EMA] {symbol if symbol else ''} | EMA{p} latest: {emas[f'ema{p}'][-1]}")
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

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY","e0m1UiAaR4426tNCHHkDYflvpG1Qd3XN") 
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN","8019146040:AAGwaTuYBj8n7iNpnGF8m4wn9KpdZA0kbfA") 
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID","-1002266463234") 

# --- DISCORD WEBHOOK (add yours here) ---
DISCORD_WEBHOOK_URL = "https://discordapp.com/api/webhooks/1405716607111528600/E-BShgFYwkQadlqYWfeuYCgiFMirI4nSMZ_O7fTtrX29RKhcodeJ7zcXCCUd17EtBOkZ"

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

# Exception list for hot stocks that bypass float restrictions
FLOAT_EXCEPTION_SYMBOLS = {"OCTO", "GME", "AMC", "BBBY", "DWAC"}  # Add high-volume stocks that may be outside normal float range

vwap_cum_vol = defaultdict(float)
vwap_cum_pv = defaultdict(float)
rvol_history = defaultdict(lambda: deque(maxlen=20))
RVOL_MIN = 2.0

EVENT_LOG_FILE = "event_log.csv"
OUTCOME_LOG_FILE = "outcome_log.csv"

# --- AUTO ML TRAINING SYSTEM ---
recent_alerts = {}  # symbol -> alert_time
alert_prices = {}   # symbol -> entry_price
alert_types = {}    # symbol -> alert_type
market_context_cache = {}  # Cache market data

def get_market_context():
    """Collect broader market data for ML training"""
    now = datetime.now(timezone.utc)
    
    # Cache market data for 1 minute to avoid excessive API calls
    if 'last_update' in market_context_cache:
        if (now - market_context_cache['last_update']).total_seconds() < 60:
            return market_context_cache['data']
    
    try:
        # Get market metrics (simplified version - expand as needed)
        ny_time = now.astimezone(pytz.timezone("America/New_York"))
        market_open = dt_time(9, 30)
        
        if ny_time.time() >= market_open:
            minutes_since_open = (ny_time.hour - 9) * 60 + (ny_time.minute - 30)
        else:
            minutes_since_open = 0
            
        context = {
            "minutes_since_open": minutes_since_open,
            "day_of_week": ny_time.weekday(),
            "hour_of_day": ny_time.hour,
            "is_power_hour": 15 <= ny_time.hour < 16,
            "is_opening_hour": 9 <= ny_time.hour < 11,
            "total_symbols_tracked": len(candles),
            "active_alerts_count": len(recent_alerts)
        }
        
        # Cache the result
        market_context_cache['data'] = context
        market_context_cache['last_update'] = now
        
        return context
        
    except Exception as e:
        logger.warning(f"Error getting market context: {e}")
        return {
            "minutes_since_open": 0,
            "day_of_week": 0,
            "hour_of_day": 12,
            "is_power_hour": False,
            "is_opening_hour": False,
            "total_symbols_tracked": 0,
            "active_alerts_count": 0
        }

def track_alert_for_outcome(symbol, alert_type, price):
    """Track new alert for outcome monitoring"""
    now = datetime.now(timezone.utc)
    recent_alerts[symbol] = now
    alert_prices[symbol] = price
    alert_types[symbol] = alert_type
    logger.info(f"[OUTCOME TRACKING] Started tracking {symbol} {alert_type} at ${price:.2f}")

def auto_label_success(symbol, entry_price, current_price, minutes_elapsed):
    """Automatically determine if alert was profitable"""
    if entry_price <= 0:
        return {"return_pct": 0, "is_successful": False, "quality_score": 0}
        
    return_pct = (current_price - entry_price) / entry_price
    
    # Different success thresholds based on timeframe
    if minutes_elapsed <= 5:
        success_threshold = 0.015  # 1.5% in 5 minutes
    elif minutes_elapsed <= 15:
        success_threshold = 0.025  # 2.5% in 15 minutes
    elif minutes_elapsed <= 30:
        success_threshold = 0.035  # 3.5% in 30 minutes
    else:
        success_threshold = 0.05   # 5% in 1 hour+
    
    is_successful = return_pct >= success_threshold
    quality_score = min(max(return_pct * 100, 0), 100)  # 0-100 scale
    
    return {
        "return_pct": return_pct,
        "is_successful": is_successful,
        "quality_score": quality_score,
        "success_threshold": success_threshold
    }

async def check_alert_outcomes():
    """Check outcomes of recent alerts and log results"""
    now = datetime.now(timezone.utc)
    completed_alerts = []
    
    for symbol, alert_time in recent_alerts.items():
        minutes_elapsed = (now - alert_time).total_seconds() / 60
        
        # Check outcomes at 5, 15, 30, and 60 minute marks
        check_times = [5, 15, 30, 60]
        for check_minutes in check_times:
            if minutes_elapsed >= check_minutes and symbol in alert_prices:
                current_price = last_trade_price.get(symbol)
                if current_price:
                    entry_price = alert_prices[symbol]
                    alert_type = alert_types.get(symbol, "unknown")
                    
                    outcome = auto_label_success(symbol, entry_price, current_price, check_minutes)
                    
                    # Log the outcome
                    log_outcome(symbol, alert_type, entry_price, current_price, 
                               alert_time, check_minutes, outcome)
                    
                    logger.info(f"[OUTCOME] {symbol} {alert_type} @ {check_minutes}min: "
                              f"{outcome['return_pct']*100:.1f}% {'SUCCESS' if outcome['is_successful'] else 'FAIL'}")
        
        # Remove alerts older than 2 hours
        if minutes_elapsed > 120:
            completed_alerts.append(symbol)
    
    # Clean up completed alerts
    for symbol in completed_alerts:
        recent_alerts.pop(symbol, None)
        alert_prices.pop(symbol, None)
        alert_types.pop(symbol, None)

def log_outcome(symbol, alert_type, entry_price, current_price, alert_time, minutes_elapsed, outcome):
    """Log alert outcome for ML training"""
    row = {
        "symbol": symbol,
        "alert_type": alert_type,
        "entry_price": entry_price,
        "current_price": current_price,
        "alert_time_utc": alert_time.isoformat(),
        "check_time_utc": datetime.now(timezone.utc).isoformat(),
        "minutes_elapsed": minutes_elapsed,
        **outcome,
        **get_market_context()
    }
    
    header = list(row.keys())
    write_header = not os.path.exists(OUTCOME_LOG_FILE) or os.path.getsize(OUTCOME_LOG_FILE) == 0
    
    with open(OUTCOME_LOG_FILE, "a", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)
        if write_header:
            writer.writeheader()
        writer.writerow(row)

async def retrain_model_if_needed():
    """Retrain ML model with new outcome data"""
    try:
        if not os.path.exists(OUTCOME_LOG_FILE):
            return
            
        # Check if we have enough new data
        outcome_df = pd.read_csv(OUTCOME_LOG_FILE)
        if len(outcome_df) < 50:  # Need at least 50 outcomes
            return
            
        # Load event log
        if not os.path.exists(EVENT_LOG_FILE):
            return
            
        event_df = pd.read_csv(EVENT_LOG_FILE)
        
        # Merge event and outcome data
        merged_df = pd.merge(event_df, outcome_df, 
                           left_on=['symbol', 'event_type'], 
                           right_on=['symbol', 'alert_type'], 
                           how='inner')
        
        if len(merged_df) < 30:
            return
            
        # Prepare features for training
        feature_columns = ['price', 'volume', 'rvol', 'minutes_since_open', 
                          'day_of_week', 'is_power_hour', 'is_opening_hour']
        
        # Fill missing features with defaults
        for col in feature_columns:
            if col not in merged_df.columns:
                if col == 'rvol':
                    merged_df[col] = 2.0
                elif col in ['minutes_since_open', 'day_of_week']:
                    merged_df[col] = 0
                else:
                    merged_df[col] = False
        
        X = merged_df[feature_columns].fillna(0)
        y = merged_df['is_successful'].astype(int)
        
        if len(X) >= 30 and y.sum() > 5:  # At least 30 samples and 5 successes
            from sklearn.ensemble import RandomForestClassifier
            
            # Train new model
            new_model = RandomForestClassifier(n_estimators=100, random_state=42)
            new_model.fit(X, y)
            
            # Save updated model
            joblib.dump(new_model, "runner_model_updated.joblib")
            
            # Update global model
            global runner_clf
            runner_clf = new_model
            
            logger.info(f"[ML UPDATE] Model retrained with {len(X)} samples, "
                       f"{y.sum()} successes ({y.mean()*100:.1f}% success rate)")
            
            # Archive old outcome data to prevent memory issues
            if len(outcome_df) > 1000:
                archive_file = f"outcome_archive_{datetime.now().strftime('%Y%m%d')}.csv"
                outcome_df.iloc[:-200].to_csv(archive_file, index=False)
                outcome_df.iloc[-200:].to_csv(OUTCOME_LOG_FILE, index=False)
                logger.info(f"[ML UPDATE] Archived old outcomes to {archive_file}")
                
    except Exception as e:
        logger.error(f"[ML UPDATE] Error retraining model: {e}")

def log_event(event_type, symbol, price, volume, event_time, extra_features=None):
    extra_features = extra_features or {}
    
    # Auto-add market context to all events
    market_context = get_market_context()
    
    row = {
        "event_type": event_type,
        "symbol": symbol,
        "price": price,
        "volume": volume,
        "event_time_utc": event_time.isoformat(),
        **extra_features,
        **market_context  # AUTO-ADD market context
    }
    
    # Track alerts for outcome monitoring
    alert_types_list = ["perfect_setup", "runner", "volume_spike", "ema_stack", "warming_up", "dip_play"]
    if event_type in alert_types_list:
        track_alert_for_outcome(symbol, event_type, price)
    
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
    
    # Check if it's a weekday
    if now_ny.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False
    
    # Check if it's within extended hours (4 AM to 8 PM ET)
    return dt_time(4, 0) <= now_ny.time() <= dt_time(20, 0)

def get_session_type(dt):
    """Determine if a timestamp is premarket, regular, or afterhours"""
    ny = pytz.timezone("America/New_York")
    dt_ny = dt.astimezone(ny)
    time = dt_ny.time()
    
    if dt_time(4, 0) <= time < dt_time(9, 30):
        return "premarket"
    elif dt_time(9, 30) <= time < dt_time(16, 0):
        return "regular"
    elif dt_time(16, 0) <= time <= dt_time(20, 0):
        return "afterhours"
    else:
        return "closed"

async def send_telegram_async(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    data = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data) as resp:
                result = await resp.text()
                if resp.status not in (200, 204):
                    logger.error(f"Telegram send error: {result}")
    except Exception as e:
        logger.error(f"Telegram send error: {e}")

async def send_discord_async(message):
    data = {
        "content": message
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(DISCORD_WEBHOOK_URL, json=data) as resp:
                result = await resp.text()
                if resp.status not in (200, 204):
                    logger.error(f"Discord send error: {result}")
    except Exception as e:
        logger.error(f"Discord send error: {e}")

async def send_all_alerts(message):
    await send_telegram_async(message)
    await send_discord_async(message)

def escape_html(s):
    return html.escape(s or "")

CANDLE_MAXLEN = 30

candles = defaultdict(lambda: deque(maxlen=CANDLE_MAXLEN))
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

ALERT_COOLDOWN_MINUTES = 2  # Reduced from 5 to 2 minutes for faster day trading alerts
last_alert_time = defaultdict(lambda: datetime.min.replace(tzinfo=timezone.utc))

# --- ALERT PRIORITIZATION SYSTEM ---
ALERT_PRIORITIES = {
    "perfect_setup": 95,     # Highest priority
    "ema_stack": 90,        # High priority
    "runner": 85,           # Good priority
    "volume_spike": 80,     # Medium priority
    "dip_play": 75,         # Lower priority
    "warming_up": 70        # Lowest priority
}

def get_alert_score(alert_type, symbol, data):
    """Calculate quality score for an alert (0-100)"""
    base_score = ALERT_PRIORITIES.get(alert_type, 50)
    
    # Boost score based on quality indicators
    rvol = data.get('rvol', 1.0)
    volume = data.get('volume', 0)
    price_move = data.get('price_move', 0)
    
    # RVOL boost (up to +10 points)
    if rvol > 4.0:
        base_score += 10
    elif rvol > 3.0:
        base_score += 7
    elif rvol > 2.5:
        base_score += 5
    elif rvol > 2.0:
        base_score += 3
    
    # Volume boost (up to +5 points)
    if volume > 500000:
        base_score += 5
    elif volume > 300000:
        base_score += 3
    elif volume > 200000:
        base_score += 2
    
    # Price movement boost (up to +5 points)
    if price_move > 0.08:  # 8%+
        base_score += 5
    elif price_move > 0.05:  # 5%+
        base_score += 3
    elif price_move > 0.03:  # 3%+
        base_score += 2
    
    return min(base_score, 100)

pending_alerts = defaultdict(list)  # Store multiple alerts per symbol

async def send_best_alert(symbol):
    """Send only the highest scoring alert for a symbol"""
    if symbol == "OCTO":
        logger.info(f"[🚨 OCTO DEBUG] send_best_alert called, pending alerts: {len(pending_alerts.get(symbol, []))}")
    
    if symbol not in pending_alerts or not pending_alerts[symbol]:
        if symbol == "OCTO":
            logger.info(f"[🚨 OCTO DEBUG] No pending alerts for {symbol}")
        return
    
    # Find highest scoring alert
    best_alert = max(pending_alerts[symbol], key=lambda x: x['score'])
    
    if symbol == "OCTO":
        logger.info(f"[🚨 OCTO DEBUG] Best alert: {best_alert['type']} | Score: {best_alert['score']}")
    
    # Only send if score is high enough (LOWERED for faster alerts)
    if best_alert['score'] >= 50:
        await send_all_alerts(best_alert['message'])
        logger.info(f"[ALERT SENT] {symbol} | {best_alert['type']} | Score: {best_alert['score']}")
    else:
        logger.info(f"[ALERT SKIPPED] {symbol} | Best score: {best_alert['score']} (threshold: 50)")
    
    # Clear pending alerts for this symbol
    pending_alerts[symbol].clear()

warming_up_was_true = defaultdict(bool)
runner_was_true = defaultdict(bool)
dip_play_was_true = defaultdict(bool)
vwap_reclaim_was_true = defaultdict(bool)
volume_spike_was_true = defaultdict(bool)
ema_stack_was_true = defaultdict(bool)

def get_scanned_tickers():
    return set(candles.keys())

vwap_candles = defaultdict(lambda: deque(maxlen=CANDLE_MAXLEN))
vwap_session_date = defaultdict(lambda: None)

def get_session_date(dt):
    ny = pytz.timezone("America/New_York")
    dt_ny = dt.astimezone(ny)
    if dt_ny.time() < dt_time(4, 0):
        return dt_ny.date() - timedelta(days=1)
    return dt_ny.date()

def check_volume_spike(candles_seq, vwap_value):
    """Improved volume spike detection with better RVOL calculation and price momentum"""
    if len(candles_seq) < 10:  # Need more candles for stable RVOL
        return False, {}
    
    curr_candle = list(candles_seq)[-1]
    prev_candle = list(candles_seq)[-2]
    curr_volume = curr_candle['volume']
    
    # Use 8 trailing candles instead of 3 for more stable RVOL
    trailing_volumes = [c['volume'] for c in list(candles_seq)[-9:-1]]
    trailing_avg = sum(trailing_volumes) / 8 if len(trailing_volumes) == 8 else 1
    rvol = curr_volume / trailing_avg if trailing_avg > 0 else 0
    above_vwap = curr_candle['close'] > vwap_value

    session_type = get_session_type(curr_candle['start_time'])
    if session_type in ["premarket", "afterhours"]:
        min_volume = 200_000
    else:
        min_volume = 125_000

    wick_ok = curr_candle['close'] >= 0.75 * curr_candle['high']
    
    # STRICT UPWARD MOVEMENT ONLY - NO NEGATIVE OR FLAT MOVES
    price_momentum = (curr_candle['close'] - curr_candle['open']) / curr_candle['open'] if curr_candle['open'] > 0 else 0
    prev_momentum = (curr_candle['close'] - prev_candle['close']) / prev_candle['close'] if prev_candle['close'] > 0 else 0
    
    # Must have STRONG positive movement - eliminates flat stocks but catches early runners
    is_green_candle = curr_candle['close'] > curr_candle['open'] and price_momentum >= 0.035  # At least 3.5% green candle (BALANCED)
    rising_from_prev = curr_candle['close'] > prev_candle['close'] and prev_momentum >= 0.02  # At least 2% above previous (BALANCED)
    
    # ONLY POSITIVE MOVEMENT ALERTS
    bullish_momentum = is_green_candle and rising_from_prev

    symbol = curr_candle.get('symbol', '?')
    
    # Special debug logging for problematic symbols
    if symbol in ["PMI", "ETHZ", "ETHW"]:
        logger.error(
            f"[🚨 {symbol} DEBUG] DETAILED ANALYSIS | "
            f"Open={curr_candle['open']}, Close={curr_candle['close']}, "
            f"PrevClose={prev_candle['close']}, "
            f"Price_momentum={price_momentum*100:.2f}%, Prev_momentum={prev_momentum*100:.2f}%, "
            f"Vol={curr_volume}, RVOL={rvol:.2f}, Above_VWAP={above_vwap}, "
            f"Green_candle={is_green_candle} (needs >=3.5%), "
            f"Rising_from_prev={rising_from_prev} (needs >=2%), "
            f"Bullish_momentum={bullish_momentum}"
        )
    
    logger.info(
        f"[VOLUME SPIKE] {symbol} | "
        f"Vol={curr_volume}, RVOL={rvol:.2f}, Above VWAP={above_vwap}, "
        f"Green Candle={is_green_candle} ({price_momentum*100:.1f}%), "
        f"Rising from prev={rising_from_prev} ({prev_momentum*100:.1f}%), Wick OK={wick_ok}, "
        f"Bullish momentum={bullish_momentum}"
    )
    
    # Debug each condition individually
    vol_ok = curr_volume >= min_volume
    rvol_ok = rvol >= 1.8
    vwap_ok = above_vwap
    wick_check = wick_ok
    momentum_ok = bullish_momentum
    
    spike_detected = vol_ok and rvol_ok and vwap_ok and wick_check and momentum_ok
    
    # SPECIAL LOG: Rejected for being below VWAP
    if not vwap_ok and (vol_ok or rvol_ok):  # Would have triggered except for VWAP
        logger.info(f"[❌ VOLUME SPIKE REJECTED] {curr_candle.get('symbol', '?')} | BELOW VWAP | Close={curr_candle['close']:.4f} < VWAP={vwap_value:.4f}")
    
    # Log detailed debug info
    logger.info(
        f"[SPIKE DEBUG] {curr_candle.get('symbol', '?')} | "
        f"vol_ok={vol_ok} ({curr_volume}>={min_volume}), "
        f"rvol_ok={rvol_ok} ({rvol:.2f}>=1.8), "
        f"vwap_ok={vwap_ok}, wick_ok={wick_check}, momentum_ok={momentum_ok}, "
        f"FINAL: spike_detected={spike_detected}"
    )
    
    # Return both result and data for scoring
    data = {
        'rvol': rvol,
        'volume': curr_volume,
        'price_move': price_momentum,
        'above_vwap': above_vwap
    }
    
    return spike_detected, data
    
current_session_date = None

def get_ny_date():
    ny = pytz.timezone("America/New_York")
    now_utc = datetime.now(timezone.utc)
    now_ny = now_utc.astimezone(ny)
    return now_ny.date()

# PATCH: FRESHNESS CHECK for price
MAX_PRICE_AGE_SECONDS = 5  # REAL-TIME for day trading - not 60s delays!
def get_display_price(symbol, fallback, max_age_seconds=MAX_PRICE_AGE_SECONDS):
    price = last_trade_price[symbol]
    trade_time = last_trade_time[symbol]
    now = datetime.now(timezone.utc)
    if (
        price is not None
        and trade_time is not None
        and (now - trade_time).total_seconds() < max_age_seconds
    ):
        return price
    return fallback

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

# PATCH: SESSION RESET FUNCTION
def reset_symbol_state():
    for d in [
        candles,
        vwap_candles,
        last_trade_price,
        last_trade_volume,
        last_trade_time,
        recent_high,
        last_alert_time,
        warming_up_was_true,
        runner_was_true,
        dip_play_was_true,
        vwap_reclaim_was_true,
        volume_spike_was_true,
        ema_stack_was_true,
        premarket_open_prices,
        premarket_last_prices,
        premarket_volumes,
        alerted_symbols,
        runner_alerted_today,
        below_vwap_streak,
        vwap_reclaimed_once,
        dip_play_seen,
        volume_spike_alerted,
        rvol_spike_alerted,
        halted_symbols,
        pending_runner_alert
    ]:
        if hasattr(d, "clear"):
            d.clear()
    # Clear stored EMAs
    stored_emas.clear()
    logger.info("Cleared all per-symbol session state for new trading day!")

async def alert_perfect_setup(symbol, closes, volumes, highs, lows, candles_seq, vwap_value):
    closes_np = np.array(closes)
    volumes_np = np.array(volumes)
    highs_np = np.array(highs)
    lows_np = np.array(lows)

    # Use stored EMAs for Perfect Setup
    emas = get_stored_emas(symbol, [5, 8, 13])
    if emas['ema5'] is None or emas['ema8'] is None or emas['ema13'] is None:
        logger.info(f"[PERFECT SETUP] {symbol}: EMAs not initialized, skipping")
        return
    ema5 = emas['ema5']
    ema8 = emas['ema8']
    ema13 = emas['ema13']

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
        if last_trade_volume[symbol] < 250:
            logger.info(f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})")
            return
        alert_text = (
            f"🚨 <b>PERFECT SETUP</b> 🚨\n"
            f"<b>{escape_html(symbol)}</b> | ${get_display_price(symbol, last_close):.2f} | Vol: {int(last_volume/1000)}K | RVOL: {rvol:.1f}\n\n"
            f"Trend: EMA5 > EMA8 > EMA13\n"
            f"{'Above VWAP' if last_close > vwap_value else 'Below VWAP'}"
            f" | MACD↑"
            f" | RSI: {int(round(last_rsi))}"
        )
        # Calculate alert score and add to pending alerts
        alert_data = {
            'rvol': rvol,
            'volume': last_volume,
            'price_move': (last_close - candles_seq[-1]['open']) / candles_seq[-1]['open'] if candles_seq[-1]['open'] > 0 else 0
        }
        score = get_alert_score("perfect_setup", symbol, alert_data)
        
        # Add to pending alerts instead of sending immediately
        pending_alerts[symbol].append({
            'type': 'perfect_setup',
            'score': score,
            'message': alert_text
        })
        
        # Send the best alert for this symbol
        await send_best_alert(symbol)
        
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
                "bullish_engulfing": bullish_engulf,
                "alert_score": score
            }
        )
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
        # Reset stored EMAs for new day
        stored_emas.clear()
        print(f"[DEBUG] Reset alert state for new trading day: {today_ny}")
        print(f"[DEBUG] Reset stored EMAs for new trading day")

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
    # Check exception list first, then float range
    if symbol not in FLOAT_EXCEPTION_SYMBOLS and (float_shares is None or not (MIN_FLOAT_SHARES <= float_shares <= MAX_FLOAT_SHARES)):
        if symbol == "OCTO":
            logger.info(f"[🚨 OCTO DEBUG] FILTERED OUT due to float {float_shares} (range: {MIN_FLOAT_SHARES}-{MAX_FLOAT_SHARES})")
        logger.debug(f"Skipping {symbol} due to float {float_shares}")
        return
    
    # Log exception processing
    if symbol in FLOAT_EXCEPTION_SYMBOLS:
        logger.info(f"[FLOAT EXCEPTION] {symbol} bypassing float filter (actual: {float_shares})")
    
    # OCTO-specific debug for successful processing
    if symbol == "OCTO":
        logger.info(f"[🚨 OCTO DEBUG] PASSED float filter ({float_shares}), processing candle: {open_}/{high}/{low}/{close}/{volume}")
    # DEBUG: Show why symbols might be filtered
    if symbol in ["OCTO", "GRND", "EQS", "OSRH", "BJDX", "EBMT"]:
        logger.info(f"[🔥 FILTER DEBUG] {symbol} | market_scan_time={is_market_scan_time()} | price={close} | price_ok={close <= 20.00}")
    
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
    
    # Update stored EMAs with new price
    current_emas = update_stored_emas(symbol, close)
    vwap_candles[symbol].append({
        'open': open_,
        'high': high,
        'low': low,
        'close': close,
        'volume': volume,
        'start_time': start_time
    })

    # --- Debug: Print session candles and price info ---
    logger.info(
        f"[DEBUG] {symbol} | Session candles: {len(vwap_candles[symbol])} | Candle times: "
        f"{vwap_candles[symbol][0]['start_time'] if vwap_candles[symbol] else 'n/a'} - "
        f"{vwap_candles[symbol][-1]['start_time'] if vwap_candles[symbol] else 'n/a'}"
    )
    logger.info(
        f"[DEBUG] {symbol} | VWAP={vwap_candles_numpy(vwap_candles[symbol]):.4f} | "
        f"Last Trade Price={last_trade_price[symbol]} | Last Trade Volume={last_trade_volume[symbol]} | "
        f"Last Trade Time={last_trade_time[symbol]} | Candle Close={close}"
    )
    logger.info(
        f"[DEBUG] {symbol} | All candle closes: {[c['close'] for c in vwap_candles[symbol]]}"
    )
    logger.info(
        f"[DEBUG] {symbol} | All candle volumes: {[c['volume'] for c in vwap_candles[symbol]]}"
    )

    # --- PERFECT SETUP SCANNER ---
    if len(candles_seq) >= 30:
        closes = [c['close'] for c in list(candles_seq)[-30:]]
        highs = [c['high'] for c in list(candles_seq)[-30:]]
        lows = [c['low'] for c in list(candles_seq)[-30:]]
        volumes = [c['volume'] for c in list(candles_seq)[-30:]]
        vwap_value = vwap_candles_numpy(list(vwap_candles[symbol]))
        if not ema_stack_was_true[symbol]:
            await alert_perfect_setup(symbol, closes, volumes, highs, lows, list(candles_seq)[-30:], vwap_value)

    # --- Warming Up Logic with PATCH for accurate price and liquidity ---
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
        logger.info(
            f"[ALERT DEBUG] {symbol} | Alert Type: warming_up | VWAP={vwap_wu:.4f} | Last Trade={last_trade_price[symbol]} | Candle Close={close_wu} | Candle Volume={volume_wu}"
        )
        # Use stored EMAs
        emas = get_stored_emas(symbol, [5, 8, 13])
        logger.info(f"[EMA DEBUG] {symbol} | Warming Up | EMA5={emas.get('ema5', 'N/A')}, EMA8={emas.get('ema8', 'N/A')}, EMA13={emas.get('ema13', 'N/A')}")
        if warming_up_criteria and not warming_up_was_true[symbol]:
            if last_trade_volume[symbol] < 250:
                logger.info(f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})")
                return
            if (now - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                return
            log_event("warming_up", symbol, get_display_price(symbol, close_wu), volume_wu, event_time, {
                "price_move": price_move_wu,
                "dollar_volume": dollar_volume_wu
            })
            price_str = f"{get_display_price(symbol, close_wu):.2f}"
            alert_text = (
                f"🌡️ <b>{escape_html(symbol)}</b> Warming Up\n"
                f"Current Price: ${price_str}"
            )
            # Calculate alert score and add to pending alerts
            alert_data = {
                'rvol': volume_wu / avg_vol_5,
                'volume': volume_wu,
                'price_move': price_move_wu
            }
            score = get_alert_score("warming_up", symbol, alert_data)
            
            # Add to pending alerts instead of sending immediately
            pending_alerts[symbol].append({
                'type': 'warming_up',
                'score': score,
                'message': alert_text
            })
            
            # Send the best alert for this symbol
            await send_best_alert(symbol)
            
            warming_up_was_true[symbol] = True
            alerted_symbols[symbol] = today
            last_alert_time[symbol] = now

    # --- Runner Logic with trend check, upper wick filter, real-time price in alert, and debug logging ---
    if len(candles_seq) >= 6:
        last_6 = list(candles_seq)[-6:]
        volumes_5 = [c['volume'] for c in last_6[:-1]]
        avg_vol_5 = sum(volumes_5) / 5
        last_candle = last_6[-1]
        open_rn = last_candle['open']
        close_rn = last_candle['close']
        high_rn = last_candle['high']
        volume_rn = last_candle['volume']
        price_move_rn = (close_rn - open_rn) / open_rn if open_rn > 0 else 0
        vwap_rn = vwap_candles_numpy(vwap_candles[symbol]) if vwap_candles[symbol] else 0

        closes_for_trend = [c['close'] for c in last_6[-3:]]
        price_rising_trend = all(x < y for x, y in zip(closes_for_trend, closes_for_trend[1:]))
        price_not_dropping = closes_for_trend[-2] < closes_for_trend[-1]
        wick_ok = close_rn >= 0.75 * high_rn

        logger.info(
            f"[RUNNER DEBUG] {symbol} | Closes trend: {closes_for_trend} | price_rising_trend={price_rising_trend} | price_not_dropping={price_not_dropping} | wick_ok={wick_ok}"
        )
        logger.info(
            f"[ALERT DEBUG] {symbol} | Alert Type: runner | VWAP={vwap_rn:.4f} | Last Trade={last_trade_price[symbol]} | Candle Close={close_rn} | Candle Volume={volume_rn}"
        )
        # Use stored EMAs for Runner Detection
        emas = get_stored_emas(symbol, [5, 8, 13])
        logger.info(f"[STORED EMA] {symbol} | Runner | EMA5={emas.get('ema5', 'N/A')}, EMA8={emas.get('ema8', 'N/A')}, EMA13={emas.get('ema13', 'N/A')}")

        # IMPROVED: Lower threshold but add volume confirmation
        volume_increasing = len(last_6) >= 3 and all(
            last_6[i]['volume'] <= last_6[i+1]['volume'] 
            for i in range(-3, -1)
        )
        
        runner_criteria = (
            volume_rn >= 1.8 * avg_vol_5 and  # Lower volume requirement
            price_move_rn >= 0.03 and        # 3% instead of 6% - CATCH EARLIER
            close_rn >= 0.10 and
            close_rn > vwap_rn and
            price_rising_trend and
            price_not_dropping and
            wick_ok and
            volume_increasing  # NEW: Volume must be increasing
        )

        if runner_criteria and not runner_was_true[symbol]:
            if last_trade_volume[symbol] < 250:
                logger.info(f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})")
                return
            if (now - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                return
            # Calculate alert score and add to pending alerts
            alert_data = {
                'rvol': volume_rn / avg_vol_5,
                'volume': volume_rn,
                'price_move': price_move_rn
            }
            score = get_alert_score("runner", symbol, alert_data)
            
            log_event("runner", symbol, get_display_price(symbol, close_rn), volume_rn, event_time, {
                "price_move": price_move_rn,
                "trend_closes": closes_for_trend,
                "wick_ok": wick_ok,
                "vwap": vwap_rn,
                "last_trade_price": last_trade_price[symbol],
                "alert_score": score
            })
            price_str = f"{get_display_price(symbol, close_rn):.2f}"
            alert_text = (
                f"🏃‍♂️ <b>{escape_html(symbol)}</b> Runner\n"
                f"Current Price: ${price_str} (+{price_move_rn*100:.1f}%)"
            )
            
            # Add to pending alerts instead of sending immediately
            pending_alerts[symbol].append({
                'type': 'runner',
                'score': score,
                'message': alert_text
            })
            
            # Send the best alert for this symbol
            await send_best_alert(symbol)
            
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
            logger.info(
                f"[ALERT DEBUG] {symbol} | Alert Type: dip_play | VWAP={vwap_candles_numpy(vwap_candles[symbol]):.4f} | Last Trade={last_trade_price[symbol]} | Candle Close={close} | Candle Volume={volume}"
            )
            # Use stored EMAs for Dip Play
            emas = get_stored_emas(symbol, [5, 8, 13])
            logger.info(f"[STORED EMA] {symbol} | Dip Play | EMA5={emas.get('ema5', 'N/A')}, EMA8={emas.get('ema8', 'N/A')}, EMA13={emas.get('ema13', 'N/A')}")
            if dip_play_criteria and not dip_play_was_true[symbol]:
                if (now - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                    return
                if last_trade_volume[symbol] < 250:
                    logger.info(f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})")
                    return
                log_event("dip_play", symbol, get_display_price(symbol, close), volume, event_time, {
                    "dip_pct": dip_pct
                })
                price_str = f"{get_display_price(symbol, close):.2f}"
                alert_text = (
                    f"📉 <b>{escape_html(symbol)}</b> Dip Play\n"
                    f"Current Price: ${price_str}"
                )
                # Calculate alert score and add to pending alerts
                alert_data = {
                    'rvol': 2.0,  # Assume decent RVOL for dip plays
                    'volume': volume,
                    'price_move': dip_pct * -1  # Negative for dip
                }
                score = get_alert_score("dip_play", symbol, alert_data)
                
                # Add to pending alerts instead of sending immediately
                pending_alerts[symbol].append({
                    'type': 'dip_play',
                    'score': score,
                    'message': alert_text
                })
                
                # Send the best alert for this symbol
                await send_best_alert(symbol)
                
                dip_play_was_true[symbol] = True
                dip_play_seen.add(symbol)
                alerted_symbols[symbol] = today
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
            curr_candle['volume'] >= 100_000 and  # Keep high volume requirement for quality
            rvol >= 2.0  # Keep high RVOL requirement for quality
        )
        
        # DEBUG: Show why VWAP reclaim alerts might not fire
        if symbol in ["OCTO", "GRND", "EQS"]:
            logger.info(f"[💎 VWAP RECLAIM DEBUG] {symbol} | criteria_met={vwap_reclaim_criteria} | prev_close<prev_vwap={prev_candle['close'] < (prev_vwap if prev_vwap is not None else prev_candle['close'])} | curr_close>curr_vwap={curr_candle['close'] > (curr_vwap if curr_vwap is not None else curr_candle['close'])} | volume>50K={curr_candle['volume'] >= 50_000} | rvol>1.5={rvol >= 1.5}")
        logger.info(
            f"[ALERT DEBUG] {symbol} | Alert Type: vwap_reclaim | VWAP={curr_vwap:.4f} | Last Trade={last_trade_price[symbol]} | Candle Close={curr_candle['close']} | Candle Volume={curr_candle['volume']}"
        )
        # Use stored EMAs 
        emas = get_stored_emas(symbol, [5, 8, 13])
        logger.info(f"[EMA DEBUG] {symbol} | VWAP Reclaim | EMA5={emas.get('ema5', 'N/A')}, EMA8={emas.get('ema8', 'N/A')}, EMA13={emas.get('ema13', 'N/A')}")
        if vwap_reclaim_criteria and not vwap_reclaim_was_true[symbol]:
            if (now - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                return
            if last_trade_volume[symbol] < 250:
                logger.info(f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})")
                return
            log_event("vwap_reclaim", symbol, get_display_price(symbol, curr_candle['close']), curr_candle['volume'], event_time, {
                "rvol": rvol
            })
            price_str = f"{get_display_price(symbol, curr_candle['close']):.2f}"
            vwap_str = f"{curr_vwap:.2f}" if curr_vwap is not None else "?"
            vol_str = f"{curr_candle['volume']:,}"
            rvol_str = f"{rvol:.2f}"
            alert_text = (
                f"📈 <b>{escape_html(symbol)}</b> VWAP Reclaim!\n"
                f"Price: ${price_str} | VWAP: ${vwap_str}"
            )
            await send_all_alerts(alert_text)
            vwap_reclaim_was_true[symbol] = True
            alerted_symbols[symbol] = today
            last_alert_time[symbol] = now

    # Volume Spike Logic PATCH
    vwap_value = vwap_candles_numpy(vwap_candles[symbol]) if vwap_candles[symbol] else 0
    logger.info(
        f"[ALERT DEBUG] {symbol} | Alert Type: volume_spike | VWAP={vwap_value:.4f} | Last Trade={last_trade_price[symbol]} | Candle Close={close} | Candle Volume={volume}"
    )
    # Use stored EMAs for Volume Spike
    emas = get_stored_emas(symbol, [5, 8, 13])
    logger.info(f"[STORED EMA] {symbol} | Volume Spike | EMA5={emas.get('ema5', 'N/A')}, EMA8={emas.get('ema8', 'N/A')}, EMA13={emas.get('ema13', 'N/A')}")
    spike_detected, spike_data = check_volume_spike(candles_seq, vwap_value)
    
    # DEBUG: Show why volume spike alerts might not fire
    if symbol in ["OCTO", "GRND", "EQS", "OSRH", "BJDX", "EBMT"]:  # Debug key symbols
        logger.info(f"[🔥 VOLUME SPIKE DEBUG] {symbol} | spike_detected={spike_detected} | volume_spike_was_true={volume_spike_was_true[symbol]} | last_trade_volume={last_trade_volume[symbol]} | cooldown_ok={(now - last_alert_time[symbol]).total_seconds() > ALERT_COOLDOWN_MINUTES * 60}")
        if spike_detected:
            logger.info(f"[🔥 SPIKE DATA] {symbol} | RVOL={spike_data['rvol']:.2f} | Volume={spike_data['volume']} | Above_VWAP={spike_data['above_vwap']} | Price_move={spike_data['price_move']*100:.1f}%")
    
    if spike_detected and not volume_spike_was_true[symbol]:
        if last_trade_volume[symbol] < 250:
            logger.info(f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})")
            return
        if (now - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
            logger.info(f"[COOLDOWN] {symbol}: Skipping alert due to cooldown ({(now - last_alert_time[symbol]).total_seconds():.0f}s ago)")
            return
        
        # Calculate alert score and add to pending alerts
        score = get_alert_score("volume_spike", symbol, spike_data)
        
        # DEBUG: Log volume spike alert addition
        if symbol in ["OCTO", "GRND", "EQS", "OSRH", "BJDX", "EBMT"]:
            logger.info(f"[📊 VOLUME SPIKE] {symbol} | Adding to pending_alerts | Score: {score} | Volume: {spike_data['volume']} | RVOL: {spike_data['rvol']:.2f}")
        
        price_str = f"{get_display_price(symbol, close):.2f}"
        rvol_str = f"{spike_data['rvol']:.1f}"
        move_pct = spike_data['price_move'] * 100
        
        # Show actual percentage with proper sign
        if move_pct >= 0:
            move_str = f"+{move_pct:.1f}%"
        else:
            move_str = f"{move_pct:.1f}%"
            
        alert_text = (
            f"🚀 <b>{escape_html(symbol)}</b> Volume Spike!\n"
            f"Price: ${price_str} ({move_str}) | Vol: {spike_data['volume']:,} | RVOL: {rvol_str}"
        )
        
        # Add to pending alerts instead of sending immediately
        pending_alerts[symbol].append({
            'type': 'volume_spike',
            'score': score,
            'message': alert_text
        })
        
        # Send the best alert for this symbol
        await send_best_alert(symbol)
        
        log_event("volume_spike", symbol, get_display_price(symbol, close), volume, event_time, spike_data)
        volume_spike_was_true[symbol] = True
        alerted_symbols[symbol] = today
        last_alert_time[symbol] = now

    # EMA Stack Criteria PATCH (uses stored EMAs)
    if len(candles_seq) >= 20:
        vwap_value = vwap_candles_numpy(vwap_candles[symbol]) if vwap_candles[symbol] else 0
        
        # Use stored EMAs for EMA Stack
        emas = get_stored_emas(symbol, [5, 8, 13])
        logger.info(f"[STORED EMA] {symbol} | EMA Stack | EMA5={emas.get('ema5', 'N/A')}, EMA8={emas.get('ema8', 'N/A')}, EMA13={emas.get('ema13', 'N/A')}")
        
        if emas['ema5'] is None or emas['ema8'] is None or emas['ema13'] is None:
            logger.info(f"[EMA STACK] {symbol}: EMAs not ready, skipping")
            return
            
        ema5 = emas['ema5']
        ema8 = emas['ema8']
        ema13 = emas['ema13']
        
        session_type = get_session_type(start_time)
        logger.info(f"[EMA STACK] {symbol}: Checking session type: {session_type}")
        
        # Session-specific volumes and ratios
        if session_type == "regular":
            min_candles = 20
            min_dollar_vol = 15_000
            min_ratio = 1.005  # 0.5% spread (regular market)
        else:  # premarket/afterhours
            min_candles = 15  # Lower requirements for extended hours
            min_dollar_vol = 10_000
            min_ratio = 1.003  # 0.3% spread (extended hours)
        
        # Calculate actual dollar volume
        dollar_volume = close * volume
        ratio = ema5 / ema13 if ema13 > 0 else 0
        logger.info(f"[EMA STACK DEBUG] {symbol}: session={session_type}, ema5={ema5:.2f}, ema8={ema8:.2f}, ema13={ema13:.2f}, vwap={vwap_value:.2f}, close={close}, volume={volume}, dollar_volume={dollar_volume:.2f}, ratio={ratio:.4f}, criteria={ratio >= min_ratio}")
        
        ema_stack_criteria = (
            ema5 > ema8 > ema13 and  # True trend
            close > ema5 and
            close > vwap_value and
            ratio >= min_ratio and
            dollar_volume >= min_dollar_vol
        )
        
        if ema_stack_criteria and not ema_stack_was_true[symbol]:
            if last_trade_volume[symbol] < 250:
                logger.info(f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})")
                return
            if (now - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                return
            trailing_vols = [c['volume'] for c in list(candles_seq)[:-1]]
            avg_trailing = sum(trailing_vols[-20:]) / min(len(trailing_vols), 20) if trailing_vols else 1
            rvol_stack = volume / avg_trailing if avg_trailing > 0 else 1
            
            # Calculate alert score and add to pending alerts
            alert_data = {
                'rvol': rvol_stack,
                'volume': volume,
                'price_move': (close - open_) / open_ if open_ > 0 else 0
            }
            score = get_alert_score("ema_stack", symbol, alert_data)
            
            log_event("ema_stack", symbol, get_display_price(symbol, close), volume, event_time, {
                "ema5": ema5,
                "ema8": ema8,
                "ema13": ema13,
                "vwap": vwap_value,
                "rvol": rvol_stack,
                "ratio": ratio,
                "alert_score": score
            })
            
            price_str = f"{get_display_price(symbol, close):.2f}"
            alert_text = (
                f"📊 <b>{escape_html(symbol)}</b> EMA Stack!\n"
                f"Price: ${price_str} | EMAs: 5>8>13 | Above VWAP"
            )
            
            # Add to pending alerts instead of sending immediately
            pending_alerts[symbol].append({
                'type': 'ema_stack',
                'score': score,
                'message': alert_text
            })
            
            # Send the best alert for this symbol
            await send_best_alert(symbol)
            
            ema_stack_was_true[symbol] = True
            alerted_symbols[symbol] = today
            last_alert_time[symbol] = now
        else:
            # Debug logging for non-qualifying symbols
            if session_type == "regular" and len(candles_seq) >= min_candles:
                logger.info(f"[EMA STACK] {symbol}: Using stored 5,8,13 EMAs only (before 11am, {len(candles_seq)} candles)")
            elif session_type != "regular":
                logger.info(f"[EMA STACK] {symbol}: {session_type} session - using relaxed criteria")
            else:
                logger.info(f"[EMA STACK] {symbol}: Skipping - need {min_candles} candles, have {len(candles_seq)} for {session_type}")

async def market_close_alert_loop():
    """Send premarket gainers alert at 9:24:55am ET every day"""
    while True:
        try:
            ny_tz = pytz.timezone("America/New_York")
            now_ny = datetime.now(ny_tz)
            
            # Target time: 9:24:55 AM ET (5 seconds before market open)
            target_time = now_ny.replace(hour=9, minute=24, second=55, microsecond=0)
            
            # If we're past today's target, set for tomorrow
            if now_ny > target_time:
                target_time += timedelta(days=1)
            
            # Skip weekends
            while target_time.weekday() >= 5:  # Saturday = 5, Sunday = 6
                target_time += timedelta(days=1)
            
            target_utc = target_time.astimezone(timezone.utc)
            sleep_seconds = (target_utc - datetime.now(timezone.utc)).total_seconds()
            
            if sleep_seconds > 0:
                logger.info(f"[SCHEDULER] Sleeping {sleep_seconds:.0f}s until next premarket alert at {target_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                await asyncio.sleep(sleep_seconds)
            
            # Check if it's still a weekday (in case we slept through weekend)
            now_ny = datetime.now(ny_tz)
            if now_ny.weekday() < 5:  # Monday=0, Friday=4
                await send_premarket_gainers_alert()
            
            # Wait a bit to avoid duplicate sends
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"[SCHEDULER] Error in market close alert loop: {e}")
            await asyncio.sleep(60)  # Wait a minute before retrying

async def premarket_gainers_alert_loop():
    """Send a summary alert at 8:01pm ET every day"""
    while True:
        try:
            ny_tz = pytz.timezone("America/New_York")
            now_ny = datetime.now(ny_tz)
            
            # Target time: 8:01 PM ET
            target_time = now_ny.replace(hour=20, minute=1, second=0, microsecond=0)
            
            # If we're past today's target, set for tomorrow
            if now_ny > target_time:
                target_time += timedelta(days=1)
            
            # Skip weekends
            while target_time.weekday() >= 5:  # Saturday = 5, Sunday = 6
                target_time += timedelta(days=1)
            
            target_utc = target_time.astimezone(timezone.utc)
            sleep_seconds = (target_utc - datetime.now(timezone.utc)).total_seconds()
            
            if sleep_seconds > 0:
                logger.info(f"[SCHEDULER] Sleeping {sleep_seconds:.0f}s until next end-of-day alert at {target_time.strftime('%Y-%m-%d %H:%M:%S %Z')}")
                await asyncio.sleep(sleep_seconds)
            
            # Check if it's still a weekday
            now_ny = datetime.now(ny_tz)
            if now_ny.weekday() < 5:  # Monday=0, Friday=4
                await send_eod_summary_alert()
            
            # Wait a bit to avoid duplicate sends
            await asyncio.sleep(60)
            
        except Exception as e:
            logger.error(f"[SCHEDULER] Error in premarket gainers alert loop: {e}")
            await asyncio.sleep(60)

async def send_premarket_gainers_alert():
    """Send premarket gainers alert"""
    if not premarket_open_prices:
        logger.info("[PREMARKET] No premarket data available")
        return
        
    gainers = []
    for symbol, open_price in premarket_open_prices.items():
        if symbol in premarket_last_prices:
            last_price = premarket_last_prices[symbol]
            volume = premarket_volumes.get(symbol, 0)
            if open_price > 0:
                gain_pct = ((last_price - open_price) / open_price) * 100
                if gain_pct >= 5.0 and volume >= 10000:  # 5%+ gain with 10K+ volume
                    gainers.append((symbol, gain_pct, last_price, volume))
    
    # Sort by gain percentage
    gainers.sort(key=lambda x: x[1], reverse=True)
    
    if gainers:
        message = "🌅 <b>PREMARKET GAINERS</b> 🌅\n\n"
        for symbol, gain_pct, price, volume in gainers[:10]:  # Top 10
            message += f"<b>{escape_html(symbol)}</b> | ${price:.2f} (+{gain_pct:.1f}%) | Vol: {int(volume/1000)}K\n"
        message += "\n🔔 Market opens in 5 minutes!"
    else:
        message = "🌅 <b>PREMARKET UPDATE</b>\n\nNo significant gainers found this morning."
    
    await send_all_alerts(message)
    logger.info(f"[PREMARKET] Sent gainers alert with {len(gainers)} symbols")

async def send_eod_summary_alert():
    """Send end-of-day summary"""
    symbols_tracked = len(candles)
    alerts_sent = len(alerted_symbols)
    
    # Get symbols we sent alerts for today
    alerted_today = list(alerted_symbols.keys())[:5]  # Top 5
    
    message = f"📊 <b>END OF DAY SUMMARY</b> 📊\n\n"
    message += f"Symbols Tracked: {symbols_tracked:,}\n"
    message += f"Alerts Sent Today: {alerts_sent}\n\n"
    
    if alerted_today:
        message += "🔥 <b>Today's Alert Symbols:</b>\n"
        for symbol in alerted_today:
            current_price = last_trade_price.get(symbol, 0)
            message += f"• {escape_html(symbol)}: ${current_price:.2f}\n"
    
    message += "\n💤 See you tomorrow for more alerts!"
    
    await send_all_alerts(message)
    logger.info(f"[EOD] Sent end-of-day summary")

async def handle_halt_event(event):
    symbol = event.get("symbol", "")
    halted_symbols.add(symbol)
    alert_text = f"⚠️ <b>HALT ALERT</b> ⚠️\n<b>{escape_html(symbol)}</b> has been halted"
    await send_all_alerts(alert_text)

async def handle_resume_event(event):
    symbol = event.get("symbol", "")
    if symbol in halted_symbols:
        halted_symbols.remove(symbol)
        current_price = last_trade_price.get(symbol, "Unknown")
        alert_text = f"▶️ <b>RESUME ALERT</b> ▶️\n<b>{escape_html(symbol)}</b> trading resumed at ${current_price}"
        await send_all_alerts(alert_text)

async def get_screener_data():
    """Fetch symbols from Polygon.io screener - SIMPLIFIED VERSION"""
    try:
        url = f"https://api.polygon.io/v3/reference/tickers"
        params = {
            "market": "stocks",
            "active": "true",
            "limit": MAX_SYMBOLS,
            "apikey": POLYGON_API_KEY
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    symbols = [ticker["ticker"] for ticker in data.get("results", [])]
                    logger.info(f"[SCREENER] Fetched {len(symbols)} symbols from Polygon screener")
                    return symbols
                else:
                    logger.error(f"[SCREENER] HTTP {resp.status}: {await resp.text()}")
                    return []
    except Exception as e:
        logger.error(f"[SCREENER] Error fetching screener data: {e}")
        return []

async def ingest_polygon_events():
    """Main WebSocket connection to Polygon.io for real-time data"""
    while True:
        try:
            symbols = await get_screener_data()
            if not symbols:
                logger.warning("[POLYGON] No symbols from screener, using fallback list")
                symbols = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]  # Fallback
            
            # Subscribe to ALL symbols for comprehensive scanning
            subscriptions = []
            subscriptions.extend([f"AM.{symbol}" for symbol in symbols])  # Minute aggregates
            subscriptions.extend([f"T.{symbol}" for symbol in symbols])    # Trades
            
            logger.info(f"[POLYGON] Subscribing to {len(subscriptions)} streams for {len(symbols)} symbols")
            
            uri = f"wss://socket.polygon.io/stocks"
            async with websockets.connect(uri) as websocket:
                # Authenticate
                auth_message = {"action": "auth", "params": POLYGON_API_KEY}
                await websocket.send(json.dumps(auth_message))
                response = await websocket.recv()
                auth_response = json.loads(response)
                
                # PATCH: Correct authentication success check
               if not (isinstance(auth_response, list) and auth_response and auth_response[0].get("status") == "connected"):
                   logger.error(f"[POLYGON] Authentication failed: {auth_response}")
                   await asyncio.sleep(30)
                   continue
                
                logger.info("[POLYGON] Authenticated successfully")
                
                # Subscribe in chunks to avoid rate limits
                chunk_size = 100
                for i in range(0, len(subscriptions), chunk_size):
                    chunk = subscriptions[i:i+chunk_size]
                    subscribe_message = {"action": "subscribe", "params": ",".join(chunk)}
                    await websocket.send(json.dumps(subscribe_message))
                    await asyncio.sleep(0.1)  # Small delay between chunks
                
                logger.info(f"[POLYGON] Subscribed to all data streams")
                
               
                # Process incoming messages
                async for message in websocket:
                    try:
                        events = json.loads(message)
                        logger.debug(f"[RAW MSG] {events}")
                        
                        for event in events:
                            if event.get("ev") == "T":  # Trade event
                                symbol = event["sym"]
                                last_trade_price[symbol] = event.get('p', last_trade_price[symbol])
                                last_trade_volume[symbol] = event.get('s', 0)
                                last_trade_time[symbol] = datetime.now(timezone.utc)
                                print(
                                    f"[TRADE EVENT] {symbol} | Price={event['p']} | Size={event.get('s', 0)} | Time={last_trade_time[symbol]}"
                                )
                            if event.get("ev") == "AM":
                                symbol = event["sym"]
                                open_ = event["o"]
                                high = event["h"]
                                low = event["l"]
                                close = event["c"]
                                volume = event["v"]
                                start_time = polygon_time_to_utc(event["s"])
                                
                                # DEBUG: Special logging for OCTO to track why no alerts
                                if symbol == "OCTO":
                                    logger.info(f"[🚨 OCTO DEBUG] Processing AM event: {event}")
                                    logger.info(f"[🚨 OCTO DEBUG] Received AM candle: {start_time} OHLCV: {open_}/{high}/{low}/{close}/{volume}")
                                
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
                                # Process all pending alerts and send only the best one
                                if symbol == "OCTO":
                                    logger.info(f"[🚨 OCTO DEBUG] About to call send_best_alert for {symbol}")
                                await send_best_alert(symbol)
                            elif event.get("ev") == "status" and event.get("status") == "halt":
                                await handle_halt_event(event)
                            elif event.get("ev") == "status" and event.get("status") == "resume":
                                await handle_resume_event(event)
                    except Exception as e:
                        print(f"Error processing message: {e}\nRaw: {msg}")
        except Exception as e:
            print(f"Websocket error: {e}")
            # Check if it's a connection limit error - wait longer to avoid rapid reconnects
            if "connection" in str(e).lower() or "limit" in str(e).lower():
                print("Connection limit detected - waiting 30 seconds before reconnecting...")
                await asyncio.sleep(30)
            else:
                print("General error - reconnecting in 10 seconds...")
                await asyncio.sleep(10)

async def ml_training_loop():
    """Background task for ML training and outcome tracking"""
    while True:
        try:
            # Check alert outcomes every 5 minutes
            await check_alert_outcomes()
            
            # Retrain model once per day at market close
            ny_time = datetime.now(timezone.utc).astimezone(pytz.timezone("America/New_York"))
            if ny_time.hour == 20 and ny_time.minute < 5:  # 8:00-8:05 PM ET
                await retrain_model_if_needed()
                
        except Exception as e:
            logger.error(f"[ML TRAINING] Error in ML training loop: {e}")
            
        await asyncio.sleep(300)  # 5 minutes

async def main():
    print("Main event loop running. Press Ctrl+C to exit.")
    ingest_task = asyncio.create_task(ingest_polygon_events())
    # Enabling just the scheduled alerts (9:24:55am and 8:01pm)
    close_alert_task = asyncio.create_task(market_close_alert_loop())
    premarket_alert_task = asyncio.create_task(premarket_gainers_alert_loop())
    # catalyst_news_task = asyncio.create_task(catalyst_news_alert_loop())  # ❌ DISABLED - causes connection limit
    # nasdaq_halt_task = asyncio.create_task(nasdaq_halt_scraper_loop())  # ❌ DISABLED - causes connection limit
    try:
        while True:
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        print("Main loop cancelled.")
    finally:
        ingest_task.cancel()
        close_alert_task.cancel()
        premarket_alert_task.cancel()
        # catalyst_news_task.cancel()  # DISABLED - causes connection limit
        # nasdaq_halt_task.cancel()  # DISABLED - causes connection limit
        
        await ingest_task
        await close_alert_task
        await premarket_alert_task
        # await catalyst_news_task  # DISABLED - causes connection limit  
        # await nasdaq_halt_task  # DISABLED - causes connection limit

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
