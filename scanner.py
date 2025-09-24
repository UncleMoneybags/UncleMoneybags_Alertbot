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

def is_eligible(symbol, last_price, float_shares):
    """Check if symbol meets filtering criteria: price <= $10 AND float <= 10M"""
    # Price check - ALL symbols must be <= $10 (no exceptions!)
    if last_price is None or last_price > PRICE_THRESHOLD:
        return False
        
    # Float check - exception symbols bypass ONLY float filter
    if symbol not in FLOAT_EXCEPTION_SYMBOLS:
        # FAIL-CLOSED: must have valid float data AND be <= 10M
        if float_shares is None or not (MIN_FLOAT_SHARES <= float_shares <= MAX_FLOAT_SHARES):
            return False
        
    return True

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

# Global guard to prevent multiple EMA resets per day
last_ema_reset_date = None

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

def get_last_trading_days(days_count=3):
    """Get the last N trading days (excluding weekends) in YYYY-MM-DD format"""
    now = datetime.now(timezone.utc)
    trading_days = []
    current_date = now.date()
    
    while len(trading_days) < days_count:
        # Skip weekends (Saturday=5, Sunday=6)
        if current_date.weekday() < 5:
            trading_days.append(current_date)
        current_date -= timedelta(days=1)
    
    # Return the earliest and latest dates for the API range
    earliest_date = trading_days[-1]  # Last (earliest) trading day
    latest_date = trading_days[0]     # First (latest) trading day
    
    return earliest_date.strftime('%Y-%m-%d'), latest_date.strftime('%Y-%m-%d')

async def backfill_stored_emas(symbol):
    """Backfill stored EMAs with historical 1-minute candle data"""
    try:
        # Get dynamic date range for last 3 trading days
        start_date, end_date = get_last_trading_days(3)
        
        # Fetch last 200 1-minute candles with extended hours
        url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}"
        params = {
            'adjusted': 'true',
            'sort': 'asc',
            'limit': 200,
            'apikey': POLYGON_API_KEY
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    if data.get('status') == 'OK' and data.get('results'):
                        candles = data['results']
                        logger.info(f"[EMA BACKFILL] {symbol} | Fetched {len(candles)} historical candles")
                        
                        # Seed EMAs with historical closes in chronological order
                        for candle in candles:
                            close_price = candle['c']
                            for period in [5, 8, 13, 200]:
                                stored_emas[symbol][period].update(close_price)
                        
                        # Log final seeded values
                        emas = get_stored_emas(symbol, [5, 8, 13, 200])
                        logger.info(f"[EMA BACKFILL] {symbol} | Seeded EMAs - EMA5: {emas['ema5']:.4f} | EMA8: {emas['ema8']:.4f} | EMA13: {emas['ema13']:.4f} | EMA200: {emas['ema200']:.4f}")
                        return True
                    else:
                        logger.warning(f"[EMA BACKFILL] {symbol} | No data available for backfill")
                        return False
                else:
                    logger.error(f"[EMA BACKFILL] {symbol} | API error: {response.status}")
                    return False
                    
    except Exception as e:
        logger.error(f"[EMA BACKFILL] {symbol} | Error during backfill: {e}")
        return False

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

PRICE_THRESHOLD = 10.00
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

def get_session_type(dt):
    ny = pytz.timezone("America/New_York")
    dt_ny = dt.astimezone(ny)
    t = dt_ny.time()
    if dt_time(4, 0) <= t < dt_time(9, 30):
        return "premarket"
    elif dt_time(9, 30) <= t < dt_time(16, 0):
        return "regular"
    elif dt_time(16, 0) <= t < dt_time(20, 0):
        return "afterhours"
    else:
        return "closed"

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

async def send_discord_async(message):
    if not DISCORD_WEBHOOK_URL:
        logger.error("No Discord webhook URL set!")
        return
    payload = {
        "content": message
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(DISCORD_WEBHOOK_URL, json=payload, timeout=10) as resp:
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
    if best_alert['score'] >= 30:
        await send_all_alerts(best_alert['message'])
        logger.info(f"[ALERT SENT] {symbol} | {best_alert['type']} | Score: {best_alert['score']}")
    else:
        logger.info(f"[ALERT SKIPPED] {symbol} | Best score: {best_alert['score']} (threshold: 30)")
    
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
    # EMAs are NOT reset here - they persist throughout the trading session and only reset at 04:00 ET
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
    eastern = pytz.timezone("America/New_York")
    now_et = now.astimezone(eastern)
    
    # Only reset state once per day at 04:00 ET (not on every calendar day change)
    if current_session_date != today_ny:
        current_session_date = today_ny
        alerted_symbols.clear()
        runner_alerted_today.clear()
        recent_high.clear()
        dip_play_seen.clear()
        halted_symbols.clear()
        print(f"[DEBUG] Reset alert state for new trading day: {today_ny}")
    
    # Reset EMAs only once per day at 04:00 ET for new trading session
    global last_ema_reset_date
    if (now_et.hour == 4 and now_et.minute == 0 and 
        current_session_date == today_ny and 
        last_ema_reset_date != today_ny):
        stored_emas.clear()
        last_ema_reset_date = today_ny
        print(f"[DEBUG] Reset stored EMAs for new trading session at 04:00 ET: {today_ny}")

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
    
    # Check if EMAs need backfilling for new symbols
    if not stored_emas[symbol][5].initialized:
        logger.info(f"[EMA BACKFILL] {symbol} | First candle encountered, backfilling EMAs...")
        backfill_success = await backfill_stored_emas(symbol)
        if not backfill_success:
            logger.warning(f"[EMA BACKFILL] {symbol} | Backfill failed, using current close price")
    
    # Update stored EMAs with new candle close price
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
            move_str = f"{move_pct:.1f}%"  # Negative sign already included
        
        alert_text = (
            f"🔥 <b>{escape_html(symbol)}</b> Volume Spike\n"
            f"Price: ${price_str} ({move_str})"
        )
        
        # Add to pending alerts instead of sending immediately
        pending_alerts[symbol].append({
            'type': 'volume_spike',
            'score': score,
            'message': alert_text
        })
        
        # Send the best alert for this symbol
        await send_best_alert(symbol)
        
        event_time = now
        log_event("volume_spike", symbol, get_display_price(symbol, close), volume, event_time, {
            "rvol": spike_data['rvol'],
            "vwap": vwap_value,
            "price_move": spike_data['price_move'],
            "alert_score": score
        })
        volume_spike_was_true[symbol] = True
        alerted_symbols[symbol] = today
        last_alert_time[symbol] = now

    # --- EMA STACK LOGIC PATCH (SESSION-AWARE THRESHOLDS) ---
    if (
        float_shares is not None and
        float_shares <= 10_000_000
    ):
        session_type = get_session_type(list(candles_seq)[-1]['start_time'])
        closes = [c['close'] for c in list(candles_seq)]  # Use all available candles
        
        # Session-aware minimum candle requirements
        if session_type == "premarket":
            min_candles = 15  # 15 minutes after 4am start
        elif session_type == "regular":
            min_candles = 20  # 20 minutes after 9:30am start  
        else:  # afterhours
            min_candles = 12  # 12 minutes after 4pm start
        
        # Check if we're after 11am ET for 200 EMA inclusion
        ny_tz = pytz.timezone("America/New_York")
        current_time = datetime.now(timezone.utc).astimezone(ny_tz).time()
        use_200_ema = current_time >= dt_time(11, 0) and len(closes) >= 90
        
        if len(closes) < min_candles:
            logger.info(f"[EMA STACK] {symbol}: Skipping - need {min_candles} candles, have {len(closes)} for {session_type}")
        else:
            # Get stored EMAs (much faster and more accurate)
            if use_200_ema:
                ema_periods = [5, 8, 13, 200]
                logger.info(f"[EMA STACK] {symbol}: Using stored 5,8,13,200 EMAs (after 11am, {len(closes)} candles)")
            else:
                ema_periods = [5, 8, 13]
                logger.info(f"[EMA STACK] {symbol}: Using stored 5,8,13 EMAs only (before 11am, {len(closes)} candles)")
            
            emas = get_stored_emas(symbol, ema_periods)
            
            # Check if EMAs are initialized
            if emas['ema5'] is None or emas['ema8'] is None or emas['ema13'] is None:
                logger.info(f"[EMA STACK] {symbol}: EMAs not yet initialized, skipping")
                return
            
            ema5 = emas['ema5']
            ema8 = emas['ema8']
            ema13 = emas['ema13']
            ema200 = emas.get('ema200') if use_200_ema else None
            vwap_value = vwap_candles_numpy(vwap_candles[symbol])
            last_candle = list(candles_seq)[-1]
            last_volume = last_candle['volume']
            price = closes[-1]

            if session_type in ["premarket", "afterhours"]:
                min_volume = 250_000
                min_dollar_volume = 100_000
            else:
                min_volume = 175_000
                min_dollar_volume = 100_000

            dollar_volume = price * last_volume

            # Base EMA stack criteria (5,8,13)
            base_ema_criteria = (
                ema5 > ema8 > ema13 and
                ema5 >= 1.015 * ema13 and
                price > vwap_value and
                ema5 > vwap_value and
                last_volume >= min_volume and
                dollar_volume >= min_dollar_volume
            )
            
            # Add 200 EMA criteria if available
            if use_200_ema and ema200 is not None:
                ema_stack_criteria = base_ema_criteria and price > ema200
            else:
                ema_stack_criteria = base_ema_criteria
            ema200_str = f", ema200={ema200:.2f}" if ema200 is not None else ""
            logger.info(f"[EMA STACK DEBUG] {symbol}: session={session_type}, ema5={ema5:.2f}, ema8={ema8:.2f}, ema13={ema13:.2f}{ema200_str}, vwap={vwap_value:.2f}, close={price:.2f}, volume={last_volume}, dollar_volume={dollar_volume:.2f}, ratio={ema5/ema13:.4f}, criteria={ema_stack_criteria}")

            if ema_stack_criteria and not ema_stack_was_true[symbol]:
                if ema5 / ema13 < 1.015:
                    logger.error(
                        f"[BUG] EMA stack alert would have fired but ratio invalid! {symbol}: ema5={ema5:.4f}, ema13={ema13:.4f}, ratio={ema5/ema13:.4f} (should be >= 1.015)"
                    )
                    return
                if last_trade_volume[symbol] < 250:
                    logger.info(f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})")
                    return
                if (now - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                    return
                log_event("ema_stack", symbol, get_display_price(symbol, price), last_volume, event_time, {
                    "ema5": ema5,
                    "ema8": ema8,
                    "ema13": ema13,
                    "vwap": vwap_value,
                    "session": session_type
                })
                price_str = f"{get_display_price(symbol, price):.2f}"
                ema_display = f"EMA5: {ema5:.2f}, EMA8: {ema8:.2f}, EMA13: {ema13:.2f}, VWAP: {vwap_value:.2f}"
                
                alert_text = (
                    f"⚡️ <b>{escape_html(symbol)}</b> EMA Stack [{session_type}]\n"
                    f"Current Price: ${price_str}\n"
                    f"{ema_display}"
                )
                # Calculate alert score and add to pending alerts
                alert_data = {
                    'rvol': last_volume / 100000 if last_volume > 100000 else 1.0,  # Approximate RVOL
                    'volume': last_volume,
                    'price_move': 0.02  # Assume 2% move for EMA stack
                }
                score = get_alert_score("ema_stack", symbol, alert_data)
                
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
                        await send_all_alerts(msg)
                        event_time = datetime.now(timezone.utc)
                        log_event("news_alert", symbol, 0, 0, event_time, {
                            "headline": title,
                            "link": link
                        })
                        news_seen.add(news_id)
                        save_news_id(news_id)
        await asyncio.sleep(60)

async def nasdaq_halt_scraper_loop():
    NASDAQ_HALTS_URL = "https://www.nasdaqtrader.com/rss.aspx?feed=tradehalts"
    seen_halts = set()
    halted_symbols = set()
    first_run = True

    while True:
        try:
            now = datetime.now(eastern)
            async with aiohttp.ClientSession() as session:
                async with session.get(NASDAQ_HALTS_URL) as resp:
                    rss = await resp.text()
            soup = BeautifulSoup(rss, "lxml")
            items = soup.find_all("item")

            for item in items:
                title_tag = item.find("title")
                pubdate_tag = item.find("pubDate") or item.find("pubdate")
                desc_tag = item.find("description")
              
                missing = []
                if not title_tag:
                    missing.append("title")
                if not pubdate_tag:
                    missing.append("pubDate")
                if not desc_tag:
                    missing.append("description")
                if missing:
                    logger.warning(f"[NASDAQ HALT SCRAPER] Skipping item due to missing {', '.join(missing)} tag(s): {item}")
                    continue

                try:
                    pub_dt = parsedate_to_datetime(pubdate_tag.text.strip())
                    pub_dt_eastern = pub_dt.astimezone(eastern)
                except Exception as e:
                    logger.warning(f"Could not parse or convert pubDate: {pubdate_tag.text} ({e})")
                    continue

                # Only today's halts during market hours
                if pub_dt_eastern.date() == now.date() and MARKET_OPEN <= pub_dt_eastern.time() <= MARKET_CLOSE:
                    symbol = title_tag.text.strip()
                    halt_time = pubdate_tag.text.strip()
                    reason = desc_tag.text.strip()
                    uid = f"{symbol}|{halt_time}"

                    if first_run:
                        # Don't alert on old items at startup, just record them
                        seen_halts.add(uid)
                        if reason.lower().startswith("halt"):
                            halted_symbols.add(symbol)
                        continue

                    if uid in seen_halts:
                        continue  # skip duplicates

                    seen_halts.add(uid)

                    # ---- BEGIN FILTERS PATCH ----
                    float_shares = get_float_shares(symbol)
                    if symbol not in FLOAT_EXCEPTION_SYMBOLS and (float_shares is None or not (MIN_FLOAT_SHARES <= float_shares <= MAX_FLOAT_SHARES)):
                        logger.info(f"[HALT DEBUG] Skipping {symbol}: float {float_shares} not in allowed range.")
                        continue
                    price = last_trade_price.get(symbol, None)
                    if price is None or not (0.10 <= price <= 20.00):
                        logger.info(f"[HALT DEBUG] Skipping {symbol}: price {price} not in $0.10-$20.00 range or unknown.")
                        continue
                    # ---- END FILTERS PATCH ----

                    if reason.lower().startswith("halt"):
                        print(f"🛑 {symbol} HALTED: {reason}")
                        halted_symbols.add(symbol)
                        msg = f"🛑 <b>{escape_html(symbol)}</b> HALTED (NASDAQ)\nReason: {escape_html(reason)}"
                        await send_all_alerts(msg)
                        event_time = datetime.now(timezone.utc)
                        log_event("halt", symbol, price, 0, event_time, {"source": "nasdaq_scraper", "reason": reason})
                    elif reason.lower().startswith("resume") and symbol in halted_symbols:
                        print(f"🟢 {symbol} RESUMED: {reason}")
                        halted_symbols.remove(symbol)
                        msg = f"🟢 <b>{escape_html(symbol)}</b> RESUMED (NASDAQ)\nReason: {escape_html(reason)}"
                        await send_all_alerts(msg)
                        event_time = datetime.now(timezone.utc)
                        log_event("resume", symbol, price, 0, event_time, {"source": "nasdaq_scraper", "reason": reason})
            if first_run:
                first_run = False

        except Exception as main_e:
            logger.error(f"Error in main loop: {main_e}")
        await asyncio.sleep(5)

async def premarket_gainers_alert_loop():
    eastern = pytz.timezone("America/New_York")
    sent_today = False
    while True:
        now_utc = datetime.now(timezone.utc)
        now_est = now_utc.astimezone(eastern)
        if now_est.weekday() in range(0, 5):
            if (now_est.time().hour == 9 and 
                now_est.time().minute == 24 and 
                now_est.time().second >= 55 and not sent_today):
                logger.info("Sending premarket gainers alert at 9:24:55am ET")
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
                    gainers_text = None  # Don't send message if no gainers found
                
                # Only send alert if there are actual gainers to report
                if gainers_text:
                    msg = (
                        "Market opens in 5 mins...secure the damn bag!\n"
                        "Here are the top 5 premarket gainers (since 4am):\n"
                        f"{gainers_text}"
                    )
                    await send_all_alerts(msg)
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
        
        # Only send alert on trading days (Mon-Thu) at 8:01 PM
        if now_est.weekday() in (0, 1, 2, 3):
            if now_est.time() >= dt_time(20, 1) and not sent_today:
                await send_all_alerts("Market Closed. Reconvene in pre market tomorrow.")
                event_time = datetime.now(timezone.utc)
                log_event("market_close", "CLOSE", 0, 0, event_time)
                sent_today = True
                
                # Reset session state after close (only when alert is sent)
                reset_symbol_state()
                # EMAs are NOT reset at market close - they persist overnight and only reset at 04:00 ET
        
        # Reset flag for next trading day (only reset at midnight)
        if now_est.time().hour == 0 and now_est.time().minute < 1:
            sent_today = False
            
        await asyncio.sleep(30)

async def handle_halt_event(event):
    symbol = event.get("sym")
    status = event.get("status")
    reason = event.get("reason", "")
    
    # Apply float and price filters (same as your other halt system)
    float_shares = get_float_shares(symbol)
    if symbol not in FLOAT_EXCEPTION_SYMBOLS and (float_shares is None or not (MIN_FLOAT_SHARES <= float_shares <= MAX_FLOAT_SHARES)):
        logger.info(f"[POLYGON HALT] Skipping {symbol}: float {float_shares} not in allowed range.")
        return
    
    price = last_trade_price.get(symbol, None)
    if price is None or not (0.10 <= price <= 20.00):
        logger.info(f"[POLYGON HALT] Skipping {symbol}: price {price} not in $0.10-$20.00 range or unknown.")
        return
    
    # Send halt alert for qualifying stocks
    msg = f"🛑 <b>{escape_html(symbol)}</b> HALTED (Polygon)\nPrice: ${price:.2f} | Float: {float_shares/1e6:.1f}M\nReason: {escape_html(reason)}"
    await send_all_alerts(msg)
    event_time = datetime.now(timezone.utc)
    log_event("halt", symbol, price, 0, event_time, {"status": status, "reason": reason, "source": "polygon"})
    halted_symbols.add(symbol)
    with open(HALT_LOG_FILE, "a") as f:
        f.write(f"{datetime.now(timezone.utc).isoformat()},{symbol},{status},{reason},polygon\n")
    logger.info(f"[POLYGON HALT] Alert sent for {symbol} at ${price:.2f}")

async def handle_resume_event(event):
    symbol = event.get("sym")
    reason = event.get("reason", "")
    
    # Only resume stocks that were halted and meet criteria
    if symbol in halted_symbols:
        price = last_trade_price.get(symbol, None)
        price_str = f"${price:.2f}" if price else "Unknown"
        
        msg = f"🟢 <b>{escape_html(symbol)}</b> RESUMED (Polygon)\nPrice: {price_str}\nReason: {escape_html(reason)}"
        await send_all_alerts(msg)
        event_time = datetime.now(timezone.utc)
        log_event("resume", symbol, price or 0, 0, event_time, {"reason": reason, "source": "polygon"})
        halted_symbols.remove(symbol)
        logger.info(f"[POLYGON RESUME] Alert sent for {symbol} at {price_str}")
    else:
        logger.info(f"[POLYGON RESUME] Ignoring {symbol} resume - not in halted_symbols or didn't meet criteria")

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
        if not is_market_scan_time():
            print("[SCAN PAUSED] Market closed (outside 4am-8pm EST, Mon-Fri). Sleeping 60s.")
            await asyncio.sleep(60)
            continue
        try:
            async with websockets.connect(url) as ws:
                await ws.send(json.dumps({"action": "auth", "params": POLYGON_API_KEY}))
                # Subscribe to ALL active stocks - full market scanning for dynamic discovery
                await ws.send(json.dumps({"action": "subscribe", "params": "AM.*,T.*,status"}))
                print("Subscribed to: AM.*, T.* (trades), and status (halts/resumes)")
                while True:
                    if not is_market_scan_time():
                        print("[SCAN PAUSED] Market closed during active connection. Sleeping 60s, breaking websocket.")
                        await asyncio.sleep(60)
                        break
                    msg = await ws.recv()
                    print("[RAW MSG]", msg)
                    
                    # Skip malformed or heartbeat messages
                    if not msg or len(msg.strip()) < 2 or not msg.strip().startswith('{') and not msg.strip().startswith('['):
                        print(f"[SKIP] Non-JSON message: '{msg}'")
                        continue
                        
                    try:
                        data = json.loads(msg)
                        if isinstance(data, dict):
                            data = [data]
                        for event in data:
                            if event.get("ev") == "T":
                                symbol = event["sym"]
                                price = event["p"]
                                size = event.get('s', 0)
                                
                                # Check eligibility BEFORE processing/logging
                                float_shares = get_float_shares(symbol)
                                if not is_eligible(symbol, price, float_shares):
                                    continue  # Skip ineligible symbols completely
                                
                                last_trade_price[symbol] = price
                                last_trade_volume[symbol] = size
                                last_trade_time[symbol] = datetime.now(timezone.utc)
                                print(
                                    f"[TRADE EVENT] {symbol} | Price={price} | Size={size} | Time={last_trade_time[symbol]}"
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
            elif "keepalive" in str(e).lower() or "ping" in str(e).lower() or "1011" in str(e):
                print("Keepalive/ping timeout - reconnecting immediately...")
                await asyncio.sleep(1)  # Immediate reconnection for ping timeouts
            else:
                print("General error - reconnecting in 3 seconds...")
                await asyncio.sleep(3)  # Much faster general reconnection

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
