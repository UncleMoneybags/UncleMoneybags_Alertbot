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

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY","VmF1boger0pp2M7gV5HboHheRbplmLi5") 
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
    
    # Skip weekends
    if now_ny.weekday() >= 5:  # Saturday = 5, Sunday = 6
        return False
    
    current_time = now_ny.time()
    return MARKET_OPEN <= current_time <= MARKET_CLOSE

def escape_html(text):
    return html.escape(str(text))

def get_display_price(symbol, price):
    if last_trade_price[symbol] is not None:
        now = datetime.now(timezone.utc)
        if last_trade_time[symbol] and (now - last_trade_time[symbol]).total_seconds() <= 5:  # Reduced from 60 to 5 seconds for real-time alerts
            return last_trade_price[symbol]
    return price

def get_ny_date():
    return datetime.now(timezone.utc).astimezone(pytz.timezone("America/New_York")).date()

def get_session_type(dt):
    """Return session type: 'premarket', 'regular', or 'afterhours'"""
    ny = pytz.timezone("America/New_York")
    dt_ny = dt.astimezone(ny)
    current_time = dt_ny.time()
    
    if dt_time(4, 0) <= current_time < dt_time(9, 30):
        return "premarket"
    elif dt_time(9, 30) <= current_time < dt_time(16, 0):
        return "regular"
    else:
        return "afterhours"

async def send_telegram_alert(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML"
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=data) as response:
                if response.status != 200:
                    logger.error(f"Telegram API error: {response.status}")
                    logger.error(await response.text())
    except Exception as e:
        logger.error(f"Error sending Telegram alert: {e}")

async def send_discord_alert(message):
    try:
        # Replace HTML tags with Discord markdown
        discord_message = message.replace("<b>", "**").replace("</b>", "**")
        
        data = {
            "content": discord_message,
            "username": "Stock Scanner"
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(DISCORD_WEBHOOK_URL, json=data) as response:
                if response.status not in [200, 204]:
                    logger.error(f"Discord API error: {response.status}")
                    logger.error(await response.text())
    except Exception as e:
        logger.error(f"Error sending Discord alert: {e}")

async def send_all_alerts(message):
    """Send alert to both Telegram and Discord"""
    await asyncio.gather(
        send_telegram_alert(message),
        send_discord_alert(message),
        return_exceptions=True
    )

def macd(prices, fast=12, slow=26, signal=9):
    prices = np.asarray(prices, dtype=float)
    if len(prices) < slow:
        return None, None, None
    ema_fast = pd.Series(prices).ewm(span=fast).mean().to_numpy()
    ema_slow = pd.Series(prices).ewm(span=slow).mean().to_numpy()
    macd_line = ema_fast - ema_slow
    signal_line = pd.Series(macd_line).ewm(span=signal).mean().to_numpy()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

# --- GLOBAL VARS ---
candles = defaultdict(lambda: deque(maxlen=50))
vwap_candles = defaultdict(list)
alerted_symbols = {}
warming_up_was_true = defaultdict(bool)
runner_was_true = defaultdict(bool)
dip_play_was_true = defaultdict(bool)
ema_stack_was_true = defaultdict(bool)
vwap_reclaim_was_true = defaultdict(bool)
volume_spike_was_true = defaultdict(bool)
recent_high = defaultdict(float)
current_session_date = None
runner_alerted_today = {}
dip_play_seen = set()
volume_spike_alerted = set()
rvol_spike_alerted = set()
halted_symbols = set()

ALERT_COOLDOWN_MINUTES = 2  # Reduced from 5 to 2 minutes for faster day trading alerts
last_alert_time = defaultdict(lambda: datetime.min.replace(tzinfo=timezone.utc))

# --- ALERT PRIORITIZATION SYSTEM ---
ALERT_PRIORITIES = {
    'perfect_setup': 5,
    'runner': 4,
    'volume_spike': 3,
    'ema_stack': 2,
    'warming_up': 1,
    'dip_play': 1,
    'vwap_reclaim': 1
}

# Store pending alerts by symbol
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

def get_alert_score(alert_type, symbol, alert_data):
    """Calculate alert score based on multiple factors"""
    base_score = ALERT_PRIORITIES.get(alert_type, 1) * 10
    
    # RVOL bonus
    rvol = alert_data.get('rvol', 1.0)
    rvol_bonus = min(rvol * 5, 25)  # Max 25 points
    
    # Volume bonus  
    volume = alert_data.get('volume', 0)
    volume_bonus = min(volume / 10000, 20)  # Max 20 points
    
    # Price move bonus
    price_move = abs(alert_data.get('price_move', 0))
    move_bonus = min(price_move * 100, 15)  # Max 15 points
    
    total_score = base_score + rvol_bonus + volume_bonus + move_bonus
    
    # ML bonus if available
    if runner_clf is not None:
        try:
            ml_score = score_event_ml(alert_type, symbol, 
                                    alert_data.get('price', 1.0),
                                    volume, rvol, False) * 30  # Max 30 points
            total_score += ml_score
        except Exception as e:
            logger.warning(f"ML scoring error for {symbol}: {e}")
    
    return min(total_score, 100)  # Cap at 100

def check_volume_spike(candles_seq, vwap_value):
    if len(candles_seq) < 6:
        return False, {}
    
    recent_6 = list(candles_seq)[-6:]
    last_candle = recent_6[-1]
    prev_5_avg = sum(c['volume'] for c in recent_6[:-1]) / 5
    
    rvol = last_candle['volume'] / prev_5_avg if prev_5_avg > 0 else 0
    price_move = (last_candle['close'] - last_candle['open']) / last_candle['open'] if last_candle['open'] > 0 else 0
    
    spike_data = {
        'volume': last_candle['volume'],
        'rvol': rvol,
        'above_vwap': last_candle['close'] > vwap_value,
        'price_move': price_move
    }
    
    spike_detected = (
        rvol >= 2.5 and
        last_candle['volume'] >= 100_000 and
        last_candle['close'] > vwap_value and
        price_move >= 0.02
    )
    
    return spike_detected, spike_data

async def alert_perfect_setup(symbol, closes, volumes, highs, lows, candles_seq, vwap_value):
    if ema_stack_was_true[symbol]:
        return
    
    now = datetime.now(timezone.utc)
    if (now - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
        return
    
    prices = np.asarray(closes[-30:], dtype=float)
    
    # Use stored EMAs instead of calculating from scratch
    emas = get_stored_emas(symbol, [5, 8, 13])
    ema5 = emas.get('ema5')
    ema8 = emas.get('ema8') 
    ema13 = emas.get('ema13')
    
    # Skip if EMAs not ready
    if None in [ema5, ema8, ema13]:
        return
    
    rsi_arr = rsi(prices)
    last_rsi = rsi_arr[-1] if len(rsi_arr) > 0 and not np.isnan(rsi_arr[-1]) else 50
    macd_line, macd_signal, macd_hist = macd(prices)
    last_macd_hist = macd_hist[-1] if macd_hist is not None else 0
    last_close = closes[-1]
    last_volume = volumes[-1]
    
    # Volume-based RVOL calculation
    recent_vols = volumes[-20:] if len(volumes) >= 20 else volumes
    avg_vol = sum(recent_vols[:-1]) / len(recent_vols[:-1]) if len(recent_vols) > 1 else 1
    rvol = last_volume / avg_vol if avg_vol > 0 else 0
    
    # Bullish engulfing
    if len(candles_seq) >= 2:
        prev_candle = candles_seq[-2]
        curr_candle = candles_seq[-1]
        bullish_engulf = (
            prev_candle['close'] < prev_candle['open'] and
            curr_candle['close'] > curr_candle['open'] and
            curr_candle['open'] < prev_candle['close'] and
            curr_candle['close'] > prev_candle['open']
        )
    else:
        bullish_engulf = False

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
        move_str = f"{spike_data['price_move']*100:.1f}%" if spike_data['price_move'] > 0 else "0.0%"
        
        alert_text = (
            f"🔥 <b>{escape_html(symbol)}</b> Volume Spike\n"
            f"Price: ${price_str} (+{move_str})"
        )
        
        # Add to pending alerts instead of sending immediately
        pending_alerts[symbol].append({
            'type': 'volume_spike',
            'score': score,
            'message': alert_text
        })
        
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
        
        logger.info(f"[EMA STACK] {symbol}: Skipping - need {min_candles} candles, have {len(closes)} for {session_type}")
        
        if len(closes) >= min_candles and not ema_stack_was_true[symbol]:
            # Use stored EMAs for stack checking 
            emas = get_stored_emas(symbol, [5, 8, 13])
            ema5 = emas.get('ema5')
            ema8 = emas.get('ema8')
            ema13 = emas.get('ema13')
            
            # Skip if EMAs not ready
            if None in [ema5, ema8, ema13]:
                logger.info(f"[EMA STACK] {symbol}: EMAs not ready - EMA5={ema5}, EMA8={ema8}, EMA13={ema13}")
                return
            
            # Session-aware thresholds
            if session_type == "premarket":
                volume_threshold = 25_000
                gap_threshold = 1.008  # 0.8%
            elif session_type == "regular":
                volume_threshold = 100_000
                gap_threshold = 1.015  # 1.5%
            else:  # afterhours
                volume_threshold = 15_000
                gap_threshold = 1.005  # 0.5%
            
            vwap_val = vwap_candles_numpy(vwap_candles[symbol]) if vwap_candles[symbol] else 0
            last_close = closes[-1]
            
            ema_stack_criteria = (
                ema5 > ema8 > ema13 and
                ema5 >= gap_threshold * ema13 and
                last_close > vwap_val and
                volume >= volume_threshold
            )
            
            logger.info(f"[EMA STACK DEBUG] {symbol} | {session_type} | EMA5={ema5:.4f} > EMA8={ema8:.4f} > EMA13={ema13:.4f} | gap_ok={ema5 >= gap_threshold * ema13} | above_vwap={last_close > vwap_val} | vol_ok={volume >= volume_threshold}")
            logger.info(
                f"[ALERT DEBUG] {symbol} | Alert Type: ema_stack | VWAP={vwap_val:.4f} | Last Trade={last_trade_price[symbol]} | Candle Close={last_close} | Candle Volume={volume}"
            )
            logger.info(f"[EMA DEBUG] {symbol} | EMA Stack | EMA5={ema5:.4f}, EMA8={ema8:.4f}, EMA13={ema13:.4f}")
            
            if ema_stack_criteria:
                if (now - last_alert_time[symbol]) < timedelta(minutes=ALERT_COOLDOWN_MINUTES):
                    return
                if last_trade_volume[symbol] < 250:
                    logger.info(f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})")
                    return
                    
                event_time = now
                log_event("ema_stack", symbol, get_display_price(symbol, last_close), volume, event_time, {
                    "session_type": session_type,
                    "volume_threshold": volume_threshold,
                    "gap_threshold": gap_threshold,
                    "ema5": ema5,
                    "ema8": ema8,
                    "ema13": ema13,
                    "vwap": vwap_val
                })
                
                price_str = f"{get_display_price(symbol, last_close):.2f}"
                alert_text = (
                    f"📊 <b>{escape_html(symbol)}</b> EMA Stack ({session_type.capitalize()})\n"
                    f"Price: ${price_str} | Vol: {int(volume/1000)}K"
                )
                
                # Calculate alert score and add to pending alerts
                alert_data = {
                    'rvol': 2.0,  # Assume decent RVOL
                    'volume': volume,
                    'price_move': 0.02  # Small price move assumption
                }
                score = get_alert_score("ema_stack", symbol, alert_data)
                
                # Add to pending alerts instead of sending immediately
                pending_alerts[symbol].append({
                    'type': 'ema_stack',
                    'score': score,
                    'message': alert_text
                })
                
                ema_stack_was_true[symbol] = True
                alerted_symbols[symbol] = today
                last_alert_time[symbol] = now

    # Process pending alerts for this symbol
    if pending_alerts[symbol]:
        await send_best_alert(symbol)

async def trade_stream():
    try:
        url = f"wss://socket.polygon.io/stocks"
        async with websockets.connect(url) as websocket:
            auth_data = {"action": "auth", "params": POLYGON_API_KEY}
            await websocket.send(json.dumps(auth_data))
            response = await websocket.recv()
            print(f"[POLYGON AUTH] {response}")
            
            subscription_msg = {"action": "subscribe", "params": "T.*"}
            await websocket.send(json.dumps(subscription_msg))
            response = await websocket.recv()
            print(f"[POLYGON TRADE SUB] {response}")
            
            async for message in websocket:
                try:
                    data = json.loads(message)
                    for item in data:
                        if item.get("ev") == "T":
                            symbol = item.get("sym")
                            price = item.get("p")
                            size = item.get("s")
                            timestamp = item.get("t")
                            if symbol and price and size and timestamp:
                                time_obj = polygon_time_to_utc(timestamp)
                                last_trade_price[symbol] = price
                                last_trade_volume[symbol] = size
                                last_trade_time[symbol] = time_obj
                                print(f"[TRADE EVENT] {symbol} | Price={price} | Size={size} | Time={time_obj}")
                except Exception as e:
                    logger.error(f"Error processing trade message: {e}")
    except Exception as e:
        logger.error(f"Trade stream error: {e}")
        await asyncio.sleep(10)

def get_polygon_tickers():
    try:
        import requests
        url = "https://api.polygon.io/v3/reference/tickers"
        params = {
            "apikey": POLYGON_API_KEY,
            "active": "true",
            "market": "stocks",
            "limit": 1000,
            "sort": "ticker"
        }
        
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            tickers = [result["ticker"] for result in data.get("results", [])]
            return tickers
        else:
            logger.error(f"Error fetching tickers: {response.status_code}")
            return []
    except Exception as e:
        logger.error(f"Error in get_polygon_tickers: {e}")
        return []

async def agg_ingest_loop():
    url = f"wss://socket.polygon.io/stocks"
    
    while True:
        try:
            logger.info(f"[POLYGON] Connecting to {url}")
            async with websockets.connect(url) as websocket:
                auth_data = {"action": "auth", "params": POLYGON_API_KEY}
                await websocket.send(json.dumps(auth_data))
                response = await websocket.recv()
                logger.info(f"[POLYGON AUTH] {response}")
                
                subscription_msg = {"action": "subscribe", "params": "AM.*"}
                await websocket.send(json.dumps(subscription_msg))
                response = await websocket.recv()
                logger.info(f"[POLYGON AGG SUB] {response}")
                
                last_outcome_check = datetime.now(timezone.utc)
                last_model_check = datetime.now(timezone.utc)
                
                async for message in websocket:
                    try:
                        print(f"[RAW MSG] {message}")
                        data = json.loads(message)
                        
                        for item in data:
                            if item.get("ev") == "AM":
                                symbol = item.get("sym")
                                if not symbol:
                                    continue
                                
                                open_price = item.get("o")
                                high_price = item.get("h")
                                low_price = item.get("l")
                                close_price = item.get("c")
                                volume = item.get("v")
                                start_time_ms = item.get("s")
                                
                                if all(v is not None for v in [open_price, high_price, low_price, close_price, volume, start_time_ms]):
                                    start_time = polygon_time_to_utc(start_time_ms)
                                    print(f"[POLYGON] {symbol} {start_time} o:{open_price} h:{high_price} l:{low_price} c:{close_price} v:{volume}")
                                    await on_new_candle(symbol, open_price, high_price, low_price, close_price, volume, start_time)
                        
                        # Periodic checks
                        now = datetime.now(timezone.utc)
                        
                        # Check alert outcomes every 5 minutes
                        if (now - last_outcome_check).total_seconds() > 300:
                            await check_alert_outcomes()
                            last_outcome_check = now
                        
                        # Check model retraining every 30 minutes
                        if (now - last_model_check).total_seconds() > 1800:
                            await retrain_model_if_needed()
                            last_model_check = now
                            
                    except Exception as e:
                        logger.error(f"Error processing message: {e}")
                        continue
                        
        except websockets.exceptions.ConnectionClosed:
            logger.warning("[POLYGON] Connection closed, reconnecting in 5 seconds...")
            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"[POLYGON] Connection error: {e}, reconnecting in 10 seconds...")
            await asyncio.sleep(10)

async def market_close_alert_loop():
    while True:
        try:
            ny_tz = pytz.timezone("America/New_York")
            now_ny = datetime.now(ny_tz)
            
            # Only send on weekdays
            if now_ny.weekday() < 5:  # Monday = 0, Friday = 4
                # 8:01 PM alert
                target_time = now_ny.replace(hour=20, minute=1, second=0, microsecond=0)
                if abs((now_ny - target_time).total_seconds()) < 30:
                    await send_all_alerts("📊 <b>Market Closed</b> | 8:01 PM EST | Processing complete.")
                    await asyncio.sleep(60)
            
            await asyncio.sleep(30)
        except Exception as e:
            logger.error(f"Market close alert error: {e}")
            await asyncio.sleep(60)

async def premarket_gainers_alert_loop():
    while True:
        try:
            ny_tz = pytz.timezone("America/New_York")
            now_ny = datetime.now(ny_tz)
            
            # Only send on weekdays
            if now_ny.weekday() < 5:
                # 9:24:55 AM alert
                target_time = now_ny.replace(hour=9, minute=24, second=55, microsecond=0)
                if abs((now_ny - target_time).total_seconds()) < 10:
                    await send_premarket_gainers_alert()
                    await asyncio.sleep(60)
            
            await asyncio.sleep(10)
        except Exception as e:
            logger.error(f"Premarket gainers alert error: {e}")
            await asyncio.sleep(60)

async def send_premarket_gainers_alert():
    try:
        gainers = []
        for symbol, open_price in premarket_open_prices.items():
            if symbol in premarket_last_prices and open_price > 0:
                last_price = premarket_last_prices[symbol]
                gain_pct = ((last_price - open_price) / open_price) * 100
                volume = premarket_volumes.get(symbol, 0)
                
                if gain_pct >= 10 and volume >= 10000:
                    gainers.append({
                        'symbol': symbol,
                        'gain_pct': gain_pct,
                        'volume': volume,
                        'price': last_price
                    })
        
        gainers.sort(key=lambda x: x['gain_pct'], reverse=True)
        gainers = gainers[:10]
        
        if gainers:
            alert_text = "🚀 <b>Premarket Gainers (10%+)</b>\n\n"
            for g in gainers:
                alert_text += f"<b>{escape_html(g['symbol'])}</b> +{g['gain_pct']:.1f}% | ${g['price']:.2f} | Vol: {int(g['volume']/1000)}K\n"
            
            await send_all_alerts(alert_text)
        
        # Clear premarket data for next session
        premarket_open_prices.clear()
        premarket_last_prices.clear()
        premarket_volumes.clear()
        
    except Exception as e:
        logger.error(f"Error sending premarket gainers alert: {e}")

async def main():
    logger.info("Starting Stock Market Scanner...")
    
    if not is_market_scan_time():
        logger.info("Outside market hours (4am-8pm EST, Mon-Fri). Monitoring market schedule...")
    
    ingest_task = asyncio.create_task(agg_ingest_loop())
    # trade_task = asyncio.create_task(trade_stream())  # ❌ DISABLED to prevent connection limit issues
    
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
