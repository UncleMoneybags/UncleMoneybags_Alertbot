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
    market_start = dt_time(4, 0)
    market_end = dt_time(20, 0)
    is_weekday = now_ny.weekday() < 5
    return is_weekday and market_start <= now_ny.time() <= market_end

def get_ny_date():
    ny = pytz.timezone("America/New_York")
    now_utc = datetime.now(timezone.utc)
    now_ny = now_utc.astimezone(ny)
    return now_ny.date()

# Price freshness optimization: 5 seconds for real-time alerts
PRICE_FRESHNESS_SECONDS = 5

def get_display_price(symbol, fallback_price):
    now = datetime.now(timezone.utc)
    trade_time = last_trade_time.get(symbol)
    
    # If we have a very recent trade (within 5 seconds), use that price
    if trade_time and (now - trade_time).total_seconds() <= PRICE_FRESHNESS_SECONDS:
        return last_trade_price[symbol]
    
    # Otherwise use the fallback (candle close)
    return fallback_price

def escape_html(text):
    return html.escape(str(text))

async def send_telegram_alert(message):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json=payload) as resp:
                resp_text = await resp.text()
                if resp.status != 200:
                    logger.error(f"Telegram API error: {resp_text}")
                else:
                    logger.info("Telegram alert sent successfully")
    except Exception as e:
        logger.error(f"Failed to send Telegram alert: {e}")

async def send_discord_alert(message):
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        message_clean = re.sub(r'<[^>]+>', '', message)
        payload = {"content": message_clean}
        async with aiohttp.ClientSession() as session:
            async with session.post(DISCORD_WEBHOOK_URL, json=payload) as resp:
                if resp.status == 204:
                    logger.info("Discord alert sent successfully")
                else:
                    logger.error(f"Discord webhook error: {resp.status}")
    except Exception as e:
        logger.error(f"Failed to send Discord alert: {e}")

async def send_all_alerts(message):
    await asyncio.gather(
        send_telegram_alert(message),
        send_discord_alert(message)
    )

# --- PENDING ALERTS SYSTEM WITH SCORING ---
pending_alerts = defaultdict(list)  # symbol -> [{'type': str, 'score': float, 'message': str}]
ALERT_COOLDOWN_MINUTES = 2

# Enhanced scoring with more factors
def get_alert_score(alert_type, symbol, alert_data):
    """Calculate priority score for alert (higher = better)"""
    base_scores = {
        'volume_spike': 80,
        'runner': 75,
        'perfect_setup': 90,
        'ema_stack': 70,
        'warming_up': 60,
        'dip_play': 65
    }
    
    score = base_scores.get(alert_type, 50)
    
    # Volume factor (more volume = higher score)
    volume = alert_data.get('volume', 0)
    if volume > 1_000_000:
        score += 15
    elif volume > 500_000:
        score += 10
    elif volume > 100_000:
        score += 5
    
    # RVOL factor
    rvol = alert_data.get('rvol', 1.0)
    if rvol > 5.0:
        score += 15
    elif rvol > 3.0:
        score += 10
    elif rvol > 2.0:
        score += 5
    
    # Price movement factor
    price_move = abs(alert_data.get('price_move', 0))
    if price_move > 0.10:  # 10%+ move
        score += 15
    elif price_move > 0.05:  # 5%+ move
        score += 10
    elif price_move > 0.03:  # 3%+ move
        score += 5
    
    # Exception symbols get priority boost
    if symbol in FLOAT_EXCEPTION_SYMBOLS:
        score += 10
    
    return min(score, 100)  # Cap at 100

async def send_best_alert(symbol):
    """Send the highest scoring alert for a symbol and clear the queue"""
    if symbol not in pending_alerts or not pending_alerts[symbol]:
        return
    
    # Find the alert with the highest score
    best_alert = max(pending_alerts[symbol], key=lambda x: x['score'])
    
    # Send the best alert
    await send_all_alerts(best_alert['message'])
    
    # Log which alert was chosen
    logger.info(f"[BEST ALERT] {symbol} | Sent {best_alert['type']} alert with score {best_alert['score']}")
    if len(pending_alerts[symbol]) > 1:
        other_types = [a['type'] for a in pending_alerts[symbol] if a != best_alert]
        logger.info(f"[BEST ALERT] {symbol} | Skipped lower-priority alerts: {other_types}")
    
    # Clear all pending alerts for this symbol
    pending_alerts[symbol].clear()

# State tracking
candles = defaultdict(deque)
vwap_candles = defaultdict(list)
ema_stack_was_true = defaultdict(bool)
warming_up_was_true = defaultdict(bool)
runner_was_true = defaultdict(bool)
volume_spike_was_true = defaultdict(bool)
vwap_reclaim_was_true = defaultdict(bool)
dip_play_was_true = defaultdict(bool)
alerted_symbols = defaultdict(date)
runner_alerted_today = defaultdict(date)
recent_high = defaultdict(float)
dip_play_seen = set()
halted_symbols = set()
last_alert_time = defaultdict(lambda: datetime.min.replace(tzinfo=timezone.utc))
current_session_date = None

# Session reset logic - called every minute by the hour 0 logic
def reset_session_state():
        pending_alerts.clear()
        
        # Reset stored EMAs for all symbols
        for symbol in list(stored_emas.keys()):
            reset_stored_emas(symbol)
        
        # Clear candle data
        candles.clear()
        vwap_candles.clear()
        
        # Clear derived data
        vwap_cum_vol.clear()
        vwap_cum_pv.clear()
        
        # Clear session and daily state
        vwap_session_date.clear()
        
        # Reset alert states
        ema_stack_was_true.clear()
        warming_up_was_true.clear()
        runner_was_true.clear()
        volume_spike_was_true.clear()
        vwap_reclaim_was_true.clear()
        dip_play_was_true.clear()
        alerted_symbols.clear()
        runner_alerted_today.clear()
        recent_high.clear()
        dip_play_seen.clear()
        halted_symbols.clear()
        last_alert_time.clear()
        
        logger.info("Cleared all per-symbol session state for new trading day!")

def reset_symbol_state():
    """Reset symbol state and alert trackers"""
    global alerted_symbols, runner_alerted_today, ema_stack_was_true, vwap_reclaim_was_true, volume_spike_was_true, warming_up_was_true, runner_was_true, dip_play_was_true
    
    alerted_symbols.clear()
    runner_alerted_today.clear()
    recent_high.clear()
    dip_play_seen.clear()
    halted_symbols.clear()
    ema_stack_was_true.clear()
    vwap_reclaim_was_true.clear()
    volume_spike_was_true.clear()
    warming_up_was_true.clear()
    runner_was_true.clear()
    dip_play_was_true.clear()
    last_alert_time.clear()
    print(f"[DEBUG] Reset all symbol alert tracking state")

vwap_session_date = defaultdict(lambda: None)

def get_scanned_tickers():
    return list(candles.keys())[:100]

news_seen = load_news_seen()

# --- DYNAMIC SESSION DETECTION ---
def get_session_date(dt_utc):
    """Get the trading session date for a UTC datetime."""
    ny_tz = pytz.timezone("America/New_York")
    dt_ny = dt_utc.astimezone(ny_tz)
    
    # If it's before 4 AM NY time, consider it part of the previous day's session
    if dt_ny.time() < dt_time(4, 0):
        return (dt_ny - timedelta(days=1)).date()
    else:
        return dt_ny.date()

def get_session_type(dt_utc):
    """Determine the session type (premarket, regular, afterhours)"""
    ny_tz = pytz.timezone("America/New_York")
    dt_ny = dt_utc.astimezone(ny_tz)
    time_ny = dt_ny.time()
    
    if dt_time(4, 0) <= time_ny < dt_time(9, 30):
        return "premarket"
    elif dt_time(9, 30) <= time_ny < dt_time(16, 0):
        return "regular"
    else:
        return "afterhours"

def check_volume_spike(candles_seq, vwap_value):
    if len(candles_seq) < 5:
        return False, {}
    candles_list = list(candles_seq)
    recent_5 = candles_list[-5:]
    last_candle = recent_5[-1]
    volumes = [c['volume'] for c in candles_list[-20:]]
    avg_volume = sum(volumes) / len(volumes)
    rvol = last_candle['volume'] / avg_volume if avg_volume > 0 else 0
    if rvol < RVOL_SPIKE_THRESHOLD:
        return False, {}
    if last_candle['volume'] < RVOL_SPIKE_MIN_VOLUME:
        return False, {}
    last_close = last_candle['close']
    prev_volumes = [c['volume'] for c in recent_5[:-1]]
    avg_prev_volume = sum(prev_volumes) / len(prev_volumes)
    move_from_open = (last_close - last_candle['open']) / last_candle['open'] if last_candle['open'] > 0 else 0
    above_vwap = last_close > vwap_value
    spike_data = {
        'rvol': rvol,
        'volume': last_candle['volume'],
        'above_vwap': above_vwap,
        'price_move': move_from_open
    }
    return True, spike_data

HALT_LOG_FILE = "halt_log.csv"

def macd(prices, fast=12, slow=26, signal=9):
    prices = np.asarray(prices)
    if len(prices) < slow:
        return np.full(len(prices), np.nan), np.full(len(prices), np.nan), np.full(len(prices), np.nan)
    ema_fast = pd.Series(prices).ewm(span=fast).mean()
    ema_slow = pd.Series(prices).ewm(span=slow).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal).mean()
    histogram = macd_line - signal_line
    return macd_line.to_numpy(), signal_line.to_numpy(), histogram.to_numpy()

async def alert_perfect_setup(symbol, closes, volumes, highs, lows, candles_seq, vwap_value):
    if len(closes) < 30:
        return
    prices = closes[-20:]
    last_close = closes[-1]
    last_volume = volumes[-1]
    last_20_volumes = volumes[-20:]
    avg_volume = sum(last_20_volumes) / len(last_20_volumes)
    rvol = last_volume / avg_volume if avg_volume > 0 else 0
    
    # Calculate RSI
    rsi_vals = rsi(np.array(closes[-20:]))
    last_rsi = rsi_vals[-1] if not np.isnan(rsi_vals[-1]) else 50
    
    # Calculate MACD
    macd_line, signal_line, macd_hist = macd(closes)
    last_macd_hist = macd_hist[-1] if not np.isnan(macd_hist[-1]) else 0
    
    # Use stored EMAs for Perfect Setup
    emas = get_stored_emas(symbol, [5, 8, 13])
    
    if emas['ema5'] is None or emas['ema8'] is None or emas['ema13'] is None:
        logger.info(f"[PERFECT SETUP] {symbol}: EMAs not initialized, skipping")
        return
    
    ema5 = emas['ema5']
    ema8 = emas['ema8']
    ema13 = emas['ema13']
    
    logger.info(f"[STORED EMA] {symbol} | Perfect Setup | EMA5={ema5:.4f}, EMA8={ema8:.4f}, EMA13={ema13:.4f}")
    
    # Bullish engulfing pattern
    if len(candles_seq) >= 2:
        prev_candle = candles_seq[-2]
        curr_candle = candles_seq[-1]
        bullish_engulf = (
            prev_candle['close'] < prev_candle['open'] and  # Previous candle red
            curr_candle['close'] > curr_candle['open'] and  # Current candle green
            curr_candle['open'] < prev_candle['close'] and  # Current opens below prev close
            curr_candle['close'] > prev_candle['open']      # Current closes above prev open
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
                ema_display = f"EMA5: {ema5:.2f}, EMA8: {ema8:.2f}, EMA13: {ema13:.2f}"
                if ema200 is not None:
                    ema_display += f", EMA200: {ema200:.2f}"
                ema_display += f", VWAP: {vwap_value:.2f}"
                
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
                    gainers_text = "No premarket gainers found."
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
                # Reset stored EMAs for all symbols (only when alert is sent)
                for symbol in list(stored_emas.keys()):
                    reset_stored_emas(symbol)
        
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
                                last_trade_price[symbol] = event["p"]
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
            print(f"Websocket error: {e} — reconnecting in 10 seconds...")
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
