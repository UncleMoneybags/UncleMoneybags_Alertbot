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
import pandas as pd
import time

MARKET_OPEN = dt_time(4, 0)
MARKET_CLOSE = dt_time(20, 0)
eastern = pytz.timezone("America/New_York")
logger = logging.getLogger(__name__)

# 🚀 PERFORMANCE: Reusable HTTP session (30-50% faster than creating new sessions)
http_session = None

last_trade_price = defaultdict(lambda: None)
last_trade_volume = defaultdict(lambda: 0)
last_trade_time = defaultdict(lambda: None)

# 🎯 GRANDFATHERING: Track entry price when stock first becomes eligible
# Once eligible, keep monitoring even if price runs past $15 threshold
entry_price = defaultdict(lambda: None)

float_cache = {}
float_cache_none_retry = {}
FLOAT_CACHE_NONE_RETRY_MIN = 10  # minutes


def save_float_cache():
    with open("float_cache.pkl", "wb") as f:
        pickle.dump(float_cache, f)
    with open("float_cache_none.pkl", "wb") as f:
        pickle.dump(float_cache_none_retry, f)
    print(
        f"[DEBUG] Saved float cache, entries: {len(float_cache)}, none cache: {len(float_cache_none_retry)}"
    )


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
        print(
            f"[DEBUG] Loaded float_cache_none_retry, entries: {len(float_cache_none_retry)}"
        )
    else:
        float_cache_none_retry = {}
        print(f"[DEBUG] No float_cache_none_retry found, starting new.")


async def get_float_shares(ticker):
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
        await asyncio.sleep(0.5)
        return float_shares
    except Exception as e:
        float_cache_none_retry[ticker] = now
        save_float_cache()
        if "Rate limited" in str(e):
            # Adaptive backoff for API rate limits - VPS optimization
            await asyncio.sleep(5)  # Start with 5s for VPS
        return float_cache.get(ticker, None)


load_float_cache()
# --- END FLOAT PATCH ---

# Filtering counters for debugging
filter_counts = defaultdict(int)


def is_eligible(symbol, last_price, float_shares, use_entry_price=False):
    """Check if symbol meets filtering criteria: price <= $15 AND float <= 20M
    
    Args:
        use_entry_price: If True and symbol has entry_price, use that instead of current price
                        (allows grandfathering - keeps tracking stocks that run past $15)
    """
    # 🎯 GRANDFATHERING: Use entry price for already-tracked stocks
    check_price = entry_price.get(symbol) if use_entry_price and entry_price.get(symbol) else last_price
    
    # Price check - ALL symbols must be <= $15 (increased for early momentum detection!)
    if check_price is None:
        filter_counts["price_none"] += 1
        if filter_counts["price_none"] % 100 == 1:  # Log every 100th occurrence
            logger.info(
                f"[FILTER DEBUG] {symbol} filtered: price is None (count: {filter_counts['price_none']})"
            )
        return False
    elif check_price > PRICE_THRESHOLD:
        filter_counts["price_too_high"] += 1
        if symbol in ["OCTO", "GRND", "EQS", "OSRH", "BJDX", "EBMT", "GLTO"
                      ] or filter_counts["price_too_high"] % 50 == 1:
            logger.info(
                f"[FILTER DEBUG] {symbol} filtered: price ${check_price:.2f} > ${PRICE_THRESHOLD} (count: {filter_counts['price_too_high']}) [using {'entry' if use_entry_price and entry_price.get(symbol) else 'current'} price]"
            )
        return False

    # Float check - exception symbols bypass ONLY float filter
    if symbol not in FLOAT_EXCEPTION_SYMBOLS:
        # FAIL-CLOSED: must have valid float data AND be <= 10M
        if float_shares is None:
            filter_counts["float_none"] += 1
            if symbol in ["OCTO", "GRND", "EQS"
                          ] or filter_counts["float_none"] % 100 == 1:
                logger.info(
                    f"[FILTER DEBUG] {symbol} filtered: float_shares is None (count: {filter_counts['float_none']})"
                )
            return False
        elif not (MIN_FLOAT_SHARES <= float_shares <= MAX_FLOAT_SHARES):
            filter_counts["float_out_of_range"] += 1
            if symbol in ["OCTO", "GRND", "EQS"
                          ] or filter_counts["float_out_of_range"] % 50 == 1:
                logger.info(
                    f"[FILTER DEBUG] {symbol} filtered: float_shares {float_shares/1e6:.1f}M not in range {MIN_FLOAT_SHARES/1e6:.1f}M-{MAX_FLOAT_SHARES/1e6:.1f}M (count: {filter_counts['float_out_of_range']})"
                )
            return False
    else:
        # Log exception symbols that are passing filter
        logger.info(
            f"[FILTER EXCEPTION] {symbol} bypassing float filter (float: {float_shares/1e6:.1f}M if known)"
        )

    # Symbol passed all filters
    filter_counts["passed"] += 1
    if symbol in ["OCTO", "GRND", "EQS"] or filter_counts["passed"] % 100 == 1:
        logger.info(
            f"[FILTER PASS] {symbol} eligible: price=${last_price:.2f}, float={float_shares/1e6:.1f}M (total passed: {filter_counts['passed']})"
        )

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
                self.profile[sym] = {
                    int(minidx): vols
                    for minidx, vols in by_min.items()
                }
        else:
            self.profile = defaultdict(lambda: defaultdict(list))

    def _save_profile(self):
        serializable = {
            sym: {
                str(minidx): vols
                for minidx, vols in by_min.items()
            }
            for sym, by_min in self.profile.items()
        }
        with open(PROFILE_FILE, "w") as f:
            json.dump(serializable, f)

    def add_day(self, symbol, daily_candles):
        for candle in daily_candles:
            minute_idx = get_minute_of_day(candle['start_time'])
            vol = candle['volume']
            if minute_idx < 0 or minute_idx >= MINUTES_PER_SESSION:
                continue
            self.profile.setdefault(symbol, {}).setdefault(minute_idx,
                                                           []).append(vol)
            if len(self.profile[symbol][minute_idx]) > DAYS_TO_KEEP:
                self.profile[symbol][minute_idx] = self.profile[symbol][
                    minute_idx][-DAYS_TO_KEEP:]
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


# --- BACKFILL FOR MISSED ALERTS ---
async def perform_connection_backfill():
    """Fetch missed minute bars during disconnection and run alert logic"""
    try:
        now = datetime.now(timezone.utc)
        eastern = pytz.timezone("America/New_York")
        now_et = now.astimezone(eastern)

        # Only backfill during market hours
        if not is_market_scan_time():
            print("[BACKFILL] Outside market hours, skipping backfill")
            return

        # Get last 10 minutes of data for active symbols
        backfill_minutes = 10
        end_time = now_et
        start_time = end_time - timedelta(minutes=backfill_minutes)

        # Get list of symbols that have recent activity
        active_symbols = []
        for symbol in list(
                last_trade_time.keys())[:50]:  # Limit to 50 most recent
            if symbol in last_trade_time and last_trade_time[symbol]:
                time_since_last = (now -
                                   last_trade_time[symbol]).total_seconds()
                if time_since_last < 1800:  # Active in last 30 minutes
                    active_symbols.append(symbol)

        print(
            f"[BACKFILL] Checking {len(active_symbols)} active symbols for missed alerts..."
        )

        # Fetch recent minute bars for active symbols (batch request)
        for symbol in active_symbols[:20]:  # Limit to 20 to avoid API limits
            try:
                start_date = start_time.strftime('%Y-%m-%d')
                end_date = end_time.strftime('%Y-%m-%d')

                url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/minute/{start_date}/{end_date}"
                params = {
                    'adjusted': 'true',
                    'sort': 'asc',
                    'limit': backfill_minutes,
                    'apikey': POLYGON_API_KEY
                }

                async with http_session.get(url, params=params) as response:
                    if response.status == 200:
                        data = await response.json()
                        if data.get('status') == 'OK' and data.get('results'):
                            candles = data['results']
                            print(
                                f"[BACKFILL] {symbol}: Processing {len(candles)} missed candles"
                            )

                            # Process each missed candle through alert logic
                            for candle_data in candles:
                                candle_time = polygon_time_to_utc(
                                    candle_data['t'])
                                await on_new_candle(
                                    symbol,
                                    candle_data['o'],  # open
                                    candle_data['h'],  # high  
                                    candle_data['l'],  # low
                                    candle_data['c'],  # close
                                    candle_data['v'],  # volume
                                    candle_time)
                                await send_best_alert(symbol)

                await asyncio.sleep(0.1)  # Rate limiting

            except Exception as e:
                print(f"[BACKFILL ERROR] {symbol}: {e}")

        print(f"[BACKFILL] Completed catch-up analysis")

    except Exception as e:
        print(f"[BACKFILL ERROR] General error: {e}")


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
            self.ema = (price * self.multiplier) + (self.ema *
                                                    (1 - self.multiplier))
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

    # Removed verbose logging for performance - EMAs update thousands of times per second
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
    latest_date = trading_days[0]  # First (latest) trading day

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

        async with http_session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                if data.get('status') == 'OK' and data.get('results'):
                    candles = data['results']
                    logger.info(
                        f"[EMA BACKFILL] {symbol} | Fetched {len(candles)} historical candles"
                    )

                    # Seed EMAs with historical closes in chronological order
                    for candle in candles:
                        close_price = candle['c']
                        for period in [5, 8, 13, 200]:
                            stored_emas[symbol][period].update(close_price)

                    # Log final seeded values
                    emas = get_stored_emas(symbol, [5, 8, 13, 200])
                    logger.info(
                        f"[EMA BACKFILL] {symbol} | Seeded EMAs - EMA5: {emas['ema5']:.4f} | EMA8: {emas['ema8']:.4f} | EMA13: {emas['ema13']:.4f} | EMA200: {emas['ema200']:.4f}"
                    )
                    return True
                else:
                    logger.warning(
                        f"[EMA BACKFILL] {symbol} | No data available for backfill"
                    )
                    return False
            else:
                logger.error(
                    f"[EMA BACKFILL] {symbol} | API error: {response.status}"
                )
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
def calculate_emas(prices,
                   periods=[5, 8, 13],
                   window=30,
                   symbol=None,
                   latest_trade_price=None):
    """Legacy EMA calculation - kept for fallback"""
    prices = list(prices)[-window:]
    if latest_trade_price is not None and len(prices) > 0:
        prices[-1] = latest_trade_price
    s = pd.Series(prices)
    emas = {}
    logger.info(
        f"[LEGACY EMA] {symbol if symbol else ''} | Using legacy calculation")
    for p in periods:
        emas[f"ema{p}"] = s.ewm(span=p, adjust=False).mean().to_numpy()
        logger.info(
            f"[LEGACY EMA] {symbol if symbol else ''} | EMA{p} latest: {emas[f'ema{p}'][-1]}"
        )
    return emas


def vwap_numpy(prices, volumes):
    """🚨 ENHANCED VWAP: Bulletproof calculation with comprehensive validation"""
    # Convert to numpy arrays with validation
    prices = np.asarray(prices, dtype=float)
    volumes = np.asarray(volumes, dtype=float)

    # Validate input arrays
    if len(prices) != len(volumes):
        raise ValueError(
            f"Price and volume arrays must have same length: {len(prices)} vs {len(volumes)}"
        )
    if len(prices) == 0:
        raise ValueError("Cannot calculate VWAP with empty arrays")

    # Check for invalid values
    if np.any(np.isnan(prices)) or np.any(np.isnan(volumes)):
        raise ValueError("Price or volume arrays contain NaN values")
    if np.any(np.isinf(prices)) or np.any(np.isinf(volumes)):
        raise ValueError("Price or volume arrays contain infinite values")
    if np.any(prices <= 0):
        raise ValueError(
            f"All prices must be positive, got: {prices[prices <= 0]}")
    if np.any(volumes <= 0):
        raise ValueError(
            f"All volumes must be positive, got: {volumes[volumes <= 0]}")

    # Calculate VWAP with high precision
    price_volume_products = np.multiply(prices, volumes, dtype=np.float64)
    total_vol = np.sum(volumes, dtype=np.float64)
    total_pv = np.sum(price_volume_products, dtype=np.float64)

    if total_vol <= 0:
        raise ValueError(f"Total volume must be positive, got: {total_vol}")

    vwap_result = total_pv / total_vol

    # Final validation of result
    if not np.isfinite(vwap_result) or vwap_result <= 0:
        raise ValueError(
            f"VWAP calculation resulted in invalid value: {vwap_result}")

    return float(vwap_result)


def vwap_candles_numpy(candles):
    """🚨 ENHANCED CANDLE VWAP: Bulletproof calculation with error handling"""
    if not candles:
        logger.info("[VWAP DEBUG] No candles, returning 0")
        return 0.0

    try:
        # Extract typical prices and volumes with validation
        prices = []
        volumes = []
        for i, candle in enumerate(candles):
            # Validate candle structure
            required_fields = ['high', 'low', 'close', 'volume']
            for field in required_fields:
                if field not in candle:
                    raise ValueError(
                        f"Candle {i} missing required field: {field}")

            # Calculate typical price (HLC/3)
            typical_price = (candle['high'] + candle['low'] +
                             candle['close']) / 3
            prices.append(typical_price)
            volumes.append(candle['volume'])

        logger.info(f"[VWAP DEBUG] Prices used: {prices}")
        logger.info(f"[VWAP DEBUG] Volumes used: {volumes}")

        # Use enhanced VWAP calculation
        vwap_val = vwap_numpy(prices, volumes)
        logger.info(f"[VWAP DEBUG] VWAP result: {vwap_val}")
        return vwap_val

    except Exception as e:
        logger.error(
            f"[VWAP ERROR] Failed to calculate VWAP from candles: {e}")
        return 0.0  # Return 0 on error (will be caught by validation later)


def get_valid_vwap(symbol):
    """🚨 CENTRALIZED VWAP GUARD: Returns valid VWAP or None if insufficient data"""
    if not vwap_candles[symbol] or len(vwap_candles[symbol]) < 3:
        logger.info(
            f"[VWAP GUARD] {symbol} - Insufficient VWAP data ({len(vwap_candles[symbol]) if symbol in vwap_candles else 0} candles)"
        )
        return None

    # 🚨 CRITICAL VALIDATION: Ensure all candle data is valid before VWAP calculation
    candles = vwap_candles[symbol]
    for i, candle in enumerate(candles):
        # Validate required fields exist and are positive
        required_fields = ['high', 'low', 'close', 'volume']
        for field in required_fields:
            if field not in candle or candle[field] is None or candle[
                    field] <= 0:
                logger.error(
                    f"[VWAP ERROR] {symbol} - Invalid candle {i}: {field}={candle.get(field, 'missing')}"
                )
                return None

        # Validate price relationships (high >= low >= 0, close between high/low)
        if not (candle['high'] >= candle['low'] > 0):
            logger.error(
                f"[VWAP ERROR] {symbol} - Invalid price relationship in candle {i}: high={candle['high']}, low={candle['low']}"
            )
            return None
        if not (candle['low'] <= candle['close'] <= candle['high']):
            logger.error(
                f"[VWAP ERROR] {symbol} - Close price outside high/low range in candle {i}: close={candle['close']}, high={candle['high']}, low={candle['low']}"
            )
            return None

    vwap_val = vwap_candles_numpy(vwap_candles[symbol])

    # 🚨 FINAL VALIDATION: Ensure VWAP result is reasonable
    if vwap_val <= 0 or vwap_val > 10000:  # Sanity check - no stock should be > $10,000
        logger.error(
            f"[VWAP ERROR] {symbol} - Unreasonable VWAP value: {vwap_val}")
        return None

    # Verify VWAP is within reasonable range of current prices
    recent_prices = [candle['close'] for candle in candles[-3:]]
    min_recent = min(recent_prices)
    max_recent = max(recent_prices)
    if not (min_recent * 0.5 <= vwap_val <= max_recent *
            2.0):  # VWAP should be within 50%-200% of recent prices
        logger.warning(
            f"[VWAP WARNING] {symbol} - VWAP {vwap_val:.4f} seems out of range compared to recent prices {min_recent:.4f}-{max_recent:.4f}"
        )

    logger.info(
        f"[VWAP VALIDATED] {symbol} - VWAP={vwap_val:.4f} from {len(candles)} candles"
    )
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
    sma = np.convolve(prices, np.ones(period) / period, mode='valid')
    std = np.array(
        [np.std(prices[i - period:i]) for i in range(period,
                                                     len(prices) + 1)])
    upper_band = sma + num_std * std
    lower_band = sma - num_std * std
    pad = [None] * (len(prices) - len(lower_band))
    lower_band = pad + list(lower_band)
    upper_band = pad + list(upper_band)
    sma = pad + list(sma)
    return lower_band, sma, upper_band


def polygon_time_to_utc(ts):
    return datetime.utcfromtimestamp(ts / 1000).replace(tzinfo=timezone.utc)


logging.basicConfig(level=logging.INFO,
                    format='[%(asctime)s] %(levelname)s:%(name)s:%(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger("scanner")

try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    logger.warning(
        "yfinance not installed. Run 'pip install yfinance' for float filtering."
    )
    YFINANCE_AVAILABLE = False

logger.info(
    "scanner.py is running!!! --- If you see this, your file is found and started."
)
logger.info("Imports completed successfully.")

POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# --- DISCORD WEBHOOK ---
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL")

if not POLYGON_API_KEY:
    logger.critical(
        "POLYGON_API_KEY environment variable is required! Set it and restart."
    )
    sys.exit(1)
if not TELEGRAM_BOT_TOKEN:
    logger.critical(
        "TELEGRAM_BOT_TOKEN environment variable is required! Set it and restart."
    )
    sys.exit(1)
if not TELEGRAM_CHAT_ID:
    logger.critical(
        "TELEGRAM_CHAT_ID environment variable is required! Set it and restart."
    )
    sys.exit(1)

PRICE_THRESHOLD = 15.00  # INCREASED: Allow more headroom for momentum moves
MAX_SYMBOLS = 4000
SCREENER_REFRESH_SEC = 30
MIN_ALERT_MOVE = 0.12
MIN_3MIN_VOLUME = 25000
MIN_PER_CANDLE_VOL = 25000
MIN_IPO_DAYS = 30
ALERT_PRICE_DELTA = 0.25
RVOL_SPIKE_THRESHOLD = 1.5  # REDUCED: Lower RVOL for early detection
RVOL_SPIKE_MIN_VOLUME = 25000  # REDUCED: Lower volume for early spikes

MIN_FLOAT_SHARES = 500_000
MAX_FLOAT_SHARES = 20_000_000  # UPDATED: Increased from 10M to 20M

# 🚨 ADAPTIVE VOLUME THRESHOLDS FOR MICRO-FLOAT STOCKS
def get_min_volume_for_float(float_shares):
    """Return adaptive volume threshold based on float size - catches ULY-type micro-float runners"""
    if float_shares is None:
        return 25_000  # Default for unknown float
    elif float_shares < 3_000_000:  # Micro-float (like ULY at 1.25M)
        return 7_500  # 🚨 MUCH LOWER for micro-floats - catches 8-18k/min spikes
    else:  # Regular float (3M-20M)
        return 25_000  # Standard threshold

# Exception list for hot stocks that bypass float restrictions
FLOAT_EXCEPTION_SYMBOLS = {
    "OCTO", "GME", "AMC", "BBBY", "DWAC"
}  # Add high-volume stocks that may be outside normal float range

vwap_cum_vol = defaultdict(float)
vwap_cum_pv = defaultdict(float)
rvol_history = defaultdict(lambda: deque(maxlen=20))
RVOL_MIN = 2.0

EVENT_LOG_FILE = "event_log.csv"
OUTCOME_LOG_FILE = "outcome_log.csv"

# --- AUTO ML TRAINING SYSTEM ---
recent_alerts = {}  # symbol -> alert_time
alert_prices = {}  # symbol -> entry_price
alert_types = {}  # symbol -> alert_type
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
            minutes_since_open = (ny_time.hour - 9) * 60 + (ny_time.minute -
                                                            30)
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
    logger.info(
        f"[OUTCOME TRACKING] Started tracking {symbol} {alert_type} at ${price:.2f}"
    )


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
        success_threshold = 0.05  # 5% in 1 hour+

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

                    outcome = auto_label_success(symbol, entry_price,
                                                 current_price, check_minutes)

                    # Log the outcome
                    log_outcome(symbol, alert_type, entry_price, current_price,
                                alert_time, check_minutes, outcome)

                    logger.info(
                        f"[OUTCOME] {symbol} {alert_type} @ {check_minutes}min: "
                        f"{outcome['return_pct']*100:.1f}% {'SUCCESS' if outcome['is_successful'] else 'FAIL'}"
                    )

        # Remove alerts older than 2 hours
        if minutes_elapsed > 120:
            completed_alerts.append(symbol)

    # Clean up completed alerts
    for symbol in completed_alerts:
        recent_alerts.pop(symbol, None)
        alert_prices.pop(symbol, None)
        alert_types.pop(symbol, None)


def log_outcome(symbol, alert_type, entry_price, current_price, alert_time,
                minutes_elapsed, outcome):
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
    write_header = not os.path.exists(OUTCOME_LOG_FILE) or os.path.getsize(
        OUTCOME_LOG_FILE) == 0

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
        merged_df = pd.merge(event_df,
                             outcome_df,
                             left_on=['symbol', 'event_type'],
                             right_on=['symbol', 'alert_type'],
                             how='inner')

        if len(merged_df) < 30:
            return

        # Prepare features for training
        feature_columns = [
            'price', 'volume', 'rvol', 'minutes_since_open', 'day_of_week',
            'is_power_hour', 'is_opening_hour'
        ]

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
            new_model = RandomForestClassifier(n_estimators=100,
                                               random_state=42)
            new_model.fit(X, y)

            # Save updated model
            joblib.dump(new_model, "runner_model_updated.joblib")

            # Update global model
            global runner_clf
            runner_clf = new_model

            logger.info(
                f"[ML UPDATE] Model retrained with {len(X)} samples, "
                f"{y.sum()} successes ({y.mean()*100:.1f}% success rate)")

            # Archive old outcome data to prevent memory issues
            if len(outcome_df) > 1000:
                archive_file = f"outcome_archive_{datetime.now().strftime('%Y%m%d')}.csv"
                outcome_df.iloc[:-200].to_csv(archive_file, index=False)
                outcome_df.iloc[-200:].to_csv(OUTCOME_LOG_FILE, index=False)
                logger.info(
                    f"[ML UPDATE] Archived old outcomes to {archive_file}")

    except Exception as e:
        logger.error(f"[ML UPDATE] Error retraining model: {e}")


def log_event(event_type,
              symbol,
              price,
              volume,
              event_time,
              extra_features=None):
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
    alert_types_list = [
        "perfect_setup", "runner", "volume_spike", "ema_stack", "warming_up",
        "dip_play"
    ]
    if event_type in alert_types_list:
        track_alert_for_outcome(symbol, event_type, price)

    header = list(row.keys())
    write_header = not os.path.exists(EVENT_LOG_FILE) or os.path.getsize(
        EVENT_LOG_FILE) == 0
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
    X = np.array([[
        price, volume if volume is not None else 0,
        rvol if rvol is not None else 1.0, prepost_f, event_type_code
    ]])
    prob = runner_clf.predict_proba(X)[0, 1]
    return prob


KEYWORDS = [
    "offering", "FDA", "approval", "acquisition", "merger", "bankruptcy",
    "delisting", "reverse split", "split", "halt", "investigation", "lawsuit",
    "earnings", "guidance", "clinical", "phase 1", "phase 2", "phase 3",
    "partnership", "contract", "dividend", "buyback", "sec", "subpoena",
    "settlement", "short squeeze", "recall", "resigns", "appoints",
    "collaboration", "sec filing", "patent", "discontinued", "withdraw",
    "spike", "upsize", "pricing", "withdraws", "grants", "fires", "director",
    "ceo", "cfo"
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
        timeout = aiohttp.ClientTimeout(total=10)
        async with http_session.post(url, data=payload,
                                timeout=timeout) as resp:
            result_text = await resp.text()
            logger.debug(
                f"[DEBUG] Telegram API status: {resp.status}, response: {result_text}"
            )
            if resp.status != 200:
                logger.error(f"[DEBUG] Telegram send error: {result_text}")
    except Exception as e:
        logger.error(f"[DEBUG] Telegram send error: {e}")


async def send_discord_async(message):
    if not DISCORD_WEBHOOK_URL:
        logger.debug(
            "No Discord webhook URL configured, skipping Discord notification")
        return
    payload = {"content": message}
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with http_session.post(DISCORD_WEBHOOK_URL,
                                json=payload,
                                timeout=timeout) as resp:
            if resp.status in (200, 204):
                logger.debug("Discord alert sent successfully")
            else:
                result = await resp.text()
                logger.warning(
                    f"Discord send failed: HTTP {resp.status} - {result}")
    except Exception as e:
        logger.warning(f"Discord send error: {e}")
        # Don't let Discord failures crash the scanner


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

last_volume_spike_time = defaultdict(
    lambda: datetime.min.replace(tzinfo=timezone.utc))
last_runner_alert_time = defaultdict(
    lambda: datetime.min.replace(tzinfo=timezone.utc))
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

ALERT_COOLDOWN_MINUTES = 1  # 🚨 REDUCED to 1 minute - catch rapid momentum shifts
# 🚨 NEW: Track last alert time PER ALERT TYPE (not just per symbol)
last_alert_time = defaultdict(
    lambda: defaultdict(lambda: datetime.min.replace(tzinfo=timezone.utc)))

# --- ALERT PRIORITIZATION SYSTEM ---
ALERT_PRIORITIES = {
    "perfect_setup": 95,  # Highest priority
    "ema_stack": 90,  # High priority
    "runner": 85,  # Good priority
    "volume_spike": 80,  # Medium priority
    "dip_play": 75,  # Lower priority
    "warming_up": 70  # Lowest priority
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
        logger.info(
            f"[🚨 OCTO DEBUG] send_best_alert called, pending alerts: {len(pending_alerts.get(symbol, []))}"
        )

    if symbol not in pending_alerts or not pending_alerts[symbol]:
        if symbol == "OCTO":
            logger.info(f"[🚨 OCTO DEBUG] No pending alerts for {symbol}")
        return

    # Find highest scoring alert
    best_alert = max(pending_alerts[symbol], key=lambda x: x['score'])

    if symbol == "OCTO":
        logger.info(
            f"[🚨 OCTO DEBUG] Best alert: {best_alert['type']} | Score: {best_alert['score']}"
        )

    # Only send if score is high enough (LOWERED for faster alerts)
    if best_alert['score'] >= 20:  # 🚨 LOWERED from 30 to 20 - catch more valid setups
        await send_all_alerts(best_alert['message'])
        logger.info(
            f"[ALERT SENT] {symbol} | {best_alert['type']} | Score: {best_alert['score']}"
        )
    else:
        logger.info(
            f"[ALERT SKIPPED] {symbol} | Best score: {best_alert['score']} (threshold: 20)"
        )

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


def check_volume_spike(candles_seq, vwap_value, float_shares=None):
    """Improved volume spike detection with better RVOL calculation and price momentum
    
    Args:
        float_shares: Float size for adaptive thresholds - catches micro-float spikes like ULY
    """
    if len(candles_seq) < 4:  # REDUCED: Only need 4 candles to detect early spikes
        return False, {}

    curr_candle = list(candles_seq)[-1]
    prev_candle = list(candles_seq)[-2]
    curr_volume = curr_candle['volume']
    symbol = curr_candle.get('symbol', '?')

    # ADAPTIVE: Use 3 trailing candles for early detection (4 total needed)
    trailing_volumes = [c['volume'] for c in list(candles_seq)[-4:-1]]
    trailing_avg = sum(trailing_volumes) / 3 if len(
        trailing_volumes) == 3 else 1
    rvol = curr_volume / trailing_avg if trailing_avg > 0 else 0
    # 🚨 CRITICAL FIX: Use real-time price for VWAP comparison
    current_price_spike = last_trade_price.get(symbol)
    above_vwap = current_price_spike is not None and current_price_spike > vwap_value

    # 🚨 ADAPTIVE volume threshold - catches ULY-type micro-float spikes (8-18k shares/min)
    min_volume = get_min_volume_for_float(float_shares)  # 7.5k for micro-floats, 25k for regular

    # 🔥 SIMPLIFIED: Volume spike must come with RISING price
    price_momentum = (curr_candle['close'] - curr_candle['open']
                      ) / curr_candle['open'] if curr_candle['open'] > 0 else 0
    prev_momentum = (curr_candle['close'] - prev_candle['close']
                     ) / prev_candle['close'] if prev_candle['close'] > 0 else 0
    
    # Green candle with meaningful move OR rising from previous with meaningful move
    is_green_candle = curr_candle['close'] > curr_candle['open'] and price_momentum >= 0.005  # 0.5%+ for early detection
    rising_from_prev = curr_candle['close'] > prev_candle['close'] and prev_momentum >= 0.003  # 0.3%+ from previous
    
    # Price must be moving UP (green candle OR rising from previous)
    bullish_momentum = is_green_candle or rising_from_prev

    symbol = curr_candle.get('symbol', '?')

    logger.info(
        f"[VOLUME SPIKE CHECK] {symbol} | "
        f"Vol={curr_volume}, RVOL={rvol:.2f}, "
        f"Green={is_green_candle} ({price_momentum*100:.1f}%), "
        f"Rising={rising_from_prev}, Momentum={bullish_momentum}, "
        f"Above VWAP={above_vwap}")

    # Check each condition
    vol_ok = curr_volume >= min_volume
    rvol_ok = rvol >= RVOL_SPIKE_THRESHOLD  # 1.5x threshold for early detection
    vwap_ok = above_vwap
    momentum_ok = bullish_momentum

    spike_detected = vol_ok and rvol_ok and vwap_ok and momentum_ok

    # SPECIAL LOG: Rejected for being below VWAP
    if not vwap_ok and (vol_ok
                        or rvol_ok):  # Would have triggered except for VWAP
        logger.info(
            f"[❌ VOLUME SPIKE REJECTED] {curr_candle.get('symbol', '?')} | BELOW VWAP | Close={curr_candle['close']:.4f} < VWAP={vwap_value:.4f}"
        )

    # Log detailed debug info
    logger.info(
        f"[SPIKE DEBUG] {curr_candle.get('symbol', '?')} | "
        f"vol_ok={vol_ok} ({curr_volume}>={min_volume}), "
        f"rvol_ok={rvol_ok} ({rvol:.2f}>={RVOL_SPIKE_THRESHOLD}), "
        f"vwap_ok={vwap_ok}, momentum_ok={momentum_ok}, "
        f"FINAL: spike_detected={spike_detected}")

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


def get_display_price(symbol, fallback, fallback_time=None, max_age_seconds=MAX_PRICE_AGE_SECONDS):
    """Get freshest available price - compares real-time trade vs candle close timestamps
    Returns None if all data is stale (>max_age_seconds) to prevent false alerts"""
    trade_price = last_trade_price.get(symbol)
    trade_time = last_trade_time.get(symbol)
    now = datetime.now(timezone.utc)
    
    # If we have a real-time trade price with timestamp
    if trade_price is not None and trade_time is not None:
        trade_age = (now - trade_time).total_seconds()
        
        # If fallback has a timestamp, compare which is fresher
        if fallback_time is not None:
            fallback_age = (now - fallback_time).total_seconds()
            # Use whichever is fresher and within max age
            if trade_age < fallback_age and trade_age < max_age_seconds:
                return trade_price
            elif fallback_age < max_age_seconds:
                return fallback
            # Both stale - return None to block alerts
            return None
        
        # No fallback timestamp - use trade if fresh enough
        if trade_age < max_age_seconds:
            return trade_price
        # Trade is stale and no fallback timestamp - return None
        return None
    
    # No valid real-time data - use fallback only if we have its timestamp and it's fresh
    if fallback_time is not None:
        fallback_age = (now - fallback_time).total_seconds()
        if fallback_age < max_age_seconds:
            return fallback
        # Fallback is stale - return None
        return None
    
    # No timestamp info - return fallback as last resort (for compatibility)
    return fallback


def is_bullish_engulfing(candles_seq):
    if len(candles_seq) < 2:
        return False
    prev = candles_seq[-2]
    curr = candles_seq[-1]
    return ((prev["close"] < prev["open"]) and (curr["close"] > curr["open"])
            and (curr["close"] > prev["open"])
            and (curr["open"] < prev["close"]))


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
            candles, vwap_candles, last_trade_price, last_trade_volume,
            last_trade_time, recent_high, last_alert_time, warming_up_was_true,
            runner_was_true, dip_play_was_true, vwap_reclaim_was_true,
            volume_spike_was_true, ema_stack_was_true, premarket_open_prices,
            premarket_last_prices, premarket_volumes, alerted_symbols,
            runner_alerted_today, below_vwap_streak, vwap_reclaimed_once,
            dip_play_seen, volume_spike_alerted, rvol_spike_alerted,
            halted_symbols, pending_runner_alert
    ]:
        if hasattr(d, "clear"):
            d.clear()
    # EMAs are NOT reset here - they persist throughout the trading session and only reset at 04:00 ET
    logger.info("Cleared all per-symbol session state for new trading day!")


async def alert_perfect_setup(symbol, closes, volumes, highs, lows,
                              candles_seq, vwap_value):
    # Debug prints for ALL alert processing - MAIN LOOP
    print(f"[MAIN LOOP DEBUG] Processing symbol: {symbol}")
    if candles_seq and len(candles_seq) >= 6:
        last_6 = list(candles_seq)[-6:]
        volumes_5 = [c['volume'] for c in last_6[:-1]]
        avg_vol_5 = sum(volumes_5) / 5 if volumes_5 else 0
        last_candle = last_6[-1]
        open_rn = last_candle['open']
        close_rn = last_candle['close']
        high_rn = last_candle['high']
        volume_rn = last_candle['volume']
        price_move_rn = (close_rn - open_rn) / open_rn if open_rn > 0 else 0

        # Volume increasing check
        volume_increasing = len(last_6) >= 3 and all(
            last_6[i]['volume'] <= last_6[i + 1]['volume']
            for i in range(-3, -1))

        # Price trend check
        closes_for_trend = [c['close'] for c in last_6[-3:]]
        price_rising_trend = all(
            x < y for x, y in zip(closes_for_trend, closes_for_trend[1:]))

        print(
            f"{symbol} Runner Volume: {volume_rn}, AvgVol5: {avg_vol_5}, VolPass: {volume_rn >= 2.0 * avg_vol_5}"
        )
        print(
            f"{symbol} PriceMove: {price_move_rn}, MovePass: {price_move_rn >= 0.03}"
        )
        print(
            f"{symbol} Close: {close_rn}, High: {high_rn}, WickPass: {close_rn >= 0.65 * high_rn}"
        )
        print(f"{symbol} VolumeIncreasing: {volume_increasing}")
        print(f"{symbol} PriceTrend: {price_rising_trend}")

        if not (volume_rn >= 2.0 * avg_vol_5):
            print(f"{symbol}: Failed volume runner")
        if not (price_move_rn >= 0.03):
            print(f"{symbol}: Failed price move")
        if not (close_rn >= 0.65 * high_rn):
            print(f"{symbol}: Failed wick")
        if not volume_increasing:
            print(f"{symbol}: Failed volume increasing")
        if not price_rising_trend:
            print(f"{symbol}: Failed price trend")

    closes_np = np.array(closes)
    volumes_np = np.array(volumes)
    highs_np = np.array(highs)
    lows_np = np.array(lows)

    # Use stored EMAs for Perfect Setup
    emas = get_stored_emas(symbol, [5, 8, 13])
    if emas['ema5'] is None or emas['ema8'] is None or emas['ema13'] is None:
        logger.info(
            f"[PERFECT SETUP] {symbol}: EMAs not initialized, skipping")
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

    logger.info(
        f"[ALERT DEBUG] {symbol} | Perfect Setup Check | EMA5={ema5}, EMA8={ema8}, EMA13={ema13}, VWAP={vwap_value}, Last Close={last_close}, Last Volume={last_volume}, RVOL={rvol}, RSI={last_rsi}, MACD Hist={last_macd_hist}, Bullish Engulf={bullish_engulf}"
    )
    logger.info(
        f"[DEBUG] {symbol} | Session candles: {len(candles_seq)} | Candle times: {candles_seq[0]['start_time'] if candles_seq else 'n/a'} - {candles_seq[-1]['start_time'] if candles_seq else 'n/a'}"
    )
    logger.info(f"[DEBUG] {symbol} | All candle closes: {closes}")
    logger.info(f"[DEBUG] {symbol} | All candle volumes: {volumes}")

    # 🚨 CRITICAL FIX: Use real-time price for VWAP comparison
    current_price_perfect = last_trade_price.get(symbol)
    perfect = ((ema5 > ema8 > ema13) and (ema5 >= 1.011 * ema13) and
               (current_price_perfect is not None
                and current_price_perfect > vwap_value)  # 🚨 REAL-TIME PRICE!
               and (last_volume >= 100000) and (rvol > 2.0) and (last_rsi < 70)
               and (last_macd_hist > 0) and bullish_engulf)

    # ---- PATCH: enforce ratio at alert time! ----
    if perfect:
        if ema5 / ema13 < 1.011:
            logger.error(
                f"[BUG] Perfect Setup alert would have fired but ratio invalid! {symbol}: ema5={ema5:.4f}, ema13={ema13:.4f}, ratio={ema5/ema13:.4f} (should be >= 1.011)"
            )
            return
        if last_trade_volume[symbol] < 500:
            logger.info(
                f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})"
            )
            return
        
        # 🚨 FIX: Use FRESHEST available price (real-time trade vs candle close)
        candle_time_ps = candles_seq[-1]['start_time'] + timedelta(minutes=1)
        alert_price = get_display_price(symbol, last_close, candle_time_ps)
        alert_text = (
            f"🚨 <b>PERFECT SETUP</b> 🚨\n"
            f"<b>{escape_html(symbol)}</b> | ${alert_price:.2f} | Vol: {int(last_volume/1000)}K | RVOL: {rvol:.1f}\n\n"
            f"Trend: EMA5 > EMA8 > EMA13\n"
            f"{'Above VWAP' if last_close > vwap_value else 'Below VWAP'}"
            f" | MACD↑"
            f" | RSI: {int(round(last_rsi))}")
        # Calculate alert score and add to pending alerts
        alert_data = {
            'rvol':
            rvol,
            'volume':
            last_volume,
            'price_move': (last_close - candles_seq[-1]['open']) /
            candles_seq[-1]['open'] if candles_seq[-1]['open'] > 0 else 0
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
            "perfect_setup", symbol, alert_price,
            last_volume, now, {
                "ema5": ema5,
                "ema8": ema8,
                "ema13": ema13,
                "vwap": vwap_value,
                "rvol": rvol,
                "rsi14": last_rsi,
                "macd_hist": last_macd_hist,
                "bullish_engulfing": bullish_engulf,
                "alert_score": score
            })
        ema_stack_was_true[symbol] = True


# --- PERFECT SETUP LOGIC END ---

# --- PREMARKET GAINERS TRACKING (no float filter, tracks ALL symbols) ---
premarket_open_prices = {}  # symbol -> 4am price
premarket_last_prices = {}  # symbol -> latest price up to 9:25am
premarket_volumes = {}  # symbol -> cumulative volume since 4am


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
    if (now_et.hour == 4 and now_et.minute == 0
            and current_session_date == today_ny
            and last_ema_reset_date != today_ny):
        stored_emas.clear()
        last_ema_reset_date = today_ny
        print(
            f"[DEBUG] Reset stored EMAs for new trading session at 04:00 ET: {today_ny}"
        )

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
    float_shares = await get_float_shares(symbol)
    # Check exception list first, then float range
    if symbol not in FLOAT_EXCEPTION_SYMBOLS and (
            float_shares is None
            or not (MIN_FLOAT_SHARES <= float_shares <= MAX_FLOAT_SHARES)):
        if symbol == "OCTO":
            logger.info(
                f"[🚨 OCTO DEBUG] FILTERED OUT due to float {float_shares} (range: {MIN_FLOAT_SHARES}-{MAX_FLOAT_SHARES})"
            )
        logger.debug(f"Skipping {symbol} due to float {float_shares}")
        return

    # Log exception processing
    if symbol in FLOAT_EXCEPTION_SYMBOLS:
        logger.info(
            f"[FLOAT EXCEPTION] {symbol} bypassing float filter (actual: {float_shares})"
        )

    # OCTO-specific debug for successful processing
    if symbol == "OCTO":
        logger.info(
            f"[🚨 OCTO DEBUG] PASSED float filter ({float_shares}), processing candle: {open_}/{high}/{low}/{close}/{volume}"
        )
    # DEBUG: Show why symbols might be filtered
    if symbol in ["OCTO", "GRND", "EQS", "OSRH", "BJDX", "EBMT"]:
        logger.info(
            f"[🔥 FILTER DEBUG] {symbol} | market_scan_time={is_market_scan_time()} | price={close} | price_ok={close <= 20.00}"
        )

    if not is_market_scan_time(
    ) or close > 25.00:  # INCREASED: Higher ceiling for momentum moves
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
        logger.info(
            f"[EMA BACKFILL] {symbol} | First candle encountered, backfilling EMAs..."
        )
        backfill_success = await backfill_stored_emas(symbol)
        if not backfill_success:
            logger.warning(
                f"[EMA BACKFILL] {symbol} | Backfill failed, using current close price"
            )

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

    # Removed verbose debug logging for performance - only log critical events

    # --- PERFECT SETUP SCANNER ---
    if len(candles_seq) >= 30:
        closes = [c['close'] for c in list(candles_seq)[-30:]]
        highs = [c['high'] for c in list(candles_seq)[-30:]]
        lows = [c['low'] for c in list(candles_seq)[-30:]]
        volumes = [c['volume'] for c in list(candles_seq)[-30:]]
        # 🚨 CRITICAL FIX: Use centralized VWAP guard for perfect setup
        vwap_value = get_valid_vwap(symbol)
        if vwap_value is None:
            logger.info(
                f"[VWAP GUARD] {symbol} - Blocking perfect setup alert - insufficient VWAP data"
            )
            return  # Block perfect setup alerts without valid VWAP
        if not ema_stack_was_true[symbol]:
            await alert_perfect_setup(symbol, closes, volumes, highs, lows,
                                      list(candles_seq)[-30:], vwap_value)

    # --- Warming Up Logic with STRICT MOMENTUM REQUIREMENTS ---
    if len(candles_seq) >= 3:
        available = min(6, len(candles_seq))
        last_candles = list(candles_seq)[-available:]
        volumes_prev = [c["volume"] for c in last_candles[:-1]]
        avg_vol_5 = sum(volumes_prev) / len(volumes_prev) if volumes_prev else 1
        last_candle = last_candles[-1]
        open_wu = last_candle['open']
        close_wu = last_candle['close']
        volume_wu = last_candle['volume']
        # 🚨 CRITICAL: Get real-time price for criteria evaluation
        current_price_wu = last_trade_price.get(symbol)  # Real-time market price
        price_move_wu = (close_wu - open_wu) / open_wu if open_wu > 0 else 0
        # 🚨 CRITICAL FIX: Use centralized VWAP guard for warming up alerts
        vwap_wu = get_valid_vwap(symbol)
        if vwap_wu is None:
            logger.info(
                f"[VWAP GUARD] {symbol} - Blocking warming up alert - insufficient VWAP data"
            )
            return  # Block warming up alerts without valid VWAP
        dollar_volume_wu = close_wu * volume_wu
        
        # 🚀 EARLY RUNNER DETECTION - CATCH BEFORE THEY TAKE OFF!
        closes_wu = [c['close'] for c in last_candles[-3:]]
        volumes_wu = [c['volume'] for c in last_candles[-3:]]
        
        # Simple early momentum check (not too strict)
        has_green_candle = close_wu > open_wu
        rising_from_prev = len(closes_wu) >= 2 and close_wu > closes_wu[-2]
        early_momentum = has_green_candle or rising_from_prev  # Either condition
        
        # WARMING UP = Early runner detection (low thresholds)
        min_vol_wu = get_min_volume_for_float(float_shares)  # 🚨 Adaptive: 7.5k for micro-floats, 25k for regular
        warming_up_criteria = (
            volume_wu >= 1.5 * avg_vol_5
            and  # EARLY: Just 1.5x volume spike
            volume_wu >= min_vol_wu
            and  # 🚨 ADAPTIVE minimum volume (catches ULY 8-18k spikes!)
            price_move_wu >= 0.02
            and  # EARLY: Just 2% move to catch before it runs!
            price_move_wu > 0
            and  # MUST BE POSITIVE
            early_momentum
            and  # Green candle OR rising from previous
            0.20 <= close_wu <= 25.00
            and
            current_price_wu is not None and current_price_wu > vwap_wu
            and  # Above VWAP
            dollar_volume_wu >= 50_000
            and  # Lower dollar volume for early detection
            avg_vol_5 > 5_000
        )
        # 🚨 WARMING UP DEBUG
        logger.info(
            f"[WARMING UP DEBUG] {symbol} | "
            f"Volume={volume_wu} (avg5={avg_vol_5:.0f}, req={1.5 * avg_vol_5:.0f}), "
            f"PriceMove={price_move_wu*100:.2f}% (req=2%+), "
            f"Green={has_green_candle}, Rising={rising_from_prev}, Momentum={early_momentum}, "
            f"Real-time=${current_price_wu or 0:.3f}, VWAP=${vwap_wu:.3f}, "
            f"DollarVol=${dollar_volume_wu:.0f}")
        # Use stored EMAs
        emas = get_stored_emas(symbol, [5, 8, 13])
        logger.info(
            f"[EMA DEBUG] {symbol} | Warming Up | EMA5={emas.get('ema5', 'N/A')}, EMA8={emas.get('ema8', 'N/A')}, EMA13={emas.get('ema13', 'N/A')}"
        )
        # 🚨 EXTRA SAFETY CHECK - Log when criteria would fire
        if (volume_wu >= 2.0 * avg_vol_5 and price_move_wu >= 0.03
                and 0.20 <= close_wu <= 25.00 and current_price_wu is not None
                and current_price_wu > vwap_wu
                and dollar_volume_wu >= 50_000):  # 🚨 REAL-TIME PRICE!
            logger.warning(
                f"[⚠️ OLD CRITERIA] {symbol} would have fired under old criteria! "
                f"Vol={volume_wu}, Move={price_move_wu*100:.1f}%, Close=${close_wu:.3f}, VWAP=${vwap_wu:.3f}"
            )

        if warming_up_criteria and not warming_up_was_true[symbol]:
            if last_trade_volume[symbol] < 500:
                logger.info(
                    f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})"
                )
                return
            if (now - last_alert_time[symbol]['warming_up']) < timedelta(
                    minutes=ALERT_COOLDOWN_MINUTES):
                return
            
            # 🚨 REAL-TIME PRICE CONFIRMATION: Require FRESH price above VWAP
            real_time_price_wu = last_trade_price.get(symbol)
            real_time_timestamp_wu = last_trade_time.get(symbol)
            
            # Check if we have real-time price data (explicit None check to catch $0.00)
            if real_time_price_wu is None or real_time_timestamp_wu is None:
                logger.info(f"[WARMING UP BLOCKED] {symbol} - No real-time price available for confirmation")
                return
            
            # Recompute NOW to ensure freshness check reflects actual alert moment
            now_fresh_wu = datetime.now(timezone.utc)
            
            # Verify price data is FRESH (≤5 seconds old)
            price_age_wu = (now_fresh_wu - real_time_timestamp_wu).total_seconds()
            if price_age_wu > 5:
                logger.info(f"[WARMING UP BLOCKED] {symbol} - Real-time price is stale ({price_age_wu:.1f}s old)")
                return
            
            # Verify real-time price is ACTUALLY above VWAP (not just from criteria check)
            if real_time_price_wu <= vwap_wu:
                logger.info(f"[WARMING UP BLOCKED] {symbol} - Real-time price ${real_time_price_wu:.2f} NOT above VWAP ${vwap_wu:.2f}")
                return
            
            # 🚨 FIX: Use FRESHEST available price (real-time trade vs candle close)
            candle_time_wu = list(candles_seq)[-1]['start_time'] + timedelta(minutes=1)
            alert_price = get_display_price(symbol, close_wu, candle_time_wu)
            log_event(
                "warming_up",
                symbol,
                alert_price,
                volume_wu,
                event_time,
                {
                    "price_move": price_move_wu,
                    "dollar_volume": dollar_volume_wu,
                    "candle_price": close_wu,
                    "real_time_price": last_trade_price.get(symbol)
                })
            price_str = f"{alert_price:.2f}"
            alert_text = (f"🌡️ <b>{escape_html(symbol)}</b> Warming Up\n"
                          f"Current Price: ${price_str}")
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
            last_alert_time[symbol]['warming_up'] = now

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
        # 🚨 CRITICAL FIX: Never allow alerts without valid VWAP data
        if not vwap_candles[symbol] or len(vwap_candles[symbol]) < 3:
            logger.info(
                f"[VWAP PROTECTION] {symbol} - Insufficient VWAP data, blocking runner alert"
            )
            return  # Block all alerts if no VWAP data
        vwap_rn = vwap_candles_numpy(vwap_candles[symbol])

        # 🔥 EARLY DETECTION: Check for momentum trend (2 out of 3 rising)
        closes_for_trend = [c['close'] for c in last_6[-3:]]
        volumes_for_trend = [c['volume'] for c in last_6[-3:]]
        
        # Count rising closes (2 out of 3 is good enough for early detection)
        rising_closes = sum(1 for i in range(len(closes_for_trend)-1) 
                           if closes_for_trend[i+1] > closes_for_trend[i])
        price_rising_trend = rising_closes >= 2  # At least 2 out of 3 rising
        
        # Volume should be increasing (at least current > average of previous 2)
        volume_rising_trend = volume_rn > sum(volumes_for_trend[:2]) / 2

        # 🚨 CRITICAL: Get real-time price for criteria evaluation
        current_price_rn = last_trade_price.get(symbol)  # Real-time market price

        logger.info(
            f"[RUNNER DEBUG] {symbol} | Closes: {closes_for_trend} | Rising closes: {rising_closes}/2 | Price trend: {price_rising_trend} | Volume trend: {volume_rising_trend}"
        )
        logger.info(
            f"[ALERT DEBUG] {symbol} | Alert Type: runner | VWAP={vwap_rn:.4f} | Real-time Price={current_price_rn} | Candle Close={close_rn} | Above VWAP={current_price_rn is not None and current_price_rn > vwap_rn} | Volume={volume_rn}"
        )
        # Use stored EMAs for Runner Detection
        emas = get_stored_emas(symbol, [5, 8, 13])
        logger.info(
            f"[STORED EMA] {symbol} | Runner | EMA5={emas.get('ema5', 'N/A')}, EMA8={emas.get('ema8', 'N/A')}, EMA13={emas.get('ema13', 'N/A')}"
        )

        # 🏃 RUNNER = Stocks ALREADY MOVING UP with volume and price
        min_vol_rn = get_min_volume_for_float(float_shares)  # 🚨 Adaptive: 7.5k for micro-floats, 25k for regular
        runner_criteria = (
            volume_rn >= 2.5 * avg_vol_5
            and  # STRONG: 2.5x volume - stock is moving!
            price_move_rn >= 0.05
            and  # CONFIRMED: 5% move - established momentum!
            current_price_rn is not None and current_price_rn >= 0.10
            and  # Use real-time price
            current_price_rn is not None and current_price_rn > vwap_rn
            and  # Above VWAP
            price_rising_trend
            and  # At least 2 out of 3 candles rising
            volume_rising_trend
            and  # Volume increasing
            volume_rn >= min_vol_rn  # 🚨 ADAPTIVE minimum (catches ULY 8-18k spikes!)
        )

        if runner_criteria and not runner_was_true[symbol]:
            if last_trade_volume[symbol] < 500:
                logger.info(
                    f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})"
                )
                return
            if (now - last_alert_time[symbol]['runner']) < timedelta(
                    minutes=ALERT_COOLDOWN_MINUTES):
                return
            
            # 🚨 REAL-TIME PRICE CONFIRMATION: Require FRESH price above VWAP
            real_time_price_rn = last_trade_price.get(symbol)
            real_time_timestamp_rn = last_trade_time.get(symbol)
            
            # Check if we have real-time price data (explicit None check to catch $0.00)
            if real_time_price_rn is None or real_time_timestamp_rn is None:
                logger.info(f"[RUNNER BLOCKED] {symbol} - No real-time price available for confirmation")
                return
            
            # Recompute NOW to ensure freshness check reflects actual alert moment
            now_fresh_rn = datetime.now(timezone.utc)
            
            # Verify price data is FRESH (≤5 seconds old)
            price_age_rn = (now_fresh_rn - real_time_timestamp_rn).total_seconds()
            if price_age_rn > 5:
                logger.info(f"[RUNNER BLOCKED] {symbol} - Real-time price is stale ({price_age_rn:.1f}s old)")
                return
            
            # Verify real-time price is ACTUALLY above VWAP (not just from criteria check)
            if real_time_price_rn <= vwap_rn:
                logger.info(f"[RUNNER BLOCKED] {symbol} - Real-time price ${real_time_price_rn:.2f} NOT above VWAP ${vwap_rn:.2f}")
                return
            
            # Calculate alert score and add to pending alerts
            alert_data = {
                'rvol': volume_rn / avg_vol_5,
                'volume': volume_rn,
                'price_move': price_move_rn
            }
            score = get_alert_score("runner", symbol, alert_data)

            # 🚨 FIX: Use FRESHEST available price (real-time trade vs candle close)
            candle_time_rn = last_6[-1]['start_time'] + timedelta(minutes=1)
            alert_price = get_display_price(symbol, close_rn, candle_time_rn)
            log_event(
                "runner", symbol, alert_price,
                volume_rn, event_time, {
                    "price_move": price_move_rn,
                    "trend_closes": closes_for_trend,
                    "rising_closes": rising_closes,
                    "vwap": vwap_rn,
                    "candle_price": close_rn,
                    "real_time_price": last_trade_price.get(symbol),
                    "alert_score": score
                })
            price_str = f"{alert_price:.2f}"
            alert_text = (
                f"🏃‍♂️ <b>{escape_html(symbol)}</b> Runner\n"
                f"Current Price: ${price_str} (+{price_move_rn*100:.1f}%)")

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
            last_alert_time[symbol]['runner'] = now

    # DIP PLAY LOGIC
    MIN_DIP_PCT = 0.10
    DIP_LOOKBACK = 10
    if len(candles_seq) >= DIP_LOOKBACK:
        highs = [c["high"] for c in list(candles_seq)[-DIP_LOOKBACK:]]
        rhigh = max(highs)
        recent_high[symbol] = rhigh
    if recent_high[symbol] > 0:
        dip_pct = (recent_high[symbol] - close) / recent_high[symbol]
        # 🚨 CRITICAL FIX: Use centralized VWAP guard
        current_vwap = get_valid_vwap(symbol)
        if current_vwap is None:
            logger.info(
                f"[VWAP GUARD] {symbol} - Blocking dip play alert - insufficient VWAP data"
            )
            return  # Block dip play alerts without valid VWAP
        dip_play_criteria = (
            dip_pct >= MIN_DIP_PCT and close <= 20.00
            and close > current_vwap  # ✅ NOW PROPERLY PROTECTED!
        )
        if dip_play_criteria and len(candles_seq) >= 3:
            c1, c2, c3 = list(candles_seq)[-3:]
            higher_lows = c2["low"] > c1["low"] and c3["low"] > c2["low"]
            rising_volume = c2["volume"] > c1["volume"] and c3["volume"] > c2[
                "volume"]
            dip_play_criteria = dip_play_criteria and higher_lows and rising_volume
            logger.info(
                f"[DIP PLAY DEBUG] {symbol}: dip_pct={dip_pct*100:.2f}% higher_lows={higher_lows} rising_volume={rising_volume}"
            )
            logger.info(
                f"[ALERT DEBUG] {symbol} | Alert Type: dip_play | VWAP={vwap_candles_numpy(vwap_candles[symbol]):.4f} | Last Trade={last_trade_price.get(symbol)} | Candle Close={close} | Candle Volume={volume}"
            )
            # Use stored EMAs for Dip Play
            emas = get_stored_emas(symbol, [5, 8, 13])
            logger.info(
                f"[STORED EMA] {symbol} | Dip Play | EMA5={emas.get('ema5', 'N/A')}, EMA8={emas.get('ema8', 'N/A')}, EMA13={emas.get('ema13', 'N/A')}"
            )
            if dip_play_criteria and not dip_play_was_true[symbol]:
                if (now - last_alert_time[symbol]['dip_play']) < timedelta(
                        minutes=ALERT_COOLDOWN_MINUTES):
                    return
                if last_trade_volume[symbol] < 500:
                    logger.info(
                        f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})"
                    )
                    return
                
                # 🚨 REAL-TIME PRICE CONFIRMATION: Require FRESH price above VWAP
                real_time_price = last_trade_price.get(symbol)
                real_time_timestamp = last_trade_time.get(symbol)
                
                # Check if we have real-time price data (explicit None check to catch $0.00)
                if real_time_price is None or real_time_timestamp is None:
                    logger.info(f"[DIP PLAY BLOCKED] {symbol} - No real-time price available for confirmation")
                    return
                
                # Recompute NOW to ensure freshness check reflects actual alert moment
                now_fresh = datetime.now(timezone.utc)
                
                # Verify price data is FRESH (≤5 seconds old)
                price_age = (now_fresh - real_time_timestamp).total_seconds()
                if price_age > 5:
                    logger.info(f"[DIP PLAY BLOCKED] {symbol} - Real-time price is stale ({price_age:.1f}s old)")
                    return
                
                # Verify real-time price is ACTUALLY above VWAP (not just candle close)
                if real_time_price <= current_vwap:
                    logger.info(f"[DIP PLAY BLOCKED] {symbol} - Real-time price ${real_time_price:.2f} NOT above VWAP ${current_vwap:.2f}")
                    return
                
                # 🚨 FIX: Use FRESHEST available price (real-time trade vs candle close)
                candle_time_dp = c3['start_time'] + timedelta(minutes=1)
                alert_price = get_display_price(symbol, close, candle_time_dp)
                log_event(
                    "dip_play", symbol,
                    alert_price, volume, event_time, {
                                          "dip_pct": dip_pct,
                                          "candle_price": close,
                                          "real_time_price": last_trade_price.get(symbol)
                                      })
                price_str = f"{alert_price:.2f}"
                alert_text = (f"📉 <b>{escape_html(symbol)}</b> Dip Play\n"
                              f"Current Price: ${price_str}")
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
                last_alert_time[symbol]['dip_play'] = now

    # VWAP Reclaim Logic - 🚨 CRITICAL FIX: Never allow alerts without valid VWAP data
    if len(candles_seq) >= 2 and len(vwap_candles[symbol]) >= 3:
        prev_candle = list(candles_seq)[-2]
        curr_candle = list(candles_seq)[-1]
        # Require sufficient VWAP data for both calculations
        if len(vwap_candles[symbol]) < 3:
            logger.info(
                f"[VWAP PROTECTION] {symbol} - Insufficient VWAP data for reclaim, blocking alert"
            )
            return  # Block VWAP reclaim if insufficient data
        prev_vwap = vwap_candles_numpy(list(vwap_candles[symbol])[:-1])
        curr_vwap = vwap_candles_numpy(list(vwap_candles[symbol]))
        trailing_vols = [c['volume'] for c in list(candles_seq)[:-1]]
        rvol = 0
        if trailing_vols:
            avg_trailing = sum(trailing_vols[-20:]) / min(
                len(trailing_vols), 20)
            rvol = curr_candle[
                'volume'] / avg_trailing if avg_trailing > 0 else 0
        # 🚨 CRITICAL FIX: VWAP reclaim uses ONLY candle closes for crossover detection
        # Previous logic was BROKEN - mixed real-time ticks with candle data
        prev_close = prev_candle['close']
        curr_close = curr_candle['close']
        
        # Simple, clean crossover detection: previous candle below, current candle above
        prev_below_vwap = (prev_vwap is not None and prev_close < prev_vwap)
        curr_above_vwap = (curr_vwap is not None and curr_close > curr_vwap * 1.005)  # 0.5% buffer
        
        # TRUE CROSSOVER = previous closed below AND current closed above (that's it!)
        is_true_crossover = prev_below_vwap and curr_above_vwap
        
        # Require UPWARD price movement for VWAP reclaim
        candle_price_move = (
            curr_candle['close'] - curr_candle['open']
        ) / curr_candle['open'] if curr_candle['open'] > 0 else 0
        
        # 🚨 VWAP reclaim criteria: TRUE crossover + volume confirmation
        vwap_reclaim_criteria = (
            is_true_crossover  # Previous candle closed below, current closed above!
            and candle_price_move >= 0.03  # Require 3% UPWARD move in the reclaim candle
            and curr_candle['volume'] >= 50_000  # Volume threshold
            and rvol >= 1.5  # RVOL threshold
        )

        # DEBUG: Show VWAP reclaim detection logic
        if symbol in ["OCTO", "GRND", "EQS", "PALI"]:
            logger.info(
                f"[💎 VWAP RECLAIM DEBUG] {symbol} | is_true_crossover={is_true_crossover} | prev_close={prev_close:.2f} < prev_vwap={prev_vwap:.2f} = {prev_below_vwap} | curr_close={curr_close:.2f} > curr_vwap={curr_vwap:.2f} = {curr_above_vwap} | volume={curr_candle['volume']} | rvol={rvol:.2f}"
            )
        logger.info(
            f"[VWAP RECLAIM] {symbol} | Prev: close={prev_close:.2f} vwap={prev_vwap:.2f} below={prev_below_vwap} | Curr: close={curr_close:.2f} vwap={curr_vwap:.2f} above={curr_above_vwap} | CROSSOVER={is_true_crossover}"
        )
        # Use stored EMAs
        emas = get_stored_emas(symbol, [5, 8, 13])
        logger.info(
            f"[EMA DEBUG] {symbol} | VWAP Reclaim | EMA5={emas.get('ema5', 'N/A')}, EMA8={emas.get('ema8', 'N/A')}, EMA13={emas.get('ema13', 'N/A')}"
        )
        if vwap_reclaim_criteria and not vwap_reclaim_was_true[symbol]:
            if (now - last_alert_time[symbol]['vwap_reclaim']) < timedelta(
                    minutes=ALERT_COOLDOWN_MINUTES):
                return
            if last_trade_volume[symbol] < 500:
                logger.info(
                    f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})"
                )
                return
            
            # 🚨 FIX: Use FRESHEST available price (real-time trade vs candle close)
            candle_time_vr = curr_candle['start_time'] + timedelta(minutes=1)
            alert_price = get_display_price(symbol, curr_candle['close'], candle_time_vr)
            
            # 🚨 REMOVED 0.3% margin requirement - was blocking valid reclaims
            # Price just needs to be above VWAP, period - no artificial margin required
            log_event(
                "vwap_reclaim", symbol,
                alert_price,
                curr_candle['volume'], event_time, {
                    "rvol": rvol,
                    "candle_price": curr_candle['close'],
                    "real_time_price": last_trade_price.get(symbol)
                })
            price_str = f"{alert_price:.2f}"
            vwap_str = f"{curr_vwap:.2f}" if curr_vwap is not None else "?"
            vol_str = f"{curr_candle['volume']:,}"
            rvol_str = f"{rvol:.2f}"
            alert_text = (f"📈 <b>{escape_html(symbol)}</b> VWAP Reclaim!\n"
                          f"Price: ${price_str} | VWAP: ${vwap_str}")
            await send_all_alerts(alert_text)
            vwap_reclaim_was_true[symbol] = True
            alerted_symbols[symbol] = today
            last_alert_time[symbol]['vwap_reclaim'] = now

    # Volume Spike Logic PATCH - 🚨 CRITICAL FIX: Never allow alerts without valid VWAP data
    if not vwap_candles[symbol] or len(vwap_candles[symbol]) < 3:
        logger.info(
            f"[VWAP PROTECTION] {symbol} - Insufficient VWAP data, blocking volume spike alert"
        )
        return  # Block all alerts if no VWAP data
    vwap_value = vwap_candles_numpy(vwap_candles[symbol])
    logger.info(
        f"[ALERT DEBUG] {symbol} | Alert Type: volume_spike | VWAP={vwap_value:.4f} | Last Trade={last_trade_price.get(symbol)} | Candle Close={close} | Candle Volume={volume}"
    )
    # Use stored EMAs for Volume Spike
    emas = get_stored_emas(symbol, [5, 8, 13])
    logger.info(
        f"[STORED EMA] {symbol} | Volume Spike | EMA5={emas.get('ema5', 'N/A')}, EMA8={emas.get('ema8', 'N/A')}, EMA13={emas.get('ema13', 'N/A')}"
    )
    spike_detected, spike_data = check_volume_spike(candles_seq, vwap_value, float_shares)

    # DEBUG: Show why volume spike alerts might not fire
    if symbol in ["OCTO", "GRND", "EQS", "OSRH", "BJDX",
                  "EBMT"]:  # Debug key symbols
        logger.info(
            f"[🔥 VOLUME SPIKE DEBUG] {symbol} | spike_detected={spike_detected} | volume_spike_was_true={volume_spike_was_true[symbol]} | last_trade_volume={last_trade_volume[symbol]} | cooldown_ok={(now - last_alert_time[symbol]['volume_spike']).total_seconds() > ALERT_COOLDOWN_MINUTES * 60}"
        )
        if spike_detected:
            logger.info(
                f"[🔥 SPIKE DATA] {symbol} | RVOL={spike_data['rvol']:.2f} | Volume={spike_data['volume']} | Above_VWAP={spike_data['above_vwap']} | Price_move={spike_data['price_move']*100:.1f}%"
            )

    if spike_detected and not volume_spike_was_true[symbol]:
        if last_trade_volume[symbol] < 500:
            logger.info(
                f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})"
            )
            return
        if (now - last_alert_time[symbol]['volume_spike']) < timedelta(
                minutes=ALERT_COOLDOWN_MINUTES):
            logger.info(
                f"[COOLDOWN] {symbol}: Skipping volume_spike alert due to cooldown ({(now - last_alert_time[symbol]['volume_spike']).total_seconds():.0f}s ago)"
            )
            return

        # Calculate alert score and add to pending alerts
        score = get_alert_score("volume_spike", symbol, spike_data)

        # DEBUG: Log volume spike alert addition
        if symbol in ["OCTO", "GRND", "EQS", "OSRH", "BJDX", "EBMT"]:
            logger.info(
                f"[📊 VOLUME SPIKE] {symbol} | Adding to pending_alerts | Score: {score} | Volume: {spike_data['volume']} | RVOL: {spike_data['rvol']:.2f}"
            )

        # 🚨 FIX: Use FRESHEST available price (real-time trade vs candle close)
        candle_time_vs = list(candles_seq)[-1]['start_time'] + timedelta(minutes=1)
        alert_price = get_display_price(symbol, close, candle_time_vs)
        price_str = f"{alert_price:.2f}"
        rvol_str = f"{spike_data['rvol']:.1f}"
        move_pct = spike_data['price_move'] * 100

        # Show actual percentage with proper sign
        if move_pct >= 0:
            move_str = f"+{move_pct:.1f}%"
        else:
            move_str = f"{move_pct:.1f}%"  # Negative sign already included

        alert_text = (f"🔥 <b>{escape_html(symbol)}</b> Volume Spike\n"
                      f"Price: ${price_str} ({move_str})")

        # Add to pending alerts instead of sending immediately
        pending_alerts[symbol].append({
            'type': 'volume_spike',
            'score': score,
            'message': alert_text
        })

        # Send the best alert for this symbol
        await send_best_alert(symbol)

        event_time = now
        log_event(
            "volume_spike", symbol, alert_price,
            volume, event_time, {
                "candle_price": close,
                "real_time_price": last_trade_price.get(symbol),
                "rvol": spike_data['rvol'],
                "vwap": vwap_value,
                "price_move": spike_data['price_move'],
                "alert_score": score
            })
        volume_spike_was_true[symbol] = True
        alerted_symbols[symbol] = today
        last_alert_time[symbol]['volume_spike'] = now

    # --- EMA STACK LOGIC PATCH (SESSION-AWARE THRESHOLDS) ---
    if (float_shares is not None and float_shares <= 20_000_000):
        session_type = get_session_type(list(candles_seq)[-1]['start_time'])
        closes = [c['close']
                  for c in list(candles_seq)]  # Use all available candles

        # Session-aware minimum candle requirements
        if session_type == "premarket":
            min_candles = 8  # 8 minutes after 4am start - faster premarket alerts
        elif session_type == "regular":
            min_candles = 15  # 15 minutes after 9:30am start
        else:  # afterhours
            min_candles = 12  # 12 minutes after 4pm start

        # Check if we're after 11am ET for 200 EMA inclusion
        ny_tz = pytz.timezone("America/New_York")
        current_time = datetime.now(timezone.utc).astimezone(ny_tz).time()
        use_200_ema = current_time >= dt_time(11, 0) and len(closes) >= 90

        if len(closes) < min_candles:
            logger.info(
                f"[EMA STACK] {symbol}: Skipping - need {min_candles} candles, have {len(closes)} for {session_type}"
            )
        else:
            # Get stored EMAs (much faster and more accurate)
            if use_200_ema:
                ema_periods = [5, 8, 13, 200]
                logger.info(
                    f"[EMA STACK] {symbol}: Using stored 5,8,13,200 EMAs (after 11am, {len(closes)} candles)"
                )
            else:
                ema_periods = [5, 8, 13]
                logger.info(
                    f"[EMA STACK] {symbol}: Using stored 5,8,13 EMAs only (before 11am, {len(closes)} candles)"
                )

            emas = get_stored_emas(symbol, ema_periods)

            # Check if EMAs are initialized
            if emas['ema5'] is None or emas['ema8'] is None or emas[
                    'ema13'] is None:
                logger.info(
                    f"[EMA STACK] {symbol}: EMAs not yet initialized, skipping"
                )
                return

            ema5 = emas['ema5']
            ema8 = emas['ema8']
            ema13 = emas['ema13']
            ema200 = emas.get('ema200') if use_200_ema else None
            # 🚨 CRITICAL FIX: Use centralized VWAP guard for EMA stack
            vwap_value = get_valid_vwap(symbol)
            if vwap_value is None:
                logger.info(
                    f"[VWAP GUARD] {symbol} - Blocking EMA stack alert - insufficient VWAP data"
                )
                return  # Block EMA stack alerts without valid VWAP
            last_candle = list(candles_seq)[-1]
            last_volume = last_candle['volume']
            candle_close = closes[-1]

            # 🚨 FIX: Use FRESHEST available price (real-time trade vs candle close)
            candle_time_ema = last_candle['start_time'] + timedelta(minutes=1)
            current_price_ema = get_display_price(symbol, candle_close, candle_time_ema)
            
            # Block alert if no fresh price data available
            if current_price_ema is None:
                logger.info(f"[EMA STACK BLOCKED] {symbol} - No fresh price data (all sources >5s old)")
                return
            
            # 🚨 CRITICAL VWAP CHECK - NEVER ALERT STOCKS UNDER VWAP!
            if vwap_value <= 0:
                logger.debug(
                    f"[EMA STACK FILTERED] {symbol} - invalid VWAP {vwap_value}"
                )
                return
            if current_price_ema <= vwap_value:
                logger.debug(
                    f"[EMA STACK FILTERED] {symbol} @ Real-time=${current_price_ema:.2f} - below/equal VWAP ${vwap_value:.2f}"
                )
                return

            if session_type in ["premarket", "afterhours"]:
                min_volume = 250_000
                min_dollar_volume = 50_000
            else:
                min_volume = 100_000
                min_dollar_volume = 75_000

            dollar_volume = current_price_ema * last_volume

            # STRENGTHENED EMA stack criteria (5,8,13) - MUCH more selective for higher quality alerts
            # NOTE: VWAP check moved above - real-time price already confirmed > VWAP
            base_ema_criteria = (
                ema5 > ema8 > ema13 and ema5 >= 1.011 * ema13
                and  # Keep original 1.1% spread - just ensure EMAs moving up
                current_price_ema >= 1.015 * vwap_value
                and  # 🚨 REAL-TIME price must be at least 1.5% above VWAP 
                current_price_ema >= 1.005 * ema13
                and  # 🚨 REAL-TIME price must be at least 0.5% above EMA13 
                last_volume >= min_volume and dollar_volume
                >= min_dollar_volume)

            # Add 200 EMA criteria if available
            if use_200_ema and ema200 is not None:
                ema_stack_criteria = base_ema_criteria and current_price_ema > ema200
            else:
                ema_stack_criteria = base_ema_criteria
            ema200_str = f", ema200={ema200:.2f}" if ema200 is not None else ""
            # Only log EMA stack info when alert actually fires (performance optimization)

            # 🔥 STRICT MOMENTUM CONFIRMATION - require rising volume streaks
            momentum_confirmed = False
            volume_streak_confirmed = False
            
            if base_ema_criteria:
                # Check for 2+ green candles in last 3 candles (realistic recent momentum check)
                if len(list(candles_seq)) >= 3:
                    recent_candles = list(candles_seq)[-3:]
                    green_candles = sum(1 for c in recent_candles
                                        if c['close'] > c['open'])
                    momentum_confirmed = green_candles >= 2
                    
                    # 🔥 NEW: Require rising volume streak (not just threshold)
                    recent_volumes = [c['volume'] for c in recent_candles]
                    volume_streak_confirmed = all(recent_volumes[i] <= recent_volumes[i+1] 
                                                  for i in range(len(recent_volumes)-1))

                # Alternative: Check if real-time price is up 3%+ from 5 candles ago (more realistic sustained move)
                if not momentum_confirmed and len(closes) >= 5:
                    price_5_ago = closes[-5]
                    price_gain = (current_price_ema -
                                  price_5_ago) / price_5_ago
                    momentum_confirmed = price_gain >= 0.03  # 3%+ gain from 5 candles ago
                    
                    # 🔥 NEW: Also check volume trend for 5-candle lookback
                    if len(list(candles_seq)) >= 5:
                        recent_5_volumes = [c['volume'] for c in list(candles_seq)[-5:]]
                        volume_streak_confirmed = recent_5_volumes[-1] > recent_5_volumes[0]  # Rising over 5 candles

                # Fallback: If not enough history, allow alert if real-time price > EMA5 (basic confirmation)
                if not momentum_confirmed and len(closes) < 5:
                    momentum_confirmed = current_price_ema > ema5
                    volume_streak_confirmed = True  # Allow if limited data

            # Final criteria: base EMA criteria AND momentum confirmation AND volume streak
            ema_stack_criteria = base_ema_criteria and momentum_confirmed and volume_streak_confirmed

            if ema_stack_criteria and not ema_stack_was_true[symbol]:
                vwap_margin = (current_price_ema -
                               vwap_value) / vwap_value * 100
                ema13_margin = (current_price_ema - ema13) / ema13 * 100
                logger.info(
                    f"[EMA STACK ALERT] {symbol}: session={session_type}, ema5={ema5:.2f}, ema8={ema8:.2f}, ema13={ema13:.2f}{ema200_str}, vwap={vwap_value:.2f}, real_time={current_price_ema:.2f}, candle_close={candle_close:.2f}, volume={last_volume}, dollar_volume={dollar_volume:.2f}, ratio={ema5/ema13:.4f}, vwap_margin={vwap_margin:.1f}%, ema13_margin={ema13_margin:.1f}%"
                )
                if ema5 / ema13 < 1.011:
                    logger.error(
                        f"[BUG] EMA stack alert would have fired but ratio invalid! {symbol}: ema5={ema5:.4f}, ema13={ema13:.4f}, ratio={ema5/ema13:.4f} (should be >= 1.011)"
                    )
                    return
                if last_trade_volume[symbol] < 500:
                    logger.info(
                        f"Not alerting {symbol}: last trade volume too low ({last_trade_volume[symbol]})"
                    )
                    return
                if (now - last_alert_time[symbol]['ema_stack']) < timedelta(
                        minutes=ALERT_COOLDOWN_MINUTES):
                    return
                
                # 🚨 REAL-TIME PRICE CONFIRMATION: Require FRESH price above VWAP
                real_time_price_ema = last_trade_price.get(symbol)
                real_time_timestamp_ema = last_trade_time.get(symbol)
                
                # Check if we have real-time price data (explicit None check to catch $0.00)
                if real_time_price_ema is None or real_time_timestamp_ema is None:
                    logger.info(f"[EMA STACK BLOCKED] {symbol} - No real-time price available for confirmation")
                    return
                
                # Recompute NOW to ensure freshness check reflects actual alert moment
                now_fresh_ema = datetime.now(timezone.utc)
                
                # Verify price data is FRESH (≤5 seconds old)
                price_age_ema = (now_fresh_ema - real_time_timestamp_ema).total_seconds()
                if price_age_ema > 5:
                    logger.info(f"[EMA STACK BLOCKED] {symbol} - Real-time price is stale ({price_age_ema:.1f}s old)")
                    return
                
                # Verify real-time price is ACTUALLY above VWAP (not just from get_display_price fallback)
                if real_time_price_ema <= vwap_value:
                    logger.info(f"[EMA STACK BLOCKED] {symbol} - Real-time price ${real_time_price_ema:.2f} NOT above VWAP ${vwap_value:.2f}")
                    return
                # 🚨 FIX: Use real-time price if fresh, otherwise candle close
                alert_price = get_display_price(symbol, candle_close)  # Fallback to candle close
                log_event(
                    "ema_stack", symbol,
                    alert_price, last_volume,
                    event_time, {
                        "ema5": ema5,
                        "ema8": ema8,
                        "ema13": ema13,
                        "vwap": vwap_value,
                        "session": session_type,
                        "real_time_price": last_trade_price.get(symbol)
                    })
                price_str = f"{alert_price:.2f}"
                ema_display = f"EMA5: {ema5:.2f}, EMA8: {ema8:.2f}, EMA13: {ema13:.2f}, VWAP: {vwap_value:.2f}"

                alert_text = (
                    f"⚡️ <b>{escape_html(symbol)}</b> EMA Stack [{session_type}]\n"
                    f"Current Price: ${price_str}\n"
                    f"{ema_display}")
                # Calculate alert score and add to pending alerts
                alert_data = {
                    'rvol': last_volume / 100000
                    if last_volume > 100000 else 1.0,  # Approximate RVOL
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
                last_alert_time[symbol]['ema_stack'] = now


def update_profile_for_day(symbol, day_candles):
    vol_profile.add_day(symbol, day_candles)


async def premarket_gainers_alert_loop():
    eastern = pytz.timezone("America/New_York")
    sent_today = False
    while True:
        now_utc = datetime.now(timezone.utc)
        now_est = now_utc.astimezone(eastern)
        if now_est.weekday() in range(0, 5):
            # Only send in narrow 10-second window (9:24:55-9:25:05) to avoid restart spam
            if (now_est.time().hour == 9 and now_est.time().minute == 24
                    and 55 <= now_est.time().second <= 59 and not sent_today):
                logger.info("Sending premarket gainers alert at 9:24:55am ET")
                gainers = []
                for sym in premarket_open_prices:
                    if sym in premarket_last_prices and premarket_open_prices[
                            sym] > 0:
                        pct_gain = (premarket_last_prices[sym] -
                                    premarket_open_prices[sym]
                                    ) / premarket_open_prices[sym] * 100
                        last_price = premarket_last_prices[sym]
                        total_vol = premarket_volumes.get(sym, 0)
                        if last_price <= 20 and total_vol >= 25000:
                            float_val = float_cache.get(sym)
                            float_str = f", Float: {float_val/1e6:.1f}M" if float_val else ""
                            gainers.append((sym, pct_gain, last_price,
                                            total_vol, float_str))
                gainers.sort(key=lambda x: x[1], reverse=True)
                top5 = gainers[:5]
                if top5:
                    gainers_text = "\n".join(
                        f"<b>{sym}</b>: {last_price:.2f} ({pct_gain:+.1f}%) Vol:{int(total_vol/1000)}K{float_str}"
                        for sym, pct_gain, last_price, total_vol, float_str in
                        top5)
                else:
                    gainers_text = None  # Don't send message if no gainers found

                # Only send alert if there are actual gainers to report
                if gainers_text:
                    msg = (
                        "Market opens in 5 mins...secure the damn bag!\n"
                        "Here are the top 5 premarket gainers (since 4am):\n"
                        f"{gainers_text}")
                    await send_all_alerts(msg)
                    event_time = datetime.now(timezone.utc)
                    log_event("premarket_gainers", "PREMARKET", 0, 0,
                              event_time, {"gainers": gainers_text})
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

        # Only send alert on trading days (Mon-Thu) at 8:01 PM (narrow 2-minute window to avoid restart spam)
        if now_est.weekday() in (0, 1, 2, 3):
            if (dt_time(20, 1) <= now_est.time() <= dt_time(20, 3)) and not sent_today:
                await send_all_alerts(
                    "Market Closed. Reconvene in pre market tomorrow.")
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


# Connection manager with exponential backoff
connection_backoff_delay = 15  # Start with 15 seconds
max_backoff_delay = 300  # Max 5 minutes
connection_attempts = 0


async def ingest_polygon_events():
    global connection_backoff_delay, connection_attempts
    url = "wss://socket.polygon.io/stocks"

    while True:
        if not is_market_scan_time():
            print(
                "[SCAN PAUSED] Market closed (outside 4am-8pm EST, Mon-Fri). Sleeping 60s."
            )
            await asyncio.sleep(60)
            continue

        try:
            print(
                f"[CONNECTION] Connecting to Polygon WebSocket... (attempt {connection_attempts + 1})"
            )
            async with websockets.connect(url,
                                          ping_interval=15,
                                          ping_timeout=20) as ws:
                print(
                    "[CONNECTION] Successfully connected to Polygon WebSocket")

                # Perform catch-up backfill if this is a reconnection
                if connection_attempts > 0:
                    print(
                        "[BACKFILL] Performing catch-up analysis for missed data..."
                    )
                    await perform_connection_backfill()

                # Reset backoff on successful connection (after backfill check)
                connection_backoff_delay = 15
                connection_attempts = 0

                await ws.send(
                    json.dumps({
                        "action": "auth",
                        "params": POLYGON_API_KEY
                    }))
                # Subscribe to ALL active stocks - full market scanning for dynamic discovery + halt monitoring
                await ws.send(
                    json.dumps({
                        "action": "subscribe",
                        "params": "AM.*,T.*,LULD.*"
                    }))
                print(
                    "Subscribed to: AM.* (minute bars), T.* (trades), LULD.* (halt/volatility alerts) - FULL monitoring"
                )

                while True:
                    if not is_market_scan_time():
                        print(
                            "[SCAN PAUSED] Market closed during active connection. Sleeping 60s, breaking websocket."
                        )
                        await asyncio.sleep(60)
                        break

                    msg = await ws.recv()
                    print("[RAW MSG]", msg)

                    # Skip malformed or heartbeat messages
                    if not msg or len(msg.strip()) < 2 or not msg.strip(
                    ).startswith('{') and not msg.strip().startswith('['):
                        print(f"[SKIP] Non-JSON message: '{msg}'")
                        continue

                    try:
                        data = json.loads(msg)
                        if isinstance(data, dict):
                            data = [data]
                        for event in data:
                            # Handle connection limit status messages FIRST
                            if event.get("ev") == "status" and event.get(
                                    "status") == "max_connections":
                                print(
                                    f"[🚨 CONNECTION LIMIT] Polygon API connection limit exceeded!"
                                )
                                print(
                                    f"[🚨 CONNECTION LIMIT] Message: {event.get('message', 'No details')}"
                                )
                                # Treat as throttling signal - trigger exponential backoff
                                raise ConnectionError(
                                    "max_connections_exceeded")

                            elif event.get("ev") == "T":
                                symbol = event["sym"]
                                price = event["p"]
                                size = event.get('s', 0)
                                trade_timestamp = event.get('t', 0)  # Polygon's event timestamp (milliseconds)

                                # Check eligibility BEFORE processing/logging
                                float_shares = await get_float_shares(symbol)
                                
                                # 🎯 GRANDFATHERING: Use entry price for already-tracked stocks
                                is_tracked = symbol in entry_price and entry_price[symbol] is not None
                                
                                if not is_eligible(symbol, price, float_shares, use_entry_price=is_tracked):
                                    # If not eligible and was being tracked, clear entry price
                                    if is_tracked:
                                        logger.info(f"[GRANDFATHERING] {symbol} no longer eligible, removing from tracking (entry: ${entry_price[symbol]:.2f}, current: ${price:.2f})")
                                        entry_price[symbol] = None
                                    continue  # Skip ineligible symbols completely
                                
                                # 🎯 NEW STOCK: Record entry price when first eligible
                                if not is_tracked:
                                    entry_price[symbol] = price
                                    logger.info(f"[GRANDFATHERING] {symbol} now eligible - locked entry price: ${price:.2f}")

                                # 🚨 FIX: Use Polygon's timestamp to reject stale trades
                                trade_time = datetime.fromtimestamp(trade_timestamp / 1000, tz=timezone.utc)
                                now_utc = datetime.now(timezone.utc)
                                trade_age_seconds = (now_utc - trade_time).total_seconds()
                                
                                # Reject trades older than 3 seconds (stale data)
                                if trade_age_seconds > 3:
                                    logger.debug(f"[STALE TRADE] {symbol} - Trade is {trade_age_seconds:.1f}s old, rejecting")
                                    continue
                                
                                last_trade_price[symbol] = price
                                last_trade_volume[symbol] = size
                                last_trade_time[symbol] = trade_time  # Use Polygon's timestamp, not now()
                                print(
                                    f"[TRADE EVENT] {symbol} | Price={price} | Size={size} | Time={trade_time} | Age={trade_age_seconds:.1f}s"
                                )
                            elif event.get("ev") == "AM":
                                symbol = event["sym"]
                                open_ = event["o"]
                                high = event["h"]
                                low = event["l"]
                                close = event["c"]
                                volume = event["v"]
                                start_time = polygon_time_to_utc(event["s"])

                                # DEBUG: Special logging for OCTO to track why no alerts
                                if symbol == "OCTO":
                                    logger.info(
                                        f"[🚨 OCTO DEBUG] Processing AM event: {event}"
                                    )
                                    logger.info(
                                        f"[🚨 OCTO DEBUG] Received AM candle: {start_time} OHLCV: {open_}/{high}/{low}/{close}/{volume}"
                                    )

                                print(
                                    f"[POLYGON] {symbol} {start_time} o:{open_} h:{high} l:{low} c:{close} v:{volume}"
                                )

                                candle = {
                                    "open": open_,
                                    "high": high,
                                    "low": low,
                                    "close": close,
                                    "volume": volume,
                                    "start_time": start_time,
                                }
                                if not isinstance(candles[symbol], deque):
                                    candles[symbol] = deque(candles[symbol],
                                                            maxlen=20)
                                if not isinstance(vwap_candles[symbol], list):
                                    vwap_candles[symbol] = list(
                                        vwap_candles[symbol])
                                candles[symbol].append(candle)
                                session_date = get_session_date(
                                    candle['start_time'])
                                last_session = vwap_session_date[symbol]
                                if last_session != session_date:
                                    vwap_candles[symbol] = []
                                    vwap_session_date[symbol] = session_date
                                
                                # 🚨 FIX: Reset VWAP at 9:30 AM to exclude pre-market data
                                candle_time = start_time.astimezone(eastern).time()
                                market_open_time = dt_time(9, 30)
                                
                                # If this is the first candle at or after 9:30 AM, reset VWAP
                                if len(vwap_candles[symbol]) > 0:
                                    last_candle_time = vwap_candles[symbol][-1]['start_time'].astimezone(eastern).time()
                                    # Reset if crossing from pre-market into regular session
                                    if last_candle_time < market_open_time <= candle_time:
                                        logger.info(f"[VWAP RESET] {symbol} - Market open at 9:30 AM, clearing pre-market data")
                                        vwap_candles[symbol] = []
                                        vwap_cum_vol[symbol] = 0
                                        vwap_cum_pv[symbol] = 0
                                
                                # 🚨 CORPORATE ACTION DETECTION: Reset VWAP on splits/reverse splits
                                if len(vwap_candles[symbol]) > 0:
                                    last_close = vwap_candles[symbol][-1]['close']
                                    current_close = candle['close']
                                    price_change_pct = abs(current_close - last_close) / last_close if last_close > 0 else 0
                                    if price_change_pct > 1.0:  # >100% price change = likely corporate action
                                        logger.warning(
                                            f"[VWAP RESET] {symbol} - Corporate action detected: "
                                            f"price jumped from ${last_close:.2f} to ${current_close:.2f} "
                                            f"({price_change_pct*100:.1f}% change). Resetting VWAP."
                                        )
                                        vwap_candles[symbol] = []
                                
                                vwap_candles[symbol].append(candle)
                                vwap_cum_vol[symbol] += volume
                                vwap_cum_pv[symbol] += (
                                    (high + low + close) / 3) * volume
                                
                                # 🚨 CRITICAL FIX: Update last_trade_price/time from candle as fallback
                                # Use CURRENT time to pass freshness validation (not candle time which could be stale)
                                now_utc = datetime.now(timezone.utc)
                                current_trade_time = last_trade_time.get(symbol)
                                candle_close_time = start_time + timedelta(minutes=1)
                                
                                # Update ONLY if: no trade data OR trade data is older than candle close
                                if current_trade_time is None or current_trade_time < candle_close_time:
                                    last_trade_price[symbol] = close
                                    last_trade_time[symbol] = now_utc  # Use NOW so freshness check passes!
                                    logger.info(f"[PRICE FALLBACK] {symbol} - Using candle close ${close:.2f} with current timestamp (no fresh trades)")
                                
                                await on_new_candle(symbol, open_, high, low,
                                                    close, volume, start_time)
                                # Process all pending alerts and send only the best one
                                if symbol == "OCTO":
                                    logger.info(
                                        f"[🚨 OCTO DEBUG] About to call send_best_alert for {symbol}"
                                    )
                                await send_best_alert(symbol)
                            elif event.get("ev") == "LULD":
                                # Handle LULD (Limit Up/Limit Down) events - Polygon's official halt detection!
                                symbol = event.get(
                                    "T")  # LULD uses 'T' for ticker symbol
                                high_limit = event.get("h")  # High price limit
                                low_limit = event.get("l")  # Low price limit
                                timestamp = event.get(
                                    "t", 0)  # Unix timestamp in milliseconds
                                indicators = event.get("i",
                                                       [])  # Indicators array

                                if symbol and timestamp:
                                    # Format halt key for deduplication
                                    halt_time = datetime.fromtimestamp(
                                        timestamp / 1000, tz=timezone.utc)
                                    halt_time_et = halt_time.astimezone(eastern)
                                    halt_key = f"{symbol}_{halt_time_et.strftime('%H:%M:%S')}_LULD"

                                    # Skip if already processed
                                    if halt_key in halted_symbols:
                                        continue

                                    halted_symbols.add(halt_key)

                                    # 🚨🚨 SPECIAL HALT HANDLING - BULLETPROOF 🚨🚨
                                    logger.warning(f"[LULD DETECTED] {symbol} halt at {halt_time_et.strftime('%I:%M:%S %p ET')}")
                                    
                                    try:
                                        # Try to get price from multiple sources
                                        current_price = last_trade_price.get(symbol)
                                        price_source = "last_trade"
                                        
                                        if not current_price:
                                            logger.info(f"[LULD] {symbol} - No last_trade_price, trying yfinance...")
                                            try:
                                                import yfinance as yf
                                                ticker_info = yf.Ticker(symbol).info
                                                current_price = ticker_info.get('currentPrice') or ticker_info.get('regularMarketPrice')
                                                price_source = "yfinance"
                                            except Exception as yf_error:
                                                logger.warning(f"[LULD] {symbol} - yfinance failed: {yf_error}")
                                        
                                        if not current_price and symbol in candles and len(candles[symbol]) > 0:
                                            current_price = candles[symbol][-1]['close']
                                            price_source = "candle"
                                            logger.info(f"[LULD] {symbol} - Using candle price ${current_price:.2f}")

                                        float_shares = await get_float_shares(symbol)
                                        
                                        # 🚨 SPECIAL HALT RULE: Only filter if price is KNOWN and >$15
                                        # If price unknown, send alert anyway (halts are critical!)
                                        should_alert = True
                                        filter_reason = None
                                        
                                        if current_price:
                                            logger.info(f"[LULD] {symbol} - Price ${current_price:.2f} from {price_source}")
                                            if current_price > PRICE_THRESHOLD:
                                                should_alert = False
                                                filter_reason = f"price ${current_price:.2f} > ${PRICE_THRESHOLD}"
                                        else:
                                            logger.warning(f"[LULD] {symbol} - NO PRICE DATA - will alert anyway (halt is critical)")
                                        
                                        # Check float only if we should alert based on price
                                        if should_alert and float_shares is not None:
                                            if not (MIN_FLOAT_SHARES <= float_shares <= MAX_FLOAT_SHARES or symbol in FLOAT_EXCEPTION_SYMBOLS):
                                                should_alert = False
                                                filter_reason = f"float {float_shares/1e6:.1f}M out of range"
                                        
                                        if should_alert:
                                            float_display = f"{float_shares/1e6:.1f}M" if float_shares else "⚠️ Unknown"
                                            price_display = f"${current_price:.2f}" if current_price else "⚠️ Unknown"
                                            
                                            # Determine halt reason from indicators
                                            halt_reason = "Volatility Halt"
                                            if 1 in indicators:
                                                halt_reason = "Limit Up Halt"
                                            elif 2 in indicators:
                                                halt_reason = "Limit Down Halt"

                                            # Format LULD halt alert
                                            alert_msg = f"""🛑 <b>LULD HALT ALERT</b> (Polygon Official)
                                            
<b>Symbol:</b> ${symbol}
<b>Price:</b> {price_display}
<b>Float:</b> {float_display} shares
<b>Halt Time:</b> {halt_time_et.strftime('%I:%M:%S %p ET')}
<b>Reason:</b> {halt_reason}
<b>High Limit:</b> ${high_limit:.2f if high_limit else 'N/A'}
<b>Low Limit:</b> ${low_limit:.2f if low_limit else 'N/A'}
<b>Status:</b> VOLATILITY HALT ⚠️

🚀 <i>Real-time via Polygon LULD feed</i>"""

                                            logger.warning(f"[🚨 HALT ALERT] Sending LULD halt for {symbol} @ {price_display} ({price_source})")
                                            await send_all_alerts(alert_msg)

                                            # Log halt event
                                            log_event(
                                                "polygon_luld_halt", symbol,
                                                current_price if current_price else 0, 0, halt_time, {
                                                    "halt_reason": halt_reason,
                                                    "high_limit": high_limit,
                                                    "low_limit": low_limit,
                                                    "indicators": indicators,
                                                    "float_shares": float_shares,
                                                    "price_source": price_source,
                                                    "data_source": "polygon_luld"
                                                })

                                            logger.warning(f"[✅ LULD SENT] {symbol} @ {price_display} - {halt_reason}")
                                        else:
                                            logger.info(f"[LULD FILTERED] {symbol} - {filter_reason}")

                                    except Exception as e:
                                        logger.error(f"[🚨 LULD ERROR] Error processing halt for {symbol}: {e}", exc_info=True)
                                        # 🚨 EMERGENCY: Try to send alert even on error
                                        try:
                                            emergency_msg = f"🛑 <b>HALT DETECTED</b>\n\n<b>Symbol:</b> ${symbol}\n<b>Time:</b> {halt_time_et.strftime('%I:%M:%S %p ET')}\n\n⚠️ Error getting details - check logs"
                                            await send_all_alerts(emergency_msg)
                                            logger.warning(f"[LULD] Sent emergency halt alert for {symbol}")
                                        except:
                                            pass
                                        continue
                    except Exception as e:
                        print(f"Error processing message: {e}\nRaw: {msg}")

        except Exception as e:
            connection_attempts += 1
            print(f"[CONNECTION ERROR] Websocket error: {e}")

            # Handle connection limit errors with exponential backoff
            if ("max_connections" in str(e).lower()
                    or "connection" in str(e).lower()
                    or "limit" in str(e).lower()
                    or "policy violation" in str(e).lower()
                    or "1008" in str(e)):

                print(
                    f"[🚨 CONNECTION LIMIT] Connection limit/policy violation detected!"
                )
                print(
                    f"[🚨 CONNECTION LIMIT] Backing off for {connection_backoff_delay} seconds..."
                )

                # Send alert about connection issues
                alert_msg = f"🚨 <b>SCANNER CONNECTION ISSUE</b>\n\nPolygon connection limit exceeded. Backing off for {connection_backoff_delay}s.\nAttempt: {connection_attempts}"
                try:
                    await send_all_alerts(alert_msg)
                except:
                    pass  # Don't fail on alert send

                await asyncio.sleep(connection_backoff_delay)

                # Exponential backoff: 15s → 30s → 60s → 120s → 300s (max 5min)
                connection_backoff_delay = min(connection_backoff_delay * 2,
                                               max_backoff_delay)

            elif "keepalive" in str(e).lower() or "ping" in str(
                    e).lower() or "1011" in str(e):
                print(
                    "[CONNECTION] Keepalive/ping timeout - reconnecting in 10 seconds..."
                )
                await asyncio.sleep(10)
            else:
                print(
                    f"[CONNECTION] General error - reconnecting in 30 seconds..."
                )
                await asyncio.sleep(30)


async def nasdaq_halt_monitor():
    """🚨 PRIMARY HALT SOURCE: Monitor NASDAQ every 15 seconds (more reliable than WebSocket)
    Catches ALL halt types (T1, T2, T5, T12, LUDP, etc.) with relaxed $20 price threshold
    Also alerts on RESUMES and tracks previously halted stocks to bypass price filter"""
    import aiohttp
    from datetime import datetime, timezone
    import pickle
    import os
    
    seen_halts = set()  # Track processed halts (halt_key)
    seen_resumes = set()  # Track processed resumes (resume_key)
    
    # Load persisted halted symbols (survives restarts)
    ever_halted_symbols = set()
    if os.path.exists("halted_symbols.pkl"):
        try:
            with open("halted_symbols.pkl", "rb") as f:
                ever_halted_symbols = pickle.load(f)
            logger.info(f"[HALT TRACKER] Loaded {len(ever_halted_symbols)} previously halted symbols from disk")
        except:
            logger.warning("[HALT TRACKER] Could not load halted_symbols.pkl, starting fresh")
            ever_halted_symbols = set()
    
    while True:
        try:
            ny_tz = pytz.timezone("America/New_York")
            now = datetime.now(timezone.utc).astimezone(ny_tz)
            
            # Only run during extended market hours (4am-8pm ET)
            if not (4 <= now.hour < 20):
                await asyncio.sleep(300)  # Check every 5 minutes when closed
                continue
            
            # Scrape NASDAQ halt page
            url = "https://www.nasdaqtrader.com/trader.aspx?id=tradehalts"
            timeout = aiohttp.ClientTimeout(total=10)
            async with http_session.get(url, timeout=timeout) as response:
                if response.status == 200:
                        html = await response.text()
                        
                        # Parse halt data (NASDAQ uses table format)
                        from bs4 import BeautifulSoup
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # Find halt table
                        table = soup.find('table', {'id': 'Trading_Halts'})
                        if table:
                            rows = table.find_all('tr')[1:]  # Skip header
                            
                            for row in rows:
                                cols = row.find_all('td')
                                if len(cols) >= 9:  # Need at least 9 columns
                                    symbol = cols[0].text.strip()              # Column 0: Issue Symbol
                                    halt_time_str = cols[6].text.strip()       # Column 6: Halt Time (ET)
                                    resume_time_str = cols[8].text.strip()     # Column 8: Resume Time (ET)
                                    halt_reason_code = cols[3].text.strip()    # Column 3: Reason Code
                                    
                                    # Create unique keys
                                    halt_key = f"{symbol}_{halt_time_str}_{halt_reason_code}"
                                    resume_key = f"{symbol}_{halt_time_str}_{resume_time_str}"
                                    
                                    # Check if this is a RESUME (has resume time and we haven't alerted on this resume yet)
                                    if resume_time_str and resume_time_str != '':
                                        if resume_key not in seen_resumes:
                                            seen_resumes.add(resume_key)
                                            
                                            # 🟢 RESUME ALERT
                                            logger.warning(f"[NASDAQ RESUME] {symbol} resumed at {resume_time_str} ET (halted at {halt_time_str})")
                                            
                                            try:
                                                # Get current price
                                                current_price = last_trade_price.get(symbol)
                                                if not current_price and symbol in candles and len(candles[symbol]) > 0:
                                                    current_price = candles[symbol][-1]['close']
                                                
                                                price_display = f"${current_price:.2f}" if current_price else "⚠️ Unknown"
                                                
                                                # Send resume alert (no filtering - resumes are important!)
                                                alert_msg = f"""🟢 <b>TRADING RESUMED</b> (NASDAQ)

<b>Symbol:</b> {symbol}
<b>Price:</b> {price_display}
<b>Halted:</b> {halt_time_str} ET
<b>Resumed:</b> {resume_time_str} ET
<b>Reason:</b> {halt_reason_code}
<b>Status:</b> BACK TO TRADING ▶️

🚀 <i>Real-time via NASDAQ feed</i>"""
                                                
                                                logger.warning(f"[🟢 RESUME ALERT] Sending resume for {symbol} @ {price_display}")
                                                await send_all_alerts(alert_msg)
                                                
                                                # Log resume event
                                                log_event(
                                                    "nasdaq_resume", symbol,
                                                    current_price if current_price else 0, 0, now, {
                                                        "halt_time": halt_time_str,
                                                        "resume_time": resume_time_str,
                                                        "halt_code": halt_reason_code,
                                                        "data_source": "nasdaq_scrape"
                                                    })
                                            except Exception as e:
                                                logger.error(f"[RESUME ERROR] Error processing resume for {symbol}: {e}")
                                        continue  # Don't process as halt
                                    
                                    # Process as HALT (no resume time yet)
                                    if halt_key in seen_halts:
                                        continue
                                    
                                    seen_halts.add(halt_key)
                                    
                                    # Track and persist halted symbols (survives restarts)
                                    if symbol not in ever_halted_symbols:
                                        ever_halted_symbols.add(symbol)
                                        try:
                                            with open("halted_symbols.pkl", "wb") as f:
                                                pickle.dump(ever_halted_symbols, f)
                                            logger.info(f"[HALT TRACKER] Added {symbol} to persistent halt tracker ({len(ever_halted_symbols)} total)")
                                        except Exception as e:
                                            logger.error(f"[HALT TRACKER] Failed to save halted symbols: {e}")
                                    
                                    # 🚨🚨 SPECIAL HALT HANDLING - BULLETPROOF 🚨🚨
                                    logger.warning(f"[NASDAQ DETECTED] {symbol} halt at {halt_time_str} ET (Reason: {halt_reason_code})")
                                    
                                    try:
                                        # Try to get price from multiple sources with detailed logging
                                        current_price = last_trade_price.get(symbol)
                                        price_source = "last_trade"
                                        
                                        if not current_price:
                                            logger.info(f"[HALT PRICE] {symbol} - No last_trade_price, trying yfinance...")
                                            try:
                                                import yfinance as yf
                                                ticker_info = yf.Ticker(symbol).info
                                                current_price = ticker_info.get('currentPrice') or ticker_info.get('regularMarketPrice')
                                                price_source = "yfinance"
                                                if current_price:
                                                    logger.info(f"[HALT PRICE] {symbol} - Got ${current_price:.2f} from yfinance")
                                            except Exception as yf_error:
                                                logger.warning(f"[HALT PRICE] {symbol} - yfinance failed: {yf_error}")
                                        
                                        if not current_price and symbol in candles and len(candles[symbol]) > 0:
                                            current_price = candles[symbol][-1]['close']
                                            price_source = "candle"
                                            logger.info(f"[HALT PRICE] {symbol} - Using candle price ${current_price:.2f}")
                                        
                                        # Get float with detailed logging
                                        float_shares = await get_float_shares(symbol)
                                        if float_shares:
                                            logger.info(f"[HALT FLOAT] {symbol} - Float: {float_shares/1e6:.2f}M shares")
                                        else:
                                            logger.warning(f"[HALT FLOAT] {symbol} - Could not get float data (will not block halt alert)")
                                        
                                        # 🚨 SPECIAL HALT RULE: RELAXED price filter for halts (critical events!)
                                        # Allow up to $20 for halts (vs $15 for regular alerts)
                                        # If price unknown, send alert anyway (halts are critical!)
                                        # If symbol was halted before, BYPASS price filter completely!
                                        should_alert = True
                                        filter_reason = None
                                        HALT_PRICE_THRESHOLD = 20.00  # Higher threshold for halts
                                        
                                        if current_price:
                                            logger.info(f"[NASDAQ] {symbol} - Price ${current_price:.2f} from {price_source}")
                                            
                                            # Bypass price filter if this symbol has been halted before
                                            if symbol in ever_halted_symbols:
                                                logger.warning(f"[NASDAQ] ✅ {symbol} BYPASSING price filter (previously halted, now ${current_price:.2f})")
                                            elif current_price > HALT_PRICE_THRESHOLD:
                                                should_alert = False
                                                filter_reason = f"price ${current_price:.2f} > ${HALT_PRICE_THRESHOLD}"
                                            else:
                                                logger.warning(f"[NASDAQ] ✅ {symbol} PASSED price filter (${current_price:.2f} <= ${HALT_PRICE_THRESHOLD})")
                                        else:
                                            logger.warning(f"[NASDAQ] {symbol} - NO PRICE DATA - will alert anyway (halt is critical)")
                                        
                                        # Check float only if we should alert based on price
                                        if should_alert and float_shares is not None:
                                            if not (MIN_FLOAT_SHARES <= float_shares <= MAX_FLOAT_SHARES or symbol in FLOAT_EXCEPTION_SYMBOLS):
                                                should_alert = False
                                                filter_reason = f"float {float_shares/1e6:.1f}M out of range"
                                            else:
                                                logger.warning(f"[NASDAQ] ✅ {symbol} PASSED float filter ({float_shares/1e6:.1f}M <= {MAX_FLOAT_SHARES/1e6:.0f}M)")
                                        
                                        if should_alert:
                                            float_display = f"{float_shares/1e6:.1f}M" if float_shares else "⚠️ Unknown"
                                            price_display = f"${current_price:.2f}" if current_price else "⚠️ Unknown"
                                            
                                            logger.warning(f"[HALT APPROVED] {symbol} - Sending alert! Price: {price_display}, Float: {float_display}")
                                            
                                            # Decode halt reason
                                            halt_reasons = {
                                                'T1': 'News Pending',
                                                'T2': 'News Released',
                                                'T5': 'Single Security Trading Pause',
                                                'T6': 'Regulatory Concern',
                                                'T8': 'ETF Component Security Halt',
                                                'T12': 'Additional Information Requested',
                                                'LUDP': 'Volatility Trading Pause',
                                                'LUDS': 'Straddle Condition',
                                                'MWC': 'Market-Wide Circuit Breaker',
                                                'IPO1': 'IPO Not Ready',
                                                'M': 'Volatility Trading Pause',
                                            }
                                            halt_reason = halt_reasons.get(halt_reason_code, halt_reason_code)
                                            
                                            # Send alert
                                            alert_msg = f"""🛑 <b>TRADING HALT</b> (NASDAQ)

<b>Symbol:</b> {symbol}
<b>Price:</b> {price_display}
<b>Float:</b> {float_display} shares
<b>Halt Time:</b> {halt_time_str} ET
<b>Reason:</b> {halt_reason} ({halt_reason_code})
<b>Status:</b> HALTED ⏸️

🚀 <i>Real-time via NASDAQ feed</i>"""
                                            
                                            logger.warning(f"[🚨 HALT ALERT] Sending NASDAQ halt for {symbol} @ {price_display} ({price_source})")
                                            await send_all_alerts(alert_msg)
                                            
                                            # Log halt event
                                            log_event(
                                                "nasdaq_halt", symbol,
                                                current_price if current_price else 0, 0, now, {
                                                    "halt_reason": halt_reason,
                                                    "halt_code": halt_reason_code,
                                                    "halt_time": halt_time_str,
                                                    "float_shares": float_shares,
                                                    "price_source": price_source,
                                                    "data_source": "nasdaq_scrape"
                                                })
                                            
                                            logger.warning(f"[✅ NASDAQ SENT] {symbol} @ {price_display} - {halt_reason}")
                                        else:
                                            float_str = f"{float_shares/1e6:.1f}M" if float_shares else "None"
                                            price_str = f"${current_price:.2f}" if current_price else "None"
                                            logger.warning(f"[❌ HALT FILTERED] {symbol} - {filter_reason} (Price: {price_str}, Float: {float_str})")
                                        
                                    except Exception as e:
                                        logger.error(f"[🚨 NASDAQ ERROR] Error processing halt for {symbol}: {e}", exc_info=True)
                                        # 🚨 EMERGENCY: Try to send alert even on error
                                        try:
                                            emergency_msg = f"🛑 <b>HALT DETECTED</b>\n\n<b>Symbol:</b> {symbol}\n<b>Time:</b> {halt_time_str} ET\n<b>Source:</b> NASDAQ\n\n⚠️ Error getting details - check logs"
                                            await send_all_alerts(emergency_msg)
                                            logger.warning(f"[NASDAQ] Sent emergency halt alert for {symbol}")
                                        except:
                                            pass
                                        continue
                        
        except Exception as e:
            logger.error(f"[NASDAQ HALT MONITOR] Error: {e}")
        
        # 🚨 PRIMARY HALT SOURCE: Check every 15 seconds for fast detection (more reliable than WebSocket)
        await asyncio.sleep(15)


async def ml_training_loop():
    """Background task for ML training and outcome tracking"""
    while True:
        try:
            # Check alert outcomes every 5 minutes
            await check_alert_outcomes()

            # Retrain model once per day at market close
            ny_time = datetime.now(timezone.utc).astimezone(
                pytz.timezone("America/New_York"))
            if ny_time.hour == 20 and ny_time.minute < 5:  # 8:00-8:05 PM ET
                await retrain_model_if_needed()

        except Exception as e:
            logger.error(f"[ML TRAINING] Error in ML training loop: {e}")

        await asyncio.sleep(300)  # 5 minutes


async def main():
    global http_session
    
    # 🚀 PERFORMANCE: Initialize reusable HTTP session
    http_session = aiohttp.ClientSession()
    print("[HTTP SESSION] Created reusable HTTP session for better performance")
    
    print("Main event loop running. Press Ctrl+C to exit.")
    ingest_task = asyncio.create_task(ingest_polygon_events())
    # Enabling just the scheduled alerts (9:24:55am and 8:01pm)
    close_alert_task = asyncio.create_task(market_close_alert_loop())
    premarket_alert_task = asyncio.create_task(premarket_gainers_alert_loop())
    # Enable NASDAQ halt monitoring (catches T1, T2, T5, T12 halts that Polygon LULD misses)
    nasdaq_halt_task = asyncio.create_task(nasdaq_halt_monitor())
    try:
        while True:
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        print("Main loop cancelled.")
    finally:
        ingest_task.cancel()
        close_alert_task.cancel()
        premarket_alert_task.cancel()
        nasdaq_halt_task.cancel()

        await ingest_task
        await close_alert_task
        await premarket_alert_task
        await nasdaq_halt_task
        
        # 🚀 PERFORMANCE: Close reusable HTTP session
        if http_session:
            await http_session.close()
            print("[HTTP SESSION] Closed reusable HTTP session")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
