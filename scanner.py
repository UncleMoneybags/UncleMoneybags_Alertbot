import logging
import asyncio
import websockets
import aiohttp
import json
import html
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta, date, time as dt_time
from email.utils import parsedate_to_datetime
from concurrent.futures import ThreadPoolExecutor
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

# 🚀 SAFETY NET: Register cleanup handler for unexpected exits
def cleanup_resources():
    """Emergency cleanup handler for unexpected shutdowns
    
    Ensures both HTTP session and ThreadPoolExecutor are properly closed
    even if normal shutdown path is bypassed.
    """
    global http_session, io_executor
    try:
        # Close HTTP session to prevent connection leaks
        if http_session and not http_session.closed:
            import asyncio
            try:
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(http_session.close())
                else:
                    loop.run_until_complete(http_session.close())
                logger.info("[ATEXIT] Closed HTTP session")
            except:
                # If async close fails, at least attempt sync cleanup
                try:
                    http_session.connector.close()
                except:
                    pass
        
        # Shutdown executor with wait=True to ensure in-flight writes complete
        if io_executor:
            io_executor.shutdown(wait=True)
            logger.info("[ATEXIT] Executor shutdown complete (waited for in-flight tasks)")
    except Exception as e:
        logger.error(f"[ATEXIT] Cleanup error: {e}")

atexit.register(cleanup_resources)

MARKET_OPEN = dt_time(4, 0)
MARKET_CLOSE = dt_time(20, 0)
eastern = pytz.timezone("America/New_York")
logger = logging.getLogger(__name__)

# 🚀 PERFORMANCE: Reusable HTTP session (30-50% faster than creating new sessions)
http_session = None

# 🚀 PERFORMANCE: Thread pool executor for blocking I/O operations (pickle, CSV, JSON)
# Prevents blocking the event loop during disk writes
io_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="io_worker")

last_trade_price = defaultdict(lambda: None)
last_trade_volume = defaultdict(lambda: 0)
last_trade_time = defaultdict(lambda: None)

# 🎯 GRANDFATHERING: Track entry price when stock first becomes eligible
# Once eligible, keep monitoring even if price runs past $15 threshold
entry_price = defaultdict(lambda: None)

float_cache = {}
float_cache_none_retry = {}
FLOAT_CACHE_NONE_RETRY_MIN = 10  # minutes
FLOAT_CACHE_SAVE_DEBOUNCE_SECONDS = 300  # Only save every 5 minutes to reduce disk I/O
_last_float_cache_save = datetime.min.replace(tzinfo=timezone.utc)
_float_cache_lock = asyncio.Lock()  # 🔒 Prevent concurrent write corruption


async def save_float_cache(force=False):
    """Save float cache to disk with debouncing to reduce I/O overhead.
    
    🔒 THREAD-SAFE: Uses asyncio.Lock to prevent concurrent writes that could corrupt files.
    Uses atomic writes (temp file + rename) to prevent partial writes on crash.
    
    Args:
        force: If True, save immediately regardless of debounce timer
    """
    global _last_float_cache_save
    now = datetime.now(timezone.utc)
    
    # Debounce: only save if enough time has passed or forced
    if not force and (now - _last_float_cache_save).total_seconds() < FLOAT_CACHE_SAVE_DEBOUNCE_SECONDS:
        return
    
    # 🔒 Acquire lock to prevent concurrent writes
    async with _float_cache_lock:
        # Atomic write: write to temp file then rename (prevents corruption on crash)
        tmp1 = "float_cache.pkl.tmp"
        tmp2 = "float_cache_none.pkl.tmp"
        
        # 🚀 PERFORMANCE: Run blocking I/O in thread executor to prevent event loop blocking
        loop = asyncio.get_event_loop()
        
        def write_pickle_files():
            with open(tmp1, "wb") as f:
                pickle.dump(float_cache, f)
            with open(tmp2, "wb") as f:
                pickle.dump(float_cache_none_retry, f)
        
        await loop.run_in_executor(io_executor, write_pickle_files)
        
        # Atomic rename (replaces old file atomically)
        os.replace(tmp1, "float_cache.pkl")
        os.replace(tmp2, "float_cache_none.pkl")
        
        _last_float_cache_save = now
        logger.debug(
            f"Saved float cache, entries: {len(float_cache)}, none cache: {len(float_cache_none_retry)}"
        )


def load_float_cache():
    global float_cache, float_cache_none_retry
    if os.path.exists("float_cache.pkl"):
        with open("float_cache.pkl", "rb") as f:
            float_cache = pickle.load(f)
        logger.debug(f"Loaded float cache, entries: {len(float_cache)}")
    else:
        float_cache = {}
        logger.debug("No float cache found, starting new")
    if os.path.exists("float_cache_none.pkl"):
        with open("float_cache_none.pkl", "rb") as f:
            float_cache_none_retry = pickle.load(f)
        logger.debug(
            f"Loaded float_cache_none_retry, entries: {len(float_cache_none_retry)}"
        )
    else:
        float_cache_none_retry = {}
        logger.debug("No float_cache_none_retry found, starting new")


async def get_float_shares(ticker):
    """Non-blocking float share lookup using thread executor for yfinance.
    
    Runs blocking yfinance call in thread pool to prevent event loop stalling.
    Uses debounced cache saving to reduce disk I/O overhead.
    """
    now = datetime.now(timezone.utc)
    # Check positive/real float first
    if ticker in float_cache and float_cache[ticker] is not None:
        return float_cache[ticker]
    # Check negative/None cache, only retry every N minutes
    if ticker in float_cache_none_retry:
        last_none = float_cache_none_retry[ticker]
        if (now - last_none).total_seconds() < FLOAT_CACHE_NONE_RETRY_MIN * 60:
            return None
    
    # Run blocking yfinance call in thread executor to avoid blocking event loop
    def _fetch_float():
        try:
            import yfinance as yf
            info = yf.Ticker(ticker).info
            return info.get('floatShares', None)
        except Exception as e:
            return None, e
    
    try:
        loop = asyncio.get_running_loop()
        result = await loop.run_in_executor(None, _fetch_float)
        
        # Handle result
        if isinstance(result, tuple):  # Error case
            float_shares, error = result
            float_cache_none_retry[ticker] = now
            await save_float_cache()  # Debounced, thread-safe
            if error and "Rate limited" in str(error):
                await asyncio.sleep(5)  # Backoff for rate limits
            return float_cache.get(ticker, None)
        else:
            float_shares = result
            if float_shares is not None:
                float_cache[ticker] = float_shares
                await save_float_cache()  # Debounced, thread-safe
                if ticker in float_cache_none_retry:
                    del float_cache_none_retry[ticker]
            else:
                float_cache_none_retry[ticker] = now
                await save_float_cache()  # Debounced, thread-safe
            # Removed unnecessary 0.5s sleep - slows down float lookups
            return float_shares
    except Exception as e:
        float_cache_none_retry[ticker] = now
        await save_float_cache()  # Debounced, thread-safe
        return float_cache.get(ticker, None)


load_float_cache()
# --- END FLOAT PATCH ---

# Filtering counters for debugging
filter_counts = defaultdict(int)


def is_eligible(symbol, last_price, float_shares, use_entry_price=False):
    """Check if symbol meets filtering criteria: price <= $15 AND float <= 20M
    
    Args:
        use_entry_price: REMOVED - Always uses current price (no grandfathering)
    """
    # 🚫 WARRANT FILTER: Block all warrants immediately
    if is_warrant(symbol):
        filter_counts["warrant"] = filter_counts.get("warrant", 0) + 1
        if filter_counts["warrant"] % 50 == 1:
            logger.info(f"[FILTER DEBUG] {symbol} filtered: Warrant (count: {filter_counts['warrant']})")
        return False
    
    # 🚫 GRANDFATHERING REMOVED: Always use current price to prevent alerts on $15+ stocks
    check_price = last_price
    
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
                f"[FILTER DEBUG] {symbol} filtered: price ${check_price:.2f} > ${PRICE_THRESHOLD} "
                f"(count: {filter_counts['price_too_high']})"
            )
        return False

    # 🚫 ETF FILTER: Block known ETFs and leveraged products (explicit list only)
    # Removed suffix pattern (L/S/X) to prevent false positives on real stocks like ASNS
    common_etfs = {
        'SPY', 'QQQ', 'DIA', 'IWM', 'VTI', 'VOO', 'VEA', 'VWO', 'AGG', 'BND',
        'GLD', 'SLV', 'USO', 'UNG', 'TLT', 'HYG', 'LQD', 'EEM', 'FXI', 'EWJ',
        'TQQQ', 'SQQQ', 'SOXL', 'SOXS', 'SPXL', 'SPXS', 'TECL', 'TECS',
        'JDST', 'JNUG', 'NUGT', 'DUST', 'ZSL', 'GLL', 'AGQ', 'UGLD', 'DGLD', 'DZZ',  # Gold/commodity ETFs
        'LABU', 'LABD', 'YINN', 'YANG', 'FAS', 'FAZ', 'TNA', 'TZA',
        'MSTR', 'MSTU', 'MSTZ', 'IONZ', 'IONQ',  # Crypto/quantum ETF-like products
        'TSLY', 'CONY', 'NVDY', 'MSTY', 'AIYY', 'YMAX', 'GOOY', 'TSMY',  # YieldMax income/covered call ETFs
        'SLE', 'SMD', 'AMD3', 'SKY', 'SND',  # GraniteShares leveraged products
        'VXX', 'UVXY', 'UVIX', 'VIXY', 'SVXY', 'TVIX'  # Volatility products
    }
    
    # Block only if symbol is in explicit ETF list
    if symbol in common_etfs:
        filter_counts["common_etf"] = filter_counts.get("common_etf", 0) + 1
        if filter_counts["common_etf"] % 50 == 1:
            logger.info(
                f"[FILTER DEBUG] {symbol} filtered: Known ETF/structured product (count: {filter_counts['common_etf']})"
            )
        return False

    # Float check - exception symbols bypass ONLY float filter
    if symbol not in FLOAT_EXCEPTION_SYMBOLS:
        # FAIL-OPEN: If float unknown, ALLOW symbol (only reject when we KNOW float is too high)
        if float_shares is None:
            filter_counts["float_none"] += 1
            if filter_counts["float_none"] % 100 == 1:
                logger.info(
                    f"[FLOAT UNKNOWN] {symbol} - no float data available, ALLOWING symbol to process (count: {filter_counts['float_none']})"
                )
            # ALLOW symbol through when float data unavailable
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
        float_str = f"{float_shares/1e6:.1f}M" if float_shares is not None else "Unknown"
        logger.info(
            f"[FILTER EXCEPTION] {symbol} bypassing float filter (float: {float_str})"
        )

    # Symbol passed all filters
    filter_counts["passed"] += 1
    if symbol in ["OCTO", "GRND", "EQS"] or filter_counts["passed"] % 100 == 1:
        float_str = f"{float_shares/1e6:.1f}M" if float_shares is not None else "Unknown"
        logger.info(
            f"[FILTER PASS] {symbol} eligible: price=${last_price:.2f}, float={float_str} (total passed: {filter_counts['passed']})"
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

    async def _save_profile(self):
        serializable = {
            sym: {
                str(minidx): vols
                for minidx, vols in by_min.items()
            }
            for sym, by_min in self.profile.items()
        }
        
        # 🚀 PERFORMANCE: JSON write in thread executor to prevent blocking
        def write_json():
            with open(PROFILE_FILE, "w") as f:
                json.dump(serializable, f)
        
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(io_executor, write_json)

    async def add_day(self, symbol, daily_candles):
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
        await self._save_profile()

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
            logger.info("[BACKFILL] Outside market hours, skipping backfill")
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

        logger.info(
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
                            logger.info(
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
                logger.error(f"[BACKFILL ERROR] {symbol}: {e}")

        logger.info("[BACKFILL] Completed catch-up analysis")

    except Exception as e:
        logger.error(f"[BACKFILL ERROR] General error: {e}")


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
        logger.debug("[VWAP DEBUG] No candles, returning None")
        return None

    try:
        # 🚨 FIX: Filter out zero-volume candles BEFORE calculation
        prices = []
        volumes = []
        for i, candle in enumerate(candles):
            # Validate candle structure
            required_fields = ['high', 'low', 'close', 'volume']
            for field in required_fields:
                if field not in candle:
                    raise ValueError(
                        f"Candle {i} missing required field: {field}")

            # 🚨 FIX: Skip zero/negative volume candles to prevent ValueError
            if candle['volume'] <= 0:
                logger.debug(f"[VWAP SKIP] Candle {i} has zero/negative volume: {candle['volume']}")
                continue

            # Calculate typical price (HLC/3)
            typical_price = (candle['high'] + candle['low'] +
                             candle['close']) / 3
            prices.append(typical_price)
            volumes.append(candle['volume'])

        # 🚨 FIX: Check if we have any valid candles after filtering
        if not prices or not volumes:
            logger.debug("[VWAP DEBUG] No non-zero volume candles available, returning None")
            return None

        # 🚨 PERFORMANCE: Reduced logging - these fire thousands of times per second
        logger.debug(f"[VWAP DEBUG] Prices used: {prices}")
        logger.debug(f"[VWAP DEBUG] Volumes used: {volumes}")

        # Use enhanced VWAP calculation
        vwap_val = vwap_numpy(prices, volumes)
        logger.debug(f"[VWAP DEBUG] VWAP result: {vwap_val}")
        return vwap_val

    except Exception as e:
        logger.error(
            f"[VWAP ERROR] Failed to calculate VWAP from candles: {e}")
        return None  # Return None on error so alerts are blocked until valid VWAP


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
    # Treat zero, negative, infinite or unreasonably large VWAP as "no VWAP"
    if vwap_val is None or (not np.isfinite(vwap_val)) or vwap_val <= 0 or vwap_val > 10000:
        logger.error(f"[VWAP ERROR] {symbol} - Unreasonable or missing VWAP value: {vwap_val}")
        return None

    # Verify VWAP is within reasonable range of current prices
    # 🚨 FIX: Convert deque to list before slicing to prevent "sequence index must be integer, not 'slice'" error
    candles_list = list(candles) if isinstance(candles, deque) else candles
    recent_prices = [candle['close'] for candle in candles_list[-3:]]
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
RVOL_SPIKE_THRESHOLD = 2.2  # Balanced threshold for early detection without noise
RVOL_SPIKE_MIN_VOLUME = 25000  # REDUCED: Lower volume for early spikes
AFTERHOURS_MIN_VOLUME = 100000  # STRICT: Require 100k+ volume for after-hours alerts to filter illiquid garbage

# 🚀 MEMORY MANAGEMENT: LRU tracking for symbol eviction
symbol_last_access = {}  # symbol -> last access timestamp

MIN_FLOAT_SHARES = 500_000
MAX_FLOAT_SHARES = 20_000_000  # UPDATED: Increased from 10M to 20M

# 🚨 ADAPTIVE VOLUME THRESHOLDS FOR MICRO-FLOAT STOCKS
def get_min_volume_for_float(float_shares):
    """Return adaptive volume threshold based on float size - catches ULY-type micro-float runners"""
    if float_shares is None:
        return 7_500  # FIXED: Treat unknown float as micro-float for max detection sensitivity
    elif float_shares < 2_000_000:  # FIXED: Micro-float threshold at 2M (was 3M)
        return 7_500  # 🚨 MUCH LOWER for micro-floats - catches 8-18k/min spikes
    else:  # Regular float (2M-20M)
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
    # 🚨 FIX: Guard against None or invalid prices
    if entry_price is None or entry_price <= 0 or current_price is None:
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

            # Train new model (CPU intensive - keep in main thread)
            new_model = RandomForestClassifier(n_estimators=100,
                                               random_state=42)
            new_model.fit(X, y)

            # 🚀 PERFORMANCE: Save model in thread executor to prevent blocking
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(io_executor, joblib.dump, new_model, "runner_model_updated.joblib")

            # Update global model
            global runner_clf
            runner_clf = new_model

            logger.info(
                f"[ML UPDATE] Model retrained with {len(X)} samples, "
                f"{y.sum()} successes ({y.mean()*100:.1f}% success rate)")

            # Archive old outcome data to prevent memory issues
            if len(outcome_df) > 1000:
                archive_file = f"outcome_archive_{datetime.now().strftime('%Y%m%d')}.csv"
                
                # 🚀 PERFORMANCE: CSV writes in thread executor to prevent blocking
                def write_csvs():
                    outcome_df.iloc[:-200].to_csv(archive_file, index=False)
                    outcome_df.iloc[-200:].to_csv(OUTCOME_LOG_FILE, index=False)
                
                await loop.run_in_executor(io_executor, write_csvs)
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
        "perfect_setup", "runner", "volume_spike", "ema_stack", "warming_up"
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


def fmt_price(p):
    """Safe price formatter with proper decimals for penny stocks"""
    if p is None:
        return "N/A"
    elif p < 1.0:
        return f"{p:.4f}"  # 4 decimals for penny stocks (shows $0.3398 instead of $0.34)
    elif p < 10.0:
        return f"{p:.2f}"  # 2 decimals for $1-$10 stocks
    else:
        return f"{p:.2f}"  # 2 decimals for $10+ stocks


CANDLE_MAXLEN = 30

candles = defaultdict(lambda: deque(maxlen=CANDLE_MAXLEN))
trade_candle_builders = defaultdict(list)
local_candle_builder = {}  # Track 1-min candle aggregation from trades when Polygon doesn't provide AM events
trade_candle_last_time = {}
last_alerted_price = {}
last_halt_alert = {}

last_volume_spike_time = defaultdict(
    lambda: datetime.min.replace(tzinfo=timezone.utc))
last_runner_alert_time = defaultdict(
    lambda: datetime.min.replace(tzinfo=timezone.utc))

pending_runner_alert = {}
HALT_LOG_FILE = "halt_event_log.csv"

alerted_symbols = {}
runner_alerted_today = {}  # Dict to store alert timestamp per symbol
below_vwap_streak = defaultdict(int)
vwap_reclaimed_once = defaultdict(bool)
# Dip play alerts removed per user request
recent_high = defaultdict(float)

volume_spike_alerted = set()
rvol_spike_alerted = set()
halted_symbols = set()
halt_last_alert_time = {}  # Track last halt alert time per symbol to prevent spam on reconnects

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
last_alert_sent_time = defaultdict(lambda: None)  # Track last alert time to prevent duplicates


async def send_best_alert(symbol):
    """Send only the highest scoring alert for a symbol"""
    if symbol not in pending_alerts or not pending_alerts[symbol]:
        return

    # 🚨 DUPLICATE PREVENTION: Check if we already sent an alert for this symbol recently
    now = datetime.now(timezone.utc)
    last_sent = last_alert_sent_time.get(symbol)
    if last_sent and (now - last_sent).total_seconds() < 60:  # 60 second cooldown
        time_since = int((now - last_sent).total_seconds())
        logger.info(f"[ALERT COOLDOWN] {symbol} - Already alerted {time_since}s ago, skipping duplicate")
        pending_alerts[symbol].clear()
        return

    # Find highest scoring alert
    best_alert = max(pending_alerts[symbol], key=lambda x: x['score'])

    # Only send if score is high enough (LOWERED for faster alerts)
    if best_alert['score'] >= 20:  # 🚨 LOWERED from 30 to 20 - catch more valid setups
        await send_all_alerts(best_alert['message'])
        last_alert_sent_time[symbol] = now  # 🚨 Record alert time
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
vwap_reclaim_was_true = defaultdict(bool)
volume_spike_was_true = defaultdict(bool)
ema_stack_was_true = defaultdict(bool)


def get_scanned_tickers():
    return set(candles.keys())


vwap_candles = defaultdict(lambda: deque(maxlen=CANDLE_MAXLEN))
vwap_session_date = defaultdict(lambda: None)
vwap_reset_done = defaultdict(bool)  # Track if VWAP reset at 9:30 AM for each symbol


def get_session_date(dt):
    ny = pytz.timezone("America/New_York")
    dt_ny = dt.astimezone(ny)
    if dt_ny.time() < dt_time(4, 0):
        return dt_ny.date() - timedelta(days=1)
    return dt_ny.date()


def check_volume_spike(candles_seq, vwap_value, float_shares=None):
    """Sustained volume spike detection - requires 2 consecutive green candles
    
    🚨 NO VWAP REQUIREMENT - Catches volume spikes regardless of VWAP position
    🎯 SUSTAINED MOMENTUM - Prevents alerts on single-candle spikes that fail immediately
    
    Args:
        float_shares: Float size for adaptive thresholds - catches micro-float spikes like ULY
    """
    # 🚨 FIX: Convert to list immediately to prevent slice errors
    if isinstance(candles_seq, deque):
        candles_list = list(candles_seq)
    else:
        # If not a deque, try to convert or return early
        try:
            candles_list = list(candles_seq)
        except:
            return False, {}
    
    if len(candles_list) < 5:  # Need 5 candles (3 trailing + 2 consecutive green)
        return False, {}

    curr_candle = candles_list[-1]
    prev_candle = candles_list[-2]
    curr_volume = curr_candle['volume']
    symbol = curr_candle.get('symbol', '?')

    # ADAPTIVE: Use 3 trailing candles for RVOL baseline (before the move)
    trailing_volumes = [c['volume'] for c in candles_list[-5:-2]]
    trailing_avg = sum(trailing_volumes) / 3 if len(
        trailing_volumes) == 3 else 1
    rvol = curr_volume / trailing_avg if trailing_avg > 0 else 0
    
    # Track VWAP position for logging (optional - not required for alert)
    current_price_spike = last_trade_price.get(symbol)
    above_vwap = (current_price_spike is not None and vwap_value is not None and current_price_spike > vwap_value)

    # 🚨 ADAPTIVE volume threshold - catches ULY-type micro-float spikes (8-18k shares/min)
    min_volume = get_min_volume_for_float(float_shares)  # 7.5k for micro-floats, 25k for regular

    # 🔥 SIMPLIFIED: Volume spike must come with RISING price
    price_momentum = (curr_candle['close'] - curr_candle['open']
                      ) / curr_candle['open'] if curr_candle['open'] > 0 else 0
    prev_momentum = (curr_candle['close'] - prev_candle['close']
                     ) / prev_candle['close'] if prev_candle['close'] > 0 else 0
    
    # 🚀 SUSTAINED MOMENTUM: Require 2 consecutive green candles
    # This prevents alerting on single-candle spikes that fail immediately (MAMK issue)
    prev_green = prev_candle['close'] > prev_candle['open']
    curr_green = curr_candle['close'] > curr_candle['open'] and price_momentum >= 0.005
    
    # Require BOTH current AND previous candle to be green (2 consecutive)
    sustained_momentum = curr_green and prev_green
    
    # Price must still be rising overall
    is_green_candle = curr_candle['close'] > curr_candle['open'] and price_momentum >= 0.005  # 0.5%+ for early detection
    rising_from_prev = curr_candle['close'] > prev_candle['close'] and prev_momentum >= 0.003  # 0.3%+ from previous
    
    # 🚨 TIERED PRICE MOVEMENT FILTER: Require meaningful price moves across all price ranges
    # Prevents false alerts on noise (IBIO +0.09%, TOVX +0.7¢) while catching real early momentum
    curr_price = curr_candle['close']
    absolute_move = curr_candle['close'] - prev_candle['close']
    percent_move = abs(price_momentum)
    
    # Tiered thresholds: higher absolute $ move required as price increases
    if curr_price < 1.0:
        # Under $1: Require at least 2 cents (≈2%-8% depending on price)
        min_abs_move = 0.02
        min_pct_move = 0.0  # Absolute move is the gate
        threshold_met = absolute_move >= min_abs_move
    elif curr_price < 5.0:
        # $1-$5: Require max(0.75%, $0.03) to filter IBIO-style noise
        min_abs_move = 0.03
        min_pct_move = 0.0075  # 0.75%
        threshold_met = absolute_move >= min_abs_move or percent_move >= min_pct_move
    elif curr_price < 10.0:
        # $5-$10: Require max(0.5%, $0.05) for early detection
        min_abs_move = 0.05
        min_pct_move = 0.005  # 0.5%
        threshold_met = absolute_move >= min_abs_move or percent_move >= min_pct_move
    else:
        # >$10: Require max(0.35%, $0.10) to catch established movers
        min_abs_move = 0.10
        min_pct_move = 0.0035  # 0.35%
        threshold_met = absolute_move >= min_abs_move or percent_move >= min_pct_move
    
    # Only pass if sustained momentum AND base momentum checks AND tiered threshold are met
    bullish_momentum = sustained_momentum and is_green_candle and rising_from_prev and threshold_met

    symbol = curr_candle.get('symbol', '?')

    logger.info(
        f"[VOLUME SPIKE CHECK] {symbol} | "
        f"Vol={curr_volume}, RVOL={rvol:.2f}, "
        f"Sustained={sustained_momentum} (2 consec green: prev={prev_green}, curr={curr_green}), "
        f"Green={is_green_candle} ({price_momentum*100:.1f}%), "
        f"Rising={rising_from_prev}, Momentum={bullish_momentum}, "
        f"Above VWAP={above_vwap} (info only)")

    # Check each condition - VWAP REMOVED
    vol_ok = curr_volume >= min_volume
    rvol_ok = rvol >= RVOL_SPIKE_THRESHOLD  # 2.2x threshold for early detection
    momentum_ok = bullish_momentum
    
    # 🚨 AFTER-HOURS LIQUIDITY FILTER: Require 100k+ volume to prevent illiquid garbage alerts
    session_type = get_session_type(datetime.now(timezone.utc))
    if session_type in ["premarket", "afterhours"]:
        afterhours_vol_ok = curr_volume >= AFTERHOURS_MIN_VOLUME
        if not afterhours_vol_ok:
            logger.info(f"[AFTERHOURS FILTER] {symbol} - Volume {curr_volume} < {AFTERHOURS_MIN_VOLUME} (session: {session_type}) - BLOCKING alert")
            return False, {}
        logger.info(f"[AFTERHOURS PASS] {symbol} - Volume {curr_volume} >= {AFTERHOURS_MIN_VOLUME} (session: {session_type})")

    # 🚨 VOLUME SPIKE = Volume + RVOL + Momentum ONLY (no VWAP requirement)
    spike_detected = vol_ok and rvol_ok and momentum_ok

    # Log detailed debug info
    logger.info(
        f"[SPIKE DEBUG] {symbol} | "
        f"vol_ok={vol_ok} ({curr_volume}>={min_volume}), "
        f"rvol_ok={rvol_ok} ({rvol:.2f}>={RVOL_SPIKE_THRESHOLD:.1f}), "
        f"momentum_ok={momentum_ok}, "
        f"FINAL: spike_detected={spike_detected} (VWAP not required)")

    # Return both result and data for scoring
    data = {
        'rvol': rvol,
        'volume': curr_volume,
        'price_move': price_momentum,
        'above_vwap': above_vwap  # Still tracked for info
    }

    return spike_detected, data


current_session_date = None


def get_ny_date():
    ny = pytz.timezone("America/New_York")
    now_utc = datetime.now(timezone.utc)
    now_ny = now_utc.astimezone(ny)
    return now_ny.date()


# 🚀 FRESHNESS THRESHOLDS: Consolidated timestamp validation constants
MAX_PRICE_AGE_SECONDS = 30  # RELAXED: Catch big movers even with data delays (was 5s, too strict)
MAX_TRADE_AGE_SECONDS = 15  # Tolerates network jitter/lag - prevents missing alerts due to delayed trades


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


def validate_price_above_vwap_strict(symbol, vwap_value, alert_type, max_age_seconds=MAX_PRICE_AGE_SECONDS):
    """🚨 STRICT VWAP VALIDATION - Never allows alerts under VWAP
    
    Checks ONLY real-time trade price (no candle fallback) against VWAP.
    Blocks alerts if:
    - Real-time price is stale (>max_age_seconds old)
    - Real-time price is below or equal to VWAP
    - No real-time price available
    
    This prevents the bug where candle close above VWAP but current price below VWAP.
    
    Returns: (is_valid, price, reason)
        - is_valid: True if real-time price is fresh and above VWAP
        - price: The real-time price (or None)
        - reason: Log message explaining why validation failed (or success)
    """
    real_time_price = last_trade_price.get(symbol)
    real_time_timestamp = last_trade_time.get(symbol)
    now = datetime.now(timezone.utc)
    
    # Check if we have real-time price data
    if real_time_price is None or real_time_timestamp is None:
        return (False, None, f"[{alert_type.upper()} BLOCKED] {symbol} - No real-time price available for VWAP validation")
    
    # If VWAP is not available, block strict VWAP validation — we require VWAP for this guard
    if vwap_value is None:
        return (False, real_time_price, f"[{alert_type.upper()} BLOCKED] {symbol} - VWAP data unavailable for strict VWAP validation")

    # Check freshness - MUST be within max age
    price_age = (now - real_time_timestamp).total_seconds()
    if price_age > max_age_seconds:
        return (False, real_time_price, f"[{alert_type.upper()} BLOCKED] {symbol} - Real-time price is stale ({price_age:.1f}s old)")
    
    # Check if real-time price is above VWAP
    if real_time_price <= vwap_value:
        return (False, real_time_price, f"[{alert_type.upper()} BLOCKED] {symbol} - Real-time price ${real_time_price:.2f} NOT above VWAP ${vwap_value:.2f}")
    
    # All checks passed
    return (True, real_time_price, f"[{alert_type.upper()} VWAP OK] {symbol} - Real-time price ${real_time_price:.2f} above VWAP ${vwap_value:.2f} (age: {price_age:.1f}s)")



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
            runner_was_true, vwap_reclaim_was_true,
            volume_spike_was_true, ema_stack_was_true, premarket_open_prices,
            premarket_last_prices, premarket_volumes, alerted_symbols,
            runner_alerted_today, below_vwap_streak, vwap_reclaimed_once,
            volume_spike_alerted, rvol_spike_alerted,
            halted_symbols, pending_runner_alert
    ]:
        if hasattr(d, "clear"):
            d.clear()
    # EMAs are NOT reset here - they persist throughout the trading session and only reset at 04:00 ET
    logger.info("Cleared all per-symbol session state for new trading day!")


async def alert_perfect_setup(symbol, closes, volumes, highs, lows,
                              candles_seq, vwap_value):
    # 🚨 FIX: Convert to list if it's a deque to prevent slice errors
    if isinstance(candles_seq, deque):
        candles_list = list(candles_seq)
    else:
        candles_list = candles_seq if isinstance(candles_seq, list) else list(candles_seq)
    
    # Removed verbose debug logging for performance - use logger.debug if needed
    if candles_list and len(candles_list) >= 6:
        last_6 = candles_list[-6:]
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

        # Debug logging only when needed (set to DEBUG level)
        logger.debug(
            f"{symbol} Runner: vol={volume_rn}, avg5={avg_vol_5:.0f}, volPass={volume_rn >= 2.0 * avg_vol_5}, "
            f"priceMove={price_move_rn:.3f}, movePass={price_move_rn >= 0.03}, "
            f"close={close_rn}, high={high_rn}, wickPass={close_rn >= 0.65 * high_rn}, "
            f"volIncreasing={volume_increasing}, priceTrend={price_rising_trend}"
        )

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

        # 🚨 FIX: Use FRESHEST available price (real-time trade vs candle close)
        candle_time_ps = candles_seq[-1]['start_time'] + timedelta(minutes=1)
        alert_price = get_display_price(symbol, last_close, candle_time_ps)
        alert_text = (
            f"🚨 <b>PERFECT SETUP</b> 🚨\n"
            f"<b>{escape_html(symbol)}</b> | ${fmt_price(alert_price)} | Vol: {int(last_volume/1000)}K | RVOL: {rvol:.1f}\n\n"
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


def enforce_symbol_limit(symbol):
    """🚀 MEMORY MANAGEMENT: Enforce MAX_SYMBOLS limit with LRU eviction
    
    Updates access time for symbol and evicts least recently used symbol if at limit.
    This prevents unbounded memory growth from tracking thousands of symbols.
    """
    global symbol_last_access
    
    # Update access time for current symbol
    symbol_last_access[symbol] = datetime.now(timezone.utc)
    
    # Check if we're at the limit
    if len(symbol_last_access) > MAX_SYMBOLS:
        # Find least recently used symbol
        lru_symbol = min(symbol_last_access.items(), key=lambda x: x[1])[0]
        
        # Evict from all data structures
        symbol_last_access.pop(lru_symbol, None)
        candles.pop(lru_symbol, None)
        vwap_candles.pop(lru_symbol, None)
        vwap_cum_vol.pop(lru_symbol, None)
        vwap_cum_pv.pop(lru_symbol, None)
        vwap_session_date.pop(lru_symbol, None)
        vwap_reset_done.pop(lru_symbol, None)
        stored_emas.pop(lru_symbol, None)
        last_trade_price.pop(lru_symbol, None)
        last_trade_volume.pop(lru_symbol, None)
        last_trade_time.pop(lru_symbol, None)
        entry_price.pop(lru_symbol, None)
        rvol_history.pop(lru_symbol, None)
        local_candle_builder.pop(lru_symbol, None)
        ema_stack_was_true.pop(lru_symbol, None)
        runner_alerted_today.pop(lru_symbol, None)
        
        logger.debug(f"[LRU EVICT] Removed {lru_symbol} to maintain MAX_SYMBOLS={MAX_SYMBOLS} limit (now tracking {len(symbol_last_access)} symbols)")


async def on_new_candle(symbol, open_, high, low, close, volume, start_time):
    global current_session_date
    today_ny = get_ny_date()
    now = datetime.now(timezone.utc)
    eastern = pytz.timezone("America/New_York")
    now_et = now.astimezone(eastern)
    
    # 🚀 MEMORY MANAGEMENT: Enforce symbol limit with LRU eviction
    enforce_symbol_limit(symbol)

    # Only reset state once per day at 04:00 ET (not on every calendar day change)
    if current_session_date != today_ny:
        current_session_date = today_ny
        alerted_symbols.clear()
        runner_alerted_today.clear()
        recent_high.clear()
        halted_symbols.clear()
        logger.info(f"Reset alert state for new trading day: {today_ny}")

    # Reset EMAs only once per day at 04:00 ET for new trading session
    global last_ema_reset_date
    if (now_et.hour == 4 and now_et.minute == 0
            and current_session_date == today_ny
            and last_ema_reset_date != today_ny):
        stored_emas.clear()
        last_ema_reset_date = today_ny
        logger.info(
            f"[EMA RESET] Reset stored EMAs for new trading session at 04:00 ET: {today_ny}"
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
    # 🚨 FIX: Keep vwap_candles as deque to prevent memory leak
    if not isinstance(vwap_candles[symbol], deque):
        vwap_candles[symbol] = deque(vwap_candles[symbol], maxlen=CANDLE_MAXLEN)
    float_shares = await get_float_shares(symbol)
    
    # 🚨 CRITICAL FIX: Check ALL eligibility criteria (price, float, AND ETF filter)
    # This was the bug allowing DUST, ZSL, GLL alerts - only checking float, not ETFs!
    if not is_eligible(symbol, close, float_shares, use_entry_price=True):
        logger.debug(f"[FILTER BLOCKED] {symbol} - Failed is_eligible check (ETF or criteria mismatch)")
        return

    # Log exception processing
    if symbol in FLOAT_EXCEPTION_SYMBOLS:
        logger.info(
            f"[FLOAT EXCEPTION] {symbol} bypassing float filter (actual: {float_shares})"
        )

    # OCTO-specific debug for successful processing
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
    
    # 🚨 REMOVED DUPLICATE: candles already appended before calling on_new_candle()
    # Local candle path appends at line 2816, Polygon path appends at line 2899
    # DO NOT append here or we get duplicate candles corrupting EMAs and alerts!

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
    
    # 🚨 REMOVED DUPLICATE: vwap_candles already appended before calling on_new_candle()
    # Local candle path appends at line 2840, Polygon path appends at line 2939
    # DO NOT append here or we get duplicate candles corrupting VWAP!

    # Removed verbose debug logging for performance - only log critical events

    # --- PERFECT SETUP SCANNER ---
    # 🚨 FIX: Convert deque to list once to prevent slice errors
    candles_list = list(candles_seq)
    if len(candles_list) >= 30:
        closes = [c['close'] for c in candles_list[-30:]]
        highs = [c['high'] for c in candles_list[-30:]]
        lows = [c['low'] for c in candles_list[-30:]]
        volumes = [c['volume'] for c in candles_list[-30:]]
        # 🚨 CRITICAL FIX: Use centralized VWAP guard for perfect setup
        vwap_value = get_valid_vwap(symbol)
        if vwap_value is None:
            logger.info(
                f"[VWAP GUARD] {symbol} - Blocking perfect setup alert - insufficient VWAP data"
            )
            return  # Block perfect setup alerts without valid VWAP
        if not ema_stack_was_true[symbol]:
            await alert_perfect_setup(symbol, closes, volumes, highs, lows,
                                      candles_list[-30:], vwap_value)

    # --- Warming Up Logic with STRICT MOMENTUM REQUIREMENTS ---
    # GITHUB FIX: Require exactly 6 candles (5 prior + current) for accurate avg_vol_5
    if len(candles_list) >= 6:
        last_6 = candles_list[-6:]
        prev_5 = last_6[:-1]  # The 5 candles before current
        last_candle = last_6[-1]
        
        # Compute avg_vol_5 from EXACTLY 5 prior candles
        volumes_prev_5 = [c["volume"] for c in prev_5]
        avg_vol_5 = sum(volumes_prev_5) / 5.0  # Always 5 candles
        
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
        # GITHUB FIX: Use real-time price for dollar volume (matches VWAP requirement)
        dollar_volume_wu = current_price_wu * volume_wu if current_price_wu else close_wu * volume_wu
        
        # 🚀 EARLY RUNNER DETECTION - CATCH BEFORE THEY TAKE OFF!
        closes_wu = [c['close'] for c in last_6[-3:]]
        volumes_wu = [c['volume'] for c in last_6[-3:]]
        
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
        # 🚨 FIX: Don't fire Warming Up if Runner already alerted (progression hierarchy)
        if warming_up_criteria and not warming_up_was_true[symbol] and not runner_was_true[symbol]:
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
            
            # Verify price data is FRESH (≤30 seconds old)
            price_age_wu = (now_fresh_wu - real_time_timestamp_wu).total_seconds()
            if price_age_wu > MAX_PRICE_AGE_SECONDS:
                logger.info(f"[WARMING UP BLOCKED] {symbol} - Real-time price is stale ({price_age_wu:.1f}s old, limit: {MAX_PRICE_AGE_SECONDS}s)")
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
            price_str = fmt_price(alert_price)
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
    if len(candles_list) >= 6:
        last_6 = candles_list[-6:]
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
            if (now - last_alert_time[symbol]['runner']) < timedelta(
                    minutes=ALERT_COOLDOWN_MINUTES):
                return
            
            # 🚨 STRICT VWAP VALIDATION - Checks ONLY real-time price (no candle fallback)
            vwap_valid, real_time_price_rn, vwap_reason = validate_price_above_vwap_strict(symbol, vwap_rn, "RUNNER")
            if not vwap_valid:
                logger.info(vwap_reason)
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
            price_str = fmt_price(alert_price)
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
        trailing_vols = [c['volume'] for c in candles_list[:-1]]
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
            
            # 🚨 FIX: Use FRESHEST available price (real-time trade vs candle close)
            candle_time_vr = curr_candle['start_time'] + timedelta(minutes=1)
            alert_price = get_display_price(symbol, curr_candle['close'], candle_time_vr)
            
            # Calculate alert score and add to pending alerts
            vwap_reclaim_data = {
                "rvol": rvol,
                "volume": curr_candle['volume'],
                "price_move": candle_price_move,
                "candle_price": curr_candle['close'],
                "real_time_price": last_trade_price.get(symbol)
            }
            score = get_alert_score("vwap_reclaim", symbol, vwap_reclaim_data)
            
            price_str = fmt_price(alert_price)
            vwap_str = fmt_price(curr_vwap) if curr_vwap is not None else "?"
            alert_text = (f"📈 <b>{escape_html(symbol)}</b> VWAP Reclaim!\n"
                          f"Price: ${price_str} | VWAP: ${vwap_str}")
            
            # Add to pending alerts instead of sending immediately
            pending_alerts[symbol].append({
                'type': 'vwap_reclaim',
                'score': score,
                'message': alert_text
            })
            
            # Send the best alert for this symbol
            await send_best_alert(symbol)
            
            # Log event for tracking
            log_event(
                "vwap_reclaim", symbol,
                alert_price,
                curr_candle['volume'], event_time, vwap_reclaim_data)
            
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
        if (now - last_alert_time[symbol]['volume_spike']) < timedelta(
                minutes=ALERT_COOLDOWN_MINUTES):
            logger.info(
                f"[COOLDOWN] {symbol}: Skipping volume_spike alert due to cooldown ({(now - last_alert_time[symbol]['volume_spike']).total_seconds():.0f}s ago)"
            )
            return
        
        # 🚨 STRICT VWAP VALIDATION - Final check with real-time price freshness
        vwap_valid, real_time_price_vs, vwap_reason = validate_price_above_vwap_strict(symbol, vwap_value, "VOLUME SPIKE")
        if not vwap_valid:
            logger.info(vwap_reason)
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
        price_str = fmt_price(alert_price)
        rvol_str = f"{spike_data['rvol']:.1f}"

        alert_text = (f"🔥 <b>{escape_html(symbol)}</b> Volume Spike\n"
                      f"Price: ${price_str}")

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

            # 🚨 STRICT VWAP VALIDATION - Checks ONLY real-time price (no candle fallback)
            vwap_valid, real_time_price_ema, vwap_reason = validate_price_above_vwap_strict(symbol, vwap_value, "EMA STACK")
            if not vwap_valid:
                logger.info(vwap_reason)
                return
            
            # Use validated real-time price for alert (guaranteed fresh and above VWAP)
            current_price_ema = real_time_price_ema
            
            # Additional VWAP sanity check - guard against None first
            if vwap_value is None or vwap_value <= 0:
                logger.debug(
                    f"[EMA STACK FILTERED] {symbol} - invalid VWAP {vwap_value}"
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
            # 🚨 CRITICAL: Guard vwap_value against None in multiplication
            base_ema_criteria = (
                ema5 > ema8 > ema13 and ema5 >= 1.011 * ema13
                and  # Keep original 1.1% spread - just ensure EMAs moving up
                (vwap_value is not None and current_price_ema >= 1.015 * vwap_value)
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

            # 🔥 MOMENTUM CONFIRMATION - removed strict volume streak requirement
            momentum_confirmed = False
            
            if base_ema_criteria:
                # Check for 2+ green candles in last 3 candles (realistic recent momentum check)
                if len(candles_list) >= 3:
                    recent_candles = candles_list[-3:]
                    green_candles = sum(1 for c in recent_candles
                                        if c['close'] > c['open'])
                    momentum_confirmed = green_candles >= 2

                # Alternative: Check if real-time price is up 3%+ from 5 candles ago (more realistic sustained move)
                if not momentum_confirmed and len(closes) >= 5:
                    price_5_ago = closes[-5]
                    price_gain = (current_price_ema -
                                  price_5_ago) / price_5_ago
                    momentum_confirmed = price_gain >= 0.03  # 3%+ gain from 5 candles ago

                # Fallback: If not enough history, allow alert if real-time price > EMA5 (basic confirmation)
                if not momentum_confirmed and len(closes) < 5:
                    momentum_confirmed = current_price_ema > ema5

            # Final criteria: base EMA criteria AND momentum confirmation (volume streak removed)
            ema_stack_criteria = base_ema_criteria and momentum_confirmed

            if ema_stack_criteria and not ema_stack_was_true[symbol]:
                # 🚨 CRITICAL: Guard against None vwap_value in calculation
                vwap_margin = ((current_price_ema - vwap_value) / vwap_value * 100) if vwap_value else 0
                ema13_margin = (current_price_ema - ema13) / ema13 * 100
                logger.info(
                    f"[EMA STACK ALERT] {symbol}: session={session_type}, ema5={ema5:.2f}, ema8={ema8:.2f}, ema13={ema13:.2f}{ema200_str}, vwap={vwap_value:.2f}, real_time={current_price_ema:.2f}, candle_close={candle_close:.2f}, volume={last_volume}, dollar_volume={dollar_volume:.2f}, ratio={ema5/ema13:.4f}, vwap_margin={vwap_margin:.1f}%, ema13_margin={ema13_margin:.1f}%"
                )
                if ema5 / ema13 < 1.011:
                    logger.error(
                        f"[BUG] EMA stack alert would have fired but ratio invalid! {symbol}: ema5={ema5:.4f}, ema13={ema13:.4f}, ratio={ema5/ema13:.4f} (should be >= 1.011)"
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
                
                # Verify price data is FRESH (≤30 seconds old)
                price_age_ema = (now_fresh_ema - real_time_timestamp_ema).total_seconds()
                if price_age_ema > MAX_PRICE_AGE_SECONDS:
                    logger.info(f"[EMA STACK BLOCKED] {symbol} - Real-time price is stale ({price_age_ema:.1f}s old)")
                    return
                
                # Verify real-time price is ACTUALLY above VWAP (not just from get_display_price fallback)
                if real_time_price_ema <= vwap_value:
                    logger.info(f"[EMA STACK BLOCKED] {symbol} - Real-time price ${real_time_price_ema:.2f} NOT above VWAP ${vwap_value:.2f}")
                    return
                # 🚨 FIX: Use real-time price if fresh, otherwise candle close with timestamp
                candle_time_ema = last_candle['start_time'] + timedelta(minutes=1)
                alert_price = get_display_price(symbol, candle_close, candle_time_ema)
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
                price_str = fmt_price(alert_price)
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


async def update_profile_for_day(symbol, day_candles):
    await vol_profile.add_day(symbol, day_candles)


def is_warrant(symbol):
    """Check if symbol is a warrant - filter these from gainers list"""
    warrant_suffixes = ['W', 'WS', 'WT', 'WW']
    # Check if symbol ends with warrant suffix (e.g., ABCW, XYZWS)
    for suffix in warrant_suffixes:
        if symbol.endswith(suffix) and len(symbol) > len(suffix):
            return True
    return False


def is_etf(symbol):
    """Check if symbol is a known ETF or leveraged product"""
    common_etfs = {
        'SPY', 'QQQ', 'DIA', 'IWM', 'VTI', 'VOO', 'VEA', 'VWO', 'AGG', 'BND',
        'GLD', 'SLV', 'USO', 'UNG', 'TLT', 'HYG', 'LQD', 'EEM', 'FXI', 'EWJ',
        'TQQQ', 'SQQQ', 'SOXL', 'SOXS', 'SPXL', 'SPXS', 'TECL', 'TECS',
        'JDST', 'JNUG', 'NUGT', 'DUST', 'ZSL', 'GLL', 'AGQ', 'UGLD', 'DGLD', 'DZZ',
        'LABU', 'LABD', 'YINN', 'YANG', 'FAS', 'FAZ', 'TNA', 'TZA',
        'MSTR', 'MSTU', 'MSTZ', 'IONZ', 'IONQ',
        'TSLY', 'CONY', 'NVDY', 'MSTY', 'AIYY', 'YMAX', 'GOOY', 'TSMY',
        'SLE', 'SMD', 'AMD3', 'SKY', 'SND',
        'VXX', 'UVXY', 'UVIX', 'VIXY', 'SVXY', 'TVIX'
    }
    return symbol in common_etfs


async def premarket_gainers_alert_loop():
    eastern = pytz.timezone("America/New_York")
    sent_today = False
    
    # ETF blocklist (same as main scanner)
    etf_blocklist = {
        'SPY', 'QQQ', 'DIA', 'IWM', 'VTI', 'VOO', 'VEA', 'VWO', 'AGG', 'BND',
        'GLD', 'SLV', 'USO', 'UNG', 'TLT', 'HYG', 'LQD', 'EEM', 'FXI', 'EWJ',
        'TQQQ', 'SQQQ', 'SOXL', 'SOXS', 'SPXL', 'SPXS', 'TECL', 'TECS',
        'JDST', 'JNUG', 'NUGT', 'DUST', 'ZSL', 'GLL', 'AGQ', 'UGLD', 'DGLD',
        'LABU', 'LABD', 'YINN', 'YANG', 'FAS', 'FAZ', 'TNA', 'TZA',
        'MSTR', 'MSTU', 'MSTZ', 'IONZ', 'IONQ',
        'TSLY', 'CONY', 'NVDY', 'MSTY', 'AIYY', 'YMAX', 'GOOY', 'TSMY',
        'SLE', 'SMD', 'AMD3', 'SKY', 'SND',
        'VXX', 'UVXY', 'UVIX', 'VIXY', 'SVXY', 'TVIX'
    }
    
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
                    # 🚫 Skip warrants (symbols ending in W, WS, WT, WW)
                    if is_warrant(sym):
                        logger.debug(f"[GAINERS FILTER] {sym} - Skipping warrant from gainers list")
                        continue
                    
                    # 🚫 Skip ETFs - must be real stocks only
                    if sym in etf_blocklist:
                        logger.debug(f"[GAINERS FILTER] {sym} - Skipping ETF from gainers list")
                        continue
                    
                    if sym in premarket_last_prices and premarket_open_prices[
                            sym] > 0:
                        pct_gain = (premarket_last_prices[sym] -
                                    premarket_open_prices[sym]
                                    ) / premarket_open_prices[sym] * 100
                        last_price = premarket_last_prices[sym]
                        total_vol = premarket_volumes.get(sym, 0)
                        # 🎯 STRICT FILTERS: Price ≤$15, Volume ≥50k, Gain ≥5% for quality list
                        if last_price <= 15 and total_vol >= 50000 and pct_gain >= 5.0:
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
            logger.info(
                "[SCAN PAUSED] Market closed (outside 4am-8pm EST, Mon-Fri). Sleeping 60s."
            )
            await asyncio.sleep(60)
            continue

        try:
            logger.info(
                f"[CONNECTION] Connecting to Polygon WebSocket... (attempt {connection_attempts + 1})"
            )
            async with websockets.connect(url,
                                          ping_interval=20,
                                          ping_timeout=60) as ws:
                logger.info(
                    "[CONNECTION] Successfully connected to Polygon WebSocket")

                # Perform catch-up backfill if this is a reconnection
                if connection_attempts > 0:
                    logger.info(
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
                logger.info(
                    "Subscribed to: AM.* (minute bars), T.* (trades), LULD.* (halt/volatility alerts) - FULL monitoring"
                )

                while True:
                    if not is_market_scan_time():
                        logger.info(
                            "[SCAN PAUSED] Market closed during active connection. Sleeping 60s, breaking websocket."
                        )
                        await asyncio.sleep(60)
                        break

                    msg = await ws.recv()
                    
                    # 🚨 FIX: Convert bytes to string if needed
                    if isinstance(msg, bytes):
                        msg = msg.decode('utf-8')
                    
                    # 🔇 Reduced logging: Only log in debug mode to prevent performance issues
                    logger.debug(f"[WS MSG] {msg[:200]}..." if len(msg) > 200 else f"[WS MSG] {msg}")

                    # Skip malformed or heartbeat messages
                    if not msg or len(msg.strip()) < 2 or not msg.strip().startswith('{') and not msg.strip().startswith('['):
                        logger.debug(f"[SKIP] Non-JSON message: '{msg[:100]}'")
                        continue

                    try:
                        data = json.loads(msg)
                        if isinstance(data, dict):
                            data = [data]
                        for event in data:
                            # Handle connection limit status messages FIRST
                            if event.get("ev") == "status" and event.get(
                                    "status") == "max_connections":
                                logger.warning(
                                    f"[🚨 CONNECTION LIMIT] Polygon API connection limit exceeded!"
                                )
                                logger.warning(
                                    f"[🚨 CONNECTION LIMIT] Message: {event.get('message', 'No details')}"
                                )
                                # Treat as throttling signal - trigger exponential backoff
                                raise ConnectionError(
                                    "max_connections_exceeded")

                            elif event.get("ev") == "T":
                                symbol = event["sym"]
                                price = event["p"]
                                size = event.get('s', 0)
                                trade_timestamp = event.get('t')  # 🚨 FIX: Don't default to 0
                                
                                # 🚨 FIX: Skip trades with missing timestamp (don't fake to epoch 1970)
                                if not trade_timestamp:
                                    logger.debug(f"[TRADE SKIP] {symbol} - Missing timestamp, cannot validate freshness")
                                    continue

                                # 🚀 PERFORMANCE: Fast eligibility checks FIRST (no I/O) before slow float lookup
                                
                                # 🎯 GRANDFATHERING: Use entry price for already-tracked stocks
                                is_tracked = symbol in entry_price and entry_price[symbol] is not None
                                check_price = entry_price[symbol] if is_tracked else price
                                
                                # ✅ FAST CHECK #1: Price filter (instant, no I/O) - reject 90% of stocks immediately
                                if check_price > 15:
                                    # If was tracked but now too expensive, clear tracking
                                    if is_tracked:
                                        logger.info(f"[GRANDFATHERING] {symbol} too expensive, removing from tracking (entry: ${entry_price[symbol]:.2f}, current: ${price:.2f})")
                                        entry_price[symbol] = None
                                    continue
                                
                                # ✅ FAST CHECK #2: ETF/Warrant filter (instant, no I/O)
                                if is_etf(symbol) or is_warrant(symbol):
                                    continue
                                
                                # ✅ SLOW CHECK #3: Float lookup (only for stocks ≤$15) - 90% reduction in HTTP calls
                                float_shares = await get_float_shares(symbol)
                                
                                # Final eligibility check with float data
                                if not is_eligible(symbol, price, float_shares, use_entry_price=is_tracked):
                                    # If not eligible and was being tracked, clear entry price
                                    if is_tracked:
                                        logger.info(f"[GRANDFATHERING] {symbol} no longer eligible (float), removing from tracking (entry: ${entry_price[symbol]:.2f}, current: ${price:.2f})")
                                        entry_price[symbol] = None
                                    continue  # Skip ineligible symbols completely
                                
                                # 🎯 NEW STOCK: Record entry price when first eligible
                                if not is_tracked:
                                    entry_price[symbol] = price
                                    logger.info(f"[GRANDFATHERING] {symbol} now eligible - locked entry price: ${price:.2f}")

                                # 🚨 FIX: Use Polygon's timestamp to reject stale trades
                                # Polygon timestamps are in nanoseconds, divide by 1,000,000 to get seconds
                                trade_time = datetime.fromtimestamp(trade_timestamp / 1_000_000, tz=timezone.utc)
                                now_utc = datetime.now(timezone.utc)
                                trade_age_seconds = (now_utc - trade_time).total_seconds()
                                
                                # Reject trades older than threshold (allows for network jitter)
                                if trade_age_seconds > MAX_TRADE_AGE_SECONDS:
                                    logger.debug(f"[STALE TRADE] {symbol} - Trade is {trade_age_seconds:.1f}s old, rejecting (limit: {MAX_TRADE_AGE_SECONDS}s)")
                                    continue
                                
                                last_trade_price[symbol] = price
                                last_trade_volume[symbol] = size
                                last_trade_time[symbol] = trade_time  # Use Polygon's timestamp, not now()
                                logger.debug(
                                    f"[TRADE EVENT] {symbol} | Price={price} | Size={size} | Time={trade_time} | Age={trade_age_seconds:.1f}s"
                                )
                                
                                # 🚨 LOCAL CANDLE AGGREGATION: Build 1-min candles from trades when Polygon doesn't provide AM events
                                # Track trades per minute for symbols missing Polygon candles
                                current_minute = trade_time.replace(second=0, microsecond=0)
                                
                                # Initialize tracking structures if needed
                                if symbol not in local_candle_builder:
                                    local_candle_builder[symbol] = {
                                        'minute': current_minute,
                                        'open': price,
                                        'high': price,
                                        'low': price,
                                        'close': price,
                                        'volume': size,
                                        'trade_count': 1
                                    }
                                elif local_candle_builder[symbol]['minute'] == current_minute:
                                    # Same minute - update OHLCV
                                    builder = local_candle_builder[symbol]
                                    builder['high'] = max(builder['high'], price)
                                    builder['low'] = min(builder['low'], price)
                                    builder['close'] = price
                                    builder['volume'] += size
                                    builder['trade_count'] += 1
                                else:
                                    # New minute - create candle from previous minute and start new builder
                                    prev_builder = local_candle_builder[symbol]
                                    prev_minute = prev_builder['minute']
                                    
                                    # Only create local candle if we had trades (min 1 for early detection)
                                    if prev_builder['trade_count'] >= 1:
                                        local_candle = {
                                            "open": prev_builder['open'],
                                            "high": prev_builder['high'],
                                            "low": prev_builder['low'],
                                            "close": prev_builder['close'],
                                            "volume": prev_builder['volume'],
                                            "start_time": prev_minute,
                                        }
                                        
                                        logger.debug(
                                            f"[LOCAL CANDLE] {symbol} {prev_minute} o:{local_candle['open']} h:{local_candle['high']} l:{local_candle['low']} c:{local_candle['close']} v:{local_candle['volume']} (trades:{prev_builder['trade_count']})"
                                        )
                                        
                                        # Process the local candle through normal logic
                                        if not isinstance(candles[symbol], deque):
                                            candles[symbol] = deque(candles[symbol], maxlen=20)
                                        # 🚨 FIX: Keep vwap_candles as deque to prevent memory leak
                                        if not isinstance(vwap_candles[symbol], deque):
                                            vwap_candles[symbol] = deque(vwap_candles[symbol], maxlen=CANDLE_MAXLEN)
                                        
                                        candles[symbol].append(local_candle)
                                        session_date = get_session_date(local_candle['start_time'])
                                        last_session = vwap_session_date[symbol]
                                        if last_session != session_date:
                                            vwap_candles[symbol] = deque(maxlen=CANDLE_MAXLEN)  # 🚨 FIX: Use deque not list
                                            vwap_session_date[symbol] = session_date
                                            vwap_reset_done[symbol] = False  # Reset flag for new trading day
                                        
                                        # Process VWAP reset at market open
                                        eastern = pytz.timezone("America/New_York")
                                        candle_time = prev_minute.astimezone(eastern).time()
                                        market_open_time = dt_time(9, 30)
                                        if candle_time >= market_open_time:
                                            if not vwap_reset_done[symbol]:
                                                vwap_candles[symbol] = deque(maxlen=CANDLE_MAXLEN)  # 🚨 FIX: Use deque not list
                                                vwap_cum_vol[symbol] = 0
                                                vwap_cum_pv[symbol] = 0
                                                vwap_reset_done[symbol] = True
                                                logger.info(f"[VWAP RESET] {symbol} - Cleared pre-market VWAP data at market open (9:30 AM)")
                                        
                                        vwap_candles[symbol].append(local_candle)
                                        
                                        # Trigger alert processing
                                        await on_new_candle(
                                            symbol,
                                            local_candle['open'],
                                            local_candle['high'],
                                            local_candle['low'],
                                            local_candle['close'],
                                            local_candle['volume'],
                                            prev_minute
                                        )
                                        
                                        # Send best alert if any pending
                                        if symbol in pending_alerts and pending_alerts[symbol]:
                                            await send_best_alert(symbol)
                                    
                                    # Start new minute builder
                                    local_candle_builder[symbol] = {
                                        'minute': current_minute,
                                        'open': price,
                                        'high': price,
                                        'low': price,
                                        'close': price,
                                        'volume': size,
                                        'trade_count': 1
                                    }
                            elif event.get("ev") == "AM":
                                symbol = event["sym"]
                                open_ = event["o"]
                                high = event["h"]
                                low = event["l"]
                                close = event["c"]
                                volume = event["v"]
                                start_time = polygon_time_to_utc(event["s"])

                                logger.debug(
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
                                # 🚨 FIX: Keep vwap_candles as deque to prevent memory leak
                                if not isinstance(vwap_candles[symbol], deque):
                                    vwap_candles[symbol] = deque(
                                        vwap_candles[symbol], maxlen=CANDLE_MAXLEN)
                                candles[symbol].append(candle)
                                session_date = get_session_date(
                                    candle['start_time'])
                                last_session = vwap_session_date[symbol]
                                if last_session != session_date:
                                    vwap_candles[symbol] = deque(maxlen=CANDLE_MAXLEN)  # 🚨 FIX: Use deque not list
                                    vwap_session_date[symbol] = session_date
                                    vwap_reset_done[symbol] = False  # Reset flag for new trading day
                                
                                # 🚨 FIX: Reset VWAP at 9:30 AM to exclude pre-market data
                                eastern = pytz.timezone("America/New_York")
                                candle_time = start_time.astimezone(eastern).time()
                                market_open_time = dt_time(9, 30)
                                
                                # If this is the first candle at or after 9:30 AM, reset VWAP
                                if len(vwap_candles[symbol]) > 0:
                                    last_candle_time = vwap_candles[symbol][-1]['start_time'].astimezone(eastern).time()
                                    # Reset if crossing from pre-market into regular session
                                    if last_candle_time < market_open_time <= candle_time:
                                        logger.info(f"[VWAP RESET] {symbol} - Market open at 9:30 AM, clearing pre-market data")
                                        vwap_candles[symbol] = deque(maxlen=CANDLE_MAXLEN)  # 🚨 FIX: Use deque not list
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
                                        vwap_candles[symbol] = deque(maxlen=CANDLE_MAXLEN)  # 🚨 FIX: Use deque not list
                                
                                vwap_candles[symbol].append(candle)
                                vwap_cum_vol[symbol] += volume
                                vwap_cum_pv[symbol] += (
                                    (high + low + close) / 3) * volume
                                
                                # 🚨 CRITICAL FIX: Update last_trade_price/time from candle as fallback
                                # Use REAL candle close time for freshness validation (don't fake timestamps)
                                current_trade_time = last_trade_time.get(symbol)
                                candle_close_time = start_time + timedelta(minutes=1)
                                
                                # Update ONLY if: no trade data OR trade data is older than candle close
                                if current_trade_time is None or current_trade_time < candle_close_time:
                                    last_trade_price[symbol] = close
                                    last_trade_time[symbol] = candle_close_time  # Use REAL candle time, not now()
                                    logger.info(f"[PRICE FALLBACK] {symbol} - Using candle close ${close:.2f} at {candle_close_time} (no fresh trades)")
                                
                                await on_new_candle(symbol, open_, high, low,
                                                    close, volume, start_time)
                                # Process all pending alerts and send only the best one
                                await send_best_alert(symbol)
                            elif event.get("ev") == "LULD":
                                # Handle LULD (Limit Up/Limit Down) events - Polygon's official halt detection!
                                # Robust symbol extraction - Polygon uses different keys across feeds
                                symbol = event.get("sym") or event.get("T") or event.get("ticker") or event.get("symbol")
                                high_limit = event.get("h")  # High price limit
                                low_limit = event.get("l")  # Low price limit
                                timestamp = event.get(
                                    "t", 0)  # Unix timestamp in milliseconds
                                indicators = event.get("i",
                                                       [])  # Indicators array

                                # 🔍 DEBUG: Log raw LULD event for diagnosis
                                logger.info(f"[LULD RAW EVENT] keys={list(event.keys())} symbol={symbol} event={event}")
                                
                                if symbol and timestamp:
                                    # Format halt key for deduplication
                                    # 🚨 CRITICAL FIX: Polygon LULD timestamps are in NANOSECONDS, divide by 1,000,000,000 to get seconds
                                    eastern = pytz.timezone("America/New_York")
                                    halt_time = datetime.fromtimestamp(
                                        timestamp / 1_000_000_000, tz=timezone.utc)
                                    halt_time_et = halt_time.astimezone(eastern)
                                    halt_key = f"{symbol}_{halt_time_et.strftime('%H:%M:%S')}_LULD"

                                    # 🚨 STALE EVENT FILTER: Reject LULD events older than 5 minutes
                                    # Only alert on ACTIVELY HALTED stocks, not old/resumed halts
                                    now = datetime.now(timezone.utc)
                                    halt_age_minutes = (now - halt_time).total_seconds() / 60
                                    if halt_age_minutes > 5:  # 5 minute staleness threshold - only current halts
                                        logger.info(f"[LULD SKIP] {symbol} - Halt is {halt_age_minutes:.1f} minutes old (stale event, stock likely resumed)")
                                        continue
                                    
                                    # 🚨 TIME-BASED DEDUPLICATION: Skip if we alerted on this symbol in last 5 minutes
                                    # This prevents spam when scanner reconnects and Polygon resends old LULD events
                                    last_halt_time = halt_last_alert_time.get(symbol)
                                    if last_halt_time and (now - last_halt_time).total_seconds() < 300:  # 5 minutes
                                        time_since = int((now - last_halt_time).total_seconds())
                                        logger.info(f"[LULD SKIP] {symbol} - Already alerted {time_since}s ago (within 5min window)")
                                        continue
                                    
                                    # Skip if already processed this exact halt
                                    if halt_key in halted_symbols:
                                        logger.info(f"[LULD SKIP] {symbol} - Duplicate halt key: {halt_key}")
                                        continue

                                    halted_symbols.add(halt_key)

                                    # 🚨 ACTIVE HALT VERIFICATION: Check if stock has traded since halt
                                    # If we have recent trades AFTER the halt time, stock has resumed → skip alert
                                    symbol_last_trade_time = last_trade_time.get(symbol)
                                    if symbol_last_trade_time and symbol_last_trade_time > halt_time:
                                        time_since_halt = (symbol_last_trade_time - halt_time).total_seconds() / 60
                                        logger.info(f"[LULD SKIP] {symbol} - Stock has traded {time_since_halt:.1f} min after halt (resumed, not currently halted)")
                                        continue

                                    # 🚨🚨 SPECIAL HALT HANDLING - BULLETPROOF 🚨🚨
                                    logger.warning(f"[LULD DETECTED] {symbol} halt at {halt_time_et.strftime('%I:%M:%S %p ET')}")
                                    
                                    try:
                                        # Try to get price from multiple sources
                                        current_price = last_trade_price.get(symbol)
                                        price_source = "last_trade"
                                        
                                        if not current_price:
                                            logger.info(f"[LULD] {symbol} - No last_trade_price, trying yfinance (non-blocking)...")
                                            try:
                                                def _fetch_price():
                                                    import yfinance as yf
                                                    ticker_info = yf.Ticker(symbol).info
                                                    return ticker_info.get('currentPrice') or ticker_info.get('regularMarketPrice')
                                                
                                                loop = asyncio.get_running_loop()
                                                current_price = await loop.run_in_executor(None, _fetch_price)
                                                price_source = "yfinance"
                                            except Exception as yf_error:
                                                logger.warning(f"[LULD] {symbol} - yfinance failed: {yf_error}")
                                        
                                        if not current_price and symbol in candles and len(candles[symbol]) > 0:
                                            current_price = candles[symbol][-1]['close']
                                            price_source = "candle"
                                            logger.info(f"[LULD] {symbol} - Using candle price ${current_price:.2f}")

                                        float_shares = await get_float_shares(symbol)
                                        
                                        # 🚨 STRICT HALT FILTERING: Only alert if stock meets bot criteria
                                        # Must have valid price ≤$15 AND float 500K-20M (or exception symbol)
                                        should_alert = False
                                        filter_reason = None
                                        
                                        # Price must be known and ≤$15 to alert
                                        if not current_price:
                                            filter_reason = "no price data available"
                                            logger.info(f"[LULD FILTERED] {symbol} - {filter_reason}")
                                        elif current_price > PRICE_THRESHOLD:
                                            should_alert = False
                                            filter_reason = f"price ${current_price:.2f} > ${PRICE_THRESHOLD}"
                                            logger.info(f"[LULD FILTERED] {symbol} - {filter_reason}")
                                        else:
                                            # Price is valid, check float
                                            if float_shares is None:
                                                # FAIL-OPEN: Alert on unknown floats for halts (important events)
                                                should_alert = True
                                                logger.info(f"[LULD PASS] {symbol} - Price ${current_price:.2f}, Float UNKNOWN - MEETS CRITERIA (fail-open)")
                                            elif not (MIN_FLOAT_SHARES <= float_shares <= MAX_FLOAT_SHARES or symbol in FLOAT_EXCEPTION_SYMBOLS):
                                                filter_reason = f"float {float_shares/1e6:.1f}M not in range {MIN_FLOAT_SHARES/1e6:.1f}M-{MAX_FLOAT_SHARES/1e6:.1f}M"
                                                logger.info(f"[LULD FILTERED] {symbol} - {filter_reason}")
                                            else:
                                                # Stock meets all criteria!
                                                should_alert = True
                                                logger.info(f"[LULD PASS] {symbol} - Price ${current_price:.2f}, Float {float_shares/1e6:.1f}M - MEETS CRITERIA")
                                        
                                        if should_alert:
                                            # 🔍 HALT VERIFICATION: Re-check NASDAQ before alerting
                                            halt_time_str = halt_time_et.strftime('%H:%M:%S')
                                            is_verified = await verify_halt_on_nasdaq(symbol, halt_time_str)
                                            
                                            if not is_verified:
                                                logger.warning(f"[LULD BLOCKED] {symbol} - Halt NOT verified on NASDAQ, skipping alert (prevents false positive)")
                                                continue
                                            
                                            # Format clean halt alert - just emoji, HALT, ticker, price
                                            price_str = fmt_price(current_price) if current_price else "?"
                                            alert_msg = f"🛑 <b>HALT {symbol}</b> ${price_str}"

                                            logger.warning(f"[🚨 HALT ALERT] Sending VERIFIED LULD halt for {symbol} @ ${price_str} ({price_source})")
                                            await send_all_alerts(alert_msg)

                                            # Log halt event
                                            log_event(
                                                "polygon_luld_halt", symbol,
                                                current_price if current_price else 0, 0, halt_time, {
                                                    "halt_type": "LULD",
                                                    "high_limit": high_limit,
                                                    "low_limit": low_limit,
                                                    "indicators": indicators,
                                                    "float_shares": float_shares,
                                                    "price_source": price_source,
                                                    "data_source": "polygon_luld"
                                                })

                                            # Track when we sent this halt alert (prevents spam on reconnects)
                                            halt_last_alert_time[symbol] = datetime.now(timezone.utc)
                                            logger.warning(f"[✅ LULD SENT] {symbol} @ ${price_str} - LULD halt verified")
                                        else:
                                            logger.info(f"[LULD FILTERED] {symbol} - {filter_reason}")

                                    except Exception as e:
                                        logger.error(f"[🚨 LULD ERROR] Error processing halt for {symbol}: {e}", exc_info=True)
                                        # 🚨 CRITICAL FIX: NO emergency alerts - must pass filtering to alert
                                        # If we can't verify price/float, we can't confirm it meets criteria
                                        logger.warning(f"[LULD BLOCKED] {symbol} - Error during filtering, cannot verify criteria - NO ALERT SENT")
                                        continue
                    except Exception as e:
                        logger.error(f"Error processing message: {e}\nRaw: {msg[:200]}", exc_info=True)

        except Exception as e:
            connection_attempts += 1
            logger.warning(f"[CONNECTION ERROR] Websocket error: {e}")

            # Handle connection limit errors with exponential backoff
            if ("max_connections" in str(e).lower()
                    or "connection" in str(e).lower()
                    or "limit" in str(e).lower()
                    or "policy violation" in str(e).lower()
                    or "1008" in str(e)):

                logger.warning(
                    f"[🚨 CONNECTION LIMIT] Connection limit/policy violation detected!"
                )
                logger.warning(
                    f"[🚨 CONNECTION LIMIT] Backing off for {connection_backoff_delay} seconds..."
                )

                # Connection alerts DISABLED - user only wants trade alerts, not infrastructure spam
                # alert_msg = f"🚨 <b>SCANNER CONNECTION ISSUE</b>\n\nPolygon connection limit exceeded. Backing off for {connection_backoff_delay}s.\nAttempt: {connection_attempts}"
                # try:
                #     await send_all_alerts(alert_msg)
                # except:
                #     pass  # Don't fail on alert send
                logger.info("[CONNECTION] Silently backing off, no user alert sent")

                await asyncio.sleep(connection_backoff_delay)

                # Exponential backoff: 15s → 30s → 60s → 120s → 300s (max 5min)
                connection_backoff_delay = min(connection_backoff_delay * 2,
                                               max_backoff_delay)

            elif "keepalive" in str(e).lower() or "ping" in str(
                    e).lower() or "1011" in str(e):
                logger.info(
                    "[CONNECTION] Keepalive/ping timeout - reconnecting in 10 seconds..."
                )
                await asyncio.sleep(10)
            else:
                logger.warning(
                    f"[CONNECTION] General error - reconnecting in 30 seconds..."
                )
                await asyncio.sleep(30)


async def verify_halt_on_nasdaq(symbol, halt_time_str):
    """🔍 HALT VERIFICATION: Re-check NASDAQ to confirm halt is still active
    
    Prevents false positives by verifying halt 10 seconds after initial detection.
    Returns True if halt is confirmed on NASDAQ, False otherwise.
    
    🚀 PERFORMANCE: Reuses global http_session instead of creating new session.
    """
    from bs4 import BeautifulSoup
    
    try:
        logger.info(f"[HALT VERIFY] Waiting 10s to verify {symbol} halt at {halt_time_str}...")
        await asyncio.sleep(10)  # Wait 10 seconds
        
        # 🚀 Reuse global http_session for better performance
        async with http_session.get("https://www.nasdaqtrader.com/trader.aspx?id=tradehalts", 
                                     timeout=aiohttp.ClientTimeout(total=10)) as response:
            if response.status == 200:
                html = await response.text()
                soup = BeautifulSoup(html, 'html.parser')
                table = soup.find('table', {'id': 'Trading_Halts'})
                
                if table:
                    rows = table.find_all('tr')[1:]  # Skip header
                    for row in rows:
                        cols = row.find_all('td')
                        if len(cols) >= 9:
                            nasdaq_symbol = cols[0].text.strip()
                            nasdaq_halt_time = cols[6].text.strip()
                            
                            # Match symbol and halt time
                            if nasdaq_symbol == symbol and nasdaq_halt_time == halt_time_str:
                                logger.info(f"[HALT VERIFY] ✅ {symbol} halt CONFIRMED on NASDAQ")
                                return True
        
        logger.warning(f"[HALT VERIFY] ❌ {symbol} halt NOT found on NASDAQ - likely false positive")
        return False
        
    except Exception as e:
        logger.error(f"[HALT VERIFY] Error verifying {symbol}: {e}")
        # If verification fails, assume halt is real (fail-safe)
        return True


async def rest_api_backup_scanner():
    """🚨 REST API BACKUP: Poll Polygon every 90 seconds to catch stocks WebSocket misses
    Catches micro-caps like TGL that WebSocket may skip during high-volume periods
    Only alerts if WebSocket hasn't already alerted on the stock"""
    import aiohttp
    from datetime import datetime, timezone, timedelta
    
    # Track stocks we've already sent REST backup alerts for (deduplication)
    rest_alerted_stocks = {}
    
    while True:
        # Only run during market scan hours
        if not is_market_scan_time():
            await asyncio.sleep(60)
            continue
        
        try:
            # Get current time for freshness checks
            now = datetime.now(timezone.utc)
            eastern = pytz.timezone("America/New_York")
            now_et = now.astimezone(eastern)
            
            # Query Polygon snapshot API for all stocks (catches WebSocket gaps)
            logger.info("[REST BACKUP] Polling Polygon snapshot API for top gainers (90s interval)...")
            
            async with http_session.get(
                f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey={POLYGON_API_KEY}"
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"[REST BACKUP] API error: {resp.status}")
                    await asyncio.sleep(90)  # Retry after 90 seconds
                    continue
                
                data = await resp.json()
                tickers = data.get("tickers", [])
                
                logger.info(f"[REST BACKUP] Received {len(tickers)} tickers from snapshot API")
                
                # Process each ticker for potential alerts
                candidates = []
                for ticker_data in tickers:
                    try:
                        symbol = ticker_data.get("ticker")
                        if not symbol:
                            continue
                        
                        # Get day data
                        day = ticker_data.get("day", {})
                        prev_day = ticker_data.get("prevDay", {})
                        
                        current_price = day.get("c")  # Current close
                        prev_close = prev_day.get("c")  # Previous close
                        volume = day.get("v", 0)
                        
                        if not current_price or not prev_close or prev_close == 0:
                            continue
                        
                        # Calculate % gain
                        pct_gain = ((current_price - prev_close) / prev_close) * 100
                        
                        # FILTER: Must be significant gainer (>10%) to warrant backup alert
                        if pct_gain < 10.0:
                            continue
                        
                        # FILTER: Volume requirement - must have meaningful volume (100K+ shares)
                        # Prevents alerts on low-volume garbage pumps and reverse split trash
                        if volume < 100000:
                            logger.debug(f"[REST BACKUP] {symbol} - Low volume {volume:,} < 100K, skipping")
                            continue
                        
                        # Check eligibility (price, float, ETF filter, warrants)
                        float_shares = await get_float_shares(symbol)
                        if not is_eligible(symbol, current_price, float_shares, use_entry_price=False):
                            continue
                        
                        # DEDUPLICATION: Skip if WebSocket already alerted today
                        if symbol in alerted_symbols:
                            logger.debug(f"[REST BACKUP] {symbol} - WebSocket already alerted, skipping")
                            continue
                        
                        # DEDUPLICATION: Skip if REST backup already alerted in last hour
                        last_rest_alert = rest_alerted_stocks.get(symbol)
                        if last_rest_alert and (now - last_rest_alert).total_seconds() < 3600:  # 1 hour
                            logger.debug(f"[REST BACKUP] {symbol} - Already sent REST alert in last hour, skipping")
                            continue
                        
                        candidates.append({
                            'symbol': symbol,
                            'price': current_price,
                            'gain': pct_gain,
                            'volume': volume,
                            'float': float_shares
                        })
                    
                    except Exception as e:
                        logger.debug(f"[REST BACKUP] Error processing ticker: {e}")
                        continue
                
                # Alert on top candidates (stocks WebSocket missed)
                if candidates:
                    # Sort by % gain (biggest movers first)
                    candidates.sort(key=lambda x: x['gain'], reverse=True)
                    
                    logger.info(f"[REST BACKUP] Found {len(candidates)} stocks WebSocket may have missed")
                    
                    for candidate in candidates[:5]:  # Limit to top 5 to avoid spam
                        symbol = candidate['symbol']
                        price = candidate['price']
                        gain = candidate['gain']
                        volume = candidate['volume']
                        float_shares = candidate['float']
                        
                        # Format alert - IDENTICAL to volume spike alerts
                        price_str = fmt_price(price)
                        
                        alert_msg = (
                            f"🔥 <b>{escape_html(symbol)}</b> Volume Spike\n"
                            f"Price: ${price_str}"
                        )
                        
                        await send_all_alerts(alert_msg)
                        
                        # Log the alert
                        log_event(
                            "rest_backup_alert",
                            symbol,
                            price,
                            volume,
                            now,
                            {
                                'gain_pct': gain,
                                'float': float_shares,
                                'reason': 'WebSocket gap detection'
                            }
                        )
                        
                        # Mark as alerted
                        rest_alerted_stocks[symbol] = now
                        
                        logger.warning(
                            f"[REST BACKUP ALERT] {symbol} ${price:.2f} +{gain:.1f}% - "
                            f"Caught by backup scanner (WebSocket missed)"
                        )
                        
                        # Small delay between alerts
                        await asyncio.sleep(2)
                
                else:
                    logger.info("[REST BACKUP] No new gainers detected (WebSocket is working properly)")
            
            # Clean up old REST alert tracking (older than 24 hours)
            cutoff = now - timedelta(hours=24)
            rest_alerted_stocks = {
                sym: timestamp for sym, timestamp in rest_alerted_stocks.items()
                if timestamp > cutoff
            }
        
        except Exception as e:
            logger.error(f"[REST BACKUP] Error in backup scanner: {e}")
        
        # Poll every 90 seconds (1.5 minutes) - unlimited API plan allows frequent polling
        await asyncio.sleep(90)


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
            # Only run during market scan hours (4am-8pm ET, Mon-Fri)
            if not is_market_scan_time():
                await asyncio.sleep(300)  # Check every 5 minutes when closed
                continue
            
            ny_tz = pytz.timezone("America/New_York")
            now = datetime.now(timezone.utc).astimezone(ny_tz)
            
            # Scrape NASDAQ halt page
            url = "https://www.nasdaqtrader.com/trader.aspx?id=tradehalts"
            timeout = aiohttp.ClientTimeout(total=10)
            async with http_session.get(url, timeout=timeout) as response:
                if response.status == 200:
                        html = await response.text()
                        
                        # Parse halt data (NASDAQ uses table format)
                        from bs4 import BeautifulSoup
                        soup = BeautifulSoup(html, 'html.parser')
                        
                        # 🔍 DEBUG: Log NASDAQ scrape status
                        table = soup.find('table', {'id': 'Trading_Halts'})
                        logger.info(f"[NASDAQ SCRAPE] status={response.status} content_len={len(html)} table_found={bool(table)}")
                        if table:
                            rows = table.find_all('tr')[1:]  # Skip header
                            
                            for row in rows:
                                cols = row.find_all('td')
                                if len(cols) >= 9:  # Need at least 9 columns
                                    symbol = cols[0].text.strip()              # Column 0: Issue Symbol
                                    halt_time_str = cols[6].text.strip()       # Column 6: Halt Time (ET)
                                    resume_time_str = cols[8].text.strip()     # Column 8: Resume Time (ET)
                                    halt_reason_code = cols[3].text.strip()    # Column 3: Reason Code
                                    
                                    # 🔍 DEBUG: Log extracted halt data
                                    logger.debug(f"[NASDAQ ROW] symbol={symbol} halt_time={halt_time_str} resume_time={resume_time_str} reason={halt_reason_code}")
                                    
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
                                                price_source = "last_trade"
                                                
                                                if not current_price and symbol in candles and len(candles[symbol]) > 0:
                                                    current_price = candles[symbol][-1]['close']
                                                    price_source = "candle"
                                                
                                                # Get float data
                                                float_shares = await get_float_shares(symbol)
                                                
                                                # 🚨 STRICT RESUME FILTERING: Only alert if stock meets bot criteria
                                                # Must have valid price ≤$15 AND float 500K-20M (or exception symbol)
                                                should_alert = False
                                                filter_reason = None
                                                
                                                # Price must be known and ≤$15 to alert
                                                if not current_price:
                                                    filter_reason = "no price data available"
                                                    logger.info(f"[RESUME FILTERED] {symbol} - {filter_reason}")
                                                elif current_price > PRICE_THRESHOLD:
                                                    filter_reason = f"price ${current_price:.2f} > ${PRICE_THRESHOLD}"
                                                    logger.info(f"[RESUME FILTERED] {symbol} - {filter_reason}")
                                                else:
                                                    # Price is valid, check float + movement tracking
                                                    if float_shares is None:
                                                        # Only alert if stock is ALREADY being tracked (has movement data)
                                                        # Require ≥1 candle to catch fast movers that halt quickly
                                                        if symbol in candles and len(candles[symbol]) >= 1:
                                                            should_alert = True
                                                            logger.warning(f"[RESUME PASS] {symbol} - Price ${current_price:.2f}, Float UNKNOWN but STOCK IS MOVING (has {len(candles[symbol])} candles) - ALLOWING")
                                                        else:
                                                            filter_reason = "no float data available and stock not being tracked"
                                                            logger.info(f"[RESUME FILTERED] {symbol} - {filter_reason}")
                                                    elif not (MIN_FLOAT_SHARES <= float_shares <= MAX_FLOAT_SHARES or symbol in FLOAT_EXCEPTION_SYMBOLS):
                                                        filter_reason = f"float {float_shares/1e6:.1f}M not in range {MIN_FLOAT_SHARES/1e6:.1f}M-{MAX_FLOAT_SHARES/1e6:.1f}M"
                                                        logger.info(f"[RESUME FILTERED] {symbol} - {filter_reason}")
                                                    else:
                                                        # Stock meets all criteria!
                                                        should_alert = True
                                                        logger.info(f"[RESUME PASS] {symbol} - Price ${current_price:.2f}, Float {float_shares/1e6:.1f}M - MEETS CRITERIA")
                                                
                                                if should_alert:
                                                    price_display = f"${current_price:.2f}"
                                                    float_display = f"{float_shares/1e6:.1f}M" if float_shares else "⚠️ Unknown"
                                                    
                                                    alert_msg = f"""🟢 <b>TRADING RESUMED</b> (NASDAQ)

<b>Symbol:</b> {symbol}
<b>Price:</b> {price_display}
<b>Float:</b> {float_display} shares
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
                                                            "data_source": "nasdaq_scrape",
                                                            "float_shares": float_shares,
                                                            "price_source": price_source
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
                                            # 🔒 Atomic write: temp file + rename prevents corruption
                                            # 🚀 PERFORMANCE: Run in thread executor to prevent blocking
                                            tmp_file = "halted_symbols.pkl.tmp"
                                            
                                            def write_halt_pickle():
                                                with open(tmp_file, "wb") as f:
                                                    pickle.dump(ever_halted_symbols, f)
                                            
                                            loop = asyncio.get_event_loop()
                                            await loop.run_in_executor(io_executor, write_halt_pickle)
                                            os.replace(tmp_file, "halted_symbols.pkl")
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
                                            logger.info(f"[HALT PRICE] {symbol} - No last_trade_price, trying yfinance (non-blocking)...")
                                            try:
                                                def _fetch_price():
                                                    import yfinance as yf
                                                    ticker_info = yf.Ticker(symbol).info
                                                    return ticker_info.get('currentPrice') or ticker_info.get('regularMarketPrice')
                                                
                                                loop = asyncio.get_running_loop()
                                                current_price = await loop.run_in_executor(None, _fetch_price)
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
                                        
                                        # 🚨 STRICT HALT FILTERING: Only alert if stock meets bot criteria
                                        # Must have valid price ≤$15 AND float 500K-20M (or exception symbol)
                                        should_alert = False
                                        filter_reason = None
                                        
                                        # Price must be known and ≤$15 to alert
                                        if not current_price:
                                            filter_reason = "no price data available"
                                            logger.info(f"[NASDAQ FILTERED] {symbol} - {filter_reason}")
                                        elif current_price > PRICE_THRESHOLD:
                                            should_alert = False
                                            filter_reason = f"price ${current_price:.2f} > ${PRICE_THRESHOLD}"
                                            logger.info(f"[NASDAQ FILTERED] {symbol} - {filter_reason}")
                                        else:
                                            # Price is valid, check float + movement tracking
                                            if float_shares is None:
                                                # Only alert if stock is ALREADY being tracked (has movement data)
                                                # Don't alert on random halts with unknown float
                                                # Require ≥1 candle to catch fast movers that halt quickly
                                                if symbol in candles and len(candles[symbol]) >= 1:
                                                    should_alert = True
                                                    logger.warning(f"[NASDAQ PASS] {symbol} - Price ${current_price:.2f}, Float UNKNOWN but STOCK IS MOVING (has {len(candles[symbol])} candles) - ALLOWING")
                                                else:
                                                    filter_reason = "no float data available and stock not being tracked"
                                                    logger.info(f"[NASDAQ FILTERED] {symbol} - {filter_reason}")
                                            elif not (MIN_FLOAT_SHARES <= float_shares <= MAX_FLOAT_SHARES or symbol in FLOAT_EXCEPTION_SYMBOLS):
                                                filter_reason = f"float {float_shares/1e6:.1f}M not in range {MIN_FLOAT_SHARES/1e6:.1f}M-{MAX_FLOAT_SHARES/1e6:.1f}M"
                                                logger.info(f"[NASDAQ FILTERED] {symbol} - {filter_reason}")
                                            else:
                                                # Stock meets all criteria!
                                                should_alert = True
                                                logger.info(f"[NASDAQ PASS] {symbol} - Price ${current_price:.2f}, Float {float_shares/1e6:.1f}M - MEETS CRITERIA")
                                        
                                        if should_alert:
                                            # 🔍 HALT VERIFICATION: Already on NASDAQ, no need to re-verify
                                            # (This IS the NASDAQ scraper, so halt is already confirmed)
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
                                        # DO NOT send emergency alert - if we can't verify criteria, don't alert
                                        logger.warning(f"[HALT SKIPPED] {symbol} - Could not verify price/float criteria due to error")
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
    
    # ✅ STARTUP: Check critical dependencies
    logger.info("[STARTUP] Checking dependencies...")
    missing_deps = []
    
    try:
        import bs4
        logger.info("  ✅ BeautifulSoup4 (bs4) available")
    except ImportError:
        missing_deps.append("beautifulsoup4")
        logger.error("  ❌ BeautifulSoup4 (bs4) NOT installed")
    
    try:
        import yfinance
        logger.info("  ✅ yfinance available")
    except ImportError:
        missing_deps.append("yfinance")
        logger.error("  ❌ yfinance NOT installed")
    
    if missing_deps:
        error_msg = f"\n❌ CRITICAL ERROR: Missing required dependencies!\n\nPlease install: {', '.join(missing_deps)}\n\nRun: pip install {' '.join(missing_deps)}\n"
        logger.error(error_msg)
        raise ImportError(f"Missing dependencies: {', '.join(missing_deps)}")
    
    logger.info("[STARTUP] ✅ All dependencies available\n")
    
    # 🚀 PERFORMANCE: Initialize reusable HTTP session
    http_session = aiohttp.ClientSession()
    logger.info("[HTTP SESSION] Created reusable HTTP session for better performance")
    
    logger.info("Main event loop running. Press Ctrl+C to exit.")
    ingest_task = asyncio.create_task(ingest_polygon_events())
    # Enabling just the scheduled alerts (9:24:55am and 8:01pm)
    close_alert_task = asyncio.create_task(market_close_alert_loop())
    premarket_alert_task = asyncio.create_task(premarket_gainers_alert_loop())
    # Enable NASDAQ halt monitoring (catches T1, T2, T5, T12 halts that Polygon LULD misses)
    nasdaq_halt_task = asyncio.create_task(nasdaq_halt_monitor())
    # 🔍 DISABLED: REST API backup scanner (was alerting on late/worthless moves)
    # rest_backup_task = asyncio.create_task(rest_api_backup_scanner())
    try:
        while True:
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        logger.info("Main loop cancelled.")
    finally:
        ingest_task.cancel()
        close_alert_task.cancel()
        premarket_alert_task.cancel()
        nasdaq_halt_task.cancel()
        # rest_backup_task.cancel()  # DISABLED

        # 🚨 FIX: Await cancelled tasks with error handling
        try:
            await ingest_task
        except asyncio.CancelledError:
            pass
        try:
            await close_alert_task
        except asyncio.CancelledError:
            pass
        try:
            await premarket_alert_task
        except asyncio.CancelledError:
            pass
        try:
            await nasdaq_halt_task
        except asyncio.CancelledError:
            pass
        # REST backup scanner disabled
        # try:
        #     await rest_backup_task
        # except asyncio.CancelledError:
        #     pass
        
        # 🚀 PERFORMANCE: Close reusable HTTP session
        if http_session:
            await http_session.close()
            logger.info("[HTTP SESSION] Closed reusable HTTP session")
        
        # 🚀 PERFORMANCE: Shutdown thread pool executor with wait=True
        # This ensures in-flight disk writes (pickle, CSV, JSON) complete safely
        # Blocking is acceptable here as we're already in shutdown path
        io_executor.shutdown(wait=True)
        logger.info("[IO EXECUTOR] Shutdown thread pool executor (waited for in-flight tasks)")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully...")
