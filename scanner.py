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
pending_breakout_alert = {}
HALT_LOG_FILE = "halt_event_log.csv"

alerted_symbols = set()
below_vwap_streak = defaultdict(int)
vwap_reclaimed_once = defaultdict(bool)
dip_play_seen = set()
recent_high = defaultdict(float)

# ---- PATCH: Volume spike alert per ticker per day ----
volume_spike_alerted_today = {}

# === EMA STACK ALERT ===
async def check_ema_stack_alert(symbol, candles, ema5, ema8, ema13, vwap):
    """
    candles: list of dicts with keys ['close', 'volume'], most recent last
    ema5, ema8, ema13, vwap: float values for latest close
    """
    try:
        if (
            ema5 > ema8 > ema13 and
            ema5 > vwap and ema8 > vwap and ema13 > vwap and
            (ema5 / ema13) >= 1.03 and
            candles[-1]['volume'] >= 100_000
        ):
            alert_msg = (
                f"⚡️ <b>{symbol}</b> EMA STACK ALERT\n"
                f"EMA5: {ema5:.2f}, EMA8: {ema8:.2f}, EMA13: {ema13:.2f}, VWAP: {vwap:.2f}\n"
                f"Ratio EMA5/EMA13: {ema5/ema13:.2f}\n"
                f"1-min Vol: {candles[-1]['volume']:,}"
            )
            await send_telegram_async(alert_msg)
            logger.info(f"EMA STACK ALERT sent for {symbol}")
    except Exception as e:
        logger.error(f"EMA STACK ALERT error for {symbol}: {e}")

def news_matches_keywords(headline, summary):
    text_block = f"{headline} {summary}".lower()
    return any(word.lower() in text_block for word in KEYWORDS)

def bold_keywords(text, keywords):
    def replacer(match):
        return f"<b>{match.group(0)}</b>"
    for kw in keywords:
        regex = re.compile(rf'\b({re.escape(kw)})\b', re.IGNORECASE)
        text = regex.sub(replacer, text)
    return text

async def is_under_20(tickers):
    logger.debug(f"[DEBUG] is_under_20 called for tickers: {tickers}")
    for ticker in tickers:
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}?apiKey={POLYGON_API_KEY}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    data = await resp.json()
                    price = None
                    last_trade = data.get("ticker", {}).get("lastTrade", {})
                    day = data.get("ticker", {}).get("day", {})
                    if last_trade and last_trade.get("p", 0) > 0:
                        price = last_trade["p"]
                    elif day and day.get("c", 0) > 0:
                        price = day["c"]
                    logger.debug(f"[DEBUG] Ticker {ticker} price: {price}")
                    if price is not None and 0 < price <= 20.00:
                        return True
        except Exception as e:
            logger.error(f"[DEBUG] Price check failed for {ticker}: {e}")
    return False

async def send_news_telegram_async(message):
    logger.debug(f"[DEBUG] send_news_telegram_async called. Message: {message}")
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": False
    }
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.post(url, data=payload, timeout=10) as resp:
                    result_text = await resp.text()
                    logger.debug(f"[DEBUG] Telegram API status (news): {resp.status}, response: {result_text}")
                    if resp.status == 429:
                        data = await resp.json()
                        retry = data.get("parameters", {}).get("retry_after", 30)
                        logger.warning(f"[DEBUG] Rate limited. Waiting {retry} seconds before retrying...")
                        await asyncio.sleep(retry)
                        continue
                    if resp.status != 200:
                        logger.error(f"[DEBUG] Telegram send error (news): {result_text}")
                    return
            except Exception as e:
                logger.error(f"[DEBUG] Telegram send error (news): {e}")
                return

async def news_alerts_task():
    logger.info("news_alerts_task started")
    global news_seen, latest_news_time
    url = f"https://api.polygon.io/v2/reference/news?apiKey={POLYGON_API_KEY}&limit=50"
    while True:
        logger.debug("news_alerts_task loop")
        if not is_news_alert_time():
            await asyncio.sleep(30)
            continue
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    data = await resp.json()
                    news_batch = data.get('results', [])
                    if news_batch:
                        times = [datetime.fromisoformat(item.get('published_utc').replace('Z', '+00:00')) for item in news_batch if item.get('published_utc')]
                        most_recent = max(times) if times else None
                    else:
                        most_recent = None

                    if latest_news_time is None:
                        latest_news_time = most_recent
                        await asyncio.sleep(30)
                        continue

                    for item in sorted(news_batch, key=lambda x: x.get('published_utc')):
                        if item['id'] in news_seen:
                            continue

                        headline = item.get('title', '')
                        summary = item.get('description', '')
                        tickers = item.get('tickers', [])
                        logger.debug(f"[DEBUG] News headline: {headline} | Summary: {summary} | Tickers: {tickers}")

                        if not news_matches_keywords(headline, summary):
                            continue
                        if not await is_under_20(tickers):
                            continue

                        float_filter_pass = True
                        if YFINANCE_AVAILABLE and tickers:
                            for t in tickers:
                                float_shares = get_float_shares(t)
                                logger.debug(f"[DEBUG] {t}: float={float_shares}")
                                if float_shares is None or float_shares < MIN_FLOAT_SHARES or float_shares > MAX_FLOAT_SHARES:
                                    float_filter_pass = False
                                    break
                        if not float_filter_pass:
                            logger.debug(f"[DEBUG] Skipping news for ticker due to float filter: {tickers}")
                            continue

                        if tickers:
                            tickers_html = ", ".join([
                                f'<a href="https://finance.yahoo.com/quote/{escape_html(t)}">{escape_html(t)}</a>'
                                for t in tickers
                            ])
                            tickers_str = f"<b>{tickers_html}</b>"
                        else:
                            tickers_str = ""

                        headline_clean = escape_html(headline)
                        summary_clean = escape_html(summary)
                        headline_bold = f"<b>{bold_keywords(headline_clean, KEYWORDS)}</b>"
                        summary_it = f"<i>{bold_keywords(summary_clean, KEYWORDS)}</i>" if summary_clean else ""

                        msg = f"📰 {tickers_str}\n{headline_bold}"
                        if summary_it:
                            msg += f"\n\n{summary_it}"

                        url_ = item.get("article_url") or item.get("url")
                        if url_:
                            msg += f"\n<a href=\"{escape_html(url_)}\">Read more</a>"

                        news_seen.add(item['id'])
                        save_news_id(item['id'])
                        await send_news_telegram_async(msg)
                        await asyncio.sleep(3)
                    if most_recent:
                        latest_news_time = most_recent
            if len(news_seen) > 500:
                # Use only the last 500 news ids (convert to list and back to set)
                news_seen = set(list(news_seen)[-500:])
        except Exception as e:
            logger.error(f"[DEBUG] News fetch error: {e}")
        await asyncio.sleep(30)

def is_equity_symbol(ticker):
    if re.search(r'(\.|\bW$|\bWS$|\bU$|\bR$|\bP$)', ticker):
        return False
    if ticker.endswith(('W', 'U', 'R', 'P', 'WS')) or '.' in ticker:
        return False
    if any(x in ticker for x in ['WS', 'W', 'U', 'R', 'P', '.']):
        return False
    return True

async def is_recent_ipo(ticker):
    url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={POLYGON_API_KEY}"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                data = await resp.json()
                list_date_str = data.get("results", {}).get("list_date")
                if list_date_str:
                    list_date = datetime.strptime(list_date_str, "%Y-%m-%d").date()
                    # FIX: use timezone-aware now
                    days_since_ipo = (datetime.now(timezone.utc).date() - list_date).days
                    if days_since_ipo < MIN_IPO_DAYS:
                        logger.info(f"{ticker} is a recent IPO ({days_since_ipo} days).")
                    return days_since_ipo < MIN_IPO_DAYS
        except Exception as e:
            logger.error(f"[DEBUG] IPO check failed for {ticker}: {e}")
    return False

async def fetch_top_penny_symbols():
    logger.info("fetch_top_penny_symbols called")
    penny_symbols = []
    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey={POLYGON_API_KEY}&limit=1000"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                data = await resp.json()
                if "tickers" not in data:
                    logger.debug(f"[DEBUG] Tickers snapshot API response: {data}")
                    return []
                for stock in data.get("tickers", []):
                    ticker = stock.get("ticker")
                    if not is_equity_symbol(ticker):
                        continue
                    if await is_recent_ipo(ticker):
                        continue
                    last_trade = stock.get("lastTrade", {})
                    day = stock.get("day", {})
                    price = None
                    price_time = 0
                    if last_trade and last_trade.get("p", 0) > 0:
                        price = last_trade["p"]
                        price_time = last_trade.get("t", 0)
                    if day and day.get("c", 0) > 0 and (not price or day.get("t", 0) > price_time):
                        price = day["c"]
                    volume = day.get("v", 0)
                    if price is not None and 0 < price <= PRICE_THRESHOLD:
                        float_shares = get_float_shares(ticker)
                        logger.debug(f"[DEBUG] {ticker}: float={float_shares}")
                        if float_shares is None or float_shares < MIN_FLOAT_SHARES or float_shares > MAX_FLOAT_SHARES:
                            continue
                        penny_symbols.append(ticker)
                        if len(penny_symbols) >= MAX_SYMBOLS:
                            break
        except Exception as e:
            logger.error(f"[DEBUG] Tickers snapshot fetch error: {e}")
    logger.info(f"fetch_top_penny_symbols: found {len(penny_symbols)} symbols")
    return penny_symbols

current_symbols = set()

async def dynamic_symbol_manager(symbol_queue):
    logger.info("dynamic_symbol_manager started")
    global current_symbols
    while True:
        logger.debug("dynamic_symbol_manager loop")
        if is_market_scan_time():
            symbols = await fetch_top_penny_symbols()
            if symbols:
                if set(symbols) != current_symbols:
                    logger.info(f"dynamic_symbol_manager: updating symbol_queue with {len(symbols)} symbols")
                    await symbol_queue.put(symbols)
                    current_symbols = set(symbols)
        await asyncio.sleep(SCREENER_REFRESH_SEC)

async def on_trade_event(symbol, price, size, trade_time):
    logger.debug(f"on_trade_event: {symbol}, price: {price}, size: {size}")
    candle_time = trade_time.replace(second=0, microsecond=0)
    last_time = trade_candle_last_time.get(symbol)
    trades = trade_candle_builders[symbol]

    if not isinstance(trades, (list, deque)):
        logger.error(f"[DEBUG] ERROR: trades for {symbol} is not a list/deque! Actual: {type(trades)} | Value: {trades}")
        trades = [trades]
        trade_candle_builders[symbol] = trades

    if last_time is not None and candle_time != last_time:
        if trades:
            trades = list(trades)
            trades.sort(key=lambda t: t[2])
            prices = [t[0] for t in trades]
            volumes = [t[1] for t in trades]
            open_ = prices[0]
            close = prices[-1]
            high = max(prices)
            low = min(prices)
            volume = sum(volumes)
            start_time = last_time
            await on_new_candle(symbol, open_, high, low, close, volume, start_time)
        trade_candle_builders[symbol] = []
    trade_candle_builders[symbol].append((price, size, trade_time))
    trade_candle_last_time[symbol] = candle_time

RUG_PULL_DROP_PCT = -0.10
RUG_PULL_BOUNCE_PCT = 0.05

async def on_new_candle(symbol, open_, high, low, close, volume, start_time):
    logger.debug(f"on_new_candle: {symbol} - open:{open_}, close:{close}, volume:{volume}")
    if not is_market_scan_time() or close > 20.00:
        return

    # ---- PATCH: Volume spike alert per ticker per day ----
    today = datetime.now(timezone.utc).date()
    if volume_spike_alerted_today.get(symbol, (None,))[0] != today:
        volume_spike_alerted_today[symbol] = (today, False)
    if not volume_spike_alerted_today[symbol][1]:
        if len(candles[symbol]) >= 5:
            prev_vols = [c['volume'] for c in list(candles[symbol])[-5:]]
            avg_prev_5 = sum(prev_vols) / 5
            if volume >= 150_000 and volume >= 2 * avg_prev_5:
                msg = (
                    f"🔊 <b>{escape_html(symbol)}</b> volume spike! "
                    f"{volume:,} shares in 1 minute, 2x last 5min avg ({int(avg_prev_5):,}). "
                    f"Price: ${close:.2f}"
                )
                await send_telegram_async(msg)
                volume_spike_alerted_today[symbol] = (today, True)

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
                    msg = (
                        f"📉 {escape_html(symbol)} Dip Play: Dropped {dip_pct*100:.1f}% to ${close:.2f}, "
                        f"2 higher lows with rising volume!"
                    )
                    await send_telegram_async(msg)
                    dip_play_seen.add(symbol)

    if len(candles_seq) >= 3:
        c0, c1, c2 = list(candles_seq)[-3:]
        drop_pct = (c1["close"] - c0["close"]) / c0["close"]
        if drop_pct <= RUG_PULL_DROP_PCT:
            bounce_pct = (c2["close"] - c1["close"]) / c1["close"]
            if bounce_pct < RUG_PULL_BOUNCE_PCT:
                if symbol in alerted_symbols:
                    rug_msg = f"⚠️ {symbol} rug pull warning: Now ${c2['close']:.2f}."
                    await send_telegram_async(rug_msg)

    # Breakout/runner alert logic
    DUAL_MIN_1M_PRICE_MOVE_PCT = 0.05  # PATCHED: was 0.02, now 5%
    DUAL_MIN_1M_PRICE_MOVE_ABS = 0.05
    DUAL_MIN_1M_PRICE_MOVE_ABS_2PLUS = 0.10
    PRE_BREAKOUT_DIST_PCT = 0.05  # PATCHED: was 0.02, now 0.05 (to match 5% min move)
    PRE_BREAKOUT_DIST_ABS = 0.05
    VOLUME_SPIKE_MULTIPLIER = 2.5
    VOLUME_SPIKE_MIN = 5000

    prev_vols = [c["volume"] for c in list(candles_seq)[:-1]]
    if len(prev_vols) >= 2 and vwap is not None:
        avg_prev = sum(prev_vols) / len(prev_vols)
        price_move = close - open_
        price_move_pct = price_move / open_ if open_ > 0 else 0
        min_abs = DUAL_MIN_1M_PRICE_MOVE_ABS if close < 2 else DUAL_MIN_1M_PRICE_MOVE_ABS_2PLUS
        session_high = max([c["high"] for c in candles_seq])
        dist_from_high = session_high - close
        dist_pct = dist_from_high / session_high if session_high > 0 else 0
        now = datetime.now(timezone.utc)

        if (
            volume > VOLUME_SPIKE_MIN
            and volume >= VOLUME_SPIKE_MULTIPLIER * avg_prev
            and close > open_
            and close > vwap
            and (price_move >= min_abs or price_move_pct >= DUAL_MIN_1M_PRICE_MOVE_PCT)
            and abs(close - session_high) < 1e-6
            and (now - last_volume_spike_time[symbol]).total_seconds() > 600
        ):
            pending_breakout_alert[symbol] = {
                "breakout_close": close,
                "breakout_time": start_time
            }
            last_volume_spike_time[symbol] = now

        elif (
            volume > VOLUME_SPIKE_MIN
            and volume >= VOLUME_SPIKE_MULTIPLIER * avg_prev
            and close > open_
            and close > vwap
            and (price_move >= min_abs or price_move_pct >= DUAL_MIN_1M_PRICE_MOVE_PCT)
            and close < session_high
            and (0 < dist_from_high <= PRE_BREAKOUT_DIST_ABS or 0 < dist_pct <= PRE_BREAKOUT_DIST_PCT)
            and (now - last_runner_alert_time[symbol]).total_seconds() > 900
        ):
            pending_runner_alert[symbol] = {
                "spike_close": close,
                "spike_time": start_time,
            }

    # ---- Breakout/runner confirm logic ----
    if symbol in pending_breakout_alert:
        candidate = pending_breakout_alert[symbol]
        seconds = (start_time - candidate["breakout_time"]).total_seconds()
        MIN_CONFIRM_VOL = 10000
        if seconds == 60:
            if (
                close >= candidate["breakout_close"] and
                volume >= MIN_CONFIRM_VOL and
                high > candidate["breakout_close"]
            ):
                msg = f"🚀 {symbol} BREAKOUT CONFIRMED! ${close:.2f}"
                await send_telegram_async(msg)
                alerted_symbols.add(symbol)
            del pending_breakout_alert[symbol]
        elif seconds > 60:
            del pending_breakout_alert[symbol]

    if symbol in pending_runner_alert:
        candidate = pending_runner_alert[symbol]
        if (start_time - candidate["spike_time"]).total_seconds() == 60:
            if close >= candidate["spike_close"] * 0.98:
                today = datetime.now(timezone.utc).date()
                symbol_day = f"{symbol}_{today}"
                if symbol_day not in runner_alerted_today:
                    runner_alerted_today.add(symbol_day)
                    msg = f"👀 {symbol} runner warming up: ${close:.2f}"
                    await send_telegram_async(msg)
                    alerted_symbols.add(symbol)
                else:
                    msg = f"🏃 {symbol} is RUNNING: ${close:.2f}"
                    await send_telegram_async(msg)
                    alerted_symbols.add(symbol)
            del pending_runner_alert[symbol]
        elif (start_time - candidate["spike_time"]).total_seconds() > 60:
            del pending_runner_alert[symbol]

    # --- RVOL SPIKE ALERT (with price move filter, only if no breakout/runner this run) ---
    MIN_PRICE_MOVE_PCT = 0.08  # 8% minimum price move required

    if len(candles_seq) == 3:
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
                        msg = (
                            f"🚨 {escape_html(symbol)} Volume Spike! "
                            f"Price=${c2['close']:.2f}"
                        )
                        await send_telegram_async(msg)
                        alerted_symbols.add(symbol)

                    if rvol < RVOL_MIN:
                        return
                else:
                    return
            else:
                return

            logger.debug(f"ALERT DEBUG: {symbol} c0={c0['close']} c1={c1['close']} c2={c2['close']} move={c2['close']-c0['close']}")

            if (
                c0["close"] < c1["close"] < c2["close"]
                and (c2["close"] - c0["close"]) >= MIN_ALERT_MOVE
                and total_volume >= MIN_3MIN_VOLUME
                and c0["volume"] >= MIN_PER_CANDLE_VOL
                and c1["volume"] >= MIN_PER_CANDLE_VOL
                and c2["volume"] >= MIN_PER_CANDLE_VOL
            ):
                move = c2["close"] - c0["close"]
                prev_price = last_alerted_price.get(symbol)
                if prev_price is not None and (c2["close"] - prev_price) < ALERT_PRICE_DELTA:
                    return
                emoji = "🚨" if prev_price is None else "💰"
                last_alerted_price[symbol] = c2["close"]

                pct_move = (c2["close"] - c0["close"]) / c0["close"] if c0["close"] > 0 else 0

                conf = 1
                if 'rvol' in locals():
                    if rvol >= 5:
                        conf += 2
                    elif rvol >= 3:
                        conf += 1
                if pct_move >= 0.10:
                    conf += 2
                elif pct_move >= 0.05:
                    conf += 1
                if move >= 0.50:
                    conf += 2
                elif move >= 0.30:
                    conf += 1
                if min(c0["volume"], c1["volume"], c2["volume"]) >= 10000:
                    conf += 1
                conf = min(conf, 10)

                if conf < 5:
                    return

                msg = (
                    f"{emoji} {escape_html(symbol)} up ${move:.2f} in 3 minutes.\n"
                    f"Now ${c2['close']:.2f}. "
                    f"<b>Confidence: {conf}/10</b>"
                )

                await send_telegram_async(msg)
                alerted_symbols.add(symbol)

                log_event(
                    event_type="spike",
                    symbol=symbol,
                    price=c2["close"],
                    volume=total_volume,
                    event_time=start_time,
                    extra_features={"rvol": rvol if 'rvol' in locals() else None, "prepost": 0}
                )
                ml_prob = score_event_ml("spike", symbol, c2["close"], total_volume, rvol if 'rvol' in locals() else None, 0)
                if ml_prob > 0.7:
                    await send_telegram_async(
                        f"🔥 <b>HIGH POTENTIAL RUNNER</b> {escape_html(symbol)} Rocket Fuel: {ml_prob:.2f} 🚀"
                    )

    # === CALL EMA STACK ALERT HERE (if you have calculated EMA5/8/13 & VWAP) ===
    # Example (uncomment and provide actual calculation where appropriate):
    # if len(candles_seq) >= 13:
    #     closes = [c['close'] for c in candles_seq]
    #     ema5 = talib.EMA(np.array(closes), timeperiod=5)[-1]
    #     ema8 = talib.EMA(np.array(closes), timeperiod=8)[-1]
    #     ema13 = talib.EMA(np.array(closes), timeperiod=13)[-1]
    #     vwap_value = vwap
    #     await check_ema_stack_alert(symbol, list(candles_seq), ema5, ema8, ema13, vwap_value)

# ...rest of the script: websocket handling, main() etc...

# Websocket and main event loop logic (example stub)
async def main():
    # Your main loop, websocket connection setup, symbol management, etc.
    # This is just a placeholder; your full code should go here.
    pass

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Shutting down gracefully...")
