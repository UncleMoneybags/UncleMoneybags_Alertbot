print("scanner.py is running!!! --- If you see this, your file is found and started.")

import asyncio
import websockets
import aiohttp
import json
import html
import re
from collections import deque, defaultdict
from datetime import datetime, time, timezone, timedelta
import pytz
import signal

# ==== ML & Logging Imports (INJECTED) ====
import csv
import os
import joblib
import numpy as np

# ==== PATCH: Yahoo Finance Imports for Float Filtering ====
try:
    import yfinance as yf
    YFINANCE_AVAILABLE = True
except ImportError:
    print("WARNING: yfinance not installed. Run 'pip install yfinance' for float filtering.")
    YFINANCE_AVAILABLE = False

print("Imports completed successfully.")

# --- CONFIG ---
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"
PRICE_THRESHOLD = 10.00
MAX_SYMBOLS = 400
SCREENER_REFRESH_SEC = 60
MIN_ALERT_MOVE = 0.15
MIN_3MIN_VOLUME = 5000
MIN_PER_CANDLE_VOL = 1000
MIN_IPO_DAYS = 30
ALERT_PRICE_DELTA = 0.25

# PATCH: Float filter config for both screener and news
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
    print("Loaded ML runner model.")
except Exception as e:
    print("WARNING: Could not load runner model:", e)
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
    now_utc = datetime.now(pytz.UTC)
    now_ny = now_utc.astimezone(ny)
    if now_ny.weekday() >= 5:
        return False
    scan_start = time(4, 0)
    scan_end = time(20, 0)
    return scan_start <= now_ny.time() <= scan_end

def is_news_alert_time():
    ny = pytz.timezone("America/New_York")
    now_utc = datetime.now(pytz.UTC)
    now_ny = now_utc.astimezone(ny)
    if now_ny.weekday() >= 5:
        return False
    start = time(7, 0)
    end = time(20, 0)
    return start <= now_ny.time() <= end

async def send_telegram_async(message):
    print(f"[DEBUG] send_telegram_async called. Message: {message}")
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
                print(f"[DEBUG] Telegram API status: {resp.status}, response: {result_text}")
                if resp.status != 200:
                    print("[DEBUG] Telegram send error:", result_text)
    except Exception as e:
        print(f"[DEBUG] Telegram send error: {e}")

def escape_html(s):
    return html.escape(s or "")

candles = defaultdict(lambda: deque(maxlen=3))
trade_candle_builders = defaultdict(list)
trade_candle_last_time = {}
last_alerted_price = {}
last_halt_alert = {}
news_seen = set()
latest_news_time = None

last_volume_spike_time = defaultdict(lambda: datetime.min.replace(tzinfo=timezone.utc))
last_runner_alert_time = defaultdict(lambda: datetime.min.replace(tzinfo=timezone.utc))
runner_alerted_today = set()

pending_runner_alert = {}
pending_breakout_alert = {}
HALT_LOG_FILE = "halt_event_log.csv"

alerted_symbols = set()

# --- VWAP Streak tracking dictionaries ---
below_vwap_streak = defaultdict(int)
vwap_reclaimed_once = defaultdict(bool)

# --- DIP PLAY tracking dictionaries ---
dip_play_seen = set()
recent_high = defaultdict(float)

def log_halt_event(item, reason=None):
    import csv, os
    row = {
        "symbol": item.get("sym"),
        "event_time": item.get("t"),
        "price": item.get("p"),
        "ev": item.get("ev"),
        "raw": str(item),
        "log_reason": reason or "",
        "logged_at": datetime.now(timezone.utc).isoformat()
    }
    write_header = not os.path.exists(HALT_LOG_FILE) or os.path.getsize(HALT_LOG_FILE) == 0
    with open(HALT_LOG_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if write_header:
            writer.writeheader()
        writer.writerow(row)

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
    print(f"[DEBUG] is_under_20 called for tickers: {tickers}")
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
                    print(f"[DEBUG] Ticker {ticker} price: {price}")
                    if price is not None and 0 < price <= 20.00:
                        return True
        except Exception as e:
            print(f"[DEBUG] Price check failed for {ticker}: {e}")
    return False

async def send_news_telegram_async(message):
    print(f"[DEBUG] send_news_telegram_async called. Message: {message}")
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
                    print(f"[DEBUG] Telegram API status (news): {resp.status}, response: {result_text}")
                    if resp.status == 429:
                        data = await resp.json()
                        retry = data.get("parameters", {}).get("retry_after", 30)
                        print(f"[DEBUG] Rate limited. Waiting {retry} seconds before retrying...")
                        await asyncio.sleep(retry)
                        continue
                    if resp.status != 200:
                        print("[DEBUG] Telegram send error (news):", result_text)
                    return
            except Exception as e:
                print(f"[DEBUG] Telegram send error (news): {e}")
                return

# PATCHED: News alerts only for tickers with float in range, and <small> -> <i>
async def news_alerts_task():
    print("news_alerts_task started")
    global news_seen, latest_news_time
    url = f"https://api.polygon.io/v2/reference/news?apiKey={POLYGON_API_KEY}&limit=50"
    while True:
        print("news_alerts_task loop")
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
                        print(f"[DEBUG] News headline: {headline} | Summary: {summary} | Tickers: {tickers}")

                        if not news_matches_keywords(headline, summary):
                            continue
                        if not await is_under_20(tickers):
                            continue

                        # PATCH: Yahoo Finance float filter for news
                        float_filter_pass = True
                        if YFINANCE_AVAILABLE and tickers:
                            for t in tickers:
                                try:
                                    info = yf.Ticker(t).info
                                    float_shares = info.get('floatShares', None)
                                    print(f"[DEBUG] {t}: float={float_shares}")
                                    if float_shares is None or float_shares < MIN_FLOAT_SHARES or float_shares > MAX_FLOAT_SHARES:
                                        float_filter_pass = False
                                        break
                                except Exception as e:
                                    print(f"[DEBUG] Yahoo float error for {t}: {e}")
                                    float_filter_pass = False
                                    break
                        if not float_filter_pass:
                            print(f"[DEBUG] Skipping news for ticker due to float filter: {tickers}")
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
                        await send_news_telegram_async(msg)
                        await asyncio.sleep(3)
                    if most_recent:
                        latest_news_time = most_recent
            if len(news_seen) > 500:
                news_seen = set(list(news_seen)[-500:])
        except Exception as e:
            print(f"[DEBUG] News fetch error: {e}")
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
                    days_since_ipo = (datetime.utcnow().date() - list_date).days
                    if days_since_ipo < MIN_IPO_DAYS:
                        print(f"{ticker} is a recent IPO ({days_since_ipo} days).")
                    return days_since_ipo < MIN_IPO_DAYS
        except Exception as e:
            print(f"[DEBUG] IPO check failed for {ticker}: {e}")
    return False

# PATCHED: Screener only returns tickers with float in range
async def fetch_top_penny_symbols():
    print("fetch_top_penny_symbols called")
    penny_symbols = []
    url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?apiKey={POLYGON_API_KEY}&limit=1000"
    async with aiohttp.ClientSession() as session:
        try:
            async with session.get(url) as resp:
                data = await resp.json()
                if "tickers" not in data:
                    print("[DEBUG] Tickers snapshot API response:", data)
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
                        # PATCH: Yahoo Finance float filter for screener
                        float_filter_pass = True
                        if YFINANCE_AVAILABLE:
                            try:
                                info = yf.Ticker(ticker).info
                                float_shares = info.get('floatShares', None)
                                print(f"[DEBUG] {ticker}: float={float_shares}")
                                if float_shares is None or float_shares < MIN_FLOAT_SHARES or float_shares > MAX_FLOAT_SHARES:
                                    float_filter_pass = False
                            except Exception as e:
                                print(f"[DEBUG] Yahoo float error for {ticker}: {e}")
                                float_filter_pass = False
                        if not float_filter_pass:
                            continue
                        penny_symbols.append(ticker)
                        if len(penny_symbols) >= MAX_SYMBOLS:
                            break
        except Exception as e:
            print("[DEBUG] Tickers snapshot fetch error:", e)
    print(f"fetch_top_penny_symbols: found {len(penny_symbols)} symbols")
    return penny_symbols

current_symbols = set()

async def dynamic_symbol_manager(symbol_queue):
    print("dynamic_symbol_manager started")
    global current_symbols
    while True:
        print("dynamic_symbol_manager loop")
        if is_market_scan_time():
            symbols = await fetch_top_penny_symbols()
            if symbols:
                if set(symbols) != current_symbols:
                    print(f"dynamic_symbol_manager: updating symbol_queue with {len(symbols)} symbols")
                    await symbol_queue.put(symbols)
                    current_symbols = set(symbols)
        await asyncio.sleep(SCREENER_REFRESH_SEC)

async def on_trade_event(symbol, price, size, trade_time):
    print(f"on_trade_event: {symbol}, price: {price}, size: {size}")
    candle_time = trade_time.replace(second=0, microsecond=0)
    last_time = trade_candle_last_time.get(symbol)
    trades = trade_candle_builders[symbol]

    if not isinstance(trades, (list, deque)):
        print(f"[DEBUG] ERROR: trades for {symbol} is not a list/deque! Actual: {type(trades)} | Value: {trades}")
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
    print(f"on_new_candle: {symbol} - open:{open_}, close:{close}, volume:{volume}")
    if not is_market_scan_time() or close > PRICE_THRESHOLD:
        return

    if not isinstance(candles[symbol], deque):
        print(f"[DEBUG] ERROR: candles[{symbol}] not deque. Found type: {type(candles[symbol])}. Value: {candles[symbol]}")
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

    # === VWAP RECLAIM ALERT REMOVED ===

    # === DIP PLAY ALERT (Chart Down Emoji) ===
    MIN_DIP_PCT = 0.15  # 15%
    DIP_LOOKBACK = 20   # candles to look back for high

    if len(candles_seq) >= DIP_LOOKBACK:
        highs = [c["high"] for c in list(candles_seq)[-DIP_LOOKBACK:]]
        rhigh = max(highs)
        recent_high[symbol] = rhigh

    if recent_high[symbol] > 0:
        dip_pct = (recent_high[symbol] - close) / recent_high[symbol]
        if dip_pct >= MIN_DIP_PCT and symbol not in dip_play_seen:
            if len(candles_seq) >= 4:
                c1, c2, c3, c4 = list(candles_seq)[-4:]
                higher_lows = c2["low"] > c1["low"] and c3["low"] > c2["low"] and c4["low"] > c3["low"]
                rising_volume = c2["volume"] > c1["volume"] and c3["volume"] > c2["volume"] and c4["volume"] > c3["volume"]
                if higher_lows and rising_volume:
                    msg = (
                        f"📉 {escape_html(symbol)} Dip Play: Dropped {dip_pct*100:.1f}% to ${close:.2f}, "
                        f"3 higher lows with rising volume!"
                    )
                    await send_telegram_async(msg)
                    dip_play_seen.add(symbol)

    # ... (rest of your original on_new_candle logic unchanged)
    # [spike, runner, breakout, rvol, etc.]
    if len(candles_seq) >= 3:
        c0, c1, c2 = list(candles_seq)[-3:]
        drop_pct = (c1["close"] - c0["close"]) / c0["close"]
        if drop_pct <= RUG_PULL_DROP_PCT:
            bounce_pct = (c2["close"] - c1["close"]) / c1["close"]
            if bounce_pct < RUG_PULL_BOUNCE_PCT:
                if symbol in alerted_symbols:
                    rug_msg = f"⚠️ {symbol} rug pull warning: Now ${c2['close']:.2f}."
                    await send_telegram_async(rug_msg)

    DUAL_MIN_1M_PRICE_MOVE_PCT = 0.02
    DUAL_MIN_1M_PRICE_MOVE_ABS = 0.05
    DUAL_MIN_1M_PRICE_MOVE_ABS_2PLUS = 0.10
    PRE_BREAKOUT_DIST_PCT = 0.02
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
                msg = f"🚀 {symbol} BREAKOUT CONFIRMED! ${close:.2f} (vol {volume:,})"
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
                    print(f"{symbol} RVOL: {rvol}")
                    if rvol < RVOL_MIN:
                        return
                else:
                    return
            else:
                return
        else:
            return

        print(f"ALERT DEBUG: {symbol} c0={c0['close']} c1={c1['close']} c2={c2['close']} move={c2['close']-c0['close']}")

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

            msg = (
                f"{emoji} {escape_html(symbol)} up ${move:.2f} ({pct_move*100:.1f}%) in 3 minutes.\n"
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

async def handle_halt_event(item):
    print(f"[DEBUG] Halt event received: {item}")
    log_halt_event(item, reason="received")

    symbol = item.get("sym")
    if not symbol or not is_equity_symbol(symbol):
        log_halt_event(item, reason="not_equity_symbol")
        return

    status = str(item.get("status", "")).lower()
    price = None
    if "p" in item and item["p"] is not None:
        price = float(item["p"])
    if price is None:
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{symbol}?apiKey={POLYGON_API_KEY}"
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as resp:
                    data = await resp.json()
                    snap = data.get("ticker", {})
                    price = snap.get("lastTrade", {}).get("p") or snap.get("day", {}).get("c")
        except Exception as e:
            print(f"[DEBUG] [HALT ALERT] Could not get price for {symbol}: {e}")
            log_halt_event(item, reason=f"price_fetch_error: {e}")

    # PATCH: Allow halt alerts for price up to $20 (previously $10)
    if price is None or price > 20.00 or price <= 0:
        log_halt_event(item, reason=f"price_above_threshold: {price}")
        return

    halt_ts = item.get("t") if "t" in item else None
    global last_halt_alert
    if halt_ts is not None:
        if symbol in last_halt_alert and last_halt_alert[symbol] == halt_ts and status != "resumed":
            log_halt_event(item, reason="duplicate_halt")
            return
        last_halt_alert[symbol] = halt_ts

    if status == "halted" or not status:
        msg = f"🚦 {escape_html(symbol)} (${price:.2f}) just got halted!"
        await send_telegram_async(msg)
        log_halt_event(item, reason="alert_sent_halted")
        event_type = "halt"
    elif status == "resumed":
        msg = f"🟢 {escape_html(symbol)} (${price:.2f}) resumed trading!"
        await send_telegram_async(msg)
        log_halt_event(item, reason="alert_sent_resumed")
        event_type = "resume"
    else:
        return

    log_event(
        event_type=event_type,
        symbol=symbol,
        price=price,
        volume=None,
        event_time=datetime.now(timezone.utc),
        extra_features={"rvol": None, "prepost": 0}
    )
    ml_prob = score_event_ml(event_type, symbol, price, 0, 1.0, 0)
    if ml_prob > 0.7:
        await send_telegram_async(
            f"🔥 <b>HIGH POTENTIAL {event_type.upper()}</b> {escape_html(symbol)} Rocket Fuel: {ml_prob:.2f} 🚀"
        )

async def send_scheduled_alerts():
    print("send_scheduled_alerts started")
    sent_open_msg = False
    sent_close_msg = False
    sent_ebook_msg = False
    while True:
        print("send_scheduled_alerts loop")
        now_utc = datetime.now(pytz.UTC)
        ny = pytz.timezone("America/New_York")
        now_ny = now_utc.astimezone(ny)
        weekday = now_ny.weekday()
        if weekday < 5 and now_ny.hour == 9 and now_ny.minute == 25:
            if not sent_open_msg:
                await send_telegram_async("Market opens in 5 mins...secure the damn bag!")
                sent_open_msg = True
        else:
            sent_open_msg = False
        if weekday < 4 and now_ny.hour == 20 and now_ny.minute == 1:
            if not sent_close_msg:
                await send_telegram_async("Market closed, reconvene in pre market tomorrow morning.")
                sent_close_msg = True
        else:
            sent_close_msg = False
        if weekday == 5 and now_ny.hour == 12 and now_ny.minute == 0:
            if not sent_ebook_msg:
                await send_telegram_async("📚 Need help trading? My ebook has everything you need to know!")
                sent_ebook_msg = True
        else:
            sent_ebook_msg = False
        await asyncio.sleep(30)

def setup_signal_handlers(loop):
    print("setup_signal_handlers called")
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.ensure_future(shutdown(loop, sig)))

async def shutdown(loop, sig):
    print(f"Received exit signal {sig.name}...")
    tasks = [t for t in asyncio.all_tasks(loop) if t is not asyncio.current_task()]
    [task.cancel() for task in tasks]
    await asyncio.gather(*tasks, return_exceptions=True)
    loop.stop()

async def trade_ws(symbol_queue):
    print("trade_ws started")
    subscribed_symbols = set()
    halt_subscribed = False
    while True:
        print("trade_ws loop")
        try:
            uri = "wss://socket.polygon.io/stocks"
            async with websockets.connect(uri, ping_interval=30, ping_timeout=10) as ws:
                await ws.send(json.dumps({"action": "auth", "params": POLYGON_API_KEY}))
                symbols = await symbol_queue.get()
                if not symbols:
                    await asyncio.sleep(SCREENER_REFRESH_SEC)
                    continue
                BATCH_SIZE = 100
                params_batches = [symbols[i:i+BATCH_SIZE] for i in range(0, len(symbols), BATCH_SIZE)]
                for batch in params_batches:
                    params = ",".join([f"T.{s}" for s in batch])
                    await ws.send(json.dumps({"action": "subscribe", "params": params}))
                    await asyncio.sleep(0.2)
                subscribed_symbols = set(symbols)

                if not halt_subscribed:
                    await ws.send(json.dumps({"action": "subscribe", "params": "H.*"}))
                    halt_subscribed = True

                import traceback
                async def ws_recv():
                    print("ws_recv started")
                    async for message in ws:
                        try:
                            print("[DEBUG] RAW MESSAGE:", message)
                            try:
                                payload = json.loads(message)
                            except Exception:
                                print("[DEBUG] Could not decode JSON:", message)
                                continue
                            if isinstance(payload, dict):
                                payload = [payload]
                            elif isinstance(payload, list):
                                pass
                            else:
                                print("[DEBUG] Unexpected payload type:", type(payload), payload)
                                continue
                            for item in payload:
                                if not isinstance(item, dict):
                                    print("[DEBUG] Unexpected item type:", type(item), item)
                                    continue
                                ev = item.get("ev")
                                if ev == "T":
                                    symbol = item.get("sym")
                                    price = float(item.get("p"))
                                    size = float(item.get("s", 0))
                                    ts = item.get("t") / 1000
                                    trade_time = datetime.utcfromtimestamp(ts).replace(tzinfo=pytz.UTC)
                                    await on_trade_event(symbol, price, size, trade_time)
                                elif ev == "H":
                                    await handle_halt_event(item)
                                else:
                                    print("[DEBUG] Unknown event type:", ev, item)
                        except Exception as e:
                            print(f"[DEBUG] !!! ERROR processing WebSocket message: {e}")
                            print(f"[DEBUG] Offending message: {message}")
                            traceback.print_exc()

                async def ws_symbols():
                    print("ws_symbols started")
                    while True:
                        new_symbols = await symbol_queue.get()
                        to_unsubscribe = [s for s in subscribed_symbols if s not in new_symbols]
                        to_subscribe = [s for s in new_symbols if s not in subscribed_symbols]
                        if not to_unsubscribe and not to_subscribe:
                            continue
                        BATCH_SIZE = 100
                        if to_unsubscribe:
                            for i in range(0, len(to_unsubscribe), BATCH_SIZE):
                                remove_params = ",".join([f"T.{s}" for s in to_unsubscribe[i:i+BATCH_SIZE]])
                                await ws.send(json.dumps({"action": "unsubscribe", "params": remove_params}))
                                await asyncio.sleep(0.2)
                        if to_subscribe:
                            for i in range(0, len(to_subscribe), BATCH_SIZE):
                                add_params = ",".join([f"T.{s}" for s in to_subscribe[i:i+BATCH_SIZE]])
                                await ws.send(json.dumps({"action": "subscribe", "params": add_params}))
                                await asyncio.sleep(0.2)
                        subscribed_symbols.clear()
                        subscribed_symbols.update(new_symbols)

                await asyncio.gather(ws_recv(), ws_symbols())
        except Exception as e:
            print(f"[DEBUG] Trade WebSocket error: {e}. Reconnecting soon...")
            await asyncio.sleep(5)

async def main():
    print("main() started.")
    loop = asyncio.get_event_loop()
    setup_signal_handlers(loop)
    symbol_queue = asyncio.Queue()
    init_syms = await fetch_top_penny_symbols()
    print(f"Initial symbols: {init_syms[:10]}... total: {len(init_syms)}")
    await symbol_queue.put(init_syms)
    await asyncio.gather(
        dynamic_symbol_manager(symbol_queue),
        trade_ws(symbol_queue),
        send_scheduled_alerts(),
        news_alerts_task()
    )

if __name__ == "__main__":
    print("Starting real-time penny stock spike scanner ($10 & under, 4am–8pm ET, Mon–Fri) with RVOL, confidence scoring, and keyword-filtered news alerts as SEPARATE alerts (now only for price-moving stocks UP 5%+)...")
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit, asyncio.CancelledError) as e:
        print(f"Bot stopped gracefully. Reason: {e}")
    except Exception as e:
        print(f"Top-level exception: {e}")
