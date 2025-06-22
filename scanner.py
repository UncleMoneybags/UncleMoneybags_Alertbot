import time
import requests
from datetime import datetime, timedelta
import pytz
from telegram import Bot
from polygon import RESTClient  # Requires: pip install polygon-api-client

# === CONFIG ===
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
bot = Bot(token=TELEGRAM_BOT_TOKEN)
client = RESTClient(api_key=POLYGON_API_KEY)

EASTERN = pytz.timezone('US/Eastern')
SCAN_START_HOUR = 4  # 4:00 AM ET
SCAN_END_HOUR = 20   # 8:00 PM ET

last_alert_time = {}
last_message_time = time.time()

def is_market_hours():
    now = datetime.now(EASTERN)
    return now.weekday() < 5 and SCAN_START_HOUR <= now.hour < SCAN_END_HOUR

def sleep_until_market_open():
    now = datetime.now(EASTERN)
    if now.hour >= SCAN_END_HOUR or now.weekday() >= 5:
        # Sleep until next weekday 4am
        days_ahead = (7 - now.weekday()) % 7
        if now.hour >= SCAN_END_HOUR or now.weekday() == 4:  # Friday after 8pm or Sat/Sun
            days_ahead = (7 - now.weekday() + 0) % 7  # Next Monday
            if days_ahead == 0:
                days_ahead = 7
        else:
            days_ahead = 1
        next_open = (now + timedelta(days=days_ahead)).replace(hour=SCAN_START_HOUR, minute=0, second=0, microsecond=0)
    else:
        # Sleep until 4am today
        next_open = now.replace(hour=SCAN_START_HOUR, minute=0, second=0, microsecond=0)
        if now > next_open:
            next_open += timedelta(days=1)
    sleep_seconds = (next_open - now).total_seconds()
    print(f"Sleeping {int(sleep_seconds)} seconds until next market open at {next_open}")
    time.sleep(sleep_seconds)

def get_float_from_polygon(symbol, retries=3):
    url = f"https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={POLYGON_API_KEY}"
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url)
            data = r.json()
            return data["results"].get("share_class_shares_outstanding", 5_000_000)
        except Exception as e:
            print(f"Float lookup failed for {symbol}, attempt {attempt}: {e}")
            if hasattr(r, 'text'):
                print("Polygon response:", r.text)
            if attempt < retries:
                time.sleep(2 ** attempt)
    return 5_000_000

def calculate_vwap(candles):
    cumulative_vp = sum(c.v * ((c.h + c.l + c.c) / 3) for c in candles)
    cumulative_vol = sum(c.v for c in candles)
    return cumulative_vp / cumulative_vol if cumulative_vol != 0 else 0

def send_telegram_alert(symbol, float_rot, rel_vol, above_vwap, reason=""):
    message = f"""
🚨 ALERT: ${symbol}

Reason: {reason}
🔁 Float Rotation: {float_rot:.2f}x
📊 Relative Volume: {rel_vol:.2f}x
📍 VWAP Position: {'ABOVE ✅' if above_vwap else 'BELOW ❌'}
⏱ Time: {time.strftime('%I:%M:%S %p')}
"""
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
    except Exception as e:
        print("Telegram error:", e)

def fetch_all_tickers():
    try:
        url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit=1000&apiKey={POLYGON_API_KEY}"
        r = requests.get(url)
        data = r.json()
        if "results" not in data:
            raise ValueError("No results key in Polygon response")
        return [item["ticker"] for item in data["results"] if item.get("primary_exchange") in ["XNYS", "XNAS"]]
    except Exception as e:
        print("Error fetching tickers:", e)
        return []

def check_volume_spikes(tickers):
    global last_message_time
    now_utc = datetime.utcnow()
    start_time = int((now_utc - timedelta(minutes=30)).timestamp() * 1000)
    end_time = int(now_utc.timestamp() * 1000)
    cooldown = 300  # 5 minutes
    now_ts = time.time()

    scanned = 0
    max_scans = 1  # Only scan 1 ticker per loop (slow down for free plan)

    # Track alert times per symbol and condition
    global last_alert_time
    if not isinstance(last_alert_time, dict):
        last_alert_time = {}

    for symbol in tickers:
        if scanned >= max_scans:
            break

        candles = []
        for attempt in range(1, 4):
            try:
                time.sleep(15)
                candles = client.get_aggs(
                    symbol,
                    1,
                    "minute",
                    from_=start_time,
                    to=end_time,
                    limit=30
                )
                break
            except Exception as e:
                err_msg = str(e)
                if 'NOT_AUTHORIZED' in err_msg or 'Your plan doesn\'t include this data timeframe' in err_msg:
                    print(f"Skipping {symbol}: {err_msg}")
                    candles = []
                    break
                print(f"Error scanning {symbol}, attempt {attempt}: {err_msg}")
                if attempt < 3:
                    time.sleep(2 ** attempt)
                else:
                    candles = []
        if not candles or len(candles) < 5:
            continue

        avg_vol = sum(c.v for c in candles[:-1]) / len(candles[:-1])
        if avg_vol < 100000:
            continue

        total_vol = sum(c.v for c in candles)
        if total_vol < 500000:
            continue

        rel_vol = total_vol / avg_vol if avg_vol > 0 else 0
        float_shares = get_float_from_polygon(symbol)
        if float_shares > 20_000_000:
            continue

        float_rotation = total_vol / float_shares
        price = candles[-1].c
        vwap = calculate_vwap(candles)
        above_vwap = price > vwap

        triggered_conditions = []

        # Separate cooldown for each condition per symbol
        symbol_alerts = last_alert_time.setdefault(symbol, {})

        # Float rotation alert
        if float_rotation >= 1.0 and now_ts - symbol_alerts.get('float_rotation', 0) > cooldown:
            send_telegram_alert(symbol, float_rotation, rel_vol, above_vwap, reason="Float Rotation ≥ 1.0x")
            symbol_alerts['float_rotation'] = now_ts
            triggered_conditions.append('float_rotation')

        # Relative volume alert
        if rel_vol >= 2.5 and now_ts - symbol_alerts.get('rel_vol', 0) > cooldown:
            send_telegram_alert(symbol, float_rotation, rel_vol, above_vwap, reason="Relative Volume ≥ 2.5x")
            symbol_alerts['rel_vol'] = now_ts
            triggered_conditions.append('rel_vol')

        # Above VWAP alert
        if above_vwap and now_ts - symbol_alerts.get('above_vwap', 0) > cooldown:
            send_telegram_alert(symbol, float_rotation, rel_vol, above_vwap, reason="Price ABOVE VWAP")
            symbol_alerts['above_vwap'] = now_ts
            triggered_conditions.append('above_vwap')

        if triggered_conditions:
            last_message_time = now_ts

        scanned += 1

def run_scanner():
    while True:
        if not is_market_hours():
            print("Outside of scanning hours. Sleeping until market open.")
            sleep_until_market_open()
            continue
        try:
            tickers = fetch_all_tickers()
            check_volume_spikes(tickers)
        except Exception as e:
            print("Main loop error:", e)
        # Slow loop, scan 1 ticker per ~90 seconds.
        time.sleep(90)

if __name__ == "__main__":
    bot.send_message(chat_id=TELEGRAM_CHAT_ID, text="Scanner bot has started and will scan only during market hours!")
    run_scanner()
