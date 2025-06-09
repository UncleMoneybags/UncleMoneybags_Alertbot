import time
import requests
from datetime import datetime, timedelta
from telegram import Bot
from polygon import RESTClient  # Requires: pip install polygon-api-client

# === CONFIG ===
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
bot = Bot(token=TELEGRAM_BOT_TOKEN)
client = RESTClient(api_key=POLYGON_API_KEY)

# === COOLDOWN TRACKER ===
last_alert_time = {}
last_message_time = time.time()

# === FLOAT LOOKUP ===
def get_float_from_polygon(symbol):
    try:
        url = f"https://api.polygon.io/v3/reference/tickers/{symbol}?apiKey={POLYGON_API_KEY}"
        r = requests.get(url)
        data = r.json()
        return data["results"].get("share_class_shares_outstanding", 5_000_000)
    except Exception as e:
        print(f"Float lookup failed for {symbol}:", e)
        return 5_000_000

# === VWAP CALCULATION ===
def calculate_vwap(candles):
    cumulative_vp = sum(c.v * ((c.h + c.l + c.c) / 3) for c in candles)
    cumulative_vol = sum(c.v for c in candles)
    return cumulative_vp / cumulative_vol if cumulative_vol != 0 else 0

# === ALERT FUNCTION ===
def send_telegram_alert(symbol, float_rot, rel_vol, above_vwap):
    message = f"""
🚨 VOLUME SPIKE: ${symbol}

🔁 Float Rotation: {float_rot:.2f}x
📊 Relative Volume: {rel_vol:.2f}x
📍 VWAP Position: {'ABOVE ✅' if above_vwap else 'BELOW ❌'}
⏱ Time: {time.strftime('%I:%M:%S %p')}
"""
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
    except Exception as e:
        print("Telegram error:", e)

# === TICKERS TO SCAN ===
def fetch_all_tickers():
    try:
        url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit=1000&apiKey={POLYGON_API_KEY}"
        r = requests.get(url)
        data = r.json()
        if "results" not in data:
            raise ValueError("No results key in Polygon response")
        return [item["ticker"] for item in data["results"] if item["primary_exchange"] in ["XNYS", "XNAS"]]
    except Exception as e:
        print("Error fetching tickers:", e)
        return []

# === SCAN LOGIC ===
def check_volume_spikes(tickers):
    global last_message_time
    now_utc = datetime.utcnow()
    start_time = (now_utc - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    cooldown = 300  # 5 minutes
    now_ts = time.time()

    scanned = 0
    max_scans = 10  # throttle control

    for symbol in tickers:
        if scanned >= max_scans:
            break

        try:
            time.sleep(1.5)  # throttle to stay within free-tier limits
            candles = client.get_aggs(
                symbol,
                1,
                "minute",
                start=start_time,
                end=end_time,
                limit=30
            )
            if len(candles) < 5:
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

            if (
                (symbol not in last_alert_time or now_ts - last_alert_time[symbol] > cooldown)
                and float_rotation >= 1.0 and rel_vol >= 2.5 and above_vwap
            ):
                send_telegram_alert(symbol, float_rotation, rel_vol, above_vwap)
                last_alert_time[symbol] = now_ts
                last_message_time = now_ts

            scanned += 1

        except Exception as e:
            print(f"Error scanning {symbol}:", e)

# === RUN LOOP ===
def run_scanner():
    while True:
        try:
            tickers = fetch_all_tickers()
            check_volume_spikes(tickers)
        except Exception as e:
            print("Main loop error:", e)
        time.sleep(90)

if __name__ == "__main__":
    run_scanner()
