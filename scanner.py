import time
import requests
from datetime import datetime, timedelta
from telegram import Bot
from polygon import RESTClient  # Requires: pip install polygon-api-client

# === CONFIG ===
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"
POLYGON_API_KEY = "0rQmpovH_B6UJU14D5HP3fIrj8F_rrDd"
bot = Bot(token=TELEGRAM_BOT_TOKEN)
client = RESTClient(api_key=POLYGON_API_KEY)

# === COOLDOWN TRACKER ===
last_alert_time = {}
last_message_time = time.time()

# === ROAST MODE ===
roast_lines = [
    "📉 Markets so dry even jesus couldn't make wine.",
    "⏳ Still scanning... even the poors are asleep.",
    "💀 Dead volume. Is this a market or a cemetery?",
    "🫠 Float's not rotating, it's evaporating.",
    "📪 Tape's so empty, I thought it was Sunday."
]

saturday_roast = "📚 The broke people are scanning for next week bangers... when all they need is my Stock Guide For The Poors book."

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
🚨 TARGET ACQUIRED: ${symbol}

📈 Float Rotation: {float_rot:.2f}x
📊 Relative Volume: {rel_vol:.2f}x
📍 VWAP Position: {'ABOVE ✅' if above_vwap else 'BELOW ❌'}
🕒 Time Triggered: {time.strftime('%I:%M:%S %p')}

💰 Potential momentum ignition in progress. Monitor closely.
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
        return [item["ticker"] for item in data["results"] if item["primary_exchange"] in ["XNYS", "XNAS"]]
    except Exception as e:
        print("Error fetching tickers:", e)
        return ["GME", "CVNA", "AI", "NVDA"]

# === SCAN LOGIC ===
def check_volume_spikes(tickers):
    global last_message_time
    now_utc = datetime.utcnow()
    start_time = (now_utc - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
    cooldown = 300  # 5 minutes
    now_ts = time.time()

    for symbol in tickers:
        try:
            aggs = client.get_aggs(
                symbol=symbol,
                multiplier=1,
                timespan="minute",
                from_=start_time,
                to=end_time,
                limit=30
            )
            candles = list(aggs)
            if len(candles) < 5:
                continue

            avg_vol = sum(c.v for c in candles[:-1]) / len(candles[:-1])
            total_vol = sum(c.v for c in candles)
            rel_vol = total_vol / avg_vol if avg_vol > 0 else 0
            float_shares = get_float_from_polygon(symbol)
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
        except Exception as e:
            print(f"Error scanning {symbol}:", e)

# === ROAST CHECK ===
def maybe_send_roast():
    global last_message_time
    now = time.time()
    if now - last_message_time >= 1200:  # 20 min
        roast = random.choice(roast_lines)
        try:
            bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=roast)
            last_message_time = now
        except Exception as e:
            print("Roast send failed:", e)

# === SATURDAY ROAST ===
def saturday_check():
    now = datetime.now()
    if now.weekday() == 5 and now.hour == 12 and now.minute == 0:
        try:
            bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=saturday_roast)
        except Exception as e:
            print("Saturday roast failed:", e)

# === RUN LOOP ===
def run_scanner():
    while True:
        try:
            tickers = fetch_all_tickers()
            check_volume_spikes(tickers)
            maybe_send_roast()
            saturday_check()
        except Exception as e:
            print("Main loop error:", e)
        time.sleep(60)

# === TEST ALERT ===
def send_test_alert():
    try:
        bot.send_message(
            chat_id=TELEGRAM_CHAT_ID,
            text="🚨 TEST ALERT: Uncle Moneybags is watching. The tape better wake up.",
            parse_mode="HTML"
        )
        print("✅ Test alert sent.")
    except Exception as e:
        print("Test alert failed:", e)

# Uncomment to send a test
# send_test_alert()

if __name__ == "__main__":
    run_scanner()
