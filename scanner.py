import time
import requests
from datetime import datetime, timedelta
from telegram import Bot
from polygon import RESTClient  # Requires: pip install polygon-api-client
import random

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

morning_alert_sent = False

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
