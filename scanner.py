import time
import requests
import random
import schedule
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
last_alert_ts = time.time()

# === ROASTS ===
slow_market_lines = [
    "🕳️ The scanners are silent. The poors must’ve run out of money.",
    "💤 Market is so slow even the scammers took the day off",
    "🔍 No action. Retail must be watching TikTok instead of tickers",
    "📉 Even the algos took a nap. Stay strapped, this quiet won’t last.",
    "📵 I've seen more movement in a nursing home."
]

# === FUNCTIONS ===
def send_telegram_alert(symbol, float_rot, rel_vol, above_vwap):
    global last_alert_ts
    message = f"""
🚨 TARGET ACQUIRED: ${symbol}

📈 Float Rotation: {float_rot:.2f}x
📊 Relative Volume: {rel_vol:.2f}x
🗍 VWAP Position: {'ABOVE ✅' if above_vwap else 'BELOW ❌'}
🕒 Time Triggered: {time.strftime('%I:%M:%S %p')}

💰 Potential momentum ignition in progress. Monitor closely.
"""
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message.strip(), parse_mode="HTML")
        last_alert_ts = time.time()
    except Exception as e:
        print("Telegram error:", e)

def maybe_post_slow_market_update():
    now_ts = time.time()
    if now_ts - last_alert_ts > 1200:  # 20 minutes
        roast = random.choice(slow_market_lines)
        try:
            bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=roast)
        except Exception as e:
            print("Failed to send roast:", e)
        last_alert_ts = now_ts

def send_saturday_roast():
    if datetime.now().weekday() == 5:  # Saturday
        roast = (
            "💸 The broke are out there searching YouTube for 'how to trade' "
            "when all they need is my complete Stock Trading Guide for the Poors."
        )
        try:
            bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=roast)
        except Exception as e:
            print("Failed to send Saturday roast:", e)

# === DATA ===
def fetch_low_float_tickers():
    tickers = []
    page = 1
    while True:
        url = f"https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit=1000&page={page}&apiKey={POLYGON_API_KEY}"
        resp = requests.get(url).json()
        for t in resp.get("results", []):
            if t.get("primary_exchange") in ["XNYS", "XNAS"] and t.get("type") == "CS":
                tickers.append(t["ticker"])
        if "next_url" not in resp:
            break
        page += 1
    return tickers

def calculate_vwap(candles):
    cumulative_vp = sum(c.v * ((c.h + c.l + c.c) / 3) for c in candles)
    cumulative_vol = sum(c.v for c in candles)
    return cumulative_vp / cumulative_vol if cumulative_vol != 0 else 0

def check_volume_spikes(tickers):
    global last_alert_ts
    for symbol in tickers:
        try:
            aggs = client.get_aggs(
                symbol=symbol,
                multiplier=1,
                timespan="minute",
                from_=(datetime.utcnow() - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                to=datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
                limit=30,
                adjusted=True
            )
            candles = list(aggs)
            if len(candles) < 5:
                continue

            avg_vol = sum(c.v for c in candles[:-1]) / len(candles[:-1])
            total_vol = sum(c.v for c in candles)
            rel_vol = total_vol / avg_vol if avg_vol > 0 else 0
            float_shares = 5_000_000
            float_rotation = total_vol / float_shares
            price = candles[-1].c
            vwap = calculate_vwap(candles)
            above_vwap = price > vwap

            cooldown = 300  # 5 mins
            now_ts = time.time()

            if (
                symbol not in last_alert_time or now_ts - last_alert_time[symbol] > cooldown
            ) and float_rotation >= 1.0 and rel_vol >= 2.5 and above_vwap:
                send_telegram_alert(symbol, float_rotation, rel_vol, above_vwap)
                last_alert_time[symbol] = now_ts
        except Exception as e:
            print(f"Error scanning {symbol}:", e)

# === MAIN LOOP ===
schedule.every().saturday.at("12:00").do(send_saturday_roast)

def run_scanner():
    while True:
        try:
            tickers = fetch_low_float_tickers()
            check_volume_spikes(tickers)
            maybe_post_slow_market_update()
            schedule.run_pending()
        except Exception as e:
            print("Main loop error:", e)
        time.sleep(60)

if __name__ == "__main__":
    run_scanner()
