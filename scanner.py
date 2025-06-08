import time
import requests
from datetime import datetime, timedelta
from telegram import Bot
from polygon import RESTClient

# === CONFIG ===
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"
POLYGON_API_KEY = "0rQmpovH_B6UJU14D5HP3fIrj8F_rrDd"

bot = Bot(token=TELEGRAM_BOT_TOKEN)
client = RESTClient(api_key=POLYGON_API_KEY)
last_alert_time = {}

# === ALERT FORMAT ===
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

# === VWAP CALCULATION ===
def calculate_vwap(candles):
    cumulative_vp = sum(c.v * ((c.h + c.l + c.c) / 3) for c in candles)
    cumulative_vol = sum(c.v for c in candles)
    return cumulative_vp / cumulative_vol if cumulative_vol != 0 else 0

# === DYNAMIC TICKER FETCH ===
def fetch_tickers_from_exchange():
    tickers = []
    url = "https://api.polygon.io/v3/reference/tickers"
    params = {
        "market": "stocks",
        "active": "true",
        "limit": 1000,
        "apiKey": POLYGON_API_KEY
    }

    while True:
        try:
            response = requests.get(url, params=params)
            data = response.json()
            for t in data["results"]:
                if (
                    t.get("primary_exchange") in ["XNAS", "XNYS"]
                    and t.get("type") == "CS"
                    and t.get("currency_name") == "USD"
                ):
                    tickers.append(t["ticker"])
            if "next_url" in data:
                url = f"https://api.polygon.io{data['next_url']}&apiKey={POLYGON_API_KEY}"
                params = None  # Already in URL
            else:
                break
        except Exception as e:
            print("Error fetching tickers:", e)
            break
    return tickers

# === MAIN SCANNER ===
def scan_stocks():
    tickers = fetch_tickers_from_exchange()
    now_utc = datetime.utcnow()
    start_time = (now_utc - timedelta(minutes=30)).strftime("%Y-%m-%dT%H:%M:%SZ")
    end_time = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")

    for symbol in tickers:
        try:
            # Get 1-min candles (30-minute window)
            candles = list(client.get_aggs(
                symbol=symbol,
                multiplier=1,
                timespan="minute",
                from_=start_time,
                to=end_time,
                limit=30
            ))
            if len(candles) < 5:
                continue

            # Daily volume for relative volume
            daily = client.get_aggs(symbol, 1, "day", limit=2)
            if not daily or len(daily) < 2:
                continue
            avg_vol = daily[-2].v

            float_shares = 5_000_000  # You can upgrade this with float scraping later
            total_vol = sum(c.v for c in candles)
            float_rotation = total_vol / float_shares
            rel_vol = total_vol / avg_vol if avg_vol > 0 else 0
            price = candles[-1].c
            vwap = calculate_vwap(candles)
            above_vwap = price > vwap

            cooldown = 300
            now_ts = time.time()

            if (
                symbol not in last_alert_time or now_ts - last_alert_time[symbol] > cooldown
            ) and float_rotation >= 1.0 and rel_vol >= 2.5 and above_vwap:
                send_telegram_alert(symbol, float_rotation, rel_vol, above_vwap)
                last_alert_time[symbol] = now_ts

        except Exception as e:
            print(f"Error scanning {symbol}:", e)

# === EXECUTION LOOP ===
if __name__ == "__main__":
    while True:
        try:
            scan_stocks()
        except Exception as e:
            print("Scanner error:", e)
        time.sleep(60)
