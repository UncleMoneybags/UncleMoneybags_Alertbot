
import time
import requests
from telegram import Bot
from polygon import RESTClient  # Requires: pip install polygon-api-client

# === CONFIG ===
TELEGRAM_BOT_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1001234567890"
POLYGON_API_KEY = "0rQmpovH_B6UJU14D5HP3fIrj8F_rrDd"
bot = Bot(token=TELEGRAM_BOT_TOKEN)

# === ALERT FUNCTION ===
def send_telegram_alert(message):
    try:
        bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message, parse_mode="HTML")
    except Exception as e:
        print("Telegram error:", e)

# === TICKER UNIVERSE ===
def fetch_low_float_tickers():
    # Replace this with your own list or API
    return ["GNS", "TOP", "TBLT", "HCDI", "SINT", "PEGY", "USEG"]

# === VWAP CALCULATION ===
def calculate_vwap(candles):
    cumulative_vp = sum(c.v * ((c.h + c.l + c.c) / 3) for c in candles)
    cumulative_vol = sum(c.v for c in candles)
    return cumulative_vp / cumulative_vol if cumulative_vol != 0 else 0

# === SCAN LOGIC ===
def check_volume_spikes(tickers):
    client = RESTClient(api_key=POLYGON_API_KEY)
    for ticker in tickers:
        try:
            # Get last 2 days for avg volume
            daily = client.get_aggs(ticker, 1, "day", limit=2)
            if not daily or len(daily) < 2:
                continue
            avg_vol = daily[-2].v

            # Get intraday candles (e.g., last 10 x 1-minute)
            intraday = client.get_aggs(ticker, 1, "minute", limit=10)
            if not intraday:
                continue
            cur_vol = sum(c.v for c in intraday)
            rel_vol = cur_vol / avg_vol if avg_vol != 0 else 0
            last_price = intraday[-1].c
            vwap = calculate_vwap(intraday)

            if cur_vol > 2 * avg_vol and rel_vol > 2.0 and last_price > vwap:
                message = f"""<b>🚨 Volume Spike Alert</b>
<b>Ticker:</b> {ticker}
<b>Price:</b> ${last_price:.2f}
<b>VWAP:</b> ${vwap:.2f}
<b>Current Vol:</b> {cur_vol}
<b>Avg Vol:</b> {avg_vol}
<b>RelVol:</b> {rel_vol:.2f}
<b>Condition:</b> Price > VWAP, RelVol > 2.0"""
                send_telegram_alert(message)
        except Exception as e:
            print(f"Error scanning {ticker}:", e)

# === RUN LOOP ===
def run_scanner():
    while True:
        try:
            tickers = fetch_low_float_tickers()
            check_volume_spikes(tickers)
        except Exception as e:
            print("Main loop error:", e)
        time.sleep(60)

if __name__ == "__main__":
    run_scanner()
