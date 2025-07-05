import websocket
import json
import pytz
from datetime import datetime
import requests
from collections import deque, defaultdict

# --- CONFIG ---
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
TELEGRAM_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"

PRICE_CUTOFF = 5.00
REL_VOL_LOOKBACK = 10
REL_VOL_THRESHOLD = 2.0
FAST_JUMP_AMOUNT = 0.10

def send_telegram(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    try:
        requests.post(url, data=payload, timeout=10)
    except Exception as e:
        print(f"Telegram send error: {e}")

def is_scan_window():
    # Only scan Mon-Fri, 4am-8pm EST
    eastern = pytz.timezone("US/Eastern")
    now_est = datetime.now(eastern)
    weekday = now_est.weekday()  # Monday=0, Sunday=6
    hour = now_est.hour
    minute = now_est.minute
    if 0 <= weekday <= 4:
        if (hour > 4 and hour < 20):
            return True
        if hour == 4 and minute >= 0:
            return True
        if hour == 20 and minute == 0:
            return True
    return False

class SymbolState:
    def __init__(self):
        self.last_3closes = deque(maxlen=4)  # [oldest, ..., newest] for price spike
        self.last_3_closes_ts = deque(maxlen=4)
        self.vol_history = deque(maxlen=REL_VOL_LOOKBACK + 1)  # for relative volume
        self.last_price_spike_ts = 0
        self.last_relvol_spike_ts = 0
        self.last_fastjump_ts = 0

def on_open(ws):
    print("WebSocket opened and subscribing...")
    ws.send(json.dumps({"action": "auth", "params": POLYGON_API_KEY}))
    ws.send(json.dumps({"action": "subscribe", "params": "AM.*"})) # AM.* = all minute aggregates

symbol_states = defaultdict(SymbolState)

def on_message(ws, message):
    if not is_scan_window():
        return
    bars = json.loads(message)
    if not isinstance(bars, list):
        bars = [bars]
    for bar in bars:
        if bar.get("ev") != "AM":
            continue
        symbol = bar["sym"]
        close = bar.get("c")
        openp = bar.get("o")
        ts = bar.get("s", bar.get("t"))  # s is epoch for the candle; fallback to t if missing
        if close is None or openp is None or close > PRICE_CUTOFF or close == 0:
            continue

        state = symbol_states[symbol]

        # --- 1. PRICE SPIKE: 3 consecutive higher closes ---
        state.last_3closes.append(close)
        state.last_3_closes_ts.append(ts)
        if len(state.last_3closes) == 4:
            # [c[0], c[1], c[2], c[3]] oldest to newest
            if (state.last_3closes[1] > state.last_3closes[0] and
                state.last_3closes[2] > state.last_3closes[1] and
                state.last_3closes[3] > state.last_3closes[2] and
                state.last_3_closes_ts[3] != state.last_price_spike_ts):
                msg = (
                    f"🚀 <b>{symbol}</b> PRICE SPIKE!\n"
                    f"3 green closes: {state.last_3closes[0]:.2f} → {state.last_3closes[1]:.2f} → {state.last_3closes[2]:.2f} → {state.last_3closes[3]:.2f}\n"
                    f"Latest price: ${state.last_3closes[3]:.2f}"
                )
                send_telegram(msg)
                state.last_price_spike_ts = state.last_3_closes_ts[3]

        # --- 2. RELATIVE VOLUME SPIKE ---
        curr_vol = bar.get("v", 0)
        state.vol_history.append(curr_vol)
        if len(state.vol_history) > REL_VOL_LOOKBACK:
            avg_vol = sum(list(state.vol_history)[-REL_VOL_LOOKBACK-1:-1]) / REL_VOL_LOOKBACK
            rel_vol = curr_vol / avg_vol if avg_vol else 0
            if rel_vol >= REL_VOL_THRESHOLD and ts != state.last_relvol_spike_ts:
                msg = (
                    f"🔥 <b>{symbol}</b> RELATIVE VOLUME SPIKE!\n"
                    f"Volume {curr_vol} ({rel_vol:.1f}x last {REL_VOL_LOOKBACK}min avg)\n"
                    f"Price: ${close:.2f}"
                )
                send_telegram(msg)
                state.last_relvol_spike_ts = ts

        # --- 3. FAST PRICE JUMP ---
        price_jump = close - openp
        if (close > openp and price_jump >= FAST_JUMP_AMOUNT and ts != state.last_fastjump_ts):
            msg = (
                f"⚡ <b>{symbol}</b> FAST PRICE JUMP!\n"
                f"1min candle: Open ${openp:.2f} → Close ${close:.2f} (+${price_jump:.2f})"
            )
            send_telegram(msg)
            state.last_fastjump_ts = ts

def on_error(ws, error):
    print("WebSocket error:", error)

def on_close(ws, close_status_code, close_msg):
    print("WebSocket closed:", close_status_code, close_msg)

def run_ws():
    ws = websocket.WebSocketApp(
        "wss://socket.polygon.io/stocks",
        on_open=on_open,
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.run_forever()

if __name__ == "__main__":
    print("Starting real-time WebSocket penny stock bot with separate alerts!")
    run_ws()
