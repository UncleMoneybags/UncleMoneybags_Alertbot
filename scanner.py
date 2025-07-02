import requests
import time
from datetime import datetime, timedelta
import os
import json
import threading

# For Eastern timezone support:
try:
    from zoneinfo import ZoneInfo
    TZ_NY = ZoneInfo("America/New_York")
    print("[DEBUG] Using zoneinfo for America/New_York", flush=True)
except ImportError:
    import pytz
    TZ_NY = pytz.timezone("US/Eastern")
    print("[DEBUG] Using pytz for US/Eastern", flush=True)

from websocket import WebSocketApp

print("[DEBUG] SCANNER.PY IS RUNNING", flush=True)

# === CONFIGURATION ===
POLYGON_API_KEY = "VmF1boger0pp2M7gV5HboHheRbplmLi5"
TELEGRAM_TOKEN = "8019146040:AAGRj0hJn2ZUKj1loEEYdy0iuij6KFbSPSc"
TELEGRAM_CHAT_ID = "-1002266463234"

PRICE_MAX = 10.0
VOLUME_SPIKE_MULT = 1.1
MIN_AVG_VOLUME = 1
NEWS_LOOKBACK_MINUTES = 60
SEEN_NEWS_FILE = "seen_news.txt"
SCREENER_LIMIT = 50  # How many top stocks to monitor

def fetch_volatile_tickers():
    print("[DEBUG] Entered fetch_volatile_tickers", flush=True)
    url = (
        f"https://api.polygon.io/v3/reference/tickers"
        f"?market=stocks&active=true&order=desc&limit=1000&apiKey={POLYGON_API_KEY}"
    )
    try:
        resp = requests.get(url)
        tickers = []
        if resp.status_code == 200:
            data = resp.json().get("results", [])
            print(f"[DEBUG] Polygon returned {len(data)} tickers", flush=True)
            symbols = [t["ticker"] for t in data if t.get("type") == "CS"]
            for symbol in symbols:
                detail_url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev?adjusted=true&apiKey={POLYGON_API_KEY}"
                det_resp = requests.get(detail_url)
                if det_resp.status_code == 200:
                    results = det_resp.json().get("results", [])
                    if results:
                        close = results[0].get("c", 0)
                        volume = results[0].get("v", 0)
                        if close and close <= PRICE_MAX and volume:
                            tickers.append((symbol, volume))
                if len(tickers) >= SCREENER_LIMIT * 2:
                    break
            tickers = sorted(tickers, key=lambda x: x[1], reverse=True)[:SCREENER_LIMIT]
            final = [t[0] for t in tickers]
            print(f"[DEBUG] Screener selected: {final}", flush=True)
            return final
        else:
            print(f"[ERROR] Polygon screener failed, status {resp.status_code}", flush=True)
            return ["SNTG", "GNS", "TBLT", "TOP", "HUBC", "HUDI", "CYN", "PEGY", "RAYA", "COMS", "PMNT"]
    except Exception as e:
        print(f"[ERROR] Exception in fetch_volatile_tickers: {e}", flush=True)
        return ["SNTG", "GNS", "TBLT", "TOP", "HUBC", "HUDI", "CYN", "PEGY", "RAYA", "COMS", "PMNT"]

def send_telegram_alert(message):
    url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
    try:
        resp = requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': message})
        print(f"[DEBUG] Telegram alert sent: {message} | Status: {resp.status_code}", flush=True)
    except Exception as e:
        print(f"[ERROR] Error sending Telegram message: {e}", flush=True)

def load_seen_news_ids(filename=SEEN_NEWS_FILE):
    print("[DEBUG] Loading seen news IDs", flush=True)
    if not os.path.exists(filename):
        return set()
    with open(filename, "r") as f:
        lines = set(line.strip() for line in f)
        print(f"[DEBUG] Loaded {len(lines)} seen news IDs", flush=True)
        return lines

def save_seen_news_id(news_id, filename=SEEN_NEWS_FILE):
    with open(filename, "a") as f:
        f.write(f"{news_id}\n")
    print(f"[DEBUG] Saved news ID: {news_id}", flush=True)

def scan_for_news(tickers, seen_news_ids, bad_tickers=None):
    if bad_tickers is None:
        bad_tickers = set()
    now_ny = datetime.now(TZ_NY)
    print(f"[DEBUG] Scanning news for tickers: {tickers} at Eastern time {now_ny}", flush=True)
    since = (now_ny - timedelta(minutes=NEWS_LOOKBACK_MINUTES)).astimezone().isoformat()[:16]

    for ticker in tickers:
        if ticker in bad_tickers:
            continue  # Skip tickers that already failed

        url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&published_utc.gte={since}&limit=5&apiKey={POLYGON_API_KEY}"
        try:
            resp = requests.get(url, timeout=8)
            if resp.status_code == 400:
                print(f"[ERROR] News API failed for {ticker}: 400 (Bad request) - removing from future lookups.", flush=True)
                bad_tickers.add(ticker)
                continue
            if resp.status_code != 200:
                print(f"[ERROR] News API failed for {ticker}: {resp.status_code} {resp.text}", flush=True)
                continue
            news_items = resp.json().get("results", [])
            print(f"[DEBUG] {ticker}: {len(news_items)} news items", flush=True)
            for news in news_items:
                if news["id"] not in seen_news_ids:
                    seen_news_ids.add(news["id"])
                    save_seen_news_id(news["id"])
                    headline = news["title"]
                    url_link = news.get("article_url", "")
                    alert = f"📰 NEWS: {ticker} - {headline}\n{url_link}"
                    send_telegram_alert(alert)
        except Exception as e:
            print(f"[ERROR] News error {ticker}: {e}", flush=True)

class RealTimeScanner:
    def __init__(self, tickers):
        print("[DEBUG] RealTimeScanner init", flush=True)
        self.tickers = tickers
        self.last_alerts = {}
        self.recent_minute_vols = {ticker: [] for ticker in tickers}
        self.ws = None
        self.active = True

    def on_message(self, ws, message):
        print(f"[WS] Message received at {datetime.now(TZ_NY)} (Eastern): {message[:100]}", flush=True)
        try:
            data = json.loads(message)
            for event in data:
                # Volume spike detection (AM = Aggregate per Minute)
                if event.get("ev") == "AM":
                    ticker = event["sym"]
                    price = event["c"]
                    curr_vol = event["v"]
                    if price > PRICE_MAX:
                        continue
                    vols = self.recent_minute_vols.get(ticker, [])
                    if len(vols) >= 3:
                        vols.pop(0)
                    vols.append(curr_vol)
                    self.recent_minute_vols[ticker] = vols
                    avg_prev_vol = sum(vols[:-1]) / (len(vols)-1) if len(vols) > 1 else 0
                    if len(vols) >= 2 and curr_vol > avg_prev_vol * VOLUME_SPIKE_MULT and avg_prev_vol > MIN_AVG_VOLUME:
                        last_key = (price, curr_vol)
                        if self.last_alerts.get(ticker) != last_key:
                            msg = (f"🚨 {ticker}: Price ${price:.2f} | {curr_vol:,} 1min vol (>1.1x avg {int(avg_prev_vol):,})\n"
                                   f"https://finance.yahoo.com/quote/{ticker}")
                            send_telegram_alert(msg)
                            self.last_alerts[ticker] = last_key
                            print(f"[DEBUG] Volume spike alert sent for {ticker}", flush=True)
                # Halt detection (code "H" or "U" events in Polygon)
                elif event.get("ev") == "status":
                    ticker = event.get("sym")
                    status = event.get("status")
                    if status in ("halt", "halted", "T12", "H10", "H11", "H4", "H9", "U3", "U4"):
                        msg = f"⛔️ HALT: {ticker} - status: {status}"
                        send_telegram_alert(msg)
                        print(f"[DEBUG] Halt alert sent for {ticker} ({status})", flush=True)
        except Exception as e:
            print(f"[ERROR][WS] {e}", flush=True)

    def on_error(self, ws, error):
        print(f"[ERROR][WS] {error}", flush=True)

    def on_close(self, ws, close_status_code, close_msg):
        print(f"[WS] Connection closed: {close_status_code} {close_msg}", flush=True)

    def on_open(self, ws):
        print(f"[WS] Connected to Polygon WebSocket at {datetime.now(TZ_NY)} (Eastern)", flush=True)
        try:
            auth_data = {
                "action": "auth",
                "params": POLYGON_API_KEY
            }
            ws.send(json.dumps(auth_data))
            sub_str = ",".join([f"A.{ticker}" for ticker in self.tickers])
            ws.send(json.dumps({"action": "subscribe", "params": sub_str}))
            ws.send(json.dumps({"action": "subscribe", "params": "status"}))
            print(f"[DEBUG] Subscribed to tickers: {self.tickers}", flush=True)
        except Exception as e:
            print(f"[ERROR][WS][on_open] {e}", flush=True)

    # Bulletproof run() with reconnect logic
    def run(self):
        print("[DEBUG] RealTimeScanner run() called", flush=True)
        ws_url = "wss://socket.polygon.io/stocks"
        while self.active:
            try:
                self.ws = WebSocketApp(ws_url,
                                  on_open=self.on_open,
                                  on_message=self.on_message,
                                  on_error=self.on_error,
                                  on_close=self.on_close)
                print("[DEBUG] WebSocketApp initialized, starting run_forever...", flush=True)
                self.ws.run_forever(ping_interval=30, ping_timeout=10)
                print("[WS] run_forever exited, will attempt reconnect in 10 seconds", flush=True)
            except Exception as e:
                print(f"[ERROR][WS][run] {e}", flush=True)
            time.sleep(10)  # Wait before reconnecting

def within_scan_window():
    # Use Eastern time for window check
    now = datetime.now(TZ_NY)
    print(f"[DEBUG] Server datetime now (Eastern): {now}", flush=True)
    # Monday is 0, Sunday is 6
    if now.weekday() >= 5:
        print("[DEBUG] Today is weekend. Not scanning.", flush=True)
        return False
    scan_start = now.replace(hour=4, minute=0, second=0, microsecond=0)
    scan_end = now.replace(hour=20, minute=0, second=0, microsecond=0)
    in_window = scan_start <= now <= scan_end
    print(f"[DEBUG] Scan window check (Eastern): {in_window}", flush=True)
    return in_window

if __name__ == "__main__":
    print("[DEBUG] In __main__ block", flush=True)
    try:
        tickers = fetch_volatile_tickers()
        print(f"[INFO] Will monitor: {tickers}", flush=True)
        seen_news_ids = load_seen_news_ids()
        scanner = RealTimeScanner(tickers)
        ws_thread = None
        bad_tickers = set()

        while True:
            print(f"[DEBUG] Top of main loop | Heartbeat {datetime.now(TZ_NY)}", flush=True)
            if within_scan_window():
                if ws_thread is None or not ws_thread.is_alive():
                    ws_thread = threading.Thread(target=scanner.run, daemon=True)
                    ws_thread.start()
                    print("[INFO] WebSocket thread started.", flush=True)
                scan_for_news(tickers, seen_news_ids, bad_tickers)
            else:
                print("[INFO] Outside scan window. Bot sleeping...", flush=True)
                time.sleep(60)
            time.sleep(60)
    except Exception as e:
        print(f"[ERROR] Exception in main: {e}", flush=True)
