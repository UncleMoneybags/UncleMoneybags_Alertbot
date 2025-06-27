import requests
import time
from datetime import datetime, timedelta
import os
import json
import threading
from websocket import WebSocketApp

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
    """
    Dynamically fetch tickers under $10 with high recent volume using Polygon Advanced plan.
    """
    url = (
        f"https://api.polygon.io/v3/reference/tickers"
        f"?market=stocks&active=true&order=desc&limit=1000&apiKey={POLYGON_API_KEY}"
    )
    resp = requests.get(url)
    tickers = []
    if resp.status_code == 200:
        data = resp.json().get("results", [])
        # Only common stocks; filter out ETFs, warrants, etc.
        symbols = [t["ticker"] for t in data if t.get("type") == "CS"]
        # For each symbol, get the previous close and volume
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
            if len(tickers) >= SCREENER_LIMIT * 2:  # Slightly overfetch in case some fail
                break
        # Sort by volume (as a proxy for liquidity/volatility)
        tickers = sorted(tickers, key=lambda x: x[1], reverse=True)[:SCREENER_LIMIT]
        final = [t[0] for t in tickers]
        print(f"[DEBUG] Screener selected: {final}")
        return final
    else:
        print("[ERROR] Polygon screener failed, using fallback static list.")
        return ["SNTG", "GNS", "TBLT", "TOP", "HUBC", "HUDI", "CYN", "PEGY", "RAYA", "COMS", "PMNT"]

def send_telegram_alert(message):
    url = f'https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage'
    try:
        requests.post(url, data={'chat_id': TELEGRAM_CHAT_ID, 'text': message})
    except Exception as e:
        print(f"[ERROR] Error sending Telegram message: {e}")

def load_seen_news_ids(filename=SEEN_NEWS_FILE):
    if not os.path.exists(filename):
        return set()
    with open(filename, "r") as f:
        return set(line.strip() for line in f)

def save_seen_news_id(news_id, filename=SEEN_NEWS_FILE):
    with open(filename, "a") as f:
        f.write(f"{news_id}\n")

def scan_for_news(tickers, seen_news_ids):
    since = (datetime.utcnow() - timedelta(minutes=NEWS_LOOKBACK_MINUTES)).isoformat()[:16]
    for ticker in tickers:
        url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&published_utc.gte={since}&limit=5&apiKey={POLYGON_API_KEY}"
        try:
            resp = requests.get(url, timeout=8)
            news_items = resp.json().get("results", [])
            for news in news_items:
                if news["id"] not in seen_news_ids:
                    seen_news_ids.add(news["id"])
                    save_seen_news_id(news["id"])
                    headline = news["title"]
                    url_link = news.get("article_url", "")
                    alert = f"📰 NEWS: {ticker} - {headline}\n{url_link}"
                    send_telegram_alert(alert)
        except Exception as e:
            print(f"[ERROR] News error {ticker}: {e}")

class RealTimeScanner:
    def __init__(self, tickers):
        self.tickers = tickers
        self.last_alerts = {}
        self.recent_minute_vols = {ticker: [] for ticker in tickers}

    def on_message(self, ws, message):
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
                # Halt detection (code "H" or "U" events in Polygon)
                elif event.get("ev") == "status":
                    ticker = event.get("sym")
                    status = event.get("status")
                    if status in ("halt", "halted", "T12", "H10", "H11", "H4", "H9", "U3", "U4"):
                        msg = f"⛔️ HALT: {ticker} - status: {status}"
                        send_telegram_alert(msg)
        except Exception as e:
            print(f"[ERROR][WS] {e}")

    def on_error(self, ws, error):
        print(f"[ERROR][WS] {error}")

    def on_close(self, ws, close_status_code, close_msg):
        print(f"[WS] Connection closed: {close_status_code} {close_msg}")

    def on_open(self, ws):
        auth_data = {
            "action": "auth",
            "params": POLYGON_API_KEY
        }
        ws.send(json.dumps(auth_data))
        sub_str = ",".join([f"A.{ticker}" for ticker in self.tickers])
        ws.send(json.dumps({"action": "subscribe", "params": sub_str}))
        ws.send(json.dumps({"action": "subscribe", "params": "status"}))

    def run(self):
        ws_url = "wss://socket.polygon.io/stocks"
        ws = WebSocketApp(ws_url,
                          on_open=self.on_open,
                          on_message=self.on_message,
                          on_error=self.on_error,
                          on_close=self.on_close)
        ws.run_forever(ping_interval=30, ping_timeout=10)

if __name__ == "__main__":
    tickers = fetch_volatile_tickers()
    print(f"[INFO] Will monitor: {tickers}")
    seen_news_ids = load_seen_news_ids()
    scanner = RealTimeScanner(tickers)
    ws_thread = threading.Thread(target=scanner.run, daemon=True)
    ws_thread.start()
    while True:
        scan_for_news(tickers, seen_news_ids)
        time.sleep(60)
