# trade_signal_manager.py
# Updated TradeSignalManager with dynamic technical TP levels (PDH, PMH, Fib extensions, psychological levels)

import os
import json
import asyncio
import aiohttp
import uuid
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, List, Any, Tuple
from dataclasses import dataclass, asdict, field
import logging

# Configure logger based on environment; avoid forcing basicConfig at import which can be overridden downstream
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

# =============================================================================
# Configuration (tweak as needed)
# =============================================================================

BASE_STOP_LOSS_PCT = 0.05
BASE_TAKE_PROFIT_PCT = 0.10

TRAILING_ACTIVATION_PCT = 0.10
TRAILING_DISTANCE_PCT = 0.08

RVOL_TP_MULTIPLIER = 0.5
MAX_RVOL_MULTIPLIER = 4.0

PRICE_THRESHOLD_LOW = 5.0
PRICE_THRESHOLD_HIGH = 20.0

ATR_PERIODS = 14
ATR_TP_MULTIPLIER = 2.0
ATR_SL_MULTIPLIER = 1.5

MAX_OPEN_POSITIONS = 50
POSITION_TIMEOUT_HOURS = 4

DATA_DIR = os.environ.get("DATA_DIR", "./data")
TRADES_FILE = os.path.join(DATA_DIR, "open_trades.json")
TRADE_LOG_FILE = os.path.join(DATA_DIR, "trade_log.csv")

ALLOW_MULTIPLE_POSITIONS_PER_SYMBOL = True

CONFLUENCE_TOLERANCE = 0.005
FIB_EXTENSIONS = [1.618, 2.0, 2.618]
FIB_RETRACEMENTS = [0.382, 0.50, 0.618]  # Entry levels (pullback zones)
MIN_TP_DISTANCE_PCT = 0.02

# =============================================================================
# R:R Guardrails - Professional Day Trading Standards for $0.10-$15 stocks
# =============================================================================

MIN_R_MULTIPLES = [1.0, 2.0, 3.0]
MAX_R_MULTIPLES = [4.0, 6.0, 8.0]
MAX_PCT_FROM_ENTRY = [0.35, 0.60, 0.90]
MIN_RISK_FLOOR_PCT = 0.005  # 0.5% of entry price minimum

# =============================================================================
# Data classes
# =============================================================================


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


@dataclass
class OpenPosition:
    id: str
    symbol: str
    entry_price: float
    entry_time: str
    stop_loss: float
    take_profit: float
    tp1: Optional[float] = None
    tp2: Optional[float] = None
    tp3: Optional[float] = None
    trailing_stop: Optional[float] = None
    highest_price: float = 0.0
    rvol: float = 1.0
    atr: Optional[float] = None
    alert_type: str = ""
    ml_score: Optional[float] = None
    trailing_active: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OpenPosition":
        allowed = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in data.items() if k in allowed}
        return cls(**filtered)


@dataclass
class ClosedTrade:
    symbol: str
    entry_price: float
    exit_price: float
    entry_time: str
    exit_time: str
    pnl_pct: float
    exit_reason: str
    alert_type: str
    ml_score: Optional[float]
    duration_minutes: int


# (TakeProfitPlanner and DynamicTargetCalculator unchanged — omitted in this snippet for brevity)
# ... (copy existing implementations back into file, unchanged) ...
# For completeness include the full content in your repo; below we continue with Manager class, but keep the planner/calculator code identical.

# =============================================================================
# Manager
# =============================================================================

class TradeSignalManager:
    def __init__(
        self,
        telegram_token: Optional[str] = None,
        telegram_chat_id: Optional[str] = None,
        discord_webhook_url: Optional[str] = None,
        http_session: Optional[aiohttp.ClientSession] = None,
        signals_chat_id: Optional[str] = None,
    ):
        self.telegram_token = telegram_token
        self.telegram_chat_id = telegram_chat_id
        self.signals_chat_id = signals_chat_id or telegram_chat_id
        self.discord_webhook_url = discord_webhook_url
        self.http_session = http_session
        self._owns_session = False

        # positions stored by id
        self.positions: Dict[str, OpenPosition] = {}
        # index symbol -> set of ids (for quick symbol lookups)
        self._symbol_index: Dict[str, set] = {}

        # use the dynamic calculator (kept as before)
        # self.calculator = DynamicTargetCalculator()
        # (ensure you preserve the existing initialization for calculator)
        # For compatibility, if DynamicTargetCalculator is used elsewhere, leave as-is.

        os.makedirs(DATA_DIR, exist_ok=True)

        # async lock for concurrency safety
        self._lock = asyncio.Lock()

        # load persisted positions (non-blocking)
        self._load_positions_sync()

    async def initialize(self):
        if self.http_session is None:
            self.http_session = aiohttp.ClientSession()
            self._owns_session = True

    async def cleanup(self):
        await self._save_positions()
        if self._owns_session and self.http_session:
            await self.http_session.close()

    # persistence functions unchanged...
    # ... (copy existing persistence functions here) ...

    async def handle_alert(
        self,
        symbol: str,
        price: float,
        rvol: float,
        alert_type: str,
        ml_score: Optional[float] = None,
        atr: Optional[float] = None,
        recent_candles: Optional[List[Dict[str, float]]] = None,
        timestamp: Optional[datetime] = None,
        pdh: Optional[float] = None,
        pdl: Optional[float] = None,
        pmh: Optional[float] = None,
        pml: Optional[float] = None,
        vwap: Optional[float] = None,
        ema8: Optional[float] = None,
        ema21: Optional[float] = None,
        session_high: Optional[float] = None,
        opening_range_high: Optional[float] = None,
    ) -> Optional[OpenPosition]:
        now = timestamp or datetime.now(timezone.utc)
        entry_time = now.isoformat().replace("+00:00", "Z")

        # ENTRY LOG — helps confirm alerts reach the manager
        logger.debug("[TradeManager] handle_alert called: symbol=%s price=%s rvol=%s alert_type=%s", symbol, price, rvol, alert_type)

        async with self._lock:
            if len(self.positions) >= MAX_OPEN_POSITIONS:
                logger.warning("[TradeManager] Max open positions reached, skipping new alert")
                return None

            if not ALLOW_MULTIPLE_POSITIONS_PER_SYMBOL and (symbol in self._symbol_index and self._symbol_index[symbol]):
                logger.info("[TradeManager] Already in position for %s, skipping (multiple not allowed)", symbol)
                return None

            # (rest of handle_alert unchanged — keep exact logic)
            # ... (copy existing handle_alert implementation into the file) ...

        # After position creation, we send alerts as before
        # Log that we are about to send buy alert for visibility
        logger.debug("[TradeManager] Sending buy alert for %s (position id %s)", symbol, position.id if 'position' in locals() else "N/A")
        await self._send_buy_alert(
            position,
            current_price=price,
            entry_sources=entry_sources,
            pdh=pdh,
            pmh=pmh,
        )

        logger.info(
            "Opened position %s %s | Current: $%s | Entry: $%s (%s) | TP1=%s TP2=%s TP3=%s SL=%s",
            pid, symbol, price, suggested_entry, entry_sources, tp1, tp2, tp3, stop_loss
        )
        return position

    # update_price, update_price_bar, force_close, get_open_positions unchanged...
    # ... (copy existing implementations here) ...

    # ---------------- notifications ----------------

    def _format_price(self, price: float) -> str:
        if price >= 10:
            return f"${price:.2f}"
        elif price >= 1:
            return f"${price:.3f}"
        else:
            return f"${price:.4f}"

    async def _send_buy_alert(
        self,
        position: OpenPosition,
        current_price: Optional[float] = None,
        entry_sources: Optional[str] = None,
        pdh: Optional[float] = None,
        pmh: Optional[float] = None,
    ):
        # (message formatting unchanged)
        # ... (existing code) ...
        message = "\n".join(lines)
        await self._send_notifications(message, parse_mode="Markdown")

    async def _send_sell_alert(self, trade: ClosedTrade):
        # (message formatting unchanged)
        message = "\n".join(lines)
        await self._send_notifications(message, parse_mode="Markdown")

    async def _send_notifications(self, message: str, parse_mode: Optional[str] = None, use_signals_chat: bool = True):
        """Send notifications. use_signals_chat=True sends to the dedicated signals chat."""
        tasks = []
        chat_id = self.signals_chat_id if use_signals_chat else self.telegram_chat_id
        if self.telegram_token and chat_id:
            tasks.append(self._send_telegram(message, chat_id=chat_id, parse_mode=parse_mode))
        if self.discord_webhook_url:
            tasks.append(self._send_discord(message))

        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    logger.error("[TradeManager] Notification error: %s", res)

    async def _send_telegram(self, message: str, chat_id: Optional[str] = None, parse_mode: Optional[str] = None):
        if not self.http_session:
            logger.debug("[TradeManager] No http_session; cannot send telegram message.")
            return
        target_chat = chat_id or self.signals_chat_id
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {"chat_id": target_chat, "text": message}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        try:
            logger.debug("[TradeManager] Sending Telegram message to %s. payload size=%d", target_chat, len(message))
            async with self.http_session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                text = await resp.text()
                if resp.status == 200:
                    logger.debug("[TradeManager] Telegram sent successfully: status=200 response=%s", text)
                else:
                    logger.error("[TradeManager] Telegram error %s: %s", resp.status, text)
        except Exception as e:
            logger.exception("[TradeManager] Telegram send failed: %s", e)

    async def _send_discord(self, message: str):
        if not self.http_session or not self.discord_webhook_url:
            logger.debug("[TradeManager] No http_session or discord webhook not set; skipping Discord send.")
            return
        payload = {"content": message}
        try:
            async with self.http_session.post(self.discord_webhook_url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status not in (200, 204):
                    text = await resp.text()
                    logger.error("[TradeManager] Discord error %s: %s", resp.status, text)
                else:
                    logger.debug("[TradeManager] Discord send OK status=%s", resp.status)
        except Exception as e:
            logger.exception("[TradeManager] Discord send failed: %s", e)

# Keep the rest of the file (TakeProfitPlanner, DynamicTargetCalculator, remaining functions) unchanged and ensure to copy the full original implementations back into this file in your repo.
# The above edits are focused on adding entry logs and notification delivery logs to help locate where alerts are being dropped.
