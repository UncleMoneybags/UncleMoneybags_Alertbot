"""
Trade Signal Integration Layer

This file connects the scanner alerts to the TradeSignalManager
WITHOUT modifying scanner.py. 

Run this alongside scanner.py, or import it into a wrapper.

Two integration methods:
1. Webhook-based: Scanner posts alerts to a local endpoint, this file receives them
2. Import-based: Import this into a wrapper that calls both scanner and trade manager
"""

import os
import asyncio
import aiohttp
import logging
from typing import Optional, Dict, Any, List
from collections import deque
from urllib.parse import quote_plus

from trade_signal_manager import TradeSignalManager, OpenPosition

logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================

TELEGRAM_BOT_TOKEN = os.environ.get('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.environ.get('TELEGRAM_CHAT_ID')
TELEGRAM_SIGNALS_CHAT_ID = os.environ.get('TELEGRAM_SIGNALS_CHAT_ID')
DISCORD_WEBHOOK_URL = os.environ.get('DISCORD_WEBHOOK_URL')

# Price update interval (seconds)
PRICE_CHECK_INTERVAL = 5

# Polygon API for live prices
POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY')

# Module-level init lock to avoid concurrent init/start races
_init_lock = asyncio.Lock()


# =============================================================================
# Alert Queue - Scanner deposits alerts here
# =============================================================================

class AlertQueue:
    """
    Thread-safe queue for scanner to deposit alerts.
    The integration layer picks them up and processes them.

    Note: __len__ is not async-safe; use the async length() method when an
    accurate, race-free count is required.
    """
    
    def __init__(self, maxlen: int = 1000):
        self._queue: deque = deque(maxlen=maxlen)
        self._lock = asyncio.Lock()
    
    async def push(self, alert: Dict[str, Any]):
        """Add an alert to the queue"""
        async with self._lock:
            self._queue.append(alert)
    
    async def pop_all(self) -> List[Dict[str, Any]]:
        """Get and clear all pending alerts"""
        async with self._lock:
            alerts = list(self._queue)
            self._queue.clear()
            return alerts
    
    async def length(self) -> int:
        """Async-safe length"""
        async with self._lock:
            return len(self._queue)
    
    def __len__(self):
        # Best-effort, may race with concurrent push/pop_all.
        return len(self._queue)


# Global alert queue - scanner can import and use this
alert_queue = AlertQueue()


# =============================================================================
# Price Feed - Get live prices for open positions
# =============================================================================

class PriceFeed:
    """Fetches live prices for position monitoring"""
    
    def __init__(self, http_session: aiohttp.ClientSession):
        self.session = http_session
        self.api_key = POLYGON_API_KEY
        if not self.api_key:
            logger.warning("[PriceFeed] POLYGON_API_KEY is not set; live price fetching will be disabled.")
        # Conservative defaults for retries
        self._max_retries = 3
        self._backoff_base = 0.5  # seconds
    
    async def get_price(self, symbol: str) -> Optional[float]:
        """Get current price for a symbol from Polygon with retry/backoff"""
        if not self.api_key or not self.session:
            return None
        
        # Be safe with symbol formatting for URL path
        symbol_safe = quote_plus(symbol.strip().upper())
        url = f"https://api.polygon.io/v2/last/trade/{symbol_safe}"
        params = {"apiKey": self.api_key}
        
        for attempt in range(1, self._max_retries + 1):
            try:
                timeout = aiohttp.ClientTimeout(total=5)
                async with self.session.get(url, params=params, timeout=timeout) as resp:
                    status = resp.status
                    if status == 200:
                        # Parse robustly and cast to float when possible
                        data = await resp.json()
                        results = data.get("results") or {}
                        p = None
                        if isinstance(results, dict):
                            p = results.get("p")
                        # Some responses might include a top-level 'price' or other keys
                        if p is None:
                            p = data.get("price") or data.get("last") or data.get("last_trade_price")
                        if p is None:
                            logger.debug(f"[PriceFeed] No price found for {symbol} in response: {data}")
                            return None
                        try:
                            price = float(p)
                            if price <= 0:
                                logger.debug(f"[PriceFeed] Non-positive price for {symbol}: {price}")
                                return None
                            return price
                        except (TypeError, ValueError):
                            logger.debug(f"[PriceFeed] Unable to cast price to float for {symbol}: {p}")
                            return None
                    elif status in (429, 500, 502, 503, 504):
                        # Retryable server/rate-limit errors
                        text = await resp.text()
                        logger.debug(f"[PriceFeed] Retryable response {status} for {symbol}: {text}")
                        if attempt < self._max_retries:
                            await asyncio.sleep(self._backoff_base * (2 ** (attempt - 1)))
                            continue
                        return None
                    else:
                        # Non-retryable error
                        text = await resp.text()
                        logger.debug(f"[PriceFeed] Non-200 response for {symbol}: {status} - {text}")
                        return None
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.debug(f"[PriceFeed] Exception fetching price for {symbol} (attempt {attempt}): {e}")
                if attempt < self._max_retries:
                    await asyncio.sleep(self._backoff_base * (2 ** (attempt - 1)))
                    continue
                return None
        
        return None
    
    async def get_prices_batch(self, symbols: List[str]) -> Dict[str, float]:
        """Get prices for multiple symbols (limited concurrency, chunked)"""
        results: Dict[str, float] = {}
        
        # Process in small batches to limit concurrent requests / rate limits
        batch_size = 10
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            tasks = [self.get_price(sym) for sym in batch]
            prices = await asyncio.gather(*tasks, return_exceptions=True)
            for sym, price in zip(batch, prices):
                if isinstance(price, (int, float)) and price > 0:
                    results[sym] = float(price)
                else:
                    # Keep debug log for missing prices to aid diagnostics
                    logger.debug(f"[PriceFeed] No valid price for {sym}: {price}")
        
        return results


# =============================================================================
# Integration Manager
# =============================================================================

class TradeSignalIntegration:
    """
    Main integration class that:
    1. Receives alerts from scanner (via queue)
    2. Manages positions via TradeSignalManager
    3. Monitors live prices and triggers exits
    """
    
    def __init__(self):
        self.trade_manager: Optional[TradeSignalManager] = None
        self.price_feed: Optional[PriceFeed] = None
        self.http_session: Optional[aiohttp.ClientSession] = None
        self._running = False
        self._tasks: List[asyncio.Task] = []
        self._start_lock = asyncio.Lock()
    
    async def start(self):
        """Initialize and start the integration"""
        async with self._start_lock:
            if self._running:
                logger.debug("[Integration] start() called but integration already running")
                return
            
            logger.info("[Integration] Starting trade signal integration...")
            
            # Create HTTP session with sensible defaults (connection limit + timeouts)
            connector = aiohttp.TCPConnector(limit=30)
            timeout = aiohttp.ClientTimeout(total=10)
            self.http_session = aiohttp.ClientSession(connector=connector, timeout=timeout)
            
            try:
                # Initialize trade manager
                self.trade_manager = TradeSignalManager(
                    telegram_token=TELEGRAM_BOT_TOKEN,
                    telegram_chat_id=TELEGRAM_CHAT_ID,
                    discord_webhook_url=DISCORD_WEBHOOK_URL,
                    http_session=self.http_session,
                    signals_chat_id=TELEGRAM_SIGNALS_CHAT_ID
                )
                await self.trade_manager.initialize()
                
                # Initialize price feed
                self.price_feed = PriceFeed(self.http_session)
                
                self._running = True
                
                # Start background tasks
                self._tasks.append(asyncio.create_task(self._alert_processor_loop()))
                self._tasks.append(asyncio.create_task(self._price_monitor_loop()))
                
                logger.info("[Integration] Trade signal integration started")
            except Exception:
                # Ensure we close the session on failure to avoid resource leaks
                if self.http_session:
                    try:
                        await self.http_session.close()
                    except Exception:
                        logger.exception("[Integration] Error closing HTTP session during start() failure")
                    self.http_session = None
                logger.exception("[Integration] Failed to start integration")
                raise
    
    async def stop(self):
        """Stop the integration"""
        logger.info("[Integration] Stopping trade signal integration...")
        self._running = False
        
        # Cancel tasks
        for task in self._tasks:
            task.cancel()
        
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        
        # Cleanup trade manager
        if self.trade_manager:
            try:
                await self.trade_manager.cleanup()
            except Exception:
                logger.exception("[Integration] Error while cleaning up trade manager")
            self.trade_manager = None
        
        # Close HTTP session
        if self.http_session:
            try:
                await self.http_session.close()
            except Exception:
                logger.exception("[Integration] Error closing HTTP session")
            self.http_session = None
        
        logger.info("[Integration] Trade signal integration stopped")
    
    async def _alert_processor_loop(self):
        """Process alerts from the queue"""
        while self._running:
            try:
                alerts = await alert_queue.pop_all()
                
                for alert in alerts:
                    await self._process_alert(alert)
                
                await asyncio.sleep(0.5)  # Check every 500ms
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Integration] Alert processor error: {e}", exc_info=True)
                await asyncio.sleep(1)
    
    async def _process_alert(self, alert: Dict[str, Any]):
        """Process a single alert from the scanner"""
        try:
            symbol = alert.get('symbol')
            price = alert.get('price')
            rvol = alert.get('rvol', 1.0)
            alert_type = alert.get('alert_type', 'Unknown')
            ml_score = alert.get('ml_score')
            atr = alert.get('atr')
            recent_candles = alert.get('recent_candles')
            
            # Key levels for entry/TP calculation
            pdh = alert.get('pdh')
            pdl = alert.get('pdl')
            pmh = alert.get('pmh')
            pml = alert.get('pml')
            vwap = alert.get('vwap')
            ema8 = alert.get('ema8')
            ema21 = alert.get('ema21')
            session_high = alert.get('session_high')
            opening_range_high = alert.get('opening_range_high')
            
            # Validate inputs (avoid rejecting valid price 0.0)
            if not symbol or price is None:
                logger.warning(f"[Integration] Invalid alert (missing symbol or price): {alert}")
                return
            
            if not self.trade_manager:
                logger.error("[Integration] Trade manager not initialized")
                return
            
            # Pass to trade manager with all key levels
            position = await self.trade_manager.handle_alert(
                symbol=symbol,
                price=price,
                rvol=rvol,
                alert_type=alert_type,
                ml_score=ml_score,
                atr=atr,
                recent_candles=recent_candles,
                pdh=pdh,
                pdl=pdl,
                pmh=pmh,
                pml=pml,
                vwap=vwap,
                ema8=ema8,
                ema21=ema21,
                session_high=session_high,
                opening_range_high=opening_range_high,
            )
            
            if position:
                logger.info(f"[Integration] Created position for {symbol}")
            
        except Exception as e:
            logger.error(f"[Integration] Error processing alert: {e}", exc_info=True)
    
    async def _price_monitor_loop(self):
        """Monitor prices for open positions"""
        while self._running:
            try:
                if not self.trade_manager or not self.price_feed:
                    await asyncio.sleep(1)
                    continue
                
                positions = self.trade_manager.get_open_positions()
                
                if positions:
                    symbols = [p.symbol for p in positions]
                    prices = await self.price_feed.get_prices_batch(symbols)
                    
                    for symbol, price in prices.items():
                        closed_list = await self.trade_manager.update_price(symbol, price)
                        for closed in closed_list:
                            logger.info(
                                f"[Integration] Position closed: {symbol} | "
                                f"P/L: {closed.pnl_pct:+.1f}% | Reason: {closed.exit_reason}"
                            )
                
                await asyncio.sleep(PRICE_CHECK_INTERVAL)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[Integration] Price monitor error: {e}", exc_info=True)
                await asyncio.sleep(5)
    
    # =========================================================================
    # Direct API (for use when importing into scanner wrapper)
    # =========================================================================
    
    async def on_scanner_alert(
        self,
        symbol: str,
        price: float,
        rvol: float = 1.0,
        alert_type: str = "Alert",
        ml_score: Optional[float] = None,
        atr: Optional[float] = None,
        recent_candles: Optional[List[Dict]] = None
    ) -> Optional[OpenPosition]:
        """
        Direct method to process a scanner alert.
        Call this from your scanner wrapper instead of using the queue.
        """
        if not self.trade_manager:
            logger.error("[Integration] Trade manager not initialized")
            return None
        
        return await self.trade_manager.handle_alert(
            symbol=symbol,
            price=price,
            rvol=rvol,
            alert_type=alert_type,
            ml_score=ml_score,
            atr=atr,
            recent_candles=recent_candles
        )
    
    async def on_price_update(self, symbol: str, price: float) -> List[Any]:
        """
        Direct method to update price for a position (tick-level).
        Call this when you receive a trade tick in your scanner.
        Returns list of ClosedTrade-like objects if any positions were closed.
        """
        if not self.trade_manager:
            return []
        
        return await self.trade_manager.update_price(symbol, price)
    
    async def on_bar_update(self, symbol: str, open_p: float, high: float, low: float, close_p: float) -> List[Any]:
        """
        Direct method to update price using bar data (OHLC).
        Use this when you receive 1m/5m bars instead of ticks.
        Better detection of intrabar TP/SL hits.
        Returns list of ClosedTrade-like objects if any positions were closed.
        """
        if not self.trade_manager:
            return []
        
        return await self.trade_manager.update_price_bar(symbol, open_p, high, low, close_p)
    
    def get_open_positions(self) -> List[OpenPosition]:
        """Get all open positions"""
        if not self.trade_manager:
            return []
        return self.trade_manager.get_open_positions()


# =============================================================================
# Convenience Functions
# =============================================================================

# Global integration instance
_integration: Optional[TradeSignalIntegration] = None


async def init_trade_signals() -> TradeSignalIntegration:
    """Initialize the trade signal integration (call once at startup)"""
    global _integration
    async with _init_lock:
        if _integration is None:
            _integration = TradeSignalIntegration()
            await _integration.start()
    return _integration


async def shutdown_trade_signals():
    """Shutdown the trade signal integration"""
    global _integration
    if _integration:
        await _integration.stop()
        _integration = None


async def queue_alert(
    symbol: str,
    price: float,
    rvol: float = 1.0,
    alert_type: str = "Alert",
    ml_score: Optional[float] = None,
    **kwargs
):
    """
    Queue an alert for processing.

    Use this from scanner.py when you send an alert:
        from trade_signal_integration import queue_alert
        await queue_alert(symbol=symbol, price=price, rvol=rvol, alert_type="Volume Spike", ml_score=score)
    """
    await alert_queue.push({
        'symbol': symbol,
        'price': price,
        'rvol': rvol,
        'alert_type': alert_type,
        'ml_score': ml_score,
        **kwargs
    })


# =============================================================================
# Example: Scanner Wrapper
# =============================================================================

async def example_scanner_wrapper():
    """
    Example showing how to wrap your scanner with trade signals.
    
    This is NOT meant to be run - it's a template showing the integration pattern.
    """
    
    # 1. Initialize trade signals
    integration = await init_trade_signals()
    
    # 2. Your scanner logic here (simplified example)
    async def handle_candle(symbol: str, candle: dict, rvol: float):
        # ... your existing alert logic ...
        
        # When you decide to send an alert:
        price = candle['close']
        
        # Create position with dynamic TP/SL
        position = await integration.on_scanner_alert(
            symbol=symbol,
            price=price,
            rvol=rvol,
            alert_type="Volume Spike"
        )
        
        if position:
            print(f"Position opened: {symbol} @ ${price}")
            print(f"  TP: ${position.take_profit}")
            print(f"  SL: ${position.stop_loss}")
    
    async def handle_trade_tick(symbol: str, price: float):
        # When you get a trade tick, update positions
        closed_list = await integration.on_price_update(symbol, price)
        
        for closed in closed_list:
            print(f"Position closed: {symbol}")
            print(f"  P/L: {closed.pnl_pct:+.1f}%")
    
    # 3. Cleanup on shutdown
    # await shutdown_trade_signals()


# =============================================================================
# Main (for testing)
# =============================================================================

async def main():
    """Test the integration"""
    print("=== Trade Signal Integration Test ===\n")
    
    # Initialize
    integration = await init_trade_signals()
    
    # Simulate an alert
    print("1. Simulating scanner alert for MULN...")
    position = await integration.on_scanner_alert(
        symbol="MULN",
        price=2.50,
        rvol=4.5,
        alert_type="Volume Spike",
        ml_score=0.72
    )
    
    if position:
        print(f"   Position created!")
        print(f"   Entry: ${position.entry_price:.2f}")
        print(f"   TP: ${position.take_profit:.2f}")
        print(f"   SL: ${position.stop_loss:.2f}")
    
    # Simulate price updates
    print("\n2. Simulating price updates...")
    for price in [2.60, 2.80, 3.00, 3.20, 2.95]:
        print(f"   Price update: ${price:.2f}")
        closed_list = await integration.on_price_update("MULN", price)
        if closed_list:
            for closed in closed_list:
                print(f"   ✅ Position closed! P/L: {closed.pnl_pct:+.1f}%")
            break
        
        pos = integration.get_open_positions()
        if pos:
            p = pos[0]
            print(f"   Trailing active: {p.trailing_active}, Stop: ${p.trailing_stop or p.stop_loss:.2f}")
    
    # Cleanup
    await shutdown_trade_signals()
    print("\n=== Test Complete ===")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
