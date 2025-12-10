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

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

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


# =============================================================================
# Take Profit Planner - Dynamic Technical Level Calculator
# =============================================================================


class TakeProfitPlanner:
    """
    Calculates dynamic take profit levels using professional day trading methodology:
    - Previous Day High/Low (PDH/PDL)
    - Pre-market High/Low (PMH/PML)
    - Intraday session highs
    - Fibonacci extensions (1.618, 2.0, 2.618)
    - Psychological levels ($X.00, $X.50, $X.25)
    - Confluence detection when multiple levels align
    """
    
    @staticmethod
    def find_swing_points(candles: List[Dict], lookback: int = 3) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        if not candles or len(candles) < lookback * 2 + 1:
            return None, None, None
        
        lows = []
        highs = []
        
        for i, c in enumerate(candles):
            low = float(c.get("low", c.get("l", 0)) or 0)
            high = float(c.get("high", c.get("h", 0)) or 0)
            lows.append(low)
            highs.append(high)
        
        swing_lows = []
        swing_highs = []
        
        for i in range(lookback, len(candles) - lookback):
            is_swing_low = all(lows[i] <= lows[i-j] for j in range(1, lookback+1)) and \
                           all(lows[i] <= lows[i+j] for j in range(1, lookback+1))
            is_swing_high = all(highs[i] >= highs[i-j] for j in range(1, lookback+1)) and \
                            all(highs[i] >= highs[i+j] for j in range(1, lookback+1))
            
            if is_swing_low:
                swing_lows.append((i, lows[i]))
            if is_swing_high:
                swing_highs.append((i, highs[i]))
        
        if not swing_lows or not swing_highs:
            return None, None, None
        
        point_a_idx, point_a = swing_lows[-1] if swing_lows else (0, min(lows))
        
        valid_highs = [(idx, val) for idx, val in swing_highs if idx > point_a_idx]
        if not valid_highs:
            return point_a, max(highs), lows[-1] if lows else None
        
        point_b_idx, point_b = max(valid_highs, key=lambda x: x[1])
        
        valid_lows = [(idx, val) for idx, val in swing_lows if idx > point_b_idx]
        if valid_lows:
            point_c = min(valid_lows, key=lambda x: x[1])[1]
        else:
            point_c = min(lows[point_b_idx:]) if point_b_idx < len(lows) else lows[-1]
        
        return point_a, point_b, point_c
    
    @staticmethod
    def calculate_fib_extensions(point_a: float, point_b: float, point_c: float) -> List[Tuple[float, str]]:
        if point_a is None or point_b is None or point_c is None or point_a >= point_b:
            return []
        
        range_ab = point_b - point_a
        levels = []
        
        for ext in FIB_EXTENSIONS:
            target = point_c + (range_ab * ext)
            levels.append((target, f"Fib {ext}"))
        
        return levels
    
    @staticmethod
    def get_psychological_levels(entry_price: float, max_levels: int = 5) -> List[Tuple[float, str]]:
        seen_prices: set = set()
        levels = []
        
        if entry_price < 1.0:
            increments = [0.10, 0.25, 0.50, 0.75, 1.00]
            base = int(entry_price * 10) / 10
            for inc in increments:
                level = round(base + inc, 4)
                if level > entry_price * 1.01 and level not in seen_prices:
                    seen_prices.add(level)
                    levels.append((level, "Psych"))
                    if len(levels) >= max_levels:
                        break
        elif entry_price < 10.0:
            base = int(entry_price)
            for i in range(1, max_levels + 1):
                half = round(base + 0.5 * i, 4)
                whole = round(float(base + i), 4)
                if half > entry_price * 1.01 and half not in seen_prices:
                    seen_prices.add(half)
                    levels.append((half, "Psych"))
                if whole > entry_price * 1.01 and whole not in seen_prices:
                    seen_prices.add(whole)
                    levels.append((whole, "Psych"))
        else:
            base = int(entry_price)
            for i in range(1, max_levels + 1):
                level = round(float(base + i), 4)
                if level > entry_price * 1.01 and level not in seen_prices:
                    seen_prices.add(level)
                    levels.append((level, "Psych"))
        
        return sorted(levels, key=lambda x: x[0])[:max_levels]
    
    @staticmethod
    def detect_confluence(levels: List[Tuple[float, str]], tolerance: float = CONFLUENCE_TOLERANCE) -> List[Tuple[float, str, bool]]:
        if not levels:
            return []
        
        sorted_levels = sorted(levels, key=lambda x: x[0])
        result = []
        
        i = 0
        while i < len(sorted_levels):
            current_price, current_source = sorted_levels[i]
            cluster = [(current_price, current_source)]
            
            j = i + 1
            while j < len(sorted_levels):
                next_price, next_source = sorted_levels[j]
                if abs(next_price - current_price) / current_price <= tolerance:
                    cluster.append((next_price, next_source))
                    j += 1
                else:
                    break
            
            is_confluence = len(cluster) > 1
            avg_price = sum(p for p, _ in cluster) / len(cluster)
            sources = " + ".join(s for _, s in cluster)
            
            result.append((avg_price, sources, is_confluence))
            i = j
        
        return result
    
    @classmethod
    def calculate_tp_levels(
        cls,
        entry_price: float,
        pdh: Optional[float] = None,
        pdl: Optional[float] = None,
        pmh: Optional[float] = None,
        pml: Optional[float] = None,
        session_high: Optional[float] = None,
        opening_range_high: Optional[float] = None,
        recent_candles: Optional[List[Dict]] = None,
        rvol: float = 1.0,
    ) -> Tuple[Optional[float], Optional[float], Optional[float], float]:
        PRIORITY_MAP = {
            "PDH": 1,
            "PMH": 2,
            "ORH": 3,
            "Intraday High": 4,
            "Fib 1.618": 5,
            "Fib 2.0": 5,
            "Fib 2.618": 5,
            "Psych": 6,
        }
        
        if pml and pml < entry_price:
            stop_loss = pml * 0.99
            logger.debug(f"[TP] Stop loss set from PML: {stop_loss:.4f}")
        elif pdl and pdl < entry_price:
            stop_loss = pdl * 0.99
            logger.debug(f"[TP] Stop loss set from PDL: {stop_loss:.4f}")
        else:
            stop_loss = entry_price * (1 - BASE_STOP_LOSS_PCT)
            logger.debug(f"[TP] Stop loss using default {BASE_STOP_LOSS_PCT*100}%: {stop_loss:.4f}")
        
        risk_per_share = entry_price - stop_loss
        min_risk = entry_price * MIN_RISK_FLOOR_PCT
        if risk_per_share < min_risk:
            old_sl = stop_loss
            stop_loss = entry_price - min_risk
            risk_per_share = min_risk
            logger.debug(f"[TP] Risk floor applied: SL adjusted {old_sl:.4f} -> {stop_loss:.4f} (risk={min_risk:.4f})")
        
        all_levels: List[Tuple[float, str, int]] = []
        min_distance = entry_price * MIN_TP_DISTANCE_PCT
        
        if pdh and pdh > entry_price + min_distance:
            all_levels.append((pdh, "PDH", PRIORITY_MAP["PDH"]))
        
        if pmh and pmh > entry_price + min_distance:
            all_levels.append((pmh, "PMH", PRIORITY_MAP["PMH"]))
        
        if opening_range_high and opening_range_high > entry_price + min_distance:
            all_levels.append((opening_range_high, "ORH", PRIORITY_MAP["ORH"]))
        
        if session_high and session_high > entry_price + min_distance:
            all_levels.append((session_high, "Intraday High", PRIORITY_MAP["Intraday High"]))
        
        if recent_candles and len(recent_candles) >= 7:
            point_a, point_b, point_c = cls.find_swing_points(recent_candles)
            if point_a is not None and point_b is not None and point_c is not None:
                fib_levels = cls.calculate_fib_extensions(point_a, point_b, point_c)
                for price, label in fib_levels:
                    if price > entry_price + min_distance:
                        priority = PRIORITY_MAP.get(label, 5)
                        all_levels.append((price, label, priority))
        
        psych_levels = cls.get_psychological_levels(entry_price, max_levels=3)
        for price, label in psych_levels:
            if price > entry_price + min_distance:
                all_levels.append((price, label, PRIORITY_MAP["Psych"]))
        
        def is_valid_for_slot(level_price: float, slot_index: int) -> bool:
            if level_price <= entry_price:
                return False
            
            reward = level_price - entry_price
            r_multiple = reward / risk_per_share
            pct_distance = reward / entry_price
            
            min_r = MIN_R_MULTIPLES[slot_index]
            max_r = MAX_R_MULTIPLES[slot_index]
            max_pct = MAX_PCT_FROM_ENTRY[slot_index]
            
            return r_multiple >= min_r and r_multiple <= max_r and pct_distance <= max_pct
        
        def get_r_multiple(level_price: float) -> float:
            if level_price <= entry_price:
                return 0.0
            return (level_price - entry_price) / risk_per_share
        
        def generate_fallback_tp(slot_index: int) -> float:
            rvol_scale = min(1 + (max(1.0, rvol) - 1) * RVOL_TP_MULTIPLIER, MAX_RVOL_MULTIPLIER)
            base_r = MIN_R_MULTIPLES[slot_index] * rvol_scale
            capped_r = min(base_r, MAX_R_MULTIPLES[slot_index])
            
            tp_from_risk = entry_price + (capped_r * risk_per_share)
            max_tp = entry_price * (1 + MAX_PCT_FROM_ENTRY[slot_index])
            
            return min(tp_from_risk, max_tp)
        
        if not all_levels:
            tp1 = generate_fallback_tp(0)
            tp2 = generate_fallback_tp(1)
            tp3 = generate_fallback_tp(2)
            logger.debug(f"[TP] No technical levels found, using risk-based fallback: TP1={tp1:.4f} TP2={tp2:.4f} TP3={tp3:.4f}")
            return (round(tp1, 4), round(tp2, 4), round(tp3, 4), round(stop_loss, 4))
        
        boosted_levels: List[Tuple[float, str, float]] = []
        sorted_by_price = sorted(all_levels, key=lambda x: x[0])
        
        i = 0
        while i < len(sorted_by_price):
            price, source, priority = sorted_by_price[i]
            cluster = [(price, source, priority)]
            
            j = i + 1
            while j < len(sorted_by_price):
                next_price, next_source, next_priority = sorted_by_price[j]
                if abs(next_price - price) / price <= CONFLUENCE_TOLERANCE:
                    cluster.append((next_price, next_source, next_priority))
                    j += 1
                else:
                    break
            
            is_confluence = len(cluster) > 1
            best_priority = min(p for _, _, p in cluster)
            best_level = min(cluster, key=lambda x: x[2])
            best_price = best_level[0]
            
            if is_confluence:
                sources = " + ".join(s for _, s, _ in cluster)
                effective_priority = best_priority - 0.5
            else:
                sources = source
                effective_priority = best_priority
            
            boosted_levels.append((best_price, sources, effective_priority))
            i = j
        
        sorted_by_priority = sorted(boosted_levels, key=lambda x: (x[2], x[0]))
        
        tp_levels: List[Optional[float]] = [None, None, None]
        used_prices: set = set()
        
        for slot in range(3):
            for level_price, source, priority in sorted_by_priority:
                rounded_price = round(level_price, 6)
                if rounded_price in used_prices:
                    continue
                if is_valid_for_slot(level_price, slot):
                    tp_levels[slot] = level_price
                    used_prices.add(rounded_price)
                    break
            
            if tp_levels[slot] is None:
                tp_levels[slot] = generate_fallback_tp(slot)
        
        tp1_val = tp_levels[0] if tp_levels[0] is not None else generate_fallback_tp(0)
        tp2_val = tp_levels[1] if tp_levels[1] is not None else generate_fallback_tp(1)
        tp3_val = tp_levels[2] if tp_levels[2] is not None else generate_fallback_tp(2)
        
        if tp2_val <= tp1_val:
            tp2_val = generate_fallback_tp(1)
            if tp2_val <= tp1_val:
                tp2_val = tp1_val * 1.05
        
        if tp3_val <= tp2_val:
            tp3_val = generate_fallback_tp(2)
            if tp3_val <= tp2_val:
                tp3_val = tp2_val * 1.05
        
        return (round(tp1_val, 4), round(tp2_val, 4), round(tp3_val, 4), round(stop_loss, 4))

    @classmethod
    def calculate_fib_retracements(cls, swing_low: float, swing_high: float) -> List[Tuple[float, str]]:
        if swing_low >= swing_high or swing_low <= 0:
            return []
        
        range_size = swing_high - swing_low
        levels = []
        
        for ret in FIB_RETRACEMENTS:
            level = swing_high - (range_size * ret)
            pct_label = f"{int(round(ret * 100))}%"  # 0.618 -> 62%, 0.382 -> 38%
            levels.append((level, f"Fib {pct_label}"))
        
        return levels

    @classmethod
    def get_psychological_support_levels(cls, current_price: float, min_price: float = 0.10, max_levels: int = 5) -> List[Tuple[float, str]]:
        levels = []
        
        if current_price < 1.0:
            increments = [0.05, 0.10, 0.15, 0.20, 0.25]
            base = int(current_price * 20) / 20  # Round to nearest 0.05
            for inc in increments:
                level = base - inc
                if level >= min_price and level < current_price * 0.99:
                    levels.append((level, "Psych"))
                    if len(levels) >= max_levels:
                        break
        elif current_price < 10.0:
            base = int(current_price)
            for i in range(0, max_levels + 1):
                half = base - 0.5 * i
                whole = base - i
                if half >= min_price and half < current_price * 0.99:
                    levels.append((half, "Psych"))
                if whole >= min_price and whole < current_price * 0.99:
                    levels.append((float(whole), "Psych"))
        else:
            base = int(current_price)
            for i in range(1, max_levels + 1):
                level = base - i
                if level >= min_price and level < current_price * 0.99:
                    levels.append((float(level), "Psych"))
        
        return sorted(levels, key=lambda x: x[0], reverse=True)[:max_levels]

    @classmethod
    def calculate_entry_levels(
        cls,
        current_price: float,
        pdl: Optional[float] = None,
        pml: Optional[float] = None,
        vwap: Optional[float] = None,
        ema8: Optional[float] = None,
        ema21: Optional[float] = None,
        recent_candles: Optional[List[Dict]] = None,
    ) -> Tuple[float, str, List[Tuple[float, str, bool]]]:
        SUPPORT_PRIORITY = {
            "PDL": 1,
            "PML": 2,
            "VWAP": 3,
            "Fib 38%": 4,
            "Fib 50%": 4,
            "Fib 62%": 4,
            "EMA 8": 5,
            "EMA 21": 5,
            "Psych": 6,
        }
        
        all_levels: List[Tuple[float, str, int]] = []
        min_entry = current_price * 0.70  # Don't look for entries more than 30% below
        max_entry = current_price * 0.995  # Entry must be below current price
        
        if pdl and min_entry <= pdl < max_entry:
            all_levels.append((pdl, "PDL", SUPPORT_PRIORITY["PDL"]))
        
        if pml and min_entry <= pml < max_entry:
            all_levels.append((pml, "PML", SUPPORT_PRIORITY["PML"]))
        
        if vwap and min_entry <= vwap < max_entry:
            all_levels.append((vwap, "VWAP", SUPPORT_PRIORITY["VWAP"]))
        
        if ema8 and min_entry <= ema8 < max_entry:
            all_levels.append((ema8, "EMA 8", SUPPORT_PRIORITY["EMA 8"]))
        
        if ema21 and min_entry <= ema21 < max_entry:
            all_levels.append((ema21, "EMA 21", SUPPORT_PRIORITY.get("EMA 21", 5)))
        
        if recent_candles and len(recent_candles) >= 5:
            lows = [float(c.get("low", c.get("l", 0)) or 0) for c in recent_candles]
            highs = [float(c.get("high", c.get("h", 0)) or 0) for c in recent_candles]
            
            swing_low = min(lows) if lows else 0
            swing_high = max(highs) if highs else current_price
            
            if swing_low > 0 and swing_high > swing_low:
                fib_levels = cls.calculate_fib_retracements(swing_low, swing_high)
                for price, label in fib_levels:
                    if min_entry <= price < max_entry:
                        priority = SUPPORT_PRIORITY.get(label, 4)
                        all_levels.append((price, label, priority))
        
        psych_levels = cls.get_psychological_support_levels(current_price, min_price=min_entry)
        for price, label in psych_levels:
            if min_entry <= price < max_entry:
                all_levels.append((price, label, SUPPORT_PRIORITY["Psych"]))
        
        if not all_levels:
            return (current_price, "Market", [])
        
        sorted_by_price = sorted(all_levels, key=lambda x: x[0], reverse=True)  # Highest first (closest to current)
        
        confluent_levels: List[Tuple[float, str, bool]] = []
        i = 0
        while i < len(sorted_by_price):
            price, source, priority = sorted_by_price[i]
            cluster = [(price, source, priority)]
            
            j = i + 1
            while j < len(sorted_by_price):
                next_price, next_source, next_priority = sorted_by_price[j]
                if abs(next_price - price) / price <= 0.01:
                    cluster.append((next_price, next_source, next_priority))
                    j += 1
                else:
                    break
            
            is_confluence = len(cluster) > 1
            avg_price = sum(p for p, _, _ in cluster) / len(cluster)
            best_priority = min(p for _, _, p in cluster)
            
            if is_confluence:
                sources = " + ".join(s for _, s, _ in cluster)
                effective_priority = best_priority - (len(cluster) * 0.3)  # More confluence = better priority
            else:
                sources = source
                effective_priority = best_priority
            
            confluent_levels.append((avg_price, sources, is_confluence, effective_priority))
            i = j
        
        sorted_levels = sorted(confluent_levels, key=lambda x: (x[3], current_price - x[0]))
        
        best = sorted_levels[0]
        best_price = round(best[0], 4)
        best_sources = best[1]
        
        all_formatted = [(round(l[0], 4), l[1], l[2]) for l in sorted_levels]
        
        return (best_price, best_sources, all_formatted)


# =============================================================================
# Calculator (Legacy - kept for backward compatibility)
# =============================================================================


class DynamicTargetCalculator:
    @staticmethod
    def calculate_targets(
        entry_price: float,
        rvol: float,
        atr: Optional[float] = None,
        recent_candles: Optional[List[Dict[str, float]]] = None,
    ) -> Tuple[float, float]:
        if atr is None and recent_candles and len(recent_candles) >= 2:
            atr = DynamicTargetCalculator._calculate_atr(recent_candles)

        if atr is not None and atr > 0:
            tp_distance = atr * ATR_TP_MULTIPLIER
            sl_distance = atr * ATR_SL_MULTIPLIER
            rvol_scale = min(1 + (max(1.0, rvol) - 1) * RVOL_TP_MULTIPLIER, MAX_RVOL_MULTIPLIER)
            tp_distance *= rvol_scale
            take_profit = entry_price + tp_distance
            stop_loss = entry_price - sl_distance
        else:
            tp_pct = BASE_TAKE_PROFIT_PCT
            sl_pct = BASE_STOP_LOSS_PCT
            rvol_scale = min(1 + (max(1.0, rvol) - 1) * RVOL_TP_MULTIPLIER, MAX_RVOL_MULTIPLIER)

            tp_pct *= rvol_scale

            if entry_price < PRICE_THRESHOLD_LOW:
                price_scale = PRICE_THRESHOLD_LOW / max(entry_price, 0.5)
                price_scale = min(price_scale, 3.0)
                tp_pct *= price_scale
                sl_pct *= min(price_scale, 1.5)
            elif entry_price > PRICE_THRESHOLD_HIGH:
                price_scale = PRICE_THRESHOLD_HIGH / entry_price
                tp_pct *= price_scale

            take_profit = entry_price * (1 + tp_pct)
            stop_loss = entry_price * (1 - sl_pct)

        min_tp_distance = entry_price * 0.02
        min_sl_distance = entry_price * 0.02

        if take_profit - entry_price < min_tp_distance:
            take_profit = entry_price + min_tp_distance
        if entry_price - stop_loss < min_sl_distance:
            stop_loss = entry_price - min_sl_distance

        return round(take_profit, 6), round(stop_loss, 6)

    @staticmethod
    def _calculate_atr(candles: List[Dict[str, float]], periods: int = ATR_PERIODS) -> float:
        use = candles[-min(len(candles), periods):]
        if len(use) < 2:
            return 0.0
        trs = []
        for i in range(1, len(use)):
            curr = use[i]
            prev = use[i - 1]
            high = float(curr.get("high", curr.get("h", 0) or 0))
            low = float(curr.get("low", curr.get("l", 0) or 0))
            prev_close = float(prev.get("close", prev.get("c", 0) or 0))
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            trs.append(tr)
        return sum(trs) / len(trs) if trs else 0.0

    @staticmethod
    def calculate_trailing_stop(entry_price: float, current_price: float, highest_price: float) -> Tuple[bool, Optional[float]]:
        gain_pct = (highest_price - entry_price) / entry_price
        if gain_pct >= TRAILING_ACTIVATION_PCT:
            trailing_stop = highest_price * (1 - TRAILING_DISTANCE_PCT)
            return True, round(trailing_stop, 6)
        return False, None


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

        self.calculator = DynamicTargetCalculator()

        os.makedirs(DATA_DIR, exist_ok=True)

        # async lock for concurrency safety
        self._lock = asyncio.Lock()

        # load persisted positions (non-blocking)
        # this will be awaited by initialize if needed
        self._load_positions_sync()

    async def initialize(self):
        if self.http_session is None:
            self.http_session = aiohttp.ClientSession()
            self._owns_session = True

    async def cleanup(self):
        await self._save_positions()
        if self._owns_session and self.http_session:
            await self.http_session.close()

    # ---------------- persistence (async-friendly) ----------------
    def _load_positions_sync(self):
        try:
            if os.path.exists(TRADES_FILE):
                with open(TRADES_FILE, "r") as f:
                    try:
                        data = json.load(f)
                    except (json.JSONDecodeError, ValueError):
                        logger.warning("[TradeManager] trades file exists but is empty or invalid; starting fresh")
                        data = {}
                # data is dict: id -> position dict
                for _id, d in data.items():
                    try:
                        pos = OpenPosition.from_dict(d)
                        self.positions[_id] = pos
                        self._symbol_index.setdefault(pos.symbol, set()).add(_id)
                    except Exception as e:
                        logger.warning(f"[TradeManager] Skipping invalid saved position {_id}: {e}")
                logger.info(f"[TradeManager] Loaded {len(self.positions)} open positions")
        except Exception as e:
            logger.error(f"[TradeManager] Error loading positions: {e}")
            self.positions = {}
            self._symbol_index = {}

    async def _save_positions(self):
        # run blocking file write out of event loop
        async with self._lock:
            try:
                data = {pid: pos.to_dict() for pid, pos in self.positions.items()}
                temp_file = TRADES_FILE + ".tmp"

                def _write():
                    with open(temp_file, "w") as f:
                        json.dump(data, f, indent=2)
                    os.replace(temp_file, TRADES_FILE)

                await asyncio.to_thread(_write)
            except Exception as e:
                logger.error(f"[TradeManager] Error saving positions: {e}")

    async def _log_trade(self, trade: ClosedTrade):
        def _append():
            file_exists = os.path.exists(TRADE_LOG_FILE)
            with open(TRADE_LOG_FILE, "a") as f:
                if not file_exists:
                    f.write("symbol,entry_price,exit_price,entry_time,exit_time,pnl_pct,exit_reason,alert_type,ml_score,duration_minutes\n")
                f.write(
                    f"{trade.symbol},{trade.entry_price},{trade.exit_price},"
                    f"{trade.entry_time},{trade.exit_time},{trade.pnl_pct},"
                    f"{trade.exit_reason},{trade.alert_type},{trade.ml_score or ''},"
                    f"{trade.duration_minutes}\n"
                )

        try:
            await asyncio.to_thread(_append)
        except Exception as e:
            logger.error(f"[TradeManager] Error logging trade: {e}")

    # ---------------- core API ----------------

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

        async with self._lock:
            if len(self.positions) >= MAX_OPEN_POSITIONS:
                logger.warning("Max open positions reached, skipping new alert")
                return None

            if not ALLOW_MULTIPLE_POSITIONS_PER_SYMBOL and (symbol in self._symbol_index and self._symbol_index[symbol]):
                logger.info(f"Already in position for {symbol}, skipping (multiple not allowed)")
                return None

            # STEP 1: Calculate best entry using confluence detection
            suggested_entry, entry_sources, all_support_levels = TakeProfitPlanner.calculate_entry_levels(
                current_price=price,
                pdl=pdl,
                pml=pml,
                vwap=vwap,
                ema8=ema8,
                ema21=ema21,
                recent_candles=recent_candles,
            )
            
            # STEP 2: Calculate TP levels FROM the suggested entry (not current price)
            tp1, tp2, tp3, stop_loss = TakeProfitPlanner.calculate_tp_levels(
                entry_price=suggested_entry,  # Use suggested entry, not current price
                pdh=pdh,
                pdl=pdl,
                pmh=pmh,
                pml=pml,
                session_high=session_high,
                opening_range_high=opening_range_high,
                recent_candles=recent_candles,
                rvol=rvol,
            )

            take_profit = tp1 if tp1 else suggested_entry * 1.10

            pid = str(uuid.uuid4())
            position = OpenPosition(
                id=pid,
                symbol=symbol,
                entry_price=float(suggested_entry),  # Use suggested entry
                entry_time=entry_time,
                stop_loss=float(stop_loss),
                take_profit=float(take_profit),
                tp1=tp1,
                tp2=tp2,
                tp3=tp3,
                trailing_stop=None,
                highest_price=float(price),  # Track actual current price
                rvol=float(rvol),
                atr=atr,
                alert_type=alert_type,
                ml_score=ml_score,
                trailing_active=False,
            )

            self.positions[pid] = position
            self._symbol_index.setdefault(symbol, set()).add(pid)

            asyncio.create_task(self._save_positions())

        # Pass extra info for enhanced buy alert
        await self._send_buy_alert(
            position, 
            current_price=price, 
            entry_sources=entry_sources,
            pdh=pdh,
            pmh=pmh,
        )

        logger.info(
            f"Opened position {pid} {symbol} | Current: ${price:.4f} | Entry: ${suggested_entry:.4f} ({entry_sources}) | TP1={tp1} TP2={tp2} TP3={tp3} SL={stop_loss:.4f}"
        )
        return position

    async def update_price(self, symbol: str, current_price: float, timestamp: Optional[datetime] = None) -> List[ClosedTrade]:
        now = timestamp or datetime.now(timezone.utc)
        closed: List[ClosedTrade] = []

        async with self._lock:
            ids = list(self._symbol_index.get(symbol, set()))
            for pid in ids:
                pos = self.positions.get(pid)
                if not pos:
                    continue

                # update highest price
                if current_price > pos.highest_price:
                    pos.highest_price = current_price
                    is_active, trailing_stop = self.calculator.calculate_trailing_stop(pos.entry_price, current_price, pos.highest_price)
                    if is_active:
                        pos.trailing_active = True
                        if trailing_stop and (pos.trailing_stop is None or trailing_stop > pos.trailing_stop):
                            pos.trailing_stop = trailing_stop

                exit_reason = None
                exit_price = float(current_price)

                # priority checks
                if current_price >= pos.take_profit:
                    exit_reason = "TP"
                    exit_price = pos.take_profit
                elif pos.trailing_active and pos.trailing_stop and current_price <= pos.trailing_stop:
                    exit_reason = "TRAILING"
                    exit_price = pos.trailing_stop
                elif current_price <= pos.stop_loss:
                    exit_reason = "SL"
                    exit_price = pos.stop_loss
                else:
                    entry_time = datetime.fromisoformat(pos.entry_time.replace("Z", "+00:00"))
                    if (now - entry_time) > timedelta(hours=POSITION_TIMEOUT_HOURS):
                        exit_reason = "TIMEOUT"

                if exit_reason:
                    # remove from state
                    _pos = self.positions.pop(pid, None)
                    if pid in self._symbol_index.get(symbol, set()):
                        self._symbol_index[symbol].discard(pid)

                    entry_time_str = _pos.entry_time if _pos else utcnow_iso()
                    entry_price = _pos.entry_price if _pos else 0.0
                    duration = int((now - datetime.fromisoformat(entry_time_str.replace("Z", "+00:00"))).total_seconds() / 60) if _pos else 0
                    pnl_pct = ((exit_price - entry_price) / entry_price) * 100 if entry_price else 0.0

                    closed_trade = ClosedTrade(
                        symbol=symbol,
                        entry_price=entry_price,
                        exit_price=exit_price,
                        entry_time=entry_time_str,
                        exit_time=now.isoformat().replace("+00:00", "Z"),
                        pnl_pct=round(pnl_pct, 2),
                        exit_reason=exit_reason,
                        alert_type=_pos.alert_type if _pos else "",
                        ml_score=_pos.ml_score if _pos else None,
                        duration_minutes=duration,
                    )
                    closed.append(closed_trade)

                    # persist and log
                    asyncio.create_task(self._save_positions())
                    asyncio.create_task(self._log_trade(closed_trade))

        # send SELL notifications outside lock
        for ct in closed:
            await self._send_sell_alert(ct)
            logger.info(f"Closed {ct.symbol} exit={ct.exit_price} reason={ct.exit_reason} pnl={ct.pnl_pct:+.2f}%")

        return closed

    async def update_price_bar(self, symbol: str, open_p: float, high: float, low: float, close_p: float, timestamp: Optional[datetime] = None) -> List[ClosedTrade]:
        now = timestamp or datetime.now(timezone.utc)
        closed: List[ClosedTrade] = []

        async with self._lock:
            ids = list(self._symbol_index.get(symbol, set()))
            for pid in ids:
                pos = self.positions.get(pid)
                if not pos:
                    continue

                # update highest
                if high > pos.highest_price:
                    pos.highest_price = high

                tp_hit = low <= pos.take_profit <= high
                sl_hit = low <= pos.stop_loss <= high

                exit_reason = None
                exit_price = None

                if tp_hit and not sl_hit:
                    exit_reason = "TP"
                    exit_price = pos.take_profit
                elif sl_hit and not tp_hit:
                    exit_reason = "SL"
                    exit_price = pos.stop_loss
                elif tp_hit and sl_hit:
                    if close_p >= pos.take_profit:
                        exit_reason = "TP"
                        exit_price = pos.take_profit
                    elif close_p <= pos.stop_loss:
                        exit_reason = "SL"
                        exit_price = pos.stop_loss
                    else:
                        if close_p >= open_p:
                            exit_reason = "TP"
                            exit_price = pos.take_profit
                        else:
                            exit_reason = "SL"
                            exit_price = pos.stop_loss
                else:
                    is_active, trailing_stop = self.calculator.calculate_trailing_stop(pos.entry_price, close_p, pos.highest_price)
                    if is_active:
                        pos.trailing_active = True
                        if trailing_stop and (pos.trailing_stop is None or trailing_stop > pos.trailing_stop):
                            pos.trailing_stop = trailing_stop
                    if pos.trailing_active and pos.trailing_stop and close_p <= pos.trailing_stop:
                        exit_reason = "TRAILING"
                        exit_price = pos.trailing_stop
                    else:
                        entry_time = datetime.fromisoformat(pos.entry_time.replace("Z", "+00:00"))
                        if (now - entry_time) > timedelta(hours=POSITION_TIMEOUT_HOURS):
                            exit_reason = "TIMEOUT"
                            exit_price = close_p

                if exit_reason and exit_price is not None:
                    _pos = self.positions.pop(pid, None)
                    if pid in self._symbol_index.get(symbol, set()):
                        self._symbol_index[symbol].discard(pid)

                    entry_time_str = _pos.entry_time if _pos else utcnow_iso()
                    entry_price = _pos.entry_price if _pos else 0.0
                    duration = int((now - datetime.fromisoformat(entry_time_str.replace("Z", "+00:00"))).total_seconds() / 60) if _pos else 0
                    final_exit_price = float(exit_price)
                    pnl_pct = ((final_exit_price - entry_price) / entry_price) * 100 if entry_price else 0.0

                    closed_trade = ClosedTrade(
                        symbol=symbol,
                        entry_price=entry_price,
                        exit_price=final_exit_price,
                        entry_time=entry_time_str,
                        exit_time=now.isoformat().replace("+00:00", "Z"),
                        pnl_pct=round(pnl_pct, 2),
                        exit_reason=exit_reason,
                        alert_type=_pos.alert_type if _pos else "",
                        ml_score=_pos.ml_score if _pos else None,
                        duration_minutes=duration,
                    )
                    closed.append(closed_trade)

                    asyncio.create_task(self._save_positions())
                    asyncio.create_task(self._log_trade(closed_trade))

        for ct in closed:
            await self._send_sell_alert(ct)
            logger.info(f"[Bar close] Closed {ct.symbol} exit={ct.exit_price} reason={ct.exit_reason} pnl={ct.pnl_pct:+.2f}%")

        return closed

    async def force_close(self, position_id: str, exit_price: float, reason: str = "MANUAL") -> Optional[ClosedTrade]:
        async with self._lock:
            pos = self.positions.pop(position_id, None)
            if not pos:
                return None
            self._symbol_index.get(pos.symbol, set()).discard(position_id)
            await self._save_positions()

        now = datetime.now(timezone.utc)
        duration = int((now - datetime.fromisoformat(pos.entry_time.replace("Z", "+00:00"))).total_seconds() / 60)
        pnl_pct = ((exit_price - pos.entry_price) / pos.entry_price) * 100
        closed = ClosedTrade(
            symbol=pos.symbol,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            entry_time=pos.entry_time,
            exit_time=now.isoformat().replace("+00:00", "Z"),
            pnl_pct=round(pnl_pct, 2),
            exit_reason=reason,
            alert_type=pos.alert_type,
            ml_score=pos.ml_score,
            duration_minutes=duration,
        )
        await self._log_trade(closed)
        await self._send_sell_alert(closed)
        return closed

    def get_open_positions(self) -> List[OpenPosition]:
        return list(self.positions.values())

    def get_positions_by_symbol(self, symbol: str) -> List[OpenPosition]:
        ids = list(self._symbol_index.get(symbol, set()))
        return [self.positions[i] for i in ids if i in self.positions]

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
        current_str = self._format_price(current_price) if current_price else ""
        entry_str = self._format_price(position.entry_price)
        tp1_str = self._format_price(position.tp1) if position.tp1 else self._format_price(position.take_profit)
        tp2_str = self._format_price(position.tp2) if position.tp2 else ""
        tp3_str = self._format_price(position.tp3) if position.tp3 else ""
        sl_str = self._format_price(position.stop_loss)
        
        risk = position.entry_price - position.stop_loss
        if risk > 0 and position.tp1:
            r1 = (position.tp1 - position.entry_price) / risk
            r1_str = f" ({r1:.1f}R)"
        else:
            r1_str = ""
        
        if risk > 0 and position.tp2:
            r2 = (position.tp2 - position.entry_price) / risk
            r2_str = f" ({r2:.1f}R)"
        else:
            r2_str = ""
            
        if risk > 0 and position.tp3:
            r3 = (position.tp3 - position.entry_price) / risk
            r3_str = f" ({r3:.1f}R)"
        else:
            r3_str = ""
        
        lines = [
            "━━━━━━━━━━━━━━━━━━",
            f"🟢 *BUY SIGNAL: {position.symbol}*",
            "━━━━━━━━━━━━━━━━━━",
            "",
        ]
        
        if current_price and abs(current_price - position.entry_price) > 0.001:
            lines.append(f"📈 *Current:* {current_str}")
        
        if entry_sources and entry_sources != "Market":
            lines.append(f"📍 *Entry:* {entry_str}")
            lines.append(f"    _({entry_sources})_")
        else:
            lines.append(f"📍 *Entry:* {entry_str}")
        
        lines.append("")
        lines.append(f"🎯 TP1: {tp1_str}{r1_str}")
        
        if tp2_str:
            lines.append(f"🎯 TP2: {tp2_str}{r2_str}")
        if tp3_str:
            lines.append(f"🎯 TP3: {tp3_str}{r3_str}")
        
        lines.extend([
            "",
            f"🛑 *Stop Loss:* {sl_str}",
        ])
        
        message = "\n".join(lines)
        await self._send_notifications(message, parse_mode="Markdown")

    async def _send_sell_alert(self, trade: ClosedTrade):
        emoji = "✅" if trade.pnl_pct > 0 else ("⏰" if trade.exit_reason == "TIMEOUT" else "🛑")
        exit_str = self._format_price(trade.exit_price)
        entry_str = self._format_price(trade.entry_price)
        
        reason_map = {
            "TP": "Target Hit",
            "SL": "Stop Loss Hit", 
            "TRAILING": "Trailing Stop Hit",
            "TIMEOUT": "Position Expired",
            "MANUAL": "Manual Exit"
        }
        
        lines = [
            "━━━━━━━━━━━━━━━━━━",
            f"{emoji} *SOLD: {trade.symbol}*",
            "━━━━━━━━━━━━━━━━━━",
            "",
            f"*Exit:* {exit_str}",
            f"*Entry:* {entry_str}",
            "",
            f"*P/L:* {trade.pnl_pct:+.1f}%",
            f"Reason: {reason_map.get(trade.exit_reason, trade.exit_reason)}",
            f"Duration: {trade.duration_minutes} min",
        ]
        
        message = "\n".join(lines)
        await self._send_notifications(message, parse_mode="Markdown")

    async def _send_notifications(self, message: str, parse_mode: Optional[str] = None, use_signals_chat: bool = True):
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
                    logger.error("Notification error: %s", res)

    async def _send_telegram(self, message: str, chat_id: Optional[str] = None, parse_mode: Optional[str] = None):
        if not self.http_session:
            return
        target_chat = chat_id or self.signals_chat_id
        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {"chat_id": target_chat, "text": message}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        try:
            async with self.http_session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    logger.error("Telegram error %s: %s", resp.status, text)
        except Exception as e:
            logger.error("Telegram send failed: %s", e)

    async def _send_discord(self, message: str):
        if not self.http_session or not self.discord_webhook_url:
            return
        payload = {"content": message}
        try:
            async with self.http_session.post(self.discord_webhook_url, json=payload, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                if resp.status not in (200, 204):
                    text = await resp.text()
                    logger.error("Discord error %s: %s", resp.status, text)
        except Exception as e:
            logger.error("Discord send failed: %s", e)


# =============================================================================
# Demo - Dynamic Technical TP Levels
# =============================================================================

async def demo():
    """Demo showing dynamic TP calculation with technical levels."""
    manager = TradeSignalManager()
    await manager.initialize()

    sample_candles = [
        {"high": 2.20, "low": 2.10, "close": 2.15},
        {"high": 2.30, "low": 2.15, "close": 2.28},
        {"high": 2.45, "low": 2.25, "close": 2.40},
        {"high": 2.55, "low": 2.35, "close": 2.50},
        {"high": 2.60, "low": 2.45, "close": 2.55},
        {"high": 2.52, "low": 2.40, "close": 2.45},
        {"high": 2.48, "low": 2.38, "close": 2.42},
        {"high": 2.55, "low": 2.40, "close": 2.50},
    ]

    position = await manager.handle_alert(
        symbol="FLYE",
        price=2.50,
        rvol=4.5,
        alert_type="Volume Spike",
        ml_score=0.72,
        pdh=3.25,
        pdl=2.10,
        pmh=2.85,
        pml=2.30,
        session_high=2.65,
        recent_candles=sample_candles,
    )
    
    if position:
        print(f"Opened: {position.symbol}")
        print(f"  Entry: ${position.entry_price:.4f}")
        print(f"  TP1: ${position.tp1:.4f}" if position.tp1 else "  TP1: N/A")
        print(f"  TP2: ${position.tp2:.4f}" if position.tp2 else "  TP2: N/A")
        print(f"  TP3: ${position.tp3:.4f}" if position.tp3 else "  TP3: N/A")
        print(f"  Stop Loss: ${position.stop_loss:.4f}")
    
    await manager.cleanup()


async def demo_tp_planner():
    """Demo the TakeProfitPlanner directly."""
    tp1, tp2, tp3, sl = TakeProfitPlanner.calculate_tp_levels(
        entry_price=0.56,
        pdh=0.85,
        pmh=0.72,
        pdl=0.45,
        pml=0.50,
        session_high=0.65,
        rvol=3.5,
    )
    
    print("TakeProfitPlanner Demo:")
    print(f"  Entry: $0.56")
    print(f"  TP1: ${tp1:.4f}" if tp1 else "  TP1: N/A")
    print(f"  TP2: ${tp2:.4f}" if tp2 else "  TP2: N/A")
    print(f"  TP3: ${tp3:.4f}" if tp3 else "  TP3: N/A")
    print(f"  Stop Loss: ${sl:.4f}")


if __name__ == "__main__":
    asyncio.run(demo())
    asyncio.run(demo_tp_planner())
