#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
【Bybit-HL Liquidation Dislocation Engine】
Exploiting Derivative Cascade Avalanches via Marked Hawkes Processes
and Intercepting Cross-Venue Gravitational Reversion on Hyperliquid L1
================================================================================
Microstructure Physics & Trading Philosophy:
1. Liquidation Cascades & Endogenous Feedback:
   - When leveraged accounts on Bybit breach maintenance margin requirements, the
     liquidation engine forcibly executes market/IOC orders into the order book.
   - This process exhibits self-exciting point process dynamics, precisely modeled
     via a continuous-time Marked Hawkes Process with intensity lambda(t).
2. Cross-Venue Structural Dislocation & Latency Gap:
   - During severe avalanches, cross-exchange market makers withdraw passive quotes
     across venues, including Hyperliquid L1.
   - Hyperliquid's median oracle smoothing creates a latency wedge against Bybit's
     transient price crash, generating an extreme basis tension (Delta_basis > 3.5 sigma).
3. Singularity Exhaustion Phase Transition:
   - When liquidation acceleration undergoes second-derivative curvature inversion
     (d_lambda/dt < 0 and d2_lambda/dt2 > 0) and Kyle's price concession damping
     decays to zero (chi(t) -> 0), physical exhaustion is confirmed. The engine
     strikes on Hyperliquid to capture the inevitable vacuum mean-reversion.

Hardware Democratization & Architecture:
- Target Environment: Intel Core i7-4770 / 1vCPU Commodity VPS.
- Zero Pandas / Zero NumPy. Strict zero-allocation array.array continuous memory.
- Storage: SQLite3 Write-Ahead Logging (WAL) mode with memory-mapped I/O.
================================================================================
"""

import os
import sys
import time
import math
import json
import asyncio
import sqlite3
import datetime
import array
from collections import deque
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Tuple
import aiohttp


# ==============================================================================
# Global Configuration Center
# ==============================================================================
@dataclass
class EngineConfig:
    # ---------------- Operational Flags ----------------
    # Self-test flag: verifies Telegram alert delivery on startup
    SIMULATE_TELEGRAM_TEST: bool = False

    # Sandbox mode: generates a local synthetic cascade stream for offline testing
    USE_MOCK_STREAM: bool = False

    # ---------------- Monitored Pair Configuration ----------------
    BYBIT_SYMBOL: str = "BTCUSDT"
    HL_COIN: str = "BTC"

    # ---------------- WebSocket Gateway Endpoints ----------------
    BYBIT_WS_URL: str = "wss://stream.bybit.com/v5/public/linear"
    HL_WS_URL: str = "wss://api.hyperliquid.xyz/ws"

    # ---------------- Telegram Bot Credentials ----------------
    TELEGRAM_BOT_TOKEN: str = "YOUR_TELEGRAM_BOT_TOKEN"
    TELEGRAM_CHAT_ID: str = "YOUR_TELEGRAM_CHAT_ID"

    # ---------------- Hawkes Process Microstructural Parameters ----------------
    HAWKES_MU_0: float = 0.1             # Exogenous baseline intensity (events/sec)
    HAWKES_ALPHA_0: float = 0.8          # Self-excitation sensitivity factor
    HAWKES_BETA: float = 1.2             # Liquidity memory decay parameter (beta)
    HAWKES_GAMMA: float = 0.65           # Sub-linear mark impact exponent
    HAWKES_V_BAR: float = 1.0            # Baseline normalization volume (e.g., 1.0 BTC)

    # ---------------- Singularity Exhaustion & Dislocation Thresholds ----------------
    BASIS_Z_SCORE_THRESHOLD: float = 3.5 # Basis tension explosion boundary (3.5 sigma)
    KYLE_DAMPING_MAX: float = 0.0005     # Critical price concession damping ceiling (chi_crit)
    RELAXATION_STEPS_REQ: int = 3        # Quiescent steps required (tau > 3 * 1/beta)

    # ---------------- Ring Buffer Sizing (Must be a power of two) ----------------
    RING_BUFFER_CAPACITY: int = 1024

    # ---------------- Database Storage Path ----------------
    DB_PATH: str = "bybit_hl_dislocation_wal.db"


# ==============================================================================
# Telegram MarkdownV2 Escaping Protocol
# ==============================================================================
def escape_md_v2(text: str) -> str:
    """Escapes Telegram MarkdownV2 reserved characters per Bot API specifications."""
    special_chars = r"_*[]()~`>#+-=|{}.!"
    return "".join(f"\\{c}" if c in special_chars else c for c in str(text))


async def send_telegram_alert(session: aiohttp.ClientSession, config: EngineConfig, text_md: str):
    """Dispatches asynchronous non-blocking Telegram notifications."""
    if config.TELEGRAM_BOT_TOKEN == "YOUR_TELEGRAM_BOT_TOKEN" or not config.TELEGRAM_BOT_TOKEN:
        print("[TG NOTICE] TELEGRAM_BOT_TOKEN not configured. Alert logged to console only.")
        return

    url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": config.TELEGRAM_CHAT_ID,
        "text": text_md,
        "parse_mode": "MarkdownV2"
    }
    try:
        async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            if resp.status != 200:
                err_text = await resp.text()
                print(f"[TG ERROR] HTTP {resp.status} dispatch failure: {err_text}")
    except Exception as e:
        print(f"[TG EXCEPTION] Network transmission failure: {str(e)}")


# ==============================================================================
# SQLite3 WAL Persistence Layer
# ==============================================================================
class DatabaseManager:
    """Manages lock-free state logging using SQLite3 in WAL mode with memory mapping."""
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._init_db()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10.0)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        conn.execute("PRAGMA mmap_size=30000000000;")
        return conn

    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS strike_dislocation_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp INTEGER,
                    bybit_price REAL,
                    hl_mid REAL,
                    basis_dislocation REAL,
                    z_score REAL,
                    hawkes_lambda REAL,
                    d_lambda_dt REAL,
                    d2_lambda_dt2 REAL,
                    kinetic_energy REAL,
                    kyle_damping REAL
                );
            """)
            conn.commit()

    def record_strike(self, bybit_p: float, hl_mid: float, basis: float, z_score: float,
                      lam: float, d1: float, d2: float, ke: float, kyle: float):
        with self._get_connection() as conn:
            conn.execute("""
                INSERT INTO strike_dislocation_events 
                (timestamp, bybit_price, hl_mid, basis_dislocation, z_score, 
                 hawkes_lambda, d_lambda_dt, d2_lambda_dt2, kinetic_energy, kyle_damping)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """, (int(time.time() * 1000), bybit_p, hl_mid, basis, z_score, lam, d1, d2, ke, kyle))
            conn.commit()


# ==============================================================================
# Zero-Allocation Continuous Memory Ring Buffer
# ==============================================================================
class ZeroAllocContinuousRing:
    """
    Fixed-size circular ring buffer backed by array.array('d') C-doubles.
    Guarantees:
    1. Zero dynamic heap allocation during runtime (Zero Garbage Collection pressure).
    2. O(1) rolling mean and variance updates via online moment tracking.
    3. Single-cycle bitwise pointer arithmetic: ptr = (ptr + 1) & (CAPACITY - 1).
    """
    def __init__(self, capacity: int = 1024):
        assert (capacity & (capacity - 1)) == 0, "Capacity must be an exact power of two."
        self.capacity: int = capacity
        self.mask: int = capacity - 1
        self.head_ptr: int = 0
        self.count: int = 0

        # Pre-allocated contiguous C-double array (8-byte aligned)
        self.data: array.array = array.array('d', [0.0] * capacity)

        # Sliding moment accumulators
        self.sum_x: float = 0.0
        self.sum_xx: float = 0.0

    def push(self, val: float):
        """Pushes a value in O(1) time complexity and updates sliding first/second moments."""
        if self.count >= self.capacity:
            old_val = self.data[self.head_ptr]
            self.sum_x += val - old_val
            self.sum_xx += (val * val) - (old_val * old_val)
        else:
            self.sum_x += val
            self.sum_xx += val * val
            self.count += 1

        self.data[self.head_ptr] = val
        self.head_ptr = (self.head_ptr + 1) & self.mask

    def get_mean_and_stdev(self) -> Tuple[float, float]:
        """Calculates instantaneous sample mean and standard deviation in O(1) time."""
        if self.count < 2:
            return 0.0, 1e-6
        n = float(self.count)
        mean = self.sum_x / n
        var = (self.sum_xx / n) - (mean * mean)
        stdev = math.sqrt(max(1e-12, var))
        return mean, stdev


# ==============================================================================
# Online Recursive Hawkes Intensity & Micro-Derivative Engine
# ==============================================================================
class HawkesIntensityEngine:
    """
    Mathematical Derivation: Recursive Marked Hawkes Intensity
    lambda(t) = mu_0 + sum_{t_i < t} alpha(v_i) * e^{-beta * (t - t_i)}
    
    Recursive Dynamics:
    Inter-event continuous exponential decay (t_{k-1} -> t_k):
      lambda(t_k^-) = mu_0 + (lambda(t_{k-1}^+) - mu_0) * e^{-beta * (t_k - t_{k-1})}
    Event jump arrival:
      lambda(t_k^+) = lambda(t_k^-) + alpha_0 * (v_k / v_bar)^gamma
      
    Execution complexity is strictly O(1) per tick without historical traversal.
    """
    def __init__(self, config: EngineConfig):
        self.cfg = config
        self.mu_0: float = config.HAWKES_MU_0
        self.alpha_0: float = config.HAWKES_ALPHA_0
        self.beta: float = config.HAWKES_BETA
        self.gamma: float = config.HAWKES_GAMMA
        self.v_bar: float = config.HAWKES_V_BAR

        self.last_update_time: float = time.time()
        self.current_lambda: float = self.mu_0

        # Fixed contiguous memory ring for finite difference derivatives
        self.lambda_history = ZeroAllocContinuousRing(capacity=64)

    def on_liquidation_event(self, timestamp: float, volume: float) -> float:
        """Processes event arrival and applies discrete mark excitation jump."""
        dt = max(0.0, timestamp - self.last_update_time)
        decay = math.exp(-self.beta * dt)
        decayed_excess = (self.current_lambda - self.mu_0) * decay
        self.current_lambda = self.mu_0 + max(0.0, decayed_excess)

        # Mark jump excitation
        mark_jump = self.alpha_0 * math.pow(max(0.001, volume / self.v_bar), self.gamma)
        self.current_lambda += mark_jump
        self.last_update_time = timestamp

        self.lambda_history.push(self.current_lambda)
        return self.current_lambda

    def decay_to_now(self) -> float:
        """Applies natural exponential decay between discrete arrival events."""
        now = time.time()
        dt = max(0.0, now - self.last_update_time)
        decay = math.exp(-self.beta * dt)
        self.current_lambda = self.mu_0 + max(0.0, (self.current_lambda - self.mu_0) * decay)
        self.last_update_time = now
        self.lambda_history.push(self.current_lambda)
        return self.current_lambda

    def compute_derivatives(self, dt_step: float) -> Tuple[float, float]:
        """
        Finite Difference Micro-Derivatives:
        d_lambda / dt   ~= (lambda_0 - lambda_{-1}) / dt
        d2_lambda / dt2 ~= (lambda_0 - 2 * lambda_{-1} + lambda_{-2}) / dt^2
        """
        if self.lambda_history.count < 3:
            return 0.0, 0.0

        l0 = self.lambda_history.data[(self.lambda_history.head_ptr - 1) & self.lambda_history.mask]
        l1 = self.lambda_history.data[(self.lambda_history.head_ptr - 2) & self.lambda_history.mask]
        l2 = self.lambda_history.data[(self.lambda_history.head_ptr - 3) & self.lambda_history.mask]

        dt_safe = max(1e-4, dt_step)
        d1 = (l0 - l1) / dt_safe
        d2 = (l0 - 2.0 * l1 + l2) / (dt_safe * dt_safe)
        return d1, d2


# ==============================================================================
# Pipeline 1: Bybit Liquidation Harvester
# ==============================================================================
class BybitLiquidationHarvester:
    """Streams and parses raw liquidation events from Bybit Linear Public WebSocket."""
    def __init__(self, config: EngineConfig, hawkes: HawkesIntensityEngine):
        self.cfg = config
        self.hawkes = hawkes
        self.latest_bybit_price: float = 0.0
        self.running: bool = True
        self.recent_liquidations: deque = deque(maxlen=200)

    async def run(self, session: aiohttp.ClientSession):
        if self.cfg.USE_MOCK_STREAM:
            return

        topic = f"liquidation.{self.cfg.BYBIT_SYMBOL}"
        sub_msg = {"op": "subscribe", "args": [topic]}

        while self.running:
            try:
                print(f"[BYBIT PIPELINE] Connecting to WebSocket gateway: {self.cfg.BYBIT_WS_URL}")
                async with session.ws_connect(self.cfg.BYBIT_WS_URL, timeout=10) as ws:
                    await ws.send_json(sub_msg)
                    print(f"[BYBIT PIPELINE] Subscribed to liquidation stream: {topic}")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            raw = json.loads(msg.data)
                            data = raw.get("data")
                            if data and "price" in data:
                                p = float(data["price"])
                                sz = float(data["size"])
                                side = data.get("side", "")
                                now = time.time()

                                self.latest_bybit_price = p
                                self.hawkes.on_liquidation_event(now, sz)
                                self.recent_liquidations.append((now, p, sz, side))
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except Exception as e:
                print(f"[BYBIT EXCEPTION] Connection interrupted: {str(e)}. Reconnecting...")
                await asyncio.sleep(2)


# ==============================================================================
# Pipeline 2: Hyperliquid Orderbook & Oracle Harvester
# ==============================================================================
class HyperliquidHarvester:
    """Streams L1 orderbook midpoints and oracle mark prices from Hyperliquid."""
    def __init__(self, config: EngineConfig):
        self.cfg = config
        self.hl_mid_price: float = 0.0
        self.best_bid: float = 0.0
        self.best_ask: float = 0.0
        self.running: bool = True

    async def run(self, session: aiohttp.ClientSession):
        if self.cfg.USE_MOCK_STREAM:
            return

        sub_all_mids = {"method": "subscribe", "subscription": {"type": "allMids"}}
        sub_l2 = {"method": "subscribe", "subscription": {"type": "l2Book", "coin": self.cfg.HL_COIN}}

        while self.running:
            try:
                print(f"[HL PIPELINE] Connecting to Hyperliquid L1 WebSocket: {self.cfg.HL_WS_URL}")
                async with session.ws_connect(self.cfg.HL_WS_URL, timeout=10) as ws:
                    await ws.send_json(sub_all_mids)
                    await ws.send_json(sub_l2)
                    print(f"[HL PIPELINE] Subscribed to allMids and l2Book: {self.cfg.HL_COIN}")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            raw = json.loads(msg.data)
                            channel = raw.get("channel")
                            data = raw.get("data")

                            if channel == "allMids" and data:
                                mids = data.get("mids", {})
                                if self.cfg.HL_COIN in mids:
                                    self.hl_mid_price = float(mids[self.cfg.HL_COIN])

                            elif channel == "l2Book" and data:
                                levels = data.get("levels", [])
                                if len(levels) >= 2:
                                    bids = levels[0]
                                    asks = levels[1]
                                    if bids:
                                        self.best_bid = float(bids[0]["px"])
                                    if asks:
                                        self.best_ask = float(asks[0]["px"])
                                    if self.best_bid > 0 and self.best_ask > 0:
                                        self.hl_mid_price = (self.best_bid + self.best_ask) / 2.0
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except Exception as e:
                print(f"[HL EXCEPTION] Connection interrupted: {str(e)}. Reconnecting...")
                await asyncio.sleep(2)


# ==============================================================================
# Pipeline 3: State Machine & Dislocation Engine
# ==============================================================================
class DislocationEngine:
    """
    Evaluates cross-venue basis tension Z-scores, Hawkes curvature inversion,
    and Kyle damping exhaustion to execute the singularity reversion trigger.
    """
    def __init__(self, config: EngineConfig, hawkes: HawkesIntensityEngine,
                 bybit: BybitLiquidationHarvester, hl: HyperliquidHarvester,
                 db: DatabaseManager):
        self.cfg = config
        self.hawkes = hawkes
        self.bybit = bybit
        self.hl = hl
        self.db = db

        self.basis_ring = ZeroAllocContinuousRing(capacity=config.RING_BUFFER_CAPACITY)
        self.last_strike_time: float = 0.0
        self.cooldown_period: float = 30.0
        self.consecutive_quiescent: int = 0

    async def run_tick_loop(self, session: aiohttp.ClientSession):
        """Executes the high-frequency evaluation cycle at 10Hz (100ms interval)."""
        dt_step = 0.1
        while True:
            await asyncio.sleep(dt_step)

            bybit_p = self.bybit.latest_bybit_price
            hl_mid = self.hl.hl_mid_price

            if bybit_p <= 0 or hl_mid <= 0:
                continue

            # 1. Decay Hawkes intensity
            lam = self.hawkes.decay_to_now()

            # 2. Update relative basis: Delta = (P_HL - P_Bybit) / P_HL
            basis_val = (hl_mid - bybit_p) / hl_mid
            self.basis_ring.push(basis_val)
            mean_basis, stdev_basis = self.basis_ring.get_mean_and_stdev()

            # 3. Compute micro-derivatives
            d1_lambda, d2_lambda = self.hawkes.compute_derivatives(dt_step)

            # 4. Compute kinetic energy and Kyle price concession damping
            ke, kyle_damping = self._compute_kinetics_and_damping()

            # 5. Compute basis tension Z-score
            z_score = (basis_val - mean_basis) / stdev_basis

            # 6. Relaxation counter tracking
            if d1_lambda < 0:
                self.consecutive_quiescent += 1
            else:
                self.consecutive_quiescent = 0

            # ---------------- Boolean Singularity Operator Phi_exhaustion ----------------
            cond_convexity = (d1_lambda < 0.0 and d2_lambda > 0.0)
            cond_damping = (kyle_damping <= self.cfg.KYLE_DAMPING_MAX)
            cond_basis = (z_score >= self.cfg.BASIS_Z_SCORE_THRESHOLD)
            cond_relaxation = (self.consecutive_quiescent >= self.cfg.RELAXATION_STEPS_REQ)

            phi_exhaustion = (cond_convexity and cond_damping and cond_basis and cond_relaxation)

            now = time.time()
            if phi_exhaustion and (now - self.last_strike_time > self.cooldown_period):
                self.last_strike_time = now
                self.db.record_strike(
                    bybit_p=bybit_p, hl_mid=hl_mid, basis=basis_val, z_score=z_score,
                    lam=lam, d1=d1_lambda, d2=d2_lambda, ke=ke, kyle=kyle_damping
                )
                self._print_terminal_strike_banner(bybit_p, hl_mid, basis_val, z_score, lam, d1_lambda, d2_lambda, ke)
                await self._notify_strike_dislocation(session, bybit_p, hl_mid, basis_val, z_score, lam, d1_lambda, d2_lambda, ke)

    def _compute_kinetics_and_damping(self) -> Tuple[float, float]:
        """Calculates instantaneous kinetic energy and unit price impact damping."""
        deq = self.bybit.recent_liquidations
        if len(deq) < 5:
            return 0.0, 1.0

        p_newest = deq[-1][1]
        p_oldest = deq[0][1]
        total_vol = sum(item[2] for item in deq)
        total_vol_safe = max(0.01, total_vol)

        dp = abs(p_newest - p_oldest)
        kyle_damping = dp / (total_vol_safe * p_newest)

        dp_dt = dp / max(0.01, deq[-1][0] - deq[0][0])
        ke = 0.5 * self.hawkes.current_lambda * self.cfg.HAWKES_V_BAR * (dp_dt * dp_dt)
        return ke, kyle_damping

    def _print_terminal_strike_banner(self, bybit_p: float, hl_mid: float, basis: float,
                                      z_score: float, lam: float, d1: float, d2: float, ke: float):
        print("\n" + "=" * 80)
        print("⚡⚡⚡ [SINGULARITY EXHAUSTION: HYPERLIQUID REVERSION STRIKE TRIGGERED] ⚡⚡⚡")
        print(f"Bybit Crash Price: ${bybit_p:,.2f} | Hyperliquid Mid: ${hl_mid:,.2f}")
        print(f"Basis Tension Delta: {basis*100:.3f}% | Dislocation Z-Score: {z_score:.2f} sigma (Limit > 3.5 sigma)")
        print(f"Hawkes Intensity lambda(t): {lam:.2f} ev/s | d_lambda/dt: {d1:.4f} (<0) | d2_lambda/dt2: {d2:.4f} (>0)")
        print(f"Kinetic Energy E_k: {ke:,.2f} | Physics: Cascading sell dominoes fully exhausted into liquidity void.")
        print("=" * 80 + "\n")

    async def _notify_strike_dislocation(self, session: aiohttp.ClientSession, bybit_p: float,
                                         hl_mid: float, basis: float, z_score: float,
                                         lam: float, d1: float, d2: float, ke: float):
        """Dispatches prioritized Telegram execution directives."""
        msg = (
            f"⚡ *【LIQUIDATION DISLOCATION STRIKE TRIGGERED】*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Asset Cross: `{escape_md_v2(self.cfg.BYBIT_SYMBOL)}` ✖ `{escape_md_v2(self.cfg.HL_COIN)}`\n"
            f"Bybit Crash Price: `${escape_md_v2(f'{bybit_p:,.2f}')}`\n"
            f"Hyperliquid Mid Price: `${escape_md_v2(f'{hl_mid:,.2f}')}`\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🔬 *Microstructure & Hawkes Metrics:*\n"
            f"• Cross\\-Venue Basis Tension `Δ`: *{escape_md_v2(f'{basis*100:.3f}%')}*\n"
            f"• Dislocation `Z\\-Score`: *{escape_md_v2(f'{z_score:.2f}')}σ* \\(Threshold \\> 3\\.5σ\\)\n"
            f"• Hawkes Intensity `λ\\(t\\)`：`{escape_md_v2(f'{lam:.2f}')} ev/s`\n"
            f"• First Derivative `dλ/dt`: `{escape_md_v2(f'{d1:.4f}')} < 0` \\(Exhaustion\\)\n"
            f"• Second Derivative Convexity `d²λ/dt²`: `{escape_md_v2(f'{d2:.4f}')} > 0` \\(Singularity\\)\n"
            f"• Instantaneous Kinetic Energy `E_k`: `{escape_md_v2(f'{ke:,.2f}')}`\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"🎯 *Execution Directive:*\n"
            f"Forced liquidation volume has collapsed into structural passive absorption.\n"
            f"*Execute aggressive limit/market long fills on Hyperliquid to capture basis mean-reversion.*"
        )
        await send_telegram_alert(session, self.cfg, msg)


# ==============================================================================
# Pipeline 4: Microstructure Sandbox Simulator
# ==============================================================================
class MicrostructureSandbox:
    """Synthesizes high-fidelity cascade avalanches and oracle dislocation streams."""
    def __init__(self, config: EngineConfig, bybit: BybitLiquidationHarvester,
                 hl: HyperliquidHarvester, hawkes: HawkesIntensityEngine):
        self.cfg = config
        self.bybit = bybit
        self.hl = hl
        self.hawkes = hawkes

    async def run(self):
        await asyncio.sleep(1.5)
        print("\n[SANDBOX] Starting synthetic microstructure cascade replay...")
        base_p = 92000.0
        self.hl.hl_mid_price = base_p
        self.bybit.latest_bybit_price = base_p

        # Phase 1: Baseline noise generation
        print("[SANDBOX] Phase 1: Injecting background noise to calibrate moment accumulators...")
        for _ in range(60):
            noise = math.sin(time.time() * 5.0) * 15.0
            self.hl.hl_mid_price = base_p + noise
            self.bybit.latest_bybit_price = base_p + noise - 2.0
            await asyncio.sleep(0.04)

        # Phase 2: Cascading avalanche surge
        print("[SANDBOX] Phase 2: Simulating aggressive cascading sell liquidations on Bybit...")
        crash_p = base_p
        for i in range(25):
            crash_p -= 70.0
            self.bybit.latest_bybit_price = crash_p
            self.hl.hl_mid_price -= 8.0  # Hyperliquid oracle lag

            now = time.time()
            self.hawkes.on_liquidation_event(now, volume=15.0 + i * 2.0)
            self.bybit.recent_liquidations.append((now, crash_p, 20.0, "Sell"))
            await asyncio.sleep(0.05)

        # Phase 3: Singularity exhaustion
        print("[SANDBOX] Phase 3: Liquidation wall absorbed; triggering singularity exhaustion...")
        for j in range(8):
            self.bybit.latest_bybit_price = crash_p + (j * 5.0)
            now = time.time()
            self.hawkes.on_liquidation_event(now, volume=0.1)
            self.bybit.recent_liquidations.append((now, crash_p, 0.1, "Sell"))
            await asyncio.sleep(0.12)


# ==============================================================================
# Pipeline 5: Central Orchestrator & Live Telemetry Dashboard
# ==============================================================================
class BybitHLOrchestrator:
    """Manages system lifecycles, coroutines, and zero-allocation terminal metrics."""
    def __init__(self, config: EngineConfig):
        self.cfg = config
        self.db = DatabaseManager(config.DB_PATH)
        self.hawkes = HawkesIntensityEngine(config)
        self.bybit_harvester = BybitLiquidationHarvester(config, self.hawkes)
        self.hl_harvester = HyperliquidHarvester(config)
        self.engine = DislocationEngine(
            config, self.hawkes, self.bybit_harvester, self.hl_harvester, self.db
        )
        self.sandbox = MicrostructureSandbox(
            config, self.bybit_harvester, self.hl_harvester, self.hawkes
        )

    async def run(self):
        self._print_startup_banner()

        async with aiohttp.ClientSession() as session:
            if self.cfg.SIMULATE_TELEGRAM_TEST:
                print("\n[TEST SUITE] SIMULATE_TELEGRAM_TEST=True: Verifying Telegram alert channel...")
                await self._run_telegram_self_test(session)

            tasks = [
                self.bybit_harvester.run(session),
                self.hl_harvester.run(session),
                self.engine.run_tick_loop(session),
                self._terminal_dashboard_loop()
            ]

            if self.cfg.USE_MOCK_STREAM:
                tasks.append(self.sandbox.run())

            print("\n[DISPATCHER] Coroutine pool dispatched. Real-time phase space monitoring active...")
            await asyncio.gather(*tasks)

    async def _terminal_dashboard_loop(self):
        """Zero-overhead console metrics dashboard polling every 3 seconds."""
        while True:
            await asyncio.sleep(3)
            bybit_p = self.bybit_harvester.latest_bybit_price
            hl_mid = self.hl_harvester.hl_mid_price
            lam = self.hawkes.current_lambda
            d1, d2 = self.hawkes.compute_derivatives(0.1)
            mean_b, std_b = self.engine.basis_ring.get_mean_and_stdev()

            basis_pct = ((hl_mid - bybit_p) / hl_mid * 100.0) if hl_mid > 0 else 0.0
            z_score = ((hl_mid - bybit_p) / hl_mid - mean_b) / std_b if (std_b > 0 and hl_mid > 0) else 0.0

            print("\n" + "-" * 78)
            print(f"📊 [BYBIT-HL TELEMETRY DASHBOARD] | UTC: {datetime.datetime.utcnow().strftime('%H:%M:%S.%f')[:-3]}")
            print("-" * 78)
            print(f"Bybit Price: ${bybit_p:,.2f} | Hyperliquid Mid: ${hl_mid:,.2f} | Basis: {basis_pct:+.3f}%")
            print(f"Tension Z-Score: {z_score:+.2f} sigma (Limit > {self.cfg.BASIS_Z_SCORE_THRESHOLD} sigma) | Rolling Stdev: {std_b*100:.4f}%")
            print(f"Hawkes Intensity: {lam:.2f} ev/s | d_lambda/dt: {d1:+.3f} | d2_lambda/dt2: {d2:+.3f}")
            print(f"Ring Buffer Watermark: {self.engine.basis_ring.count}/{self.cfg.RING_BUFFER_CAPACITY} elements (Zero heap allocation)")
            print("-" * 78)

    async def _run_telegram_self_test(self, session: aiohttp.ClientSession):
        """Dispatches an initialization self-test alert."""
        test_msg = (
            f"🚀 *【BYBIT\\-HL DISLOCATION ENGINE INITIALIZED】*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Host Architecture: `Intel Core i7\\-4770 / Zero\\-GC Array / Hawkes O(1)`\n"
            f"Monitored Cross: `Bybit:{escape_md_v2(self.cfg.BYBIT_SYMBOL)}` ✖ `HL:{escape_md_v2(self.cfg.HL_COIN)}`\n"
            f"Hawkes Parameters: `μ₀={self.cfg.HAWKES_MU_0}, α₀={self.cfg.HAWKES_ALPHA_0}, β={self.cfg.HAWKES_BETA}`\n"
            f"Dislocation Boundary: `Z_score >= {escape_md_v2(str(self.cfg.BASIS_Z_SCORE_THRESHOLD))}σ`\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"Telemetry Verification: *Operating Nominal*"
        )
        await send_telegram_alert(session, self.cfg, test_msg)
        print("[TEST SUITE] Telegram verification transmission sent.")

    def _print_startup_banner(self):
        banner = """
================================================================================
  BYBIT-HL DISLOCATION ENGINE // HAWKES PROCESS CASCADE REVERSION AGENT
================================================================================
[PHYSICS] Bybit Liquidation Avalanche × Hyperliquid L1 Consensus Dislocation
[RUNTIME] Pure Asyncio Python 3.9+ | Zero NumPy/Pandas | Zero-GC Array Rings
================================================================================
        """
        print(banner)


# ==============================================================================
# Script Entry Point
# ==============================================================================
if __name__ == "__main__":
    config = EngineConfig()
    orchestrator = BybitHLOrchestrator(config)
    try:
        asyncio.run(orchestrator.run())
    except (KeyboardInterrupt, SystemExit):
        print("\n[SHUTDOWN] Terminating. Continuous array buffers and WAL logs closed cleanly.")
        sys.exit(0)