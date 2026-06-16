"""
PSX REST Polling Monitor
========================
Replaces the broken WebSocket monitor with a robust pure-REST polling approach.

Strategy (3 tiers, fastest to slowest):
  1. Bulk  — /api/stats/REG  (top gainers/losers/volume — instant, ~50 symbols)
  2. Ticks — /api/ticks/{market}/{symbol} polled concurrently via a thread pool
  3. Fall  — single-symbol REST on cache-miss in get_price()

Refresh cycle:
  - Stats  endpoint: every POLL_INTERVAL_FAST seconds (default 7s)
  - Full   tick sweep: every POLL_INTERVAL_FULL seconds (default 30s)
  - Each   cycle is non-blocking; results land in price_cache immediately.

No websocket-client, no gevent, no socketio — just requests + threading.
"""

import os
import time
import threading
import logging
import requests
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)

# ── Env-driven config ─────────────────────────────────────────────────────────
ALERT_THRESHOLD     = float(os.environ.get("ALERT_THRESHOLD",     "4.0"))
MIN_VALUE_THRESHOLD = float(os.environ.get("MIN_VALUE_THRESHOLD", "20000000"))
MONITOR_WINDOW      = int(os.environ.get("MONITOR_WINDOW",        "1200"))
REQUEST_DELAY       = float(os.environ.get("REQUEST_DELAY",       "0.1"))   # between per-sym requests
POLL_INTERVAL_FAST  = float(os.environ.get("POLL_INTERVAL_FAST",  "7"))     # stats refresh (seconds)
POLL_INTERVAL_FULL  = float(os.environ.get("POLL_INTERVAL_FULL",  "30"))    # full tick sweep (seconds)
MAX_WORKERS         = int(os.environ.get("REST_WORKERS",           "12"))    # concurrent tick fetchers

ALL_MARKETS = ["REG", "IDX", "FUT", "ODL", "BNB"]

PSX_BASE   = "https://psxterminal.com"
HEADERS    = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json",
    "Origin": PSX_BASE,
    "Referer": PSX_BASE + "/",
}


class PSXRestMonitor:
    """
    Drop-in replacement for PSXWebSocketMonitor using only REST APIs.
    Public interface is identical so no other file needs structural changes.
    """

    def __init__(self):
        # {symbol: {price, change, change_percent, volume, value, updated_at, …}}
        self.price_cache: dict = {}
        # {symbol: [(unix_ts, price), …]}
        self.price_history: dict = defaultdict(list)
        # Registered alerts (same structure as websocket_monitor)
        self.alerts: dict = {}

        self._lock = threading.Lock()

        self._stats = {
            "total_updates": 0,
            "alerts_triggered": 0,
            "last_fast_poll": None,
            "last_full_poll": None,
            "poll_errors": 0,
        }

        # Known symbol list (populated on first fetch, updated periodically)
        self._symbols: list = []

        # Tracks whether a full sweep is already running
        self._sweep_running = False

        # Bootstrap: seed immediately, then start background threads
        self._seed_stats()
        self._fetch_symbol_list()
        self._start_background()

    # ── Startup helpers ───────────────────────────────────────────────────────

    def _start_background(self):
        threading.Thread(target=self._fast_poll_loop,  daemon=True, name="psx-fast").start()
        threading.Thread(target=self._full_sweep_loop, daemon=True, name="psx-full").start()
        logger.info("✅ PSX REST monitor background threads started.")

    # ── Symbol list ───────────────────────────────────────────────────────────

    def _fetch_symbol_list(self):
        try:
            resp = requests.get(f"{PSX_BASE}/api/symbols", headers=HEADERS, timeout=10)
            if resp.status_code == 200:
                data = resp.json()
                if data.get("success"):
                    self._symbols = data.get("data", [])
                    logger.info(f"✅ Symbol list: {len(self._symbols)} symbols.")
        except Exception as exc:
            logger.warning(f"Symbol list fetch failed: {exc}")

    # ── Tier 1: fast stats poll ────────────────────────────────────────────────

    def _seed_stats(self):
        """Fetch /api/stats/REG — covers top ~50 movers instantly."""
        try:
            resp = requests.get(f"{PSX_BASE}/api/stats/REG", headers=HEADERS, timeout=10)
            if resp.status_code != 200:
                return
            payload = resp.json()
            if not payload.get("success"):
                return
            reg = payload.get("data", {})
            seeded = 0
            now_ts = time.time()
            for category in ("topGainers", "topLosers", "topVolume"):
                for stock in reg.get(category, []):
                    sym = stock.get("symbol")
                    price = stock.get("price")
                    if not (sym and price):
                        continue
                    pch = stock.get("changePercent", 0)
                    if -1.0 < pch < 1.0 and pch != 0:
                        pch *= 100
                    entry = {
                        "price": price,
                        "change": stock.get("change", 0),
                        "change_percent": pch,
                        "volume": stock.get("volume", 0),
                        "value": stock.get("value", 0),
                        "market": "REG",
                        "updated_at": datetime.now().isoformat(),
                    }
                    with self._lock:
                        self.price_cache[sym] = entry
                        self.price_history[sym].append((now_ts, price))
                        self._trim_history(sym, now_ts)
                    self._check_alerts(sym, price)
                    seeded += 1

            with self._lock:
                self._stats["total_updates"] += seeded
                self._stats["last_fast_poll"] = datetime.now().isoformat()

            logger.info(f"📡 Stats poll: {seeded} prices updated.")
        except Exception as exc:
            logger.warning(f"Stats poll error: {exc}")
            with self._lock:
                self._stats["poll_errors"] += 1

    def _fast_poll_loop(self):
        """Run stats poll every POLL_INTERVAL_FAST seconds."""
        while True:
            time.sleep(POLL_INTERVAL_FAST)
            self._seed_stats()

    # ── Tier 2: full concurrent tick sweep ────────────────────────────────────

    def _fetch_tick(self, symbol: str) -> tuple[str, dict | None]:
        """
        Fetch tick data for one symbol across all markets.
        Returns (symbol, entry_dict) or (symbol, None).
        """
        for market in ALL_MARKETS:
            try:
                resp = requests.get(
                    f"{PSX_BASE}/api/ticks/{market}/{symbol}",
                    headers=HEADERS,
                    timeout=5,
                )
                if resp.status_code != 200:
                    continue
                data = resp.json()
                if not data.get("success"):
                    continue
                tick = data.get("data", {})
                price = tick.get("price")
                if not price:
                    continue
                pch = tick.get("changePercent", 0)
                if -1.0 < pch < 1.0 and pch != 0:
                    pch *= 100
                return symbol, {
                    "price": price,
                    "change": tick.get("change", 0),
                    "change_percent": pch,
                    "volume": tick.get("volume", 0),
                    "value": tick.get("value", 0),
                    "ldcp": tick.get("ldcp", price),
                    "high": tick.get("high", price),
                    "low": tick.get("low", price),
                    "market": market,
                    "updated_at": datetime.now().isoformat(),
                }
            except Exception:
                pass
        return symbol, None

    def _run_full_sweep(self):
        """Concurrently fetch ticks for all known symbols."""
        if self._sweep_running:
            logger.debug("Full sweep already running — skipping.")
            return
        self._sweep_running = True
        symbols = list(self._symbols)  # snapshot

        if not symbols:
            self._sweep_running = False
            return

        logger.info(f"🔄 Full tick sweep for {len(symbols)} symbols (workers={MAX_WORKERS})…")
        updated = 0
        now_ts = time.time()

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
            futures = {pool.submit(self._fetch_tick, sym): sym for sym in symbols}
            for future in as_completed(futures):
                try:
                    sym, entry = future.result()
                    if entry:
                        with self._lock:
                            self.price_cache[sym] = entry
                            self.price_history[sym].append((now_ts, entry["price"]))
                            self._trim_history(sym, now_ts)
                        self._check_alerts(sym, entry["price"])
                        updated += 1
                except Exception:
                    pass

        with self._lock:
            self._stats["total_updates"] += updated
            self._stats["last_full_poll"] = datetime.now().isoformat()

        logger.info(f"✅ Full sweep done: {updated}/{len(symbols)} updated.")
        self._sweep_running = False

    def _full_sweep_loop(self):
        """Run full tick sweep every POLL_INTERVAL_FULL seconds."""
        # Initial delay: let stats seed first
        time.sleep(15)
        while True:
            self._run_full_sweep()
            time.sleep(POLL_INTERVAL_FULL)

    # ── History helper ────────────────────────────────────────────────────────

    def _trim_history(self, symbol: str, now_ts: float):
        """Keep only entries within the rolling MONITOR_WINDOW."""
        self.price_history[symbol] = [
            p for p in self.price_history[symbol]
            if now_ts - p[0] <= MONITOR_WINDOW
        ]

    # ── Alert checking ────────────────────────────────────────────────────────

    def _check_alerts(self, symbol: str, current_price: float):
        if symbol not in self.alerts:
            return
        with self._lock:
            stock_data = self.price_cache.get(symbol, {})
            volume = stock_data.get("volume", 0)
            history = list(self.price_history.get(symbol, []))

        traded_value = current_price * volume
        if traded_value < MIN_VALUE_THRESHOLD:
            return
        if len(history) < 2:
            return
        old_price = history[0][1]
        if old_price <= 0:
            return
        price_gain = ((current_price - old_price) / old_price) * 100
        if price_gain < ALERT_THRESHOLD:
            return
        for alert in self.alerts[symbol]:
            if alert.get("active"):
                self._trigger_alert(alert, current_price, price_gain)

    def _trigger_alert(self, alert: dict, current_price: float, change_pct: float):
        symbol = alert.get("symbol")
        logger.info(f"🚨 ALERT: {symbol} {change_pct:+.2f}% @ Rs. {current_price:.2f}")
        alert["active"] = False
        alert["triggered_at"] = datetime.now().isoformat()
        alert["trigger_price"] = current_price
        alert["trigger_percent"] = change_pct
        with self._lock:
            self._stats["alerts_triggered"] += 1

    # ── Public API (identical to PSXWebSocketMonitor) ─────────────────────────

    def get_price(self, symbol: str) -> dict | None:
        """Return cached price, falling back to a live REST call."""
        symbol = symbol.upper()
        with self._lock:
            cached = self.price_cache.get(symbol)
        if cached and cached.get("price"):
            return cached

        # Tier-3 fallback: live single-symbol fetch
        logger.info(f"Cache miss for {symbol} — live REST fallback…")
        _, entry = self._fetch_tick(symbol)
        if entry:
            with self._lock:
                self.price_cache[symbol] = entry
        return entry  # None if not found

    def get_all_prices(self) -> dict:
        with self._lock:
            return {
                sym: data
                for sym, data in self.price_cache.items()
                if data.get("price") is not None
            }

    def add_alert(self, symbol: str, phone: str, base_price: float,
                  threshold: float = 5.0, direction: str = "both") -> dict:
        symbol = symbol.upper()
        self.alerts.setdefault(symbol, [])
        alert = {
            "id": f"{symbol}-{int(time.time())}",
            "symbol": symbol,
            "phone": phone,
            "base_price": float(base_price),
            "threshold": float(threshold),
            "direction": direction,
            "active": True,
            "created_at": datetime.now().isoformat(),
        }
        self.alerts[symbol].append(alert)
        return alert

    def get_alerts(self, symbol: str | None = None) -> list | dict:
        if symbol:
            return self.alerts.get(symbol.upper(), [])
        return self.alerts

    def deactivate_alert(self, alert_id: str) -> bool:
        for alerts in self.alerts.values():
            for alert in alerts:
                if alert.get("id") == alert_id:
                    alert["active"] = False
                    return True
        return False

    def get_stats(self) -> dict:
        with self._lock:
            s = dict(self._stats)
            cached = len([d for d in self.price_cache.values() if d.get("price")])
            active_alerts = sum(
                sum(1 for a in alerts if a.get("active"))
                for alerts in self.alerts.values()
            )
        return {
            **s,
            "connected": True,        # always "connected" with REST
            "cached_symbols": cached,
            "active_alerts": active_alerts,
            "poll_interval_fast": POLL_INTERVAL_FAST,
            "poll_interval_full": POLL_INTERVAL_FULL,
        }
