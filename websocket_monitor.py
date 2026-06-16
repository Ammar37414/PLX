"""
PSX WebSocket Monitor — simplified & fixed version
====================================================
Key fixes vs. previous version:
  1. Cache warmer now tries ALL market types (REG, IDX, FUT, ODL, BNB) for every
     symbol, so stocks listed outside REG are no longer silently skipped.
  2. `None`-price placeholders are no longer inserted; the warmer will therefore
     always attempt a fetch for an uncached symbol.
  3. `_periodic_refresh` cancels any running warmer before spawning a new one to
     prevent thread pile-up.
  4. WebSocket subscription extended to all known market types.
  5. `get_price()` returns None cleanly when all fallbacks fail (no stale dict).
  6. Single, clear `get_all_prices()` output — only symbols with a valid price.
"""

import os
import websocket
import json
import threading
import time
from datetime import datetime
from collections import defaultdict
import requests
import logging

# ── Alert logic — all pulled from environment (see .env.example) ──────────────
ALERT_THRESHOLD   = float(os.environ.get("ALERT_THRESHOLD",   "4.0"))    # % surge to trigger
MIN_VALUE_THRESHOLD = float(os.environ.get("MIN_VALUE_THRESHOLD", "20000000"))  # Rs. min traded value
MONITOR_WINDOW    = int(os.environ.get("MONITOR_WINDOW",    "1200"))     # rolling window seconds
REQUEST_DELAY     = float(os.environ.get("REQUEST_DELAY",   "0.65"))     # seconds between REST calls

logger = logging.getLogger(__name__)

# All PSX market segments known to psxterminal.com
ALL_MARKETS = ["REG", "IDX", "FUT", "ODL", "BNB"]


class PSXWebSocketMonitor:
    """Real-time PSX stock monitor using WebSocket + REST fallback."""

    def __init__(self):
        # ── WebSocket state ────────────────────────────────────────────────
        self.ws_url = "wss://psxterminal.com/"
        self.ws = None
        self.connected = False

        # Shared HTTP headers (mimic a real browser to avoid bot-blocks)
        self.headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
            "Origin": "https://psxterminal.com",
        }

        # ── Data stores ────────────────────────────────────────────────────
        # {symbol: {price, change, change_percent, volume, value, …}}
        self.price_cache: dict = {}
        # {symbol: [(unix_ts, price), …]}  — rolling 20-min window
        self.price_history: dict = defaultdict(list)
        # Alerts registered by the app layer
        self.alerts: dict = {}

        # ── Stats ──────────────────────────────────────────────────────────
        self.stats = {
            "total_updates": 0,
            "alerts_triggered": 0,
            "websocket_errors": 0,
            "connected_at": None,
        }

        # ── Reconnect bookkeeping ──────────────────────────────────────────
        self.reconnect_attempts = 0
        self.max_reconnect = 10

        # Reference to any running warmer so we can avoid duplicates
        self._warmer_thread: threading.Thread | None = None

        # ── Bootstrap ─────────────────────────────────────────────────────
        self._fetch_initial_data()   # seed from REST
        self._start_background()     # start WS + housekeeping threads

    # ──────────────────────────────────────────────────────────────────────
    # Internal: startup helpers
    # ──────────────────────────────────────────────────────────────────────

    def _start_background(self):
        """Launch WebSocket, heartbeat-monitor, and refresh threads."""
        threading.Thread(target=self._connect_websocket, daemon=True,
                         name="psx-ws").start()
        threading.Thread(target=self._monitor_connection, daemon=True,
                         name="psx-heartbeat").start()
        threading.Thread(target=self._periodic_refresh, daemon=True,
                         name="psx-refresh").start()

    # ──────────────────────────────────────────────────────────────────────
    # Internal: REST data fetching
    # ──────────────────────────────────────────────────────────────────────

    def _fetch_initial_data(self):
        """
        1. Fetch symbol list from /api/symbols.
        2. Quick-seed from /api/stats/REG (top-movers — immediate data).
        3. Launch background warmer for remaining symbols.
        """
        try:
            logger.info("Fetching symbol list from PSX Terminal…")
            resp = requests.get(
                "https://psxterminal.com/api/symbols",
                headers=self.headers,
                timeout=10,
            )
            symbols: list[str] = []
            if resp.status_code == 200:
                data = resp.json()
                if data.get("success"):
                    symbols = data.get("data", [])
                    logger.info(f"✅ {len(symbols)} symbols found.")
            else:
                logger.warning(f"Symbol list fetch returned HTTP {resp.status_code}")
        except Exception as exc:
            logger.error(f"Could not fetch symbol list: {exc}")
            symbols = []

        # Quick seed from stats endpoint (instant data for top-movers)
        self._seed_from_stats()

        # Kick off the background warmer only if not already running
        if symbols and not (
            self._warmer_thread and self._warmer_thread.is_alive()
        ):
            self._warmer_thread = threading.Thread(
                target=self._warm_cache,
                args=(symbols,),
                daemon=True,
                name="psx-warmer",
            )
            self._warmer_thread.start()

    def _seed_from_stats(self):
        """Quick-seed top gainers/losers/volume from the stats endpoint."""
        try:
            resp = requests.get(
                "https://psxterminal.com/api/stats/REG",
                headers=self.headers,
                timeout=10,
            )
            if resp.status_code != 200:
                return
            stats_data = resp.json()
            if not stats_data.get("success"):
                return
            reg_data = stats_data.get("data", {})
            seeded = 0
            for category in ("topGainers", "topLosers", "topVolume"):
                for stock in reg_data.get(category, []):
                    sym = stock.get("symbol")
                    price = stock.get("price")
                    if sym and price:
                        self.price_cache[sym] = {
                            "price": price,
                            "change": stock.get("change", 0),
                            "change_percent": stock.get("changePercent", 0) * 100,
                            "volume": stock.get("volume", 0),
                            "value": stock.get("value", 0),
                            "updated_at": datetime.now().isoformat(),
                        }
                        seeded += 1
            logger.info(f"✅ Seeded {seeded} top-mover prices from stats/REG.")
        except Exception as exc:
            logger.warning(f"Stats seed failed: {exc}")

    def _warm_cache(self, symbols: list[str]):
        """
        Fetch a price for every symbol that is not yet in the cache.

        FIX: Try REG first, then other markets (IDX, FUT, ODL, BNB) so that
        non-REG stocks are not silently dropped.
        """
        logger.info(f"🔥 Cache warmer started for {len(symbols)} symbols…")
        found = 0
        for idx, sym in enumerate(symbols, start=1):
            # Skip if we already have a valid price
            if self.price_cache.get(sym, {}).get("price"):
                continue

            for market in ALL_MARKETS:
                try:
                    time.sleep(REQUEST_DELAY)  # controlled by REQUEST_DELAY env var
                    resp = requests.get(
                        f"https://psxterminal.com/api/ticks/{market}/{sym}",
                        headers=self.headers,
                        timeout=5,
                    )
                    if resp.status_code != 200:
                        continue
                    data = resp.json()
                    if not data.get("success"):
                        continue
                    tick = data.get("data", {})
                    price = tick.get("price")
                    if price:
                        pch = tick.get("changePercent", 0)
                        # Normalize: API sometimes returns 0.019 instead of 1.9
                        if -1.0 < pch < 1.0 and pch != 0:
                            pch *= 100
                        self.price_cache[sym] = {
                            "price": price,
                            "change": tick.get("change", 0),
                            "change_percent": pch,
                            "volume": tick.get("volume", 0),
                            "value": tick.get("value", 0),
                            "market": market,
                            "updated_at": datetime.now().isoformat(),
                        }
                        found += 1
                        break  # stop trying other markets for this symbol
                except Exception:
                    pass  # silent: keep moving to next market / symbol

            if idx % 20 == 0:
                logger.info(
                    f"🔥 Warmer progress: {idx}/{len(symbols)} processed, "
                    f"{found} prices cached."
                )

        logger.info(f"✅ Cache warmer finished. {found}/{len(symbols)} symbols priced.")

    def _periodic_refresh(self):
        """Re-run initial fetch every 5 minutes as a fallback."""
        while True:
            time.sleep(300)
            logger.info("🔄 Periodic refresh triggered…")
            self._seed_from_stats()  # quick update for top-movers
            # Only start a new warmer if the previous one has finished
            if not (self._warmer_thread and self._warmer_thread.is_alive()):
                self._fetch_initial_data()

    # ──────────────────────────────────────────────────────────────────────
    # Internal: WebSocket
    # ──────────────────────────────────────────────────────────────────────

    def _connect_websocket(self):
        """Connect (and reconnect) to the PSX WebSocket."""
        while self.reconnect_attempts < self.max_reconnect:
            try:
                logger.info(f"Connecting to {self.ws_url}…")
                self.ws = websocket.WebSocketApp(
                    self.ws_url,
                    on_open=self._on_open,
                    on_message=self._on_message,
                    on_error=self._on_error,
                    on_close=self._on_close,
                    header=self.headers,
                )
                self.ws.run_forever(ping_interval=30, ping_timeout=10, reconnect=5)
            except Exception as exc:
                logger.error(f"WebSocket connect error: {exc}")
                self.reconnect_attempts += 1
                backoff = min(30, 2 ** self.reconnect_attempts)
                logger.info(f"Retrying in {backoff}s (attempt {self.reconnect_attempts})…")
                time.sleep(backoff)

    def _on_open(self, ws):
        logger.info("✅ WebSocket connected to PSX Terminal.")
        self.connected = True
        self.reconnect_attempts = 0
        self.stats["connected_at"] = datetime.now()

        # Subscribe to all market segments
        for market in ALL_MARKETS:
            msg = {
                "type": "subscribe",
                "subscriptionType": "marketData",
                "params": {"marketType": market},
                "requestId": f"sub-{market}-{int(time.time())}",
            }
            try:
                ws.send(json.dumps(msg))
                logger.info(f"Subscribed to {market} market data.")
            except Exception as exc:
                logger.warning(f"Subscribe {market} failed: {exc}")

    def _on_message(self, ws, raw):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug(f"Non-JSON message (first 100 chars): {raw[:100]}")
            return

        msg_type = data.get("type")

        if msg_type == "welcome":
            logger.info(f"Server: {data.get('message')}")
        elif msg_type == "tickUpdate":
            self._process_tick(data)
        elif msg_type == "ping":
            try:
                ws.send(json.dumps({"type": "pong", "timestamp": data.get("timestamp")}))
            except Exception:
                pass
        elif msg_type == "error":
            logger.error(f"PSX WS error msg: {data.get('message')}")
            self.stats["websocket_errors"] += 1

        self.stats["total_updates"] += 1
        if self.stats["total_updates"] % 200 == 0:
            logger.info(
                f"📡 WS updates: {self.stats['total_updates']}, "
                f"cached symbols: {len(self.price_cache)}"
            )

    def _process_tick(self, data: dict):
        """Parse a tickUpdate message and update the price cache."""
        tick = data.get("tick", {})

        # Symbol can live at different paths depending on the server version
        symbol = (
            data.get("symbol")
            or tick.get("s")
            or data.get("data", {}).get("symbol")
            or data.get("data", {}).get("s")
        )
        # Price likewise
        price = (
            tick.get("c")
            or data.get("data", {}).get("c")
            or data.get("data", {}).get("price")
        )

        if not (symbol and price):
            return  # incomplete tick — skip

        # Normalize change-percent (sometimes a decimal fraction, not %)
        raw_pch = tick.get("pch", 0)
        if -1.0 < raw_pch < 1.0 and raw_pch != 0:
            raw_pch *= 100

        now_ts = time.time()
        self.price_cache[symbol] = {
            "price": price,
            "change": tick.get("ch", 0),
            "change_percent": raw_pch,
            "volume": tick.get("v", 0),
            "trades": tick.get("tr", 0),
            "value": tick.get("val", 0),
            "high": tick.get("h", price),
            "low": tick.get("l", price),
            "timestamp": data.get("timestamp", int(now_ts * 1000)),
            "updated_at": datetime.now().isoformat(),
            "market_status": tick.get("st", "UNKNOWN"),
        }

        # Rolling window history (length controlled by MONITOR_WINDOW env var)
        self.price_history[symbol].append((now_ts, price))
        self.price_history[symbol] = [
            p for p in self.price_history[symbol] if now_ts - p[0] <= MONITOR_WINDOW
        ]

        self._check_alerts(symbol, price)

    def _on_error(self, ws, error):
        logger.error(f"WebSocket error: {error}")
        self.connected = False
        self.stats["websocket_errors"] += 1

    def _on_close(self, ws, code, msg):
        logger.info(f"WebSocket closed: {code} — {msg}")
        self.connected = False

    def _monitor_connection(self):
        """Watchdog: restart the WS connection if it drops."""
        while True:
            time.sleep(30)
            if not self.connected:
                logger.warning("WebSocket is down — attempting reconnect…")
                self.reconnect_attempts = 0  # reset counter for fresh attempt
                self._connect_websocket()

    # ──────────────────────────────────────────────────────────────────────
    # Internal: alert checking
    # ──────────────────────────────────────────────────────────────────────

    def _check_alerts(self, symbol: str, current_price: float):
        """Fire registered alerts if conditions are met."""
        if symbol not in self.alerts:
            return

        stock_data = self.price_cache.get(symbol, {})
        volume = stock_data.get("volume", 0)
        traded_value = current_price * volume
        if traded_value < MIN_VALUE_THRESHOLD:
            return

        history = self.price_history.get(symbol, [])
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
        self.stats["alerts_triggered"] += 1

    # ──────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────

    def get_price(self, symbol: str) -> dict | None:
        """
        Return cached price data for *symbol*, attempting REST fallback if
        not yet cached.  Returns None (not an empty dict) when unavailable.
        """
        symbol = symbol.upper()
        cached = self.price_cache.get(symbol)
        if cached and cached.get("price"):
            return cached

        # REST fallback: try every market type
        logger.info(f"Cache miss for {symbol} — trying REST fallback…")
        for market in ALL_MARKETS:
            try:
                resp = requests.get(
                    f"https://psxterminal.com/api/ticks/{market}/{symbol}",
                    headers=self.headers,
                    timeout=5,
                )
                if resp.status_code != 200:
                    continue
                data = resp.json()
                if not data.get("success"):
                    continue
                tick = data.get("data", {})
                price = tick.get("price")
                if price:
                    pch = tick.get("changePercent", 0)
                    if -1.0 < pch < 1.0 and pch != 0:
                        pch *= 100
                    entry = {
                        "price": price,
                        "change_percent": pch,
                        "volume": tick.get("volume", 0),
                        "market": market,
                        "updated_at": datetime.now().isoformat(),
                    }
                    self.price_cache[symbol] = entry
                    return entry
            except Exception as exc:
                logger.debug(f"REST fallback {market}/{symbol}: {exc}")

        logger.warning(f"No price found for {symbol} across all markets.")
        return None  # explicit None — not a stale dict

    def get_all_prices(self) -> dict:
        """Return only symbols with a valid (non-None) price."""
        return {
            sym: data
            for sym, data in self.price_cache.items()
            if data.get("price") is not None
        }

    def add_alert(self, symbol: str, phone: str, base_price: float,
                  threshold: float = 5.0, direction: str = "both") -> dict:
        """Register a price alert for a symbol."""
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
        logger.info(f"Alert registered: {symbol} @ Rs. {base_price} (±{threshold}%) → {phone}")
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
                    logger.info(f"Alert deactivated: {alert_id}")
                    return True
        return False

    def get_stats(self) -> dict:
        uptime_secs = 0
        if self.stats["connected_at"]:
            uptime_secs = int(
                (datetime.now() - self.stats["connected_at"]).total_seconds()
            )
        return {
            **self.stats,
            "connected": self.connected,
            "cached_symbols": len(self.get_all_prices()),
            "active_alerts": sum(
                sum(1 for a in alerts if a.get("active"))
                for alerts in self.alerts.values()
            ),
            "uptime": uptime_secs,
        }