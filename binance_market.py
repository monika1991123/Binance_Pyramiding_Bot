import json
import logging
import threading
import time
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Iterable, List, Optional

import requests
import websocket

from config import SETTINGS, Settings

LOG = logging.getLogger("binance.market")


@dataclass(frozen=True)
class SymbolRule:
    symbol: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    min_notional: Decimal


class BinanceFuturesMarket:
    def __init__(self, settings: Settings = SETTINGS):
        self.cfg = settings
        self.http = requests.Session()
        self.http.headers.update({"User-Agent": "binance-pyramiding-paper-v2/1"})
        self.lock = threading.RLock()
        self.rules: Dict[str, SymbolRule] = {}
        self.exchange_info_at = 0.0
        self.last_used_weight = 0
        self.last_weight_at = 0.0
        self.last_http_ok_at = 0.0
        self.last_http_error = ""
        self.books: Dict[str, dict] = {}
        self.marks: Dict[str, dict] = {}
        self.ws_public_app: Optional[websocket.WebSocketApp] = None
        self.ws_market_app: Optional[websocket.WebSocketApp] = None
        self.ws_public_thread: Optional[threading.Thread] = None
        self.ws_market_thread: Optional[threading.Thread] = None
        self.ws_stop = threading.Event()
        self.ws_symbols: List[str] = []
        self.ws_public_connected = False
        self.ws_market_connected = False
        self.ws_public_last_message_at = 0.0
        self.ws_market_last_message_at = 0.0
        self.ws_public_last_error = ""
        self.ws_market_last_error = ""

    def _respect_soft_limit(self) -> None:
        with self.lock:
            used = self.last_used_weight
            age = time.time() - self.last_weight_at
        if used >= self.cfg.rest_soft_weight_per_minute and age < 60:
            sleep_for = min(5.0, max(0.5, 60.0 - age))
            LOG.warning("REST soft limit pause: used_weight=%s sleep=%.1fs", used, sleep_for)
            time.sleep(sleep_for)

    def _get(self, path: str, params: Optional[dict] = None, timeout: float = 10.0):
        url = f"{self.cfg.rest_base}{path}"
        last_error = None
        for attempt in range(6):
            self._respect_soft_limit()
            try:
                response = self.http.get(url, params=params, timeout=timeout)
                weight = response.headers.get("X-MBX-USED-WEIGHT-1M")
                if weight is not None:
                    with self.lock:
                        self.last_used_weight = int(float(weight))
                        self.last_weight_at = time.time()
                if response.status_code in (418, 429):
                    retry_after = response.headers.get("Retry-After")
                    delay = float(retry_after) if retry_after else min(60.0, 2.0 ** (attempt + 1))
                    self.last_http_error = f"HTTP {response.status_code}: {response.text[:180]}"
                    LOG.warning("Binance rate limit HTTP %s; backoff %.1fs", response.status_code, delay)
                    time.sleep(delay)
                    continue
                if response.status_code >= 500:
                    delay = min(20.0, 1.5 ** (attempt + 1))
                    self.last_http_error = f"HTTP {response.status_code}: {response.text[:180]}"
                    time.sleep(delay)
                    continue
                response.raise_for_status()
                self.last_http_ok_at = time.time()
                self.last_http_error = ""
                return response.json()
            except (requests.RequestException, ValueError) as exc:
                last_error = exc
                self.last_http_error = str(exc)
                time.sleep(min(15.0, 1.5 ** (attempt + 1)))
        raise RuntimeError(f"Binance REST failed {path}: {last_error or self.last_http_error}")

    def refresh_exchange_info(self, force: bool = False) -> Dict[str, SymbolRule]:
        if not force and self.rules and time.time() - self.exchange_info_at < 6 * 3600:
            return self.rules
        payload = self._get("/fapi/v1/exchangeInfo")
        parsed: Dict[str, SymbolRule] = {}
        for item in payload.get("symbols", []):
            if item.get("quoteAsset") != "USDT" or item.get("contractType") != "PERPETUAL" or item.get("status") != "TRADING":
                continue
            filters = {f.get("filterType"): f for f in item.get("filters", [])}
            price_filter = filters.get("PRICE_FILTER", {})
            lot_filter = filters.get("LOT_SIZE", {})
            notional_filter = filters.get("MIN_NOTIONAL", {})
            try:
                rule = SymbolRule(
                    symbol=item["symbol"],
                    tick_size=Decimal(str(price_filter.get("tickSize", "0"))),
                    step_size=Decimal(str(lot_filter.get("stepSize", "0"))),
                    min_qty=Decimal(str(lot_filter.get("minQty", "0"))),
                    max_qty=Decimal(str(lot_filter.get("maxQty", "0"))),
                    min_notional=Decimal(str(notional_filter.get("notional", "0"))),
                )
            except Exception:
                continue
            if rule.tick_size > 0 and rule.step_size > 0:
                parsed[rule.symbol] = rule
        if not parsed:
            raise RuntimeError("No Binance USDT perpetual symbols parsed from exchangeInfo")
        with self.lock:
            self.rules = parsed
            self.exchange_info_at = time.time()
        return parsed

    def tradable_symbols(self) -> List[str]:
        return sorted(self.refresh_exchange_info().keys())

    @staticmethod
    def _floor_step(value: Decimal, step: Decimal) -> Decimal:
        if step <= 0:
            return value
        units = (value / step).to_integral_value(rounding=ROUND_DOWN)
        return units * step

    def round_price(self, symbol: str, value: float) -> float:
        rule = self.refresh_exchange_info().get(symbol)
        if not rule:
            raise KeyError(f"Unknown Binance symbol rule: {symbol}")
        return float(self._floor_step(Decimal(str(value)), rule.tick_size))

    def round_qty(self, symbol: str, value: float) -> float:
        rule = self.refresh_exchange_info().get(symbol)
        if not rule:
            raise KeyError(f"Unknown Binance symbol rule: {symbol}")
        qty = self._floor_step(Decimal(str(value)), rule.step_size)
        if qty < rule.min_qty:
            return 0.0
        if rule.max_qty > 0:
            qty = min(qty, rule.max_qty)
        return float(qty)

    def min_notional(self, symbol: str) -> float:
        rule = self.refresh_exchange_info().get(symbol)
        return float(rule.min_notional) if rule else 0.0

    def all_24h_tickers(self) -> List[dict]:
        payload = self._get("/fapi/v1/ticker/24hr", timeout=15.0)
        return payload if isinstance(payload, list) else []

    def all_book_tickers(self) -> Dict[str, dict]:
        payload = self._get("/fapi/v1/ticker/bookTicker", timeout=15.0)
        rows = payload if isinstance(payload, list) else [payload]
        result = {}
        now = int(time.time() * 1000)
        for row in rows:
            symbol = row.get("symbol")
            if not symbol:
                continue
            try:
                result[symbol] = {
                    "bid": float(row["bidPrice"]),
                    "ask": float(row["askPrice"]),
                    "bid_qty": float(row.get("bidQty", 0)),
                    "ask_qty": float(row.get("askQty", 0)),
                    "time": int(row.get("time") or now),
                }
            except (KeyError, TypeError, ValueError):
                continue
        return result

    def klines(self, symbol: str, interval: str, limit: int) -> List[dict]:
        payload = self._get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        now_ms = int(time.time() * 1000)
        bars = []
        for row in payload if isinstance(payload, list) else []:
            try:
                bars.append({
                    "open_time": int(row[0]),
                    "open": float(row[1]),
                    "high": float(row[2]),
                    "low": float(row[3]),
                    "close": float(row[4]),
                    "volume": float(row[5]),
                    "close_time": int(row[6]),
                    "closed": int(row[6]) < now_ms,
                })
            except (IndexError, TypeError, ValueError):
                continue
        return bars

    def book(self, symbol: str, max_age: float = 8.0) -> dict:
        with self.lock:
            cached = dict(self.books.get(symbol, {}))
        if cached and time.time() - cached.get("received_at", 0) <= max_age:
            return cached
        row = self._get("/fapi/v1/ticker/bookTicker", {"symbol": symbol})
        result = {
            "bid": float(row["bidPrice"]),
            "ask": float(row["askPrice"]),
            "bid_qty": float(row.get("bidQty", 0)),
            "ask_qty": float(row.get("askQty", 0)),
            "time": int(row.get("time") or time.time() * 1000),
            "received_at": time.time(),
            "source": "REST-fallback",
        }
        with self.lock:
            self.books[symbol] = result
        return result

    def mark_price(self, symbol: str, max_age: float = 8.0) -> float:
        with self.lock:
            cached = dict(self.marks.get(symbol, {}))
        if cached and time.time() - cached.get("received_at", 0) <= max_age:
            return float(cached["price"])
        row = self._get("/fapi/v1/premiumIndex", {"symbol": symbol})
        price = float(row["markPrice"])
        with self.lock:
            self.marks[symbol] = {"price": price, "time": int(row.get("time") or time.time() * 1000), "received_at": time.time(), "source": "REST-fallback"}
        return price

    def _on_public_open(self, _ws) -> None:
        self.ws_public_connected = True
        self.ws_public_last_error = ""
        LOG.info("Binance public WebSocket connected: %s symbols", len(self.ws_symbols))

    def _on_market_open(self, _ws) -> None:
        self.ws_market_connected = True
        self.ws_market_last_error = ""
        LOG.info("Binance market WebSocket connected: %s symbols", len(self.ws_symbols))

    def _on_public_close(self, _ws, status_code, message) -> None:
        self.ws_public_connected = False
        if not self.ws_stop.is_set():
            LOG.warning("Binance public WebSocket closed: %s %s", status_code, message)

    def _on_market_close(self, _ws, status_code, message) -> None:
        self.ws_market_connected = False
        if not self.ws_stop.is_set():
            LOG.warning("Binance market WebSocket closed: %s %s", status_code, message)

    def _on_public_error(self, _ws, error) -> None:
        self.ws_public_connected = False
        self.ws_public_last_error = str(error)
        if not self.ws_stop.is_set():
            LOG.warning("Binance public WebSocket error: %s", error)

    def _on_market_error(self, _ws, error) -> None:
        self.ws_market_connected = False
        self.ws_market_last_error = str(error)
        if not self.ws_stop.is_set():
            LOG.warning("Binance market WebSocket error: %s", error)

    def _decode_ws(self, message: str) -> tuple:
        packet = json.loads(message)
        data = packet.get("data", packet)
        return data, time.time()

    def _on_public_message(self, _ws, message: str) -> None:
        try:
            data, now = self._decode_ws(message)
            if data.get("e") == "bookTicker" and data.get("s"):
                symbol = data["s"]
                row = {
                    "bid": float(data["b"]),
                    "ask": float(data["a"]),
                    "bid_qty": float(data.get("B", 0)),
                    "ask_qty": float(data.get("A", 0)),
                    "time": int(data.get("E") or data.get("T") or now * 1000),
                    "received_at": now,
                    "source": "WS-public",
                }
                with self.lock:
                    self.books[symbol] = row
                self.ws_public_last_message_at = now
        except Exception as exc:
            self.ws_public_last_error = f"decode: {exc}"

    def _on_market_message(self, _ws, message: str) -> None:
        try:
            data, now = self._decode_ws(message)
            if data.get("e") == "markPriceUpdate" and data.get("s"):
                symbol = data["s"]
                with self.lock:
                    self.marks[symbol] = {
                        "price": float(data["p"]),
                        "moving_average": float(data["ap"]) if data.get("ap") not in (None, "") else None,
                        "time": int(data.get("E") or now * 1000),
                        "received_at": now,
                        "source": "WS-market",
                    }
                self.ws_market_last_message_at = now
        except Exception as exc:
            self.ws_market_last_error = f"decode: {exc}"

    def _ws_worker(self, category: str, stop: threading.Event, symbols: List[str]) -> None:
        def current_callback(callback):
            def guarded(*args):
                with self.lock:
                    if not stop.is_set() and self.ws_stop is stop:
                        callback(*args)
                    elif callback in (self._on_public_open, self._on_market_open):
                        # close() may have raced with run_forever() starting up.
                        args[0].close()
            return guarded

        backoff = 1.0
        while not stop.is_set():
            if not symbols:
                return
            if category == "public":
                streams = [f"{symbol.lower()}@bookTicker" for symbol in symbols]
                url = self.cfg.ws_public_base + "/".join(streams)
                app = websocket.WebSocketApp(url, on_open=current_callback(self._on_public_open),
                                             on_message=current_callback(self._on_public_message),
                                             on_error=current_callback(self._on_public_error),
                                             on_close=current_callback(self._on_public_close))
            else:
                streams = [f"{symbol.lower()}@markPrice@1s" for symbol in symbols]
                url = self.cfg.ws_market_base + "/".join(streams)
                app = websocket.WebSocketApp(url, on_open=current_callback(self._on_market_open),
                                             on_message=current_callback(self._on_market_message),
                                             on_error=current_callback(self._on_market_error),
                                             on_close=current_callback(self._on_market_close))
            with self.lock:
                if stop.is_set():
                    return
                if category == "public":
                    self.ws_public_app = app
                else:
                    self.ws_market_app = app
            try:
                app.run_forever(ping_interval=120, ping_timeout=30)
            except Exception as exc:
                with self.lock:
                    if not stop.is_set():
                        if category == "public":
                            self.ws_public_last_error = str(exc)
                        else:
                            self.ws_market_last_error = str(exc)
            finally:
                app.close()
            with self.lock:
                if self.ws_stop is stop:
                    if category == "public":
                        self.ws_public_connected = False
                    else:
                        self.ws_market_connected = False
            if stop.wait(backoff):
                break
            backoff = min(30.0, backoff * 1.8)

    def start_streams(self, symbols: Iterable[str]) -> None:
        wanted = sorted(set(symbols))
        both_alive = (self.ws_public_thread and self.ws_public_thread.is_alive() and
                      self.ws_market_thread and self.ws_market_thread.is_alive())
        if wanted == self.ws_symbols and both_alive:
            return
        self.stop_streams()
        self.ws_symbols = wanted
        # Each generation owns its stop event; timed-out old workers stay stopped.
        self.ws_stop = threading.Event()
        if not wanted:
            return
        self.ws_public_thread = threading.Thread(target=self._ws_worker, args=("public", self.ws_stop, wanted), name="binance-public-ws", daemon=True)
        self.ws_market_thread = threading.Thread(target=self._ws_worker, args=("market", self.ws_stop, wanted), name="binance-market-ws", daemon=True)
        self.ws_public_thread.start()
        self.ws_market_thread.start()

    def stop_streams(self) -> None:
        with self.lock:
            self.ws_stop.set()
            apps = (self.ws_public_app, self.ws_market_app)
        for app in apps:
            if app is not None:
                try:
                    app.close()
                except Exception:
                    pass
        for thread in (self.ws_public_thread, self.ws_market_thread):
            if thread and thread.is_alive() and thread is not threading.current_thread():
                thread.join(timeout=3.0)
        self.ws_public_app = None
        self.ws_market_app = None
        self.ws_public_thread = None
        self.ws_market_thread = None
        self.ws_public_connected = False
        self.ws_market_connected = False

    def status(self) -> dict:
        with self.lock:
            return {
                "rest_used_weight_1m": self.last_used_weight,
                "rest_last_ok_at": self.last_http_ok_at,
                "rest_last_error": self.last_http_error,
                "ws_connected": self.ws_public_connected and self.ws_market_connected,
                "ws_public_connected": self.ws_public_connected,
                "ws_market_connected": self.ws_market_connected,
                "ws_symbols": list(self.ws_symbols),
                "ws_public_last_message_at": self.ws_public_last_message_at,
                "ws_market_last_message_at": self.ws_market_last_message_at,
                "ws_public_last_error": self.ws_public_last_error,
                "ws_market_last_error": self.ws_market_last_error,
                "book_cache_size": len(self.books),
                "mark_cache_size": len(self.marks),
            }
