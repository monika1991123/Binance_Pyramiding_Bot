#!/usr/bin/env python3
import json
import logging
import signal
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from binance_market import BinanceFuturesMarket
from config import SETTINGS
from paper_broker import PaperBroker
from strategy import PyramidingEngine
from storage import read_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
LOG = logging.getLogger("binance.main")
STOP = threading.Event()
STARTED_AT = time.time()


def start_health_server(engine: PyramidingEngine):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, _fmt, *_args):
            return

        def do_GET(self):
            if self.path not in ("/health", "/state"):
                self.send_response(404)
                self.end_headers()
                return
            runtime = read_json(SETTINGS.runtime_state_file, {})
            if self.path == "/health":
                now = time.time()
                payload = {
                    "ok": bool(runtime) and not bool(engine.last_error),
                    "version": "V2",
                    "exchange": "BINANCE_USDM",
                    "strategyMode": "pyramiding",
                    "tradingMode": "paper",
                    "uptimeSeconds": int(now - STARTED_AT),
                    "lastCycleAt": runtime.get("lastCycleAt", 0),
                    "lastScanAt": runtime.get("lastScanAt", 0),
                    "activeUniverse": runtime.get("universe", {}).get("active", []),
                    "positions": len(runtime.get("positions", [])),
                    "blockedReason": runtime.get("blockedReason"),
                    "market": runtime.get("market", {}),
                    "lastError": engine.last_error,
                }
            else:
                payload = runtime
            raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

    server = ThreadingHTTPServer((SETTINGS.health_host, SETTINGS.health_port), Handler)
    thread = threading.Thread(target=server.serve_forever, name="health-http", daemon=True)
    thread.start()
    return server


def main() -> int:
    market = BinanceFuturesMarket()
    broker = PaperBroker(market)
    engine = PyramidingEngine(market, broker)

    def stop_handler(signum, _frame):
        LOG.info("signal %s received, shutting down", signum)
        STOP.set()

    signal.signal(signal.SIGTERM, stop_handler)
    signal.signal(signal.SIGINT, stop_handler)
    engine.initialize()
    health = start_health_server(engine)
    LOG.info("Binance Pyramiding V2 Paper running; health=http://%s:%s/health", SETTINGS.health_host, SETTINGS.health_port)
    try:
        while not STOP.wait(SETTINGS.manage_interval_seconds):
            engine.cycle()
    finally:
        try:
            engine.write_runtime()
        except Exception:
            LOG.exception("final runtime snapshot failed")
        health.shutdown()
        health.server_close()
        market.stop_streams()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
