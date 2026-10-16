import json
import threading
import time
import unittest
from unittest.mock import patch

from binance_market import BinanceFuturesMarket


class FakeApp:
    instances = []

    def __init__(self, url, **callbacks):
        self.url = url
        self.callbacks = callbacks
        self.started = threading.Event()
        self.closed = threading.Event()
        self.release = threading.Event()
        self.delay_close = False
        self.instances.append(self)

    def run_forever(self, **kwargs):
        self.callbacks['on_open'](self)
        self.started.set()
        self.closed.wait(10)
        if self.delay_close:
            self.release.wait(10)
        self.callbacks['on_close'](self, None, None)

    def close(self):
        self.closed.set()


class StreamLifecycleTests(unittest.TestCase):
    def setUp(self):
        FakeApp.instances = []
        self.market = BinanceFuturesMarket()
        self.patch = patch('binance_market.websocket.WebSocketApp', FakeApp)
        self.patch.start()

    def tearDown(self):
        for app in FakeApp.instances:
            app.release.set()
        self.market.stop_streams()
        self.market.http.close()
        self.patch.stop()

    def wait_started(self, count):
        deadline = time.monotonic() + 3
        while len(FakeApp.instances) < count and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(len(FakeApp.instances), count)
        for app in FakeApp.instances:
            self.assertTrue(app.started.wait(3))

    def test_repeated_switches_and_same_symbols_do_not_leak(self):
        old_threads = []
        for symbol in ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'BTCUSDT']:
            count = len(FakeApp.instances) + 2
            self.market.start_streams([symbol, symbol])
            self.wait_started(count)
            self.assertTrue(all(not t.is_alive() for t in old_threads))
            old_threads += [self.market.ws_public_thread, self.market.ws_market_thread]
            self.market.start_streams([symbol])
            self.assertEqual(len(FakeApp.instances), count)
        self.market.start_streams([])
        self.assertTrue(all(not t.is_alive() for t in old_threads))
        self.assertTrue(self.market.ws_stop.is_set() or not self.market.ws_symbols)

    def test_delayed_old_workers_cannot_reconnect_or_overwrite_new_data(self):
        self.market.start_streams(['BTCUSDT'])
        self.wait_started(2)
        old_apps = list(FakeApp.instances)
        old_threads = [self.market.ws_public_thread, self.market.ws_market_thread]
        old_stop = self.market.ws_stop
        for app in old_apps:
            app.delay_close = True
        # Simulate join timeout without spending six seconds in every test.
        with patch.object(old_threads[0], 'join'), patch.object(old_threads[1], 'join'):
            self.market.start_streams(['ETHUSDT'])
        self.wait_started(4)
        self.assertTrue(old_stop.is_set())
        self.assertIsNot(old_stop, self.market.ws_stop)
        messages = [
            {'e': 'bookTicker', 's': 'ETHUSDT', 'b': '100', 'a': '101'},
            {'e': 'markPriceUpdate', 's': 'ETHUSDT', 'p': '100.5'},
        ]
        for app in FakeApp.instances[2:]:
            msg = messages[0 if '@bookTicker' in app.url else 1]
            app.callbacks['on_message'](app, json.dumps(msg))
        for app in old_apps:
            msg = {'e': 'bookTicker', 's': 'ETHUSDT', 'b': '1', 'a': '2'}
            app.callbacks['on_message'](app, json.dumps(msg))
            app.callbacks['on_error'](app, RuntimeError('old connection'))
            app.callbacks['on_open'](app)
            app.release.set()
        for thread in old_threads:
            thread.join(3)
            self.assertFalse(thread.is_alive())
        self.assertEqual(len(FakeApp.instances), 4)
        self.assertEqual(self.market.book('ETHUSDT')['bid'], 100)
        self.assertEqual(self.market.mark_price('ETHUSDT'), 100.5)
        status = self.market.status()
        self.assertTrue(status['ws_connected'])
        self.assertEqual(status['ws_public_last_error'], '')
        self.assertEqual(status['ws_market_last_error'], '')


if __name__ == '__main__':
    unittest.main()
