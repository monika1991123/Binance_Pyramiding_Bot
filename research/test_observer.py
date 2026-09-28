"""Run: python3 -B research/test_observer.py. No network or production writes."""
import tempfile
from pathlib import Path
from observer import replay, init_db, curve_stats


def fill(oid, action, qty, price, side='BUY', symbol='TEST'):
    return dict(oid=oid, time=oid * 1000, action=action, quantity=qty,
                price=price, side=side, symbol=symbol, fee=qty * price * .0005)


fs = [fill(1, 'ENTRY1', 2, 100), fill(2, 'ADD1', 1, 110),
      fill(4, 'STOP_EXIT', 3, 120, 'SELL')]
fund = [dict(symbol='TEST', fundingTime=3000, markPrice='115', fundingRate='.01')]
a = replay(fs, fund, 'pyramiding', 0, {})
b = replay(fs, fund, 'no_add', 0, {})
assert abs(a['cash'] - (10000 + 50 - .335 - 3.45)) < 1e-8
assert abs(b['cash'] - (10000 + 40 - .22 - 2.3)) < 1e-8
assert not a['positions'] and len(a['completed']) == 1
assert replay(fs, fund, 'pyramiding', 1500, {})['cash'] == 10000
short = [fill(1, 'ENTRY1', 2, 100, 'SELL'), fill(4, 'STOP_EXIT', 2, 90)]
assert replay(short, fund, 'long_only', 0, {})['cash'] == 10000
assert abs(replay(short, fund, 'pyramiding', 0, {})['cash'] - (10000 + 20 - .19 + 2.3)) < 1e-8
partial = fs[:2] + [fill(4, 'STOP_EXIT', 2, 120, 'SELL'), fill(5, 'RECONCILE_DUST_EXIT', 1, 120, 'SELL')]
assert abs(replay(partial, fund, 'no_add', 0, {})['cash'] - b['cash']) < 1e-8
assert replay(fs[:1], [], 'pyramiding', 0, {})['equity'] is None
assert replay(fs[:1], [], 'pyramiding', 0, {'TEST': 101})['unrealizedPnl'] == 2
with tempfile.TemporaryDirectory() as directory:
    db = init_db(Path(directory) / 'test.sqlite3')
    db.executemany('INSERT INTO curves(time,variant,equity) VALUES(?,?,?)', [(1, 'x', 100), (2, 'x', 120), (3, 'x', 90), (4, 'x', 110)])
    assert curve_stats(db, 'x')['sampledMaxDrawdownPct'] == 25
    db.close()
print('PASS: funding signs, fees, paired sizing, partial exits, cutoff, missing marks, drawdown')
