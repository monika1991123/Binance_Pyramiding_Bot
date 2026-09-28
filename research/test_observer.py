"""Run: python3 -B research/test_observer.py. No network or production writes."""
import tempfile
from pathlib import Path
from observer import replay, init_db, curve_stats, cooldown_start


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
# Full exit starts cooldown; a skipped cycle's add/exit must not reset it.
cool = [fill(1, 'ENTRY1', 2, 100), fill(2, 'STOP_EXIT', 1, 110, 'SELL'),
        fill(3, 'STOP_EXIT', 1, 110, 'SELL'),
        fill(4, 'ENTRY1', 2, 100, 'SELL'), fill(5, 'ADD1', 1, 90, 'SELL'),
        fill(6, 'STOP_EXIT', 3, 80),
        fill(7, 'ENTRY1', 1, 100, symbol='OTHER'),
        fill(1802, 'ENTRY1', 1, 100), fill(1802.5, 'STOP_EXIT', 1, 110, 'SELL'),
        fill(1803, 'ENTRY1', 1, 100)]
c = replay(cool, [], 'cooldown_30m', 0, {'TEST': 101, 'OTHER': 101})
assert [f['sourceOid'] for f in c['skippedEntries']] == [4, 1802]
assert [f['sourceOid'] for f in c['ledger']] == [1, 2, 3, 7, 1803]
assert c['cooldownUntil']['TEST'] == 1803000  # Exact boundary accepted, both directions blocked.
assert abs(c['cash'] - (10000 + 20 - .21 - .05 - .05)) < 1e-8
assert replay(cool, [], 'cooldown_30m', 0, {'TEST': 101, 'OTHER': 101}) == c
with tempfile.TemporaryDirectory() as directory:
    db = init_db(Path(directory) / 'test.sqlite3')
    db.executemany('INSERT INTO curves(time,variant,equity) VALUES(?,?,?)', [(1, 'x', 100), (2, 'x', 120), (3, 'x', 90), (4, 'x', 110)])
    assert curve_stats(db, 'x')['sampledMaxDrawdownPct'] == 25
    db.execute('INSERT INTO meta VALUES(?,?)', ('forwardStart', '123'))
    start = cooldown_start(db)
    db.close()
    db = init_db(Path(directory) / 'test.sqlite3')
    assert cooldown_start(db) == start
    assert db.execute("SELECT value FROM meta WHERE key='forwardStart'").fetchone()[0] == '123'
    assert db.execute("SELECT count(*) FROM curves WHERE variant='cooldown_30m'").fetchone()[0] == 1
    db.close()
print('PASS: funding signs, fees, paired sizing, partial exits, cutoff, missing marks, drawdown')
print('PASS: cooldown boundary, partial close, skipped-cycle isolation, other symbols, persistent independent start')
