#!/usr/bin/env python3
"""Read-only source observer; all writes stay in research/output. No order API."""
import argparse
import bisect
import datetime as dt
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
OUT = Path(__file__).resolve().parent / 'output'
SOURCE = ROOT / 'runtime-data'
VARIANTS = ('pyramiding', 'long_only', 'no_add', 'cooldown_30m')
CODE = ('config.py', 'strategy.py', 'paper_broker.py', 'main.py',
        'binance_market.py', 'storage.py', 'binance-pyramiding-paper.service')


def atomic(path, value):
    tmp = path.with_suffix(path.suffix + '.tmp')
    tmp.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding='utf-8')
    os.replace(tmp, path)


def stamp(ms):
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone(dt.timedelta(hours=8))).isoformat()


def public_get(path, params):
    # Fixed public host and GET-only allowlist: cannot send orders or credentials.
    assert path in ('fundingRate', 'klines')
    url = 'https://fapi.binance.com/fapi/v1/' + path + '?' + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            result = json.load(response)
    except urllib.error.HTTPError as exc:
        if exc.code in (418, 429):
            retry = max(900, int(float(exc.headers.get('Retry-After', '900'))))
            raise RuntimeError(f'RATE_LIMIT:{retry}') from exc
        raise
    if not isinstance(result, list):
        raise ValueError('Expected Binance public data array')
    time.sleep(0.15)
    return result


def replay(fills, funding, variant, cutoff, marks):
    cash = 10000.0
    positions, ledger, completed = {}, [], []
    cooldown_until, skipped = {}, []
    fees = funding_net = gross = 0.0
    events = [(f['time'], 1, f) for f in fills if f['time'] >= cutoff]
    events += [(f['fundingTime'], 0, f) for f in funding if f['fundingTime'] >= cutoff]
    for when, kind, f in sorted(events, key=lambda e: (e[0], e[1], e[2].get('oid', 0))):
        symbol = f['symbol']
        pos = positions.get(symbol)
        if kind == 0:
            if not pos:
                continue
            delta = -pos['sign'] * pos['quantity'] * float(f['markPrice']) * float(f['fundingRate'])
            cash += delta
            funding_net += delta
            pos['net'] += delta
            ledger.append({'time': when, 'symbol': symbol, 'action': 'FUNDING', 'net': delta,
                           'rate': f['fundingRate'], 'markPrice': f['markPrice']})
            continue
        action = f['action']
        source_qty, price = float(f['quantity']), float(f['price'])
        if not all(math.isfinite(x) and x > 0 for x in (source_qty, price)):
            raise ValueError('Invalid source fill')
        if action == 'ENTRY1':
            if variant == 'cooldown_30m' and when < cooldown_until.get(symbol, 0):
                skipped.append({'time': when, 'symbol': symbol, 'sourceOid': f['oid'],
                                'reason': 'same_symbol_cooldown_30m',
                                'cooldownUntil': cooldown_until[symbol]})
                continue
            if variant == 'long_only' and f['side'] != 'BUY':
                continue
            if pos:
                raise ValueError('Duplicate source entry')
            pos = {'symbol': symbol, 'sign': 1 if f['side'] == 'BUY' else -1,
                   'quantity': 0.0, 'sourceQuantity': 0.0, 'entryPrice': 0.0,
                   'openedAt': when, 'net': 0.0, 'adds': 0}
            positions[symbol] = pos
        if not pos:
            continue  # Pre-observation positions and deliberately excluded shorts.
        closed_pnl = 0.0
        if action in ('ENTRY1', 'ADD1', 'ADD2'):
            pos['sourceQuantity'] += source_qty
            if variant == 'no_add' and action != 'ENTRY1':
                continue
            qty = source_qty
            total = pos['quantity'] + qty
            pos['entryPrice'] = (pos['entryPrice'] * pos['quantity'] + price * qty) / total
            pos['quantity'] = total
            pos['adds'] += action != 'ENTRY1'
        else:
            if source_qty > pos['sourceQuantity'] + 1e-7:
                raise ValueError('Exit exceeds source position')
            qty = pos['quantity'] * min(1.0, source_qty / pos['sourceQuantity'])
            closed_pnl = pos['sign'] * (price - pos['entryPrice']) * qty
            pos['quantity'] -= qty
            pos['sourceQuantity'] -= source_qty
        fee = float(f['fee']) * qty / source_qty
        delta = closed_pnl - fee
        gross += closed_pnl
        fees += fee
        cash += delta
        pos['net'] += delta
        ledger.append({'time': when, 'sourceOid': f['oid'], 'symbol': symbol, 'action': action,
                       'quantity': qty, 'price': price, 'fee': fee, 'closedPnl': closed_pnl, 'net': delta})
        if pos['sourceQuantity'] < 1e-8:
            completed.append({**pos, 'closedAt': when})
            if variant == 'cooldown_30m':
                cooldown_until[symbol] = when + 30 * 60000
            del positions[symbol]
    unreal = 0.0
    missing = []
    for symbol, pos in positions.items():
        mark = marks.get(symbol)
        if mark is None or not math.isfinite(mark) or mark <= 0:
            missing.append(symbol)
            pos['unrealizedPnl'] = None
        else:
            pos['markPrice'] = mark
            pos['unrealizedPnl'] = pos['sign'] * (mark - pos['entryPrice']) * pos['quantity']
            unreal += pos['unrealizedPnl']
    wins = [p['net'] for p in completed if p['net'] > 0]
    losses = [p['net'] for p in completed if p['net'] < 0]
    streak = worst = 0
    for p in completed:
        streak = streak + 1 if p['net'] < 0 else 0
        worst = max(worst, streak)
    return {'startingBalance': 10000, 'cash': cash, 'equity': None if missing else cash + unreal,
            'unrealizedPnl': None if missing else unreal, 'realizedGrossPnl': gross,
            'fees': fees, 'fundingNet': funding_net, 'realizedNetPnl': cash - 10000,
            'positions': positions, 'ledger': ledger, 'completed': completed,
            'skippedEntries': skipped, 'cooldownUntil': cooldown_until,
            'missingMarks': missing, 'closedCount': len(completed),
            'winRatePct': 100 * len(wins) / len(completed) if completed else None,
            'profitFactor': sum(wins) / -sum(losses) if losses else None,
            'expectancy': sum(p['net'] for p in completed) / len(completed) if completed else None,
            'maxConsecutiveLosses': worst}


def init_db(path):
    db = sqlite3.connect(path)
    db.executescript('''
    CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS fills(oid INTEGER PRIMARY KEY, payload TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS snapshots(time INTEGER PRIMARY KEY, payload TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS curves(time INTEGER, variant TEXT, equity REAL,
      PRIMARY KEY(time, variant));
    ''')
    if 'funding_net' not in {row[1] for row in db.execute('PRAGMA table_info(curves)')}:
        db.execute('ALTER TABLE curves ADD COLUMN funding_net REAL NOT NULL DEFAULT 0')
    return db


def collect(db):
    # Atomic source files are read twice across the snapshot boundary; reject a torn pair.
    for _ in range(4):
        paper = json.loads((SOURCE / 'binance-pyramiding-paper-state.json').read_text())
        runtime = json.loads((SOURCE / 'runtime-status.json').read_text())
        if abs(paper['cash'] - runtime['account']['cash']) < 1e-7 and {
            s: (p['quantity'], p['entryPrice']) for s, p in paper['positions'].items()
        } == {p['symbol']: (p['quantity'], p['entryPrice']) for p in runtime['positions']}:
            break
        time.sleep(0.3)
    else:
        raise RuntimeError('Source snapshot changed while reading; retry next cycle')
    if time.time() * 1000 - runtime['generatedAt'] > 120000:
        raise RuntimeError('Source runtime stale; refusing fabricated fresh equity')
    for f in paper['fills']:
        payload = json.dumps(f, sort_keys=True)
        old = db.execute('SELECT payload FROM fills WHERE oid=?', (f['oid'],)).fetchone()
        if old and old[0] != payload:
            raise RuntimeError('Source fill changed; manual audit needed')
        db.execute('INSERT OR IGNORE INTO fills VALUES(?,?)', (f['oid'], payload))
    sample = {k: runtime[k] for k in ('generatedAt', 'account', 'positions', 'risk', 'market')}
    db.execute('INSERT OR IGNORE INTO snapshots VALUES(?,?)',
               (runtime['generatedAt'], json.dumps(sample)))
    db.commit()
    fills = [json.loads(row[0]) for row in db.execute('SELECT payload FROM fills ORDER BY oid')]
    if abs(10000 + sum(f['net'] for f in fills) - paper['cash']) > 1e-6:
        raise RuntimeError('Source ledger incomplete or cash adjusted; stop comparison')
    return paper, runtime, fills


def funding_cache(fills, until):
    path = OUT / 'funding.json'
    cache = json.loads(path.read_text()) if path.exists() else {}
    first = {}
    for f in fills:
        first[f['symbol']] = min(first.get(f['symbol'], f['time']), f['time'])
    for symbol, start in sorted(first.items()):
        item = cache.setdefault(symbol, {'through': start - 1, 'rows': []})
        if until - item['through'] < 900000:
            continue
        # Overlap catches publication delay; deduplicate by settlement timestamp.
        cursor = max(start, item['through'] - 3600000)
        rows = {r['fundingTime']: r for r in item['rows']}
        while cursor <= until:
            batch = public_get('fundingRate', {'symbol': symbol, 'startTime': cursor,
                                               'endTime': until, 'limit': 1000})
            for row in batch:
                price = float(row.get('markPrice') or 0)
                rate = float(row['fundingRate'])
                if (not math.isfinite(price) or price <= 0 or not math.isfinite(rate)
                        or row['symbol'] != symbol or not cursor <= row['fundingTime'] <= until):
                    raise ValueError(f'{symbol}: invalid settlement data')
                rows[row['fundingTime']] = row
            if len(batch) < 1000:
                break
            next_cursor = max(r['fundingTime'] for r in batch) + 1
            if next_cursor <= cursor:
                raise ValueError('Funding pagination failed to advance')
            cursor = next_cursor
        item.update(through=until, rows=sorted(rows.values(), key=lambda r: r['fundingTime']))
        atomic(path, cache)
    return [r for item in cache.values() for r in item['rows']], min(x['through'] for x in cache.values())


def verify_winner(account):
    if not account['completed']:
        return {}
    winner = max(account['completed'], key=lambda p: p['net'])
    path = OUT / 'winner-verification.json'
    if path.exists():
        old = json.loads(path.read_text())
        if old.get('openedAt') == winner['openedAt'] and old.get('symbol') == winner['symbol']:
            return old
    rows = []
    for f in account['ledger']:
        if f['symbol'] != winner['symbol'] or not winner['openedAt'] <= f['time'] <= winner['closedAt'] or f['action'] == 'FUNDING':
            continue
        minute = f['time'] // 60000 * 60000
        candles = public_get('klines', {'symbol': f['symbol'], 'interval': '1m',
                                      'startTime': minute, 'endTime': minute + 59999, 'limit': 1})
        candle = next((r for r in candles if r[0] == minute), None)
        rows.append({'fill': f, 'candle': candle,
                     'insideTradeRange': bool(candle and float(candle[3]) <= f['price'] <= float(candle[2]))})
    result = {'symbol': winner['symbol'], 'openedAt': winner['openedAt'], 'closedAt': winner['closedAt'],
              'rows': rows, 'limitation': '1m trade ranges cannot prove exact quote, depth or fillability; simulated spread/slippage may put a fill outside range.'}
    atomic(path, result)
    return result


def curve_stats(db, variant):
    rows = db.execute('SELECT time,equity FROM curves WHERE variant=? ORDER BY time', (variant,)).fetchall()
    peak = None
    worst = 0.0
    for _, equity in rows:
        peak = equity if peak is None else max(peak, equity)
        worst = max(worst, 100 * (peak - equity) / peak) if peak > 0 else worst
    return {'samples': len(rows), 'firstAt': rows[0][0] if rows else None,
            'sampledMaxDrawdownPct': worst, 'peakEquity': peak}


def cooldown_start(db):
    key = 'forwardStart:cooldown_30m'
    row = db.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
    if row:
        return int(row[0])
    start = int(time.time() * 1000)
    db.execute('INSERT INTO meta VALUES(?,?)', (key, str(start)))
    db.execute('INSERT INTO curves(time,variant,equity) VALUES(?,?,?)',
               (start, 'cooldown_30m', 10000.0))
    db.commit()
    return start


def run_once(db):
    paper, runtime, fills = collect(db)
    until = runtime['generatedAt']
    row = db.execute("SELECT value FROM meta WHERE key='forwardStart'").fetchone()
    if row is None:
        cutoff = int(time.time() * 1000)
        db.execute('INSERT INTO meta VALUES(?,?)', ('forwardStart', str(cutoff)))
        for variant in VARIANTS[:-1]:
            db.execute('INSERT INTO curves(time,variant,equity) VALUES(?,?,?)', (cutoff, variant, 10000.0))
        db.commit()
    else:
        cutoff = int(row[0])
    starts = {v: cutoff for v in VARIANTS}
    starts['cooldown_30m'] = cooldown_start(db)
    marks = {p['symbol']: p['markPrice'] for p in runtime['positions']}
    # Source mark fallback is not independently detectable; retain this limitation in report.
    errors = []
    cooldown = OUT / 'api-cooldown.json'
    allowed = not cooldown.exists() or json.loads(cooldown.read_text())['until'] <= time.time()
    funding, through = [], None
    try:
        if not allowed:
            raise RuntimeError('Public API cooldown active')
        funding, through = funding_cache(fills, until)
    except Exception as exc:
        errors.append(str(exc))
        if str(exc).startswith('RATE_LIMIT:'):
            atomic(cooldown, {'until': time.time() + int(str(exc).split(':')[1])})
        elif allowed:
            atomic(cooldown, {'until': time.time() + 900})
        path = OUT / 'funding.json'
        if path.exists():
            cache = json.loads(path.read_text())
            funding = [r for v in cache.values() for r in v['rows']]
    reports = {}
    for period, start in [('historical_paired', 0), ('forward_paired', cutoff)]:
        reports[period] = {}
        for v in VARIANTS:
            effective_start = starts[v] if period == 'forward_paired' else start
            reports[period][v] = {**replay(fills, funding, v, effective_start, marks),
                                  'startedAt': effective_start}
    for v, account in reports['forward_paired'].items():
        if account['equity'] is not None and not errors and until >= starts[v]:
            db.execute('INSERT OR REPLACE INTO curves VALUES(?,?,?,?)',
                       (until, v, account['equity'], account['fundingNet']))
        # Reapply late-published funding at its settlement time, not its fetch time.
        settlements = sorted((f['time'], f['net']) for f in account['ledger'] if f['action'] == 'FUNDING')
        times, totals = [], [0.0]
        for when, net in settlements:
            times.append(when)
            totals.append(totals[-1] + net)
        for when, equity, old_net in db.execute('SELECT time,equity,funding_net FROM curves WHERE variant=?', (v,)).fetchall():
            new_net = totals[bisect.bisect_right(times, when)]
            if new_net != old_net:
                db.execute('UPDATE curves SET equity=?,funding_net=? WHERE time=? AND variant=?',
                           (equity - old_net + new_net, new_net, when, v))
    db.execute('INSERT OR IGNORE INTO curves(time,variant,equity) VALUES(?,?,?)', (until, 'source_unadjusted', runtime['account']['equity']))
    db.commit()
    verification = {}
    try:
        if allowed and not errors:
            verification = verify_winner(reports['historical_paired']['pyramiding'])
    except Exception as exc:
        errors.append('winner verification: ' + str(exc))
        if str(exc).startswith('RATE_LIMIT:'):
            atomic(cooldown, {'until': time.time() + int(str(exc).split(':')[1])})
    result = {'generatedAt': until, 'timeUTC8': stamp(until), 'forwardStartUTC8': stamp(cutoff),
              'forwardStartsUTC8': {v: stamp(t) for v, t in starts.items()},
              'sourceAccount': runtime['account'], 'sourceFills': len(fills),
              'sourceLedgerReconciled': True, 'fundingCheckedThrough': through,
              'errors': errors, 'winnerVerification': verification,
              'curves': {v: curve_stats(db, v) for v in (*VARIANTS, 'source_unadjusted')},
              'comparisons': reports,
              'limitations': ['Paper only; shared executed-entry/exit cohort, not all raw signals.',
                'Cooldown group starts separately; compare matching periods, not lifetime totals across different start dates.',
                'No-add uses same base quantity and source exits; no independent trailing or rescaling.',
                'Historical paired results are in-sample counterfactuals, not forward results.',
                'No historical equity recovery; observed sampled drawdown starts at observer launch.',
                'Source mark fallback to entry cannot be detected; raw snapshots preserved for audit.',
                'Funding uses published settlement mark/rate, with at most 15m polling lag; no depth impact or liquidation model.',
                'Forward adjusted samples are provisional until funding arrives, then corrected at settlement time; source unadjusted samples are unaffected.',
                'Source fills at exact funding timestamp are ordered after settlement; exchange boundary is uncertain.',
                'Comparisons do not independently enforce margin or source risk gates; they isolate trade attribution.']}
    atomic(OUT / 'report.json', result)
    lines = ['# Binance 原版只读审计与配对对照', '', f'快照：{stamp(until)}；前向起点：{stamp(cutoff)}', '',
             f'cooldown_30m 独立起点：{stamp(starts["cooldown_30m"])}；同币本组整仓退出后冷却30分钟，跳过的交易不延长冷却。', '',
             f'原版逐笔对账：通过；累计 {len(fills)} 笔。资金费查询截至：{stamp(through) if through else "不完整"}。', '']
    for period, accounts in reports.items():
        lines += [f'## {period}', '', '|组别|初始资金|权益|已实现净额含资金费|浮盈亏|手续费|资金费净额|结束周期|',
                  '|---|---:|---:|---:|---:|---:|---:|---:|']
        for name, a in accounts.items():
            num = lambda x: '缺失' if x is None else f'{x:.2f}'
            lines.append(f'|{name}|10000|{num(a["equity"])}|{a["realizedNetPnl"]:.2f}|{num(a["unrealizedPnl"])}|{a["fees"]:.2f}|{a["fundingNet"]:.2f}|{a["closedCount"]}|')
        lines.append('')
    lines += [f'冷却组前向已跳过开仓：{len(reports["forward_paired"]["cooldown_30m"]["skippedEntries"])} 笔。明细见 report.json。', '']
    lines += ['## 启用以来采样权益最大回撤', '', '|组别|样本数|最大回撤|', '|---|---:|---:|']
    for name, c in result['curves'].items():
        lines.append(f'|{name}|{c["samples"]}|{c["sampledMaxDrawdownPct"]:.4f}%|')
    lines += ['', '前向无成交时：等待观察器启用后的原版新开仓；既有仓位不迁入。', '']
    lines += ['## 边界与未验证项', '', *['- ' + s for s in result['limitations']], '',
              '错误：' + (json.dumps(errors, ensure_ascii=False) if errors else '无'),
              '', '详细逐笔账本、持仓、统计与行情证据见同目录 report.json、winner-verification.json；原始快照与权益采样见 observer.sqlite3。']
    tmp = OUT / 'report.md.tmp'
    tmp.write_text('\n'.join(lines) + '\n', encoding='utf-8')
    os.replace(tmp, OUT / 'report.md')
    atomic(OUT / 'health.json', {'ok': not errors, 'observedAt': int(time.time() * 1000),
                                'sourceAt': until, 'errors': errors})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--watch', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    OUT.mkdir(parents=True, exist_ok=True)
    with (OUT / 'observer.lock').open('w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        hashes = {name: hashlib.sha256((ROOT / name).read_bytes()).hexdigest() for name in CODE}
        if not (OUT / 'source-hashes.json').exists():
            atomic(OUT / 'source-hashes.json', hashes)
        db = init_db(OUT / 'observer.sqlite3')
        while True:
            try:
                run_once(db)
            except Exception as exc:
                db.rollback()
                atomic(OUT / 'health.json', {'ok': False, 'observedAt': int(time.time() * 1000), 'error': str(exc)})
                if not args.watch:
                    raise
            if not args.watch:
                break
            time.sleep(15)


if __name__ == '__main__':
    main()
