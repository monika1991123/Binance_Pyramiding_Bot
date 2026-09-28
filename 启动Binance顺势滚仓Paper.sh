#!/usr/bin/env bash
set -euo pipefail
cd /home/ubuntu/Binance_Pyramiding_Bot

# MCPX 与 ubuntu 可能先后读取同一源码；禁止 Python 在项目目录写 __pycache__，避免跨用户 .pyc 权限冲突。
export PYTHONDONTWRITEBYTECODE=1

echo '=== [1/4] Python 语法检查（只读，不写 __pycache__） ==='
python3 - <<'PY'
from pathlib import Path
for name in ('config.py','storage.py','binance_market.py','paper_broker.py','strategy.py','main.py'):
    source = Path(name).read_text(encoding='utf-8')
    compile(source, name, 'exec')
    print('syntax_ok', name)
PY

echo '=== [2/4] Binance 公共接口只读自检 ==='
python3 - <<'PY'
from binance_market import BinanceFuturesMarket
m=BinanceFuturesMarket()
rules=m.refresh_exchange_info(force=True)
assert 'BTCUSDT' in rules
bars=m.klines('BTCUSDT','5m',3)
assert len(bars)>=2
book=m.book('BTCUSDT',max_age=0)
assert book['ask']>=book['bid']>0
print('exchangeInfo_symbols=',len(rules),'BTCUSDT_book=',book['bid'],book['ask'])
PY

echo '=== [3/4] 安装/更新 user systemd unit ==='
systemctl --user link /home/ubuntu/Binance_Pyramiding_Bot/binance-pyramiding-paper.service >/dev/null 2>&1 || true
systemctl --user daemon-reload
systemctl --user enable binance-pyramiding-paper.service >/dev/null
# 发布/修复后必须明确重启：enable --now 对已经 active 的服务不会保证重新加载新代码。
systemctl --user restart binance-pyramiding-paper.service

echo '=== [4/4] 等待健康检查 ==='
for i in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:3012/health >/tmp/binance-pyramiding-health.json 2>/dev/null; then
    cat /tmp/binance-pyramiding-health.json
    echo
    echo '✅ Binance 顺势滚仓 V2 · 10000U Paper 已启动'
    exit 0
  fi
  sleep 1
done
systemctl --user --no-pager --full status binance-pyramiding-paper.service || true
exit 1
