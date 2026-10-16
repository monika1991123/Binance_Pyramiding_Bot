# Binance 顺势滚仓 V2 · 10000U Paper

独立于 `/home/ubuntu/Hyperliquid_Bot` 的 Binance USDⓈ-M Futures Paper 版本。不会读取 Binance API Key，不会向 Binance 提交真实订单。

## 策略口径

- 交易所：Binance USDⓈ-M USDT 永续，状态 `TRADING`。
- 动态标的池：24h `quoteVolume` Top30，价差 ≤ 0.40%，再按 ATR%、24h range%、成交量加速、成交额分位数加权，选 Top12。
- 入场：最新已收盘 5m K，Donchian12 突破，当前成交量 ≥ 前20根均量 1.40×。
- 趋势：最新已收盘 1h K，LONG 为 `price > EMA20 > EMA50` 且 EMA20 上升；SHORT 反向。
- ATR14；初始止损 2.00 ATR。
- 首仓风险目标 ≤ 500U 且 ≤ 权益 5%。
- 组合开放风险 ≤ `min(6000U, equity*60%)`。
- 最多 12 个 Campaign，同方向最多 8 个，名义杠杆预算 5×。
- Variant C：+0.75R 先抬止损到首仓附近，再加 0.75× Base；+1.50R 再先抬止损到约 +1R，再加 0.50× Base。
- 第二次加仓后进入 2.20 ATR trailing；止损只允许向盈利方向收紧。
- **亏损方向永不加仓。**
- 日内权益损失 ≥ 5%：停止新开仓；Peak Drawdown Kill 阈值仍为 15%，但 Paper 前向阶段当前通过 `drawdown_kill_enabled=False` 暂时关闭，可随时恢复。
- 重启会对 Paper position / Campaign / protective stop 做一致性检查；不一致时禁止新开仓。

## 限流设计

- REST 只用于 exchangeInfo、15分钟 Universe 刷新、5m/1h 已收盘 K 线补取和 WebSocket 异常时的盘口/mark fallback。
- Top12 的 best bid/ask 与 mark price 常态使用 Binance Futures WebSocket。
- 读取 `X-MBX-USED-WEIGHT-1M`；达到内部软阈值时主动减速。
- HTTP 429/418 尊重 `Retry-After`，否则指数退避，不继续冲击接口。

## 独立文件

正式 user-systemd 使用 `/home/ubuntu/Binance_Pyramiding_Bot/runtime-data/`：

- `runtime-data/binance-pyramiding-paper-state.json`：10000U Paper 现金、仓位、fills、保护止损记录。
- `runtime-data/binance-pyramiding-state.json`：Campaign、风险、Universe、扫描游标、事件。
- `runtime-data/runtime-status.json`：前端与健康检查使用的只读汇总快照。

MCPX 开发验收默认使用项目内 `data/`，与正式账本隔离。正式 `runtime-data/` 由 ubuntu 的 user-systemd 首次运行时创建。全部状态使用同目录临时文件 + 原子替换，权限 0600，不通过放宽权限共享。

## 启动

```bash
bash /home/ubuntu/Binance_Pyramiding_Bot/启动Binance顺势滚仓Paper.sh
```

健康检查：`http://127.0.0.1:3012/health`
