import logging
import math
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from binance_market import BinanceFuturesMarket
from config import SETTINGS, Settings
from paper_broker import PaperBroker
from storage import JsonState, atomic_json_write

LOG = logging.getLogger("binance.strategy")


def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def ema(values: List[float], period: int) -> float:
    if len(values) < period:
        raise ValueError("not enough values for EMA")
    alpha = 2.0 / (period + 1.0)
    value = sum(values[:period]) / period
    for item in values[period:]:
        value = alpha * item + (1.0 - alpha) * value
    return value


def atr(bars: List[dict], period: int) -> float:
    if len(bars) < period + 1:
        raise ValueError("not enough bars for ATR")
    trs = []
    for index in range(1, len(bars)):
        prev_close = bars[index - 1]["close"]
        row = bars[index]
        trs.append(max(row["high"] - row["low"], abs(row["high"] - prev_close), abs(row["low"] - prev_close)))
    window = trs[-period:]
    return sum(window) / len(window)


def percentile_map(rows: List[dict], key: str) -> Dict[str, float]:
    ordered = sorted(rows, key=lambda row: (row[key], row["symbol"]))
    result: Dict[str, float] = {}
    total = len(ordered)
    if total == 1:
        result[ordered[0]["symbol"]] = 1.0
        return result
    i = 0
    while i < total:
        j = i
        while j + 1 < total and ordered[j + 1][key] == ordered[i][key]:
            j += 1
        rank = ((i + j) / 2.0) / max(1, total - 1)
        for k in range(i, j + 1):
            result[ordered[k]["symbol"]] = rank
        i = j + 1
    return result


def _default_strategy_state(cfg: Settings) -> dict:
    now = int(time.time() * 1000)
    return {
        "version": 2,
        "exchange": "BINANCE_USDM",
        "mode": "paper",
        "campaigns": {},
        "risk": {
            "peakEquity": cfg.start_balance,
            "dailyKey": utc_day(),
            "dailyStartBalance": cfg.start_balance,
            "drawdownKillActive": False,
        },
        "blockedReason": None,
        "lastEvaluated": {},
        "universe": {"topVolume": [], "active": [], "scores": [], "updatedAt": 0},
        "events": [],
        "createdAt": now,
        "updatedAt": now,
    }


class PyramidingEngine:
    def __init__(self, market: BinanceFuturesMarket, broker: PaperBroker, settings: Settings = SETTINGS):
        self.market = market
        self.broker = broker
        self.cfg = settings
        self.state = JsonState(settings.strategy_state_file, lambda: _default_strategy_state(settings))
        snapshot = self.state.snapshot()
        self.active_symbols = list(snapshot.get("universe", {}).get("active", []))
        self.bars5: Dict[str, List[dict]] = {}
        self.bars1: Dict[str, List[dict]] = {}
        self.last_universe_refresh = float(snapshot.get("universe", {}).get("updatedAt", 0)) / 1000.0
        self.last_scan_at = 0.0
        self.last_scan_candle = 0
        self.last_cycle_at = 0.0
        self.last_reconcile_at = 0.0
        self.last_error = ""

    def _event(self, kind: str, message: str, **fields) -> None:
        event = {"time": int(time.time() * 1000), "kind": kind, "message": message, **fields}
        LOG.info("%s: %s %s", kind, message, fields if fields else "")
        def mutate(data: dict):
            data.setdefault("events", []).append(event)
            data["events"] = data["events"][-200:]
            data["updatedAt"] = int(time.time() * 1000)
        self.state.mutate(mutate)

    def _campaigns(self) -> dict:
        return self.state.snapshot().get("campaigns", {})

    def _save_campaign(self, symbol: str, campaign: dict) -> None:
        campaign["updatedAt"] = int(time.time() * 1000)
        def mutate(data: dict):
            data.setdefault("campaigns", {})[symbol] = campaign
            data["updatedAt"] = int(time.time() * 1000)
        self.state.mutate(mutate)

    def _remove_campaign(self, symbol: str) -> None:
        def mutate(data: dict):
            data.setdefault("campaigns", {}).pop(symbol, None)
            data["updatedAt"] = int(time.time() * 1000)
        self.state.mutate(mutate)

    def _set_blocked(self, reason: Optional[str]) -> None:
        def mutate(data: dict):
            data["blockedReason"] = reason
            data["updatedAt"] = int(time.time() * 1000)
        self.state.mutate(mutate)

    @staticmethod
    def _closed(bars: List[dict]) -> List[dict]:
        return [row for row in bars if row.get("closed")]

    def _universe_features(self, symbol: str, day_volume: float, spread_pct: float) -> Optional[dict]:
        bars = self._closed(self.market.klines(symbol, "5m", 288))
        if len(bars) < 30:
            return None
        try:
            current_atr = atr(bars, self.cfg.atr_period)
        except ValueError:
            return None
        close = bars[-1]["close"]
        if close <= 0:
            return None
        range_pct = (max(x["high"] for x in bars) - min(x["low"] for x in bars)) / bars[0]["open"] * 100.0
        recent = sum(x["volume"] for x in bars[-12:])
        prior = sum(x["volume"] for x in bars[-24:-12])
        accel = recent / prior if prior > 0 else 0.0
        self.bars5[symbol] = bars
        return {
            "symbol": symbol,
            "dayVolume": day_volume,
            "atrPct": current_atr / close * 100.0,
            "rangePct": range_pct,
            "volumeAcceleration": accel,
            "spreadPct": spread_pct,
            "score": 0.0,
        }

    def refresh_universe(self, force: bool = False) -> List[str]:
        if not force and self.active_symbols and time.time() - self.last_universe_refresh < self.cfg.universe_refresh_seconds:
            return self.active_symbols
        tradable = set(self.market.tradable_symbols())
        tickers = self.market.all_24h_tickers()
        books = self.market.all_book_tickers()
        volume_rows = []
        for row in tickers:
            symbol = row.get("symbol")
            if symbol not in tradable:
                continue
            try:
                day_volume = float(row.get("quoteVolume", 0))
                book = books.get(symbol)
                if not book or day_volume <= 0:
                    continue
                mid = (book["bid"] + book["ask"]) / 2.0
                if mid <= 0:
                    continue
                spread_pct = (book["ask"] - book["bid"]) / mid * 100.0
                if spread_pct < 0 or spread_pct > self.cfg.max_spread_pct:
                    continue
                volume_rows.append((day_volume, symbol, spread_pct))
            except (TypeError, ValueError, ZeroDivisionError):
                continue
        volume_rows.sort(reverse=True)
        top_volume = volume_rows[: self.cfg.volume_universe_size]
        features = []
        for day_volume, symbol, spread_pct in top_volume:
            try:
                row = self._universe_features(symbol, day_volume, spread_pct)
                if row and all(math.isfinite(float(row[k])) for k in ("dayVolume", "atrPct", "rangePct", "volumeAcceleration", "spreadPct")):
                    features.append(row)
            except Exception as exc:
                LOG.warning("Universe feature failed %s: %s", symbol, exc)
        if not features:
            if self.active_symbols:
                self._event("UNIVERSE_WARN", "动态标的池刷新失败，保留上一轮", error="no eligible candidates")
                return self.active_symbols
            raise RuntimeError("No eligible Binance futures candidates for dynamic universe")
        p_atr = percentile_map(features, "atrPct")
        p_range = percentile_map(features, "rangePct")
        p_accel = percentile_map(features, "volumeAcceleration")
        p_turnover = percentile_map(features, "dayVolume")
        for row in features:
            symbol = row["symbol"]
            row["score"] = 100.0 * (0.40 * p_atr[symbol] + 0.25 * p_range[symbol] + 0.20 * p_accel[symbol] + 0.15 * p_turnover[symbol])
        features.sort(key=lambda row: (-row["score"], -row["dayVolume"], row["symbol"]))
        selected = features[: self.cfg.active_universe_size]
        self.active_symbols = [row["symbol"] for row in selected]
        for symbol in self.active_symbols:
            try:
                self.bars1[symbol] = self._closed(self.market.klines(symbol, "1h", 60))
            except Exception as exc:
                LOG.warning("1h preload failed %s: %s", symbol, exc)
        campaign_symbols = list(self._campaigns().keys())
        self.market.start_streams(self.active_symbols + campaign_symbols)
        now_ms = int(time.time() * 1000)
        def mutate(data: dict):
            data["universe"] = {
                "topVolume": [symbol for _, symbol, _ in top_volume],
                "active": list(self.active_symbols),
                "scores": selected,
                "updatedAt": now_ms,
            }
            data["updatedAt"] = now_ms
        self.state.mutate(mutate)
        self.last_universe_refresh = time.time()
        self._event("UNIVERSE", "Binance 动态标的池已刷新", active=self.active_symbols)
        return self.active_symbols

    def _trend(self, bars: List[dict]) -> Optional[str]:
        bars = self._closed(bars)
        if len(bars) < 51:
            return None
        closes = [x["close"] for x in bars]
        current = closes[-1]
        e20 = ema(closes, 20)
        e50 = ema(closes, 50)
        prior_e20 = ema(closes[:-1], 20)
        if current > e20 > e50 and e20 > prior_e20:
            return "LONG"
        if current < e20 < e50 and e20 < prior_e20:
            return "SHORT"
        return None

    def _entry_candidate(self, symbol: str) -> Optional[dict]:
        bars5 = self._closed(self.bars5.get(symbol, []))
        bars1 = self._closed(self.bars1.get(symbol, []))
        need = max(self.cfg.breakout_period + 2, self.cfg.volume_period + 2, self.cfg.atr_period + 2)
        if len(bars5) < need or len(bars1) < 51:
            return None
        current = bars5[-1]
        previous = bars5[:-1]
        breakout_window = previous[-self.cfg.breakout_period:]
        volume_window = previous[-self.cfg.volume_period:]
        resistance = max(x["high"] for x in breakout_window)
        support = min(x["low"] for x in breakout_window)
        avg_volume = sum(x["volume"] for x in volume_window) / len(volume_window)
        volume_ratio = current["volume"] / avg_volume if avg_volume > 0 else 0.0
        if volume_ratio < self.cfg.volume_multiplier:
            return None
        direction = self._trend(bars1)
        current_atr = atr(bars5, self.cfg.atr_period)
        closes = [x["close"] for x in bars1]
        e20, e50 = ema(closes, 20), ema(closes, 50)
        boundary = resistance if current["close"] > resistance else support
        breakout_strength = abs(current["close"] - boundary) / current_atr if current_atr > 0 else 0.0
        volume_strength = min(1.0, volume_ratio / 3.0)
        trend_strength = min(1.0, abs(e20 - e50) / current_atr) if current_atr > 0 else 0.0
        score = min(100.0, 50.0 * min(1.0, breakout_strength) + 30.0 * volume_strength + 20.0 * trend_strength)
        if current["close"] > resistance and direction == "LONG":
            return {"symbol": symbol, "side": "LONG", "atr": current_atr, "candle": current["open_time"], "score": score,
                    "reason": f"5m Donchian12 LONG + volume {volume_ratio:.2f}x + 1h EMA20>EMA50 rising"}
        if current["close"] < support and direction == "SHORT":
            return {"symbol": symbol, "side": "SHORT", "atr": current_atr, "candle": current["open_time"], "score": score,
                    "reason": f"5m Donchian12 SHORT + volume {volume_ratio:.2f}x + 1h EMA20<EMA50 falling"}
        return None

    def _refresh_active_bars(self) -> None:
        active = set(self.active_symbols)
        campaign_symbols = set(self._campaigns().keys())
        for symbol in sorted(active | campaign_symbols):
            try:
                self.bars5[symbol] = self._closed(self.market.klines(symbol, "5m", 80))
                if symbol in active:
                    self.bars1[symbol] = self._closed(self.market.klines(symbol, "1h", 60))
            except Exception as exc:
                LOG.warning("Tracked candle refresh failed %s: %s", symbol, exc)

    def _open_risk(self, positions: Optional[Dict[str, dict]] = None) -> float:
        if positions is None:
            positions = {x["symbol"]: x for x in self.broker.positions()}
        total = 0.0
        for symbol, campaign in self._campaigns().items():
            pos = positions.get(symbol)
            if not pos:
                continue
            entry = float(pos["entryPrice"])
            stop = float(campaign["currentStop"])
            qty = float(pos["quantity"])
            adverse = max(entry - stop, 0.0) if pos["side"] == "LONG" else max(stop - entry, 0.0)
            total += adverse * qty
        return total

    def update_risk(self) -> dict:
        equity = self.broker.equity()
        snapshot = self.state.snapshot()
        risk = dict(snapshot.get("risk", {}))
        peak = max(float(risk.get("peakEquity", equity)), equity)
        day = utc_day()
        daily_start = float(risk.get("dailyStartBalance", equity))
        if risk.get("dailyKey") != day:
            daily_start = equity
            risk["dailyKey"] = day
        drawdown_pct = max(0.0, (peak - equity) / peak * 100.0) if peak > 0 else 0.0
        daily_loss_pct = max(0.0, (daily_start - equity) / daily_start * 100.0) if daily_start > 0 else 0.0
        kill = bool(risk.get("drawdownKillActive")) or drawdown_pct >= self.cfg.max_drawdown_pct
        risk.update({
            "peakEquity": peak,
            "dailyStartBalance": daily_start,
            "drawdownKillActive": kill,
            "drawdownPct": drawdown_pct,
            "dailyLossPct": daily_loss_pct,
            "dailyLossHit": daily_loss_pct >= self.cfg.daily_loss_pct,
            "equity": equity,
            "openRiskUsd": self._open_risk(),
            "openRiskCapUsd": min(self.cfg.max_open_risk_usd, equity * self.cfg.max_portfolio_risk_pct / 100.0),
            "updatedAt": int(time.time() * 1000),
        })
        def mutate(data: dict):
            data["risk"] = risk
            data["updatedAt"] = int(time.time() * 1000)
        self.state.mutate(mutate)
        return risk

    def reconcile(self) -> bool:
        raw_positions = self.broker.state.snapshot().get("positions", {})
        campaigns = self._campaigns()
        blocked = None
        for symbol, campaign in list(campaigns.items()):
            pos = raw_positions.get(symbol)
            if not pos:
                self.broker.cancel_stop(symbol)
                self._remove_campaign(symbol)
                self._event("RECONCILE", "清理无对应 Paper 仓位的陈旧 Campaign", symbol=symbol)
                continue
            expected_qty = float(campaign.get("totalQuantity", 0))
            actual_qty = float(pos.get("quantity", 0))
            qty_diff = abs(actual_qty - expected_qty) / expected_qty if expected_qty > 0 else 1.0
            if pos.get("side") != campaign.get("side") or qty_diff > 0.01:
                campaign["stage"] = "RECONCILE_REQUIRED"
                self._save_campaign(symbol, campaign)
                blocked = f"{symbol} Paper position differs from Campaign state"
                break
            stop_order = self.broker.state.snapshot().get("stopOrders", {}).get(symbol)
            if not stop_order or abs(float(stop_order.get("quantity", 0)) - actual_qty) / max(actual_qty, 1e-12) > 0.01:
                oid = self.broker.replace_stop(symbol, float(campaign["currentStop"]), actual_qty)
                campaign["stopOrderId"] = oid
                self._save_campaign(symbol, campaign)
        if not blocked:
            current_campaigns = self._campaigns()
            for symbol, pos in list(raw_positions.items()):
                if symbol in current_campaigns:
                    continue
                actual_qty = abs(float(pos.get("quantity", 0)))
                entry_price = abs(float(pos.get("entryPrice", 0)))
                notional = actual_qty * entry_price
                min_notional = self.market.min_notional(symbol)
                rounded_qty = self.market.round_qty(symbol, actual_qty)
                # 仅自动处理已经小到无法形成正常 Binance 订单的 orphan dust。
                # 真正有价值的孤儿仓位仍必须阻断，绝不能静默吞掉。
                is_dust = actual_qty > 0 and (rounded_qty <= 0 or (min_notional > 0 and notional < min_notional))
                if is_dust:
                    fill = self.broker.close(symbol, action="RECONCILE_DUST_EXIT")
                    if fill:
                        self._event(
                            "RECONCILE_DUST_CLEARED",
                            "自动清理无 Campaign 的不可交易 Paper 尾差仓",
                            symbol=symbol,
                            quantity=fill.get("quantity"),
                            price=fill.get("price"),
                            notional=notional,
                            minNotional=min_notional,
                        )
                        continue
                blocked = f"{symbol} has Paper position but no Campaign state"
                break
        self._set_blocked(blocked)
        self.last_reconcile_at = time.time()
        if blocked:
            self._event("RECONCILE_BLOCK", "检测到仓位/策略状态不一致，禁止新开仓", reason=blocked)
            return False
        return True

    def _can_open(self, side: str, risk: dict) -> bool:
        snapshot = self.state.snapshot()
        if snapshot.get("blockedReason") or risk.get("drawdownKillActive") or risk.get("dailyLossHit"):
            return False
        positions = self.broker.positions()
        if len(positions) >= self.cfg.max_positions:
            return False
        if sum(1 for p in positions if p["side"] == side) >= self.cfg.max_same_direction:
            return False
        return True

    def _campaign_scale(self) -> float:
        return 1.0 + (self.cfg.add1_size if self.cfg.max_adds >= 1 else 0.0) + (self.cfg.add2_size if self.cfg.max_adds >= 2 else 0.0)

    def _open_campaign(self, candidate: dict) -> bool:
        symbol, side = candidate["symbol"], candidate["side"]
        if symbol in self._campaigns():
            return False
        risk = self.update_risk()
        if not self._can_open(side, risk):
            return False
        positions = self.broker.positions()
        current_open_risk = self._open_risk({p["symbol"]: p for p in positions})
        cap = min(self.cfg.max_open_risk_usd, risk["equity"] * self.cfg.max_portfolio_risk_pct / 100.0)
        remaining_slots = max(1, self.cfg.max_positions - len(positions))
        desired_risk = min(self.cfg.initial_risk_usd, risk["equity"] * self.cfg.initial_risk_pct / 100.0,
                           max(0.0, cap - current_open_risk) / remaining_slots)
        if desired_risk <= 0:
            return False
        book = self.market.book(symbol)
        reference = book["ask"] if side == "LONG" else book["bid"]
        stop_distance = float(candidate["atr"]) * self.cfg.initial_stop_atr
        if reference <= 0 or stop_distance <= 0:
            return False
        qty_by_risk = desired_risk / stop_distance
        campaign_notional_cap = risk["equity"] * self.cfg.max_leverage / self.cfg.max_positions
        base_notional_cap = campaign_notional_cap / self._campaign_scale()
        qty_by_notional = base_notional_cap / reference
        qty = self.market.round_qty(symbol, min(qty_by_risk, qty_by_notional))
        if qty <= 0 or qty * reference < self.market.min_notional(symbol):
            return False
        fill = self.broker.open_or_add(symbol, side, qty, "ENTRY1")
        if not fill:
            return False
        initial_stop_raw = fill["price"] - stop_distance if side == "LONG" else fill["price"] + stop_distance
        initial_stop = self.market.round_price(symbol, initial_stop_raw)
        actual_r = abs(fill["price"] - initial_stop)
        campaign = {
            "id": f"{symbol}-{side}-{int(time.time()*1000)}",
            "symbol": symbol,
            "side": side,
            "stage": "ENTRY1" if self.cfg.max_adds else "TRAILING",
            "baseEntry": fill["price"],
            "initialStop": initial_stop,
            "initialRiskDistance": actual_r,
            "baseQuantity": fill["quantity"],
            "totalQuantity": fill["quantity"],
            "currentStop": initial_stop,
            "favorableExtreme": fill["price"],
            "initialRiskUsd": actual_r * fill["quantity"],
            "lastAtr": float(candidate["atr"]),
            "legs": [{"kind": "ENTRY1", "price": fill["price"], "quantity": fill["quantity"], "time": int(time.time()*1000)}],
            "reason": candidate["reason"],
            "score": candidate["score"],
            "startedAt": int(time.time() * 1000),
            "updatedAt": int(time.time() * 1000),
        }
        oid = self.broker.replace_stop(symbol, initial_stop, fill["quantity"])
        if oid is None:
            self.broker.close(symbol, action="PROTECTION_FAIL_EXIT")
            return False
        campaign["stopOrderId"] = oid
        self._save_campaign(symbol, campaign)
        self.market.start_streams(self.active_symbols + list(self._campaigns().keys()))
        self._event("OPEN", "顺势 Campaign 首仓已建立并确认保护止损", symbol=symbol, side=side,
                    entry=fill["price"], stop=initial_stop, quantity=fill["quantity"], reason=candidate["reason"])
        return True

    def scan_entries(self) -> None:
        self._refresh_active_bars()
        state_snapshot = self.state.snapshot()
        last_eval = dict(state_snapshot.get("lastEvaluated", {}))
        candidates = []
        newest_candle = 0
        evaluated = []
        for symbol in list(self.active_symbols):
            bars = self._closed(self.bars5.get(symbol, []))
            if not bars:
                continue
            candle = int(bars[-1]["open_time"])
            newest_candle = max(newest_candle, candle)
            if candle <= int(last_eval.get(symbol, 0)):
                continue
            evaluated.append((symbol, candle))
            candidate = self._entry_candidate(symbol)
            if candidate:
                candidates.append(candidate)
        if evaluated:
            def mutate(data: dict):
                target = data.setdefault("lastEvaluated", {})
                for symbol, candle in evaluated:
                    target[symbol] = candle
                data["updatedAt"] = int(time.time() * 1000)
            self.state.mutate(mutate)
        candidates.sort(key=lambda row: (-row["score"], row["symbol"]))
        opened = []
        for candidate in candidates:
            if self._open_campaign(candidate):
                opened.append(candidate["symbol"])
        self.last_scan_at = time.time()
        self.last_scan_candle = newest_candle
        self._event("SCAN", "5m 已收盘 K 扫描完成", evaluated=len(evaluated), candidates=len(candidates), opened=opened)

    @staticmethod
    def _ratchet(side: str, current: float, candidate: float) -> float:
        return max(current, candidate) if side == "LONG" else min(current, candidate)

    def _raise_stop_then_add(self, campaign: dict, trigger_r: float, size_mult: float, next_stage: str, leg_kind: str) -> bool:
        symbol, side = campaign["symbol"], campaign["side"]
        pos = self.broker.position(symbol)
        if not pos:
            return False
        if leg_kind == "ADD1":
            target = float(campaign["baseEntry"])
        else:
            distance = float(campaign["initialRiskDistance"])
            target = float(campaign["baseEntry"]) + distance if side == "LONG" else float(campaign["baseEntry"]) - distance
        target = self.market.round_price(symbol, target)
        new_stop = self._ratchet(side, float(campaign["currentStop"]), target)
        oid = self.broker.replace_stop(symbol, new_stop, float(pos["quantity"]))
        if oid is None:
            return False
        campaign["currentStop"] = new_stop
        campaign["stopOrderId"] = oid
        self._save_campaign(symbol, campaign)
        add_qty = self.market.round_qty(symbol, float(campaign["baseQuantity"]) * size_mult)
        if add_qty <= 0:
            return False
        fill = self.broker.open_or_add(symbol, side, add_qty, leg_kind)
        if not fill:
            return False
        live_after_add = self.broker.state.snapshot().get("positions", {}).get(symbol)
        if not live_after_add:
            return False
        # Campaign 总量以 Paper 账本实际仓位为唯一真值，避免 float 累加造成
        # Campaign/止损数量比真实仓位少一个 stepSize。
        campaign["totalQuantity"] = float(live_after_add["quantity"])
        campaign["stage"] = next_stage
        campaign.setdefault("legs", []).append({"kind": leg_kind, "price": fill["price"], "quantity": fill["quantity"], "time": int(time.time()*1000)})
        oid = self.broker.replace_stop(symbol, float(campaign["currentStop"]), float(campaign["totalQuantity"]))
        if oid is None:
            emergency = self.broker.close(symbol, action="PROTECTION_FAIL_EXIT")
            self._remove_campaign(symbol)
            self._event("PROTECTION_FAIL", f"{leg_kind} 后无法覆盖新总仓位保护止损，已立即退出", symbol=symbol, side=side,
                        exitPrice=emergency.get("price") if emergency else None)
            return False
        campaign["stopOrderId"] = oid
        self._save_campaign(symbol, campaign)
        self._event("ADD", f"{leg_kind} 已执行：先抬保护止损，再顺势加仓", symbol=symbol, side=side,
                    triggerR=trigger_r, addQuantity=fill["quantity"], stop=campaign["currentStop"])
        return True

    def manage_campaigns(self) -> None:
        if time.time() - self.last_reconcile_at >= 60:
            self.reconcile()
        risk = self.update_risk()
        positions = {p["symbol"]: p for p in self.broker.positions()}
        for symbol, campaign in list(self._campaigns().items()):
            pos = positions.get(symbol)
            if not pos:
                continue
            side = campaign["side"]
            mark = float(pos["markPrice"])
            stop = float(campaign["currentStop"])
            stop_hit = mark <= stop if side == "LONG" else mark >= stop
            if stop_hit:
                fill = self.broker.close(symbol, action="STOP_EXIT")
                if fill:
                    self._remove_campaign(symbol)
                    self._event("EXIT", "保护止损触发，Campaign 全部退出", symbol=symbol, side=side,
                                price=fill["price"], closedPnl=fill["closedPnl"])
                continue
            favorable = float(campaign.get("favorableExtreme", campaign["baseEntry"]))
            favorable = max(favorable, mark) if side == "LONG" else min(favorable, mark)
            campaign["favorableExtreme"] = favorable
            distance = max(float(campaign["initialRiskDistance"]), 1e-12)
            current_r = (mark - float(campaign["baseEntry"])) / distance if side == "LONG" else (float(campaign["baseEntry"]) - mark) / distance
            campaign["currentR"] = current_r
            bars = self._closed(self.bars5.get(symbol, []))
            if len(bars) >= self.cfg.atr_period + 1:
                try:
                    campaign["lastAtr"] = atr(bars, self.cfg.atr_period)
                except ValueError:
                    pass
            if campaign.get("stage") == "ENTRY1" and current_r >= self.cfg.add1_trigger_r:
                self._raise_stop_then_add(campaign, self.cfg.add1_trigger_r, self.cfg.add1_size, "ADD1", "ADD1")
            if campaign.get("stage") == "ADD1" and current_r >= self.cfg.add2_trigger_r:
                self._raise_stop_then_add(campaign, self.cfg.add2_trigger_r, self.cfg.add2_size, "TRAILING", "ADD2")
            if campaign.get("stage") == "TRAILING":
                trailing_distance = float(campaign.get("lastAtr", 0)) * self.cfg.trailing_atr
                if trailing_distance > 0:
                    candidate_stop = favorable - trailing_distance if side == "LONG" else favorable + trailing_distance
                    candidate_stop = self.market.round_price(symbol, candidate_stop)
                    new_stop = self._ratchet(side, float(campaign["currentStop"]), candidate_stop)
                    if abs(new_stop - float(campaign["currentStop"])) > 0:
                        oid = self.broker.replace_stop(symbol, new_stop, float(campaign["totalQuantity"]))
                        if oid is not None:
                            campaign["currentStop"] = new_stop
                            campaign["stopOrderId"] = oid
            self._save_campaign(symbol, campaign)
        self.last_cycle_at = time.time()
        self.write_runtime(risk)

    def should_scan(self) -> bool:
        now = int(time.time())
        interval = 300
        closed_open = ((now // interval) - 1) * interval * 1000
        return now % interval >= self.cfg.scan_grace_seconds and closed_open > self.last_scan_candle

    def write_runtime(self, risk: Optional[dict] = None) -> dict:
        risk = risk or self.update_risk()
        paper = self.broker.snapshot()
        strategy = self.state.snapshot()
        campaigns = strategy.get("campaigns", {})
        positions = []
        for pos in paper.get("positionsList", []):
            campaign = campaigns.get(pos["symbol"], {})
            positions.append({
                **pos,
                "stage": campaign.get("stage"),
                "currentStop": campaign.get("currentStop"),
                "currentR": campaign.get("currentR"),
                "baseEntry": campaign.get("baseEntry"),
                "initialRiskUsd": campaign.get("initialRiskUsd"),
                "lastAtr": campaign.get("lastAtr"),
                "stopOrderId": campaign.get("stopOrderId"),
                "legs": campaign.get("legs", []),
                "reason": campaign.get("reason"),
            })
        payload = {
            "version": "V2",
            "exchange": "Binance USDⓈ-M Futures",
            "strategyMode": "pyramiding",
            "tradingMode": "paper",
            "generatedAt": int(time.time() * 1000),
            "lastCycleAt": int(self.last_cycle_at * 1000) if self.last_cycle_at else 0,
            "lastScanAt": int(self.last_scan_at * 1000) if self.last_scan_at else 0,
            "lastScanCandle": self.last_scan_candle,
            "account": {
                "startingBalance": paper.get("startingBalance", self.cfg.start_balance),
                "cash": paper.get("cash"),
                "equity": paper.get("equity"),
                "unrealizedPnl": paper.get("unrealizedPnl"),
                "totalPnl": float(paper.get("equity", self.cfg.start_balance)) - float(paper.get("performanceBaseline", self.cfg.start_balance)),
            },
            "risk": risk,
            "blockedReason": strategy.get("blockedReason"),
            "positions": positions,
            "campaigns": campaigns,
            "recentFills": list(reversed(paper.get("fills", [])[-30:])),
            "closedFills": list(reversed([
                fill for fill in paper.get("fills", [])
                if isinstance(fill, dict)
                and ("EXIT" in str(fill.get("action") or "").upper() or float(fill.get("closedPnl", 0) or 0) != 0.0)
            ][-500:])),
            "stopOrders": paper.get("stopOrders", {}),
            "universe": strategy.get("universe", {}),
            "events": list(reversed(strategy.get("events", [])[-50:])),
            "market": self.market.status(),
            "config": {
                "volumeUniverseSize": self.cfg.volume_universe_size,
                "activeUniverseSize": self.cfg.active_universe_size,
                "maxSpreadPct": self.cfg.max_spread_pct,
                "entryTimeframe": self.cfg.entry_timeframe,
                "trendTimeframe": self.cfg.trend_timeframe,
                "breakoutPeriod": self.cfg.breakout_period,
                "volumePeriod": self.cfg.volume_period,
                "volumeMultiplier": self.cfg.volume_multiplier,
                "atrPeriod": self.cfg.atr_period,
                "initialStopAtr": self.cfg.initial_stop_atr,
                "trailingAtr": self.cfg.trailing_atr,
                "initialRiskPct": self.cfg.initial_risk_pct,
                "initialRiskUsd": self.cfg.initial_risk_usd,
                "maxOpenRiskUsd": self.cfg.max_open_risk_usd,
                "maxPortfolioRiskPct": self.cfg.max_portfolio_risk_pct,
                "add1TriggerR": self.cfg.add1_trigger_r,
                "add1Size": self.cfg.add1_size,
                "add2TriggerR": self.cfg.add2_trigger_r,
                "add2Size": self.cfg.add2_size,
                "maxPositions": self.cfg.max_positions,
                "maxSameDirection": self.cfg.max_same_direction,
                "maxDrawdownPct": self.cfg.max_drawdown_pct,
                "dailyLossPct": self.cfg.daily_loss_pct,
                "maxLeverage": self.cfg.max_leverage,
                "feeBps": self.cfg.fee_bps,
                "slippageBps": self.cfg.slippage_bps,
            },
        }
        atomic_json_write(self.cfg.runtime_state_file, payload)
        return payload

    def initialize(self) -> None:
        self.market.refresh_exchange_info(force=True)
        self.reconcile()
        self.refresh_universe(force=True)
        self._refresh_active_bars()
        self.update_risk()
        self.write_runtime()
        self._event("START", "Binance 顺势滚仓 V2 Paper 初始化完成", active=self.active_symbols)

    def cycle(self) -> None:
        try:
            self.refresh_universe()
            if self.should_scan():
                self.scan_entries()
            self.manage_campaigns()
            self.last_error = ""
        except Exception as exc:
            self.last_error = str(exc)
            LOG.exception("Strategy cycle failed")
            try:
                self.write_runtime()
            except Exception:
                pass
