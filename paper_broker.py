import time
from decimal import Decimal
from typing import Dict, List, Optional

from binance_market import BinanceFuturesMarket
from config import SETTINGS, Settings
from storage import JsonState


def _default_paper_state(cfg: Settings) -> dict:
    now = int(time.time() * 1000)
    return {
        "version": 1,
        "exchange": "BINANCE_USDM",
        "mode": "paper",
        "startingBalance": cfg.start_balance,
        "performanceBaseline": cfg.start_balance,
        "cash": cfg.start_balance,
        "positions": {},
        "fills": [],
        "stopOrders": {},
        "nextOrderId": 1,
        "createdAt": now,
        "updatedAt": now,
    }


class PaperBroker:
    def __init__(self, market: BinanceFuturesMarket, settings: Settings = SETTINGS):
        self.market = market
        self.cfg = settings
        self.state = JsonState(settings.paper_state_file, lambda: _default_paper_state(settings))

    def _next_oid(self, data: dict) -> int:
        oid = int(data.get("nextOrderId", 1))
        data["nextOrderId"] = oid + 1
        return oid

    def _fill_price(self, symbol: str, side: str) -> float:
        book = self.market.book(symbol)
        slip = self.cfg.slippage_bps / 10000.0
        raw = book["ask"] * (1.0 + slip) if side == "BUY" else book["bid"] * (1.0 - slip)
        return self.market.round_price(symbol, raw)

    def _append_fill(self, data: dict, *, oid: int, symbol: str, side: str, action: str,
                     price: float, quantity: float, closed_pnl: float, fee: float) -> None:
        data.setdefault("fills", []).append({
            "oid": oid,
            "symbol": symbol,
            "side": side,
            "action": action,
            "price": price,
            "quantity": quantity,
            "closedPnl": closed_pnl,
            "fee": fee,
            "net": closed_pnl - fee,
            "time": int(time.time() * 1000),
        })
        if len(data["fills"]) > 5000:
            data["fills"] = data["fills"][-5000:]

    def open_or_add(self, symbol: str, side: str, requested_qty: float, action: str) -> Optional[dict]:
        if side not in ("LONG", "SHORT"):
            raise ValueError("side must be LONG or SHORT")
        order_side = "BUY" if side == "LONG" else "SELL"
        qty = self.market.round_qty(symbol, requested_qty)
        if qty <= 0:
            return None
        price = self._fill_price(symbol, order_side)
        if price <= 0 or price * qty < self.market.min_notional(symbol):
            return None
        fee = price * qty * self.cfg.fee_bps / 10000.0
        result = {}

        def mutate(data: dict):
            nonlocal result
            positions = data.setdefault("positions", {})
            existing = positions.get(symbol)
            if existing and existing.get("side") != side:
                raise RuntimeError(f"Paper flip disabled for {symbol}; close existing position first")
            oid = self._next_oid(data)
            if existing:
                old_qty = float(existing["quantity"])
                old_entry = float(existing["entryPrice"])
                # Paper 数量来自 Binance stepSize 离散值；使用十进制定点相加，
                # 避免 62.335 之类数量变成 62.334999... 后在平仓时被再次 floor 少一个 step。
                new_qty = float(Decimal(str(old_qty)) + Decimal(str(qty)))
                existing["entryPrice"] = (old_entry * old_qty + price * qty) / new_qty
                existing["quantity"] = new_qty
                existing["updatedAt"] = int(time.time() * 1000)
            else:
                positions[symbol] = {
                    "symbol": symbol,
                    "side": side,
                    "quantity": qty,
                    "entryPrice": price,
                    "openedAt": int(time.time() * 1000),
                    "updatedAt": int(time.time() * 1000),
                }
            data["cash"] = float(data.get("cash", self.cfg.start_balance)) - fee
            data["updatedAt"] = int(time.time() * 1000)
            self._append_fill(data, oid=oid, symbol=symbol, side=order_side, action=action,
                              price=price, quantity=qty, closed_pnl=0.0, fee=fee)
            result = {"oid": oid, "price": price, "quantity": qty, "fee": fee}

        self.state.mutate(mutate)
        return result

    def close(self, symbol: str, requested_qty: Optional[float] = None, action: str = "CLOSE") -> Optional[dict]:
        snapshot = self.state.snapshot()
        existing = snapshot.get("positions", {}).get(symbol)
        if not existing:
            return None
        side = existing["side"]
        order_side = "SELL" if side == "LONG" else "BUY"
        available_qty = float(existing["quantity"])
        # requested_qty=None 表示“整仓退出”。Paper 账本必须精确归零，不能把已经
        # 合法离散化过的持仓数量再次向下取整，否则浮点尾差可能留下 1 个 step 的 dust。
        if requested_qty is None:
            qty = available_qty
        else:
            qty = self.market.round_qty(symbol, min(available_qty, requested_qty))
        if qty <= 0:
            return None
        price = self._fill_price(symbol, order_side)
        fee = price * qty * self.cfg.fee_bps / 10000.0
        entry = float(existing["entryPrice"])
        pnl = (price - entry) * qty if side == "LONG" else (entry - price) * qty
        result = {}

        def mutate(data: dict):
            nonlocal result
            live = data.setdefault("positions", {}).get(symbol)
            if not live:
                return
            current_qty = float(live["quantity"])
            close_qty = min(qty, current_qty)
            oid = self._next_oid(data)
            remaining = current_qty - close_qty
            if remaining <= 1e-15:
                data["positions"].pop(symbol, None)
                data.setdefault("stopOrders", {}).pop(symbol, None)
            else:
                live["quantity"] = remaining
                live["updatedAt"] = int(time.time() * 1000)
            data["cash"] = float(data.get("cash", self.cfg.start_balance)) + pnl - fee
            data["updatedAt"] = int(time.time() * 1000)
            self._append_fill(data, oid=oid, symbol=symbol, side=order_side, action=action,
                              price=price, quantity=close_qty, closed_pnl=pnl, fee=fee)
            result = {"oid": oid, "price": price, "quantity": close_qty, "closedPnl": pnl, "fee": fee}

        self.state.mutate(mutate)
        return result or None

    def replace_stop(self, symbol: str, trigger_price: float, quantity: float) -> Optional[int]:
        qty = self.market.round_qty(symbol, quantity)
        if qty <= 0:
            return None
        trigger = self.market.round_price(symbol, trigger_price)
        result = {"oid": None}

        def mutate(data: dict):
            oid = self._next_oid(data)
            data.setdefault("stopOrders", {})[symbol] = {
                "oid": oid,
                "symbol": symbol,
                "triggerPrice": trigger,
                "quantity": qty,
                "updatedAt": int(time.time() * 1000),
            }
            data["updatedAt"] = int(time.time() * 1000)
            result["oid"] = oid

        self.state.mutate(mutate)
        return result["oid"]

    def cancel_stop(self, symbol: str) -> None:
        def mutate(data: dict):
            data.setdefault("stopOrders", {}).pop(symbol, None)
            data["updatedAt"] = int(time.time() * 1000)
        self.state.mutate(mutate)

    def positions(self) -> List[dict]:
        snapshot = self.state.snapshot()
        result = []
        for symbol, pos in snapshot.get("positions", {}).items():
            try:
                mark = self.market.mark_price(symbol)
            except Exception:
                mark = float(pos["entryPrice"])
            qty = float(pos["quantity"])
            entry = float(pos["entryPrice"])
            side = pos["side"]
            unreal = (mark - entry) * qty if side == "LONG" else (entry - mark) * qty
            result.append({**pos, "markPrice": mark, "unrealizedPnl": unreal})
        return result

    def position(self, symbol: str) -> Optional[dict]:
        for pos in self.positions():
            if pos["symbol"] == symbol:
                return pos
        return None

    def equity(self) -> float:
        snapshot = self.state.snapshot()
        cash = float(snapshot.get("cash", self.cfg.start_balance))
        return cash + sum(float(pos.get("unrealizedPnl", 0)) for pos in self.positions())

    def snapshot(self) -> dict:
        raw = self.state.snapshot()
        positions = self.positions()
        cash = float(raw.get("cash", self.cfg.start_balance))
        equity = cash + sum(float(p["unrealizedPnl"]) for p in positions)
        return {
            **raw,
            "cash": cash,
            "equity": equity,
            "unrealizedPnl": equity - cash,
            "positionsList": positions,
        }
