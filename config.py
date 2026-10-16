import os
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent
DATA_DIR = Path(os.environ.get("BINANCE_PYRAMIDING_DATA_DIR", str(ROOT / "data"))).resolve()

@dataclass(frozen=True)
class Settings:
    rest_base: str = "https://fapi.binance.com"
    ws_public_base: str = "wss://fstream.binance.com/public/stream?streams="
    ws_market_base: str = "wss://fstream.binance.com/market/stream?streams="
    start_balance: float = 10000.0
    fee_bps: float = 5.0
    slippage_bps: float = 2.0
    volume_universe_size: int = 30
    active_universe_size: int = 12
    universe_refresh_seconds: int = 900
    max_spread_pct: float = 0.40
    entry_timeframe: str = "5m"
    trend_timeframe: str = "1h"
    breakout_period: int = 12
    volume_period: int = 20
    volume_multiplier: float = 1.40
    atr_period: int = 14
    initial_stop_atr: float = 2.00
    trailing_atr: float = 2.20
    initial_risk_pct: float = 5.00
    initial_risk_usd: float = 500.0
    max_open_risk_usd: float = 6000.0
    max_portfolio_risk_pct: float = 60.0
    max_adds: int = 2
    add1_trigger_r: float = 0.75
    add1_size: float = 0.75
    add2_trigger_r: float = 1.50
    add2_size: float = 0.50
    max_positions: int = 12
    max_same_direction: int = 8
    max_drawdown_pct: float = 15.0
    drawdown_kill_enabled: bool = False  # Paper 前向阶段暂时关闭 Peak Drawdown Kill
    daily_loss_pct: float = 5.0
    daily_loss_gate_enabled: bool = False  # Paper 测试阶段暂时关闭日亏损 5% 开仓门禁；仍保留统计
    max_leverage: float = 5.0
    scan_grace_seconds: int = 8
    manage_interval_seconds: int = 3
    rest_soft_weight_per_minute: int = 1200
    health_host: str = "127.0.0.1"
    health_port: int = 3012
    paper_state_file: Path = DATA_DIR / "binance-pyramiding-paper-state.json"
    strategy_state_file: Path = DATA_DIR / "binance-pyramiding-state.json"
    runtime_state_file: Path = DATA_DIR / "runtime-status.json"

SETTINGS = Settings()
