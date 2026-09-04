# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.3
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% [markdown] papermill={"duration": 0.011078, "end_time": "2026-06-14T17:08:05.164903+00:00", "exception": false, "start_time": "2026-06-14T17:08:05.153825+00:00", "status": "completed"}
# # Alpaca Paper Trading Demo
#
# **Docker image**: `ml4t`
#
# **Chapter**: 25 - Live Trading Systems
# **Section**: 25.3 (Alpaca Integration)
# **Learning Outcome**: LO2 - Connect ml4t-backtest strategies to live brokers
#
# This notebook demonstrates:
# 1. Connecting to Alpaca paper trading account
# 2. Querying account information and positions
# 3. Real-time data feed with bar/quote/trade streaming
# 4. Safe Alpaca paper submission with isolated shadow-mode order examples
# 5. Strategy execution with ETF momentum signals
#
# **Learning Objectives**
# - Verify the environment, SDK, and account state before connecting a strategy to a live broker.
# - See how the same backtest strategy is wrapped with explicit paper-mode risk controls.
# - Compare the optional live-feed wiring with the default offline simulation path.
#
# **Prerequisites**:
# - Alpaca account with API keys (paper trading enabled)
# - Environment variables: ALPACA_API_KEY, ALPACA_SECRET_KEY
# - Familiarity with the ETF case study strategy used earlier in the book
#
# **Why Alpaca alongside IB**: see §25.3 for the broker-comparison narrative.
#
# **Data Contract**:
# - **Input**: Deterministic simulated bars by default; Alpaca bars after explicit opt-in
# - **Output**: Alpaca paper account state, signals, and isolated shadow order examples

# %% papermill={"duration": 3.438989, "end_time": "2026-06-14T17:08:08.613240+00:00", "exception": false, "start_time": "2026-06-14T17:08:05.174251+00:00", "status": "completed"}
"""Connect ml4t strategies to Alpaca with explicit paper and shadow risk controls."""

import asyncio
import logging
import os
import warnings
from datetime import UTC, datetime, timedelta

import numpy as np
import polars as pl
from async_utils import run_async
from ml4t.backtest import OrderSide, Strategy
from ml4t.backtest.types import Order, OrderType, Position
from ml4t.specs import ExecutionCapability

from utils.paths import display_path, get_output_dir
from utils.reproducibility import set_global_seeds

# alpaca-py is an optional broker SDK; the simulated path runs without it. The
# try/except is the only such optional import in the notebook.
HAS_ALPACA_SDK = False
try:
    import alpaca  # noqa: F401
    from alpaca.trading.client import TradingClient
    from ml4t.live import AlpacaBroker, AlpacaDataFeed, LiveEngine, LiveRiskConfig
    from ml4t.live.safety import SafeBroker

    HAS_ALPACA_SDK = True
except ImportError:
    pass

warnings.filterwarnings("ignore")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)
logging.getLogger("alpaca").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

if HAS_ALPACA_SDK:
    print("[OK] ml4t.live Alpaca components imported")
else:
    print("Alpaca SDK not installed (uv add alpaca-py); running simulation only")


if HAS_ALPACA_SDK:

    class PaperReportingAlpacaBroker(AlpacaBroker):
        """Capture cached PAPER positions before provider cleanup."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.paper_positions = None
            self.paper_positions_error = None

        async def disconnect(self):
            """Capture local positions once, then always run the normal disconnect."""
            try:
                if self.paper_positions is None and self.paper_positions_error is None:
                    self.paper_positions = self.positions
            except Exception as error:
                self.paper_positions_error = type(error).__name__
            finally:
                await super().disconnect()


# %% papermill={"duration": 0.004675, "end_time": "2026-06-14T17:08:08.619490+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.614815+00:00", "status": "completed"} tags=["parameters"]
from datetime import datetime
from zoneinfo import ZoneInfo


def require_engine_mode(value: str | None) -> str:
    """Require an explicit PAPER or SHADOW engine selection."""
    if value not in {"paper", "shadow"}:
        raise ValueError("ML4T_ENGINE_MODE must be explicitly set to 'paper' or 'shadow'")
    return value


def resolve_session_duration_seconds(value: str, now_et: datetime | None = None) -> int:
    """Resolve `full` or a bounded number of regular-session minutes."""
    selection = value.strip().lower() if isinstance(value, str) else ""
    if selection == "full":
        current = now_et or datetime.now(ZoneInfo("America/New_York"))
        close = current.replace(hour=16, minute=0, second=0, microsecond=0)
        return min(390 * 60, max(60, int((close - current).total_seconds())))
    if not selection.isdecimal():
        raise ValueError("ML4T_SESSION_DURATION_MINUTES must be 'full' or an integer")
    minutes = int(selection)
    if not 1 <= minutes <= 390:
        raise ValueError("ML4T_SESSION_DURATION_MINUTES must be between 1 and 390")
    return minutes * 60


def parse_order_demo_opt_in(value: str | None) -> bool:
    """Accept only an explicit 1 to enable the educational SHADOW order demo."""
    if value in {None, "0"}:
        return False
    if value == "1":
        return True
    raise ValueError("ML4T_RUN_EDUCATIONAL_ORDER_DEMO must be '0' or '1'")


ENGINE_MODE = require_engine_mode(os.environ.get("ML4T_ENGINE_MODE"))
DEMO_DURATION_SECONDS = resolve_session_duration_seconds(
    os.environ.get("ML4T_SESSION_DURATION_MINUTES", "full")
)
RUN_EDUCATIONAL_ORDER_DEMO = parse_order_demo_opt_in(
    os.environ.get("ML4T_RUN_EDUCATIONAL_ORDER_DEMO")
)
MAX_SYMBOLS = 0
SIMULATION_STEPS = 10
LIVE_FEED = 1  # explicit opt-in; default execution is offline and paper-safe
SEED = 42

# %% papermill={"duration": 0.142512, "end_time": "2026-06-14T17:08:08.763511+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.620999+00:00", "status": "completed"}
# The Alpaca WebSocket loop is incompatible with papermill's nest_asyncio:
# `asyncio.wait_for` cannot reliably cancel the inner streaming task, so a
# headless run hangs past DEMO_DURATION_SECONDS. Detect papermill via its
# injected env var and fall back to the simulated path. Interactive Jupyter is
# unaffected.
if os.environ.get("ML4T_HEADLESS_PAPERMILL") == "1":
    LIVE_FEED = 0
    print("Headless papermill detected: LIVE_FEED disabled, simulated path will run")

set_global_seeds(SEED)

ALPACA_API_KEY = os.environ.get("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.environ.get("ALPACA_SECRET_KEY", "")
PAPER_TRADING = True

ALL_SYMBOLS = ["SPY", "QQQ", "IWM"]
SYMBOLS = ALL_SYMBOLS[:MAX_SYMBOLS] if MAX_SYMBOLS > 0 else ALL_SYMBOLS.copy()

# %% [markdown] papermill={"duration": 0.001442, "end_time": "2026-06-14T17:08:08.766554+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.765112+00:00", "status": "completed"}
# ## 1. Credential Verification
#
# Alpaca requires API keys for authentication. For paper trading:
# - Sign up at https://alpaca.markets
# - Generate API keys in the dashboard
# - Set environment variables (never hardcode!)


# %% papermill={"duration": 0.005645, "end_time": "2026-06-14T17:08:08.773742+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.768097+00:00", "status": "completed"}
def verify_credentials():
    """Check that API credentials and SDK are available."""
    if not HAS_ALPACA_SDK:
        print("\n" + "=" * 60)
        print("ALPACA SDK NOT INSTALLED")
        print("=" * 60)
        print("\nTo connect to Alpaca, install the SDK:")
        print("   uv add alpaca-py")
        print("\nRunning in DEMO MODE with simulated broker...")
        return False

    if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
        print("\n" + "=" * 60)
        print("ALPACA CREDENTIALS NOT FOUND")
        print("=" * 60)
        print("\nTo run this notebook with real Alpaca connection:")
        print("1. Create an Alpaca account at https://alpaca.markets")
        print("2. Generate API keys in the dashboard")
        print("3. Set environment variables:")
        print("   export ALPACA_API_KEY='PKXXXXXXXX'")
        print("   export ALPACA_SECRET_KEY='xxxxxxxxxx'")
        print("\nRunning in DEMO MODE with simulated broker...")
        return False

    print("\n" + "=" * 60)
    print("ALPACA CREDENTIALS VERIFIED")
    print("=" * 60)
    print(f"Engine Mode: {ENGINE_MODE.upper()}")
    if ENGINE_MODE == "paper":
        print(f"Paper Trading: {'YES' if PAPER_TRADING else 'NO (LIVE!)'}")
    else:
        print("Credentials will authenticate Alpaca IEX market data only")
    return True


HAS_CREDENTIALS = verify_credentials()

# %% [markdown] papermill={"duration": 0.001399, "end_time": "2026-06-14T17:08:08.776599+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.775200+00:00", "status": "completed"}
# **Finding**: The credential gate explicitly distinguishes missing SDKs from missing secrets, which makes
# the execution mode observable before the notebook touches a broker connection.
#
# **Trading implication**: Live notebooks should never hide whether they are authenticated, shadowing, or
# fully simulated because that status determines the operational risk of every downstream action.
#
# %% [markdown] papermill={"duration": 0.001368, "end_time": "2026-06-14T17:08:08.779406+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.778038+00:00", "status": "completed"}
# ## 2. Connect to Alpaca
#
# The AlpacaBroker class handles:
# - REST API for orders and account info
# - WebSocket for real-time order updates
# - Automatic position/order synchronization


# %% papermill={"duration": 0.130348, "end_time": "2026-06-14T17:08:08.911179+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.780831+00:00", "status": "completed"}
def get_alpaca_account_snapshot():
    """Fetch an account snapshot without starting the streaming broker session."""
    if not HAS_CREDENTIALS:
        return None, None, None, None

    print("\n" + "=" * 60)
    print("CONNECTING TO ALPACA")
    print("=" * 60)

    trading_client = TradingClient(
        api_key=ALPACA_API_KEY,
        secret_key=ALPACA_SECRET_KEY,
        paper=PAPER_TRADING,
    )
    account = trading_client.get_account()
    raw_positions = trading_client.get_all_positions()

    broker = PaperReportingAlpacaBroker(
        api_key=ALPACA_API_KEY,
        secret_key=ALPACA_SECRET_KEY,
        paper=PAPER_TRADING,
    )

    print("\nConnected to Alpaca")
    print(f"   Paper Trading: {'YES' if PAPER_TRADING else 'NO'}")

    nlv = float(account.equity)
    cash = float(account.cash)

    print("\nACCOUNT READINESS")
    print("   Account values received: [OK]")

    # Get positions
    positions = {
        position.symbol: {
            "quantity": float(position.qty),
            "entry_price": float(position.avg_entry_price),
            "current_price": float(position.current_price or position.avg_entry_price),
        }
        for position in raw_positions
    }
    print(f"   Open positions received: {len(positions)}")

    return broker, nlv, cash, positions


# PAPER alone needs the Alpaca trading/account client. SHADOW defers broker
# construction to the local MockBroker in the engine-wiring section.
if ENGINE_MODE == "paper" and LIVE_FEED and HAS_CREDENTIALS and HAS_ALPACA_SDK:
    try:
        broker, nlv, cash, positions = get_alpaca_account_snapshot()
    except Exception as e:
        print(f"\nConnection failed: {e}")
        print("\nTroubleshooting:")
        print("1. Are your API keys correct?")
        print("2. Is your account enabled for paper trading?")
        print("3. Check https://status.alpaca.markets for outages")
        raise RuntimeError("Alpaca paper session unavailable") from e
elif ENGINE_MODE == "shadow":
    broker = None
    print("SHADOW selected; no Alpaca trading/account client was constructed.")
else:
    broker = None
    print("Offline mode selected; no Alpaca account request was made.")

# %% [markdown] papermill={"duration": 0.006293, "end_time": "2026-06-14T17:08:08.927567+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.921274+00:00", "status": "completed"}
# **Finding**: The account summary confirms that the broker adapter exposes the same cash, equity, and
# position state the strategy will rely on later in the session.
#
# **Trading implication**: A live strategy should always prove that its broker snapshot is sane before it
# starts listening to market data; otherwise even correct signals can be routed with stale inventory.
#
# %% [markdown] papermill={"duration": 0.001909, "end_time": "2026-06-14T17:08:08.932368+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.930459+00:00", "status": "completed"}
# ## 3. ETF Momentum Strategy
#
# This strategy is **identical** to what we use in backtesting.
# Uses the ETF case study with SPY, QQQ, IWM.


# %% papermill={"duration": 0.006685, "end_time": "2026-06-14T17:08:08.940512+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.933827+00:00", "status": "completed"}
class ETFMomentumStrategy(Strategy):
    """ETF momentum strategy for live trading.

    Tracks 5-day momentum across ETFs and generates signals
    when momentum crosses thresholds.

    This is the SAME code used in backtest - zero changes for live!
    """

    def __init__(self, lookback: int = 5, threshold: float = 0.02, position_size: int = 10):
        self.lookback = lookback
        self.threshold = threshold
        self.position_size = position_size
        self.prices: dict[str, list[float]] = {}
        self.signals: list[dict] = []

    def on_start(self, broker):
        """Called when engine starts."""
        logger.info(f"Strategy started: ETFMomentum(lookback={self.lookback})")
        for symbol in SYMBOLS:
            self.prices[symbol] = []

    def on_data(self, timestamp: datetime, data: dict, context: dict, broker):
        """Called for each bar.

        Args:
            timestamp: Bar timestamp
            data: {symbol: {'open', 'high', 'low', 'close', 'volume'}}
            context: Additional metadata (vwap, trade_count, etc.)
            broker: Broker instance (sync interface)
        """
        for symbol, bar in data.items():
            if symbol not in self.prices:
                self.prices[symbol] = []

            close = bar["close"]
            self.prices[symbol].append(close)

            # Calculate momentum
            if len(self.prices[symbol]) > self.lookback:
                old_price = self.prices[symbol][-self.lookback - 1]
                momentum = (close - old_price) / old_price

                # Get current position
                position = broker.get_position(symbol)
                has_position = position is not None and position.quantity > 0

                # Generate signals
                if momentum > self.threshold and not has_position:
                    signal = {
                        "timestamp": timestamp,
                        "symbol": symbol,
                        "action": "BUY",
                        "momentum": momentum,
                        "price": close,
                    }
                    self.signals.append(signal)
                    logger.info(f"BUY {symbol}: Momentum {momentum:.2%} > {self.threshold:.2%}")
                    broker.submit_order(symbol, self.position_size, side=OrderSide.BUY)

                elif momentum < -self.threshold and has_position:
                    signal = {
                        "timestamp": timestamp,
                        "symbol": symbol,
                        "action": "SELL",
                        "momentum": momentum,
                        "price": close,
                    }
                    self.signals.append(signal)
                    logger.info(f"SELL {symbol}: Momentum {momentum:.2%} < -{self.threshold:.2%}")
                    broker.submit_order(symbol, self.position_size, side=OrderSide.SELL)

    def on_end(self, broker):
        """Called when engine stops."""
        logger.info(f"Strategy ended. Signals generated: {len(self.signals)}")


# %% [markdown] papermill={"duration": 0.001441, "end_time": "2026-06-14T17:08:08.943512+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.942071+00:00", "status": "completed"}
# ### Structured Signal and Order Log
#
# Both the live and simulated paths accumulate signals and order intentions into a Polars frame so reviewers
# can inspect what the strategy decided at every step, in the same shape, regardless of whether the broker is
# Alpaca or the in-notebook mock.


# %% papermill={"duration": 0.004285, "end_time": "2026-06-14T17:08:08.949254+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.944969+00:00", "status": "completed"}
def signals_to_frame(signals: list[dict]) -> pl.DataFrame:
    """Render the strategy's signal accumulator as a Polars frame for display."""
    if not signals:
        return pl.DataFrame(
            schema={
                "timestamp": pl.Datetime,
                "symbol": pl.String,
                "action": pl.String,
                "momentum": pl.Float64,
                "price": pl.Float64,
            }
        )
    return pl.DataFrame(signals)


# %% [markdown] papermill={"duration": 0.001419, "end_time": "2026-06-14T17:08:08.952147+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.950728+00:00", "status": "completed"}
# ## 4. Safe Broker Configuration
#
# Before going live, we wrap the broker with `SafeBroker` which provides:
# - Explicit PAPER and SHADOW execution modes
# - Position limits
# - Order rate limiting
# - Kill switch


# %% papermill={"duration": 0.00441, "end_time": "2026-06-14T17:08:08.957971+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.953561+00:00", "status": "completed"}
def create_safe_broker(underlying_broker, mode, state_scope):
    """Create a SafeBroker for an explicitly selected PAPER or SHADOW path."""
    if mode not in {"paper", "shadow"}:
        raise ValueError("mode must be 'paper' or 'shadow'")

    state_filenames = {
        ("paper", "session"): "risk_state_paper.json",
        ("shadow", "session"): "risk_state_shadow_qualification.json",
        ("shadow", "order_demo"): "risk_state_order_demo_shadow.json",
    }
    try:
        state_filename = state_filenames[(mode, state_scope)]
    except KeyError as error:
        raise ValueError(f"unsupported mode/state scope: {mode}/{state_scope}") from error
    risk_state_path = get_output_dir(25, "alpaca_paper_demo") / state_filename
    risk_config = LiveRiskConfig(
        shadow_mode=mode == "shadow",
        execution_mode=mode,
        max_position_value=50_000.0,
        max_order_value=10_000.0,
        max_orders_per_minute=10,
        dedup_window_seconds=1.0 if mode == "paper" else 0.0,
        # On a PAPER mismatch, inspect Alpaca positions/orders read-only, compare the
        # persisted snapshot, and reconcile only after finding the cause. Never delete
        # the state file merely to bypass the startup block.
        fail_on_reconciliation_mismatch=mode == "paper",
        state_file=str(risk_state_path),
    )

    safe_broker = SafeBroker(underlying_broker, risk_config)

    print("\n" + "=" * 60)
    if mode == "paper":
        print("RISK CONFIGURATION (ALPACA PAPER MODE)")
    else:
        print("RISK CONFIGURATION (SHADOW MODE)")
    print("=" * 60)
    if mode == "paper":
        print("   Execution Mode: PAPER (orders may be submitted to Alpaca paper account)")
    else:
        print("   Execution Mode: SHADOW (virtual orders only; no Alpaca orders)")
    print(f"   Max Position Value: ${risk_config.max_position_value:,.0f}")
    print(f"   Max Order Value: ${risk_config.max_order_value:,.0f}")
    print(f"   Rate Limit: {risk_config.max_orders_per_minute}/minute")
    print(f"   Risk State: {display_path(risk_state_path)}")

    return safe_broker, risk_config


# %% [markdown] papermill={"duration": 0.00141, "end_time": "2026-06-14T17:08:08.960841+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.959431+00:00", "status": "completed"}
# **Finding**: The risk-configuration printout makes the selected execution mode and exposure limits visible before the live
# feed starts emitting data.
#
# **Trading implication**: Broker wrappers should surface their active limits explicitly because a live
# rollout is only as safe as the controls that are actually enabled at runtime.
#
# %% [markdown] papermill={"duration": 0.001366, "end_time": "2026-06-14T17:08:08.963642+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.962276+00:00", "status": "completed"}
# ## 5. Real-Time Data Feed
#
# AlpacaDataFeed subscribes to real-time market data:
# - **bars**: OHLCV aggregates (1-minute default)
# - **quotes**: Bid/ask with sizes
# - **trades**: Individual trades
#
# Data sources:
# - **IEX**: Free, 15-min delayed for some symbols
# - **SIP**: Premium, real-time from all exchanges


# %% [markdown] papermill={"duration": 0.001371, "end_time": "2026-06-14T17:08:08.966442+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.965071+00:00", "status": "completed"}
# ### Simulated Path: Flat Dict Portfolio
#
# When credentials or the SDK are missing, the demo runs against a tiny in-notebook broker. The portfolio is a
# flat dict (`{"cash": float, "positions": {symbol: {qty, entry_price}}}`) rather than nested dataclasses so
# the simulated state can be read directly into a Polars frame for display alongside the live path.


# %% papermill={"duration": 0.00605, "end_time": "2026-06-14T17:08:08.973924+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.967874+00:00", "status": "completed"}
class MockBroker:
    """Local simulation broker and fail-closed SHADOW transport stub.

    The portfolio is a flat dict to keep the simulation state inspectable; it is
    also exposed through the asynchronous live-broker protocol so SafeBroker can
    run SHADOW without constructing an Alpaca trading client.
    """

    REF_PRICES = {"SPY": 600.0, "QQQ": 520.0, "IWM": 225.0}

    def __init__(self, initial_cash: float = 100_000.0):
        self.portfolio = {"cash": initial_cash, "positions": {}}
        self.order_log: list[dict] = []
        self.current_prices = dict(self.REF_PRICES)
        self.current_timestamp = datetime(2025, 1, 2, tzinfo=UTC)
        self._connected = False
        self.provider_submission_count = 0

    @property
    def positions(self) -> dict[str, Position]:
        """Return an independent typed view of the local simulation positions."""
        return {
            symbol: Position(
                asset=symbol,
                quantity=position["quantity"],
                entry_price=position["entry_price"],
                entry_time=self.current_timestamp,
                current_price=self.current_prices[symbol],
            )
            for symbol, position in self.portfolio["positions"].items()
        }

    @property
    def pending_orders(self) -> list[Order]:
        """The local broker never has provider-side pending orders."""
        return []

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def execution_capabilities(self) -> frozenset[ExecutionCapability]:
        return frozenset(
            {
                ExecutionCapability.LIMIT,
                ExecutionCapability.STOP,
                ExecutionCapability.STOP_LIMIT,
            }
        )

    async def connect(self) -> None:
        self._connected = True

    async def disconnect(self) -> None:
        self._connected = False

    async def is_connected_async(self) -> bool:
        return self._connected

    def assert_paper_trading(self) -> None:
        raise RuntimeError("MockBroker is SHADOW-only for live engine wiring")

    def assert_live_trading(self) -> None:
        raise RuntimeError("MockBroker does not support live execution")

    async def get_positions_async(self) -> dict[str, Position]:
        return self.positions

    async def get_pending_orders_async(self, asset: str | None = None) -> list[Order]:
        return []

    async def get_position_async(self, asset: str) -> Position | None:
        return self.positions.get(asset)

    async def get_account_value_async(self) -> float:
        position_value = sum(position.market_value for position in self.positions.values())
        return self.portfolio["cash"] + position_value

    async def get_cash_async(self) -> float:
        return self.portfolio["cash"]

    async def submit_order_async(
        self,
        asset: str,
        quantity: float,
        side: OrderSide | None = None,
        order_type: OrderType = OrderType.MARKET,
        limit_price: float | None = None,
        stop_price: float | None = None,
        **kwargs,
    ) -> Order:
        self.provider_submission_count += 1
        raise RuntimeError("SafeBroker SHADOW must not forward provider orders")

    async def cancel_order_async(self, order_id: str) -> bool:
        raise RuntimeError("SafeBroker SHADOW must not forward provider cancellations")

    async def replace_order_async(
        self,
        order_id: str,
        *,
        quantity: float | None = None,
        limit_price: float | None = None,
        stop_price: float | None = None,
    ) -> Order:
        raise RuntimeError("SafeBroker SHADOW must not forward provider replacements")

    async def close_position_async(self, asset: str) -> Order | None:
        raise RuntimeError("SafeBroker SHADOW must not forward provider closes")

    def update_market(self, timestamp: datetime, prices: dict[str, float]) -> None:
        """Update the simulated market used for subsequent fills."""
        self.current_timestamp = timestamp
        self.current_prices.update(prices)

    def get_position(self, symbol: str):
        pos = self.portfolio["positions"].get(symbol)
        if pos is None:
            return None

        class _PosView:
            quantity = pos["quantity"]

        return _PosView()

    def submit_order(self, asset: str, quantity: int, side=None, **kwargs) -> dict:
        price = self.current_prices[asset]
        if isinstance(side, OrderSide):
            side_name = side.value.upper()
        elif side is None:
            side_name = "BUY"
        else:
            side_name = str(getattr(side, "value", side)).upper()
        status = "rejected"
        if side is None or side == OrderSide.BUY:
            cost = quantity * price
            if cost <= self.portfolio["cash"]:
                self.portfolio["cash"] -= cost
                pos = self.portfolio["positions"].get(asset)
                if pos is None:
                    self.portfolio["positions"][asset] = {
                        "quantity": quantity,
                        "entry_price": price,
                    }
                else:
                    total_qty = pos["quantity"] + quantity
                    avg = (pos["quantity"] * pos["entry_price"] + quantity * price) / total_qty
                    self.portfolio["positions"][asset] = {
                        "quantity": total_qty,
                        "entry_price": avg,
                    }
                status = "filled"
        elif side == OrderSide.SELL:
            pos = self.portfolio["positions"].get(asset)
            if pos is not None and pos["quantity"] >= quantity:
                self.portfolio["cash"] += quantity * price
                remaining = pos["quantity"] - quantity
                if remaining == 0:
                    del self.portfolio["positions"][asset]
                else:
                    self.portfolio["positions"][asset] = {
                        "quantity": remaining,
                        "entry_price": pos["entry_price"],
                    }
                status = "filled"
        else:
            status = "unsupported"
        order = {
            "order_id": f"SIM-{len(self.order_log) + 1}",
            "timestamp": self.current_timestamp,
            "symbol": asset,
            "side": side_name,
            "quantity": quantity,
            "price": price,
            "status": status,
        }
        self.order_log.append(order)
        return order


# %% [markdown] papermill={"duration": 0.001408, "end_time": "2026-06-14T17:08:08.976800+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.975392+00:00", "status": "completed"}
# ### Live Path: Engine Wiring
#
# The live path constructs `AlpacaDataFeed > SafeBroker > LiveEngine`. Each helper is small enough to inspect
# at a glance; `run_engine_for_duration` is the only async piece, and `display_engine_results` is purely
# read-only post-processing.


# %% papermill={"duration": 0.00409, "end_time": "2026-06-14T17:08:08.982331+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.978241+00:00", "status": "completed"}
def create_alpaca_engine(strategy, mode):
    """Build the IEX feed and an explicitly selected PAPER or SHADOW broker."""
    global broker
    mode = require_engine_mode(mode)
    if mode == "paper":
        if broker is None or not isinstance(broker, PaperReportingAlpacaBroker):
            raise RuntimeError("PAPER mode requires a PaperReportingAlpacaBroker")
    else:
        broker = MockBroker()
    safe_broker, _ = create_safe_broker(broker, mode, "session")
    feed = AlpacaDataFeed(
        api_key=ALPACA_API_KEY,
        secret_key=ALPACA_SECRET_KEY,
        symbols=SYMBOLS,
        data_type="bars",
        feed="iex",
        experimental=True,
    )
    engine = LiveEngine(strategy=strategy, broker=safe_broker, feed=feed)
    # Alpaca SDK retries aggressively under nest_asyncio; quiet the retry logs.
    for name in [
        "alpaca",
        "alpaca.data",
        "alpaca.data.live",
        "alpaca.data.live.websocket",
        "alpaca.trading.stream",
        "websockets",
    ]:
        logging.getLogger(name).setLevel(logging.CRITICAL)
    return engine, feed, broker, safe_broker


# %% papermill={"duration": 0.003989, "end_time": "2026-06-14T17:08:08.987792+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.983803+00:00", "status": "completed"}
async def run_engine_for_duration(engine, duration_s: int):
    """Connect the engine and let it stream for at most `duration_s` seconds."""
    await asyncio.wait_for(engine.connect(), timeout=10)
    try:
        await asyncio.wait_for(engine.run(), timeout=duration_s)
    except TimeoutError:
        print(f"Demo duration ({duration_s}s) reached")


# %% papermill={"duration": 0.004271, "end_time": "2026-06-14T17:08:08.993566+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.989295+00:00", "status": "completed"}
def display_engine_results(
    strategy, session_broker, safe_broker, feed, engine, mode, shadow_summary=None
):
    """Print engine stats and render the strategy's signal log as a Polars frame."""
    print("Engine stats:", {k: engine.stats[k] for k in list(engine.stats)[:6]})
    print("Feed stats: ", {k: feed.stats[k] for k in list(feed.stats)[:6]})

    if mode == "paper":
        print("Alpaca paper account value: unavailable (teardown REST query omitted)")
        print("Alpaca paper account cash: unavailable (teardown REST query omitted)")
        positions = session_broker.paper_positions
        if positions is None:
            error_name = session_broker.paper_positions_error or "not captured"
            print(
                "WARNING: Alpaca paper cached positions unavailable before broker cleanup "
                f"({error_name})"
            )
        else:
            print("Alpaca paper cached positions captured before broker cleanup:")
            for symbol, pos in positions.items():
                value = pos.quantity * (pos.current_price or pos.entry_price)
                print(
                    f"   {symbol}: {pos.quantity} shares @ ${pos.entry_price:.2f} = ${value:,.2f}"
                )
    elif shadow_summary is None:
        print("WARNING: SHADOW virtual portfolio summary unavailable")
    else:
        account_value, cash, positions = shadow_summary
        print(f"SHADOW virtual portfolio value: ${account_value:,.2f}")
        print(f"SHADOW virtual cash: ${cash:,.2f}")
        for symbol, pos in positions.items():
            value = pos.quantity * (pos.current_price or pos.entry_price)
            print(f"   {symbol}: {pos.quantity} shares @ ${pos.entry_price:.2f} = ${value:,.2f}")

    print(f"\nSignals: {len(strategy.signals)}")
    return signals_to_frame(strategy.signals)


# %% compliance: skip cell_size papermill={"duration": 0.00441, "end_time": "2026-06-14T17:08:08.999488+00:00", "exception": false, "start_time": "2026-06-14T17:08:08.995078+00:00", "status": "completed"}
async def capture_shadow_summary(safe_broker):
    """Capture public SafeBroker SHADOW state after engine cleanup."""
    return (
        await safe_broker.get_account_value_async(),
        await safe_broker.get_cash_async(),
        safe_broker.get_positions(),
    )


async def run_live_demo_with_feed(mode, duration_seconds):
    """Run the explicitly selected Alpaca or offline simulation path."""
    mode = require_engine_mode(mode)
    if not LIVE_FEED:
        print(f"LIVE_FEED={LIVE_FEED}: running the offline simulation")
        return await run_simulated_demo()
    if not HAS_CREDENTIALS:
        raise RuntimeError("LIVE_FEED requires the Alpaca SDK and IEX data credentials")

    if mode == "paper":
        print("LIVE TRADING DEMO (ALPACA PAPER MODE)")
    else:
        print("SHADOW QUALIFICATION MODE (ALPACA IEX DATA; NO PROVIDER ORDERS)")
    strategy = ETFMomentumStrategy(lookback=5, threshold=0.02, position_size=10)
    engine, feed, session_broker, safe_broker = create_alpaca_engine(strategy, mode)
    print(f"Starting {mode.upper()} engine for {duration_seconds}s; watching {', '.join(SYMBOLS)}")

    shadow_summary = None
    try:
        await run_engine_for_duration(engine, duration_seconds)
    except Exception as error:
        try:
            if mode == "shadow":
                shadow_summary = await capture_shadow_summary(safe_broker)
            display_engine_results(
                strategy,
                session_broker,
                safe_broker,
                feed,
                engine,
                mode,
                shadow_summary,
            )
        except Exception as reporting_error:
            error.add_note(
                f"end-of-session reporting also failed: {type(reporting_error).__name__}"
            )
        raise
    finally:
        feed.stop()
    if mode == "shadow":
        shadow_summary = await capture_shadow_summary(safe_broker)
    return (
        display_engine_results(
            strategy,
            session_broker,
            safe_broker,
            feed,
            engine,
            mode,
            shadow_summary,
        ),
        pl.DataFrame(),
    )


# %% papermill={"duration": 0.005144, "end_time": "2026-06-14T17:08:09.006117+00:00", "exception": false, "start_time": "2026-06-14T17:08:09.000973+00:00", "status": "completed"}
async def run_simulated_demo() -> tuple[pl.DataFrame, pl.DataFrame]:
    """Run the strategy against the flat-dict MockBroker; return the signal log."""
    print("SIMULATED DEMO (no live Alpaca feed)")
    strategy = ETFMomentumStrategy(lookback=3, threshold=0.01, position_size=10)
    mock_broker = MockBroker()
    strategy.on_start(mock_broker)

    set_global_seeds(SEED)
    base_prices = dict(MockBroker.REF_PRICES)
    start = datetime(2025, 1, 2, 14, 30, tzinfo=UTC)
    for step in range(SIMULATION_STEPS):
        timestamp = start + timedelta(minutes=step)
        data = {}
        for symbol in SYMBOLS:
            base_prices[symbol] *= 1 + np.random.normal(0.001, 0.01)
            data[symbol] = {
                "open": base_prices[symbol] * 0.999,
                "high": base_prices[symbol] * 1.002,
                "low": base_prices[symbol] * 0.998,
                "close": base_prices[symbol],
                "volume": int(np.random.randint(100000, 1000000)),
            }
        mock_broker.update_market(timestamp, {symbol: bar["close"] for symbol, bar in data.items()})
        strategy.on_data(timestamp, data, {}, mock_broker)
    strategy.on_end(mock_broker)

    print(f"Simulated cash: ${mock_broker.portfolio['cash']:,.2f}")
    for symbol, pos in mock_broker.portfolio["positions"].items():
        value = pos["quantity"] * pos["entry_price"]
        print(f"   {symbol}: {pos['quantity']} shares @ ${pos['entry_price']:.2f} = ${value:,.2f}")
    print(f"Signals: {len(strategy.signals)}; Orders: {len(mock_broker.order_log)}")

    signal_frame = signals_to_frame(strategy.signals)
    order_frame = (
        pl.DataFrame(mock_broker.order_log)
        if mock_broker.order_log
        else pl.DataFrame(
            schema={
                "order_id": pl.String,
                "timestamp": pl.Datetime,
                "symbol": pl.String,
                "side": pl.String,
                "quantity": pl.Int64,
                "price": pl.Float64,
                "status": pl.String,
            }
        )
    )
    return signal_frame, order_frame


# %% papermill={"duration": 0.008403, "end_time": "2026-06-14T17:08:09.016034+00:00", "exception": false, "start_time": "2026-06-14T17:08:09.007631+00:00", "status": "completed"}
# Run the demo through the shared async helper in notebooks and exported Python.
demo_signal_log, demo_order_log = run_async(
    run_live_demo_with_feed(ENGINE_MODE, DEMO_DURATION_SECONDS)
)
demo_signal_log

# %%
demo_order_log

# %% [markdown] papermill={"duration": 0.001627, "end_time": "2026-06-14T17:08:09.019309+00:00", "exception": false, "start_time": "2026-06-14T17:08:09.017682+00:00", "status": "completed"}
# **Finding**: The selected execution mode is explicit. The offline path exercises the same strategy interface
# with an inspectable broker, while the opt-in live path adds `SafeBroker` and Alpaca transport controls.
#
# **Trading implication**: Keeping the live and simulated paths structurally aligned makes it easier to
# detect true broker-side issues instead of debugging differences introduced by the demo environment.
#
# %% [markdown] papermill={"duration": 0.001518, "end_time": "2026-06-14T17:08:09.022425+00:00", "exception": false, "start_time": "2026-06-14T17:08:09.020907+00:00", "status": "completed"}
# ## 6. Order Type Demonstrations
#
# Alpaca supports various order types:
# - **MARKET**: Execute immediately at best available price
# - **LIMIT**: Execute at specified price or better
# - **STOP**: Trigger market order when price reaches stop
# - **STOP_LIMIT**: Trigger limit order when price reaches stop


# %% papermill={"duration": 0.005795, "end_time": "2026-06-14T17:08:09.029770+00:00", "exception": false, "start_time": "2026-06-14T17:08:09.023975+00:00", "status": "completed"}
async def demonstrate_order_types():
    """Demonstrate different order types (shadow mode)."""
    if not HAS_CREDENTIALS or broker is None:
        print("\nSkipping order demo - no credentials")
        return
    if not LIVE_FEED:
        # SafeBroker requires fresh market data for staleness checks; without the
        # live WebSocket feed there are no recent quotes, so submit_order_async
        # raises RiskLimitError. Skip the demo under headless papermill.
        print(f"\nLIVE_FEED={LIVE_FEED}: skipping order-type demo (needs live market data)")
        return

    print("\n" + "=" * 60)
    print("ORDER TYPE DEMONSTRATIONS (Shadow Mode)")
    print("=" * 60)

    safe_broker, _ = create_safe_broker(broker, "shadow", "order_demo")

    # Market order
    print("\n1. MARKET ORDER")
    order = await safe_broker.submit_order_async("SPY", 10, side=OrderSide.BUY)
    print(f"   Order ID: {order.order_id}")
    print("   Type: MARKET")
    print(f"   Status: {order.status.value}")

    # Limit order
    print("\n2. LIMIT ORDER")
    order = await safe_broker.submit_order_async(
        "QQQ", 5, side=OrderSide.BUY, order_type=OrderType.LIMIT, limit_price=500.00
    )
    print(f"   Order ID: {order.order_id}")
    print("   Type: LIMIT @ $500.00")
    print(f"   Status: {order.status.value}")

    # Stop order
    print("\n3. STOP ORDER")
    order = await safe_broker.submit_order_async(
        "IWM", 10, side=OrderSide.SELL, order_type=OrderType.STOP, stop_price=220.00
    )
    print(f"   Order ID: {order.order_id}")
    print("   Type: STOP @ $220.00")
    print(f"   Status: {order.status.value}")

    # Show virtual portfolio through the public SafeBroker interface.
    print("\nVirtual Portfolio After Orders:")
    print(f"   Cash: ${await safe_broker.get_cash_async():,.2f}")
    for symbol, pos in safe_broker.positions.items():
        print(f"   {symbol}: {pos.quantity} shares")


if RUN_EDUCATIONAL_ORDER_DEMO:
    run_async(demonstrate_order_types())
else:
    print("Educational SHADOW order demo disabled; set ML4T_RUN_EDUCATIONAL_ORDER_DEMO=1")

# %% [markdown] papermill={"duration": 0.001614, "end_time": "2026-06-14T17:08:09.032985+00:00", "exception": false, "start_time": "2026-06-14T17:08:09.031371+00:00", "status": "completed"}
# **Finding**: The optional live path defines shadow-mode examples for each order type. The default offline
# run skips those submissions explicitly because it has no Alpaca client, live feed, or `SafeBroker`.
#
# **Trading implication**: After credentials and a live paper feed are available, shadow-mode submission is
# the intermediate stage that tests routing, validation, and guardrails without creating exposure.
#
# %% [markdown] papermill={"duration": 0.001568, "end_time": "2026-06-14T17:08:09.036152+00:00", "exception": false, "start_time": "2026-06-14T17:08:09.034584+00:00", "status": "completed"}
# ## 7. Clean Shutdown
#
# The session should end with an explicit disconnect so the next run starts from a known broker state instead
# of inheriting stale subscriptions or session assumptions.


# %% papermill={"duration": 0.004124, "end_time": "2026-06-14T17:08:09.041848+00:00", "exception": false, "start_time": "2026-06-14T17:08:09.037724+00:00", "status": "completed"}
# Disconnect the selected session broker if engine cleanup has not already done so.
if broker is not None:
    run_async(broker.disconnect())
    if ENGINE_MODE == "paper":
        print("\nDisconnected from Alpaca PAPER")
    else:
        print("\nDisconnected local SHADOW broker")

# %% [markdown] papermill={"duration": 0.001576, "end_time": "2026-06-14T17:08:09.045032+00:00", "exception": false, "start_time": "2026-06-14T17:08:09.043456+00:00", "status": "completed"}
# ## Summary
#
# This notebook requires an explicit PAPER or SHADOW engine selection. Both live-feed paths use Alpaca IEX
# market data; only PAPER constructs an Alpaca trading/account client. The educational SHADOW order demo is
# a separate, explicit opt-in.
#
# 1. **Authentication**: API key/secret via environment variables
# 2. **Connection**: Paper trading account access
# 3. **Account Info**: Query equity, cash, positions
# 4. **Real-Time Feed**: Subscribe to bars/quotes/trades
# 5. **Safe Trading**: Use SafeBroker in Alpaca paper mode
# 6. **Order Types**: Market, limit, stop orders
#
# ### Alpaca vs IB Comparison
#
# | Feature | Alpaca | Interactive Brokers |
# |---------|--------|---------------------|
# | Minimum Balance | None | None (but higher margin reqs) |
# | Commissions | Free | $0-1 per trade |
# | Real-time Data | Free (IEX) | Paid subscription |
# | API Complexity | Simple REST | Complex socket protocol |
# | Crypto | Yes (24/7) | Limited |
# | Paper Trading | Yes | Yes |
#
# ### Next Steps
#
# 1. Run in shadow mode for 1-2 weeks
# 2. Verify signals match backtest expectations
# 3. Enable paper trading (`shadow_mode=False`)
# 4. Monitor for 2-4 weeks on paper
# 5. Gradually transition to live with small positions
#
# ### Crypto Trading
#
# See `05_alpaca_crypto_live_demo.py` for 24/7 crypto trading demonstration.

# %% papermill={"duration": 0.004178, "end_time": "2026-06-14T17:08:09.050777+00:00", "exception": false, "start_time": "2026-06-14T17:08:09.046599+00:00", "status": "completed"}
print("\n" + "=" * 60)
print(f"{ENGINE_MODE.upper()} SESSION COMPLETE")
print("=" * 60)
print(f"Symbols: {', '.join(SYMBOLS)}")
print(f"Paper Trading: {'YES' if ENGINE_MODE == 'paper' else 'NO'}")
shadow_mode_state = "ENABLED" if RUN_EDUCATIONAL_ORDER_DEMO else "DISABLED"
print(f"Educational Shadow Mode: {shadow_mode_state}")
execution_mode = (
    f"Alpaca IEX feed + {ENGINE_MODE.upper()} broker" if LIVE_FEED else "offline simulation"
)
print(f"Execution Mode: {execution_mode}")
print("The same ETFMomentumStrategy interface drives the selected execution path.")

# %% [markdown] papermill={"duration": 0.0016, "end_time": "2026-06-14T17:08:09.054057+00:00", "exception": false, "start_time": "2026-06-14T17:08:09.052457+00:00", "status": "completed"}
# ## Key Takeaways
#
# **Finding**: Alpaca provides a compact optional live-trading stack, while the default run demonstrates
# the shared strategy interface and offline broker without claiming that live controls executed.
#
# **Trading implication**: The portability of the strategy object matters more than the specific broker API.
# If strategy logic survives the move from backtest to shadow mode unchanged, the remaining work is mostly
# about operational controls.
#
# **Next**: Compare this flow with `03_ib_paper_trading_demo.py` and then use
# `05_alpaca_crypto_live_demo.py` for the 24/7 crypto variant of the same deployment pattern.
