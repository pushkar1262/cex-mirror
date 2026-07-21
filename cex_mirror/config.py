"""Configuration models.

One YAML file describes global settings plus a list of pairs. Each pair inherits
`defaults` and may override any field. This replaces the previous approach of one
Hummingbot script-config YAML (and one Docker container) per pair.

Field names and semantics are carried over verbatim from the original strategy
config (conf_binance_to_my_cex_orderbook_mirror_*.yml) so behaviour matches.
"""

from __future__ import annotations

import logging
import os
from decimal import Decimal
from pathlib import Path
from typing import List, Optional

import yaml
from pydantic import BaseModel, Field, field_validator

log = logging.getLogger("cex_mirror.config")


class MyCexSettings(BaseModel):
    # Raw JWT is read from this environment variable (never stored in the file).
    jwt_env: str = "MYCEX_JWT"
    order_service_url: str = "https://order-exchange.antiers.work"
    # Max concurrent in-flight REST requests to my_cex (shared across all pairs).
    max_concurrency: int = 20
    # Light retry on transient failures, mirrors the old API_MAX_RETRIES.
    max_retries: int = 2
    request_timeout: float = 10.0

    # Resolved at load time from `jwt_env`.
    jwt: Optional[str] = None


class PairDefaults(BaseModel):
    """Per-pair tunables. Names match the original strategy config exactly."""

    levels: int = 25
    max_order_amount: Decimal = Decimal("0")  # 0 = use exact source amount
    price_precision: str = "0.01"
    amount_tolerance: Decimal = Decimal("0.05")
    refresh_interval: float = 1.0
    mirror_market_orders: bool = True
    max_market_order_amount: Decimal = Decimal("0")  # 0 = use exact source amount
    min_seconds_between_market_orders: float = 0.0
    min_notional: Decimal = Decimal("5")

    @field_validator("max_order_amount", "amount_tolerance", "max_market_order_amount",
                     "min_notional", mode="before")
    @classmethod
    def _as_decimal(cls, v):
        return Decimal(str(v))


class PairConfig(PairDefaults):
    """A single pair to mirror. Inherits every tunable from PairDefaults."""

    source: str = Field(..., description="Trading pair on the source exchange, e.g. BTC-USDT")
    mycex: str = Field(..., description="Corresponding trading pair on my_cex, e.g. BTC-USDT")
    # Required per pair (no global default): the amount quantization step, e.g. "0.00001".
    amount_precision: str = Field(..., description="Amount quantization step for this pair, e.g. 0.00001")

    @property
    def source_symbol(self) -> str:
        """Binance symbol form: BTC-USDT -> BTCUSDT (lowercase used for WS streams)."""
        return self.source.replace("-", "").replace("/", "").upper()

    @property
    def mycex_symbol(self) -> str:
        """my_cex symbol form: the API rejects separators, so BTC-USDT -> BTCUSDT."""
        return self.mycex.replace("-", "").replace("/", "").upper()


class Config(BaseModel):
    mycex: MyCexSettings = MyCexSettings()
    defaults: PairDefaults = PairDefaults()
    pairs: List[PairConfig] = []
    # How often to print the status line, in seconds. 0 disables.
    status_interval: float = 30.0
    # Cancel all tracked resting orders on graceful shutdown.
    cancel_on_shutdown: bool = True
    # On startup, adopt/reconcile pre-existing open orders on my_cex (survives crashes).
    reconcile_on_startup: bool = True


def load_config(path: str | Path) -> Config:
    """Load YAML, apply defaults to every pair, and resolve the JWT from env."""
    path = Path(path)
    raw = yaml.safe_load(path.read_text()) or {}

    mycex = MyCexSettings(**raw.get("mycex", {}))
    defaults = PairDefaults(**raw.get("defaults", {}))

    # Merge global defaults under each pair's explicit fields.
    default_dict = defaults.model_dump()
    pairs: List[PairConfig] = []
    for entry in raw.get("pairs", []):
        # amount_precision has no global default; a pair without it is skipped, not fatal.
        if not entry.get("amount_precision"):
            log.warning(
                "Skipping pair %s: no 'amount_precision' set (required per pair, no default).",
                entry.get("mycex") or entry.get("source") or "<unknown>",
            )
            continue
        merged = {**default_dict, **entry}
        pairs.append(PairConfig(**merged))

    cfg = Config(
        mycex=mycex,
        defaults=defaults,
        pairs=pairs,
        status_interval=raw.get("status_interval", 30.0),
        cancel_on_shutdown=raw.get("cancel_on_shutdown", True),
        reconcile_on_startup=raw.get("reconcile_on_startup", True),
    )

    jwt = os.environ.get(cfg.mycex.jwt_env, "").strip()
    if not jwt:
        raise RuntimeError(
            f"my_cex JWT not found. Set the '{cfg.mycex.jwt_env}' environment variable "
            f"(or a .env file) to the raw JWT for the self-trading user."
        )
    cfg.mycex.jwt = jwt

    if not cfg.pairs:
        raise RuntimeError("No pairs configured. Add at least one entry under 'pairs:'.")

    return cfg
