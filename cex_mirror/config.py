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


class KafkaSettings(BaseModel):
    """Consumer for the platform's `market-lifecycle` topic.

    When admins add/enable a pair in the admin panel, other services are notified
    over Kafka; this consumer lets the mirror auto-start (and persist) that pair so a
    freshly-created market immediately gets mirrored traffic. Disabled unless
    `enabled: true` and at least one bootstrap server is set.
    """

    enabled: bool = False
    # Comma-separated list also accepted; normalised to a list below.
    bootstrap_servers: List[str] = ["localhost:9092"]
    topic: str = "market-lifecycle"
    group_id: str = "cex-mirror"
    # Where to start when the group has no committed offset: "latest" (only new
    # events) or "earliest" (replay history). "latest" is right for live add-a-pair.
    auto_offset_reset: str = "latest"

    @field_validator("bootstrap_servers", mode="before")
    @classmethod
    def _split_servers(cls, v):
        if isinstance(v, str):
            return [s.strip() for s in v.split(",") if s.strip()]
        return v


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
    kafka: KafkaSettings = KafkaSettings()
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
    kafka = KafkaSettings(**raw.get("kafka", {}))
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
        kafka=kafka,
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

    # With Kafka enabled, starting empty is fine — pairs arrive as market-lifecycle
    # events. Otherwise we need at least one statically-configured pair.
    if not cfg.pairs and not cfg.kafka.enabled:
        raise RuntimeError(
            "No pairs configured. Add at least one entry under 'pairs:', "
            "or enable the Kafka consumer (kafka.enabled: true) to add pairs at runtime."
        )

    return cfg


def pair_from_market_event(event: dict, defaults: PairDefaults) -> Optional[PairConfig]:
    """Build a PairConfig from a `market-lifecycle` event's `market` object.

    Maps the event's real decimal steps to our quantization fields:
      price_precision (tick) <- market.tick_size
      amount_precision (step) <- market.step_size
    min_notional <- market.min_total when positive (else inherits the default).

    Returns None if the event lacks the fields needed to mirror the pair.
    """
    market = event.get("market") or {}
    base = str(market.get("base_currency_id") or "").strip().upper()
    quote = str(market.get("quote_currency_id") or "").strip().upper()
    if not base or not quote:
        log.warning("market event %s missing base/quote currency; ignoring", event.get("event_id"))
        return None

    tick = str(market.get("tick_size") or "").strip()
    step = str(market.get("step_size") or "").strip()
    # tick_size / step_size of "0" (or empty) are unusable as a quantization step.
    if not tick or Decimal(tick) <= 0:
        log.warning("market %s%s has non-positive tick_size %r; ignoring", base, quote, tick)
        return None
    if not step or Decimal(step) <= 0:
        log.warning("market %s%s has non-positive step_size %r; ignoring", base, quote, step)
        return None

    merged = defaults.model_dump()
    merged.update(
        {
            "source": f"{base}-{quote}",
            "mycex": f"{base}-{quote}",
            "price_precision": tick,
            "amount_precision": step,
        }
    )
    min_total = str(market.get("min_total") or "").strip()
    if min_total and Decimal(min_total) > 0:
        merged["min_notional"] = Decimal(min_total)

    return PairConfig(**merged)


def append_pair_to_yaml(path: str | Path, pair: PairConfig) -> None:
    """Persist a runtime-added pair into config.yaml so a restart resumes it.

    Full-file rewrite via PyYAML (comments are not preserved). Idempotent: a pair
    whose `mycex` already appears under `pairs:` is left untouched.
    """
    path = Path(path)
    raw = yaml.safe_load(path.read_text()) or {}
    existing = raw.get("pairs") or []

    def _norm(sym: str) -> str:
        return str(sym or "").replace("-", "").replace("/", "").upper()

    if any(_norm(e.get("mycex") or e.get("source")) == pair.mycex_symbol for e in existing):
        return

    entry = {
        "source": pair.source,
        "mycex": pair.mycex,
        "levels": pair.levels,
        "price_precision": pair.price_precision,
        "amount_precision": pair.amount_precision,
        "refresh_interval": pair.refresh_interval,
    }
    # Only serialise a cap when one is actually set (0 = "use exact source amount").
    if pair.max_order_amount > 0:
        entry["max_order_amount"] = str(pair.max_order_amount)

    existing.append(entry)
    raw["pairs"] = existing
    path.write_text(yaml.safe_dump(raw, sort_keys=False, default_flow_style=False))
    log.info("Persisted pair %s to %s", pair.mycex, path)


def remove_pair_from_yaml(path: str | Path, mycex_symbol: str) -> bool:
    """Drop any pair whose symbol matches `mycex_symbol` (no-separator form, e.g.
    TICSUSDT) from config.yaml so a restart does not resurrect a delisted market.

    Full-file rewrite via PyYAML (comments not preserved). Returns True if a pair
    was removed.
    """
    path = Path(path)
    raw = yaml.safe_load(path.read_text()) or {}
    existing = raw.get("pairs") or []

    def _norm(sym: str) -> str:
        return str(sym or "").replace("-", "").replace("/", "").upper()

    target = _norm(mycex_symbol)
    kept = [e for e in existing if _norm(e.get("mycex") or e.get("source")) != target]
    if len(kept) == len(existing):
        return False
    raw["pairs"] = kept
    path.write_text(yaml.safe_dump(raw, sort_keys=False, default_flow_style=False))
    log.info("Removed pair %s from %s", mycex_symbol, path)
    return True
