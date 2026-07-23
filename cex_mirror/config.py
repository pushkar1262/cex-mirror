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
    # API paths (relative to order_service_url), configurable so a different order
    # service layout needs no code change. The orders path is used for POST (place),
    # DELETE /{id} (cancel), and GET ?status=pending&market= (open-orders).
    exchange_info_path: str = "/api/v1/exchange-info"
    orders_path: str = "/api/v1/orders"
    # Max concurrent in-flight REST requests to my_cex (shared across all pairs).
    max_concurrency: int = 20
    # Light retry on transient failures, mirrors the old API_MAX_RETRIES.
    max_retries: int = 2
    request_timeout: float = 10.0
    # If exchange-info is unreachable at startup, keep retrying every this-many seconds
    # (with capped backoff) instead of exiting — the mirror starts once it comes online.
    bootstrap_retry_interval: float = 10.0

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
    # How often (seconds) to retry pairs the order service didn't know yet, so they
    # auto-start once it registers them. 0 disables retrying (skip permanently).
    pending_retry_interval: float = 15.0

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

    # Starting with an empty `pairs:` list is fine: at boot the mirror discovers every
    # active market from the order service's exchange-info, and (if Kafka is enabled)
    # tracks lifecycle changes thereafter. config.yaml entries are optional overrides.
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


def pair_from_exchange_info(entry: dict, defaults: PairDefaults) -> Optional[PairConfig]:
    """Build a PairConfig from a my_cex exchange-info entry (the startup bootstrap
    source), for a market with no explicit config.yaml override.

    Maps the order service's own trading rules to our fields:
      price_precision (tick) <- TickSize
      amount_precision (step) <- StepSize
      min_notional          <- MinNotional (when positive)
    Everything else (levels, refresh_interval, mirror_market_orders, ...) comes from
    the global defaults. Returns None if the entry lacks usable tick/step.
    """
    symbol = str(entry.get("Symbol") or entry.get("symbol") or "").strip().upper()
    base = str(entry.get("BaseAsset") or entry.get("baseAsset") or "").strip().upper()
    quote = str(entry.get("QuoteAsset") or entry.get("quoteAsset") or "").strip().upper()
    if not (base and quote) and symbol:
        # Fall back to splitting the symbol on the known quote if assets are absent.
        for q in ("USDT", "USDC", "BTC", "ETH", "BUSD"):
            if symbol.endswith(q) and len(symbol) > len(q):
                base, quote = symbol[: -len(q)], q
                break
    if not base or not quote:
        log.warning("exchange-info entry %r missing base/quote; skipping", symbol or entry)
        return None

    tick = str(entry.get("TickSize") or entry.get("tickSize") or "").strip()
    step = str(entry.get("StepSize") or entry.get("stepSize") or "").strip()
    if not tick or Decimal(tick) <= 0:
        log.warning("market %s has non-positive TickSize %r; skipping", symbol, tick)
        return None
    if not step or Decimal(step) <= 0:
        log.warning("market %s has non-positive StepSize %r; skipping", symbol, step)
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
    min_notional = str(entry.get("MinNotional") or entry.get("minNotional") or "").strip()
    if min_notional and Decimal(min_notional) > 0:
        merged["min_notional"] = Decimal(min_notional)

    return PairConfig(**merged)


def exchange_info_is_active(entry: dict) -> bool:
    """Whether an exchange-info entry is an active/tradeable market."""
    status = str(entry.get("Status") or entry.get("status") or "").strip().upper()
    # Empty status treated as active (lenient on field shape); else must be TRADING.
    return status in ("", "TRADING")


def symbol_norm(sym) -> str:
    """No-separator upper form, e.g. 'BTC-USDT' -> 'BTCUSDT'."""
    return str(sym or "").replace("-", "").replace("/", "").upper()


def _ruamel():
    """A round-trip YAML handler that preserves comments, indentation, and quoting."""
    from ruamel.yaml import YAML  # imported lazily; only needed when persisting

    y = YAML()
    y.preserve_quotes = True
    # Match the hand-written config.yaml layout: list items indented under their key
    # (two-space block sequence with the dash offset two spaces in).
    y.indent(mapping=2, sequence=4, offset=2)
    return y


def append_pair_to_yaml(path: str | Path, pair: PairConfig) -> None:
    """Persist a runtime-added pair into config.yaml so a restart resumes it.

    Uses ruamel.yaml round-trip so existing comments and formatting are preserved.
    Idempotent: a pair whose `mycex` already appears under `pairs:` is left untouched.
    """
    path = Path(path)
    y = _ruamel()
    with path.open("r") as f:
        data = y.load(f) or {}

    existing = data.get("pairs")
    if existing is None:
        from ruamel.yaml.comments import CommentedSeq

        existing = CommentedSeq()
        data["pairs"] = existing

    if any(symbol_norm(e.get("mycex") or e.get("source")) == pair.mycex_symbol for e in existing):
        return

    from ruamel.yaml.comments import CommentedMap
    from ruamel.yaml.scalarstring import DoubleQuotedScalarString as dq

    entry = CommentedMap()
    entry["source"] = pair.source
    entry["mycex"] = pair.mycex
    entry["levels"] = pair.levels
    # Quote the precision steps (double-quoted, matching the hand-written entries) so
    # YAML keeps them as strings — e.g. "0.001", not the float 0.001.
    entry["price_precision"] = dq(pair.price_precision)
    entry["amount_precision"] = dq(pair.amount_precision)
    entry["refresh_interval"] = pair.refresh_interval
    # Only serialise a cap when one is actually set (0 = "use exact source amount").
    if pair.max_order_amount > 0:
        entry["max_order_amount"] = str(pair.max_order_amount)

    existing.append(entry)
    # Blank line before the new entry, matching the spacing between hand-written pairs.
    if len(existing) > 1:
        existing.yaml_set_comment_before_after_key(len(existing) - 1, before="\n")
    with path.open("w") as f:
        y.dump(data, f)
    log.info("Persisted pair %s to %s", pair.mycex, path)


def remove_pair_from_yaml(path: str | Path, mycex_symbol: str) -> bool:
    """Drop any pair whose symbol matches `mycex_symbol` (no-separator form, e.g.
    TICSUSDT) from config.yaml so a restart does not resurrect a delisted market.

    Uses ruamel.yaml round-trip so remaining comments/formatting are preserved.
    Returns True if a pair was removed.
    """
    path = Path(path)
    y = _ruamel()
    with path.open("r") as f:
        data = y.load(f) or {}

    existing = data.get("pairs") or []
    target = symbol_norm(mycex_symbol)
    # Remove in place (highest index first) so ruamel keeps surrounding structure.
    to_remove = [
        i for i, e in enumerate(existing)
        if symbol_norm(e.get("mycex") or e.get("source")) == target
    ]
    if not to_remove:
        return False
    for i in reversed(to_remove):
        del existing[i]
    with path.open("w") as f:
        y.dump(data, f)
    log.info("Removed pair %s from %s", mycex_symbol, path)
    return True
