"""Entrypoint: load config, wire the shared feed + client, run one MirrorEngine per
pair in a single asyncio event loop.

Usage:
    python -m cex_mirror [path/to/config.yaml]      (default: ./config.yaml)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from typing import Dict, List

from .binance_feed import BinanceFeed
from .config import (
    Config,
    PairConfig,
    exchange_info_is_active,
    load_config,
    pair_from_exchange_info,
)
from .kafka_consumer import MarketLifecycleConsumer
from .mirror_engine import MirrorEngine
from .mycex_client import MyCexClient
from .pair_manager import PairManager
from .status import format_status

log = logging.getLogger("cex_mirror")


def _setup_logging() -> None:
    level = os.environ.get("LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # aiokafka's own INFO logs (subscription state, group coordinator join/sync/leave,
    # partition assignments) are noisy connection housekeeping — quiet them to WARNING
    # so only our own "connecting/connected/event received" lines and real problems show.
    # Respects LOG_LEVEL=DEBUG: if you're debugging, keep aiokafka's chatter too.
    if getattr(logging, level, logging.INFO) > logging.DEBUG:
        logging.getLogger("aiokafka").setLevel(logging.WARNING)


async def _reconcile_loop(engine: MirrorEngine, stop: asyncio.Event) -> None:
    interval = engine.pair.refresh_interval
    while not stop.is_set():
        try:
            await engine.reconcile()
        except Exception:
            log.exception("[%s] reconcile error", engine.pair.mycex)
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass


async def _status_loop(manager: PairManager, stop: asyncio.Event, interval: float) -> None:
    if interval <= 0:
        return
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            print(format_status(manager.engines), flush=True)


async def _pending_retry_loop(manager: PairManager, stop: asyncio.Event, interval: float) -> None:
    """Periodically retry pairs the order service didn't know yet (auto-start once
    it registers them). Only relevant with Kafka; harmless if nothing is pending."""
    if interval <= 0:
        return
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            try:
                await manager.retry_pending()
            except Exception:
                log.exception("pending-pair retry error")


def _bootstrap_pairs(cfg: Config, exchange_info: list) -> list[PairConfig]:
    """The set of pairs to start at boot: every ACTIVE market on the order service,
    plus any explicitly-configured pair not (yet) listed there.

    A market with a matching entry in config.yaml uses that entry (hand-tuned levels,
    caps, precision overrides); a market with no config entry is built from the order
    service's own trading rules (tick/step/min-notional) over the global defaults.
    Config pairs absent from exchange-info are still included — add_pair will gate them
    on order-service registration and queue them until they appear.
    """
    from .config import symbol_norm  # local: normaliser lives with the config models

    overrides = {p.mycex_symbol: p for p in cfg.pairs}
    used_overrides: set[str] = set()
    pairs: list[PairConfig] = []

    for entry in exchange_info:
        symbol = symbol_norm(entry.get("Symbol") or entry.get("symbol"))
        if not symbol:
            continue
        if not exchange_info_is_active(entry):
            log.info("Skipping inactive market %s (status=%s)", symbol, entry.get("Status"))
            continue
        if symbol in overrides:
            pairs.append(overrides[symbol])
            used_overrides.add(symbol)
        else:
            p = pair_from_exchange_info(entry, cfg.defaults)
            if p is not None:
                pairs.append(p)

    # Include configured pairs the order service didn't list (queued for retry by add_pair).
    for sym, p in overrides.items():
        if sym not in used_overrides:
            log.info("Configured pair %s not in exchange-info; will queue until registered", sym)
            pairs.append(p)

    return pairs


async def _bootstrap_loop(
    cfg: Config, client: MyCexClient, manager: PairManager, stop: asyncio.Event
) -> None:
    """Fetch exchange-info and start every active market. If the order service is
    unreachable, keep retrying (capped backoff) instead of exiting — so a down service
    at boot only delays mirroring, never prevents startup. Runs once it succeeds."""
    interval = cfg.mycex.bootstrap_retry_interval
    backoff = interval
    while not stop.is_set():
        try:
            info = await client.exchange_info()
        except Exception as e:
            log.warning(
                "exchange-info unreachable (%s); retrying in %.0fs (order service may be down)",
                e, backoff,
            )
            try:
                await asyncio.wait_for(stop.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, 60.0)
            continue

        log.info("Connected to my_cex; %d symbol(s) reported.", len(info))
        bootstrap_pairs = _bootstrap_pairs(cfg, info)
        if bootstrap_pairs:
            log.info("Bootstrapping %d active market(s) from exchange-info.", len(bootstrap_pairs))
            await asyncio.gather(
                *(manager.add_pair(p, persist=False) for p in bootstrap_pairs),
                return_exceptions=True,
            )
        else:
            log.info("No active markets to bootstrap yet.")
        return  # done; lifecycle changes handled by Kafka + pending-retry from here


async def run(cfg: Config, config_path: str) -> None:
    stop = asyncio.Event()

    # Map Binance symbol -> engine, for routing trade callbacks. Shared with the
    # PairManager so dynamically-added pairs receive trade-tape events too.
    engines_by_symbol: Dict[str, MirrorEngine] = {}

    async def trade_callback(symbol: str, price: str, qty: str, is_buyer_maker: bool) -> None:
        eng = engines_by_symbol.get(symbol.upper())
        if eng is not None:
            try:
                await eng.on_public_trade(price, qty, is_buyer_maker)
            except Exception:
                log.exception("[%s] trade-tape error", eng.pair.mycex)

    async with MyCexClient(
        cfg.mycex.order_service_url,
        cfg.mycex.jwt,
        exchange_info_path=cfg.mycex.exchange_info_path,
        orders_path=cfg.mycex.orders_path,
        max_concurrency=cfg.mycex.max_concurrency,
        max_retries=cfg.mycex.max_retries,
        request_timeout=cfg.mycex.request_timeout,
    ) as client:
        # Trades are mirrored whenever the defaults (or any configured pair) enable it.
        # We can't yet know the bootstrap set (exchange-info may be down and retried in
        # the background), so wire the callback on the broader condition — an unmatched
        # callback is a harmless no-op.
        any_market = cfg.defaults.mirror_market_orders or any(
            p.mirror_market_orders for p in cfg.pairs
        )
        # Feed starts empty; every pair (startup or Kafka) subscribes dynamically via add_pair.
        feed = BinanceFeed([], trade_callback if any_market else None)
        await feed.start()

        # Factory shared by static + dynamic pairs so their reconcile loops are identical.
        def start_reconcile_loop(engine: MirrorEngine) -> asyncio.Task:
            return asyncio.create_task(_reconcile_loop(engine, stop))

        manager = PairManager(
            cfg, client, feed, engines_by_symbol, start_reconcile_loop, config_path
        )

        # Signal handling for graceful shutdown.
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:
                pass  # e.g. Windows

        aux_tasks = [asyncio.create_task(_status_loop(manager, stop, cfg.status_interval))]

        # Bootstrap active markets from exchange-info in the background: if the order
        # service is momentarily down, keep retrying instead of failing to start. Kafka
        # (below) runs meanwhile, so lifecycle events are handled while we wait.
        aux_tasks.append(asyncio.create_task(_bootstrap_loop(cfg, client, manager, stop)))

        # Start the market-lifecycle consumer so admin-created pairs auto-start.
        consumer: MarketLifecycleConsumer | None = None
        if cfg.kafka.enabled:
            consumer = MarketLifecycleConsumer(cfg.kafka, manager.handle_market_event)
            # Fail fast: kafka.enabled means the market-lifecycle consumer is load-bearing
            # (it's how pairs are disabled/delisted). Silently continuing without it lets
            # the mirror keep quoting a disabled market — so surface the failure and exit
            # rather than run half-configured. Set kafka.enabled: false to run without it.
            try:
                await consumer.start()
            except Exception as e:
                log.error(
                    "Kafka consumer failed to start and kafka.enabled is true; aborting. "
                    "Fix the broker/dependencies, or set kafka.enabled: false to run "
                    "without live pair lifecycle. Cause: %s",
                    e,
                )
                raise
            # Retry pairs the order service didn't know yet (only meaningful with Kafka).
            aux_tasks.append(asyncio.create_task(
                _pending_retry_loop(manager, stop, cfg.kafka.pending_retry_interval)
            ))

        log.info(
            "Mirror started%s. Bootstrapping markets from exchange-info. Ctrl-C to stop.",
            " with live add-a-pair via Kafka" if consumer else "",
        )
        await stop.wait()
        log.info("Shutting down...")

        if consumer is not None:
            await consumer.stop()

        for t in aux_tasks:
            t.cancel()
        for t in manager.tasks:
            t.cancel()
        await asyncio.gather(*aux_tasks, *manager.tasks, return_exceptions=True)
        await feed.stop()

        if cfg.cancel_on_shutdown:
            log.info("Cancelling all resting orders...")
            results = await asyncio.gather(*(e.cancel_all() for e in manager.engines))
            log.info("Cancelled %d resting order(s).", sum(results))


def main() -> None:
    _setup_logging()
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    parser = argparse.ArgumentParser(description="Binance -> my_cex order book mirror")
    parser.add_argument("config", nargs="?", default="config.yaml", help="Path to config.yaml")
    args = parser.parse_args()

    try:
        cfg = load_config(args.config)
    except Exception as e:
        log.error("Config error: %s", e)
        sys.exit(1)

    try:
        asyncio.run(run(cfg, args.config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
