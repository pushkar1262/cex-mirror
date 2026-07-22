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
from .config import Config, load_config
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
        max_concurrency=cfg.mycex.max_concurrency,
        max_retries=cfg.mycex.max_retries,
        request_timeout=cfg.mycex.request_timeout,
    ) as client:
        # Sanity check the JWT / connectivity via exchange-info (also warms trading rules).
        try:
            info = await client.exchange_info()
            log.info("Connected to my_cex; %d symbols reported.", len(info))
        except Exception as e:
            log.error("Could not reach my_cex exchange-info (check JWT / URL): %s", e)
            return

        # With Kafka enabled, a market that only mirrors limit orders can still later
        # be joined by one that mirrors trades; wire the trade callback if EITHER a
        # configured pair wants it or Kafka may add such a pair.
        any_market = any(p.mirror_market_orders for p in cfg.pairs) or (
            cfg.kafka.enabled and cfg.defaults.mirror_market_orders
        )
        symbols = [p.source_symbol for p in cfg.pairs]
        feed = BinanceFeed(symbols, trade_callback if any_market else None)
        await feed.start()

        # Factory shared by static + dynamic pairs so their reconcile loops are identical.
        def start_reconcile_loop(engine: MirrorEngine) -> asyncio.Task:
            return asyncio.create_task(_reconcile_loop(engine, stop))

        manager = PairManager(
            cfg, client, feed, engines_by_symbol, start_reconcile_loop, config_path
        )

        # Register statically-configured pairs (startup recovery, then reconcile loop).
        static_engines = [MirrorEngine(p, client, feed) for p in cfg.pairs]
        if cfg.reconcile_on_startup and static_engines:
            await asyncio.gather(*(e.adopt_open_orders() for e in static_engines))
        for eng in static_engines:
            manager.register_static(eng, start_reconcile_loop(eng))

        # Signal handling for graceful shutdown.
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:
                pass  # e.g. Windows

        status_task = asyncio.create_task(_status_loop(manager, stop, cfg.status_interval))

        # Start the market-lifecycle consumer so admin-created pairs auto-start.
        consumer: MarketLifecycleConsumer | None = None
        if cfg.kafka.enabled:
            consumer = MarketLifecycleConsumer(cfg.kafka, manager.handle_market_event)
            try:
                await consumer.start()
            except Exception as e:
                log.error("Kafka consumer failed to start (continuing without it): %s", e)
                consumer = None

        log.info(
            "Mirror running for %d pair(s)%s. Ctrl-C to stop.",
            len(manager.engines),
            " + live add-a-pair via Kafka" if consumer else "",
        )
        await stop.wait()
        log.info("Shutting down...")

        if consumer is not None:
            await consumer.stop()

        status_task.cancel()
        for t in manager.tasks:
            t.cancel()
        await asyncio.gather(status_task, *manager.tasks, return_exceptions=True)
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
