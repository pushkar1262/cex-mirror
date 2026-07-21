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
from .mirror_engine import MirrorEngine
from .mycex_client import MyCexClient
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


async def _status_loop(engines: List[MirrorEngine], stop: asyncio.Event, interval: float) -> None:
    if interval <= 0:
        return
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except asyncio.TimeoutError:
            print(format_status(engines), flush=True)


async def run(cfg: Config) -> None:
    stop = asyncio.Event()

    # Map Binance symbol -> engine, for routing trade callbacks.
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

        symbols = [p.source_symbol for p in cfg.pairs]
        any_market = any(p.mirror_market_orders for p in cfg.pairs)
        feed = BinanceFeed(symbols, trade_callback if any_market else None)

        engines: List[MirrorEngine] = []
        for p in cfg.pairs:
            eng = MirrorEngine(p, client, feed)
            engines.append(eng)
            engines_by_symbol[p.source_symbol] = eng

        # Startup recovery: adopt any pre-existing open orders (crash resilience).
        if cfg.reconcile_on_startup:
            await asyncio.gather(*(e.adopt_open_orders() for e in engines))

        await feed.start()

        # Signal handling for graceful shutdown.
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop.set)
            except NotImplementedError:
                pass  # e.g. Windows

        tasks = [asyncio.create_task(_reconcile_loop(e, stop)) for e in engines]
        tasks.append(asyncio.create_task(_status_loop(engines, stop, cfg.status_interval)))

        log.info("Mirror running for %d pair(s). Ctrl-C to stop.", len(engines))
        await stop.wait()
        log.info("Shutting down...")

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await feed.stop()

        if cfg.cancel_on_shutdown:
            log.info("Cancelling all resting orders...")
            results = await asyncio.gather(*(e.cancel_all() for e in engines))
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
        asyncio.run(run(cfg))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
