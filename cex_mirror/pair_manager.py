"""Runtime registry of mirror engines, supporting dynamic add and remove of pairs.

At startup __main__ builds the shared feed + client and registers the statically
configured pairs. When a `market-lifecycle` Kafka event arrives, `handle_market_event`
routes it:

  * created/updated/state_changed (enabled) -> add_pair: opens a dedicated Binance
    subscription, creates an engine, adopts pre-existing open orders, starts a reconcile
    loop, and persists the pair to config.yaml so a restart resumes it.
  * delisted -> remove_pair: stops that pair's reconcile loop, cancels its resting
    orders, unsubscribes its feed, and drops it from config.yaml.

Both operations leave **every other running pair untouched**.

Concurrency: all engine add/remove goes through one asyncio.Lock so overlapping events
for the same market can't create duplicate engines or race a teardown. Everything runs
on the one event loop the mirror already uses.
"""

from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Callable, Dict, List

from .binance_feed import BinanceFeed
from .config import (
    Config,
    PairConfig,
    append_pair_to_yaml,
    pair_from_market_event,
    remove_pair_from_yaml,
)
from .mirror_engine import MirrorEngine
from .mycex_client import MyCexClient

log = logging.getLogger("cex_mirror.pairs")

# Started per-engine reconcile loop: (engine, stop_event) -> Task
ReconcileLoopFactory = Callable[[MirrorEngine], "asyncio.Task"]


class PairManager:
    def __init__(
        self,
        cfg: Config,
        client: MyCexClient,
        feed: BinanceFeed,
        engines_by_symbol: Dict[str, MirrorEngine],
        reconcile_loop_factory: ReconcileLoopFactory,
        config_path: str | Path,
    ):
        self._cfg = cfg
        self._client = client
        self._feed = feed
        # Shared with __main__ so trade-tape callbacks route to newly-added engines too.
        self._engines_by_symbol = engines_by_symbol
        self._start_reconcile_loop = reconcile_loop_factory
        self._config_path = Path(config_path)

        self._engines: List[MirrorEngine] = []
        self._tasks: List[asyncio.Task] = []
        # Binance symbol -> its reconcile-loop task, so a single pair can be torn down.
        self._task_by_symbol: Dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()

    @property
    def engines(self) -> List[MirrorEngine]:
        return self._engines

    @property
    def tasks(self) -> List[asyncio.Task]:
        return self._tasks

    def register_static(self, engine: MirrorEngine, task: asyncio.Task) -> None:
        """Adopt an engine/loop that __main__ created for a startup-configured pair."""
        self._engines.append(engine)
        self._tasks.append(task)
        self._task_by_symbol[engine.source_symbol] = task
        self._engines_by_symbol[engine.source_symbol] = engine

    # ------------------------------------------------------------------
    # Kafka-driven dynamic add
    # ------------------------------------------------------------------

    async def handle_market_event(self, event: dict) -> None:
        """Entry point for a `market-lifecycle` event.

        market.delisted -> stop mirroring the pair and forget it.
        market.created/updated/state_changed with state=enabled -> start mirroring.
        Duplicate/known pairs are a no-op; pairs Binance does not list are skipped.
        """
        event_type = str(event.get("event_type", "")).strip().lower()

        # Delist carries no `market` object — just market_id (no-separator symbol).
        if event_type == "market.delisted":
            market_id = str(event.get("market_id") or "").strip().upper()
            if not market_id:
                log.warning("market.delisted event %s has no market_id; ignoring", event.get("event_id"))
                return
            await self.remove_pair(market_id)
            return

        market = event.get("market") or {}
        state = str(market.get("state", "")).strip().lower()
        pair = pair_from_market_event(event, self._cfg.defaults)
        if pair is None:
            return  # already logged why

        if state and state != "enabled":
            log.info(
                "market %s is %s (not enabled); not starting mirror for it yet",
                pair.mycex_symbol, state,
            )
            return

        await self.add_pair(pair)

    async def add_pair(self, pair: PairConfig) -> bool:
        """Start mirroring `pair` at runtime. Returns True if newly started.

        Idempotent per market symbol. Validates the source symbol on Binance first
        (skip if absent). Existing engines are never touched.
        """
        async with self._lock:
            if pair.source_symbol in self._engines_by_symbol:
                log.debug("pair %s already mirroring; ignoring", pair.mycex_symbol)
                return False

            # No source book on Binance => nothing to mirror. Skip (don't crash).
            if not await self._feed.symbol_exists_on_binance(pair.source_symbol):
                log.warning(
                    "Binance does not list %s (from market %s); skipping mirror",
                    pair.source_symbol, pair.mycex_symbol,
                )
                return False

            log.info("Starting mirror for new pair %s (source %s)", pair.mycex, pair.source_symbol)

            # Subscribe the feed (own connection, does not disturb running pairs).
            self._feed.add_symbol(pair.source_symbol)

            engine = MirrorEngine(pair, self._client, self._feed)
            # Register for trade-tape routing BEFORE starting the loop.
            self._engines_by_symbol[pair.source_symbol] = engine
            self._engines.append(engine)

            # Crash-recovery parity with startup: adopt any pre-existing open orders.
            if self._cfg.reconcile_on_startup:
                try:
                    await engine.adopt_open_orders()
                except Exception:
                    log.exception("[%s] adopt_open_orders failed on dynamic add", pair.mycex)

            task = self._start_reconcile_loop(engine)
            self._tasks.append(task)
            self._task_by_symbol[pair.source_symbol] = task

        # Persist outside the lock (file I/O); append is itself idempotent.
        try:
            append_pair_to_yaml(self._config_path, pair)
        except Exception:
            log.exception("Failed to persist pair %s to %s", pair.mycex, self._config_path)

        log.info("Mirror active for %s; %d pair(s) total", pair.mycex, len(self._engines))
        return True

    # ------------------------------------------------------------------
    # Kafka-driven dynamic remove (delist)
    # ------------------------------------------------------------------

    async def remove_pair(self, market_id: str) -> bool:
        """Stop mirroring a delisted market and forget it. Returns True if removed.

        Cancels the pair's reconcile loop, cancels all its resting orders, unsubscribes
        its Binance feed, and drops it from config.yaml so a restart won't resurrect it.
        Other pairs are untouched. Idempotent: unknown market_id is a logged no-op.
        """
        market_id = str(market_id or "").strip().upper()

        async with self._lock:
            # market_id is the no-separator symbol; match it against either side of
            # each engine's pair (source/mycex are identical for Kafka-added pairs).
            engine = self._find_engine(market_id)
            if engine is None:
                log.info("market.delisted %s: not currently mirroring; nothing to stop", market_id)
                # Still clean config in case it lingered from a prior run.
                self._safe_remove_from_config(market_id)
                return False

            source_symbol = engine.source_symbol
            log.info("Delisting %s: stopping mirror", engine.pair.mycex)

            # Stop this pair's reconcile loop (does not touch other loops).
            task = self._task_by_symbol.pop(source_symbol, None)
            if task is not None:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                if task in self._tasks:
                    self._tasks.remove(task)

            # Cancel all its resting orders so the delisted market's book is cleared.
            try:
                n = await engine.cancel_all()
                log.info("[%s] cancelled %d resting order(s) on delist", engine.pair.mycex, n)
            except Exception:
                log.exception("[%s] cancel_all failed during delist", engine.pair.mycex)

            # Drop from registries and unsubscribe the feed.
            self._engines_by_symbol.pop(source_symbol, None)
            if engine in self._engines:
                self._engines.remove(engine)
            try:
                await self._feed.remove_symbol(source_symbol)
            except Exception:
                log.exception("[%s] feed unsubscribe failed during delist", engine.pair.mycex)

        # Persist removal outside the lock (file I/O).
        self._safe_remove_from_config(market_id)
        log.info("Delisted %s; %d pair(s) remaining", market_id, len(self._engines))
        return True

    def _find_engine(self, market_id: str) -> MirrorEngine | None:
        for eng in self._engines:
            if market_id in (eng.pair.source_symbol, eng.pair.mycex_symbol):
                return eng
        return None

    def _safe_remove_from_config(self, market_id: str) -> None:
        try:
            remove_pair_from_yaml(self._config_path, market_id)
        except Exception:
            log.exception("Failed to remove pair %s from %s", market_id, self._config_path)
