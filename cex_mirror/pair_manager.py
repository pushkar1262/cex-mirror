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
from .mycex_client import MyCexClient, MyCexError

log = logging.getLogger("cex_mirror.pairs")

# Started per-engine reconcile loop: (engine, stop_event) -> Task
ReconcileLoopFactory = Callable[[MirrorEngine], "asyncio.Task"]


def _market_symbol(event: dict) -> str:
    """No-separator market symbol for an event, e.g. TICSUSDT.

    Prefers base+quote from the `market` object; falls back to `market_id`
    (which delist events carry directly). Empty string if neither is present.
    """
    market = event.get("market") or {}
    base = str(market.get("base_currency_id") or "").strip().upper()
    quote = str(market.get("quote_currency_id") or "").strip().upper()
    if base and quote:
        return f"{base}{quote}"
    return str(event.get("market_id") or "").strip().upper()


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
        # Pairs the order service didn't know yet — retried periodically (source_symbol
        # -> PairConfig). Avoids order-placement spam for markets my_cex hasn't registered.
        self._pending: Dict[str, PairConfig] = {}
        self._lock = asyncio.Lock()

    @property
    def engines(self) -> List[MirrorEngine]:
        return self._engines

    @property
    def tasks(self) -> List[asyncio.Task]:
        return self._tasks

    # ------------------------------------------------------------------
    # Dynamic add (startup-configured and Kafka-driven)
    # ------------------------------------------------------------------

    async def handle_market_event(self, event: dict) -> None:
        """Entry point for a `market-lifecycle` event.

        market.delisted -> stop mirroring the pair and forget it.
        created/updated/state_changed with state=enabled  -> start mirroring.
        created/updated/state_changed with state=disabled -> equivalent to a delist:
            if the pair is currently running, tear it down and forget it.
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

        # `state` is a binary flag: "enabled" | "disabled". "disabled" is equivalent to a
        # delist — stop mirroring the pair and forget it. An unrecognised value is left
        # alone (logged) rather than risk tearing down a live pair on a bad message.
        if state == "disabled":
            market_id = _market_symbol(event)
            if not market_id:
                log.info("market event %s is disabled but has no resolvable symbol; ignoring",
                         event.get("event_id"))
                return
            log.info("market %s is disabled; stopping mirror (equivalent to delist)", market_id)
            await self.remove_pair(market_id)
            return

        if state and state != "enabled":
            log.warning(
                "market event %s has unrecognised state %r (expected enabled|disabled); ignoring",
                event.get("event_id"), state,
            )
            return

        pair = pair_from_market_event(event, self._cfg.defaults)
        if pair is None:
            return  # already logged why

        await self.add_pair(pair)

    async def add_pair(self, pair: PairConfig, *, persist: bool = True) -> bool:
        """Start mirroring `pair`. Returns True if newly started.

        Idempotent per market symbol. Validates the source symbol on Binance and that
        the order service knows the market (unknown -> queued for retry, no order spam).
        Existing engines are never touched. Used for both startup-configured pairs
        (persist=False, they're already in config.yaml) and Kafka-added pairs
        (persist=True, appended to config.yaml).
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

            # Order service must know the market, else every placed order 400s
            # ("unknown market") and floods the log. If unknown (or exchange-info is
            # momentarily unreachable), queue it and retry later instead of starting.
            newly_pending = pair.source_symbol not in self._pending
            try:
                known = await self._client.symbol_known(pair.mycex_symbol)
            except MyCexError as e:
                self._pending[pair.source_symbol] = pair
                if newly_pending:
                    log.warning(
                        "Could not verify %s on order service (%s); queued, will retry",
                        pair.mycex_symbol, e,
                    )
                return False
            if not known:
                self._pending[pair.source_symbol] = pair
                if newly_pending:
                    log.info(
                        "Order service does not know %s yet; queued, will start when "
                        "it is registered (no orders placed meanwhile)",
                        pair.mycex_symbol,
                    )
                return False

            # Known now — clear any pending entry and proceed.
            self._pending.pop(pair.source_symbol, None)

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

        # Persist outside the lock (file I/O); append is itself idempotent. Skipped for
        # startup-configured pairs, which are already present in config.yaml.
        if persist:
            try:
                append_pair_to_yaml(self._config_path, pair)
            except Exception:
                log.exception("Failed to persist pair %s to %s", pair.mycex, self._config_path)

        log.info("Mirror active for %s; %d pair(s) total", pair.mycex, len(self._engines))
        return True

    async def retry_pending(self) -> None:
        """Re-attempt pairs the order service didn't know yet; start any now-registered.

        Called periodically by __main__. add_pair() re-checks and re-queues on failure,
        so this just replays a snapshot of the pending set. Logs which pairs are still
        waiting on the order service, so it's clear the blocker is on that side.
        """
        if not self._pending:
            return
        waiting = sorted(p.mycex_symbol for p in self._pending.values())
        log.info(
            "Retrying %d pair(s) not yet active on the order service: %s",
            len(waiting), ", ".join(waiting),
        )
        for symbol, pair in list(self._pending.items()):
            # add_pair pops it from _pending on success; leaves it if still unknown.
            if symbol not in self._pending:
                continue  # started by a concurrent event
            await self.add_pair(pair)

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
            # A market that was only queued (order service didn't know it yet) should
            # stop being retried when it's delisted/disabled.
            was_pending = self._pending.pop(market_id, None) is not None

            engine = self._find_engine(market_id)
            if engine is None:
                if was_pending:
                    log.info("market %s was pending (never started); dropped from retry queue", market_id)
                else:
                    log.info("market.delisted %s: not currently mirroring; nothing to stop", market_id)
                # Still clean config in case it lingered from a prior run.
                self._safe_remove_from_config(market_id)
                return was_pending

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
