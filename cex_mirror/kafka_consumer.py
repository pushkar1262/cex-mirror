"""Kafka consumer for the platform's `market-lifecycle` topic.

When admins create/enable a trading pair in the admin panel, the platform emits an
event so downstream services react. This mirror listens so a freshly-created market
starts getting mirrored order-book traffic immediately (see PairManager).

Event shapes handled (schema_version 1):

  event_type: "market.created"        -> a new market exists
  event_type: "market.updated"        -> market params changed
  event_type: "market.state_changed"  -> enabled/disabled toggled
  event_type: "market.delisted"       -> market removed (no `market` object, just market_id)

The create/update/state_changed events carry a `market` object with base/quote currency,
tick/step sizes, min_total, and a `state` ("enabled" | "disabled"); a delist carries only
market_id. We hand every relevant event to a callback (PairManager.handle_market_event),
which does validation, dedup, add, and remove.

This module deliberately does no order logic; it just decodes messages and dispatches.
aiokafka is optional — importing this module without it installed fails only when the
consumer is actually started (Kafka disabled => never imported at runtime path).
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Awaitable, Callable

from .config import KafkaSettings

log = logging.getLogger("cex_mirror.kafka")

# event_type values we act on: add/update markets, plus delist (remove).
_HANDLED_EVENT_TYPES = {
    "market.created",
    "market.updated",
    "market.state_changed",
    "market.delisted",
}

# (raw_event_dict) -> awaitable; PairManager supplies this.
MarketEventCallback = Callable[[dict], Awaitable[None]]


class MarketLifecycleConsumer:
    def __init__(self, settings: KafkaSettings, on_event: MarketEventCallback):
        self._settings = settings
        self._on_event = on_event
        self._consumer = None  # aiokafka.AIOKafkaConsumer, lazily created
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        """Connect and begin consuming in a background task. Raises if aiokafka is
        missing or the broker is unreachable at startup."""
        try:
            from aiokafka import AIOKafkaConsumer
        except ImportError as e:  # pragma: no cover - depends on optional extra
            raise RuntimeError(
                "kafka.enabled is true but 'aiokafka' is not installed. "
                "Install it (pip install aiokafka) or set kafka.enabled: false."
            ) from e

        brokers = ",".join(self._settings.bootstrap_servers)
        log.info(
            "Connecting to Kafka: brokers=%s topic=%s group=%s (%s)...",
            brokers, self._settings.topic, self._settings.group_id,
            self._settings.auto_offset_reset,
        )
        self._consumer = AIOKafkaConsumer(
            self._settings.topic,
            bootstrap_servers=self._settings.bootstrap_servers,
            group_id=self._settings.group_id,
            auto_offset_reset=self._settings.auto_offset_reset,
            enable_auto_commit=True,
            value_deserializer=lambda b: b,  # decode ourselves; be tolerant of bad bytes
        )
        await self._consumer.start()
        log.info(
            "Connected to Kafka; consuming topic %r on brokers %s (group=%s). "
            "Waiting for market-lifecycle events...",
            self._settings.topic, brokers, self._settings.group_id,
        )
        self._task = asyncio.create_task(self._consume_loop())

    async def _consume_loop(self) -> None:
        assert self._consumer is not None
        # Poll in batches so a fetch/decode error on one batch (e.g. an unsupported
        # compression codec) is caught and retried instead of killing the consumer
        # for the rest of the process's life. Individual message handling errors are
        # already contained in _dispatch.
        backoff = 1.0
        while not self._stop.is_set():
            try:
                batches = await self._consumer.getmany(timeout_ms=1000)
                backoff = 1.0
                for _tp, messages in batches.items():
                    for msg in messages:
                        if self._stop.is_set():
                            return
                        await self._dispatch(msg.value)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                # Don't die: log and keep polling. A bad/uncompressible batch or a
                # transient broker hiccup must not permanently stop the consumer.
                log.error(
                    "Kafka poll error (%s: %s); retrying in %.1fs",
                    type(e).__name__, e, backoff,
                )
                try:
                    await asyncio.wait_for(self._stop.wait(), timeout=backoff)
                except asyncio.TimeoutError:
                    pass
                backoff = min(backoff * 2, 30.0)

    async def _dispatch(self, raw: bytes) -> None:
        try:
            event = json.loads(raw)
        except (ValueError, TypeError) as e:
            log.warning("Skipping malformed market-lifecycle message: %s", e)
            return

        event_type = str(event.get("event_type", ""))
        # Log every event that arrives on the topic, so it's visible that the consumer
        # is live and receiving — not just when something is acted on.
        log.info(
            "market-lifecycle event received: type=%s market=%s id=%s",
            event_type or "<none>",
            event.get("market_id", "<none>"),
            event.get("event_id", "<none>"),
        )
        if event_type not in _HANDLED_EVENT_TYPES:
            log.info("Ignoring event_type %r (not add/remove)", event_type)
            return

        try:
            await self._on_event(event)
        except Exception:
            # A bad single event must never take down the consumer.
            log.exception(
                "Error handling market event %s (%s)",
                event.get("event_id"),
                event.get("market_id"),
            )

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._consumer is not None:
            await self._consumer.stop()
            log.info("Kafka consumer stopped")
