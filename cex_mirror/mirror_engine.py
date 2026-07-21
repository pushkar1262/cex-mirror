"""Per-pair mirror engine.

Reproduces the original strategy behaviour (binance_to_my_cex_orderbook_mirror.py):

  LIMIT mirroring  — every `refresh_interval`s, read the top `levels` of the source
    book, quantize/cap/notional-filter, then reconcile against our own tracked resting
    orders: cancel stale (price no longer targeted) or drifted (> amount_tolerance)
    orders; place orders for uncovered target prices.

  MARKET mirroring — for each public source trade, fire a same-side market order on
    my_cex, capped by max_market_order_amount and throttled by
    min_seconds_between_market_orders, skipping sub-min_notional trades.

All state is in memory and per-pair. Shares one MyCexClient across all engines.
"""

from __future__ import annotations

import logging
import time
from decimal import Decimal
from typing import Optional

from .binance_feed import BinanceFeed
from .config import PairConfig
from .mycex_client import MyCexClient
from .order_tracker import OrderTracker, RestingOrder
from .quantize import prepare_levels, prepare_market_amount

log = logging.getLogger("cex_mirror.engine")


class MirrorEngine:
    def __init__(
        self,
        pair: PairConfig,
        client: MyCexClient,
        feed: BinanceFeed,
    ):
        self.pair = pair
        self._client = client
        self._feed = feed
        self._tracker = OrderTracker()

        self._price_tick = Decimal(pair.price_precision)
        self._amount_step = Decimal(pair.amount_precision)

        # stats
        self.placed = 0
        self.cancelled = 0
        self.market_orders = 0
        self._last_market_ts = 0.0

    @property
    def source_symbol(self) -> str:
        return self.pair.source_symbol

    @property
    def tracker(self) -> OrderTracker:
        return self._tracker

    # ------------------------------------------------------------------
    # Startup recovery: adopt pre-existing open orders (crash resilience)
    # ------------------------------------------------------------------

    async def adopt_open_orders(self) -> int:
        """Pull open orders from my_cex and register them so the reconcile loop manages
        (keeps or cancels) them instead of leaving orphans after an ungraceful exit."""
        orders = await self._client.open_orders(self.pair.mycex_symbol)
        adopted = 0
        for o in orders:
            try:
                side = str(o.get("side", "")).lower()
                if side not in ("buy", "sell"):
                    continue
                oid = str(o.get("orderID") or o.get("orderId") or o.get("id") or "")
                if not oid:
                    continue
                price = Decimal(str(o.get("price", "0"))).quantize(self._price_tick)
                amount = Decimal(str(o.get("quantity", o.get("qty", "0"))))
                if price <= 0:
                    continue
                # If two adopted orders land on the same side+price bucket, cancel the extra.
                if self._tracker.get(side, price) is not None:
                    await self._cancel(RestingOrder(oid, side, price, amount))
                    continue
                self._tracker.add(RestingOrder(oid, side, price, amount))
                adopted += 1
            except Exception:
                log.exception("[%s] failed to adopt order %s", self.pair.mycex, o)
        if adopted:
            log.info("[%s] adopted %d pre-existing open order(s) on startup", self.pair.mycex, adopted)
        return adopted

    # ------------------------------------------------------------------
    # Limit reconcile
    # ------------------------------------------------------------------

    async def reconcile(self) -> None:
        book = self._feed.book(self.source_symbol)
        if not book.ready:
            return
        raw_bids = book.top_bids(self.pair.levels)
        raw_asks = book.top_asks(self.pair.levels)
        if not raw_bids and not raw_asks:
            return

        target_bids = prepare_levels(
            raw_bids,
            levels=self.pair.levels,
            price_tick=self._price_tick,
            amount_step=self._amount_step,
            max_order_amount=self.pair.max_order_amount,
            min_notional=self.pair.min_notional,
        )
        target_asks = prepare_levels(
            raw_asks,
            levels=self.pair.levels,
            price_tick=self._price_tick,
            amount_step=self._amount_step,
            max_order_amount=self.pair.max_order_amount,
            min_notional=self.pair.min_notional,
        )
        await self._reconcile_side("buy", target_bids)
        await self._reconcile_side("sell", target_asks)

    async def _reconcile_side(self, side: str, target_levels) -> None:
        target_map = {p: a for p, a in target_levels}

        # Cancel resting orders that are stale (price no longer targeted) or drifted.
        for price, order in self._tracker.side_orders(side):
            target_amount = target_map.get(price)
            if target_amount is None:
                await self._cancel(order)
            else:
                drift = abs(order.amount - target_amount) / target_amount if target_amount > 0 else Decimal(1)
                if drift > self.pair.amount_tolerance:
                    await self._cancel(order)

        # Place orders for uncovered target prices.
        for price, amount in target_levels:
            if self._tracker.get(side, price) is not None:
                continue
            await self._place_limit(side, price, amount)

    async def _place_limit(self, side: str, price: Decimal, amount: Decimal) -> None:
        oid = await self._client.place_order(
            pair=self.pair.mycex_symbol, side=side, order_type="limit", quantity=amount, price=price
        )
        if oid:
            self._tracker.add(RestingOrder(oid, side, price, amount))
            self.placed += 1

    async def _cancel(self, order: RestingOrder) -> None:
        # Optimistically drop from tracker; the reconcile loop will re-place if the
        # price is still a target and the cancel actually failed on the backend.
        self._tracker.remove(order.side, order.price)
        ok = await self._client.cancel_order(order.exchange_order_id)
        if ok:
            self.cancelled += 1

    # ------------------------------------------------------------------
    # Market (trade-tape) mirroring
    # ------------------------------------------------------------------

    async def on_public_trade(self, price_s: str, qty_s: str, is_buyer_maker: bool) -> None:
        if not self.pair.mirror_market_orders:
            return
        now = time.monotonic()
        if now - self._last_market_ts < self.pair.min_seconds_between_market_orders:
            return
        prepared = prepare_market_amount(
            price_s, qty_s,
            price_tick=self._price_tick,
            amount_step=self._amount_step,
            max_market_order_amount=self.pair.max_market_order_amount,
            min_notional=self.pair.min_notional,
        )
        if prepared is None:
            return
        _price, amount = prepared
        # is_buyer_maker True => the aggressor was a seller => mirror as SELL.
        side = "sell" if is_buyer_maker else "buy"
        oid = await self._client.place_order(
            pair=self.pair.mycex_symbol, side=side, order_type="market", quantity=amount
        )
        if oid:
            self._last_market_ts = now
            self.market_orders += 1

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    async def cancel_all(self) -> int:
        n = 0
        for order in list(self._tracker.all_orders()):
            if await self._client.cancel_order(order.exchange_order_id):
                n += 1
            self._tracker.remove(order.side, order.price)
        return n
