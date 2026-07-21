"""In-memory resting-order state for one pair.

Because this process is the only order-placer for the self-trading user, we do not
need to read the my_cex order book. We remember exactly what we placed, keyed by the
quantized price on a given side, and reconcile against that. State is per-pair, so the
old "BTC orders showing up in the ETH market" cross-pair bug is structurally impossible.

No persistence: on a clean shutdown we cancel everything here; on startup we optionally
adopt any pre-existing open orders (crash recovery) via adopt().
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal
from typing import Dict, Iterator, Optional, Tuple


@dataclass
class RestingOrder:
    exchange_order_id: str
    side: str            # "buy" | "sell"
    price: Decimal
    amount: Decimal


@dataclass
class OrderTracker:
    # side -> {price -> RestingOrder}
    _by_side: Dict[str, Dict[Decimal, RestingOrder]] = field(
        default_factory=lambda: {"buy": {}, "sell": {}}
    )

    def get(self, side: str, price: Decimal) -> Optional[RestingOrder]:
        return self._by_side[side].get(price)

    def add(self, order: RestingOrder) -> None:
        self._by_side[order.side][order.price] = order

    def remove(self, side: str, price: Decimal) -> None:
        self._by_side[side].pop(price, None)

    def prices(self, side: str) -> set[Decimal]:
        return set(self._by_side[side].keys())

    def side_orders(self, side: str) -> Iterator[Tuple[Decimal, RestingOrder]]:
        # Copy so callers may mutate the tracker while iterating (cancel during reconcile).
        return list(self._by_side[side].items())

    def all_orders(self) -> Iterator[RestingOrder]:
        for side in ("buy", "sell"):
            yield from self._by_side[side].values()

    def count(self, side: str) -> int:
        return len(self._by_side[side])
