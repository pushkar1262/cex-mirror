"""Decimal quantization + level filtering.

Ported verbatim from the strategy's _read_levels / _on_public_trade rounding:
  - price: ROUND_DOWN to price_precision tick
  - amount: ROUND_HALF_UP to amount_precision step
  - optional cap via max_order_amount (0 = no cap)
  - drop any level with price*amount < min_notional
"""

from __future__ import annotations

from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from typing import List, Optional, Tuple

Level = Tuple[Decimal, Decimal]  # (price, amount)


def quantize_price(price: Decimal, tick: Decimal) -> Decimal:
    return price.quantize(tick, rounding=ROUND_DOWN)


def quantize_amount(amount: Decimal, step: Decimal) -> Decimal:
    return amount.quantize(step, rounding=ROUND_HALF_UP)


def prepare_levels(
    raw_levels: List[Tuple[str, str]],
    *,
    levels: int,
    price_tick: Decimal,
    amount_step: Decimal,
    max_order_amount: Decimal,
    min_notional: Decimal,
) -> List[Level]:
    """Convert raw [(price_str, amount_str), ...] from the source book into the
    quantized, capped, notional-filtered target levels to mirror."""
    result: List[Level] = []
    for price_s, amount_s in raw_levels:
        if len(result) >= levels:
            break
        price = quantize_price(Decimal(str(price_s)), price_tick)
        amount = quantize_amount(Decimal(str(amount_s)), amount_step)
        if max_order_amount > 0:
            amount = min(amount, max_order_amount)
        if price <= 0 or amount <= 0:
            continue
        if min_notional > 0 and price * amount < min_notional:
            continue
        result.append((price, amount))
    return result


def prepare_market_amount(
    price_s: str,
    amount_s: str,
    *,
    price_tick: Decimal,
    amount_step: Decimal,
    max_market_order_amount: Decimal,
    min_notional: Decimal,
) -> Optional[Level]:
    """Quantize a public trade into a (price, amount) market order, or None if filtered."""
    price = quantize_price(Decimal(str(price_s)), price_tick)
    amount = quantize_amount(Decimal(str(amount_s)), amount_step)
    if max_market_order_amount > 0:
        amount = min(amount, max_market_order_amount)
    if amount <= 0:
        return None
    if min_notional > 0 and price * amount < min_notional:
        return None
    return price, amount
