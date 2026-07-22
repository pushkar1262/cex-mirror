"""Async REST client for my_cex.

Talks only to the order service. Endpoints and payload shapes are taken directly
from the Hummingbot connector (my_cex_constants.py / my_cex_exchange.py):

  POST   /api/v1/orders                {pair, side, orderType, quantity, price?} -> {"orderID": "<id>"}
  DELETE /api/v1/orders/{orderID}
  GET    /api/v1/orders?status=pending&market=<pair>   -> open orders (for startup recovery)
  GET    /api/v1/exchange-info                         -> [{Symbol, Status, BaseAsset, ...}]

Auth is a single Bearer JWT valid for all pairs (self-trading user).
All order-placement / cancellation failures are logged (per the requirement) but
never crash the process.
"""

from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any, Dict, List, Optional

import aiohttp

log = logging.getLogger("cex_mirror.mycex")


class MyCexError(Exception):
    """Raised on a non-retryable my_cex API error (already logged by the caller path)."""


class MyCexClient:
    def __init__(
        self,
        base_url: str,
        jwt: str,
        *,
        max_concurrency: int = 20,
        max_retries: int = 2,
        request_timeout: float = 10.0,
    ):
        self._base = base_url.rstrip("/")
        self._jwt = jwt
        self._max_retries = max_retries
        self._sem = asyncio.Semaphore(max_concurrency)
        self._timeout = aiohttp.ClientTimeout(total=request_timeout)
        self._session: Optional[aiohttp.ClientSession] = None

    async def __aenter__(self) -> "MyCexClient":
        self._session = aiohttp.ClientSession(
            timeout=self._timeout,
            headers={"Authorization": f"Bearer {self._jwt}"},
        )
        return self

    async def __aexit__(self, *exc) -> None:
        if self._session is not None:
            await self._session.close()

    # ------------------------------------------------------------------
    # Low-level request with retry
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: Optional[dict] = None,
        params: Optional[dict] = None,
    ) -> Any:
        assert self._session is not None, "MyCexClient must be used as an async context manager"
        url = f"{self._base}{path}"
        last_exc: Optional[Exception] = None

        for attempt in range(1, self._max_retries + 1):
            try:
                async with self._sem:
                    async with self._session.request(method, url, json=json, params=params) as resp:
                        body_text = await resp.text()
                        if resp.status >= 400:
                            # Retry only on 5xx / 429; 4xx (bad order, min-notional, etc.) is terminal.
                            if resp.status in (429,) or 500 <= resp.status < 600:
                                last_exc = MyCexError(f"HTTP {resp.status}: {body_text}")
                                await asyncio.sleep(min(2 ** attempt * 0.1, 2.0))
                                continue
                            raise MyCexError(f"HTTP {resp.status}: {body_text}")
                        return self._parse_json(body_text)
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_exc = e
                await asyncio.sleep(min(2 ** attempt * 0.1, 2.0))

        raise MyCexError(f"{method} {path} failed after {self._max_retries} attempts: {last_exc}")

    @staticmethod
    def _parse_json(text: str) -> Any:
        import json as _json
        try:
            return _json.loads(text) if text else {}
        except ValueError:
            return {"raw": text}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def exchange_info(self) -> List[Dict[str, Any]]:
        data = await self._request("GET", "/api/v1/exchange-info")
        return data if isinstance(data, list) else data.get("symbols", [])

    async def symbol_known(self, symbol: str) -> bool:
        """Whether the order service knows this market (no-separator symbol, e.g.
        DOGEUSDT). Placing orders for an unknown market returns HTTP 400
        'unknown market', so a dynamically-added pair should check this first.

        Raises MyCexError if exchange-info can't be reached, so callers can retry
        later rather than treating a transient failure as 'not listed'."""
        symbol = symbol.upper()
        for s in await self.exchange_info():
            sym = str(s.get("Symbol") or s.get("symbol") or "").upper()
            if sym == symbol:
                status = str(s.get("Status") or s.get("status") or "").upper()
                # Empty status treated as known/tradeable (be lenient on field shape).
                return status in ("", "TRADING")
        return False

    async def place_order(
        self,
        *,
        pair: str,
        side: str,          # "buy" | "sell"
        order_type: str,    # "limit" | "market"
        quantity: Decimal,
        price: Optional[Decimal] = None,
    ) -> Optional[str]:
        """Place an order. Returns the exchange orderID, or None on failure (logged)."""
        body: Dict[str, Any] = {
            "pair": pair,
            "side": side,
            "orderType": order_type,
            "quantity": f"{quantity:f}",
        }
        if order_type == "limit":
            if price is None:
                log.error("[place] limit order for %s missing price; skipping", pair)
                return None
            body["price"] = f"{price:f}"
        try:
            result = await self._request("POST", "/api/v1/orders", json=body)
        except MyCexError as e:
            log.error(
                "[place] FAILED %s %s %s qty=%s price=%s -> %s",
                pair, side, order_type, f"{quantity:f}", (f"{price:f}" if price else "-"), e,
            )
            return None
        order_id = result.get("orderID") if isinstance(result, dict) else None
        if not order_id:
            log.error(
                "[place] no orderID in response for %s %s %s qty=%s price=%s -> %s",
                pair, side, order_type, f"{quantity:f}", (f"{price:f}" if price else "-"), result,
            )
            return None
        return str(order_id)

    async def cancel_order(self, exchange_order_id: str) -> bool:
        """Cancel by exchange orderID. Returns True on success, False on failure (logged)."""
        try:
            await self._request("DELETE", f"/api/v1/orders/{exchange_order_id}")
            return True
        except MyCexError as e:
            # "order not found" means it already filled/cancelled — not an error worth alarming on.
            if "not found" in str(e).lower():
                log.debug("[cancel] order %s already gone: %s", exchange_order_id, e)
                return True
            log.error("[cancel] FAILED order %s -> %s", exchange_order_id, e)
            return False

    async def open_orders(self, market: str) -> List[Dict[str, Any]]:
        """Pre-existing open orders for a pair, used for startup recovery."""
        try:
            data = await self._request(
                "GET", "/api/v1/orders", params={"status": "pending", "market": market}
            )
        except MyCexError as e:
            log.error("[open_orders] FAILED for %s -> %s", market, e)
            return []
        if isinstance(data, list):
            return data
        return data.get("orders", data.get("data", []))
