"""Binance public market-data feed for many pairs over shared WebSocket connections.

No API key required. Uses the public combined stream:

  wss://stream.binance.com:9443/stream?streams=btcusdt@depth@100ms/btcusdt@trade/...

For each pair we maintain a local order book by the standard Binance procedure:
  1. Subscribe to the diff depth stream, buffering events.
  2. Fetch a REST depth snapshot (https://api.binance.com/api/v3/depth).
  3. Apply buffered + subsequent diffs whose update-id range follows the snapshot.

This gives a real-time in-memory book per pair that mirror engines read each cycle.
Public trades are dispatched to a per-pair callback for market-order mirroring.

Streams are chunked across multiple connections (Binance caps ~1024 streams/conn and
limits message rate); a few hundred pairs still fit in a handful of connections.
"""

from __future__ import annotations

import asyncio
import json
import logging
from decimal import Decimal
from typing import Awaitable, Callable, Dict, List, Optional, Tuple

import aiohttp
import websockets

log = logging.getLogger("cex_mirror.binance")

WS_BASE = "wss://stream.binance.com:9443/stream"
REST_DEPTH = "https://api.binance.com/api/v3/depth"
REST_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo"

# Streams per WS connection. Binance allows up to 1024; keep headroom.
STREAMS_PER_CONN = 200
# Depth snapshot levels to fetch (max 5000 REST; we only need `levels` but grab enough).
SNAPSHOT_LIMIT = 1000

TradeCallback = Callable[[str, str, str, bool], Awaitable[None]]
# (symbol, price, qty, is_buyer_maker)


class OrderBook:
    """A single pair's local book. Bids/asks keyed by Decimal price -> Decimal qty."""

    def __init__(self) -> None:
        self.bids: Dict[Decimal, Decimal] = {}
        self.asks: Dict[Decimal, Decimal] = {}
        self.last_update_id: int = 0
        self.ready: bool = False

    def apply_side(self, book: Dict[Decimal, Decimal], levels: List[List[str]]) -> None:
        for price_s, qty_s in levels:
            price = Decimal(price_s)
            qty = Decimal(qty_s)
            if qty == 0:
                book.pop(price, None)
            else:
                book[price] = qty

    def top_bids(self, n: int) -> List[Tuple[str, str]]:
        out = sorted(self.bids.items(), key=lambda kv: kv[0], reverse=True)[:n]
        return [(str(p), str(q)) for p, q in out]

    def top_asks(self, n: int) -> List[Tuple[str, str]]:
        out = sorted(self.asks.items(), key=lambda kv: kv[0])[:n]
        return [(str(p), str(q)) for p, q in out]


class BinanceFeed:
    def __init__(self, symbols: List[str], trade_callback: Optional[TradeCallback] = None):
        # symbols are Binance form e.g. "BTCUSDT"
        self._symbols = [s.upper() for s in symbols]
        self._books: Dict[str, OrderBook] = {s: OrderBook() for s in self._symbols}
        self._diff_buffers: Dict[str, List[dict]] = {s: [] for s in self._symbols}
        self._trade_callback = trade_callback
        self._session: Optional[aiohttp.ClientSession] = None
        self._tasks: List[asyncio.Task] = []
        # symbol -> its dedicated connection task, for runtime add/remove_symbol.
        self._dynamic_conns: Dict[str, asyncio.Task] = {}
        self._stop = asyncio.Event()

    def book(self, symbol: str) -> OrderBook:
        return self._books[symbol.upper()]

    def has_symbol(self, symbol: str) -> bool:
        return symbol.upper() in self._books

    async def symbol_exists_on_binance(self, symbol: str) -> bool:
        """Whether Binance lists this symbol (TRADING status). Used to skip mirroring
        admin-created pairs that have no source book (e.g. custom/test tokens)."""
        assert self._session is not None, "BinanceFeed.start() must run before validation"
        try:
            async with self._session.get(
                REST_EXCHANGE_INFO, params={"symbol": symbol.upper()}
            ) as resp:
                if resp.status != 200:
                    return False
                data = await resp.json()
        except Exception as e:
            log.warning("[binance] exchange-info check failed for %s: %s", symbol, e)
            return False
        for s in data.get("symbols", []):
            if s.get("symbol", "").upper() == symbol.upper():
                return s.get("status", "").upper() == "TRADING"
        return False

    async def start(self) -> None:
        self._session = aiohttp.ClientSession()
        # One task per connection chunk.
        chunks = [
            self._symbols[i:i + STREAMS_PER_CONN]
            for i in range(0, len(self._symbols), STREAMS_PER_CONN)
        ]
        for idx, chunk in enumerate(chunks):
            self._tasks.append(asyncio.create_task(self._run_connection(idx, chunk)))

    def add_symbol(self, symbol: str) -> bool:
        """Subscribe a new symbol at runtime on its own WS connection, without
        touching existing connections. Idempotent. Returns True if newly added.

        Must be called after start() (a session must exist). Each dynamically-added
        symbol gets a dedicated connection; static startup pairs stay chunked.
        """
        symbol = symbol.upper()
        if symbol in self._books:
            return False
        assert self._session is not None, "BinanceFeed.start() must run before add_symbol()"
        self._symbols.append(symbol)
        self._books[symbol] = OrderBook()
        self._diff_buffers[symbol] = []
        task = asyncio.create_task(self._run_connection(len(self._tasks), [symbol]))
        self._tasks.append(task)
        self._dynamic_conns[symbol] = task
        log.info("[binance] dynamically subscribed %s (conn %d)", symbol, len(self._tasks) - 1)
        return True

    async def remove_symbol(self, symbol: str) -> bool:
        """Unsubscribe a runtime-added symbol: tear down its dedicated WS connection
        and drop its book. Idempotent. Returns True if it was removed.

        Only symbols added via add_symbol() have their own cancellable connection;
        statically-chunked startup symbols are not individually removable (a delist
        for one still stops its engine — the book just goes stale, harmlessly).
        """
        symbol = symbol.upper()
        task = self._dynamic_conns.pop(symbol, None)
        if task is not None:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            if task in self._tasks:
                self._tasks.remove(task)
        removed = symbol in self._books
        self._books.pop(symbol, None)
        self._diff_buffers.pop(symbol, None)
        if symbol in self._symbols:
            self._symbols.remove(symbol)
        if removed:
            log.info("[binance] unsubscribed %s", symbol)
        return removed

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except asyncio.CancelledError:
                pass
        if self._session is not None:
            await self._session.close()

    def _streams_for(self, symbols: List[str]) -> str:
        parts: List[str] = []
        for s in symbols:
            low = s.lower()
            parts.append(f"{low}@depth@100ms")
            if self._trade_callback is not None:
                parts.append(f"{low}@trade")
        return "/".join(parts)

    async def _run_connection(self, idx: int, symbols: List[str]) -> None:
        url = f"{WS_BASE}?streams={self._streams_for(symbols)}"
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20,
                                               max_queue=None) as ws:
                    log.info("[binance] conn %d up (%d pairs)", idx, len(symbols))
                    backoff = 1.0
                    # (Re)initialize snapshots for this connection's pairs.
                    for s in symbols:
                        self._books[s].ready = False
                        self._diff_buffers[s] = []
                        asyncio.create_task(self._bootstrap_snapshot(s))
                    async for raw in ws:
                        if self._stop.is_set():
                            break
                        await self._on_message(raw)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                if self._stop.is_set():
                    break
                log.warning("[binance] conn %d dropped: %s; reconnecting in %.1fs", idx, e, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _bootstrap_snapshot(self, symbol: str) -> None:
        """Fetch REST depth snapshot and reconcile with buffered diffs."""
        assert self._session is not None
        try:
            async with self._session.get(
                REST_DEPTH, params={"symbol": symbol, "limit": SNAPSHOT_LIMIT}
            ) as resp:
                snap = await resp.json()
        except Exception as e:
            log.warning("[binance] snapshot fetch failed for %s: %s; will retry on next diff", symbol, e)
            return

        book = self._books[symbol]
        book.bids = {Decimal(p): Decimal(q) for p, q in snap.get("bids", [])}
        book.asks = {Decimal(p): Decimal(q) for p, q in snap.get("asks", [])}
        book.last_update_id = int(snap.get("lastUpdateId", 0))

        # Apply buffered diffs that are newer than the snapshot.
        for diff in self._diff_buffers.get(symbol, []):
            self._apply_diff(book, diff)
        self._diff_buffers[symbol] = []
        book.ready = True
        log.info("[binance] book ready: %s (u=%d)", symbol, book.last_update_id)

    def _apply_diff(self, book: "OrderBook", diff: dict) -> None:
        # Binance diff: U=first update id, u=final update id in event.
        u = int(diff.get("u", 0))
        if u <= book.last_update_id:
            return  # already covered by snapshot
        book.apply_side(book.bids, diff.get("b", []))
        book.apply_side(book.asks, diff.get("a", []))
        book.last_update_id = u

    async def _on_message(self, raw: str) -> None:
        try:
            msg = json.loads(raw)
        except ValueError:
            return
        data = msg.get("data", msg)
        event = data.get("e", "")
        symbol = data.get("s", "").upper()
        if symbol not in self._books:
            return

        if event == "depthUpdate":
            book = self._books[symbol]
            if not book.ready:
                self._diff_buffers[symbol].append(data)
            else:
                self._apply_diff(book, data)
        elif event == "trade" and self._trade_callback is not None:
            # p=price, q=qty, m=is buyer the maker (True => sell-side aggressor)
            await self._trade_callback(
                symbol, data.get("p", "0"), data.get("q", "0"), bool(data.get("m", False))
            )
