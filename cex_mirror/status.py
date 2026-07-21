"""Periodic status line to stdout, summarising all engines."""

from __future__ import annotations

import logging
from typing import List

from .mirror_engine import MirrorEngine

log = logging.getLogger("cex_mirror.status")


def format_status(engines: List[MirrorEngine]) -> str:
    lines = ["", "  Binance -> my_cex mirror  |  pairs: %d" % len(engines), "  " + "-" * 60]
    header = f"  {'pair':<12}{'bids':>6}{'asks':>6}{'placed':>9}{'cancel':>9}{'mkt':>7}{'src':>6}"
    lines.append(header)
    tot_p = tot_c = tot_m = 0
    for e in engines:
        book = e._feed.book(e.source_symbol)
        src = "up" if book.ready else "..."
        lines.append(
            f"  {e.pair.mycex:<12}"
            f"{e.tracker.count('buy'):>6}{e.tracker.count('sell'):>6}"
            f"{e.placed:>9}{e.cancelled:>9}{e.market_orders:>7}{src:>6}"
        )
        tot_p += e.placed
        tot_c += e.cancelled
        tot_m += e.market_orders
    lines.append("  " + "-" * 60)
    lines.append(f"  {'TOTAL':<12}{'':>6}{'':>6}{tot_p:>9}{tot_c:>9}{tot_m:>7}")
    return "\n".join(lines)
