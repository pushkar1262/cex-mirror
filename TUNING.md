# Tuning orders/sec

There is no single "orders per second" setting. The rate is an **emergent**
result of two independent order sources, each with its own knobs. To tune
throughput, you adjust these. Every knob can be set globally under `defaults:`
or per-pair in the `pairs:` list of `config.yaml`, so you can run one pair hot
and keep others calm.

## Source 1 — LIMIT mirroring (the main driver)

Every `refresh_interval` seconds, each engine reads the top `levels` of the
Binance book and reconciles against its own resting orders — cancelling
stale/drifted orders and placing new ones for uncovered prices
(`cex_mirror/mirror_engine.py`, `reconcile` / `_reconcile_side`). The reconcile
loop just sleeps `refresh_interval` between passes
(`cex_mirror/__main__.py`, `_reconcile_loop`).

Per pair, rough peak request rate:

    orders/pass ≈ levels × 2 (both sides) × churn

...spread over each `refresh_interval`. In steady state most levels are
unchanged, so you mostly pay for the levels that moved (a cancel + a place
each).

The three knobs, in order of impact:

| Knob | Where | Effect on orders/sec |
|---|---|---|
| `refresh_interval` | per pair / `defaults` | **Strongest.** Halving it (1.0 → 0.5) doubles reconcile passes → ~doubles request rate. Raising it lowers rate. |
| `levels` | per pair / `defaults` | Directly scales orders placed/cancelled per pass. BTC pair is at `60`; the default is `15`. |
| `amount_tolerance` | per pair / `defaults` | Deadband before re-quoting a drifted level. **Lower** → more cancel/replace churn (more orders/sec). **Higher** → fewer. Currently `0.05` (5%). |

## Source 2 — MARKET (trade-tape) mirroring

For each public Binance trade, a same-side market order fires
(`cex_mirror/mirror_engine.py`, `on_public_trade`). This is driven by Binance
trade frequency, throttled by:

- `min_seconds_between_market_orders` — currently `0` (no throttle, one order
  per trade). **Raise it** to cap this source's rate; e.g. `0.5` = at most
  2/sec per pair.
- `mirror_market_orders: false` — disables this source entirely.
- `min_notional` / `max_market_order_amount` — filter out small/large trades.

## The ceiling (safety limit, not a rate setter)

`mycex.max_concurrency` (currently `20`) caps concurrent in-flight REST
requests across all pairs (`cex_mirror/config.py`, `MyCexSettings`). It doesn't
*set* the rate — it clamps the peak. If your `refresh_interval` / `levels`
demand more than 20 simultaneous requests, they queue, so this is your backstop
against overwhelming the my_cex API.

## Concrete recipes

### To increase orders/sec

1. Lower `refresh_interval` (e.g. `1` → `0.5` or `0.25`) — biggest lever.
2. Increase `levels`.
3. Lower `amount_tolerance` (e.g. `0.05` → `0.02`) so smaller price/size moves
   trigger re-quotes.
4. Lower `min_seconds_between_market_orders` toward `0`, keep
   `mirror_market_orders: true`.
5. Raise `max_concurrency` so the extra load isn't throttled at the client.

### To decrease orders/sec

1. Raise `refresh_interval`.
2. Reduce `levels`.
3. Raise `amount_tolerance` (wider deadband = less churn).
4. Raise `min_seconds_between_market_orders`, or set
   `mirror_market_orders: false`.
5. Lower `max_concurrency` to hard-cap the peak.
