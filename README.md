# cex-mirror

A lightweight, standalone replacement for the Hummingbot `my_cex` order-book-mirror
setup. It mirrors the **Binance** order book (and trade tape) onto **my_cex** by
placing orders as a single self-trading user — so the market looks live and you can
test trades on your platform.

## Why this exists

The original approach ran **one full Hummingbot Docker container per pair**
(~2 GB cap each), which does not scale. This project runs **all pairs in one
in-memory asyncio process** with no database:

- One (or a few) shared Binance public WebSocket connection(s) feed every pair.
- One shared REST client places/cancels orders on my_cex using a single JWT.
- Order state is tracked in memory per pair — no DB, no keystore.

Target footprint: a single container well under ~512 MB for a dozen+ pairs, vs.
N × 2 GB containers before. Adding a pair is one line in `config.yaml`, not a new
container.

## Behaviour (matches the original strategy)

- **Limit mirroring** — every `refresh_interval`s, reads the top `levels` of the
  Binance book, quantizes price (`price_precision`, round-down) and amount
  (`amount_precision`, round-half-up), caps at `max_order_amount`, drops levels below
  `min_notional`, then reconciles resting orders: cancels stale/drifted
  (`amount_tolerance`) orders and places any missing target levels.
- **Market mirroring** — for each public Binance trade, fires a same-side market
  order on my_cex, honoring `mirror_market_orders`, `max_market_order_amount`,
  `min_seconds_between_market_orders`, and `min_notional`.

Config field names and semantics are identical to the old
`conf_binance_to_my_cex_orderbook_mirror_*.yml` files.

## Setup

```bash
cd cex-mirror
python -m venv .venv && source .venv/bin/activate
pip install .

cp .env.example .env      # then put the raw JWT in MYCEX_JWT
# edit config.yaml to list your pairs
```

The JWT must be the **raw** token (the old `conf/connectors/my_cex.yml` stored it
encrypted in Hummingbot's keystore — decrypt it once and paste the plain JWT here).

## Run

```bash
python -m cex_mirror config.yaml
# or, after `pip install`:
cex-mirror config.yaml
```

Verbose per-order logs: `LOG_LEVEL=DEBUG python -m cex_mirror`.

### Docker

```bash
export MYCEX_JWT=<raw-jwt>
docker compose up --build -d
docker compose logs -f
```

## Config

See `config.yaml`. Global `defaults` apply to every pair; each pair may override any
field. `mycex.jwt_env` names the environment variable holding the JWT.

`cancel_on_shutdown` cancels all resting orders on Ctrl-C/SIGTERM.
`reconcile_on_startup` adopts any pre-existing open orders at boot (so a crash/kill -9
never leaves orphaned orders on the book).

## Layout

| File | Responsibility |
|------|----------------|
| `config.py` | Load/validate YAML, apply defaults, resolve JWT from env |
| `binance_feed.py` | Shared Binance WS: multiplexed depth+trade, in-memory books |
| `mycex_client.py` | Async REST client (exchange-info, place, cancel, open-orders) |
| `quantize.py` | Decimal price/amount quantization + min-notional filter |
| `order_tracker.py` | In-memory per-pair resting-order state |
| `mirror_engine.py` | Per-pair reconcile loop + trade-tape handler |
| `status.py` | Periodic status line |
| `__main__.py` | Wire everything, run the event loop, handle shutdown |
```
