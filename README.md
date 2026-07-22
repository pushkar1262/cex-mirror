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

## Auto-adding pairs (admin panel / Kafka)

When an admin adds a pair in the platform's admin panel, the platform emits an event on
the `market-lifecycle` Kafka topic. Enable the consumer and the mirror will **start
mirroring a newly-created/enabled market immediately** — a brand-new market gets traffic
as soon as it exists — and **append the pair to `config.yaml`** so a restart resumes it.

```yaml
kafka:
  enabled: true
  bootstrap_servers: broker1:9092,broker2:9092
  topic: market-lifecycle
  group_id: cex-mirror
  auto_offset_reset: latest   # latest = only new events; earliest = replay history
```

Install the extra (already included in the Docker image):

```bash
pip install ".[kafka]"
```

Behaviour:

- Handles `market.created`, `market.updated`, `market.state_changed`, and
  `market.delisted` events.
- **Adding** (`created` / `updated` / `state_changed` with `state: enabled`): starts
  mirroring the market.
- Adding a pair **does not disturb pairs already running** — the new pair gets its own
  Binance subscription and reconcile loop; existing engines/connections are untouched.
- `tick_size` → price quantization, `step_size` → amount quantization, `min_total` →
  `min_notional`.
- If **Binance does not list** the pair's symbol (e.g. a custom/test token), it is logged
  and skipped — there's no source book to mirror. The consumer keeps running for others.
- **Removing** — two equivalent triggers stop a running pair: `market.delisted`, and any
  `created`/`updated`/`state_changed` event with `state: disabled` (`state` is a binary
  flag, `enabled` | `disabled`). Either one stops that pair's reconcile loop, cancels all
  its resting orders, unsubscribes its Binance feed, and removes it from `config.yaml`.
  Every other running pair is left untouched. Removing a pair that isn't running is a
  harmless no-op.
- The pair set is persisted via a full PyYAML rewrite of `config.yaml`; hand-written
  comments in that file are not preserved once the first dynamic add/remove is written.

With `kafka.enabled: true` you may start with an empty `pairs:` list and let every pair
arrive over Kafka.

## Layout

| File | Responsibility |
|------|----------------|
| `config.py` | Load/validate YAML, apply defaults, resolve JWT from env, event→pair mapping + config persistence |
| `binance_feed.py` | Shared Binance WS: multiplexed depth+trade, in-memory books, runtime symbol add + validation |
| `mycex_client.py` | Async REST client (exchange-info, place, cancel, open-orders) |
| `quantize.py` | Decimal price/amount quantization + min-notional filter |
| `order_tracker.py` | In-memory per-pair resting-order state |
| `mirror_engine.py` | Per-pair reconcile loop + trade-tape handler |
| `kafka_consumer.py` | Consume `market-lifecycle`, dispatch market events |
| `pair_manager.py` | Live engine registry; dynamic add-a-pair without disturbing running pairs |
| `status.py` | Periodic status line |
| `__main__.py` | Wire everything, run the event loop, handle shutdown |
```
