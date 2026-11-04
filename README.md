# order-pipeline

[![ci](https://github.com/BilalWarraich/order-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/BilalWarraich/order-pipeline/actions/workflows/ci.yml)

Small order processing service I've been hacking on to play with goroutines/channels
in something closer to a real backend than the usual tutorial examples.

The idea: the API accepts an order, saves it as `pending` and returns `202` straight away.
A pool of workers picks orders off a channel and runs them through some (fake) steps:

```
POST /orders -> save (pending) -> queue -> worker
                                            |-- inventory check --\
                                            |-- fraud check ------+-> charge payment -> done / failed
```

Inventory and fraud run concurrently, payment only runs if both pass. Each step retries
with exponential backoff, and there's a timeout on the whole thing.

If the queue is full the API returns `503` with `Retry-After` instead of blocking.

A sweeper goroutine runs every 30s and re-queues orders that have been `pending` for too long
(queue was full, or the process died before a worker got to them). Workers *claim* an order with
a conditional update (`... WHERE status = 'pending'`) before touching it, so an order that ends up
queued twice still only gets processed once.

If the process dies while a worker is on an order, that order would sit in `processing` forever.
So the sweeper also resets orders that have been `processing` for longer than `STUCK_PROCESSING_SECS`
back to `pending` (one conditional `UPDATE ... WHERE status = 'processing' AND updated_at < ...`), and
they get picked up again like any other stuck order. The threshold has to be well above the processor
timeout (5s), otherwise a slow but alive worker would get its order taken away.

This makes processing **at-least-once**: after a crash an order can go through some steps twice (e.g.
inventory got reserved, then the process died before payment). That's the trade-off for never losing an
order, and it's why the claim step and idempotency keys exist. In a real system the downstream calls
(payment etc.) would need their own idempotency keys too.

On SIGTERM the http server stops first, then the sweeper, then the pool drains whatever is left in the queue.

## Live demo

Running on Render's free tier (with Neon postgres): https://order-pipeline-hkod.onrender.com

It sleeps when idle, so the first request might take ~30-50s to wake up.

```
curl https://order-pipeline-hkod.onrender.com/healthz

curl -XPOST https://order-pipeline-hkod.onrender.com/orders -d '{
  "customer_id": "c1",
  "items": [{"sku": "KB-01", "qty": 2, "price_cents": 4999}]
}'

curl https://order-pipeline-hkod.onrender.com/orders?limit=5
curl https://order-pipeline-hkod.onrender.com/stats
```

## Running

Without docker (uses an in-memory store):

```
go run ./cmd/server
```

With postgres:

```
docker compose up --build
```

## Trying it out

```
curl -XPOST localhost:8080/orders -d '{
  "customer_id": "c1",
  "items": [{"sku": "KB-01", "qty": 2, "price_cents": 4999}]
}'

curl localhost:8080/orders/<id>
curl localhost:8080/orders?limit=10
curl localhost:8080/stats
curl localhost:8080/metrics
```

Send an `Idempotency-Key` header to make retries safe: the same key with the same body returns
the original order (`200`, `Idempotent-Replayed: true`) instead of creating a second one, the same
key with a different body is a `422`. Uniqueness is enforced by a unique index in postgres,
so two identical requests arriving at the same time still only create one order.

```
curl -XPOST localhost:8080/orders -H 'Idempotency-Key: 7f1c...' -d '{...}'
```

Orders over $5000 get rejected by the "fraud check", so you can see the failed path.

## Config

| env | default | |
|---|---|---|
| `ADDR` | `:8080` | |
| `DATABASE_URL` | - | in-memory store if empty |
| `WORKERS` | 4 | |
| `QUEUE_SIZE` | 100 | |
| `FAIL_RATE` | 0 | chance a fake step fails, 0-1 |
| `STUCK_AFTER_SECS` | 120 | sweeper re-queues pending orders older than this |
| `STUCK_PROCESSING_SECS` | 300 | orders in `processing` longer than this get reset to pending (0 = off) |

## Deploying

There's a `render.yaml` so it can go on Render's free tier:

1. create a free postgres on [neon.tech](https://neon.tech) and copy the connection string
2. on Render: New -> Blueprint -> pick this repo
3. paste the neon connection string as `DATABASE_URL`

Tables get created on startup (`migrations/`). If `DATABASE_URL` is empty it just runs with the
in-memory store, which also works for a quick demo.

Free instances sleep after ~15 min idle, so the first request can take a while.

## Tests

```
make test
```

## TODO

- [x] sweeper for orders stuck in `pending` (e.g. after a crash, or when queue was full)
- [x] same thing for orders stuck in `processing` after a crash
- [x] prometheus metrics (`/metrics`: request latency per route, queue depth, processed/failed)
- [ ] replace the in-process queue with NATS or something so it can scale out
- [x] idempotency keys on POST
