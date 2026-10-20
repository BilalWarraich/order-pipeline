# order-pipeline

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

On SIGTERM the http server stops first, then the pool drains whatever is left in the queue.

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

- [ ] sweeper for orders stuck in `pending` (e.g. after a crash, or when queue was full)
- [ ] prometheus metrics
- [ ] replace the in-process queue with NATS or something so it can scale out
- [ ] real idempotency keys on POST
