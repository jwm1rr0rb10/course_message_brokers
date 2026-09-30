# Runnable examples for the NATS course

Everything here is exercised by CI ([`.github/workflows/examples.yml`](../../.github/workflows/examples.yml)) on every push and on a weekly schedule: `go vet` and the Go tests (unit tests plus the integration tests against an embedded `nats-server`), then the smoke test against the docker compose cluster, once on the image the course pins (2.15) and once on the newest release (`NATS_IMAGE` override). If a new release changes behaviour the course describes, the build goes red.

```
examples/
├── cluster/            the three-node cluster from module 2.5 (docker compose)
├── scripts/
│   ├── smoke-test.sh   runs the course's CLI commands and checks the results
│   └── local-cluster.sh  the same cluster without Docker
├── go/
│   ├── cmd/publisher   JetStream publishing with Nats-Msg-Id (5.5, 7.3)
│   ├── cmd/worker      pull consumer with Ack / NakWithDelay / Term (6.5)
│   ├── cmd/dlq-mover   dead-letter queue built on advisories (14.3)
│   ├── internal/dlq    the DLQ logic, with unit and integration tests
│   └── coursetest      integration tests for statements made in the course
│                       (embedded nats-server by default, or $NATS_URL)
└── python/             pub/sub (4.3), JetStream worker (5.6, 6.6), checks
```

## 1. Start the cluster

With Docker:

```bash
cd examples/cluster
docker compose up -d                          # pinned image (nats:2.15.0-alpine)
NATS_IMAGE=nats:alpine docker compose up -d   # or the newest release
```

Without Docker (needs `nats-server` in `PATH`):

```bash
./examples/scripts/local-cluster.sh start
# ...
./examples/scripts/local-cluster.sh stop
```

Both give you nodes on `localhost:4222/4223/4224`, monitoring on `8222/8223/8224`, an application account `APP` (clients without credentials land there, for learning only) and the system account `SYS` (`admin` / `admin`).

## 2. Run the smoke test

Requires the [`nats` CLI](https://github.com/nats-io/natscli), `curl` and `python3` (used to read JSON). The script is non-interactive and exits non-zero on the first failed check, so CI runs it as is.

```bash
# Docker cluster
./examples/scripts/smoke-test.sh

# local cluster
STOP_NODE_CMD='./examples/scripts/local-cluster.sh stop-node {node}' \
START_NODE_CMD='./examples/scripts/local-cluster.sh start-node {node}' \
  ./examples/scripts/smoke-test.sh
```

It creates the streams from the course, checks deduplication, backoff validation, a filtered mirror, KV, leader step-down, and finally stops the stream leader to confirm the data survives.

## 3. Go

Requires Go 1.26+ (the current `nats.go` and the embedded `nats-server` used by the tests need it).

```bash
cd examples/go
go run ./cmd/dlq-mover &                     # terminal 1
go run ./cmd/publisher -n 10 -poison         # publishes 10 orders and one invalid message
go run ./cmd/worker -fail-rate 0.3           # Acks, NakWithDelay with a growing delay, Terms the poison
nats stream view DLQ                         # the poison message is here

go vet ./...
go test -count=1 ./...                       # unit + integration tests on an embedded nats-server

# the same integration tests against the running cluster
NATS_URL=nats://localhost:4222,nats://localhost:4223,nats://localhost:4224 \
COURSE_REQUIRE_BROKER=1 go test -count=1 ./...
```

Without `NATS_URL` every integration test starts its own embedded single-node `nats-server` (JetStream, store in a temp dir), so no Docker is needed. With `NATS_URL` the tests use that server: if it is unreachable they are skipped, unless `COURSE_REQUIRE_BROKER=1` is set (CI), in which case they fail.

The worker retries transient errors with `NakWithDelay`, taking the delay from its own schedule by `NumDelivered` (1s, 5s, 30s, 1m). It does not rely on the consumer's `BackOff`: that only applies when `AckWait` expires, and a plain `Nak` ignores it (module 6.4, checked by `coursetest`). Poison messages go through `dlq.Terminate`, which publishes them to the DLQ before `Term`: on workqueue and interest streams `Term` deletes the message, so the mover would have nothing to copy (module 14.3).

Run the publisher twice: the second run reports `duplicate=true` for every order, because each order has a stable `Nats-Msg-Id`.

## 4. Python

```bash
cd examples/python
pip install -r requirements.txt
python pubsub.py
python jetstream_worker.py
python check.py
```

## Environment variables

| Variable | Default | Used by |
|---|---|---|
| `NATS_URL` | Go programs and Python: the three localhost ports; Go tests: embedded server | Go, Python |
| `NATS_REPLICAS` | `3` (`1` for the embedded server) | Go and Python tests (set `1` for a single node) |
| `COURSE_REQUIRE_BROKER` | unset | Go tests: `1` turns "server unreachable, skip" into a failure |
| `NATS_IMAGE`, `NATS_BOX_IMAGE` | `nats:2.15.0-alpine`, `natsio/nats-box:0.20.0` | docker compose |
| `NATS_CLI` | `nats` | smoke test |
| `NATS_URLS`, `MONITOR_URL` | localhost cluster | smoke test |
| `STOP_NODE_CMD`, `START_NODE_CMD` | `docker stop/start {node}` | smoke test |
