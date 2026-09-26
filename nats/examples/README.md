# Runnable examples for the NATS course

Everything here is exercised by CI ([`.github/workflows/examples.yml`](../.github/workflows/examples.yml)) against the NATS version the course targets (2.12) and against the newest 2.x release, plus a weekly scheduled run. If a new release changes behaviour the course describes, the build goes red.

```
examples/
├── cluster/            the three-node cluster from module 2.5 (docker compose)
├── scripts/
│   ├── smoke-test.sh   runs the course's CLI commands and checks the results
│   └── local-cluster.sh  the same cluster without Docker
├── go/
│   ├── cmd/publisher   JetStream publishing with Nats-Msg-Id (5.5, 7.3)
│   ├── cmd/worker      pull consumer with Ack / Nak / Term and backoff (6.5)
│   ├── cmd/dlq-mover   dead-letter queue built on advisories (14.3)
│   ├── internal/dlq    the DLQ logic, with unit and integration tests
│   └── coursetest      integration tests for statements made in the course
└── python/             pub/sub (4.3), JetStream worker (5.6, 6.6), checks
```

## 1. Start the cluster

With Docker:

```bash
cd examples/cluster
docker compose up -d
```

Without Docker (needs `nats-server` in `PATH`):

```bash
./examples/scripts/local-cluster.sh start
# ...
./examples/scripts/local-cluster.sh stop
```

Both give you nodes on `localhost:4222/4223/4224`, monitoring on `8222/8223/8224`, an application account `APP` (clients without credentials land there, for learning only) and the system account `SYS` (`admin` / `admin`).

## 2. Run the smoke test

Requires the [`nats` CLI](https://github.com/nats-io/natscli) and `python3` (used to read JSON).

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

Requires Go 1.26+ (the current `nats.go` needs it).

```bash
cd examples/go
go mod tidy                                  # first run: creates go.sum

go run ./cmd/dlq-mover &                     # terminal 1
go run ./cmd/publisher -n 10 -poison         # publishes 10 orders and one invalid message
go run ./cmd/worker -fail-rate 0.3           # Acks, Naks with backoff, Terms the poison
nats stream view DLQ                         # the poison message is here

go test ./...                                # unit tests, no server needed
go test -tags integration -count=1 ./...     # needs the running cluster
```

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
| `NATS_URL` | the three localhost ports | Go, Python |
| `NATS_REPLICAS` | `3` | Go and Python tests (set `1` for a single node) |
| `NATS_URLS`, `MONITOR_URL` | localhost cluster | smoke test |
| `STOP_NODE_CMD`, `START_NODE_CMD` | `docker stop/start {node}` | smoke test |
