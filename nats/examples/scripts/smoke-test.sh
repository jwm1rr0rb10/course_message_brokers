#!/usr/bin/env bash
# Runs the commands from the course against the 3-node cluster and checks that
# they behave the way the course says they do. Non-interactive: exits 0 when
# every check passes and non-zero on the first failure, so CI can run it right
# after `docker compose up -d` (works with bash 3.2 on macOS).
#
#   cd examples/cluster && docker compose up -d        # pinned image
#   NATS_IMAGE=nats:alpine docker compose up -d         # or the newest release
#   ../scripts/smoke-test.sh
#
# Environment:
#   NATS_CLI         nats CLI binary (default: nats)
#   NATS_URLS        client URLs (default: the three localhost ports)
#   MONITOR_URL      monitoring endpoint of one node (default http://localhost:8222)
#   STOP_NODE_CMD    command to stop a node, "{node}" is replaced by its name
#   START_NODE_CMD   command to start it again
#
# Docker:  STOP_NODE_CMD='docker stop {node}'  START_NODE_CMD='docker start {node}'
# Local:   STOP_NODE_CMD='./scripts/local-cluster.sh stop-node {node}' ...
set -euo pipefail

NATS_CLI="${NATS_CLI:-nats}"
NATS_URLS="${NATS_URLS:-nats://localhost:4222,nats://localhost:4223,nats://localhost:4224}"
MONITOR_URL="${MONITOR_URL:-http://localhost:8222}"
DEFAULT_STOP='docker stop {node}'
DEFAULT_START='docker start {node}'
STOP_NODE_CMD="${STOP_NODE_CMD:-$DEFAULT_STOP}"
START_NODE_CMD="${START_NODE_CMD:-$DEFAULT_START}"

app()  { "$NATS_CLI" -s "$NATS_URLS" "$@"; }
sys()  { "$NATS_CLI" -s "$NATS_URLS" --user admin --password admin "$@"; }
# number of distinct servers answering `nats server list`
nservers() {
  sys server list 3 --json 2>/dev/null \
    | python3 -c 'import sys,json; print(len({s["server"]["name"] for s in json.load(sys.stdin)}))' 2>/dev/null \
    || echo 0
}

command -v "$NATS_CLI" >/dev/null 2>&1 || { echo "nats CLI not found (set NATS_CLI)" >&2; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "python3 not found" >&2; exit 1; }
command -v curl >/dev/null 2>&1 || { echo "curl not found" >&2; exit 1; }
json() { python3 -c 'import sys,json; d=json.load(sys.stdin); print(eval("d"+sys.argv[1]))' "$1"; }

PASS=0
step() { echo; echo "==> $*"; }
ok()   { PASS=$((PASS+1)); echo "    ok: $*"; }
fail() { echo "    FAIL: $*" >&2; exit 1; }

# ---------------------------------------------------------------------------
step "wait for the cluster and a JetStream meta leader"
for i in $(seq 1 60); do
  if app account info >/dev/null 2>&1 && app stream ls >/dev/null 2>&1; then break; fi
  [[ $i == 60 ]] && fail "cluster not ready after 60s"
  sleep 1
done
servers=$(nservers)
[[ "$servers" == 3 ]] || fail "expected 3 servers in 'nats server list', got $servers"
version=$(curl -fsS "$MONITOR_URL/varz" | json '["version"]')
ok "3 servers running nats-server $version, JetStream ready (module 2.5)"

# clean state so the script can be re-run
for s in ORDERS ORDERS_ARCHIVE; do app stream rm "$s" -f >/dev/null 2>&1 || true; done
app kv rm CONFIG -f >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
step "Core NATS is at-most-once: nobody subscribed, the message is gone (2.3)"
app pub smoke.core.lost "nobody hears this" >/dev/null 2>&1
got=$(app sub smoke.core.lost --count 1 --wait 2s 2>/dev/null | grep -c "nobody hears this" || true)
[[ "$got" == 0 ]] || fail "a Core NATS message survived without subscribers"
ok "message published with no subscriber was not delivered later"

# ---------------------------------------------------------------------------
step "request-reply and 'no responders' (2.4, 4.8)"
"$NATS_CLI" -s "$NATS_URLS" reply service.echo --echo >/dev/null 2>&1 &
REPLY_PID=$!
trap 'kill "$REPLY_PID" 2>/dev/null || true' EXIT
sleep 1
app request service.echo hello --timeout 2s >/dev/null 2>&1 || fail "request to a running responder failed"
kill "$REPLY_PID"; wait "$REPLY_PID" 2>/dev/null || true
sleep 0.5
out=$(app request service.echo hello --timeout 2s 2>&1 || true)
echo "$out" | grep -qi "no responders" || fail "expected 'no responders', got: $out"
ok "request succeeds with a responder, fails fast with 'no responders' without one"

# ---------------------------------------------------------------------------
step "create the ORDERS stream exactly as in module 2.8"
app stream add ORDERS \
  --subjects "shop.orders.>" \
  --storage file \
  --replicas 3 \
  --retention limits \
  --max-age 168h \
  --max-msgs=-1 \
  --max-bytes 1GB \
  --discard old \
  --dupe-window 2m \
  --defaults >/dev/null 2>&1
replicas=$(app stream info ORDERS --json | json '["config"]["num_replicas"]')
[[ "$replicas" == 3 ]] || fail "expected 3 replicas, got $replicas"
ok "stream created with replicas=3"

for i in $(seq 1 10); do app pub shop.orders.created "{\"order_id\":\"order-$i\"}" >/dev/null 2>&1; done
msgs=$(app stream info ORDERS --json | json '["state"]["messages"]')
[[ "$msgs" == 10 ]] || fail "expected 10 messages, got $msgs"
ok "10 messages stored"

curl -fsS "$MONITOR_URL/jsz?accounts=true&streams=true" | grep -q '"ORDERS"' \
  || fail "ORDERS not visible in /jsz?accounts=true&streams=true"
ok "stream visible in /jsz (2.8 practice)"

# ---------------------------------------------------------------------------
step "publish deduplication with Nats-Msg-Id (7.3)"
for _ in 1 2 3; do app pub shop.orders.paid '{"order_id":"order-1"}' -H "Nats-Msg-Id:order-1-paid" >/dev/null 2>&1; done
msgs=$(app stream info ORDERS --json | json '["state"]["messages"]')
[[ "$msgs" == 11 ]] || fail "expected 11 messages after 3 publishes with the same id, got $msgs"
ok "three publishes with one Nats-Msg-Id stored once"

# ---------------------------------------------------------------------------
step "pull consumer from module 2.8"
app consumer add ORDERS BILLING \
  --pull \
  --ack explicit \
  --deliver all \
  --filter "shop.orders.created" \
  --max-deliver 5 \
  --wait 30s \
  --defaults >/dev/null 2>&1
got=$(app consumer next ORDERS BILLING --count 2 2>/dev/null | grep -c '"order_id"' || true)
[[ "$got" == 2 ]] || fail "expected 2 messages from 'consumer next', got $got"
ok "consumer next returned 2 messages"

# ---------------------------------------------------------------------------
step "backoff rules from module 6.4"
app consumer add ORDERS BACKOFF_OK --pull --ack explicit --filter "shop.orders.created" \
  --max-deliver 6 --backoff linear --backoff-steps 5 --backoff-min 1s --backoff-max 1m \
  --defaults >/dev/null 2>&1 || fail "valid backoff consumer rejected"
ok "max_deliver (6) > backoff steps (5) accepted"
if app consumer add ORDERS BACKOFF_BAD --pull --ack explicit --filter "shop.orders.created" \
  --max-deliver 3 --backoff linear --backoff-steps 5 --backoff-min 1s --backoff-max 1m \
  --defaults >/dev/null 2>&1; then
  fail "server accepted max_deliver (3) <= backoff steps (5)"
fi
ok "max_deliver (3) <= backoff steps (5) rejected, as the course says"

# ---------------------------------------------------------------------------
step "mirror with a filter from a JSON config (13.2)"
ARCHIVE_JSON="${TMPDIR:-/tmp}/orders-archive.$$.json"
cat > "$ARCHIVE_JSON" <<'JSON'
{
  "name": "ORDERS_ARCHIVE",
  "storage": "file",
  "num_replicas": 3,
  "mirror": {
    "name": "ORDERS",
    "filter_subject": "shop.orders.created",
    "opt_start_seq": 1
  }
}
JSON
app stream add ORDERS_ARCHIVE --config "$ARCHIVE_JSON" >/dev/null 2>&1
rm -f "$ARCHIVE_JSON"
for i in $(seq 1 20); do
  m=$(app stream info ORDERS_ARCHIVE --json | json '["state"]["messages"]')
  [[ "$m" == 10 ]] && break
  sleep 0.5
done
[[ "$m" == 10 ]] || fail "mirror should hold the 10 'created' messages, has $m"
ok "filtered mirror holds only shop.orders.created (10 of 11)"

# ---------------------------------------------------------------------------
step "Key-Value Store (9.2)"
app kv add CONFIG --history 5 --replicas 3 --storage file >/dev/null 2>&1
app kv put CONFIG feature.dark_mode true >/dev/null 2>&1
val=$(app kv get CONFIG feature.dark_mode --raw)
[[ "$val" == "true" ]] || fail "kv get returned '$val'"
ok "kv put/get works"

# ---------------------------------------------------------------------------
step "leader election commands (12.2, 18.4)"
sys server cluster step-down >/dev/null 2>&1 || fail "'nats server cluster step-down' failed"
ok "meta leader stepped down"
before=$(app stream info ORDERS --json | json '["cluster"]["leader"]')
app stream cluster step-down ORDERS >/dev/null 2>&1 || fail "'nats stream cluster step-down' failed"
ok "stream leader stepped down (was $before)"

# ---------------------------------------------------------------------------
step "kill the stream leader: data stays available (2.8 practice)"
sleep 2
leader=$(app stream info ORDERS --json | json '["cluster"]["leader"]')
echo "    stopping $leader"
eval "${STOP_NODE_CMD//\{node\}/$leader}" >/dev/null
for i in $(seq 1 30); do
  new=$(app stream info ORDERS --json 2>/dev/null | json '["cluster"]["leader"]' 2>/dev/null || echo "")
  [[ -n "$new" && "$new" != "$leader" ]] && break
  sleep 1
done
[[ -n "$new" && "$new" != "$leader" ]] || fail "no new stream leader after stopping $leader"
msgs=$(app stream info ORDERS --json | json '["state"]["messages"]')
[[ "$msgs" == 11 ]] || fail "expected 11 messages after failover, got $msgs"
app pub shop.orders.created '{"order_id":"after-failover"}' >/dev/null 2>&1
ok "new leader $new, 11 messages intact, publishing works"
eval "${START_NODE_CMD//\{node\}/$leader}" >/dev/null
for i in $(seq 1 60); do
  n=$(nservers)
  [[ "$n" == 3 ]] && break
  sleep 1
done
[[ "$n" == 3 ]] || fail "$leader did not rejoin"
ok "$leader rejoined the cluster"

echo
echo "All $PASS checks passed."
