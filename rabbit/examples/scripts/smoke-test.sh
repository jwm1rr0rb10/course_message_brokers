#!/usr/bin/env bash
# Runs the commands from the RabbitMQ course against the three-node Docker
# cluster (examples/cluster) and checks they behave as the course says.
# Needs: docker, curl, python3. Works with the bash 3.2 that ships with macOS.
# Non-interactive; exits non-zero on the first failed check.
#
# Overrides (defaults match examples/cluster/docker-compose.yml):
#   RABBITMQ_HOST      host where the cluster ports are published (localhost)
#   RABBITMQ_USER/PASS management credentials (admin/admin)
#   RABBITMQ_IMAGE     only used for the log line; the image itself is chosen at
#                      `docker compose up` via the same variable
set -euo pipefail

HOST=${RABBITMQ_HOST:-localhost}
USER_PASS="${RABBITMQ_USER:-admin}:${RABBITMQ_PASS:-admin}"
AUTH="-u $USER_PASS -H content-type:application/json"
api_port() { # node -> published management port
  case "$1" in
    rabbit-1) echo 15672 ;; rabbit-2) echo 15673 ;; rabbit-3) echo 15674 ;;
    *) echo "unknown node $1" >&2; return 1 ;;
  esac
}
API=http://$HOST:15672/api

ctl()   { docker exec "${NODE:-rabbit-1}" rabbitmqctl "$@"; }
queues(){ docker exec "${NODE:-rabbit-1}" rabbitmq-queues "$@"; }
json()  { python3 -c 'import sys,json; d=json.load(sys.stdin); print(eval(sys.argv[1]))' "$1"; }
count() { # vhost queue -> messages (real time, not management stats)
  ctl list_queues -p "$1" name messages --formatter json | json "sum(q['messages'] for q in d if q['name']=='$2')"
}

PASS=0
step() { echo; echo "==> $*"; }
ok()   { PASS=$((PASS+1)); echo "    ok: $*"; }
fail() { echo "    FAIL: $*" >&2; exit 1; }
wait_for() { local n="$1"; shift; for _ in $(seq 1 "$n"); do "$@" && return 0; sleep 1; done; return 1; }
running_nodes() { ctl cluster_status --formatter json 2>/dev/null | json "len(d['running_nodes'])"; }
three_running() { [[ "$(running_nodes)" == 3 ]]; }

# ---------------------------------------------------------------------------
step "wait for the cluster (module 2.3)"
wait_for 120 three_running || fail "cluster did not form 3 running nodes"
ok "3 running nodes in cluster_status"
echo "    image: $(docker inspect -f '{{.Config.Image}}' rabbit-1 2>/dev/null || echo "${RABBITMQ_IMAGE:-?}")," \
  "version: $(ctl version 2>/dev/null || echo '?')"

# clean state for re-runs
for q in tasks payments stock notifications audit unrouted; do
  curl -s $AUTH -X DELETE "$API/queues/%2F/$q" >/dev/null || true
done
curl -s $AUTH -X DELETE "$API/vhosts/shop" >/dev/null || true
ctl clear_policy AE >/dev/null 2>&1 || true

# ---------------------------------------------------------------------------
step "first queue and first message (module 2.4)"
curl -sf $AUTH -X PUT $API/queues/%2F/tasks \
  -d '{"durable": true, "arguments": {"x-queue-type": "quorum"}}' >/dev/null
members=$(queues quorum_status tasks --formatter json | json "len(d)")
[[ "$members" == 3 ]] || fail "tasks should have 3 replicas, quorum_status shows $members"
ok "quorum queue tasks has 3 replicas"

out=$(curl -sf $AUTH -X POST $API/exchanges/%2F/amq.default/publish \
  -d '{"routing_key": "tasks", "payload": "{\"task\":\"resize\",\"image\":\"1.png\"}",
       "payload_encoding": "string", "properties": {"delivery_mode": 2}}')
[[ "$out" == *'"routed":true'* ]] || fail "publish to tasks was not routed: $out"
got=$(curl -sf $AUTH -X POST $API/queues/%2F/tasks/get \
  -d '{"count": 1, "ackmode": "ack_requeue_false", "encoding": "auto"}')
[[ "$got" == *resize* ]] || fail "get did not return the message: $got"
ok "published through the default exchange and fetched it back"

out=$(curl -sf $AUTH -X POST $API/exchanges/%2F/amq.direct/publish \
  -d '{"routing_key": "nobody", "payload": "lost", "payload_encoding": "string", "properties": {}}')
[[ "$out" == *'"routed":false'* ]] || fail "expected routed:false, got $out"
ok "unroutable message: routed=false"

# ---------------------------------------------------------------------------
step "topic topology from module 3.4"
curl -sf $AUTH -X PUT $API/exchanges/%2F/shop.events -d '{"type":"topic","durable":true}' >/dev/null
for q in payments stock notifications audit; do
  curl -sf $AUTH -X PUT $API/queues/%2F/$q -d '{"durable":true,"arguments":{"x-queue-type":"quorum"}}' >/dev/null
done
curl -sf $AUTH -X POST $API/bindings/%2F/e/shop.events/q/payments      -d '{"routing_key":"order.created"}' >/dev/null
curl -sf $AUTH -X POST $API/bindings/%2F/e/shop.events/q/stock         -d '{"routing_key":"order.created"}' >/dev/null
curl -sf $AUTH -X POST $API/bindings/%2F/e/shop.events/q/stock         -d '{"routing_key":"order.cancelled"}' >/dev/null
curl -sf $AUTH -X POST $API/bindings/%2F/e/shop.events/q/notifications -d '{"routing_key":"*.succeeded"}' >/dev/null
curl -sf $AUTH -X POST $API/bindings/%2F/e/shop.events/q/audit         -d '{"routing_key":"#"}' >/dev/null
curl -sf $AUTH -X POST $API/exchanges/%2F/shop.events/publish \
  -d '{"routing_key":"order.created","payload":"{}","payload_encoding":"string","properties":{"delivery_mode":2}}' >/dev/null
sleep 1
for pair in payments:1 stock:1 audit:1 notifications:0; do
  q=${pair%%:*}; want=${pair##*:}
  [[ "$(count / "$q")" == "$want" ]] || fail "$q should hold $want message(s), has $(count / "$q")"
done
ok "order.created reached payments, stock and audit, not notifications"

# ---------------------------------------------------------------------------
step "alternate exchange via policy (module 3.7)"
curl -sf $AUTH -X PUT $API/exchanges/%2F/shop.unrouted -d '{"type":"fanout","durable":true}' >/dev/null
curl -sf $AUTH -X PUT $API/queues/%2F/unrouted -d '{"durable":true,"arguments":{"x-queue-type":"quorum"}}' >/dev/null
curl -sf $AUTH -X POST $API/bindings/%2F/e/shop.unrouted/q/unrouted -d '{"routing_key":""}' >/dev/null
docker exec rabbit-1 rabbitmqctl set_policy AE '^shop\.' \
  '{"alternate-exchange":"shop.unrouted"}' --apply-to exchanges >/dev/null
sleep 1
curl -sf $AUTH -X POST $API/exchanges/%2F/shop.events/publish \
  -d '{"routing_key":"ordr.created","payload":"typo","payload_encoding":"string","properties":{}}' >/dev/null
sleep 1
[[ "$(count / unrouted)" == 1 ]] || fail "the typo message did not reach the unrouted queue"
ok "message with a typo in the key landed in unrouted"

# ---------------------------------------------------------------------------
step "vhost with default queue type quorum (module 4.7) and a policy (module 4.6)"
docker exec rabbit-1 rabbitmqctl add_vhost shop --default-queue-type quorum >/dev/null
ctl set_permissions -p shop admin '.*' '.*' '.*' >/dev/null
curl -sf $AUTH -X PUT $API/queues/shop/implicit -d '{"durable":true}' >/dev/null
type=$(ctl list_queues -p shop name type --formatter json | json "[q['type'] for q in d if q['name']=='implicit'][0]")
[[ "$type" == "quorum" ]] || fail "queue without x-queue-type in vhost shop is $type"
ok "queue declared without a type became quorum"

docker exec rabbit-1 rabbitmqctl set_policy -p shop shop-limits '^shop\.' \
  '{"max-length": 100000, "overflow": "reject-publish", "dead-letter-exchange": "shop.dlx", "delivery-limit": 10}' \
  --apply-to queues --priority 10 >/dev/null
policies=$(ctl list_policies -p shop)
[[ "$policies" == *shop-limits* ]] || fail "policy not listed"
ok "policy applied and listed"

# ---------------------------------------------------------------------------
step "stop the leader of tasks: the quorum queue stays available (module 2 practice, 9.6)"
curl -sf $AUTH -X POST $API/exchanges/%2F/amq.default/publish \
  -d '{"routing_key":"tasks","payload":"survives","payload_encoding":"string","properties":{"delivery_mode":2}}' >/dev/null
leader() { NODE=${1:-rabbit-1} ctl list_queues name leader --formatter json 2>/dev/null \
  | json "[q['leader'] for q in d if q['name']=='tasks'][0]"; }
old=$(leader)
victim=${old#rabbit@}
live=rabbit-1; [[ "$victim" == rabbit-1 ]] && live=rabbit-2
echo "    stopping $victim (leader $old)"
docker stop "$victim" >/dev/null
moved() { local l; l=$(leader "$live"); [[ -n "$l" && "$l" != "$old" ]]; }
wait_for 60 moved || fail "no new leader for tasks"
LIVE_API=http://$HOST:$(api_port "$live")/api
out=$(curl -sf $AUTH -X POST $LIVE_API/exchanges/%2F/amq.default/publish \
  -d '{"routing_key":"tasks","payload":"during failover","payload_encoding":"string","properties":{"delivery_mode":2}}')
[[ "$out" == *'"routed":true'* ]] || fail "publish during failover failed: $out"
[[ "$(NODE=$live count / tasks)" == 2 ]] || fail "tasks should hold 2 messages after failover"
ok "new leader $(leader "$live"), both messages available, publishing works"
docker start "$victim" >/dev/null
wait_for 120 three_running || fail "$victim did not rejoin"
ok "$victim rejoined"

# ---------------------------------------------------------------------------
step "maintenance commands (modules 9.5, 9.8)"
wait_for 60 docker exec rabbit-1 rabbitmq-queues check_if_node_is_quorum_critical >/dev/null 2>&1 \
  || fail "rabbit-1 reported as quorum critical with all nodes up"
ok "check_if_node_is_quorum_critical passes with 3 nodes"
docker exec rabbit-3 rabbitmq-upgrade drain >/dev/null
no_leaders_on_3() { local l; l=$(ctl list_queues name leader --formatter json); [[ "$l" != *rabbit@rabbit-3* ]]; }
wait_for 30 no_leaders_on_3 || fail "leaders still on rabbit-3 after drain"
ok "drain moved every quorum leader off rabbit-3"
docker exec rabbit-3 rabbitmq-upgrade revive >/dev/null
docker exec rabbit-1 rabbitmq-queues rebalance quorum >/dev/null
ok "revive and rebalance quorum ran"

# ---------------------------------------------------------------------------
step "observability and definitions (modules 16.1, 13.5)"
metrics=$(curl -sf "http://$HOST:15692/metrics") || fail "no answer on :15692/metrics"
grep -q '^rabbitmq_' <<<"$metrics" || fail "no rabbitmq_ metrics on :15692"
ok "Prometheus metrics on :15692"
docker exec rabbit-1 rabbitmqctl export_definitions /tmp/definitions.json >/dev/null
docker exec rabbit-1 grep -q '"shop.events"' /tmp/definitions.json || fail "definitions do not contain shop.events"
ok "export_definitions contains the topology"

echo
echo "All $PASS checks passed."
