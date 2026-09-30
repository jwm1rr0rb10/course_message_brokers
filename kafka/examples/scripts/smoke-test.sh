#!/usr/bin/env bash
# Runs the CLI commands from the Kafka course against the three-node Docker
# cluster (examples/cluster) and checks they behave as the course says.
#
# Needs: docker and the kprobe binary (go build ./cmd/kprobe in examples/go).
#   KPROBE=path/to/kprobe ./examples/scripts/smoke-test.sh
# Non-interactive, exits non-zero on the first failed check, works with the
# bash 3.2 that ships with macOS. Brokers it stops are started again on exit.
set -euo pipefail

KPROBE="${KPROBE:-kprobe}"
NODE="kafka-1"                           # container the CLI tools run in; switched when it is stopped
BS="$NODE:19092"                         # its INTERNAL listener, used from inside the containers
use_node_except() { # pick a running node other than $1 for kt/leader_of
  local n
  for n in kafka-1 kafka-2 kafka-3; do
    if [[ "$n" != "$1" ]]; then NODE="$n"; BS="$n:19092"; return 0; fi
  done
}
# kt TOOL ARGS...: run /opt/kafka/bin/TOOL inside $NODE
kt()  { local tool=$1; shift; docker exec    "$NODE" "/opt/kafka/bin/$tool" "$@" </dev/null; }  # no stdin
kti() { local tool=$1; shift; docker exec -i "$NODE" "/opt/kafka/bin/$tool" "$@"; }             # reads stdin (producer input)
restart_all() { # never leave a stopped broker behind, whatever happened
  local n
  for n in kafka-1 kafka-2 kafka-3; do docker start "$n" >/dev/null 2>&1 || true; done
}
trap restart_all EXIT

PASS=0
step() { echo; echo "==> $*"; }
ok()   { PASS=$((PASS+1)); echo "    ok: $*"; }
fail() { echo "    FAIL: $*" >&2; exit 1; }
leader_of() { # topic partition -> broker id of the leader
  kt kafka-topics.sh --bootstrap-server "$BS" --describe --topic "$1" \
    | awk -v p="$2" '$3=="Partition:" && $4==p { for (i=1;i<=NF;i++) if ($i=="Leader:") print $(i+1) }'
}
wait_for() { # seconds, command...
  local n="$1"; shift
  for _ in $(seq 1 "$n"); do "$@" && return 0; sleep 1; done
  return 1
}
no_urp() { [[ -z "$(kt kafka-topics.sh --bootstrap-server "$BS" --describe --under-replicated-partitions 2>/dev/null)" ]]; }

# ---------------------------------------------------------------------------
step "wait for the cluster (module 2.2)"
wait_for 90 kt kafka-topics.sh --bootstrap-server "$BS" --list >/dev/null 2>&1 || fail "cluster not ready"
voters=$(kt kafka-metadata-quorum.sh --bootstrap-server "$BS" describe --replication | grep -cE 'Leader|Follower' || true)
[[ "$voters" -ge 3 ]] || fail "expected 3 nodes in the controller quorum, got $voters"
ok "KRaft quorum with 3 nodes"

for t in orders probe; do kt kafka-topics.sh --bootstrap-server "$BS" --delete --topic "$t" >/dev/null 2>&1 || true; done
sleep 2

# ---------------------------------------------------------------------------
step "create the orders topic exactly as in module 2.3"
kt kafka-topics.sh --bootstrap-server kafka-1:19092 \
  --create --topic orders \
  --partitions 3 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config retention.ms=604800000 >/dev/null
desc=$(kt kafka-topics.sh --bootstrap-server "$BS" --describe --topic orders)
echo "$desc" | grep -q "ReplicationFactor: 3" || fail "orders should have RF 3: $desc"
echo "$desc" | grep -q "min.insync.replicas=2" || fail "min.insync.replicas not set: $desc"
ok "orders: 3 partitions, RF 3, min.insync.replicas=2"

docker exec "$NODE" sh -c 'ls /var/lib/kafka/data' | grep -q '^orders-' \
  || fail "partition directories are not in /var/lib/kafka/data (check KAFKA_LOG_DIRS)"
ok "data lives in /var/lib/kafka/data on the volume (module 2.6)"

# ---------------------------------------------------------------------------
step "keyed messages keep order within one partition (module 2.4)"
printf '%s\n' \
  'order-1:{"event":"OrderCreated","order_id":"order-1"}' \
  'order-2:{"event":"OrderCreated","order_id":"order-2"}' \
  'order-1:{"event":"OrderPaid","order_id":"order-1"}' \
  'order-1:{"event":"OrderShipped","order_id":"order-1"}' \
| kti kafka-console-producer.sh --bootstrap-server "$BS" --topic orders \
    --property parse.key=true --property key.separator=: >/dev/null

out=$(kt kafka-console-consumer.sh --bootstrap-server "$BS" --topic orders --group billing \
  --from-beginning --max-messages 4 --timeout-ms 30000 \
  --property print.key=true --property print.partition=true 2>/dev/null)
parts=$(echo "$out" | awk -F'\t' '$2=="order-1" {print $1}' | sort -u | wc -l | tr -d ' ')
seq_ok=$(echo "$out" | awk -F'\t' '$2=="order-1" {print $3}' | grep -o '"event":"[A-Za-z]*"' | tr '\n' ' ')
[[ "$parts" == 1 ]] || fail "order-1 events spread over $parts partitions: $out"
[[ "$seq_ok" == '"event":"OrderCreated" "event":"OrderPaid" "event":"OrderShipped" ' ]] \
  || fail "order-1 events out of order: $seq_ok"
ok "all order-1 events in one partition, in order"

# ---------------------------------------------------------------------------
step "consumer group offsets (modules 2.5, 5.9)"
lags=$(kt kafka-consumer-groups.sh --bootstrap-server "$BS" --describe --group billing 2>/dev/null \
  | awk '$1=="billing" && $2=="orders" {print $6}' | sort -u | tr '\n' ' ')
[[ "$lags" == "0 " ]] || fail "billing should have lag 0 on every partition, got: $lags"
ok "billing group committed everything (lag 0)"

kt kafka-consumer-groups.sh --bootstrap-server "$BS" --group billing --topic orders \
  --reset-offsets --to-earliest --execute >/dev/null
total_lag=$(kt kafka-consumer-groups.sh --bootstrap-server "$BS" --describe --group billing 2>/dev/null \
  | awk '$1=="billing" && $2=="orders" {s+=$6} END {print s}')
[[ "$total_lag" == 4 ]] || fail "after reset to earliest the lag should be 4, got $total_lag"
ok "reset to earliest: the group will re-read all 4 messages"

# ---------------------------------------------------------------------------
step "topic configuration (module 3.6)"
kt kafka-configs.sh --bootstrap-server "$BS" --alter --entity-type topics --entity-name orders \
  --add-config retention.ms=1209600000 >/dev/null
kt kafka-configs.sh --bootstrap-server "$BS" --describe --entity-type topics --entity-name orders \
  | grep -q "retention.ms=1209600000" || fail "retention.ms not updated"
ok "retention.ms changed with kafka-configs.sh"

# ---------------------------------------------------------------------------
step "min.insync.replicas vs acks (module 8.2)"
kt kafka-topics.sh --bootstrap-server "$BS" --create --topic probe --partitions 1 \
  --replication-factor 3 --config min.insync.replicas=3 >/dev/null
sleep 2
"$KPROBE" -topic probe -acks all >/dev/null || fail "acks=all should work with all 3 replicas up"
ok "acks=all with 3/3 replicas in ISR"

victim=kafka-3
if [[ "$(leader_of probe 0)" == 3 ]]; then victim=kafka-2; fi
use_node_except "$victim"
docker stop "$victim" >/dev/null
isr_shrunk() { # newer versions print extra columns (Elr, LastKnownElr) after Isr
  local isr
  isr=$(kt kafka-topics.sh --bootstrap-server "$BS" --describe --topic probe \
    | awk '{for (i=1;i<=NF;i++) if ($i=="Isr:") print $(i+1)}')
  [[ -n "$isr" && $(tr ',' '\n' <<< "$isr" | wc -l) -eq 2 ]]
}
wait_for 60 isr_shrunk || fail "ISR did not shrink after stopping $victim"
if "$KPROBE" -topic probe -acks all -timeout 10s >/dev/null; then
  fail "acks=all succeeded with 2 replicas in ISR and min.insync.replicas=3"
fi
ok "acks=all rejected: ISR (2) < min.insync.replicas (3)"
"$KPROBE" -topic probe -acks leader >/dev/null || fail "acks=1 should still work"
ok "acks=1 still accepted: min.insync.replicas only applies to acks=all"
docker start "$victim" >/dev/null
wait_for 120 no_urp || fail "$victim did not catch up"
ok "$victim is back in sync"
use_node_except none

# ---------------------------------------------------------------------------
step "stop the leader of orders/0: data stays available (module 2.7 practice)"
old=$(leader_of orders 0)
[[ "$old" =~ ^[123]$ ]] || fail "unexpected leader of orders/0: '$old'"
use_node_except "kafka-$old"            # the CLI must not run inside the broker we stop
docker stop "kafka-$old" >/dev/null
new_leader() { local l; l=$(leader_of orders 0); [[ -n "$l" && "$l" != "$old" && "$l" != "none" ]]; }
wait_for 60 new_leader || fail "no new leader for orders/0"
count=$(kt kafka-console-consumer.sh --bootstrap-server "$BS" --topic orders --from-beginning \
  --max-messages 4 --timeout-ms 30000 2>/dev/null | wc -l | tr -d ' ')
[[ "$count" == 4 ]] || fail "expected 4 messages after failover, got $count"
ok "leader moved from $old to $(leader_of orders 0), all 4 messages readable"
docker start "kafka-$old" >/dev/null
wait_for 120 no_urp || fail "kafka-$old did not catch up"
use_node_except none

kt kafka-leader-election.sh --bootstrap-server "$BS" --election-type preferred --all-topic-partitions >/dev/null 2>&1 || true
ok "kafka-$old rejoined; preferred leader election ran"

# ---------------------------------------------------------------------------
step "health checks from module 8.7"
for flag in --under-replicated-partitions --under-min-isr-partitions --unavailable-partitions; do
  [[ -z "$(kt kafka-topics.sh --bootstrap-server "$BS" --describe $flag)" ]] || fail "$flag is not empty"
done
ok "no under-replicated, under-min-ISR or unavailable partitions"

step "share groups tooling is present (module 12.5)"
kt kafka-features.sh --bootstrap-server "$BS" describe | sed 's/^/    /'
kt kafka-share-groups.sh --bootstrap-server "$BS" --list >/dev/null || fail "kafka-share-groups.sh --list failed"
ok "kafka-share-groups.sh works"

echo
echo "All $PASS checks passed."
