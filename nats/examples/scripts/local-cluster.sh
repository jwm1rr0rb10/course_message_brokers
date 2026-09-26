#!/usr/bin/env bash
# Runs the same 3-node cluster as cluster/docker-compose.yml, but as local
# nats-server processes (for people without Docker, and for quick iteration).
#
#   ./scripts/local-cluster.sh start     # nodes on 4222/4223/4224, monitoring 8222/8223/8224
#   ./scripts/local-cluster.sh stop-node nats-2
#   ./scripts/local-cluster.sh start-node nats-2
#   ./scripts/local-cluster.sh stop      # stop everything and delete data
#
# Requires nats-server in PATH (or NATS_SERVER=/path/to/nats-server).
set -euo pipefail

NATS_SERVER="${NATS_SERVER:-nats-server}"
DIR="${LOCAL_CLUSTER_DIR:-${TMPDIR:-/tmp}/nats-course-cluster}"

conf() {
  local i="$1"
  cat <<CONF
server_name: "nats-$i"
port: $((4221 + i))
http: $((8221 + i))

jetstream {
  store_dir: "$DIR/nats-$i/jetstream"
  max_memory_store: 1GB
  max_file_store: 10GB
}

cluster {
  name: "course"
  port: $((6221 + i))
  routes: [
    "nats://127.0.0.1:6222"
    "nats://127.0.0.1:6223"
    "nats://127.0.0.1:6224"
  ]
}

accounts {
  APP: {
    jetstream: enabled
    users: [ { user: "app", password: "app" } ]
  }
  SYS: {
    users: [ { user: "admin", password: "admin" } ]
  }
}
system_account: SYS

# LEARNING ONLY (see module 17)
no_auth_user: app
CONF
}

start_node() {
  local name="$1" i="${1#nats-}"
  mkdir -p "$DIR/$name"
  conf "$i" > "$DIR/$name/nats.conf"
  # detach from the calling shell so the node outlives it
  if command -v setsid >/dev/null 2>&1; then
    setsid nohup "$NATS_SERVER" -c "$DIR/$name/nats.conf" > "$DIR/$name/server.log" 2>&1 < /dev/null &
  else
    nohup "$NATS_SERVER" -c "$DIR/$name/nats.conf" > "$DIR/$name/server.log" 2>&1 < /dev/null &
  fi
  echo $! > "$DIR/$name/pid"
  echo "started $name (pid $(cat "$DIR/$name/pid"))"
}

stop_node() {
  local name="$1"
  if [[ -f "$DIR/$name/pid" ]]; then
    kill "$(cat "$DIR/$name/pid")" 2>/dev/null || true
    rm -f "$DIR/$name/pid"
    echo "stopped $name"
  fi
}

case "${1:-}" in
  start)      for i in 1 2 3; do start_node "nats-$i"; done ;;
  stop)       for i in 1 2 3; do stop_node "nats-$i"; done; sleep 1; rm -rf "$DIR" ;;
  start-node) start_node "$2" ;;
  stop-node)  stop_node "$2" ;;
  *) echo "usage: $0 start|stop|start-node NAME|stop-node NAME" >&2; exit 2 ;;
esac
