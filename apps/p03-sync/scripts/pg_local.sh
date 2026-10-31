#!/usr/bin/env bash
# Local PostgreSQL test cluster for Project 03.
#
# This project's P0 gates (G3 checkpoint crash windows, G4 atomic snapshot,
# G6 lease fencing, G7 DB resilience) cannot be satisfied by mocks -- see
# design section 15.1 and WF3-247. This script provisions a throwaway cluster
# with durability ON so those suites run against a real server.
#
# Usage: scripts/pg_local.sh {init|start|stop|status|psql|destroy}
#
# PG_BIN   directory holding initdb/pg_ctl/psql (required unless on PATH)
# PG_DATA  cluster data directory          (default: $PWD/.pgdata)
# PG_PORT  TCP port                        (default: 55432)
set -euo pipefail

PG_BIN="${PG_BIN:-}"
PG_DATA="${PG_DATA:-$PWD/.pgdata}"
PG_PORT="${PG_PORT:-55432}"

bin() {
    if [[ -n "$PG_BIN" ]]; then printf '%s/%s' "$PG_BIN" "$1"; else printf '%s' "$1"; fi
}

init() {
    [[ -f "$PG_DATA/PG_VERSION" ]] && { echo "cluster already initialised at $PG_DATA"; return 0; }
    "$(bin initdb)" -D "$PG_DATA" -U postgres --auth=trust -E UTF8 --locale=C
    cat >> "$PG_DATA/postgresql.conf" <<CONF

# Project 03 test cluster. Durability stays ON: a benchmark or durability claim
# produced with fsync=off is a different durability mode (design 16.4 item 7).
port = $PG_PORT
listen_addresses = 'localhost'
fsync = on
synchronous_commit = on
full_page_writes = on
max_connections = 120
log_min_messages = warning
log_lock_waits = on
deadlock_timeout = '200ms'
CONF
}

start() {
    init
    "$(bin pg_ctl)" -D "$PG_DATA" -l "$PG_DATA/server.log" start > "$PG_DATA/pg_ctl.out" 2>&1 || {
        cat "$PG_DATA/pg_ctl.out"; return 1; }
    for _ in $(seq 1 60); do
        if "$(bin psql)" -h localhost -p "$PG_PORT" -U postgres -tAc 'select 1' >/dev/null 2>&1; then
            echo "postgres ready on port $PG_PORT"
            bootstrap
            return 0
        fi
    done
    echo "postgres did not become ready; see $PG_DATA/server.log" >&2
    return 1
}

# Creates the runtime and test databases plus the two roles the design requires:
# a migration owner that holds DDL and an application role that does not (11.1).
bootstrap() {
    local psql="$(bin psql) -h localhost -p $PG_PORT -U postgres"
    for db in trading_sync trading_sync_test; do
        if ! $psql -tAc "SELECT 1 FROM pg_database WHERE datname='$db'" | grep -q 1; then
            $psql -q -c "CREATE DATABASE $db"
        fi
    done
    for role in sync_migrator sync_app; do
        if ! $psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='$role'" | grep -q 1; then
            $psql -q -c "CREATE ROLE $role LOGIN"
        fi
    done
}

stop()    { "$(bin pg_ctl)" -D "$PG_DATA" -m fast stop; }
status()  { "$(bin pg_ctl)" -D "$PG_DATA" status; }
shell()   { "$(bin psql)" -h localhost -p "$PG_PORT" -U postgres "${@:-trading_sync}"; }
destroy() {
    stop 2>/dev/null || true
    echo "refusing to delete $PG_DATA automatically; remove it by hand if you are sure" >&2
    return 1
}

case "${1:-}" in
    init) init ;; start) start ;; stop) stop ;; status) status ;;
    psql) shift; shell "$@" ;; destroy) destroy ;;
    *) echo "usage: $0 {init|start|stop|status|psql|destroy}" >&2; exit 2 ;;
esac
