#!/bin/bash
# Echo-server comparison: one server process pinned to its own cores, the
# stackfull_net_bench load generator on the others.
#
#   bench/net-compare.sh <sf|raw|tokio> <connections> <bytes> [seconds] [server-workers]
#
# Prints the client's req/s and latency, then the server's CPU and context
# switches per request, from the server's own getrusage() at exit divided
# by every round trip of the run (warm-up included).
#
# Environment: BUILD (default build/release), SERVER_BUILD (server from
# another build, for A/B; default BUILD), TOKIO (tokio-compare binary,
# default bench/tokio-compare/target/release/tokio-compare), SERVER_CPUS
# (default: one hyperthread each of cores 1..<server-workers>; CPU 0 takes
# most interrupts), CLIENT_CPUS (default 8-15) and CLIENT_WORKERS (default
# 8). Servers exit on their own after the run.
set -e
kind=$1; conns=$2; bytes=$3; secs=${4:-3}; workers=${5:-4}
cd "$(dirname "$0")/.."
build=${BUILD:-build/release}
server_build=${SERVER_BUILD:-$build} # A/B: another build's server, same client
tokio=${TOKIO:-bench/tokio-compare/target/release/tokio-compare}
server_cpus=${SERVER_CPUS:-$(seq -s, 2 2 $((2 * workers)))}
client_cpus=${CLIENT_CPUS:-8-15}
client_workers=${CLIENT_WORKERS:-8}
port=$((10000 + RANDOM % 10000)) # below the ephemeral range the client connects from
life=$(echo "$secs + 3" | bc)
usage=$(mktemp -p "$build" net-compare.XXXXXX)
trap 'rm -f "$usage"' EXIT

case $kind in
  sf)    taskset -c "$server_cpus" "$server_build/bench/stackfull_net_bench" server $port $workers $life 2>"$usage" & ;;
  raw)   taskset -c "$server_cpus" "$server_build/bench/stackfull_net_bench" rawserver $port $workers $life 2>"$usage" & ;;
  tokio) taskset -c "$server_cpus" "$tokio" echo-server $port $workers $life 2>"$usage" & ;;
  *) echo "unknown server kind: $kind" >&2; exit 2 ;;
esac
server=$!
sleep 0.5
out=$(taskset -c "$client_cpus" "$build/bench/stackfull_net_bench" client $port $conns $bytes $secs $client_workers)
wait $server || true
trips=$(echo "$out" | awk '/^round trips in all:/ {print $5}')
per=$(awk -v n="$trips" '/^RUSAGE/ && n > 0 {printf "server per request: user %.2f + sys %.2f us", 1e6 * $3 / n, 1e6 * $5 / n}' "$usage")
echo "$(printf '%-5s' $kind) $(echo "$out" | grep -v '^round trips')  $per"
