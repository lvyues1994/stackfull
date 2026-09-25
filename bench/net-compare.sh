#!/bin/bash
# Echo-server comparison: one server process pinned to its own cores, the
# stackfull_net_bench load generator on the others, server CPU read from
# /proc while the client measures.
#
#   bench/net-compare.sh <sf|raw|tokio> <connections> <bytes> [seconds] [server-workers]
#
# Environment: BUILD (default build/release), TOKIO (tokio-compare binary,
# default bench/tokio-compare/target/release/tokio-compare), SERVER_CPUS
# (default: one hyperthread of each of the first <server-workers> cores),
# CLIENT_CPUS (default 8-23). Servers exit on their own after the run.
set -e
kind=$1; conns=$2; bytes=$3; secs=${4:-3}; workers=${5:-4}
cd "$(dirname "$0")/.."
build=${BUILD:-build/release}
tokio=${TOKIO:-bench/tokio-compare/target/release/tokio-compare}
server_cpus=${SERVER_CPUS:-$(seq -s, 0 2 $((2 * workers - 2)))}
client_cpus=${CLIENT_CPUS:-8-23}
port=$((20000 + RANDOM % 20000))
life=$(echo "$secs + 4" | bc)

case $kind in
  sf)    taskset -c "$server_cpus" "$build/bench/stackfull_net_bench" server $port $workers $life & ;;
  raw)   taskset -c "$server_cpus" "$build/bench/stackfull_net_bench" rawserver $port $workers $life & ;;
  tokio) taskset -c "$server_cpus" "$tokio" echo-server $port $workers $life & ;;
  *) echo "unknown server kind: $kind" >&2; exit 2 ;;
esac
pid=$!
sleep 0.5
times() { awk '{print $14, $15}' /proc/$pid/stat; }
out=$(taskset -c "$client_cpus" "$build/bench/stackfull_net_bench" client $port $conns $bytes $secs 16 &
      client=$!
      sleep 0.8; read -r u0 s0 < <(times); t0=$(date +%s.%N)
      sleep $secs; read -r u1 s1 < <(times); t1=$(date +%s.%N)
      echo "CPU $(echo "($u1 - $u0) / 100 / ($t1 - $t0)" | bc -l) $(echo "($s1 - $s0) / 100 / ($t1 - $t0)" | bc -l)"
      wait $client)
wait $pid 2>/dev/null || true
cpu=$(echo "$out" | awk '/^CPU/ {printf "user %.2f + sys %.2f cores", $2, $3}')
echo "$(printf '%-5s' $kind) $(echo "$out" | grep -v '^CPU')  server $cpu"
