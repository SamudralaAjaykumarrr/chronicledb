#!/usr/bin/env bash
# demo-local-cluster.sh starts a real, local three-node ChronicleDB
# cluster (three separate OS processes, real TCP, real disk) so you
# can see leader election, a replicated write, and the observability
# endpoints (docs/observability.md) without writing any code. See
# docs/quickstart.md for the equivalent manual, step-by-step commands.
#
# Everything this script creates lives under a fresh temporary
# directory; it never touches any other file. It never deletes
# anything except that one temporary directory's own contents, and
# only when you ask it to (see the printed cleanup instructions).
#
# Requires: go, curl. Uses only 127.0.0.1 ports.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

echo "Building chronicledb-node..."
BIN_DIR="$(mktemp -d -t chronicledb-demo-bin-XXXXXX)"
go build -o "${BIN_DIR}/chronicledb-node" ./cmd/chronicledb-node

WORK_DIR="$(mktemp -d -t chronicledb-demo-data-XXXXXX)"
echo "Demo cluster data/log directory: ${WORK_DIR}"

IDS=(n1 n2 n3)
declare -A RAFT_ADDR HTTP_ADDR
PORT=19000
for id in "${IDS[@]}"; do
	RAFT_ADDR[$id]="127.0.0.1:$((PORT++))"
	HTTP_ADDR[$id]="127.0.0.1:$((PORT++))"
done
CLUSTER_FLAG=$(
	IFS=,
	echo "${IDS[*]}"
)

PIDS=()
CLEANED_UP=0
cleanup() {
	[[ "$CLEANED_UP" == 1 ]] && return
	CLEANED_UP=1
	echo
	echo "Stopping demo cluster..."
	for pid in "${PIDS[@]:-}"; do
		kill "$pid" 2>/dev/null || true
	done
	wait 2>/dev/null || true
	rm -rf "$BIN_DIR"
	echo "Demo data left at: ${WORK_DIR}"
	echo "(this script never deletes it for you; remove it yourself when done: rm -rf ${WORK_DIR})"
}
trap cleanup EXIT INT TERM

for id in "${IDS[@]}"; do
	peers=""
	for other in "${IDS[@]}"; do
		[[ "$other" == "$id" ]] && continue
		peers+="${other}=${RAFT_ADDR[$other]},"
	done
	peers="${peers%,}"
	mkdir -p "${WORK_DIR}/${id}"
	"${BIN_DIR}/chronicledb-node" \
		-id="$id" -listen="${RAFT_ADDR[$id]}" -http="${HTTP_ADDR[$id]}" \
		-datadir="${WORK_DIR}/${id}" -cluster="$CLUSTER_FLAG" -peers="$peers" \
		>"${WORK_DIR}/${id}.log" 2>&1 &
	PIDS+=("$!")
done

echo "Waiting for the cluster to elect a leader..."
LEADER=""
for _ in $(seq 1 100); do
	for id in "${IDS[@]}"; do
		status="$(curl -s -m 1 "http://${HTTP_ADDR[$id]}/status" 2>/dev/null || true)"
		if [[ "$status" == *'"Role":2'* ]]; then
			LEADER="$id"
			break 2
		fi
	done
	sleep 0.1
done

if [[ -z "$LEADER" ]]; then
	echo "No leader elected within the timeout. Node logs:" >&2
	for id in "${IDS[@]}"; do
		echo "--- ${id} (${WORK_DIR}/${id}.log) ---" >&2
		tail -n 20 "${WORK_DIR}/${id}.log" >&2 || true
	done
	exit 1
fi

echo "Leader elected: ${LEADER}"
echo
echo "Node endpoints (control-plane HTTP, see docs/observability.md):"
for id in "${IDS[@]}"; do
	echo "  ${id}: http://${HTTP_ADDR[$id]}  (raft transport ${RAFT_ADDR[$id]}, log: ${WORK_DIR}/${id}.log)"
done

echo
echo "Proposing a write against the leader (http://${HTTP_ADDR[$LEADER]}/propose)..."
curl -s -X POST "http://${HTTP_ADDR[$LEADER]}/propose" \
	-d '{"requestId":"demo-1","txnId":1,"startSeq":0,"mutations":[{"key":"hello","value":"world"}]}'
echo
echo
echo "Reading it back on a different node (http://${HTTP_ADDR[${IDS[1]}]}/status shows the same committed index):"
curl -s "http://${HTTP_ADDR[${IDS[1]}]}/status"
echo
echo
echo "Try these yourself while the cluster is running:"
echo "  curl http://${HTTP_ADDR[$LEADER]}/status"
echo "  curl http://${HTTP_ADDR[$LEADER]}/metrics"
echo "  curl http://${HTTP_ADDR[$LEADER]}/health"
echo
echo "Press Ctrl+C to stop the demo cluster (this does not delete ${WORK_DIR})."
wait
