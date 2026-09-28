#!/usr/bin/env bash
# SPDX-License-Identifier: AGPL-3.0-only
# Copyright 2026 100monkeys.ai
#
# Runs the worker image against a real Temporal server and runs one
# aegis_workflow to completion. Used by CI's Image job.
#
# It checks what unit tests cannot: that the image runs the Node in .nvmrc,
# that the Temporal SDK's native core bridge loads in it, and that the
# worker connects to Temporal, registers a workflow definition, and runs
# a workflow with a Human state (a signal of 300 KB, then a timer) to the
# end.
#
# Needs: the image, a Postgres on 127.0.0.1:5432 (user and database
# "worker", no password), Node on the runner for the stand-in, and two
# Temporal CLIs: $TEMPORAL_SERVER, whose bundled dev server is the Temporal
# Server version production runs, and $TEMPORAL, a current CLI for the
# client commands (the older CLI has no `workflow result`).
#
# usage: run.sh <image>
set -euo pipefail

image="$1"
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../.." && pwd)"
work="${RUNNER_TEMP:-/tmp}/image-check"
mkdir -p "$work"
temporal="${TEMPORAL:-temporal}"
temporal_server="${TEMPORAL_SERVER:-$temporal}"
address=127.0.0.1:7233
pids=()

cleanup() {
  docker rm -f worker-check >/dev/null 2>&1 || true
  for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
}
trap cleanup EXIT

major="$(cat "$root/.nvmrc")"
node="$(docker run --rm "$image" node -p process.versions.node)"
echo "The image runs Node ${node}; .nvmrc says ${major}."
test "${node%%.*}" = "${major}"

# The core bridge is the SDK's native addon; loading it proves the image
# has the glibc and the prebuilt binary it needs.
docker run --rm "$image" node -e '
  const bridge = require("@temporalio/core-bridge");
  if (typeof bridge.native !== "object") throw new Error("no native bridge");
  console.log("core bridge loaded:", require.resolve("@temporalio/core-bridge"));
'

psql -h 127.0.0.1 -U worker -d worker -v ON_ERROR_STOP=1 -qc "
  CREATE TABLE workflow_definitions (
    workflow_id TEXT PRIMARY KEY, tenant_id TEXT, name TEXT, version TEXT,
    scope TEXT, definition JSONB NOT NULL, registered_at TIMESTAMPTZ,
    definition_hash TEXT)"

EVENTS_FILE="$work/events.jsonl" STUB_PORT=18088 node "$here/stand-in.mjs" 2>"$work/stand-in.err" &
pids+=($!)

"$temporal_server" server start-dev --ip 127.0.0.1 --port 7233 --headless --log-level warn \
  >"$work/temporal.out" 2>"$work/temporal.err" &
pids+=($!)
for _ in $(seq 1 60); do
  "$temporal" operator cluster health --address "$address" >/dev/null 2>&1 && break
  sleep 1
done
"$temporal" operator cluster health --address "$address"

docker run -d --name worker-check --network host \
  -e TEMPORAL_ADDRESS="$address" \
  -e DATABASE_URL=postgresql://worker@127.0.0.1:5432/worker \
  -e AEGIS_ORCHESTRATOR_URL=http://127.0.0.1:18088 \
  -e KEYCLOAK_HOST=http://127.0.0.1:18088 \
  -e KEYCLOAK_SYSTEM_REALM=aegis-system \
  -e KEYCLOAK_CLIENT_ID=image-check \
  -e KEYCLOAK_CLIENT_SECRET=image-check \
  -e HTTP_PORT=13000 -e HTTP_HOST=127.0.0.1 -e METRICS_BIND=127.0.0.1 \
  -e LOG_LEVEL=info \
  "$image" >/dev/null

for _ in $(seq 1 60); do
  if curl -fsS http://127.0.0.1:13000/health >/dev/null 2>&1 &&
    docker logs worker-check 2>&1 | grep -q "Temporal worker created"; then
    break
  fi
  sleep 1
done
curl -fsS http://127.0.0.1:13000/health
echo

definition='{"workflow_id":"11111111-2222-3333-4444-555555555555","tenant_id":"tenant-image-check","name":"image-check-human","version":"1.0.0","initial_state":"ask","context":{},"states":{"ask":{"kind":"Human","prompt":"Approve?","timeout":"120s","transitions":[{"condition":"always","target":"wait"}]},"wait":{"kind":"Human","prompt":"Anything else?","timeout":"2s","default_response":"auto","transitions":[]}}}'
echo "register: $(curl -fsS -X POST -H 'Content-Type: application/json' --data "$definition" http://127.0.0.1:13000/register-workflow)"

execution="image-check-$(date +%s)"
"$temporal" workflow start --address "$address" --task-queue aegis-agents \
  --type aegis_workflow --workflow-id "$execution" \
  --input '{"workflow_id":"11111111-2222-3333-4444-555555555555","input":{},"tenant_id":"tenant-image-check"}'
sleep 3
node -e 'process.stdout.write(JSON.stringify("x".repeat(300000)))' >"$work/signal.json"
"$temporal" workflow signal --address "$address" --workflow-id "$execution" \
  --name humanInput --input-file "$work/signal.json"
timeout 120 "$temporal" workflow result --address "$address" \
  --workflow-id "$execution" -o json >"$work/result.json"

node - "$work/result.json" "$work/events.jsonl" <<'JS'
const fs = require("node:fs");
const [resultFile, eventsFile] = process.argv.slice(2);
const d = JSON.parse(fs.readFileSync(resultFile, "utf8"));
let r = d.result;
if (Array.isArray(r)) r = r[0];
const answer = r?.blackboard?.ask?.response;
console.log(
  `temporal status ${d.status}; workflow status ${r?.status}; ` +
    `final state ${r?.final_state}; answer ${String(answer ?? "").length} characters`,
);
const events = fs.readFileSync(eventsFile, "utf8").trim().split("\n").map((l) => JSON.parse(l).event_type);
console.log(`events posted to the orchestrator: ${events.join(" ")}`);
if (d.status !== "COMPLETED") throw new Error(`workflow did not complete: ${d.status}`);
if (String(answer).length !== 300000) throw new Error("the signal did not reach the blackboard");
if (events.length === 0) throw new Error("no event reached the orchestrator");
JS

echo "worker log lines at warn or error: $(docker logs worker-check 2>&1 | grep -ciE '"level":(40|50)|ERROR' || true)"
