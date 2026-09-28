// SPDX-License-Identifier: AGPL-3.0-only
// Copyright 2026 100monkeys.ai
//
// Stand-in for the two HTTP services the worker calls besides Temporal and
// Postgres: Keycloak's client-credentials token endpoint, and the
// orchestrator's /v1/temporal-events. Every event is appended to EVENTS_FILE.
// Used by run.sh in CI's Image job.
import http from "node:http";
import { appendFileSync } from "node:fs";

const port = Number(process.env.STUB_PORT || 18088);
const eventsFile = process.env.EVENTS_FILE;

http
  .createServer((req, res) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      if (req.method === "POST" && req.url.endsWith("/protocol/openid-connect/token")) {
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ access_token: "stub-token", expires_in: 300 }));
        return;
      }
      if (req.method === "POST" && req.url === "/v1/temporal-events") {
        let ev = {};
        try { ev = JSON.parse(body); } catch {}
        appendFileSync(eventsFile, JSON.stringify({
          event_type: ev.event_type,
          seq: ev.temporal_sequence_number,
          execution_id: ev.execution_id,
          bytes: body.length,
          auth: req.headers.authorization,
        }) + "\n");
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end("{}");
        return;
      }
      res.writeHead(404);
      res.end();
    });
  })
  .listen(port, "127.0.0.1", () => console.error(`stub listening ${port}`));
