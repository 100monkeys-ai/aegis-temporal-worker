/**
 * The AEGIS runtime wire contract, exercised with the real libraries.
 *
 * `client.test.ts` mocks `@grpc/proto-loader` and `@grpc/grpc-js`, so nothing
 * there encodes or decodes a message. These tests load the vendored
 * `aegis-proto` with the options the client uses and put real bytes through
 * `protobufjs` and a real `@grpc/grpc-js` server and client, so a change in any
 * of those three libraries that alters what the worker sends or receives turns
 * this file red.
 */

import * as grpc from "@grpc/grpc-js";
import * as protoLoader from "@grpc/proto-loader";
import { afterEach, describe, expect, it } from "vitest";
import { PROTO_LOADER_OPTIONS, PROTO_PATH } from "./proto-options.js";

const packageDefinition = protoLoader.loadSync(
  PROTO_PATH,
  PROTO_LOADER_OPTIONS,
);

const runtimeService = packageDefinition[
  "aegis.runtime.v1.AegisRuntime"
] as protoLoader.ServiceDefinition;

const executeAgent = runtimeService.ExecuteAgent;

describe("aegis.runtime.v1 wire contract", () => {
  it("encodes an ExecutionEvent oneof to the exact protobuf bytes", () => {
    const bytes = executeAgent.responseSerialize({
      execution_started: {
        execution_id: "e1",
        agent_id: "a",
        started_at: "t",
      },
    });

    // ExecutionEvent field 1 (length-delimited, 10 bytes) wrapping
    // ExecutionStarted { 1: "e1", 2: "a", 3: "t" }.
    expect(bytes.toString("hex")).toBe("0a0a0a0265311201611a0174");
  });

  it("decodes an ExecutionEvent oneof into the case name and its payload", () => {
    const bytes = executeAgent.responseSerialize({
      iteration_failed: {
        execution_id: "exec-1",
        iteration_number: 3,
        error: { error_type: "Timeout", message: "took too long" },
        failed_at: "2026-09-28T00:00:00Z",
      },
    });

    const decoded = executeAgent.responseDeserialize(bytes) as Record<
      string,
      any
    >;

    expect(decoded.event).toBe("iteration_failed");
    expect(decoded.iteration_failed).toMatchObject({
      execution_id: "exec-1",
      iteration_number: 3,
      error: { error_type: "Timeout", message: "took too long" },
      failed_at: "2026-09-28T00:00:00Z",
    });
  });

  it("round-trips an ExecuteAgentRequest with multi-byte text and int64 sizes", () => {
    const input = "résumé — 日本語 — 🧪 é";
    const request = {
      agent_id: "agent-1",
      input,
      context_json: JSON.stringify({ note: input }),
      tenant_id: "tenant-a",
      intent: "Ünïcödé intent",
      attachments: [
        {
          volume_id: "vol-1",
          path: "/files/ドキュメント.pdf",
          name: "ドキュメント.pdf",
          mime_type: "application/pdf",
          size: 2 ** 40 + 7,
        },
      ],
    };

    const decoded = executeAgent.requestDeserialize(
      executeAgent.requestSerialize(request),
    ) as Record<string, any>;

    expect(decoded).toMatchObject(request);
    expect(typeof decoded.attachments[0].size).toBe("number");
    expect(decoded.workflow_execution_id).toBeUndefined();
  });

  it("round-trips a RunImageGenerateRequest with every optional argument, count an integer", () => {
    const method = runtimeService.RunImageGenerate;
    const request = {
      workflow_execution_id: "exec-1",
      prompt: "a grey monkey reading under a tree — 日本語",
      alias: "image",
      size: "1024x1024",
      count: 2,
      path: "generated/",
    };

    const decoded = method.requestDeserialize(
      method.requestSerialize(request),
    ) as Record<string, unknown>;

    expect(decoded).toMatchObject(request);
    expect(typeof decoded.count).toBe("number");
  });

  it("round-trips a RunImageGenerateRequest with no optional argument, each absent", () => {
    const method = runtimeService.RunImageGenerate;
    const decoded = method.requestDeserialize(
      method.requestSerialize({
        workflow_execution_id: "exec-1",
        prompt: "a monkey",
      }),
    ) as Record<string, unknown>;

    expect(decoded.workflow_execution_id).toBe("exec-1");
    expect(decoded.prompt).toBe("a monkey");
    expect(decoded.alias).toBeUndefined();
    expect(decoded.size).toBeUndefined();
    expect(decoded.count).toBeUndefined();
    expect(decoded.path).toBeUndefined();
  });

  it("round-trips a RunImageGenerateResponse with one GeneratedFile and no sentence", () => {
    const method = runtimeService.RunImageGenerate;
    const file = {
      volume_id: "9f0b2e1f-02cb-4e5b-9e45-51b16e5beaf6",
      path: "generated/1.png",
      content_type: "image/png",
      size_bytes: 2 ** 40 + 7,
    };

    const decoded = method.responseDeserialize(
      method.responseSerialize({ files: [file] }),
    ) as Record<string, any>;

    expect(decoded.files).toEqual([file]);
    expect(typeof decoded.files[0].size_bytes).toBe("number");
    expect(decoded.sentence).toBeUndefined();
  });

  it("reads a RunImageGenerateResponse refusal with its sentence and no files", () => {
    const method = runtimeService.RunImageGenerate;
    const decoded = method.responseDeserialize(
      method.responseSerialize({
        sentence: "this node has no image generation model",
      }),
    ) as Record<string, any>;

    expect(decoded.files).toEqual([]);
    expect(decoded.sentence).toBe("this node has no image generation model");
  });

  describe("over a real @grpc/grpc-js channel", () => {
    let server: grpc.Server | undefined;
    let client: grpc.Client | undefined;

    afterEach(() => {
      client?.close();
      server?.forceShutdown();
      client = undefined;
      server = undefined;
    });

    it("streams ExecuteAgent events from server to client", async () => {
      const proto = grpc.loadPackageDefinition(packageDefinition) as any;
      const AegisRuntime = proto.aegis.runtime.v1.AegisRuntime;

      let received: Record<string, any> | undefined;
      server = new grpc.Server();
      server.addService(AegisRuntime.service, {
        ExecuteAgent: (call: grpc.ServerWritableStream<any, any>) => {
          received = call.request;
          call.write({
            execution_started: {
              execution_id: "exec-9",
              agent_id: call.request.agent_id,
              started_at: "2026-09-28T00:00:00Z",
            },
          });
          call.write({
            execution_completed: {
              execution_id: "exec-9",
              final_output: `echo: ${call.request.input}`,
              total_iterations: 2,
              completed_at: "2026-09-28T00:00:01Z",
            },
          });
          call.end();
        },
      });

      const port = await new Promise<number>((resolve, reject) => {
        server!.bindAsync(
          "127.0.0.1:0",
          grpc.ServerCredentials.createInsecure(),
          (err, boundPort) => (err ? reject(err) : resolve(boundPort)),
        );
      });

      client = new AegisRuntime(
        `127.0.0.1:${port}`,
        grpc.credentials.createInsecure(),
      );

      const events = await new Promise<Record<string, any>[]>(
        (resolve, reject) => {
          const collected: Record<string, any>[] = [];
          const call = (client as any).ExecuteAgent({
            agent_id: "agent-7",
            input: "héllo 🌍",
            tenant_id: "tenant-a",
          });
          call.on("data", (event: Record<string, any>) =>
            collected.push(event),
          );
          call.on("error", reject);
          call.on("end", () => resolve(collected));
        },
      );

      expect(received).toMatchObject({
        agent_id: "agent-7",
        input: "héllo 🌍",
        tenant_id: "tenant-a",
      });
      expect(events.map((e) => e.event)).toEqual([
        "execution_started",
        "execution_completed",
      ]);
      expect(events[0].execution_started).toMatchObject({
        execution_id: "exec-9",
        agent_id: "agent-7",
      });
      expect(events[1].execution_completed).toMatchObject({
        execution_id: "exec-9",
        final_output: "echo: héllo 🌍",
        total_iterations: 2,
      });
    });
  });
});
