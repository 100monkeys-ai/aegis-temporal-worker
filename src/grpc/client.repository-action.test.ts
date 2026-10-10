/**
 * The Forge's wire contract, with the real libraries: the vendored
 * `aegis-proto` loads with the client's options and declares
 * RunRepositoryAction and ExecuteContainerRunRequest's `network_mode`, and
 * their messages round-trip through protobufjs as the worker sends and reads
 * them. `client.test.ts` mocks the loader, so it cannot hold this.
 */

import * as protoLoader from "@grpc/proto-loader";
import { describe, expect, it } from "vitest";
import { PROTO_LOADER_OPTIONS, PROTO_PATH } from "./proto-options.js";

const packageDefinition = protoLoader.loadSync(
  PROTO_PATH,
  PROTO_LOADER_OPTIONS,
);

const runtimeService = packageDefinition[
  "aegis.runtime.v1.AegisRuntime"
] as protoLoader.ServiceDefinition;

describe("RunRepositoryAction on the wire", () => {
  it("is a method of the runtime service", () => {
    expect(runtimeService.RunRepositoryAction?.path).toBe(
      "/aegis.runtime.v1.AegisRuntime/RunRepositoryAction",
    );
  });

  it("round-trips a commit's request with its message", () => {
    const method = runtimeService.RunRepositoryAction;
    const decoded = method.requestDeserialize(
      method.requestSerialize({
        workflow_execution_id: "exec-1",
        action: "commit",
        message: "the-forge: name the judge",
      }),
    ) as Record<string, unknown>;

    expect(decoded).toMatchObject({
      workflow_execution_id: "exec-1",
      action: "commit",
      message: "the-forge: name the judge",
    });
  });

  it("reads a refusal with its sentence and no commit", () => {
    const method = runtimeService.RunRepositoryAction;
    const decoded = method.responseDeserialize(
      method.responseSerialize({
        branch: "aegis/run-exec-1",
        ref: "main",
        sentence: "this run holds no repository",
      }),
    ) as Record<string, unknown>;

    expect(decoded.branch).toBe("aegis/run-exec-1");
    expect(decoded.ref).toBe("main");
    expect(decoded.sentence).toBe("this run holds no repository");
    expect(decoded.commit_sha).toBeUndefined();
    expect(decoded.diff).toBeUndefined();
  });

  it("reads a landing's commit sha", () => {
    const method = runtimeService.RunRepositoryAction;
    const decoded = method.responseDeserialize(
      method.responseSerialize({
        commit_sha: "abc1234",
        branch: "aegis/run-exec-1",
        ref: "main",
      }),
    ) as Record<string, unknown>;

    expect(decoded.commit_sha).toBe("abc1234");
    expect(decoded.sentence).toBeUndefined();
  });
});

describe("ExecuteContainerRunRequest on the wire", () => {
  it("carries network_mode", () => {
    const method = runtimeService.ExecuteContainerRun;
    const decoded = method.requestDeserialize(
      method.requestSerialize({
        execution_id: "exec-1",
        name: "EXECUTE_TESTS",
        image: "python:3.11-slim",
        command: ["pytest"],
        volumes: [
          { name: "repository", mount_path: "/workspace", read_only: false },
        ],
        network_mode: "egress",
      }),
    ) as Record<string, any>;

    expect(decoded.network_mode).toBe("egress");
    expect(decoded.volumes[0].name).toBe("repository");
  });
});
