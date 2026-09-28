/**
 * How the AEGIS runtime contract is loaded from `aegis-proto`.
 *
 * One definition, shared by the gRPC client and by the contract test that
 * encodes and decodes real messages, so the shape the test asserts is the shape
 * the client receives.
 */

import type { Options } from "@grpc/proto-loader";

// In Docker: /app/aegis-proto/proto/aegis_runtime.proto
// In development (from repo root): ./aegis-proto/proto/aegis_runtime.proto
export const PROTO_PATH =
  process.env.PROTO_PATH || "./aegis-proto/proto/aegis_runtime.proto";

export const PROTO_LOADER_OPTIONS: Options = {
  keepCase: true,
  longs: Number,
  enums: String,
  defaults: true,
  oneofs: true,
};
