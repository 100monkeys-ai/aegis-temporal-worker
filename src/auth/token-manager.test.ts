import { describe, expect, it, vi } from "vitest";

describe("getServiceToken", () => {
  it("resolves to no token, without throwing at import, when no service account is configured", async () => {
    vi.resetModules();

    process.env.DATABASE_URL =
      "postgresql://temporal:temporal@localhost:5432/aegis";
    delete process.env.KEYCLOAK_HOST;
    delete process.env.KEYCLOAK_SYSTEM_REALM;
    delete process.env.KEYCLOAK_CLIENT_ID;
    delete process.env.KEYCLOAK_CLIENT_SECRET;

    // Importing must not throw: the worker starts without Keycloak.
    const { getServiceToken } = await import("./token-manager.js");

    // The `aegis init` stack has no Keycloak and its orchestrator has no
    // `spec.iam`, so the call goes out without a token rather than failing.
    await expect(getServiceToken()).resolves.toBeUndefined();
  });
});
