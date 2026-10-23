import { describe, expect, it, vi } from "vitest";

describe("getServiceToken", () => {
  it("fails at the call, naming the four variables, when no service account is configured", async () => {
    vi.resetModules();

    process.env.DATABASE_URL =
      "postgresql://temporal:temporal@localhost:5432/aegis";
    delete process.env.KEYCLOAK_HOST;
    delete process.env.KEYCLOAK_SYSTEM_REALM;
    delete process.env.KEYCLOAK_CLIENT_ID;
    delete process.env.KEYCLOAK_CLIENT_SECRET;

    // Importing must not throw: the worker starts without Keycloak.
    const { getServiceToken } = await import("./token-manager.js");

    await expect(getServiceToken()).rejects.toThrow(
      /KEYCLOAK_HOST, KEYCLOAK_SYSTEM_REALM, KEYCLOAK_CLIENT_ID and KEYCLOAK_CLIENT_SECRET/,
    );
  });
});
