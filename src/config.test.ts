import { describe, expect, it, vi } from "vitest";

describe("config", () => {
  it("loads defaults and required values from environment", async () => {
    vi.resetModules();

    process.env.DATABASE_URL =
      "postgresql://temporal:temporal@localhost:5432/aegis";
    process.env.KEYCLOAK_HOST = "http://aegis-iam:8180";
    process.env.KEYCLOAK_SYSTEM_REALM = "aegis-system";
    process.env.KEYCLOAK_CLIENT_ID = "aegis-temporal-worker";
    process.env.KEYCLOAK_CLIENT_SECRET = "test-secret";
    delete process.env.TEMPORAL_ADDRESS;
    delete process.env.HTTP_PORT;
    delete process.env.LOG_LEVEL;

    const { config } = await import("./config.js");

    expect(config.database.url).toBe(
      "postgresql://temporal:temporal@localhost:5432/aegis",
    );
    expect(config.temporal.address).toBe("localhost:7233");
    expect(config.http.port).toBe(3000);
    expect(config.logging.level).toBe("info");
  });

  it("starts without a Keycloak service account when none of its variables is set", async () => {
    vi.resetModules();

    process.env.DATABASE_URL =
      "postgresql://temporal:temporal@localhost:5432/aegis";
    delete process.env.KEYCLOAK_HOST;
    delete process.env.KEYCLOAK_SYSTEM_REALM;
    delete process.env.KEYCLOAK_CLIENT_ID;
    delete process.env.KEYCLOAK_CLIENT_SECRET;

    const { config } = await import("./config.js");

    // `aegis init` deploys no Keycloak, so the worker must come up with no
    // service account at all and fail only when a token is actually needed.
    expect(config.serviceAccount).toBeUndefined();
    expect(config.http.port).toBe(3000);
  });

  it("rejects a partial Keycloak service account by naming the missing variables", async () => {
    vi.resetModules();

    process.env.DATABASE_URL =
      "postgresql://temporal:temporal@localhost:5432/aegis";
    process.env.KEYCLOAK_HOST = "http://aegis-iam:8180";
    process.env.KEYCLOAK_CLIENT_ID = "aegis-temporal-worker";
    delete process.env.KEYCLOAK_SYSTEM_REALM;
    delete process.env.KEYCLOAK_CLIENT_SECRET;

    const { config } = await import("./config.js");

    expect(() => config.serviceAccount).toThrow(
      /missing: KEYCLOAK_SYSTEM_REALM, KEYCLOAK_CLIENT_SECRET/,
    );
  });
});
