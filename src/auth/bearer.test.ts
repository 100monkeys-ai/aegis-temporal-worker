import { describe, expect, it } from "vitest";
import { bearerHeaders } from "./bearer.js";

describe("bearerHeaders", () => {
  it("carries the token as a Bearer header", () => {
    expect(bearerHeaders("t0k")).toEqual({ Authorization: "Bearer t0k" });
  });

  it("sends no Authorization header at all when there is no token", () => {
    expect(bearerHeaders(undefined)).toEqual({});
  });
});
