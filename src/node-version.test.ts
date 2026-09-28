/**
 * One Node line, declared once.
 *
 * `.nvmrc` holds the Node major version this worker is built, tested and
 * shipped on. Every other place that names a Node version must agree with
 * it, or this test fails:
 *
 * - both stages of the Dockerfile (the same image, `node:<major>-slim`,
 *   pinned by digest; slim because the Temporal SDK's native core bridge
 *   needs glibc);
 * - every `actions/setup-node` step in every workflow, which must read
 *   `.nvmrc` and not name a version of its own;
 * - `engines.node` and `@types/node` in package.json and the lockfile.
 *
 * CI's Image job checks the other half: that the built image runs that
 * Node and runs a workflow to completion against a Temporal server.
 * Before this test the image ran Node 25, which is past its end of life,
 * CI tested Node 20, the audit ran on Node 22 and engines allowed 20.
 */
import { readdirSync, readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const read = (file: string) => readFileSync(join(root, file), "utf8");

const nvmrc = read(".nvmrc");
const major = nvmrc.trim();
const pkg = JSON.parse(read("package.json"));
const lock = JSON.parse(read("package-lock.json"));
const exact = new RegExp(`^\\^${major}\\.\\d+\\.\\d+$`);

describe("the Node line", () => {
  it(".nvmrc holds a bare major version and nothing else", () => {
    expect(nvmrc).toMatch(/^\d+\n$/);
  });

  it("both Dockerfile stages use node:<major>-slim, pinned by the same digest", () => {
    const images = read("Dockerfile")
      .split(/\r?\n/)
      .filter((l) => /^FROM\s/i.test(l))
      .map((l) => l.split(/\s+/)[1]);
    expect(images).toHaveLength(2);
    for (const image of images) {
      expect(image).toMatch(new RegExp(`^node:${major}-slim@sha256:[0-9a-f]{64}$`));
    }
    expect(new Set(images).size).toBe(1);
  });

  it("every setup-node step in every workflow reads .nvmrc", () => {
    const dir = join(root, ".github/workflows");
    let steps = 0;
    for (const file of readdirSync(dir).filter((f) => /\.ya?ml$/.test(f))) {
      const lines = readFileSync(join(dir, file), "utf8").split("\n");
      const uses = lines.filter((l) => /uses:\s*actions\/setup-node@/.test(l)).length;
      const reads = lines.filter((l) => /^\s*node-version-file:\s*"?\.nvmrc"?\s*$/.test(l)).length;
      steps += uses;
      expect(`${file}: ${reads} of ${uses} read .nvmrc`).toBe(`${file}: ${uses} of ${uses} read .nvmrc`);
      expect(lines.filter((l) => /^\s*node-version:/.test(l)).map((l) => `${file}: ${l.trim()}`)).toEqual([]);
    }
    // ci.yml sets up Node in two jobs, npm-publish.yml in two, security.yml
    // in one.
    expect(steps).toBe(5);
  });

  it("engines.node allows this major and no other", () => {
    expect(pkg.engines.node).toMatch(exact);
    expect(lock.packages[""].engines.node).toBe(pkg.engines.node);
  });

  it("@types/node describes this major", () => {
    const range = pkg.devDependencies["@types/node"] as string;
    expect(range).toMatch(exact);
    expect(lock.packages[""].devDependencies["@types/node"]).toBe(range);
    const resolved = lock.packages["node_modules/@types/node"].version as string;
    expect(resolved.split(".")[0]).toBe(major);
  });
});
