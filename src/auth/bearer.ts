/**
 * Authorization headers for calls to the orchestrator.
 *
 * A worker with no Keycloak service account (the `aegis init` stack deploys
 * no Keycloak) sends no token. The orchestrator accepts that only when its
 * config has no `spec.iam`, which is exactly the local development node; a
 * production node refuses the call with 401, which is the visible failure
 * that configuration deserves.
 */
export function bearerHeaders(
  token: string | undefined,
): Record<string, string> {
  return token ? { Authorization: `Bearer ${token}` } : {};
}
