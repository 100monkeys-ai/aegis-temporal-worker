/**
 * Keycloak Service Account Token Manager
 * Implements OAuth2 client credentials flow with caching.
 */

import { config } from "../config.js";
import { logger } from "../logger.js";

interface TokenResponse {
  access_token: string;
  expires_in: number;
  token_type: string;
}

let cachedToken: string | null = null;
let tokenExpiresAt: number = 0;
let warnedNoServiceAccount = false;

/**
 * Returns a valid Bearer token for the temporal-worker service account, or
 * `undefined` when no service account is configured. Fetches a new token from
 * Keycloak when the cache is empty or within 30 seconds of expiry.
 *
 * No service account (none of KEYCLOAK_HOST, KEYCLOAK_SYSTEM_REALM,
 * KEYCLOAK_CLIENT_ID, KEYCLOAK_CLIENT_SECRET set) is the `aegis init` stack,
 * which deploys no Keycloak: the worker then calls the orchestrator without a
 * token, which an orchestrator without `spec.iam` accepts and a production one
 * refuses with 401. Said once, at the first call, so the log names it.
 */
export async function getServiceToken(): Promise<string | undefined> {
  const now = Date.now();

  if (cachedToken !== null && now < tokenExpiresAt) {
    logger.debug("Returning cached service account token");
    return cachedToken;
  }

  const serviceAccount = config.serviceAccount;
  if (!serviceAccount) {
    if (!warnedNoServiceAccount) {
      warnedNoServiceAccount = true;
      logger.warn(
        "No Keycloak service account is configured (KEYCLOAK_HOST, KEYCLOAK_SYSTEM_REALM, KEYCLOAK_CLIENT_ID and KEYCLOAK_CLIENT_SECRET are unset): calling the orchestrator without a token, which only a node without spec.iam accepts",
      );
    }
    return undefined;
  }

  const { keycloakHost, realm, clientId, clientSecret } = serviceAccount;
  const tokenUrl = `${keycloakHost}/realms/${realm}/protocol/openid-connect/token`;

  logger.info(
    { token_url: tokenUrl, client_id: clientId },
    "Fetching service account token from Keycloak",
  );

  const body = new URLSearchParams({
    grant_type: "client_credentials",
    client_id: clientId,
    client_secret: clientSecret,
    scope:
      "agent:read agent:execute execution:read execution:stream workflow:read volume:read volume:write",
  });

  const response = await fetch(tokenUrl, {
    method: "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body: body.toString(),
  });

  if (!response.ok) {
    const responseBody = await response.text();
    throw new Error(
      `Keycloak token fetch failed: HTTP ${response.status} — ${responseBody}`,
    );
  }

  const data = (await response.json()) as TokenResponse;

  cachedToken = data.access_token;
  // Cache until (expires_in - 30) seconds from now
  tokenExpiresAt = now + (data.expires_in - 30) * 1000;

  logger.debug(
    { expires_in: data.expires_in, client_id: clientId },
    "Service account token refreshed",
  );

  return cachedToken;
}
