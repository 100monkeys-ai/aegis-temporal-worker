/**
 * Keycloak Service Account Token Manager
 * Implements OAuth2 client credentials flow with caching.
 */

import { config } from '../config.js';
import { logger } from '../logger.js';

interface TokenResponse {
  access_token: string;
  expires_in: number;
  token_type: string;
}

let cachedToken: string | null = null;
let tokenExpiresAt: number = 0;

/**
 * Returns a valid Bearer token for the temporal-worker service account.
 * Fetches a new token from Keycloak when the cache is empty or within
 * 30 seconds of expiry.
 */
export async function getServiceToken(): Promise<string> {
  const now = Date.now();

  if (cachedToken !== null && now < tokenExpiresAt) {
    logger.debug('Returning cached service account token');
    return cachedToken;
  }

  const { keycloakHost, realm, clientId, clientSecret } = config.serviceAccount;
  const tokenUrl = `${keycloakHost}/realms/${realm}/protocol/openid-connect/token`;

  logger.info({ token_url: tokenUrl, client_id: clientId }, 'Fetching service account token from Keycloak');

  const body = new URLSearchParams({
    grant_type: 'client_credentials',
    client_id: clientId,
    client_secret: clientSecret,
  });

  const response = await fetch(tokenUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
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
    'Service account token refreshed',
  );

  return cachedToken;
}
