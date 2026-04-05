// Stub service account env vars required by config.ts at module load time.
// Tests do not exercise token fetching; these are placeholders only.
process.env.KEYCLOAK_HOST = "http://aegis-iam:8180";
process.env.KEYCLOAK_SYSTEM_REALM = "aegis-system";
process.env.KEYCLOAK_CLIENT_ID = "aegis-temporal-worker";
process.env.KEYCLOAK_CLIENT_SECRET = "test-secret";
