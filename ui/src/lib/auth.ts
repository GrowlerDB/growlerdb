// OIDC **authorization-code + PKCE**. The UI authenticates the human against
// the IdP (Keycloak by default) and forwards the resulting bearer token to the Engine API
// (see `api.ts`); the Engine gateway validates it. No client secret — PKCE is the
// public-client flow. The pure pieces (verifier/challenge/authorize-URL) are unit-tested; the
// redirect + token exchange are thin wrappers over the IdP's discovery document.

export interface OidcConfig {
  issuer: string;
  clientId: string;
  redirectUri: string;
  scope: string;
}

const TOKEN_KEY = 'growlerdb.token';
const VERIFIER_KEY = 'growlerdb.pkce_verifier';
const STATE_KEY = 'growlerdb.pkce_state';

/** The stored bearer token, or `null`. */
export function getToken(): string | null {
  return sessionStorage.getItem(TOKEN_KEY);
}

export function setToken(token: string): void {
  sessionStorage.setItem(TOKEN_KEY, token);
}

export function clearToken(): void {
  sessionStorage.removeItem(TOKEN_KEY);
}

export function isAuthenticated(): boolean {
  return getToken() !== null;
}

/** Whether `token` (default: the stored one) is past its JWT `exp`. A token with no
 *  `exp` is treated as non-expiring here — the gateway is still the authority (it verifies `exp`
 *  server-side); this just lets the client skip a known-401 request and re-gate proactively. */
export function isTokenExpired(token: string | null = getToken()): boolean {
  if (!token) return false;
  const claims = decodeJwtClaims(token);
  const exp = claims && typeof claims.exp === 'number' ? claims.exp : null;
  return exp !== null && Date.now() >= exp * 1000;
}

/** The signed-in user, derived from the bearer's claims, or `null` when not authenticated. An
 *  interim client-side read of the JWT until the server identity surface (`GET /v1/me`)
 *  lands — never faked: `null` means "show the unauthenticated state". `roles` is best-effort from
 *  common claim shapes (the gateway is the authority on roles). */
export interface CurrentUser {
  subject: string;
  name: string;
  email?: string;
  roles: string[];
}

export function currentUser(): CurrentUser | null {
  const token = getToken();
  if (!token) return null;
  const claims = decodeJwtClaims(token);
  if (!claims) return null;
  const subject = String(claims.sub ?? '');
  if (!subject) return null;
  const name = String(claims.name ?? claims.preferred_username ?? claims.email ?? subject);
  const roles = extractRoles(claims);
  return {
    subject,
    name,
    email: typeof claims.email === 'string' ? claims.email : undefined,
    roles,
  };
}

/** Decode a JWT's payload (no signature check — the Engine gateway verifies; this is for display). */
function decodeJwtClaims(token: string): Record<string, unknown> | null {
  const part = token.split('.')[1];
  if (!part) return null;
  try {
    const json = atob(part.replace(/-/g, '+').replace(/_/g, '/'));
    return JSON.parse(json) as Record<string, unknown>;
  } catch {
    return null;
  }
}

function extractRoles(claims: Record<string, unknown>): string[] {
  const direct = claims.roles ?? claims.groups;
  if (Array.isArray(direct)) return direct.map(String);
  // Keycloak nests realm roles under realm_access.roles.
  const realm = claims.realm_access;
  if (realm && typeof realm === 'object' && Array.isArray((realm as { roles?: unknown }).roles)) {
    return ((realm as { roles: unknown[] }).roles ?? []).map(String);
  }
  return [];
}

/** Initials for an avatar from a display name (e.g. "Kira Johansson" → "KJ", "o.kim" → "OK"). */
export function initials(name: string): string {
  const parts = name.split(/[\s._-]+/).filter(Boolean);
  if (parts.length === 0) return '?';
  if (parts.length === 1) return parts[0].slice(0, 2).toUpperCase();
  return (parts[0][0] + parts[parts.length - 1][0]).toUpperCase();
}

// Unreserved characters allowed in a PKCE `code_verifier` (RFC 7636).
const VERIFIER_ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~';

/** A high-entropy PKCE `code_verifier` (RFC 7636: 43–128 unreserved chars). */
export function randomVerifier(length = 64): string {
  const bytes = new Uint8Array(length);
  crypto.getRandomValues(bytes);
  let out = '';
  for (const b of bytes) out += VERIFIER_ALPHABET[b % VERIFIER_ALPHABET.length];
  return out;
}

/** Base64url (no padding) of raw bytes. */
export function base64UrlEncode(input: ArrayBuffer | Uint8Array): string {
  const bytes = input instanceof Uint8Array ? input : new Uint8Array(input);
  let binary = '';
  for (const b of bytes) binary += String.fromCharCode(b);
  return btoa(binary).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

/** The PKCE `code_challenge` = base64url(SHA-256(verifier)), method S256. */
export async function pkceChallenge(verifier: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(verifier));
  return base64UrlEncode(digest);
}

/** Compose the IdP authorize URL for the code+PKCE flow. */
export function buildAuthorizeUrl(
  authorizationEndpoint: string,
  cfg: OidcConfig,
  state: string,
  challenge: string,
): string {
  const params = new URLSearchParams({
    response_type: 'code',
    client_id: cfg.clientId,
    redirect_uri: cfg.redirectUri,
    scope: cfg.scope,
    state,
    code_challenge: challenge,
    code_challenge_method: 'S256',
  });
  return `${authorizationEndpoint}?${params.toString()}`;
}

interface Discovery {
  authorization_endpoint: string;
  token_endpoint: string;
}

async function discover(issuer: string): Promise<Discovery> {
  const url = `${issuer.replace(/\/$/, '')}/.well-known/openid-configuration`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`OIDC discovery failed (${res.status})`);
  return res.json();
}

/** Begin login: stash a fresh verifier+state, then redirect to the IdP. */
export async function login(cfg: OidcConfig): Promise<void> {
  const { authorization_endpoint } = await discover(cfg.issuer);
  const verifier = randomVerifier();
  const state = randomVerifier(32);
  sessionStorage.setItem(VERIFIER_KEY, verifier);
  sessionStorage.setItem(STATE_KEY, state);
  const challenge = await pkceChallenge(verifier);
  window.location.assign(buildAuthorizeUrl(authorization_endpoint, cfg, state, challenge));
}

/** On the redirect back, exchange `?code` for a token (verifying `state`). Returns whether a
 *  callback was handled. */
export async function handleRedirectCallback(cfg: OidcConfig): Promise<boolean> {
  const params = new URLSearchParams(window.location.search);
  const code = params.get('code');
  if (!code) return false;
  if (params.get('state') !== sessionStorage.getItem(STATE_KEY)) {
    throw new Error('OIDC state mismatch');
  }
  const verifier = sessionStorage.getItem(VERIFIER_KEY);
  if (!verifier) throw new Error('missing PKCE verifier');

  const { token_endpoint } = await discover(cfg.issuer);
  const body = new URLSearchParams({
    grant_type: 'authorization_code',
    code,
    redirect_uri: cfg.redirectUri,
    client_id: cfg.clientId,
    code_verifier: verifier,
  });
  const res = await fetch(token_endpoint, {
    method: 'POST',
    headers: { 'content-type': 'application/x-www-form-urlencoded' },
    body,
  });
  if (!res.ok) throw new Error(`token exchange failed (${res.status})`);
  const tokens = await res.json();
  setToken(tokens.access_token);
  sessionStorage.removeItem(VERIFIER_KEY);
  sessionStorage.removeItem(STATE_KEY);
  window.history.replaceState({}, '', cfg.redirectUri);
  return true;
}

export function logout(): void {
  clearToken();
}
