import { describe, it, expect } from 'vitest';
import {
  randomVerifier,
  base64UrlEncode,
  pkceChallenge,
  buildAuthorizeUrl,
  isTokenExpired,
} from './auth';

describe('isTokenExpired (task-153 / B17)', () => {
  // A minimal unsigned JWT with the given payload (only the payload segment is read).
  const jwt = (payload: Record<string, unknown>) => `h.${btoa(JSON.stringify(payload))}.s`;

  it('is true for a token past its exp', () => {
    expect(isTokenExpired(jwt({ exp: Math.floor(Date.now() / 1000) - 60 }))).toBe(true);
  });
  it('is false for a token with a future exp', () => {
    expect(isTokenExpired(jwt({ exp: Math.floor(Date.now() / 1000) + 3600 }))).toBe(false);
  });
  it('is false when there is no exp (the gateway decides) or no token', () => {
    expect(isTokenExpired(jwt({ sub: 'alice' }))).toBe(false);
    expect(isTokenExpired(null)).toBe(false);
  });
});

describe('PKCE', () => {
  it('verifier has the requested length and only unreserved chars', () => {
    const v = randomVerifier(80);
    expect(v).toHaveLength(80);
    expect(v).toMatch(/^[A-Za-z0-9\-._~]+$/);
  });

  it('base64url drops +, / and padding', () => {
    expect(base64UrlEncode(new Uint8Array([255, 255, 255]))).toBe('____');
    expect(base64UrlEncode(new Uint8Array([0]))).toBe('AA');
  });

  it('challenge matches the RFC 7636 test vector', async () => {
    const verifier = 'dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk';
    expect(await pkceChallenge(verifier)).toBe('E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM');
  });

  it('authorize URL carries the code + PKCE params', () => {
    const url = new URL(
      buildAuthorizeUrl(
        'https://idp.example/auth',
        {
          issuer: 'https://idp.example',
          clientId: 'growlerdb-ui',
          redirectUri: 'https://app.example/',
          scope: 'openid profile',
        },
        'state123',
        'challenge456',
      ),
    );
    expect(url.searchParams.get('response_type')).toBe('code');
    expect(url.searchParams.get('client_id')).toBe('growlerdb-ui');
    expect(url.searchParams.get('code_challenge')).toBe('challenge456');
    expect(url.searchParams.get('code_challenge_method')).toBe('S256');
    expect(url.searchParams.get('state')).toBe('state123');
  });
});
