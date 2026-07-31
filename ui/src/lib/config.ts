// Runtime app config. OIDC is optional — with no issuer the UI runs against an open Engine
// (mirrors the gateway). Set at build time via `VITE_OIDC_*`, or overridden at runtime via
// `window.__GROWLERDB_CONFIG__` so a deployment can configure it without rebuilding.
import type { OidcConfig } from './auth';

export interface AppConfig {
  oidc?: OidcConfig;
}

declare global {
  interface Window {
    __GROWLERDB_CONFIG__?: { oidc?: Partial<OidcConfig> };
  }
}

export function loadConfig(): AppConfig {
  const env = import.meta.env;
  const runtime = window.__GROWLERDB_CONFIG__?.oidc;
  const issuer = runtime?.issuer ?? (env.VITE_OIDC_ISSUER as string | undefined);
  if (!issuer) return {};
  return {
    oidc: {
      issuer,
      clientId: runtime?.clientId ?? (env.VITE_OIDC_CLIENT_ID as string) ?? 'growlerdb-ui',
      redirectUri:
        runtime?.redirectUri ??
        (env.VITE_OIDC_REDIRECT_URI as string) ??
        `${window.location.origin}/`,
      scope: runtime?.scope ?? (env.VITE_OIDC_SCOPE as string) ?? 'openid profile',
    },
  };
}
