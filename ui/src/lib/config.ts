// Runtime app config (task-45). OIDC is optional — with no issuer configured the UI runs
// against an open Engine (mirrors the gateway, which is open until `--oidc-issuer` is set).
// Configured at build time via `VITE_OIDC_*`; a runtime `window.__GROWLERDB_CONFIG__` override
// is supported so a deployment can set it without rebuilding.
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
