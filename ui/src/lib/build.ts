// Build/runtime info for the Settings → About card (task-98). Static for now; a deployment can
// override `version` at build time via a Vite `define` without touching this file.
export const build = {
  version: (import.meta.env.VITE_GROWLERDB_VERSION as string | undefined) ?? 'dev',
  mode: 'embedded',
  license: 'Apache-2.0',
} as const;
