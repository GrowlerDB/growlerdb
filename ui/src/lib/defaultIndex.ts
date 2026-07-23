/** Pick the index the Search screen opens on, in preference order:
 *
 *   1. the user's last chosen index, if it still exists (their explicit choice wins);
 *   2. the deployment's configured default index (`GROWLERDB_DEFAULT_INDEX` → `/v1/config`), if it
 *      exists — a deployment points the console at its front door (the demo → `movies`, which has a
 *      VECTOR field, so semantic/hybrid search is one click from a fresh visitor);
 *   3. the first available index.
 *
 * Returns `''` when there are no indexes — a single-index endpoint with no control plane fronted,
 * where the caller leaves the scope empty to use the served default.
 */
export function pickDefaultIndex(
  available: string[],
  saved: string | null | undefined,
  configured: string | null | undefined,
): string {
  if (saved && available.includes(saved)) return saved;
  if (configured && available.includes(configured)) return configured;
  return available.length > 0 ? available[0] : '';
}
