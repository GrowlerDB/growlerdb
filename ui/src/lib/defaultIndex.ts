/** Pick the index the Search screen opens on, in preference order:
 *   1. the user's last chosen index, if it still exists;
 *   2. the deployment's configured default (`GROWLERDB_DEFAULT_INDEX` → `/v1/config`), if it exists;
 *   3. the first available index.
 *
 * Returns `''` when there are no indexes, so the caller leaves the scope empty to use the served default.
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
