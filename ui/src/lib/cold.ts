// Cold-tier presentation logic — pure + unit-tested. The Cluster screen's "Storage tiers"
// panel renders these from `GET /v1/cold` (per-window hot/cold tier + the shared read-through
// cache's stats), so the cost story is visible: how much of the index is parked to object storage
// and how effective the read-through cache is.
import type { ColdCacheStats, ColdStatus, WindowTier } from './api';

/** Cache hit rate in `[0, 1]` (0 when there have been no reads). */
export function hitRate(cache: ColdCacheStats): number {
  const total = cache.hits + cache.misses;
  return total === 0 ? 0 : cache.hits / total;
}

/** A hit rate as a rounded percent string, e.g. `92%`. */
export function hitRatePct(cache: ColdCacheStats): string {
  return `${Math.round(hitRate(cache) * 100)}%`;
}

/** Human-readable bytes (binary units), e.g. `1.5 GiB`. */
export function formatBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  const units = ['KiB', 'MiB', 'GiB', 'TiB', 'PiB'];
  let v = n / 1024;
  let i = 0;
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v.toFixed(v < 10 ? 1 : 0)} ${units[i]}`;
}

/** A window id (epoch-ms of the window start) as a UTC date, e.g. `2025-06-15`. */
export function windowDate(window: number): string {
  return new Date(window).toISOString().slice(0, 10);
}

/** Windows oldest-first (cold windows are the oldest), so the panel reads top-down by age. */
export function sortedWindows(status: ColdStatus): WindowTier[] {
  return [...status.windows].sort((a, b) => a.window - b.window);
}

/** A one-line summary of the tier split, e.g. `2 of 3 windows cold (read-through)`. */
export function tierSummary(status: ColdStatus): string {
  const total = status.hot + status.cold;
  return `${status.cold} of ${total} window${total === 1 ? '' : 's'} cold (read-through)`;
}
