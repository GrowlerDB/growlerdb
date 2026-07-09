// Ingestion (sync) status helpers (task-49). Pure logic — unit-tested; the Ingestion screen
// renders these. GrowlerDB has no separate "connector" entity: every index is kept in sync with
// exactly one Iceberg source by changelog ingestion, so "ingestion status" = the source head vs.
// each shard's committed checkpoint.
import type { ShardIngestion } from './api';

export type SyncState =
  | 'in_sync'
  | 'behind'
  | 'uninitialized'
  | 'no_primary'
  | 'unreachable'
  | 'source_recreated'
  | 'unknown';

// Higher = more severe; an index's headline is its worst shard.
const SEVERITY: Record<string, number> = {
  in_sync: 0,
  unknown: 1,
  uninitialized: 1,
  behind: 2,
  no_primary: 3,
  unreachable: 3,
  // The source table was dropped+recreated (task-114): the index is stale and its keys won't
  // hydrate — the most severe state (it serves wrong results until reindexed), so it's the headline.
  source_recreated: 4,
};

/** The worst (most severe) state across an index's shards — its headline sync status. */
export function worstState(shards: ShardIngestion[]): SyncState {
  if (shards.length === 0) return 'unknown';
  let worst = shards[0].state;
  for (const s of shards) {
    if ((SEVERITY[s.state] ?? 1) > (SEVERITY[worst] ?? 1)) worst = s.state;
  }
  return worst as SyncState;
}

/** Map a sync state to a badge level (reuses the global `.badge.ok/.warning/.critical` styles). */
export function badgeLevel(state: string): 'ok' | 'warning' | 'critical' | '' {
  switch (state) {
    case 'in_sync':
      return 'ok';
    case 'behind':
      return 'warning';
    case 'no_primary':
    case 'unreachable':
    case 'source_recreated':
      return 'critical';
    default:
      return '';
  }
}

/** The worst (largest) wall-clock lag across an index's shards, in ms (task-137). 0 when every
 *  shard is in sync — the headline lag matches the headline (worst) state. */
export function worstLagMs(shards: ShardIngestion[]): number {
  return shards.reduce((max, s) => Math.max(max, s.lag_ms ?? 0), 0);
}

/** Humanize a duration in ms as a coarse "Nd/Nh/Nm/Ns" label (e.g. "3m 20s"); '' for <=0. */
export function formatDuration(ms: number): string {
  const sec = Math.floor(ms / 1000);
  if (sec <= 0) return '';
  if (sec < 60) return `${sec}s`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ${sec % 60}s`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ${min % 60}m`;
  return `${Math.floor(hr / 24)}d ${hr % 24}h`;
}

/** Humanize an epoch-ms timestamp as a relative age (e.g. "3m ago"); '' for null/0. */
export function formatAge(ms: number | null, now: number): string {
  if (!ms) return '';
  const sec = Math.max(0, Math.floor((now - ms) / 1000));
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  return `${Math.floor(hr / 24)}d ago`;
}
