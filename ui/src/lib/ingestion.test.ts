import { describe, it, expect } from 'vitest';
import { worstState, badgeLevel, formatAge, worstLagMs, formatDuration } from './ingestion';
import type { ShardIngestion } from './api';

function shard(state: string, ordinal = 0, lag_ms = 0, window = 0): ShardIngestion {
  return {
    ordinal,
    node: 'http://n:50051',
    committed_snapshot_id: 1,
    index_snapshot: 1,
    state,
    lag_ms,
    window,
  };
}

describe('worstLagMs', () => {
  it('returns the largest shard lag', () => {
    expect(worstLagMs([shard('behind', 0, 5000), shard('in_sync', 1, 0)])).toBe(5000);
    expect(worstLagMs([shard('in_sync', 0, 0)])).toBe(0);
    expect(worstLagMs([])).toBe(0);
  });
});

describe('formatDuration', () => {
  it('humanizes ms into coarse units', () => {
    expect(formatDuration(0)).toBe('');
    expect(formatDuration(4200)).toBe('4s');
    expect(formatDuration(200_000)).toBe('3m 20s');
    expect(formatDuration(3_900_000)).toBe('1h 5m');
    expect(formatDuration(90_000_000)).toBe('1d 1h');
  });
});

describe('worstState', () => {
  it('returns the most severe shard state', () => {
    expect(worstState([shard('in_sync'), shard('behind', 1)])).toBe('behind');
    expect(worstState([shard('in_sync'), shard('unreachable', 1)])).toBe('unreachable');
    expect(worstState([shard('in_sync'), shard('in_sync', 1)])).toBe('in_sync');
  });

  it('treats no_primary/unreachable as worse than behind', () => {
    expect(worstState([shard('behind'), shard('no_primary', 1)])).toBe('no_primary');
  });

  it('treats source_recreated (stale index) as the most severe — the headline (task-114)', () => {
    expect(worstState([shard('behind'), shard('source_recreated', 1)])).toBe('source_recreated');
    expect(worstState([shard('unreachable'), shard('source_recreated', 1)])).toBe(
      'source_recreated',
    );
  });

  it('is "unknown" for an index with no shards', () => {
    expect(worstState([])).toBe('unknown');
  });
});

describe('badgeLevel', () => {
  it('maps states to badge severities', () => {
    expect(badgeLevel('in_sync')).toBe('ok');
    expect(badgeLevel('behind')).toBe('warning');
    expect(badgeLevel('unreachable')).toBe('critical');
    expect(badgeLevel('no_primary')).toBe('critical');
    expect(badgeLevel('source_recreated')).toBe('critical');
    expect(badgeLevel('uninitialized')).toBe('');
    expect(badgeLevel('unknown')).toBe('');
  });
});

describe('formatAge', () => {
  const now = 1_000_000_000_000;
  it('humanizes recent timestamps', () => {
    expect(formatAge(now - 5_000, now)).toBe('5s ago');
    expect(formatAge(now - 5 * 60_000, now)).toBe('5m ago');
    expect(formatAge(now - 3 * 3_600_000, now)).toBe('3h ago');
    expect(formatAge(now - 2 * 86_400_000, now)).toBe('2d ago');
  });
  it('is empty for null/0 (no source snapshot)', () => {
    expect(formatAge(null, now)).toBe('');
    expect(formatAge(0, now)).toBe('');
  });
});
