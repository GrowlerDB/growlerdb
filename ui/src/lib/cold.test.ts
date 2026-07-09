import { describe, expect, it } from 'vitest';
import type { ColdStatus } from './api';
import { formatBytes, hitRate, hitRatePct, sortedWindows, tierSummary, windowDate } from './cold';

describe('cold-tier presentation', () => {
  it('computes hit rate and percent', () => {
    expect(hitRate({ hits: 0, misses: 0, fetched_bytes: 0, cached_bytes: 0 })).toBe(0);
    expect(hitRate({ hits: 3, misses: 1, fetched_bytes: 0, cached_bytes: 0 })).toBe(0.75);
    expect(hitRatePct({ hits: 9, misses: 1, fetched_bytes: 0, cached_bytes: 0 })).toBe('90%');
  });

  it('formats bytes in binary units', () => {
    expect(formatBytes(512)).toBe('512 B');
    expect(formatBytes(1536)).toBe('1.5 KiB');
    expect(formatBytes(5 * 1024 * 1024 * 1024)).toBe('5.0 GiB');
  });

  it('renders a window id as a UTC date', () => {
    expect(windowDate(Date.UTC(2025, 5, 15))).toBe('2025-06-15');
  });

  it('sorts windows oldest-first and summarizes the split', () => {
    const status: ColdStatus = {
      hot: 1,
      cold: 2,
      cache: { hits: 1, misses: 1, fetched_bytes: 100, cached_bytes: 100 },
      windows: [
        { window: 300, cold: false, event_min: null, event_max: null },
        { window: 100, cold: true, event_min: 1, event_max: 2 },
        { window: 200, cold: true, event_min: 3, event_max: 4 },
      ],
    };
    expect(sortedWindows(status).map((w) => w.window)).toEqual([100, 200, 300]);
    expect(tierSummary(status)).toBe('2 of 3 windows cold (read-through)');
  });
});
