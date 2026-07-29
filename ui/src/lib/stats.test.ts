import { describe, it, expect } from 'vitest';
import {
  parsePromMatrix,
  latestOf,
  evaluateAlerts,
  serverAlertToDisplay,
  scopeQuery,
  topNSeries,
  type Series,
} from './stats';

describe('parsePromMatrix', () => {
  it('parses a matrix result into ms-timestamped series', () => {
    const series = parsePromMatrix(
      {
        status: 'success',
        data: {
          result: [
            {
              metric: { __name__: 'x', quantile: '0.95' },
              values: [
                [1700000000, '0.5'],
                [1700000015, '0.6'],
              ],
            },
          ],
        },
      },
      'fallback',
    );
    expect(series).toHaveLength(1);
    expect(series[0].name).toBe('quantile=0.95'); // __name__ dropped
    expect(series[0].points).toEqual([
      [1700000000000, 0.5],
      [1700000015000, 0.6],
    ]);
  });

  it('uses the label when the series has no distinguishing metric labels', () => {
    const series = parsePromMatrix(
      { status: 'success', data: { result: [{ metric: {}, values: [[1, '2']] }] } },
      'queries/s',
    );
    expect(series[0].name).toBe('queries/s');
  });

  it('handles an empty result', () => {
    expect(parsePromMatrix({ status: 'success', data: { result: [] } })).toEqual([]);
    expect(latestOf([])).toBeNull();
  });
});

describe('evaluateAlerts', () => {
  it('is quiet when all SLIs are healthy', () => {
    expect(evaluateAlerts({ errorRate: 0, latencyP99: 0.01, staleLocatorRate: 0 })).toEqual([]);
  });

  it('flags a critical error rate and warnings for latency/stale', () => {
    const alerts = evaluateAlerts({ errorRate: 0.2, latencyP99: 2, staleLocatorRate: 3 });
    expect(alerts.map((a) => a.level)).toEqual(['critical', 'warning', 'warning']);
    expect(alerts[0].name).toBe('Query errors');
  });

  it('treats missing data as not-firing', () => {
    expect(evaluateAlerts({ errorRate: null, latencyP99: null, staleLocatorRate: null })).toEqual(
      [],
    );
  });
});

describe('serverAlertToDisplay', () => {
  it('maps a critical firing alert', () => {
    expect(
      serverAlertToDisplay({
        name: 'HighQueryErrorRate',
        severity: 'critical',
        summary: '0.12 errors/s',
        state: 'firing',
      }),
    ).toEqual({ name: 'HighQueryErrorRate', level: 'critical', detail: '0.12 errors/s' });
  });

  it('marks pending alerts and defaults non-critical severities to warning', () => {
    const d = serverAlertToDisplay({
      name: 'HighQueryLatency',
      severity: 'page',
      summary: 'p99 1.8s',
      state: 'pending',
    });
    expect(d.level).toBe('warning');
    expect(d.detail).toBe('p99 1.8s (pending)');
  });
});

describe('scopeQuery', () => {
  it('is a no-op for the empty (all-indexes) scope', () => {
    const q = 'sum(rate(growlerdb_query_total[1m]))';
    expect(scopeQuery(q, '')).toBe(q);
  });

  it('injects an anchored regex matcher into a bare growlerdb_* selector', () => {
    // The matcher accepts the plain name or the name + " s<ordinal>" (the per-shard label scheme).
    expect(scopeQuery('sum(rate(growlerdb_query_total[1m]))', 'movies')).toBe(
      'sum(rate(growlerdb_query_total{index=~"movies( s[0-9]+)?"}[1m]))',
    );
  });

  it('merges the matcher into an existing label set', () => {
    expect(scopeQuery('sum(growlerdb_index_bytes_component{component="term"})', 'docs')).toBe(
      'sum(growlerdb_index_bytes_component{index=~"docs( s[0-9]+)?",component="term"})',
    );
  });

  it('scopes every growlerdb_* selector in the expression', () => {
    expect(
      scopeQuery(
        'max(max by (index) (growlerdb_index_bytes) / avg by (index) (growlerdb_index_bytes))',
        'catalog',
      ),
    ).toBe(
      'max(max by (index) (growlerdb_index_bytes{index=~"catalog( s[0-9]+)?"}) / avg by (index) (growlerdb_index_bytes{index=~"catalog( s[0-9]+)?"}))',
    );
  });

  it('escapes regex metacharacters in the index name', () => {
    expect(scopeQuery('sum(growlerdb_index_bytes)', 'a.b')).toBe(
      'sum(growlerdb_index_bytes{index=~"a\\.b( s[0-9]+)?"})',
    );
  });

  it('leaves non-growlerdb metrics (up, node_*) untouched', () => {
    expect(scopeQuery('sum(up)', 'movies')).toBe('sum(up)');
    expect(scopeQuery('max(node_memory_MemAvailable_bytes)', 'movies')).toBe(
      'max(node_memory_MemAvailable_bytes)',
    );
  });
});

describe('topNSeries', () => {
  const mk = (name: string, last: number): Series => ({
    name,
    points: [
      [1000, last],
      [2000, last],
    ],
  });

  it('returns the input unchanged when it already has ≤ n series', () => {
    const s = [mk('a', 3), mk('b', 1)];
    expect(topNSeries(s, 6)).toBe(s);
  });

  it('keeps the top-n by latest value and rolls the rest into "other"', () => {
    const s = [mk('a', 10), mk('b', 5), mk('c', 3), mk('d', 1)];
    const out = topNSeries(s, 2);
    expect(out.map((x) => x.name)).toEqual(['a', 'b', 'other']);
    // "other" is c + d summed pointwise (3+1 at each timestamp).
    expect(out[2].points).toEqual([
      [1000, 4],
      [2000, 4],
    ]);
  });
});
