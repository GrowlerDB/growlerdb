import { describe, it, expect } from 'vitest';
import { parsePromMatrix, latestOf, evaluateAlerts, serverAlertToDisplay } from './stats';

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
