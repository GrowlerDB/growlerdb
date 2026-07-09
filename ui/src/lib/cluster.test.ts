import { describe, it, expect } from 'vitest';
import {
  friendlyInstance,
  componentsFromUp,
  componentsFromIngestion,
  overall,
  badgeClass,
} from './cluster';
import type { InstantSample } from './stats';
import type { IndexIngestion } from './api';

describe('friendlyInstance', () => {
  it('maps host:port to a role name', () => {
    expect(friendlyInstance('controlplane:9101')).toBe('Control plane');
    expect(friendlyInstance('node:9102')).toBe('Node');
    expect(friendlyInstance('gateway:9103')).toBe('Gateway');
    expect(friendlyInstance('something-else:9999')).toBe('something-else');
  });
});

describe('componentsFromUp', () => {
  const up = (instance: string, value: number): InstantSample => ({
    metric: { job: 'growlerdb', instance },
    value,
  });
  it('marks up=1 ok and up=0 down', () => {
    const cs = componentsFromUp([up('controlplane:9101', 1), up('node:9102', 0)]);
    const cp = cs.find((c) => c.name === 'Control plane')!;
    const node = cs.find((c) => c.name === 'Node')!;
    expect(cp.health).toBe('ok');
    expect(node.health).toBe('down');
    expect(node.detail).toContain('DOWN');
  });

  it('ignores non-GrowlerDB targets on a shared Prometheus/Mimir (task-120)', () => {
    // A down target in another namespace must NOT count — only GrowlerDB's own targets do.
    const external: InstantSample = {
      metric: { job: 'boxowl-api', instance: '10.1.2.3:80', namespace: 'boxowl' },
      value: 0,
    };
    const gdbK8s: InstantSample = {
      metric: { job: 'growlerdb', instance: '10.1.2.4:9102', namespace: 'growlerdb' },
      value: 1,
    };
    const cs = componentsFromUp([external, gdbK8s, up('gateway:9103', 1)]);
    // The boxowl target is excluded; the growlerdb-namespace (K8s) + role-named (compose) ones stay.
    expect(cs).toHaveLength(2);
    expect(cs.every((c) => c.health === 'ok')).toBe(true);
    expect(cs.some((c) => c.detail.includes('boxowl'))).toBe(false);
  });

  it('returns no components when GrowlerDB has no scrape targets (→ not a false down)', () => {
    const external: InstantSample = {
      metric: { job: 'kube-state-metrics', instance: 'ksm:8080', namespace: 'observability' },
      value: 0,
    };
    expect(componentsFromUp([external])).toEqual([]);
  });

  it('recognises k8s targets by their gdb-* job — pod-IP instance, no namespace (task-226)', () => {
    // In k8s the `up` sample has a pod-IP instance and no namespace label; only the job identifies it.
    // Previously these matched nothing → the header rolled up to "Unknown" even when all were up.
    const gdb: InstantSample[] = [
      { metric: { job: 'gdb-controlplane', instance: '10.42.4.9:9101' }, value: 1 },
      { metric: { job: 'gdb-node', instance: '10.42.1.7:9102' }, value: 1 },
      { metric: { job: 'gdb-gateway', instance: '10.42.0.5:9103' }, value: 0 },
      { metric: { job: 'node-exporter', instance: '10.42.2.2:9100' }, value: 1 }, // not GrowlerDB
    ];
    const cs = componentsFromUp(gdb);
    expect(cs).toHaveLength(3); // the three gdb-* targets; node-exporter excluded
    expect(cs.find((c) => c.name === 'Control plane')!.health).toBe('ok');
    expect(cs.find((c) => c.name === 'Node')!.health).toBe('ok');
    expect(cs.find((c) => c.name === 'Gateway')!.health).toBe('down');
  });
});

function ingest(name: string, sourceSnap: number | null, shardState: string): IndexIngestion {
  return {
    name,
    status: 'active',
    source_table: `growlerdb.${name}`,
    routing: 'hash',
    shard_count: 1,
    source_snapshot_id: sourceSnap,
    source_timestamp_ms: sourceSnap ? 1 : null,
    shards: [
      {
        ordinal: 0,
        node: 'http://node:50051',
        committed_snapshot_id: 1,
        index_snapshot: 1,
        state: shardState,
        lag_ms: 0,
        window: 0,
      },
    ],
  };
}

describe('componentsFromIngestion', () => {
  it('reports the source readable + per-index sync', () => {
    const cs = componentsFromIngestion([ingest('docs', 5, 'in_sync')]);
    expect(cs.find((c) => c.name === 'Iceberg source')!.health).toBe('ok');
    expect(cs.find((c) => c.name === 'Ingestion: docs')!.health).toBe('ok');
  });
  it('flags an unreadable source down, but a behind index stays ok (lag is not a degradation)', () => {
    const cs = componentsFromIngestion([ingest('docs', null, 'behind')]);
    expect(cs.find((c) => c.name === 'Iceberg source')!.health).toBe('down');
    // Ingestion lag is normal for a streaming source — it must not degrade the header pill.
    expect(cs.find((c) => c.name === 'Ingestion: docs')!.health).toBe('ok');
  });
  it('flags an unreachable shard down', () => {
    const cs = componentsFromIngestion([ingest('docs', 5, 'unreachable')]);
    expect(cs.find((c) => c.name === 'Ingestion: docs')!.health).toBe('down');
  });
  it('flags a source_recreated index as degraded (warn) — impaired, not down (task-114)', () => {
    const cs = componentsFromIngestion([ingest('docs', 5, 'source_recreated')]);
    expect(cs.find((c) => c.name === 'Ingestion: docs')!.health).toBe('warn');
  });
});

describe('overall', () => {
  const c = (health: 'ok' | 'warn' | 'down' | 'unknown') => ({
    name: 'x',
    group: 'g',
    health,
    detail: '',
  });
  it('returns the worst component health', () => {
    expect(overall([c('ok'), c('warn'), c('ok')])).toBe('warn');
    expect(overall([c('ok'), c('down'), c('warn')])).toBe('down');
    expect(overall([c('ok'), c('ok')])).toBe('ok');
    expect(overall([])).toBe('unknown');
  });
});

describe('badgeClass', () => {
  it('maps health to badge severity classes', () => {
    expect(badgeClass('ok')).toBe('ok');
    expect(badgeClass('warn')).toBe('warning');
    expect(badgeClass('down')).toBe('critical');
    expect(badgeClass('unknown')).toBe('');
  });
});
