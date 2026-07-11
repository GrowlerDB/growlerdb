// Cluster status (stoplight) logic — pure + unit-tested. Assembled from two existing surfaces the
// gateway already proxies: Prometheus `up` (one sample per scrape target = each process) and the
// control plane's ingestion status (the Iceberg source + per-index sync). No backend change.
import type { InstantSample } from './stats';
import type { IndexIngestion } from './api';
import { worstState } from './ingestion';

export type Health = 'ok' | 'warn' | 'down' | 'unknown';

/** One cluster component's status line. */
export interface Component {
  name: string;
  group: string;
  health: Health;
  detail: string;
}

const INSTANCE_NAMES: Record<string, string> = {
  controlplane: 'Control plane',
  node: 'Node',
  gateway: 'Gateway',
  lgtm: 'Observability (LGTM)',
};

// Kubernetes scrape-job names (chart default) → role. In k8s a target's `instance` is a
// pod IP (not a role) and the `up` sample carries no `namespace` label, so the roll-up can't match on
// either — it matches on the `job` instead. Without this the header health reads "Unknown" on every
// k8s deploy even when all targets are up.
const GROWLERDB_JOBS: Record<string, string> = {
  'gdb-controlplane': 'Control plane',
  'gdb-node': 'Node',
  'gdb-gateway': 'Gateway',
};

/** Friendly name for a Prometheus `instance` label (`host:port` → role). */
export function friendlyInstance(instance: string): string {
  const host = instance.split(':')[0];
  return INSTANCE_NAMES[host] ?? host;
}

// The deploy namespace GrowlerDB's scrape targets carry in Kubernetes (chart default). This is the
// chart's default namespace; scoping it is not yet configurable.
const GROWLERDB_NAMESPACE = 'growlerdb';

/** Whether a Prometheus `up` sample belongs to **GrowlerDB** — so the header health roll-up isn't
 *  dragged Down by unrelated targets on a **shared** Prometheus/Mimir. A target counts if
 *  its instance maps to a known GrowlerDB role (the Compose stack names targets `controlplane:9101`
 *  …) **or** it lives in the GrowlerDB namespace (Kubernetes, where instances are pod IPs). */
export function isGrowlerdbTarget(s: InstantSample): boolean {
  const host = (s.metric.instance ?? s.metric.job ?? '').split(':')[0];
  return (
    host in INSTANCE_NAMES ||
    (s.metric.job ?? '') in GROWLERDB_JOBS ||
    s.metric.namespace === GROWLERDB_NAMESPACE
  );
}

/** One component per GrowlerDB Prometheus `up` scrape target (value 1 = up, 0 = down). Non-GrowlerDB
 *  targets on a shared metrics backend are excluded ([`isGrowlerdbTarget`]) so they can't falsely
 *  report the cluster Down. */
export function componentsFromUp(samples: InstantSample[]): Component[] {
  return samples
    .filter(isGrowlerdbTarget)
    .map((s): Component => {
      const instance = s.metric.instance ?? s.metric.job ?? 'unknown';
      const up = s.value === 1;
      return {
        // Prefer the k8s job role (pod-IP instances aren't human-friendly); fall back to the instance.
        name: GROWLERDB_JOBS[s.metric.job ?? ''] ?? friendlyInstance(instance),
        group: 'Processes',
        health: up ? 'ok' : 'down',
        detail: up ? `${instance} — scraping` : `${instance} — DOWN (not scraping)`,
      };
    })
    .sort((a, b) => a.name.localeCompare(b.name));
}

// Map each ingestion state to a header-pill health. Ingestion *lag* (`behind`) is **not** a cluster
// degradation — a continuously-streaming index is almost always a little behind its source, so
// flagging it "Degraded" is a false alarm. The lag is shown where it belongs (the Ingestion screen +
// Grafana). Only structural failures degrade the roll-up: no assigned primary, or an unreachable
// shard. (Catching a *stalled* — not merely lagging — ingestion needs a staleness threshold the API
// doesn't expose yet; tracked separately.)
const STATE_HEALTH: Record<string, Health> = {
  in_sync: 'ok',
  behind: 'ok',
  uninitialized: 'unknown',
  unknown: 'unknown',
  no_primary: 'down',
  unreachable: 'down',
  // The source was recreated: the index is stale and serving read-only. The cluster is
  // impaired but not down (search still answers) — surface it as Degraded; the Ingestion screen
  // carries the specific "source recreated — reindex" detail.
  source_recreated: 'warn',
};

/** Components from ingestion: the Iceberg source (readable?) + each index's ingestion sync. */
export function componentsFromIngestion(items: IndexIngestion[]): Component[] {
  const out: Component[] = [];
  if (items.length > 0) {
    const unreadable = items.filter((i) => i.source_snapshot_id === null).length;
    out.push({
      name: 'Iceberg source',
      group: 'Dependencies',
      health: unreadable === 0 ? 'ok' : 'down',
      detail:
        unreadable === 0
          ? 'all source tables readable'
          : `${unreadable} of ${items.length} source table(s) unreadable`,
    });
  }
  for (const i of items) {
    const st = worstState(i.shards);
    out.push({
      name: `Ingestion: ${i.name}`,
      group: 'Ingestion',
      health: STATE_HEALTH[st] ?? 'unknown',
      detail: `${i.shard_count} shard(s) — ${st.replace(/_/g, ' ')}`,
    });
  }
  return out;
}

const RANK: Record<Health, number> = { ok: 0, unknown: 1, warn: 2, down: 3 };

/** The cluster headline: the worst health across all components. */
export function overall(components: Component[]): Health {
  return components.reduce<Health>(
    (worst, c) => (RANK[c.health] > RANK[worst] ? c.health : worst),
    components.length ? 'ok' : 'unknown',
  );
}

/** Map a health to the global badge class (.badge.ok / .warning / .critical; '' = grey). */
export function badgeClass(h: Health): string {
  switch (h) {
    case 'ok':
      return 'ok';
    case 'warn':
      return 'warning';
    case 'down':
      return 'critical';
    default:
      return '';
  }
}
