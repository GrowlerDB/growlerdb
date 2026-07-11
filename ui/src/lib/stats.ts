// SLI metrics for the Observability screen. The UI queries the Engine's same-origin
// metrics proxy (`/v1/stats/...`, which forwards to Prometheus), so panels need no Prometheus
// URL or CORS. Parsing + alert evaluation are pure and unit-tested; `queryRange` is the thin
// network wrapper.
import { apiFetch } from './api';

/** A time series: a name + `[timestampMs, value]` points. */
export interface Series {
  name: string;
  points: [number, number][];
}

interface PromMatrixResult {
  metric: Record<string, string>;
  values: [number, string][];
}
interface PromResponse {
  status: string;
  data?: { result?: PromMatrixResult[] };
}

function seriesName(metric: Record<string, string>): string {
  return Object.entries(metric)
    .filter(([k]) => k !== '__name__')
    .map(([k, v]) => `${k}=${v}`)
    .join(',');
}

/** Parse a Prometheus range-query (matrix) response into chart series. */
export function parsePromMatrix(json: PromResponse, label = 'value'): Series[] {
  return (json.data?.result ?? []).map((r) => ({
    name: seriesName(r.metric) || label,
    points: r.values.map(([ts, v]) => [ts * 1000, Number(v)] as [number, number]),
  }));
}

/** The latest value across `series` (last point of the first series), or `null` if empty. */
export function latestOf(series: Series[]): number | null {
  const pts = series[0]?.points;
  return pts && pts.length > 0 ? pts[pts.length - 1][1] : null;
}

/** Run a Prometheus range query through the Engine proxy. `label` names the series when the
 *  result has no distinguishing labels (an aggregated query). */
export async function queryRange(
  query: string,
  rangeSec = 900,
  stepSec = 15,
  label = 'value',
): Promise<Series[]> {
  const end = Math.floor(Date.now() / 1000);
  const params = new URLSearchParams({
    query,
    start: String(end - rangeSec),
    end: String(end),
    step: `${stepSec}s`,
  });
  const res = await apiFetch(`/v1/stats/query_range?${params.toString()}`);
  if (!res.ok) throw new Error(`metrics query failed (${res.status})`);
  return parsePromMatrix(await res.json(), label);
}

/** One sample of a Prometheus **instant** vector: its labels + scalar value. */
export interface InstantSample {
  metric: Record<string, string>;
  value: number;
}

/** Parse a Prometheus instant-query (`vector`) response into samples. */
export function parsePromVector(json: PromResponse): InstantSample[] {
  return (json.data?.result ?? []).map((r) => {
    // An instant vector carries a single `value: [ts, "v"]` (not a `values` matrix).
    const v = (r as unknown as { value?: [number, string] }).value;
    return { metric: r.metric, value: v ? Number(v[1]) : NaN };
  });
}

/** Run a Prometheus **instant** query through the Engine proxy (e.g. `up`). */
export async function queryInstant(query: string): Promise<InstantSample[]> {
  const res = await apiFetch(`/v1/stats/query?${new URLSearchParams({ query }).toString()}`);
  if (!res.ok) throw new Error(`metrics query failed (${res.status})`);
  return parsePromVector(await res.json());
}

export type AlertLevel = 'warning' | 'critical';
export interface Alert {
  name: string;
  level: AlertLevel;
  detail: string;
}

/** A firing alert as evaluated by the metrics backend's server-side rules, normalized by the
 *  Engine's `/v1/alerts` proxy. */
export interface ServerAlert {
  name: string;
  severity: string; // 'warning' | 'critical' | …
  summary: string;
  state: string; // 'firing' | 'pending'
  value?: string;
}

/** Fetch server-evaluated firing alerts from the Engine's `/v1/alerts` proxy. Throws if the
 *  metrics backend (and thus its alerting rules) is unreachable — callers fall back to the local
 *  {@link evaluateAlerts} SLI checks. */
export async function fetchAlerts(): Promise<ServerAlert[]> {
  const res = await apiFetch('/v1/alerts');
  if (!res.ok) throw new Error(`alerts unavailable (${res.status})`);
  const json = (await res.json()) as { alerts?: ServerAlert[] };
  return json.alerts ?? [];
}

/** Map a server alert to the panel's display shape. */
export function serverAlertToDisplay(a: ServerAlert): Alert {
  return {
    name: a.name,
    level: a.severity === 'critical' ? 'critical' : 'warning',
    detail: a.state === 'pending' ? `${a.summary} (pending)` : a.summary,
  };
}

export interface SliSnapshot {
  errorRate: number | null; // query errors/s
  latencyP99: number | null; // seconds
  staleLocatorRate: number | null; // /s
}

/** Derive alert states from the latest SLI values via native thresholds. This is the
 *  **fallback** used when the metrics backend's server-side rules are unreachable; the primary
 *  source is {@link fetchAlerts} (`/v1/alerts`). Thresholds mirror the server rules so the two
 *  agree on healthy/unhealthy. */
export function evaluateAlerts(s: SliSnapshot): Alert[] {
  const alerts: Alert[] = [];
  if ((s.errorRate ?? 0) > 0.05) {
    alerts.push({
      name: 'Query errors',
      level: 'critical',
      detail: `${(s.errorRate as number).toFixed(3)} errors/s`,
    });
  }
  if ((s.latencyP99 ?? 0) > 1) {
    alerts.push({
      name: 'High query latency',
      level: 'warning',
      detail: `p99 ${(s.latencyP99 as number).toFixed(2)}s`,
    });
  }
  if ((s.staleLocatorRate ?? 0) > 1) {
    alerts.push({
      name: 'Stale-locator churn',
      level: 'warning',
      detail: `${(s.staleLocatorRate as number).toFixed(2)}/s`,
    });
  }
  return alerts;
}
