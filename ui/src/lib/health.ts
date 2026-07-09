// Cluster-health roll-up for the header Health pill (task-94). Polls the same signals the old
// Cluster screen did — Prometheus `up` + per-index ingestion — through `lib/cluster.ts`, and exposes
// one reactive `Health`. A failed scrape degrades the relevant component, never throws.
import { writable } from 'svelte/store';
import { queryInstant } from './stats';
import { getIngestion } from './api';
import {
  componentsFromUp,
  componentsFromIngestion,
  overall,
  type Component,
  type Health,
} from './cluster';

/** The current overall cluster health (`ok` until the first poll resolves). */
export const clusterHealth = writable<Health>('ok');

export async function refreshHealth(): Promise<void> {
  // Reaching this console proves the gateway is up; the other components come from the metrics +
  // control plane, each tolerant of an outage.
  const components: Component[] = [
    { name: 'gateway', group: 'Processes', health: 'ok', detail: 'serving this console' },
  ];
  try {
    components.push(...componentsFromUp(await queryInstant('up')));
  } catch {
    /* metrics unreachable — leave those components out, reflected as unknown overall */
  }
  try {
    components.push(...componentsFromIngestion(await getIngestion()));
  } catch {
    /* control plane unreachable */
  }
  clusterHealth.set(overall(components));
}

/** Start polling overall health every `ms`. Returns a stop function. */
export function startHealthPolling(ms = 15000): () => void {
  void refreshHealth();
  const timer = setInterval(() => void refreshHealth(), ms);
  return () => clearInterval(timer);
}
