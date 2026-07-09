// Network mocks for the console E2E (task-92). The SPA is a pure client of the Engine REST API,
// so intercepting `**/v1/**` lets us drive every screen deterministically with no backend. Each
// endpoint has a sensible default; a test overrides only the slot it cares about (e.g. an error or
// empty response for `search`).
import type { Page } from '@playwright/test';

export interface Reply {
  status?: number;
  /** JSON body (default 200). Ignored when `body` is set. */
  json?: unknown;
  /** Raw body (for non-JSON error payloads). */
  body?: string;
}

/** Per-endpoint overrides, keyed by the slot names below. */
export interface Overrides {
  me?: Reply;
  config?: Reply;
  search?: Reply;
  explain?: Reply;
  facets?: Reply;
  savedQueries?: Reply;
  users?: Reply;
  roles?: Reply;
  tokens?: Reply;
  suggest?: Reply;
  hydrate?: Reply;
  indexes?: Reply;
  index?: Reply;
  describeIndex?: Reply;
  describeSource?: Reply;
  compact?: Reply;
  backup?: Reply;
  backupStatus?: Reply;
  activity?: Reply;
  createIndex?: Reply;
  reindex?: Reply;
  aliases?: Reply;
  ingestion?: Reply;
  cold?: Reply;
  statsQuery?: Reply;
  statsRange?: Reply;
  alerts?: Reply;
}

function hit(id: string, fields: Record<string, unknown>, score: number) {
  return { coordinates: { identifier: [{ name: 'id', value: id }] }, score, fields };
}

/** Prometheus instant-vector response (used by the cluster `up` query). */
function promVector() {
  return {
    status: 'success',
    data: {
      resultType: 'vector',
      result: [{ metric: { instance: 'engine:9464', job: 'growlerdb' }, value: [0, '1'] }],
    },
  };
}

/** Prometheus range-matrix response (used by the observability/ingestion sparklines). Values are
 *  small + healthy: the latest (0.008) is below every alert threshold (errorRate>0.05, latencyP99>1s,
 *  staleLocatorRate>1), so the alerts panel deterministically shows "No alerts firing". */
function promMatrix() {
  // Small, healthy values: the latest (0.008) is below every alert threshold (errorRate>0.05,
  // latencyP99>1s, staleLocatorRate>1), so Observability deterministically shows "No alerts firing"
  // once a refresh resolves — no race between the initial empty state and the post-refresh eval.
  return {
    status: 'success',
    data: {
      resultType: 'matrix',
      result: [
        {
          metric: {},
          values: [
            [0, '0.002'],
            [15, '0.005'],
            [30, '0.008'],
          ],
        },
      ],
    },
  };
}

function defaults(): Required<Overrides> {
  return {
    me: { json: { authenticated: false, subject: '', roles: [] } },
    // Open mode + a configured Grafana URL (task-140): the Observability deep-dashboards card links
    // to this runtime-provided URL; tests override with no grafana_url to assert the card hides.
    config: { json: { auth_required: false, grafana_url: 'http://grafana.local:3000' } },
    search: {
      json: {
        total: 2,
        partial: false,
        hits: [
          hit('evt-1', { device_id: 'sensor-1', status: 'ok' }, 1.5),
          hit('evt-2', { device_id: 'sensor-2', status: 'critical' }, 1.2),
        ],
      },
    },
    explain: {
      json: {
        found: true,
        matched: true,
        score: 1.5,
        detail: {
          description: 'BooleanQuery',
          score: 1.5,
          details: [{ description: 'TermQuery(status:ok)', score: 1.5 }],
        },
        analyzed: [{ field: 'status', terms: ['ok'] }],
        timings: { index_ms: 0.4, hydration_ms: 1.2, total_ms: 2.0 },
        shards_scanned: 1,
        shards_total: 3,
      },
    },
    facets: {
      json: {
        facets: [
          {
            field: 'device_id',
            buckets: [
              { value: 'sensor-1', count: 5 },
              { value: 'sensor-2', count: 3 },
            ],
          },
        ],
      },
    },
    savedQueries: { json: { queries: [] } },
    users: { json: { users: [] } },
    roles: { json: { roles: ['reader', 'operator', 'admin'] } },
    tokens: { json: { tokens: [] } },
    suggest: { json: { suggestions: [] } },
    hydrate: {
      json: {
        rows: [
          {
            key: { identifier: [{ name: 'id', value: 'evt-1' }] },
            fields: { device_id: 'sensor-1', body: 'temperature within range', status: 'ok' },
          },
        ],
      },
    },
    indexes: { json: { indexes: [{ name: 'telemetry', status: 'ready' }] } },
    index: {
      json: {
        name: 'telemetry',
        status: 'ready',
        shard_count: 3,
        routing: 'hash',
        fields: [
          { path: 'id', type: 'KEYWORD', fast: false, cached: false, pk: true },
          { path: 'device_id', type: 'KEYWORD', analyzer: '', fast: true, cached: true, pk: false },
          {
            path: 'body',
            type: 'TEXT',
            analyzer: 'standard',
            fast: false,
            cached: false,
            pk: false,
            blocked: 'big text (D23)',
          },
        ],
        shards: [
          { ordinal: 0, primary: 'node-a', replicas: ['node-a2'], state: 'active' },
          { ordinal: 1, primary: 'node-b', replicas: [], state: 'active' },
          { ordinal: 2, primary: '', replicas: [], state: 'building' },
        ],
      },
    },
    describeIndex: {
      json: {
        name: 'telemetry',
        snapshot: 42,
        num_docs: 12345,
        generation_count: 2,
        checkpoint: 'snap-42',
      },
    },
    describeSource: {
      json: {
        fields: [
          { path: 'id', type: 'string' },
          { path: 'device_id', type: 'string' },
          { path: 'site', type: 'string' },
          { path: 'reading', type: 'long' },
          { path: 'blob', type: 'binary' },
        ],
        partition_fields: ['site'],
        identifier_fields: ['id'],
      },
    },
    createIndex: { json: { name: 'telemetry_v2' } },
    reindex: { json: { doc_count: 12345, snapshot: 43 } },
    compact: { json: { segments_before: 4, segments_after: 1 } },
    backup: {
      json: {
        snapshot: 42,
        file_count: 12,
        created_ms: 1700000000000,
        prefix: 'backups/telemetry',
      },
    },
    backupStatus: { json: { configured: true, present: true, snapshot: 42 } },
    activity: {
      json: {
        events: [
          {
            ts_ms: 1700000200000,
            kind: 'alias.swapped',
            message: 'alias `live` → `telemetry` swapped',
          },
          { ts_ms: 1700000000000, kind: 'index.created', message: 'index `telemetry` created' },
        ],
      },
    },
    aliases: { json: { aliases: [] } },
    ingestion: {
      json: {
        items: [
          {
            name: 'telemetry',
            status: 'ready',
            source_table: 'factory.telemetry',
            routing: 'hash',
            shard_count: 2,
            source_snapshot_id: 99,
            source_timestamp_ms: 1700000000000,
            shards: [
              {
                ordinal: 0,
                node: 'node-a',
                committed_snapshot_id: 99,
                index_snapshot: 99,
                state: 'in_sync',
                lag_ms: 0,
              },
              {
                ordinal: 1,
                node: 'node-b',
                committed_snapshot_id: 90,
                index_snapshot: 90,
                state: 'behind',
                lag_ms: 45000,
              },
            ],
          },
        ],
      },
    },
    cold: { status: 404, json: {} },
    statsQuery: { json: promVector() },
    statsRange: { json: promMatrix() },
    // Server-side alert rules (task-111): empty by default → panel shows "No alerts firing" with the
    // "Server rules" badge. Tests override with firing alerts to exercise the panel.
    alerts: { json: { alerts: [] } },
  };
}

/** Install the mock router on `page`. Call before navigating. */
export async function installMocks(page: Page, overrides: Overrides = {}): Promise<void> {
  const slots = { ...defaults(), ...overrides };

  await page.route('**/v1/**', async (route) => {
    const req = route.request();
    const url = new URL(req.url());
    const p = url.pathname;
    const m = req.method();

    let reply: Reply | undefined;
    if (p === '/v1/me') reply = slots.me;
    else if (p === '/v1/config') reply = slots.config;
    else if (p === '/v1/search') reply = slots.search;
    else if (p === '/v1/explain') reply = slots.explain;
    else if (p === '/v1/facets') reply = slots.facets;
    else if (p === '/v1/saved-queries' && m === 'GET') reply = slots.savedQueries;
    else if (p === '/v1/saved-queries' && m === 'POST') reply = { json: { id: 'sq-new' } };
    else if (p.startsWith('/v1/saved-queries/') && m === 'PUT') reply = { json: { id: 'sq-new' } };
    else if (p.startsWith('/v1/saved-queries/') && m === 'DELETE') reply = { json: {} };
    else if (p === '/v1/users' && m === 'GET') reply = slots.users;
    else if (p === '/v1/roles') reply = slots.roles;
    else if (p.startsWith('/v1/users/') && m === 'PUT')
      reply = { json: { subject: 'x', roles: [] } };
    else if (p === '/v1/tokens' && m === 'GET') reply = slots.tokens;
    else if (p === '/v1/tokens' && m === 'POST')
      reply = {
        json: {
          token: { id: 't1', label: 'ci', prefix: 'gdb_live_ab', roles: ['reader'] },
          secret: 'gdb_live_abcd1234secret',
        },
      };
    else if (p.startsWith('/v1/tokens/') && m === 'DELETE') reply = { json: {} };
    else if (p === '/v1/suggest') reply = slots.suggest;
    else if (p === '/v1/keys:get') reply = slots.hydrate;
    else if (p === '/v1/indexes' && m === 'GET') reply = slots.indexes;
    else if (p === '/v1/indexes' && m === 'POST') reply = slots.createIndex;
    else if (p.startsWith('/v1/indexes/') && m === 'DELETE') reply = { json: {} };
    else if (p.startsWith('/v1/indexes/')) reply = slots.index;
    else if (p === '/v1/index:describe') reply = slots.describeIndex;
    else if (p === '/v1/index:reindex') reply = slots.reindex;
    else if (p === '/v1/index:compact') reply = slots.compact;
    else if (p === '/v1/index:backup') reply = slots.backup;
    else if (p === '/v1/index:backup-status') reply = slots.backupStatus;
    else if (p === '/v1/index:activity') reply = slots.activity;
    else if (p === '/v1/source:describe') reply = slots.describeSource;
    else if (p === '/v1/aliases' && m === 'GET') reply = slots.aliases;
    else if (p === '/v1/aliases' && m === 'POST') reply = { json: {} };
    else if (p.startsWith('/v1/aliases/') && m === 'DELETE') reply = { json: {} };
    else if (p === '/v1/ingestion') reply = slots.ingestion;
    else if (p === '/v1/cold') reply = slots.cold;
    else if (p === '/v1/stats/query_range') reply = slots.statsRange;
    else if (p === '/v1/stats/query') reply = slots.statsQuery;
    else if (p === '/v1/alerts') reply = slots.alerts;

    if (!reply) {
      await route.fulfill({ status: 404, contentType: 'application/json', body: '{}' });
      return;
    }
    await route.fulfill({
      status: reply.status ?? 200,
      contentType: 'application/json',
      body: reply.body ?? JSON.stringify(reply.json ?? {}),
    });
  });
}
