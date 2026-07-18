<script lang="ts">
  // One screen that answers the product questions ("Does GrowlerDB keep up with Iceberg?",
  // "…match Iceberg?", "index:source size ratio?"). Organised into sub-tabs (Search · Runtime · Data · Ingestion ·
  // Source · Access) with Alerts as a persistent strip. Clean cards (value + sparkline) carry a ⓘ info
  // popover for self-serve help; a few "hero" ECharts overlays show the relationships sparklines
  // can't. Data comes from the /v1/stats Prometheus proxy + the /v1/ingestion control-plane feed.
  import { onMount, onDestroy } from 'svelte';
  import { t } from '../lib/i18n';
  import {
    queryRange,
    latestOf,
    evaluateAlerts,
    fetchAlerts,
    serverAlertToDisplay,
    type Series,
    type Alert,
  } from '../lib/stats';
  import { serverConfig, getIngestion, type IndexIngestion } from '../lib/api';
  import { worstState, badgeLevel, worstLagMs, formatDuration, formatAge } from '../lib/ingestion';
  import MetricCard from '../lib/components/MetricCard.svelte';
  import Sparkline from '../lib/components/Sparkline.svelte';
  import Badge from '../lib/components/Badge.svelte';
  import Tabs from '../lib/components/Tabs.svelte';
  import InfoDot from '../lib/components/InfoDot.svelte';
  import EChart from '../lib/components/EChart.svelte';

  const REFRESH_MS = 15000;

  // ---- section + panel model -------------------------------------------------------------------

  type SectionId = 'search' | 'runtime' | 'access' | 'data' | 'ingestion' | 'source';
  type Fmt = 'rate' | 'ms' | 'lagms' | 'pct' | 'num' | 'bytes' | 'ratio';
  type Bad = 'nonzero' | 'lag' | 'lowpct' | 'highpct';

  interface Card {
    key: string;
    label: string;
    fmt: Fmt;
    headline: string;
    queries: { label: string; q: string }[];
    help: string;
    hint?: string;
    bad?: Bad;
    /** Needs backend instrumentation not landed yet → renders as a "planned" tile. */
    planned?: boolean;
    /** Comes from the external cluster metrics stack (node-exporter/kube-state), which not every
     *  deployment runs. When its query returns nothing, show a "needs the metrics stack" state
     *  instead of a misleading 0. */
    external?: boolean;
  }

  interface Hero {
    key: string;
    label: string;
    help: string;
    unit: string;
    queries: { label: string; q: string }[];
    /** Which series render as a filled area vs. a plain line. */
    area?: string[];
    stack?: boolean;
  }

  const SEARCH_HERO: Hero = {
    key: 'search-latency',
    label: 'Query latency (p50 · p95 · p99)',
    help: 'Search response time percentiles over a 5-minute window. p99 rising while p50 stays flat points at tail latency — a slow shard or GC pause.',
    unit: 'ms',
    queries: [
      {
        label: 'p50',
        q: 'histogram_quantile(0.5, sum by (le) (rate(growlerdb_query_duration_seconds_bucket[5m]))) * 1000',
      },
      {
        label: 'p95',
        q: 'histogram_quantile(0.95, sum by (le) (rate(growlerdb_query_duration_seconds_bucket[5m]))) * 1000',
      },
      {
        label: 'p99',
        q: 'histogram_quantile(0.99, sum by (le) (rate(growlerdb_query_duration_seconds_bucket[5m]))) * 1000',
      },
    ],
  };

  const INGESTION_HERO: Hero = {
    key: 'ingestion-rate',
    label: 'Does GrowlerDB keep up? — Iceberg append vs GrowlerDB index',
    help: 'The source append rate (rows committed to Iceberg) overlaid on the GrowlerDB index rate. When the index line tracks the append line, ingestion is keeping up; a persistent gap means it is falling behind (watch the lag card).',
    unit: '/s',
    queries: [
      { label: 'Iceberg append', q: 'sum(deriv(growlerdb_source_records[5m]))' },
      { label: 'GrowlerDB index', q: 'sum(rate(growlerdb_ingested_docs_total[5m]))' },
    ],
    area: ['Iceberg append'],
  };

  const DATA_HERO: Hero = {
    key: 'data-size',
    label: 'Index size, by component',
    help: 'On-disk index size broken into its drivers — the inverted (search) index by file kind (term dictionaries, postings, positions, fieldnorms), the fast-field cache, doc store, hydration locator, and metadata — so the index:source ratio can be attributed rather than read as one lump. The components sum to the total index size exactly.',
    unit: 'bytes',
    queries: [
      { label: 'term dicts', q: 'sum(growlerdb_index_bytes_component{component="term"})' },
      { label: 'postings', q: 'sum(growlerdb_index_bytes_component{component="postings"})' },
      { label: 'positions', q: 'sum(growlerdb_index_bytes_component{component="positions"})' },
      { label: 'fieldnorms', q: 'sum(growlerdb_index_bytes_component{component="fieldnorms"})' },
      { label: 'fast', q: 'sum(growlerdb_index_bytes_component{component="fast"})' },
      { label: 'store', q: 'sum(growlerdb_index_bytes_component{component="store"})' },
      { label: 'locator', q: 'sum(growlerdb_index_bytes_component{component="locator"})' },
      { label: 'other', q: 'sum(growlerdb_index_bytes_component{component="other"})' },
    ],
    stack: true,
    area: [
      'term dicts',
      'postings',
      'positions',
      'fieldnorms',
      'fast',
      'store',
      'locator',
      'other',
    ],
  };

  const SOURCE_HERO: Hero = {
    key: 'source-smallfile',
    label: 'Small-file signal — mean source file size',
    help: 'The average data-file size in the source table. A low or falling value means the source is accumulating many small files, which slows GrowlerDB’s O(files) query path. The fix is the user’s: Iceberg compaction.',
    unit: 'bytes',
    queries: [{ label: 'avg file', q: 'avg(growlerdb_source_avg_file_bytes)' }],
    area: ['avg file'],
  };

  const HEROES: Record<SectionId, Hero | null> = {
    search: SEARCH_HERO,
    runtime: null,
    access: null,
    data: DATA_HERO,
    ingestion: INGESTION_HERO,
    source: SOURCE_HERO,
  };

  const SECTIONS: { id: SectionId; label: string; cards: Card[] }[] = [
    {
      id: 'search',
      label: 'Search',
      cards: [
        {
          key: 'qps',
          label: 'Query rate',
          fmt: 'rate',
          headline: 'v',
          queries: [{ label: 'v', q: 'sum(rate(growlerdb_query_total[1m]))' }],
          help: 'Completed searches per second across the cluster.',
        },
        {
          key: 'errors',
          label: 'Error rate',
          fmt: 'rate',
          headline: 'v',
          bad: 'nonzero',
          queries: [{ label: 'v', q: 'sum(rate(growlerdb_query_errors_total[1m]))' }],
          help: 'Searches that returned an error, per second.',
          hint: 'Anything above zero is worth a look in the node logs.',
        },
        {
          key: 'hydrate-rate',
          label: 'Hydrate rate',
          fmt: 'rate',
          headline: 'v',
          queries: [{ label: 'v', q: 'sum(rate(growlerdb_hydration_total[1m]))' }],
          help: 'Key→row hydrations per second — fetching authoritative rows from Iceberg for a hit.',
        },
        {
          key: 'hydrate-lat',
          label: 'Hydrate latency p95',
          fmt: 'ms',
          headline: 'v',
          queries: [
            {
              label: 'v',
              q: 'histogram_quantile(0.95, sum by (le) (rate(growlerdb_hydration_duration_seconds_bucket[5m]))) * 1000',
            },
          ],
          help: '95th-percentile time to hydrate a row from the source table.',
        },
        {
          key: 'stale',
          label: 'Stale-locator rate',
          fmt: 'rate',
          headline: 'v',
          bad: 'nonzero',
          queries: [{ label: 'v', q: 'sum(rate(growlerdb_stale_locators_total[5m]))' }],
          help: 'Locators Iceberg had rewritten, per second — an index↔source drift signal.',
          hint: 'Sustained >0 means the source is being compacted under the index; hydration self-heals via verify-fallback.',
        },
        {
          key: 'drift',
          label: 'Drift repaired',
          fmt: 'rate',
          headline: 'v',
          bad: 'nonzero',
          queries: [
            {
              label: 'v',
              q: 'sum(rate(growlerdb_drift_stale_total[1h])) + sum(rate(growlerdb_drift_missing_total[1h]))',
            },
          ],
          help: 'Docs the reconcile backstop repaired (stale deleted / missing re-indexed), per second.',
          hint: 'Nonzero means the index had silently drifted from the source — investigate the ingest path.',
        },
        {
          key: 'cachehit',
          label: 'Cold cache hit rate',
          fmt: 'pct',
          headline: 'v',
          bad: 'lowpct',
          queries: [
            {
              label: 'v',
              q: 'sum(rate(growlerdb_cold_cache_hits_total[5m])) / (sum(rate(growlerdb_cold_cache_hits_total[5m])) + sum(rate(growlerdb_cold_cache_misses_total[5m]))) * 100',
            },
          ],
          help: 'Share of cold-tier range reads served from cache rather than object storage.',
        },
        {
          key: 'query-status',
          label: 'Query status codes',
          fmt: 'rate',
          headline: '5xx',
          bad: 'nonzero',
          queries: [
            {
              label: '2xx',
              q: 'sum(rate(growlerdb_http_requests_total{route="/v1/search",status=~"2.."}[5m]))',
            },
            {
              label: '4xx',
              q: 'sum(rate(growlerdb_http_requests_total{route="/v1/search",status=~"4.."}[5m]))',
            },
            {
              label: '5xx',
              q: 'sum(rate(growlerdb_http_requests_total{route="/v1/search",status=~"5.."}[5m]))',
            },
          ],
          help: 'Response codes for the search endpoint. The value is the 5xx (server-error) rate; expand for the full 2xx/4xx/5xx mix. Cluster-wide API codes are on the Runtime tab.',
        },
      ],
    },
    {
      id: 'runtime',
      label: 'Runtime',
      cards: [
        {
          key: 'procs',
          label: 'Processes up',
          fmt: 'num',
          headline: 'v',
          queries: [{ label: 'v', q: 'sum(up)' }],
          help: 'Scrape targets currently reachable (gateway, nodes, control plane).',
        },
        {
          key: 'api-rate',
          label: 'API request rate',
          fmt: 'rate',
          headline: 'v',
          queries: [{ label: 'v', q: 'sum(rate(growlerdb_http_requests_total[1m]))' }],
          help: 'HTTP requests per second across all REST endpoints (search, hydrate, admin, config).',
        },
        {
          key: 'api-errors',
          label: 'API server errors',
          fmt: 'rate',
          headline: 'v',
          bad: 'nonzero',
          queries: [
            { label: 'v', q: 'sum(rate(growlerdb_http_requests_total{status=~"5.."}[1m]))' },
          ],
          help: '5xx responses per second across all endpoints — the real failure signal.',
          hint: 'Sustained >0 means the API is erroring; check the node logs.',
        },
        {
          key: 'api-status',
          label: 'API status mix',
          fmt: 'rate',
          headline: '4xx',
          queries: [
            { label: '2xx', q: 'sum(rate(growlerdb_http_requests_total{status=~"2.."}[1m]))' },
            { label: '4xx', q: 'sum(rate(growlerdb_http_requests_total{status=~"4.."}[1m]))' },
            { label: '5xx', q: 'sum(rate(growlerdb_http_requests_total{status=~"5.."}[1m]))' },
          ],
          help: 'Response-code mix across all endpoints. The value is the 4xx (client-error) rate; expand for the 2xx/4xx/5xx breakdown.',
        },
        {
          key: 'api-latency',
          label: 'API latency p95',
          fmt: 'ms',
          headline: 'v',
          queries: [
            {
              label: 'v',
              q: 'histogram_quantile(0.95, sum by (le) (rate(growlerdb_http_request_duration_seconds_bucket[5m]))) * 1000',
            },
          ],
          help: '95th-percentile request latency across endpoints (the slowest endpoint’s p95).',
        },
        {
          key: 'cpu',
          label: 'Busiest node CPU',
          fmt: 'pct',
          headline: 'v',
          external: true,
          bad: 'highpct',
          queries: [
            {
              label: 'v',
              q: 'max(100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100))',
            },
          ],
          help: 'CPU utilisation of the busiest node. From node-exporter (the cluster metrics stack); shows a "needs the metrics stack" state if that isn’t running.',
        },
        {
          key: 'mem',
          label: 'Busiest node memory',
          fmt: 'pct',
          headline: 'v',
          external: true,
          bad: 'highpct',
          queries: [
            {
              label: 'v',
              q: 'max(100 * (1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes))',
            },
          ],
          help: 'Memory used on the busiest node. From node-exporter; needs the cluster metrics stack.',
        },
        {
          key: 'disk',
          label: 'Fullest node disk',
          fmt: 'pct',
          headline: 'v',
          external: true,
          bad: 'highpct',
          queries: [
            {
              label: 'v',
              q: 'max(100 * (1 - node_filesystem_avail_bytes{fstype!~"tmpfs|overlay|ramfs|squashfs"} / node_filesystem_size_bytes{fstype!~"tmpfs|overlay|ramfs|squashfs"}))',
            },
          ],
          help: 'Fullest real filesystem across nodes. From node-exporter; needs the cluster metrics stack.',
        },
      ],
    },
    {
      id: 'data',
      label: 'Data',
      cards: [
        {
          key: 'total-size',
          label: 'GrowlerDB size',
          fmt: 'bytes',
          headline: 'v',
          queries: [{ label: 'v', q: 'sum(growlerdb_index_bytes)' }],
          help: 'Total on-disk index size across every shard.',
        },
        {
          key: 'segments',
          label: 'Live segments',
          fmt: 'num',
          headline: 'v',
          queries: [{ label: 'v', q: 'sum(growlerdb_segments_live)' }],
          help: 'Live Tantivy segments across shards — the merge-pressure signal.',
        },
        {
          key: 'match',
          label: 'Iceberg match',
          fmt: 'pct',
          headline: 'v',
          bad: 'lowpct',
          queries: [
            {
              label: 'v',
              q: 'sum(rate(growlerdb_hydration_keys_found_total[5m])) / sum(rate(growlerdb_hydration_keys_requested_total[5m])) * 100',
            },
          ],
          help: 'Share of searched keys still present in the source — 100% means the index matches Iceberg.',
          hint: 'A dip means search returned hits whose rows are gone from the source (a drifted or recreated table).',
        },
        {
          key: 'shard-skew',
          label: 'Per-shard skew',
          fmt: 'ratio',
          headline: 'v',
          queries: [
            {
              label: 'v',
              q: 'max(max by (index) (growlerdb_index_bytes) / clamp_min(avg by (index) (growlerdb_index_bytes), 1))',
            },
          ],
          help: 'Largest vs. average shard size for the most-skewed index — 1.0× means shards are balanced; higher means one shard carries more. Computed from per-shard growlerdb_index_bytes across serving nodes.',
        },
      ],
    },
    {
      id: 'ingestion',
      label: 'Ingestion',
      cards: [
        {
          key: 'throughput',
          label: 'Throughput',
          fmt: 'rate',
          headline: 'v',
          queries: [{ label: 'v', q: 'sum(rate(growlerdb_ingested_docs_total[5m]))' }],
          help: 'Documents GrowlerDB indexed per second across the cluster.',
        },
        {
          key: 'lag',
          label: 'Ingestion lag',
          fmt: 'lagms',
          headline: 'v',
          bad: 'lag',
          queries: [{ label: 'v', q: 'max(growlerdb_ingest_lag_ms)' }],
          help: 'The worst shard’s wall-clock staleness behind the source head.',
          hint: 'Above ~1s and climbing means ingest is falling behind — check the connector and nodes.',
        },
        {
          key: 'shards',
          label: 'Shards up',
          fmt: 'num',
          headline: 'up',
          queries: [
            { label: 'up', q: 'sum(growlerdb_shards_up)' },
            { label: 'total', q: 'sum(growlerdb_shards_total)' },
          ],
          help: 'Shards with a reachable primary, of the total shard count.',
        },
      ],
    },
    {
      id: 'source',
      label: 'Source',
      cards: [
        {
          key: 'src-rows',
          label: 'Source rows',
          fmt: 'num',
          headline: 'v',
          queries: [{ label: 'v', q: 'sum(growlerdb_source_records)' }],
          help: 'Rows in the source table’s current snapshot.',
        },
        {
          key: 'src-bytes',
          label: 'Source size',
          fmt: 'bytes',
          headline: 'v',
          queries: [{ label: 'v', q: 'sum(growlerdb_source_bytes)' }],
          help: 'Total data-file bytes in the source’s current snapshot.',
        },
        {
          key: 'src-files',
          label: 'Data files',
          fmt: 'num',
          headline: 'v',
          queries: [{ label: 'v', q: 'sum(growlerdb_source_data_files)' }],
          help: 'Data files in the current snapshot — the driver of GrowlerDB’s O(files) read cost.',
        },
        {
          key: 'src-avgfile',
          label: 'Avg file size',
          fmt: 'bytes',
          headline: 'v',
          bad: 'lowpct',
          queries: [{ label: 'v', q: 'avg(growlerdb_source_avg_file_bytes)' }],
          help: 'Mean source data-file size — the small-file signal.',
          hint: 'Low or falling means many small files; the source wants Iceberg compaction (rewrite_data_files).',
        },
        {
          key: 'src-deletes',
          label: 'Delete files',
          fmt: 'num',
          headline: 'v',
          bad: 'nonzero',
          queries: [{ label: 'v', q: 'sum(growlerdb_source_delete_files)' }],
          help: 'Merge-on-read delete files in the current snapshot — read overhead.',
          hint: 'A high count means reads pay to apply deletes; compaction rewrites them away.',
        },
        {
          key: 'src-snapshots',
          label: 'Snapshots',
          fmt: 'num',
          headline: 'v',
          queries: [{ label: 'v', q: 'max(growlerdb_source_snapshots)' }],
          help: 'Retained source snapshots — metadata history depth.',
          hint: 'Unbounded growth means fat metadata; the source wants expire_snapshots.',
        },
        {
          key: 'src-commits',
          label: 'Commit rate',
          fmt: 'rate',
          headline: 'v',
          queries: [{ label: 'v', q: 'sum(deriv(growlerdb_source_snapshots[10m]))' }],
          help: 'New source snapshots per second — how often the source commits.',
        },
        {
          key: 'src-partitions',
          label: 'Partition skew',
          fmt: 'ratio',
          headline: 'v',
          queries: [{ label: 'v', q: 'max(growlerdb_source_partition_skew)' }],
          help: 'Largest source partition’s record count vs. the mean, for the most-skewed index — 1.0× means partitions are evenly sized; higher means a hotspot partition (lopsided ingest or a hot key). Only reported for identity-partitioned sources.',
        },
      ],
    },
    {
      id: 'access',
      label: 'Access',
      cards: [
        {
          key: 'logins',
          label: 'Logins',
          fmt: 'rate',
          headline: 'v',
          queries: [{ label: 'v', q: 'sum(rate(growlerdb_logins_total{outcome="success"}[5m]))' }],
          help: 'Successful built-in sign-ins per second. OIDC logins are minted by the external identity provider and aren’t counted here.',
        },
        {
          key: 'login-failures',
          label: 'Login failures',
          fmt: 'rate',
          headline: 'v',
          bad: 'nonzero',
          queries: [{ label: 'v', q: 'sum(rate(growlerdb_logins_total{outcome!="success"}[5m]))' }],
          help: 'Failed sign-in attempts per second (bad credentials, lockout, or shed under load) — a brute-force or misconfiguration signal.',
          hint: 'A sustained spike is worth investigating; the login throttle locks an account after repeated failures.',
        },
      ],
    },
  ];

  // ---- state -----------------------------------------------------------------------------------

  let active = $state<SectionId>('search');
  let cardSeries = $state<Record<string, Series[]>>({});
  let heroSeries = $state<Record<string, Series[]>>({});
  let ingestion = $state<IndexIngestion[]>([]);
  let ingestionError = $state('');
  let expanded = $state<string | null>(null);
  // The card whose detail chart (full axes + legend) is open in the modal, or null.
  let detailCard = $state<Card | null>(null);
  let alerts = $state<Alert[]>([]);
  let rulesActive = $state(false);
  let error = $state('');
  let grafanaUrl = $state('');
  let timer: ReturnType<typeof setInterval> | undefined;
  let now = $state(Date.now());

  // ---- formatting ------------------------------------------------------------------------------

  function fmtBytes(n: number): { value: string; unit: string } {
    const u = ['B', 'KB', 'MB', 'GB', 'TB', 'PB'];
    let i = 0;
    let v = n;
    while (v >= 1024 && i < u.length - 1) {
      v /= 1024;
      i++;
    }
    return { value: v < 10 && i > 0 ? v.toFixed(1) : Math.round(v).toString(), unit: u[i] };
  }
  function fmtNum(n: number): { value: string; unit: string } {
    if (n >= 1_000_000) return { value: (n / 1_000_000).toFixed(1), unit: 'M' };
    if (n >= 1000) return { value: (n / 1000).toFixed(1), unit: 'k' };
    return { value: Math.round(n).toString(), unit: '' };
  }

  function display(card: Card): { value: string; unit: string } {
    if (card.planned) return { value: '—', unit: '' };
    const head = cardSeries[card.key]?.find((s) => s.name === card.headline);
    const v = latestOf(head ? [head] : []);
    if (v == null) {
      // External-stack metric with no data ⇒ the stack isn't running: show "—", not a fake 0.
      if (card.external) return { value: '—', unit: '' };
      if (card.fmt === 'bytes') return { value: '0', unit: 'B' };
      if (card.fmt === 'pct') return { value: '0', unit: '%' };
      if (card.fmt === 'ms' || card.fmt === 'lagms') return { value: '0', unit: 'ms' };
      return { value: '0', unit: card.fmt === 'rate' ? '/s' : '' };
    }
    switch (card.fmt) {
      case 'bytes':
        return fmtBytes(v);
      case 'ratio':
        return { value: v.toFixed(2), unit: '×' };
      case 'pct':
        return { value: v.toFixed(v < 10 ? 1 : 0), unit: '%' };
      case 'ms':
        return { value: Math.round(v).toString(), unit: 'ms' };
      case 'lagms':
        return v >= 1000
          ? { value: (v / 1000).toFixed(1), unit: 's' }
          : { value: Math.round(v).toString(), unit: 'ms' };
      case 'rate': {
        const f = fmtNum(v);
        return { value: f.value, unit: `${f.unit}/s` };
      }
      case 'num':
      default: {
        const f = fmtNum(v);
        return { value: f.value, unit: f.unit };
      }
    }
  }

  function sub(card: Card): string {
    if (card.key === 'shards') {
      const total = latestOf((cardSeries[card.key] ?? []).filter((s) => s.name === 'total'));
      return total == null ? '' : `of ${Math.round(total)} shards`;
    }
    return '';
  }

  function toneOf(card: Card): 'default' | 'ok' | 'warn' {
    if (card.planned) return 'default';
    const head = cardSeries[card.key]?.find((s) => s.name === card.headline);
    const v = latestOf(head ? [head] : []);
    if (v == null) return 'default';
    if (card.bad === 'nonzero') return v > 0 ? 'warn' : 'default';
    if (card.bad === 'lag') return v >= 1000 ? 'warn' : 'default';
    if (card.bad === 'lowpct') return v >= 80 ? 'ok' : v > 0 ? 'warn' : 'default';
    if (card.bad === 'highpct') return v >= 85 ? 'warn' : 'default';
    return 'default';
  }
  /** Whether a card's headline query returned any points (used to detect an absent external stack). */
  function hasData(card: Card): boolean {
    const head = cardSeries[card.key]?.find((s) => s.name === card.headline);
    return !!head && head.points.length > 0;
  }
  const TONE_COLOR: Record<'default' | 'ok' | 'warn', string> = {
    default: 'var(--accent)',
    ok: 'var(--ok)',
    warn: 'var(--warn)',
  };

  function points(card: Card): number[] {
    const head = cardSeries[card.key]?.find((s) => s.name === card.headline);
    const pts = (head?.points ?? []).map((p) => p[1]);
    return pts.length ? pts : [0, 0];
  }
  function times(card: Card): number[] {
    const head = cardSeries[card.key]?.find((s) => s.name === card.headline);
    return (head?.points ?? []).map((p) => p[0]);
  }
  /** A value formatter matching the card's unit, for the sparkline hover tooltip. */
  function cardFormat(card: Card): (n: number) => string {
    switch (card.fmt) {
      case 'bytes':
        return (n) => {
          const b = fmtBytes(n);
          return `${b.value} ${b.unit}`;
        };
      case 'ratio':
        return (n) => `${n.toFixed(2)}×`;
      case 'pct':
        return (n) => `${n.toFixed(1)}%`;
      case 'ms':
        return (n) => `${Math.round(n)} ms`;
      case 'lagms':
        return (n) => (n >= 1000 ? `${(n / 1000).toFixed(1)} s` : `${Math.round(n)} ms`);
      case 'rate':
        return (n) => {
          const f = fmtNum(n);
          return `${f.value}${f.unit}/s`;
        };
      default:
        return (n) => {
          const f = fmtNum(n);
          return `${f.value}${f.unit}`;
        };
    }
  }

  // ---- hero chart option -----------------------------------------------------------------------

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  function heroOption(hero: Hero): any {
    const loaded = heroSeries[hero.key] ?? [];
    const bytes = hero.unit === 'bytes';
    const series = hero.queries.map((q) => {
      const s = loaded.find((x) => x.name === q.label);
      const isArea = hero.area?.includes(q.label);
      return {
        name: q.label,
        type: 'line',
        smooth: true,
        showSymbol: false,
        stack: hero.stack ? 'total' : undefined,
        areaStyle: isArea ? { opacity: hero.stack ? 0.55 : 0.14 } : undefined,
        lineStyle: { width: 2 },
        data: (s?.points ?? []).map((p) => [p[0], p[1]]),
      };
    });
    return {
      legend: {},
      tooltip: {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        valueFormatter: (v: any) => {
          if (v == null) return '—';
          if (bytes) {
            const b = fmtBytes(Number(v));
            return `${b.value} ${b.unit}`;
          }
          return Number(v).toFixed(2);
        },
      },
      xAxis: { type: 'time' },
      yAxis: {
        type: 'value',
        axisLabel: {
          // eslint-disable-next-line @typescript-eslint/no-explicit-any
          formatter: (v: any) => {
            if (bytes) {
              const b = fmtBytes(Number(v));
              return `${b.value}${b.unit}`;
            }
            return fmtNum(Number(v)).value + fmtNum(Number(v)).unit;
          },
        },
      },
      series,
    };
  }

  const activeHero = $derived(HEROES[active]);

  // Full detail chart for one card (the click-to-expand modal): every series it queried, drawn
  // large with real x/y axes, a legend, and unit-aware tooltips.
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  function cardDetailOption(card: Card): any {
    const f = cardFormat(card);
    const loaded = cardSeries[card.key] ?? [];
    const single = loaded.length <= 1;
    const series = loaded.map((s) => ({
      name: s.name === 'v' ? card.label : s.name,
      type: 'line',
      smooth: true,
      showSymbol: false,
      lineStyle: { width: 2 },
      areaStyle: single ? { opacity: 0.12 } : undefined,
      data: s.points.map((p) => [p[0], p[1]]),
    }));
    return {
      legend: single ? { show: false } : {},
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      tooltip: { valueFormatter: (v: any) => (v == null ? '—' : f(Number(v))) },
      xAxis: { type: 'time' },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      yAxis: { type: 'value', axisLabel: { formatter: (v: any) => f(Number(v)) } },
      series,
    };
  }

  // ---- refresh ---------------------------------------------------------------------------------

  /** Tracks proxy health across a refresh: one bad query shouldn't blank the screen, but a total
   *  outage (every query failed) should surface the "metrics unavailable" banner. */
  interface LoadAcc {
    attempted: number;
    failed: number;
    lastError: string;
  }

  async function loadSeries(
    queries: { label: string; q: string }[],
    acc: LoadAcc,
  ): Promise<Series[]> {
    const out: Series[] = [];
    for (const { label, q } of queries) {
      acc.attempted++;
      try {
        const got = await queryRange(q, 1800, 30, label);
        if (got[0]) out.push({ name: label, points: got[0].points });
      } catch (err) {
        acc.failed++;
        acc.lastError = String(err);
      }
    }
    return out;
  }

  async function refresh() {
    now = Date.now();
    const acc: LoadAcc = { attempted: 0, failed: 0, lastError: '' };
    for (const section of SECTIONS) {
      for (const card of section.cards) {
        if (card.planned || card.queries.length === 0) continue;
        cardSeries[card.key] = await loadSeries(card.queries, acc);
      }
    }
    for (const hero of Object.values(HEROES)) {
      if (hero) heroSeries[hero.key] = await loadSeries(hero.queries, acc);
    }
    cardSeries = { ...cardSeries };
    heroSeries = { ...heroSeries };
    // Banner only when the proxy is wholly down (every query failed) — partial failures render.
    error = acc.attempted > 0 && acc.failed === acc.attempted ? acc.lastError : '';

    try {
      alerts = (await fetchAlerts()).map(serverAlertToDisplay);
      rulesActive = true;
    } catch {
      rulesActive = false;
      const s = (k: string) => cardSeries[k] ?? [];
      alerts = evaluateAlerts({
        errorRate: latestOf(s('errors')),
        latencyP99: null,
        staleLocatorRate: latestOf(s('stale')),
      });
    }

    try {
      ingestion = await getIngestion();
      ingestionError = '';
    } catch (err) {
      ingestionError = String(err);
    }
  }

  function safeHttpUrl(u: string | undefined): string {
    if (!u) return '';
    try {
      const p = new URL(u);
      return p.protocol === 'http:' || p.protocol === 'https:' ? u : '';
    } catch {
      return '';
    }
  }

  onMount(() => {
    serverConfig()
      .then((c) => (grafanaUrl = safeHttpUrl(c.grafana_url)))
      .catch(() => (grafanaUrl = ''));
    refresh();
    timer = setInterval(refresh, REFRESH_MS);
  });
  onDestroy(() => clearInterval(timer));

  const tabs = $derived(SECTIONS.map((s) => ({ id: s.id, label: s.label })));
  const activeSection = $derived(SECTIONS.find((s) => s.id === active)!);
</script>

<svelte:window onkeydown={(e) => e.key === 'Escape' && detailCard && (detailCard = null)} />

<section aria-labelledby="screen-heading">
  <h1 id="screen-heading" class="sr-only">{t('observability.title')}</h1>

  <div class="screen-toolbar">
    <p class="sub">{t('obs.subline')}</p>
    <div class="actions">
      {#if grafanaUrl}
        <a class="grafana-link" href={grafanaUrl} target="_blank" rel="noreferrer noopener">
          {t('obs.openGrafana')} ↗
        </a>
      {/if}
    </div>
  </div>

  {#if error}
    <p role="alert" class="error">{t('obs.metricsError')}: {error}</p>
  {/if}

  <!-- Alerts strip: persistent across every section — the "is anything wrong right now" answer. -->
  <div class="alerts-strip" class:has-alerts={alerts.length > 0} role="status" aria-live="polite">
    <div class="alerts-strip-head">
      <span class="eyebrow">{t('obs.alerts')}</span>
      {#if rulesActive}
        <Badge tone="ok">{t('obs.rulesActive')}</Badge>
      {:else}
        <Badge tone="planned">{t('obs.rulesFallback')}</Badge>
      {/if}
    </div>
    {#if alerts.length === 0}
      <Badge tone="ok">{t('obs.noAlerts')}</Badge>
    {:else}
      <ul class="alert-list">
        {#each alerts as a (a.name)}
          <li class="alert-row" class:critical={a.level === 'critical'}>
            <span class="alert-dot" aria-hidden="true"></span>
            <span class="alert-text">
              <span class="alert-title">{a.name}</span>
              <span class="alert-detail">{a.detail}</span>
            </span>
            <span class="alert-level">
              {a.level === 'critical' ? t('obs.levelCritical') : t('obs.levelWarning')}
            </span>
          </li>
        {/each}
      </ul>
    {/if}
  </div>

  <div class="tabbar">
    <Tabs {tabs} bind:active />
  </div>

  {#if activeHero}
    <div class="hero">
      <div class="hero-head">
        <span class="hero-title">{activeHero.label}</span>
        <InfoDot title={activeHero.label} body={activeHero.help} />
      </div>
      <EChart option={heroOption(activeHero)} ariaLabel={activeHero.label} height={240} />
    </div>
  {/if}

  <div class="metrics">
    {#each activeSection.cards as card (card.key)}
      {@const d = display(card)}
      {@const tn = toneOf(card)}
      <MetricCard label={card.label} value={d.value} unit={d.unit} sub={sub(card)} tone={tn}>
        {#snippet info()}
          <div class="card-actions">
            {#if !card.planned}
              <button
                class="dc-expand"
                type="button"
                aria-label={`Expand ${card.label}`}
                onclick={(e) => {
                  e.stopPropagation();
                  detailCard = card;
                }}
              >
                <svg width="13" height="13" viewBox="0 0 16 16" fill="none" aria-hidden="true">
                  <path
                    d="M6 2.7H3.3V5.4M10 2.7h2.7V5.4M6 13.3H3.3V10.6M10 13.3h2.7V10.6"
                    stroke="currentColor"
                    stroke-width="1.3"
                    stroke-linecap="round"
                    stroke-linejoin="round"
                  />
                </svg>
              </button>
            {/if}
            <InfoDot title={card.label} body={card.help} hint={card.hint ?? ''} />
          </div>
        {/snippet}
        {#snippet spark()}
          {#if card.planned}
            <Badge tone="planned">Coming soon</Badge>
          {:else if card.external && !hasData(card)}
            <Badge tone="planned">Needs metrics stack</Badge>
          {:else}
            <Sparkline
              points={points(card)}
              times={times(card)}
              format={cardFormat(card)}
              color={TONE_COLOR[tn]}
            />
          {/if}
        {/snippet}
      </MetricCard>
    {/each}
  </div>

  {#if active === 'ingestion'}
    <!-- Folded in from the old Ingestion tab: per-index source binding + per-shard sync detail. -->
    <div class="gb-panel drill">
      <div class="gb-panel-head">Per-index ingestion</div>
      {#if ingestionError}
        <p role="alert" class="error drill-empty">{t('ingestion.error')}: {ingestionError}</p>
      {:else if ingestion.length === 0}
        <p class="muted drill-empty">{t('ingestion.empty')}</p>
      {:else}
        <ul class="idx-list">
          {#each ingestion as idx (idx.name)}
            {@const state = worstState(idx.shards)}
            {@const lvl = badgeLevel(state)}
            {@const lag = worstLagMs(idx.shards)}
            <!-- A windowed index is sharded by time window, not ordinal: its rows carry a
                 window id, so label + render them as windows. -->
            {@const windowed = idx.shards.some((s) => s.window > 0)}
            <li class="idx">
              <button
                class="idx-row"
                aria-expanded={expanded === idx.name}
                onclick={() => (expanded = expanded === idx.name ? null : idx.name)}
              >
                <span class="idx-name mono">{idx.name}</span>
                <span class="idx-source mono">{idx.source_table}</span>
                <span class="idx-shards">{idx.shard_count} {windowed ? 'win' : 'shd'}</span>
                <span class="idx-state"
                  ><span class="badge {lvl}">{state.replace('_', ' ')}</span></span
                >
                <span class="idx-lag">{lag > 0 ? `behind ${formatDuration(lag)}` : ''}</span>
              </button>
              {#if expanded === idx.name}
                <div class="idx-detail">
                  <div class="idx-meta">
                    <span>source snapshot <b class="mono">{idx.source_snapshot_id ?? '—'}</b></span>
                    <span>committed {formatAge(idx.source_timestamp_ms, now)}</span>
                  </div>
                  <table class="shard-tbl">
                    <thead>
                      <tr
                        ><th>{windowed ? 'Window' : 'Shard'}</th><th>Node</th><th>Committed</th><th
                          >State</th
                        ><th>Lag</th></tr
                      >
                    </thead>
                    <tbody>
                      {#each idx.shards as sh (sh.window || sh.ordinal)}
                        <tr>
                          <td class="mono">{windowed ? `w${sh.window}` : sh.ordinal}</td>
                          <td class="mono">{sh.node || '—'}</td>
                          <td class="mono">{sh.committed_snapshot_id || '—'}</td>
                          <td><span class="badge {badgeLevel(sh.state)}">{sh.state}</span></td>
                          <td class="mono">{sh.lag_ms > 0 ? formatDuration(sh.lag_ms) : '0'}</td>
                        </tr>
                      {/each}
                    </tbody>
                  </table>
                </div>
              {/if}
            </li>
          {/each}
        </ul>
      {/if}
    </div>
  {/if}
</section>

{#if detailCard}
  <div class="detail-backdrop" role="presentation" onclick={() => (detailCard = null)}></div>
  <div class="detail-modal" role="dialog" aria-modal="true" aria-label={detailCard.label}>
    <div class="detail-head">
      <div class="detail-titles">
        <h2>{detailCard.label}</h2>
        <p class="detail-help">{detailCard.help}</p>
      </div>
      <button
        class="detail-close"
        onclick={() => (detailCard = null)}
        aria-label={t('common.close')}>×</button
      >
    </div>
    <EChart option={cardDetailOption(detailCard)} height={380} ariaLabel={detailCard.label} />
  </div>
{/if}

<style>
  .grafana-link {
    display: inline-flex;
    align-items: center;
    height: 32px;
    padding: 0 12px;
    border: 1px solid var(--line-strong);
    border-radius: 7px;
    background: var(--field);
    color: var(--text);
    font-weight: 600;
    font-size: 0.9em;
    text-decoration: none;
    white-space: nowrap;
  }
  .grafana-link:hover {
    border-color: var(--accent);
    color: var(--accent);
  }

  .alerts-strip {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.5rem 0.9rem;
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 9px;
    padding: 9px 13px;
    margin-bottom: 0.9rem;
  }
  .alerts-strip.has-alerts {
    border-color: var(--warn);
  }
  .alerts-strip-head {
    display: inline-flex;
    align-items: center;
    gap: 0.5rem;
  }
  .eyebrow {
    font-family: 'Geist Mono', monospace;
    font-size: 0.62rem;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--text-3);
  }
  .alert-list {
    list-style: none;
    margin: 0;
    padding: 0;
    width: 100%;
    display: flex;
    flex-direction: column;
    gap: 0.4rem;
  }
  .alert-row {
    display: flex;
    align-items: center;
    gap: 0.7rem;
    padding: 0.5rem 0.7rem;
    border: 1px solid var(--line);
    border-left: 3px solid var(--warn);
    border-radius: 7px;
    background: var(--panel);
  }
  .alert-row.critical {
    border-left-color: var(--danger);
  }
  .alert-dot {
    flex-shrink: 0;
    width: 9px;
    height: 9px;
    border-radius: 50%;
    background: var(--warn);
  }
  .alert-row.critical .alert-dot {
    background: var(--danger);
  }
  .alert-text {
    display: flex;
    flex-direction: column;
    gap: 1px;
    min-width: 0;
    flex: 1;
  }
  .alert-title {
    font-weight: 600;
    color: var(--text);
  }
  .alert-detail {
    font-size: 0.85em;
    color: var(--text-2);
  }
  .alert-level {
    flex-shrink: 0;
    font-size: 0.72em;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    font-weight: 600;
    color: var(--warn);
  }
  .alert-row.critical .alert-level {
    color: var(--danger);
  }

  .tabbar {
    margin-bottom: 1rem;
  }

  .hero {
    background: var(--panel);
    border: 1px solid var(--line);
    border-radius: 9px;
    padding: 13px 15px 8px;
    margin-bottom: 1rem;
  }
  .hero-head {
    display: flex;
    align-items: center;
    gap: 0.3rem;
    margin-bottom: 4px;
  }
  .hero-title {
    font-weight: 600;
    font-size: 0.92rem;
    color: var(--text);
  }

  .metrics {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(210px, 1fr));
    gap: 0.85rem;
  }

  .card-actions {
    display: inline-flex;
    align-items: center;
    gap: 1px;
  }
  .dc-expand {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    width: 18px;
    height: 18px;
    padding: 0;
    border: 0;
    border-radius: 4px;
    background: transparent;
    color: var(--text-3);
    cursor: pointer;
  }
  .dc-expand:hover {
    color: var(--accent);
    background: var(--accent-weakest);
  }

  .detail-backdrop {
    position: fixed;
    inset: 0;
    z-index: 50;
    background: var(--scrim);
  }
  .detail-modal {
    position: fixed;
    z-index: 51;
    top: 50%;
    left: 50%;
    transform: translate(-50%, -50%);
    width: min(780px, 92vw);
    background: var(--panel);
    border: 1px solid var(--line-strong);
    border-radius: 12px;
    box-shadow: 0 20px 60px var(--shadow);
    padding: 16px 18px 18px;
  }
  .detail-head {
    display: flex;
    align-items: flex-start;
    justify-content: space-between;
    gap: 1rem;
    margin-bottom: 10px;
  }
  .detail-titles h2 {
    margin: 0;
    font-size: 1.05rem;
  }
  .detail-help {
    margin: 4px 0 0;
    font-size: 0.85rem;
    color: var(--text-2);
    line-height: 1.45;
    max-width: 62ch;
  }
  .detail-close {
    flex: 0 0 auto;
    border: 0;
    background: transparent;
    color: var(--text-3);
    font-size: 1.5rem;
    line-height: 1;
    cursor: pointer;
    padding: 0 4px;
  }
  .detail-close:hover {
    color: var(--text);
  }

  .drill {
    margin-top: 1.25rem;
  }
  .drill-empty {
    padding: 0.5rem 0;
  }
  .idx-list {
    list-style: none;
    margin: 0;
    padding: 0;
  }
  .idx {
    border-top: 1px solid var(--line);
  }
  .idx:first-child {
    border-top: 0;
  }
  .idx-row {
    display: grid;
    grid-template-columns: minmax(110px, 1.3fr) minmax(140px, 1.9fr) 56px 104px 104px;
    align-items: center;
    gap: 0.6rem;
    width: 100%;
    text-align: left;
    background: transparent;
    border: 0;
    color: var(--text);
    font: inherit;
    padding: 0.55rem 0.2rem;
    cursor: pointer;
  }
  .idx-row:hover {
    background: var(--accent-weakest);
  }
  .idx-source {
    color: var(--text-2);
    font-size: 0.85em;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }
  .idx-shards {
    color: var(--text-3);
    font-size: 0.85em;
    text-align: right;
  }
  /* Pill in its own fixed column so every row's status pill starts at the same x. */
  .idx-state {
    justify-self: start;
  }
  .idx-lag {
    justify-self: end;
    text-align: right;
    color: var(--text-3);
    font-size: 0.82em;
  }
  .idx-detail {
    padding: 0.4rem 0.2rem 0.9rem;
  }
  .idx-meta {
    display: flex;
    gap: 1.2rem;
    color: var(--text-3);
    font-size: 0.82em;
    margin-bottom: 0.5rem;
  }
  .shard-tbl {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.85em;
  }
  .shard-tbl th {
    text-align: left;
    color: var(--text-3);
    font-weight: 500;
    font-size: 0.85em;
    padding: 3px 8px;
    border-bottom: 1px solid var(--line);
  }
  .shard-tbl td {
    padding: 4px 8px;
    border-bottom: 1px solid var(--line);
    color: var(--text-2);
  }
</style>
