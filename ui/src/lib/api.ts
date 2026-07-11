// The Engine API client. The UI is a pure client of the same gRPC/REST API programmatic callers
// use — it never reaches the Index or storage directly. Every request carries the verified bearer
// token, which the Engine gateway validates. The base URL is empty by default — the SPA is served
// *by* the Engine, so the API is same-origin.
import { getToken, clearToken, isTokenExpired } from './auth';

/** Fired when a request we authenticated is rejected 401 (expired/revoked token) — App re-gates. */
export const UNAUTHORIZED_EVENT = 'growlerdb:unauthorized';

const BASE: string = (import.meta.env.VITE_ENGINE_API as string | undefined) ?? '';

/** Authenticated fetch against the Engine API. Method defaults to GET (no body) / POST (body);
 *  pass `method` to override (e.g. DELETE). */
export async function apiFetch(path: string, body?: unknown, method?: string): Promise<Response> {
  const headers: Record<string, string> = {};
  let token = getToken();
  // Proactively drop an expired token: fire the re-gate and go unauthenticated instead of sending
  // a request the gateway would 401 anyway.
  if (token && isTokenExpired(token)) {
    clearToken();
    if (typeof window !== 'undefined') window.dispatchEvent(new Event(UNAUTHORIZED_EVENT));
    token = null;
  }
  if (token) headers['authorization'] = `Bearer ${token}`;
  const init: RequestInit = {
    method: method ?? (body === undefined ? 'GET' : 'POST'),
    headers,
  };
  if (body !== undefined) {
    headers['content-type'] = 'application/json';
    init.body = JSON.stringify(body);
  }
  const res = await fetch(`${BASE}${path}`, init);
  // A 401 on a request we *did* authenticate means the token expired or was revoked. Drop the dead
  // token and signal the app to re-gate — closed mode shows the login screen again; open mode never
  // had a token, so it's unaffected.
  if (res.status === 401 && token) {
    clearToken();
    if (typeof window !== 'undefined') window.dispatchEvent(new Event(UNAUTHORIZED_EVENT));
  }
  return res;
}

export interface KeyField {
  name: string;
  value: unknown;
}

/** A document's composite coordinate (partition + identifier fields). */
export interface Coordinates {
  partition?: KeyField[];
  identifier?: KeyField[];
}

/** One XSS-safe highlight segment: a run of text and whether it is a matched term.
 *  Mirrors {@link import('./highlight').Segment} so server + client-side highlights render alike. */
export interface HighlightSegment {
  text: string;
  marked: boolean;
}

/** Server-side highlights for a hit: field name → fragments → segment runs. Present only when the
 *  search opted in (`highlight` in the request) and a field actually matched. */
export type HitHighlight = Record<string, HighlightSegment[][]>;

export interface SearchHit {
  coordinates?: Coordinates;
  score?: number;
  /** Cached display fields returned with the hit, so a results page renders document-like rows
   *  without hydration. Absent when the index caches no display fields. */
  fields?: Record<string, unknown>;
  /** Server-side highlights: field → matched fragments (each a list of `{text, marked}` segments),
   *  reflecting the analyzed match. Absent unless the search requested highlighting. */
  highlight?: HitHighlight;
}

export interface SearchResponse {
  hits: SearchHit[];
  total: number;
  /** Set by the Gateway when a shard failed to respond, so hits/total under-count.
   *  Absent on a complete result, so a missing flag is trustworthy. */
  partial?: boolean;
  /** Shards the Gateway queried vs the index's total: a time/window filter prunes
   *  shards it can prove won't match. Both absent from a bare Node (no shard scope). */
  shards_scanned?: number;
  shards_total?: number;
  /** Opaque keyset cursor for the next page: present only on a sorted, full page.
   *  Pass it back as `cursor` to scroll. Absent for score-ranked or short pages. */
  next_cursor?: string;
}

/** The query grammar the `query` string is parsed with. */
export type QuerySyntax = 'lucene' | 'kql';

/** A sort key: order hits by `field` (descending by default). */
export interface SortKey {
  field: string;
  desc?: boolean;
}

/** Options for {@link search}. All optional; an omitted field uses the engine default. */
export interface SearchOptions {
  limit?: number;
  offset?: number;
  syntax?: QuerySyntax;
  /** Scope to a named index; the gateway 404s a name it doesn't serve. */
  index?: string;
  /** Sort keys in priority order; empty/omitted = rank by score. */
  sort?: SortKey[];
  /** `search_after` keyset cursor from a prior response's `next_cursor` (deep paging). */
  cursor?: string;
  /** Opt into server-side highlighting. `true` = default highlightable TEXT fields;
   *  an object names fields and/or bounds. Omitted = no highlights (the client-side marker is used). */
  highlight?: boolean | { fields?: string[]; max_fragments?: number; fragment_size?: number };
}

/** Run a query through the Engine `/v1/search` endpoint. Supports per-index scoping, field sort,
 *  offset paging, and `search_after` keyset scrolling. */
export async function search(query: string, opts: SearchOptions = {}): Promise<SearchResponse> {
  const body: Record<string, unknown> = {
    query,
    limit: opts.limit ?? 10,
    syntax: opts.syntax ?? 'lucene',
  };
  if (opts.offset) body.offset = opts.offset;
  if (opts.index) body.index = opts.index;
  if (opts.sort && opts.sort.length > 0) {
    body.sort = opts.sort.map((s) => ({ field: s.field, desc: s.desc ?? true }));
  }
  if (opts.cursor) body.search_after = opts.cursor;
  // Opt into server-side highlighting: `true` sends an empty object (default fields);
  // an object passes fields/bounds through. Omitted ⇒ no `highlight` key (highlighting off).
  if (opts.highlight === true) body.highlight = {};
  else if (opts.highlight && typeof opts.highlight === 'object') body.highlight = opts.highlight;
  const res = await apiFetch('/v1/search', body);
  if (!res.ok) throw new Error(`search failed (${res.status})`);
  return res.json();
}

/** One facet value and how many matching docs carry it. */
export interface FacetBucket {
  value: string;
  count: number;
}

/** A field's top facet values (terms aggregation). */
export interface FacetGroup {
  field: string;
  buckets: FacetBucket[];
}

export interface FacetsResponse {
  facets: FacetGroup[];
  /** A shard failed, so counts under-count. */
  partial?: boolean;
}

/** Compute left-rail facets for a query via `/v1/facets`: a top-N terms aggregation per
 *  field, reusing the engine's distributed Aggregate path. Fields that aren't aggregatable are
 *  skipped server-side. Returns `{ facets: [] }` on any failure — facets are a best-effort refinement
 *  and must never break the results view. */
export async function facets(query: string, fields: string[], size = 10): Promise<FacetsResponse> {
  try {
    const res = await apiFetch('/v1/facets', { query, fields, size });
    if (!res.ok) return { facets: [] };
    return (await res.json()) as FacetsResponse;
  } catch {
    return { facets: [] };
  }
}

/** One node of the BM25 score-explanation tree. */
export interface ExplainClause {
  description: string;
  score: number;
  details?: ExplainClause[];
}

/** A real query explanation for one document: BM25 tree, analyzed terms, timings. */
export interface ExplainResult {
  found: boolean;
  matched: boolean;
  score: number;
  detail?: ExplainClause;
  analyzed: { field: string; terms: string[] }[];
  timings: { index_ms: number; hydration_ms: number; total_ms: number };
  shards_scanned: number;
  shards_total: number;
}

/** Explain how `query` scores one document via `/v1/explain`. Opt-in, per-hit. */
export async function explain(
  query: string,
  coordinates: Coordinates,
  syntax: QuerySyntax = 'lucene',
  index?: string,
): Promise<ExplainResult> {
  const res = await apiFetch('/v1/explain', { query, coordinates, syntax, index });
  if (!res.ok) throw new Error(`explain failed (${res.status})`);
  return res.json();
}

/** Unauthenticated runtime config: tells the console whether to gate the app behind a
 *  login screen. Available before sign-in (unlike `/v1/me`, which 401s for anonymous on a closed
 *  gateway). */
export interface ServerConfig {
  auth_required: boolean;
  /** Built-in username/password login is available — show a login form, not just OIDC. */
  password_login?: boolean;
  /** This deployment's Grafana base URL, served at runtime so the link points at the
   *  actual deployment. Absent when unset — the console then hides the "Open Grafana" link. */
  grafana_url?: string;
}

/** A built-in login result: the session JWT to send as `Authorization: Bearer`. */
export interface LoginResult {
  token: string;
  expires_at_ms: number;
  roles: string[];
}

/** Built-in credential login: exchange a username/password for a session token via
 *  `POST /v1/login`. Throws a friendly message on bad credentials (401) or other failure. */
export async function passwordLogin(username: string, password: string): Promise<LoginResult> {
  const res = await apiFetch('/v1/login', { username, password });
  if (!res.ok) {
    throw new Error(
      res.status === 401 ? 'Invalid username or password' : `Login failed (${res.status})`,
    );
  }
  return (await res.json()) as LoginResult;
}

/** Fetch `GET /v1/config`. Defaults to **open** (un-gated) if the request fails, so a transient
 *  error can't lock anyone out — the API still enforces auth on each call regardless. */
export async function serverConfig(): Promise<ServerConfig> {
  try {
    const res = await apiFetch('/v1/config');
    if (res.ok) return (await res.json()) as ServerConfig;
  } catch {
    // fall through to the open default
  }
  return { auth_required: false };
}

/** The verified caller identity from `GET /v1/me`. */
export interface Me {
  authenticated: boolean;
  subject: string;
  display_name?: string;
  email?: string;
  tenant?: string;
  roles: string[];
}

/** Fetch the current identity from the gateway (verified server-side). Returns `null` when the
 *  request is rejected (401 on a configured gateway with no/expired token) — i.e. not signed in. */
export async function me(): Promise<Me | null> {
  try {
    const res = await apiFetch('/v1/me');
    if (!res.ok) return null;
    return (await res.json()) as Me;
  } catch {
    return null;
  }
}

/** API-token metadata — never the secret or its hash. */
export interface ApiTokenMeta {
  id: string;
  label: string;
  prefix: string;
  roles: string[];
  owner?: string;
  created_at_ms: number;
}

/** A freshly created token: metadata + the raw secret, shown once. */
export interface CreatedToken {
  token: ApiTokenMeta;
  secret: string;
}

/** List API tokens (metadata only) via `/v1/tokens`. Admin-gated. */
export async function listTokens(): Promise<ApiTokenMeta[]> {
  const res = await apiFetch('/v1/tokens');
  if (!res.ok) throw new Error(`list tokens failed (${res.status})`);
  return ((await res.json()) as { tokens?: ApiTokenMeta[] }).tokens ?? [];
}

/** Issue an API token; the `secret` is returned exactly once. */
export async function createToken(label: string, roles: string[]): Promise<CreatedToken> {
  const res = await apiFetch('/v1/tokens', { label, roles });
  if (!res.ok) throw new Error(`create token failed (${res.status})`);
  return res.json();
}

/** Revoke an API token by id. */
export async function revokeToken(id: string): Promise<void> {
  const res = await apiFetch(`/v1/tokens/${encodeURIComponent(id)}`, undefined, 'DELETE');
  if (!res.ok && res.status !== 204) throw new Error(`revoke token failed (${res.status})`);
}

/** A local role binding: an admin-granted set of roles for a subject. */
export interface RoleBinding {
  subject: string;
  roles: string[];
}

/** List local role bindings via `/v1/users`. Admin-gated server-side. */
export async function listUsers(): Promise<RoleBinding[]> {
  const res = await apiFetch('/v1/users');
  if (!res.ok) throw new Error(`list users failed (${res.status})`);
  return ((await res.json()) as { users?: RoleBinding[] }).users ?? [];
}

/** The assignable role catalog via `/v1/roles`. */
export async function listRoles(): Promise<string[]> {
  const res = await apiFetch('/v1/roles');
  if (!res.ok) return [];
  return ((await res.json()) as { roles?: string[] }).roles ?? [];
}

/** Replace a subject's local roles (empty clears the binding) via `PUT /v1/users/{subject}/roles`. */
export async function setUserRoles(subject: string, roles: string[]): Promise<RoleBinding> {
  const res = await apiFetch(`/v1/users/${encodeURIComponent(subject)}/roles`, { roles }, 'PUT');
  if (!res.ok) throw new Error(`set roles failed (${res.status})`);
  return res.json();
}

/** A server-persisted saved search. `state` is an opaque JSON blob the UI round-trips. */
export interface SavedQueryRow {
  id: string;
  name: string;
  owner?: string;
  query: string;
  state?: string;
  shared?: boolean;
  created_at_ms?: number;
}

/** List the caller's saved searches (own + shared) via `/v1/saved-queries`. */
export async function listSavedQueries(): Promise<SavedQueryRow[]> {
  const res = await apiFetch('/v1/saved-queries');
  if (!res.ok) throw new Error(`list saved queries failed (${res.status})`);
  return ((await res.json()) as { queries?: SavedQueryRow[] }).queries ?? [];
}

/** Create (no id) or update (with id) a saved search; returns the stored row. */
export async function saveSavedQuery(row: Partial<SavedQueryRow>): Promise<SavedQueryRow> {
  const path = row.id ? `/v1/saved-queries/${encodeURIComponent(row.id)}` : '/v1/saved-queries';
  const res = await apiFetch(path, row, row.id ? 'PUT' : 'POST');
  if (!res.ok) throw new Error(`save query failed (${res.status})`);
  return res.json();
}

/** Delete a saved search by id. */
export async function deleteSavedQuery(id: string): Promise<void> {
  const res = await apiFetch(`/v1/saved-queries/${encodeURIComponent(id)}`, undefined, 'DELETE');
  if (!res.ok && res.status !== 204) throw new Error(`delete query failed (${res.status})`);
}

export interface Suggestion {
  text: string;
  count: number;
}

/** Term suggestions for a field via `/v1/suggest`, used for query autocomplete.
 *  Returns `[]` on any failure rather than throwing — suggest is best-effort and is fail-closed on
 *  tenant-scoped indexes (403), so a missing dropdown must never break typing. `index` scopes the
 *  suggestion to a named index on a multi-index endpoint; empty = the default index. */
export async function suggest(
  field: string,
  text: string,
  limit = 8,
  index?: string,
): Promise<Suggestion[]> {
  try {
    const body: Record<string, unknown> = { field, text, limit };
    if (index) body.index = index;
    const res = await apiFetch('/v1/suggest', body);
    if (!res.ok) return [];
    return ((await res.json()) as { suggestions?: Suggestion[] }).suggestions ?? [];
  } catch {
    return [];
  }
}

/** A hydrated row: its coordinate + the authoritative field values from Iceberg. */
export interface Row {
  key: Coordinates;
  fields: Record<string, unknown>;
}

/** Hydrate authoritative rows by coordinate via `/v1/keys:get`. Row/column governance is
 *  enforced by the Engine on the Iceberg read, so this can only return what the
 *  caller is allowed to see. */
export async function getByKey(
  keys: Coordinates[],
  columns: string[] = [],
  index?: string,
): Promise<Row[]> {
  const res = await apiFetch('/v1/keys:get', { index, keys, columns });
  if (!res.ok) throw new Error(`hydrate failed (${res.status})`);
  const body = (await res.json()) as { rows: Row[] };
  return body.rows ?? [];
}

/** A hit's identifier as a display string (joins multi-field ids). */
export function hitId(hit: SearchHit): string {
  const ids = hit.coordinates?.identifier ?? [];
  return ids.map((f) => String(f.value)).join(' / ') || '?';
}

// ---- index management (control-plane REST) ------------------------------

export interface IndexSummary {
  name: string;
  status: string;
}

/** One per-index activity event. */
export interface ActivityEvent {
  ts_ms: number;
  kind: string;
  message: string;
}

/** The index's activity log (newest-first) via `/v1/index:activity`. Best-effort: `[]`. */
export async function listActivity(name: string, limit = 50): Promise<ActivityEvent[]> {
  try {
    const res = await apiFetch('/v1/index:activity', { index: name, limit });
    if (!res.ok) return [];
    return ((await res.json()) as { events?: ActivityEvent[] }).events ?? [];
  } catch {
    return [];
  }
}

/** Result of a compaction: live segment count before/after the merge. */
export interface CompactResult {
  segments_before: number;
  segments_after: number;
}
/** Result of a backup run. */
export interface BackupResult {
  snapshot: number;
  file_count: number;
  created_ms: number;
  prefix: string;
}
/** Last-backup status. `configured` = the node has a backup target. */
export interface BackupStatus {
  configured: boolean;
  present: boolean;
  snapshot?: number;
  created_ms?: number;
  file_count?: number;
}

/** Compact an index's segments via `/v1/index:compact`. */
export async function compactIndex(name: string): Promise<CompactResult> {
  const res = await apiFetch('/v1/index:compact', { index: name });
  if (!res.ok) throw new Error(`compact failed (${res.status})`);
  return res.json();
}

/** Run a backup via `/v1/index:backup`. Surfaces the server reason (incl. 501 not-configured). */
export async function backupIndex(name: string): Promise<BackupResult> {
  const res = await apiFetch('/v1/index:backup', { index: name });
  if (!res.ok) {
    const body = await res.text();
    let reason = body;
    try {
      reason = (JSON.parse(body) as { message?: string }).message ?? body;
    } catch {
      /* keep raw */
    }
    throw new Error(reason || `backup failed (${res.status})`);
  }
  return res.json();
}

/** Read last-backup status via `/v1/index:backup-status`. */
export async function backupStatus(name: string): Promise<BackupStatus> {
  const res = await apiFetch('/v1/index:backup-status', { index: name });
  if (!res.ok) return { configured: false, present: false };
  return res.json();
}

/** One field's resolved mapping. `blocked` is the reason the field can't be cached. */
export interface FieldMapping {
  path: string;
  type: string;
  analyzer?: string;
  fast: boolean;
  cached: boolean;
  pk: boolean;
  blocked?: string;
}

/** One shard's placement + state for the detail Shards tab. */
export interface ShardStatus {
  ordinal: number;
  window?: number;
  primary?: string;
  replicas?: string[];
  state: string;
}

export interface IndexInfo {
  name: string;
  status: string;
  shard_count: number;
  routing: string;
  /** Per-field mapping for the detail Mapping tab. */
  fields?: FieldMapping[];
  /** Per-shard placement for the detail Shards tab. */
  shards?: ShardStatus[];
}

export interface IndexStats {
  name: string;
  snapshot: number;
  num_docs: number;
  generation_count: number;
  checkpoint: string;
  /** Mapped DATE columns — candidates for the search time filter. Absent when none. */
  time_fields?: string[];
}

export interface SourceField {
  path: string;
  type: string;
}

export interface SourceSchema {
  fields: SourceField[];
  partition_fields: string[];
  identifier_fields: string[];
}

export async function listIndexes(): Promise<IndexSummary[]> {
  const res = await apiFetch('/v1/indexes');
  if (!res.ok) throw new Error(`list indexes failed (${res.status})`);
  return ((await res.json()) as { indexes: IndexSummary[] }).indexes ?? [];
}

export async function getIndex(name: string): Promise<IndexInfo> {
  const res = await apiFetch(`/v1/indexes/${encodeURIComponent(name)}`);
  if (!res.ok) throw new Error(`get index failed (${res.status})`);
  return res.json();
}

/** Per-shard-merged stats (docs, snapshot, checkpoint) — available when the gateway fronts the
 *  index. Returns `null` if the gateway can't describe it (e.g. not served here). */
export async function describeIndex(name: string): Promise<IndexStats | null> {
  const res = await apiFetch('/v1/index:describe', { index: name });
  if (!res.ok) return null;
  return res.json();
}

export async function describeSource(table: string): Promise<SourceSchema> {
  const res = await apiFetch('/v1/source:describe', { table });
  if (!res.ok) {
    const msg = await res.text();
    throw new Error(msg || `describe source failed (${res.status})`);
  }
  return res.json();
}

/** Create an index from a definition YAML. Throws with the server's reason on failure (e.g.
 *  the cached-field hard-block), so the form can surface it inline. */
export async function createIndex(definition: string): Promise<string> {
  const res = await apiFetch('/v1/indexes', { definition });
  if (!res.ok) {
    const body = await res.text();
    let reason = body;
    try {
      reason = (JSON.parse(body) as { message?: string }).message ?? body;
    } catch {
      /* keep raw */
    }
    throw new Error(reason || `create failed (${res.status})`);
  }
  return ((await res.json()) as { name: string }).name;
}

export async function dropIndex(name: string): Promise<void> {
  const res = await apiFetch(`/v1/indexes/${encodeURIComponent(name)}`, undefined, 'DELETE');
  if (!res.ok) throw new Error(`drop failed (${res.status})`);
}

/** Result of a reindex: the rebuilt index's doc count + commit snapshot. */
export interface ReindexResult {
  doc_count: number;
  snapshot: number;
}

/** Rebuild an index from its source and atomically swap it live. The write-fence
 *  lives on the owning Node, so a reindex already running surfaces as 412; a multi-shard gateway
 *  returns 501 (distributed reindex is future work). Throws with the server's reason so the screen
 *  can surface it inline. */
export async function reindexIndex(name: string): Promise<ReindexResult> {
  const res = await apiFetch('/v1/index:reindex', { index: name });
  if (!res.ok) {
    const body = await res.text();
    let reason = body;
    try {
      reason = (JSON.parse(body) as { message?: string }).message ?? body;
    } catch {
      /* keep raw */
    }
    if (res.status === 409 || res.status === 412) {
      reason = reason || 'a reindex is already in progress';
    }
    throw new Error(reason || `reindex failed (${res.status})`);
  }
  return res.json();
}

// ---- aliases / zero-downtime swap -----------------------

/** An alias and the index(es) it points at — a stable name reads route through. */
export interface Alias {
  alias: string;
  targets: string[];
}

export async function listAliases(): Promise<Alias[]> {
  const res = await apiFetch('/v1/aliases');
  if (!res.ok) throw new Error(`list aliases failed (${res.status})`);
  return ((await res.json()) as { aliases: Alias[] }).aliases ?? [];
}

/** Create or **atomically re-point** an alias to `targets` (the zero-downtime swap): one
 *  control-plane write replaces the alias's members, so reads follow with no gap. Throws with the
 *  server's reason (e.g. a name clash with an index, or an unknown target) so the form can show it. */
export async function setAlias(alias: string, targets: string[]): Promise<void> {
  const res = await apiFetch('/v1/aliases', { alias, targets });
  if (!res.ok) {
    const body = await res.text();
    let reason = body;
    try {
      reason = (JSON.parse(body) as { message?: string }).message ?? body;
    } catch {
      /* keep raw */
    }
    throw new Error(reason || `set alias failed (${res.status})`);
  }
}

export async function dropAlias(alias: string): Promise<void> {
  const res = await apiFetch(`/v1/aliases/${encodeURIComponent(alias)}`, undefined, 'DELETE');
  if (!res.ok) throw new Error(`drop alias failed (${res.status})`);
}

// ---- ingestion (sync) status -------------------------------------------
// GrowlerDB has no separate "connector": every index is kept in sync with exactly one Iceberg
// source by changelog ingestion, so "ingestion status" = the source head vs. each shard's
// committed checkpoint.

/** One shard's committed position vs. the source, from its node's Write.GetCheckpoint. */
export interface ShardIngestion {
  ordinal: number;
  node: string;
  /** The source Iceberg snapshot this shard reflects (0 = nothing committed yet). */
  committed_snapshot_id: number;
  index_snapshot: number;
  state: string; // in_sync | behind | uninitialized | no_primary | unreachable | unknown
  /** Wall-clock staleness vs the source head, ms (0 when in_sync); for a "behind by Ns" label. */
  lag_ms: number;
  /** For a windowed index: the time-window id this row represents; 0 for an ordinal shard. */
  window: number;
}

/** An index's ingestion: its source binding + how far each shard is behind the source head. */
export interface IndexIngestion {
  name: string;
  status: string;
  source_table: string;
  routing: string;
  shard_count: number;
  /** The source table's current Iceberg snapshot; `null` when the source is unreadable. */
  source_snapshot_id: number | null;
  /** Commit time of that snapshot (epoch ms); `null` when unreadable/none. */
  source_timestamp_ms: number | null;
  shards: ShardIngestion[];
}

/** Ingestion status for every registered index (control-plane proxy `/v1/ingestion`). */
export async function getIngestion(): Promise<IndexIngestion[]> {
  const res = await apiFetch('/v1/ingestion');
  if (!res.ok) throw new Error(`ingestion status failed (${res.status})`);
  return ((await res.json()) as { items: IndexIngestion[] }).items ?? [];
}

/** One window's storage tier: hot (local) or cold (read-through from object storage). */
export interface WindowTier {
  window: number;
  cold: boolean;
  event_min: number | null;
  event_max: number | null;
}

/** Read-through cache stats for the cold tier. */
export interface ColdCacheStats {
  hits: number;
  misses: number;
  fetched_bytes: number;
  cached_bytes: number;
}

/** Cold-tier status of a windowed index — per-window tier + the shared cache. */
export interface ColdStatus {
  windows: WindowTier[];
  cache: ColdCacheStats | null;
  hot: number;
  cold: number;
}

/** Cold-tier status from `GET /v1/cold`, or `null` when the served index isn't windowed (404). */
export async function getColdStatus(): Promise<ColdStatus | null> {
  const res = await apiFetch('/v1/cold');
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`cold status failed (${res.status})`);
  return (await res.json()) as ColdStatus;
}
