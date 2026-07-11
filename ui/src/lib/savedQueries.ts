// Saved searches: server-persisted per-user when authenticated, with a per-browser localStorage
// fallback for an open/anonymous gateway. Each saved search captures the full search state (query,
// syntax, index, sort, filters, time range) so restoring re-applies all of it — not just the raw
// query string.
import { isAuthenticated } from './auth';
import { listSavedQueries, saveSavedQuery, deleteSavedQuery, type SavedQueryRow } from './api';

/** The full Search state a saved search restores. */
export interface SavedState {
  query: string;
  syntax: string;
  index?: string;
  sort?: string;
  filters?: { field: string; value: string }[];
  timeField?: string;
  timePreset?: string;
  timeFrom?: string;
  timeTo?: string;
}

/** A saved search as the UI handles it. `id` is set for server-backed rows. */
export interface SavedSearch {
  id?: string;
  name: string;
  query: string;
  state: SavedState;
  shared?: boolean;
}

const KEY = 'growlerdb.saved_queries';
const MAX = 50;

function fromRow(r: SavedQueryRow): SavedSearch {
  let state: SavedState = { query: r.query, syntax: 'lucene' };
  if (r.state) {
    try {
      state = JSON.parse(r.state) as SavedState;
    } catch {
      /* keep the fallback */
    }
  }
  return { id: r.id, name: r.name, query: r.query, state, shared: r.shared };
}

function toRow(s: SavedSearch): Partial<SavedQueryRow> {
  return {
    id: s.id,
    name: s.name,
    query: s.query,
    state: JSON.stringify(s.state),
    shared: s.shared,
  };
}

// ---- localStorage fallback (anonymous gateway) ---------------------------------

function loadLocal(): SavedSearch[] {
  try {
    const raw = localStorage.getItem(KEY);
    const list = raw ? (JSON.parse(raw) as unknown) : [];
    if (!Array.isArray(list)) return [];
    // Migrate the legacy `string[]` format (raw queries) to full objects.
    return (list as unknown[]).map((x) =>
      typeof x === 'string'
        ? { name: x, query: x, state: { query: x, syntax: 'lucene' } }
        : (x as SavedSearch),
    );
  } catch {
    return [];
  }
}

function writeLocal(list: SavedSearch[]): void {
  localStorage.setItem(KEY, JSON.stringify(list.slice(0, MAX)));
}

// ---- public API (chooses server vs local by auth state) ------------------------

export async function loadSavedSearches(): Promise<SavedSearch[]> {
  if (isAuthenticated()) return (await listSavedQueries()).map(fromRow);
  return loadLocal();
}

export async function saveSearch(s: SavedSearch): Promise<SavedSearch[]> {
  if (isAuthenticated()) {
    await saveSavedQuery(toRow(s));
    return loadSavedSearches();
  }
  // De-dupe by name, newest first.
  writeLocal([s, ...loadLocal().filter((x) => x.name !== s.name)]);
  return loadLocal();
}

export async function removeSearch(s: SavedSearch): Promise<SavedSearch[]> {
  if (isAuthenticated()) {
    if (s.id) await deleteSavedQuery(s.id);
    return loadSavedSearches();
  }
  writeLocal(loadLocal().filter((x) => x.name !== s.name));
  return loadLocal();
}
