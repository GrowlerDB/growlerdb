// Pure helpers for rendering a search result row: pick a timestamp + a snippet field out of a
// hit's cached display fields. Kept pure + tested so Search.svelte stays markup.

/** Conventionally-named free-text fields, preferred as the snippet source when present. */
const SNIPPET_NAMES = [
  'body',
  'message',
  'text',
  'content',
  'description',
  'summary',
  'log',
  'msg',
];

const MAX_SNIPPET = 200;

/** Below this length an unnamed string is treated as a keyword/id (a chip), not free-text snippet. */
const MIN_SNIPPET = 24;

/** Format an epoch-**micros** value (the canonical unit) as a stable UTC `YYYY-MM-DD HH:MM:SS`
 *  string. Returns null for anything that isn't a finite number in a plausible range. */
export function formatEpochMicros(v: unknown): string | null {
  if (typeof v !== 'number' || !Number.isFinite(v)) return null;
  const ms = Math.round(v / 1000);
  const d = new Date(ms);
  const iso = Number.isNaN(d.getTime()) ? null : d.toISOString();
  return iso ? iso.slice(0, 19).replace('T', ' ') : null;
}

/** Pick the timestamp to render on a result row: the first of the index's DATE columns
 *  (`timeFields`) that this hit caches a value for. Values are canonical epoch micros. */
export function pickTimestamp(
  fields: Record<string, unknown> | undefined,
  timeFields: string[],
): { name: string; display: string } | null {
  if (!fields) return null;
  for (const name of timeFields) {
    const display = formatEpochMicros(fields[name]);
    if (display) return { name, display };
  }
  return null;
}

/** Pick a snippet source from a hit's cached fields: a conventionally-named text field if present,
 *  else the longest string value. `exclude` skips fields already rendered elsewhere (the timestamp).
 *  The value is truncated to a snippet length (the caller highlights query terms in it). */
export function pickSnippet(
  fields: Record<string, unknown> | undefined,
  exclude: ReadonlySet<string> = new Set(),
): { name: string; value: string } | null {
  if (!fields) return null;
  const strings = Object.entries(fields).filter(
    (e): e is [string, string] =>
      typeof e[1] === 'string' && e[1].trim().length > 0 && !exclude.has(e[0]),
  );
  if (strings.length === 0) return null;
  // A conventionally-named text field wins outright; otherwise fall back to the longest value, but
  // only if it's plausibly free text — a short keyword/id stays a chip rather than a fake snippet.
  const named = strings.find(([n]) => SNIPPET_NAMES.includes(n.toLowerCase()));
  const chosen = named ?? strings.reduce((a, b) => (b[1].length > a[1].length ? b : a));
  if (!named && chosen[1].length < MIN_SNIPPET) return null;
  return { name: chosen[0], value: truncate(chosen[1], MAX_SNIPPET) };
}

/** Truncate to `max` chars on a word boundary where possible, appending an ellipsis. */
export function truncate(text: string, max = MAX_SNIPPET): string {
  if (text.length <= max) return text;
  const cut = text.slice(0, max);
  const lastSpace = cut.lastIndexOf(' ');
  return (lastSpace > max * 0.6 ? cut.slice(0, lastSpace) : cut).trimEnd() + '…';
}
