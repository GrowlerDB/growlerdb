// Client-side highlighting (task-46). The Engine's wire response carries no server-side
// highlights yet, so the UI marks the query's terms in hydrated text — a best-effort visual
// aid. Returns *segments* (not HTML) so components render with <mark> safely: no innerHTML,
// no XSS.

const OPERATORS = new Set(['AND', 'OR', 'NOT', 'TO']);

/** Extract highlightable terms from a Lucene/KQL query: drop `field:` prefixes, boolean
 *  operators, quotes, leading `-`/parens, and wildcard/fuzzy/boost suffixes. Lowercased,
 *  de-duplicated. Best-effort — this is a display convenience, not a parser. */
export function queryTerms(query: string): string[] {
  const terms: string[] = [];
  const push = (raw: string) => {
    const term = raw.trim().toLowerCase();
    if (!term || OPERATORS.has(term.toUpperCase())) return;
    if (!terms.includes(term)) terms.push(term);
  };
  const re = /"([^"]+)"|(\S+)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(query)) !== null) {
    if (m[1] !== undefined) {
      for (const w of m[1].split(/\s+/)) push(w);
      continue;
    }
    let tok = m[2];
    const colon = tok.indexOf(':');
    if (colon >= 0) tok = tok.slice(colon + 1);
    tok = tok
      .replace(/^[-([{]+/, '')
      .replace(/[)\]}]+$/, '')
      .replace(/[*?~^].*$/, '');
    push(tok);
  }
  return terms;
}

/** Query terms grouped by the field they target, plus `bare` terms with no field qualifier
 *  (which hit the index's default field). Lets the UI **contain** highlighting to the field a
 *  term actually queried — so `category:guide` marks `guide` in the category cell, not the title. */
export interface ScopedTerms {
  fields: Record<string, string[]>;
  bare: string[];
}

/** Field-aware variant of {@link queryTerms}: keeps each term under the field it targets, tracking
 *  `field:( … )` groups, dropping negated (`-`/`NOT`) clauses and ranges. Best-effort, not a full
 *  parser — a display convenience. */
export function queryTermsByField(query: string): ScopedTerms {
  const fields: Record<string, string[]> = {};
  const bare: string[] = [];
  let scopeField: string | null = null; // active field inside a `field:( … )` group
  let depth = 0;

  const push = (field: string | null, raw: string) => {
    const term = raw.trim().toLowerCase();
    if (!term || OPERATORS.has(term.toUpperCase())) return;
    const bucket = field ? (fields[field] ??= []) : bare;
    if (!bucket.includes(term)) bucket.push(term);
  };

  const re = /"([^"]+)"|(\S+)/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(query)) !== null) {
    if (m[1] !== undefined) {
      for (const w of m[1].split(/\s+/)) push(scopeField, w);
      continue;
    }
    let tok = m[2];
    const negated = /^[-!]/.test(tok);
    tok = tok.replace(/^[-!]+/, '');
    let field: string | null = scopeField;
    let value = tok;
    const colon = tok.indexOf(':');
    if (colon > 0) {
      const name = tok.slice(0, colon).replace(/^[([{]+/, '');
      if (/^[A-Za-z_][\w.]*$/.test(name)) {
        field = name;
        value = tok.slice(colon + 1);
      }
    }
    const opens = (value.match(/\(/g) || []).length;
    const closes = (value.match(/\)/g) || []).length;
    if (opens > closes && field) {
      scopeField = field;
      depth += opens - closes;
    }
    const isRange = /^[[{]/.test(value.replace(/^\(+/, ''));
    const clean = value
      .replace(/^[-([{"]+/, '')
      .replace(/["*?~^)\]}]+$/, '')
      .replace(/[*?~^].*$/, '');
    if (!negated && !isRange) push(field, clean);
    if (closes > opens) {
      depth -= closes - opens;
      if (depth <= 0) {
        depth = 0;
        scopeField = null;
      }
    }
  }
  return { fields, bare };
}

/** Terms to highlight in a given field's cell: the terms that targeted that field, plus the
 *  bare (default-field) terms as a best-effort — a field a query never named gets nothing. */
export function fieldTerms(scoped: ScopedTerms, field: string): string[] {
  return [...(scoped.fields[field] ?? []), ...scoped.bare];
}

export interface Segment {
  text: string;
  hit: boolean;
}

/** Split `text` into segments, marking case-insensitive matches of any `term`. */
export function highlightSegments(text: string, terms: string[]): Segment[] {
  const escaped = terms.map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).filter(Boolean);
  if (!text || escaped.length === 0) return [{ text, hit: false }];

  const re = new RegExp(`(${escaped.join('|')})`, 'gi');
  const segments: Segment[] = [];
  let last = 0;
  let m: RegExpExecArray | null;
  while ((m = re.exec(text)) !== null) {
    if (m.index > last) segments.push({ text: text.slice(last, m.index), hit: false });
    segments.push({ text: m[0], hit: true });
    last = m.index + m[0].length;
    if (m.index === re.lastIndex) re.lastIndex++; // guard against zero-width matches
  }
  if (last < text.length) segments.push({ text: text.slice(last), hit: false });
  return segments;
}
