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
