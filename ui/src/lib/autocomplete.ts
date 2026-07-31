// Query autocomplete: find the `field:prefix` token being typed at the cursor, ask the Suggest API
// for that field's terms, and replace the prefix with the chosen value.

export interface FieldToken {
  /** The field name left of the colon (e.g. `body`). */
  field: string;
  /** The partial value the user has typed after the colon (e.g. `err`). */
  prefix: string;
  /** Index in the query string where `field` begins (so a completion can splice in place). */
  start: number;
}

// A bare `field:prefix` at the very end of the query. The prefix stops at whitespace, a second
// colon, quotes, parens, or range brackets — so we never autocomplete inside a phrase or `[a TO b]`.
const TOKEN_RE = /([\w.]+):([^\s:"()[\]]*)$/;

/** The `field:prefix` token being typed at the end of `query`, or `null` (incl. an empty prefix,
 *  which the Suggest API rejects) — so callers only fire when there's something to complete. */
export function currentFieldToken(query: string): FieldToken | null {
  const m = TOKEN_RE.exec(query);
  if (!m || m[2].length === 0) return null;
  return { field: m[1], prefix: m[2], start: m.index };
}

/** Replace `token`'s prefix with the chosen `value`, returning the new query string. */
export function withCompletion(query: string, token: FieldToken, value: string): string {
  return query.slice(0, token.start) + token.field + ':' + value;
}
