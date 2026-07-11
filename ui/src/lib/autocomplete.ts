// Query autocomplete: the search box holds a Lucene/KQL query, so completing a value
// means finding the `field:prefix` token the user is currently typing (at the cursor/end), asking
// the Suggest API for that field's terms, and replacing the prefix with the chosen value. Pure +
// unit-tested; the Svelte component handles debounce, keyboard, and the network call.

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

/**
 * The `field:prefix` token being typed at the end of `query`, or `null`. Returns `null` when there's
 * no field context or the prefix is empty (the Suggest API requires non-empty text), so callers only
 * fire a request when there's something to complete.
 */
export function currentFieldToken(query: string): FieldToken | null {
  const m = TOKEN_RE.exec(query);
  if (!m || m[2].length === 0) return null;
  return { field: m[1], prefix: m[2], start: m.index };
}

/** Replace `token`'s prefix with the chosen `value`, returning the new query string. */
export function withCompletion(query: string, token: FieldToken, value: string): string {
  return query.slice(0, token.start) + token.field + ':' + value;
}
