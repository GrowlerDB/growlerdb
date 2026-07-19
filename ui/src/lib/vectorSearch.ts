// Pure request-body builders for the vector search endpoints (`/v1/search:semantic`,
// `/v1/search:hybrid`). Kept side-effect-free + tested so `api.ts` is a thin transport wrapper and
// the wire shape (snake_case, omit-when-default) is verifiable in isolation. The engine treats an
// absent/zero `k`/`rrf_k`, and an empty `filter`/`syntax`/`index`, as "use the default", so those
// keys are simply omitted when unset.
import type { QuerySyntax } from './api';

/** RRF-k presets offered by the console's hybrid control (the fusion constant). 60 is the
 *  standard default the engine also falls back to when `rrf_k` is omitted. */
export const RRF_PRESETS = [10, 30, 60, 100] as const;
/** Default RRF-k — matches the engine's fallback (standard Reciprocal-Rank-Fusion constant). */
export const DEFAULT_RRF_K = 60;

/** Shared options for a semantic (KNN) request over a VECTOR field. */
export interface SemanticOpts {
  /** The VECTOR field path to embed + search (required). */
  vectorField: string;
  /** Number of nearest neighbors; omitted ⇒ the engine's bounded default page. */
  k?: number;
  /** Optional lexical/fast-field filter string constraining the KNN. */
  filter?: string;
  /** Filter/lexical grammar; omitted ⇒ the engine default (lucene). */
  syntax?: QuerySyntax;
  /** Target index; omitted ⇒ the endpoint's default index. */
  index?: string;
}

/** Options for a hybrid (BM25 + KNN, RRF-fused) request — semantic opts plus the fusion constant. */
export interface HybridOpts extends SemanticOpts {
  /** RRF fusion constant; omitted ⇒ the engine default (60). */
  rrfK?: number;
}

/** Body for `POST /v1/search:semantic`. `vector_field`/`query_text` are always present; every
 *  other key is emitted only when set so the engine applies its own default. */
export function semanticBody(queryText: string, opts: SemanticOpts): Record<string, unknown> {
  const body: Record<string, unknown> = {
    vector_field: opts.vectorField,
    query_text: queryText,
  };
  if (opts.k != null && opts.k > 0) body.k = opts.k;
  if (opts.filter) body.filter = opts.filter;
  if (opts.syntax) body.syntax = opts.syntax;
  if (opts.index) body.index = opts.index;
  return body;
}

/** Body for `POST /v1/search:hybrid` — the semantic body plus the optional `rrf_k`. */
export function hybridBody(queryText: string, opts: HybridOpts): Record<string, unknown> {
  const body = semanticBody(queryText, opts);
  if (opts.rrfK != null && opts.rrfK > 0) body.rrf_k = opts.rrfK;
  return body;
}
