//! The reranking seam: reorder an already-retrieved top-K by (query, document-text)
//! relevance ([D21]). A reranker sits **outside** the index — it never changes what is
//! stored or how retrieval scores; it is the optional final stage of retrieval-first
//! ([D42]), off by default and opt-in per query. The seam a real cross-encoder (local
//! `bge-reranker-base`, or an external provider) plugs into; the built-in
//! [`HashReranker`] is the deterministic dependency-free default/fallback for tests + CI.
//!
//! [D21]: ../../okf/system/decisions/d21-reranker.md
//! [D42]: ../../okf/system/decisions/d42-retrieval-first.md

use std::collections::HashSet;

use crate::embed::EmbedError;

/// Reorders an already-retrieved candidate set by (query, document) relevance. Unlike an
/// [`Embedder`](crate::embed::Embedder), a reranker never touches the index: it takes the
/// top-K documents' **text** (each hit's cached `source_field` text) and returns a new order.
pub trait Reranker: Send + Sync {
    /// The reranker model id (recorded for reproducibility / surfaced in logs).
    fn model_id(&self) -> &str;
    /// Score `docs` against `query` and return `(original_index, score)` pairs sorted
    /// **best-first**, truncated to at most `top_k`. `original_index` indexes back into the
    /// input `docs` slice so the caller can reorder its parallel hit list. A higher score is
    /// more relevant. An empty `docs` yields an empty result.
    fn rerank(
        &self,
        query: &str,
        docs: &[String],
        top_k: usize,
    ) -> Result<Vec<(usize, f32)>, EmbedError>;
}

/// Deterministic, dependency-free reranker for tests, CI, and as the fallback when no real
/// cross-encoder model is provisioned. Scores each document by its **token overlap** with the
/// query (the count of distinct query tokens the document contains). Stable across runs (no
/// RNG); ties break by the document's original position, so a rerank with no signal preserves
/// the retrieval order. NOT a semantic cross-encoder.
pub struct HashReranker {
    model_id: String,
}

impl HashReranker {
    /// Build a token-overlap reranker tagged with `model_id`.
    pub fn new(model_id: impl Into<String>) -> Self {
        Self {
            model_id: model_id.into(),
        }
    }
}

impl Reranker for HashReranker {
    fn model_id(&self) -> &str {
        &self.model_id
    }

    fn rerank(
        &self,
        query: &str,
        docs: &[String],
        top_k: usize,
    ) -> Result<Vec<(usize, f32)>, EmbedError> {
        let q: HashSet<&str> = query.split_whitespace().collect();
        let mut scored: Vec<(usize, f32)> = docs
            .iter()
            .enumerate()
            .map(|(i, doc)| {
                let overlap = if q.is_empty() {
                    0
                } else {
                    doc.split_whitespace()
                        .collect::<HashSet<&str>>()
                        .intersection(&q)
                        .count()
                };
                (i, overlap as f32)
            })
            .collect();
        // Best-first by score; a stable tiebreak on the original index preserves retrieval
        // order among equally-relevant docs (a no-signal rerank is a no-op reorder).
        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        scored.truncate(top_k);
        Ok(scored)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hash_reranker_reorders_by_token_overlap() {
        let r = HashReranker::new("test-reranker");
        assert_eq!(r.model_id(), "test-reranker");
        // Retrieval order below is deliberately "wrong": the most-relevant doc is last.
        let docs = vec![
            "apache iceberg lakehouse tables".to_string(), // 0 overlap
            "full text search relevance".to_string(),      // 0 overlap
            "semantic retrieval embeddings vector".to_string(), // 3 overlap → should win
        ];
        let order = r.rerank("vector semantic embeddings", &docs, 3).unwrap();
        // The high-overlap doc (index 2) is reranked to the front.
        assert_eq!(order[0].0, 2);
        assert!(order[0].1 > order[1].1, "the winner outscores the rest");
    }

    #[test]
    fn hash_reranker_is_deterministic_and_truncates() {
        let r = HashReranker::new("m");
        let docs = vec![
            "alpha beta".to_string(),
            "beta gamma".to_string(),
            "alpha beta gamma".to_string(),
        ];
        let a = r.rerank("alpha beta gamma", &docs, 2).unwrap();
        let b = r.rerank("alpha beta gamma", &docs, 2).unwrap();
        assert_eq!(a, b, "same input → identical order");
        assert_eq!(a.len(), 2, "top_k truncates");
        assert_eq!(a[0].0, 2, "the full-overlap doc ranks first");
    }

    #[test]
    fn hash_reranker_no_signal_preserves_order_and_handles_empties() {
        let r = HashReranker::new("m");
        // No query tokens overlap → every score 0 → stable original order.
        let docs = vec!["one".to_string(), "two".to_string(), "three".to_string()];
        let order = r.rerank("zzz", &docs, 10).unwrap();
        assert_eq!(
            order.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        // Empty doc set → empty result (never panics).
        assert!(r.rerank("q", &[], 5).unwrap().is_empty());
    }
}
