//! The real local cross-encoder reranker: `bge-reranker-base` (a BERT-family model) on Candle,
//! CPU, offline. It scores each `(query, document)` pair with a single relevance logit — the
//! cross-encoder attends over query and document jointly, so it is a strictly better relevance
//! signal than the bi-encoder retrieval it reorders. Loaded from the same local model directory
//! layout as the [`BgeEmbedder`](crate::BgeEmbedder); a missing model falls the factory back to the
//! dependency-free [`HashReranker`](growlerdb_core::HashReranker).

use std::path::{Path, PathBuf};

use candle_core::{Device, IndexOp, Tensor};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config, DTYPE};
use tokenizers::Tokenizer;

use growlerdb_core::{EmbedError, Reranker};

/// The three model files a reranker directory must contain (same layout as the embedder).
const CONFIG_JSON: &str = "config.json";
const TOKENIZER_JSON: &str = "tokenizer.json";
const WEIGHTS: &str = "model.safetensors";

/// Resolve the model directory for `model_id`:
/// `${GROWLERDB_MODEL_DIR:-~/.cache/growlerdb/models}/<model_id>/` — identical to the embedder's
/// resolution so one provisioning step serves both.
pub(crate) fn model_dir(model_id: &str) -> PathBuf {
    let base = std::env::var_os("GROWLERDB_MODEL_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(default_model_root);
    base.join(model_id)
}

/// `~/.cache/growlerdb/models`, falling back to a relative path if `HOME` is unset.
fn default_model_root() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    home.join(".cache").join("growlerdb").join("models")
}

/// A local BERT cross-encoder that scores a `(query, document)` pair by a single relevance logit:
/// the `[CLS]` hidden state through a linear classification head.
pub struct BgeReranker {
    model: BertModel,
    tokenizer: Tokenizer,
    device: Device,
    /// Classification head weight `[num_labels, hidden]` and bias `[num_labels]`.
    classifier_w: Tensor,
    classifier_b: Tensor,
    model_id: String,
}

impl BgeReranker {
    /// Load the cross-encoder named `model_id` from its resolved [`model_dir`]. Fails (so the
    /// factory can fall back to [`HashReranker`](growlerdb_core::HashReranker)) if any model file or
    /// the classification head (`classifier.weight`/`classifier.bias`) is missing or malformed.
    pub fn load(model_id: &str) -> Result<Self, EmbedError> {
        let dir = model_dir(model_id);
        let config_path = dir.join(CONFIG_JSON);
        let tokenizer_path = dir.join(TOKENIZER_JSON);
        let weights_path = dir.join(WEIGHTS);

        for p in [&config_path, &tokenizer_path, &weights_path] {
            if !p.exists() {
                return Err(EmbedError::Backend(format!("missing {}", p.display())));
            }
        }

        let config: Config = {
            let raw = std::fs::read_to_string(&config_path)
                .map_err(|e| backend(&config_path, &e.to_string()))?;
            serde_json::from_str(&raw).map_err(|e| backend(&config_path, &e.to_string()))?
        };
        let hidden = config.hidden_size;

        let tokenizer =
            Tokenizer::from_file(&tokenizer_path).map_err(|e| backend(&tokenizer_path, &e))?;

        let device = Device::Cpu;
        // SAFETY: mmap of a trusted, operator-provisioned weights file — see the identical note in
        // `bge.rs`; the model directory is read-only config, not attacker-writable input.
        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(std::slice::from_ref(&weights_path), DTYPE, &device)
                .map_err(|e| backend(&weights_path, &e.to_string()))?
        };
        let model = BertModel::load(vb.clone(), &config)
            .map_err(|e| backend(&weights_path, &e.to_string()))?;

        // Cross-encoders emit a single relevance logit: a `[num_labels, hidden]` linear head over
        // the `[CLS]` state, `num_labels == 1` for a ranking model. If the head isn't present the
        // model isn't a cross-encoder we can score with — error so the factory falls back.
        let classifier_w = vb
            .get((1, hidden), "classifier.weight")
            .map_err(|e| backend(&weights_path, &format!("classifier.weight: {e}")))?;
        let classifier_b = vb
            .get(1, "classifier.bias")
            .map_err(|e| backend(&weights_path, &format!("classifier.bias: {e}")))?;

        Ok(Self {
            model,
            tokenizer,
            device,
            classifier_w,
            classifier_b,
            model_id: model_id.to_string(),
        })
    }

    /// The relevance logit for one `(query, doc)` pair: tokenize the pair, run BERT, take the
    /// `[CLS]` hidden state, and project it through the classification head.
    fn score_pair(&self, query: &str, doc: &str) -> Result<f32, EmbedError> {
        let enc = self
            .tokenizer
            .encode((query, doc), true)
            .map_err(|e| EmbedError::Backend(format!("tokenize: {e}")))?;
        let seq = enc.get_ids().len();
        let ids = Tensor::from_slice(enc.get_ids(), (1, seq), &self.device).map_err(tensor_err)?;
        let mask = Tensor::from_slice(enc.get_attention_mask(), (1, seq), &self.device)
            .map_err(tensor_err)?;
        // Pair encoding sets segment ids (0 = query, 1 = document) — the cross-encoder signal.
        let type_ids =
            Tensor::from_slice(enc.get_type_ids(), (1, seq), &self.device).map_err(tensor_err)?;

        // [1, seq, hidden]
        let hidden = self
            .model
            .forward(&ids, &type_ids, Some(&mask))
            .map_err(|e| EmbedError::Backend(format!("bert forward: {e}")))?;
        // [CLS] is position 0 → [1, hidden].
        let cls = hidden.i((.., 0, ..)).map_err(tensor_err)?;
        // logit = cls · Wᵀ + b → [1, 1]
        let logit = cls
            .matmul(&self.classifier_w.t().map_err(tensor_err)?)
            .map_err(tensor_err)?
            .broadcast_add(&self.classifier_b)
            .map_err(tensor_err)?;
        let v: Vec<Vec<f32>> = logit.to_vec2().map_err(tensor_err)?;
        Ok(v[0][0])
    }
}

impl Reranker for BgeReranker {
    fn model_id(&self) -> &str {
        &self.model_id
    }

    fn rerank(
        &self,
        query: &str,
        docs: &[String],
        top_k: usize,
    ) -> Result<Vec<(usize, f32)>, EmbedError> {
        let mut scored: Vec<(usize, f32)> = Vec::with_capacity(docs.len());
        for (i, doc) in docs.iter().enumerate() {
            scored.push((i, self.score_pair(query, doc)?));
        }
        scored.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });
        scored.truncate(top_k);
        Ok(scored)
    }
}

fn backend(path: &Path, msg: &impl std::fmt::Display) -> EmbedError {
    EmbedError::Backend(format!("{}: {msg}", path.display()))
}

fn tensor_err(e: candle_core::Error) -> EmbedError {
    EmbedError::Backend(format!("tensor op: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// End-to-end test against a real provisioned cross-encoder. Ignored: it needs
    /// `GROWLERDB_MODEL_DIR` (or `~/.cache/growlerdb/models`) to contain
    /// `bge-reranker-base/{config.json,tokenizer.json,model.safetensors}` and never runs in CI.
    /// Run with: `cargo test -p growlerdb-embed -- --ignored bge_reranker_real_model`.
    #[test]
    #[ignore = "requires a provisioned GROWLERDB_MODEL_DIR bge-reranker-base model"]
    fn bge_reranker_real_model() {
        let r = BgeReranker::load("bge-reranker-base").expect("model should load");
        let docs = vec![
            "A kitten naps in the sunny window.".to_string(),
            "Quarterly tax filings are due next Friday.".to_string(),
        ];
        let order = r.rerank("a cat sleeping in the sun", &docs, 2).unwrap();
        // The semantically-relevant document should rank first.
        assert_eq!(order[0].0, 0, "the cat sentence should win: {order:?}");
    }
}
