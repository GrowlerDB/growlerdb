//! The real local BGE embedder: bge-small-en-v1.5 (a BERT model) on Candle, CPU, offline.

use std::path::{Path, PathBuf};

use candle_core::{Device, Tensor, D};
use candle_nn::VarBuilder;
use candle_transformers::models::bert::{BertModel, Config, DTYPE};
use tokenizers::{PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};

use growlerdb_core::index_def::VectorSpec;
use growlerdb_core::{EmbedError, Embedder};

/// The three files a model directory must contain.
const CONFIG_JSON: &str = "config.json";
const TOKENIZER_JSON: &str = "tokenizer.json";
const WEIGHTS: &str = "model.safetensors";

/// Upper bound on one BERT forward pass. Attention memory is `batch × heads × seq²` per layer —
/// unbounded, a whole-table build's texts in one pass allocate gigabytes (the arXiv demo corpus
/// OOM-killed a 4 GB node at batch 400 × seq 512). 32 keeps the worst-case pass in the
/// hundreds-of-MB range with no measurable CPU throughput cost.
const MAX_FORWARD_BATCH: usize = 32;

/// Resolve the model directory for `model_id`:
/// `${GROWLERDB_MODEL_DIR:-~/.cache/growlerdb/models}/<model_id>/`.
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

/// A local BERT embedder producing L2-normalized, mean-pooled sentence embeddings.
pub struct BgeEmbedder {
    model: BertModel,
    tokenizer: Tokenizer,
    device: Device,
    model_id: String,
    dims: usize,
}

impl BgeEmbedder {
    /// Load the model named by `spec.model` from its resolved [`model_dir`]. Fails (so the
    /// caller can fall back) if any of `config.json`, `tokenizer.json`, `model.safetensors`
    /// is missing or malformed, or if the model's hidden size doesn't match `spec.dims`.
    pub fn load(spec: &VectorSpec) -> Result<Self, EmbedError> {
        let dir = model_dir(&spec.model);
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
        let dims = config.hidden_size;
        if dims != spec.dims {
            return Err(EmbedError::DimMismatch {
                expected: spec.dims,
                got: dims,
            });
        }

        let mut tokenizer =
            Tokenizer::from_file(&tokenizer_path).map_err(|e| backend(&tokenizer_path, &e))?;
        // Batch inference needs a rectangular [batch, seq] tensor: pad each batch to its
        // longest member. The attention mask (built below) zeroes the padded positions so
        // they don't contribute to the mean pool.
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            ..Default::default()
        }));
        // BERT position embeddings stop at `max_position_embeddings` (512 for BGE): an
        // over-long text otherwise fails the whole forward pass — and, per-batch, would void
        // every other text batched with it. Embedding the head of a long text is the designed
        // degradation, so truncate to the model's window.
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: config.max_position_embeddings,
                ..Default::default()
            }))
            .map_err(|e| backend(&tokenizer_path, &e))?;

        let device = Device::Cpu;
        // SAFETY: mmap of a trusted, operator-provisioned weights file. `from_mmaped_safetensors`
        // is `unsafe` only because a concurrent external mutation of the file would be UB; the
        // model directory is read-only config, not attacker-writable input.
        let vb = unsafe {
            VarBuilder::from_mmaped_safetensors(std::slice::from_ref(&weights_path), DTYPE, &device)
                .map_err(|e| backend(&weights_path, &e.to_string()))?
        };
        let model =
            BertModel::load(vb, &config).map_err(|e| backend(&weights_path, &e.to_string()))?;

        Ok(Self {
            model,
            tokenizer,
            device,
            model_id: spec.model.clone(),
            dims,
        })
    }

    /// Tokenize + forward + mean-pool + L2-normalize a batch, returning one vector per input.
    /// Inputs beyond [`MAX_FORWARD_BATCH`] are processed in bounded sub-batches: attention
    /// memory scales with `batch × seq²`, so a single forward over a whole table build (e.g.
    /// thousands of 512-token abstracts) OOM-kills the node — sub-batching caps the peak at a
    /// few hundred MB regardless of input size, and per-sub-batch padding only pads to that
    /// sub-batch's longest sequence. Each text still embeds independently (BERT applies no
    /// cross-sequence attention), so results are identical to one big pass.
    fn embed_batch(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        if texts.is_empty() {
            return Ok(vec![]);
        }
        if texts.len() > MAX_FORWARD_BATCH {
            let mut out = Vec::with_capacity(texts.len());
            for chunk in texts.chunks(MAX_FORWARD_BATCH) {
                out.extend(self.embed_batch(chunk)?);
            }
            return Ok(out);
        }
        let inputs: Vec<String> = texts.to_vec();
        let encodings = self
            .tokenizer
            .encode_batch(inputs, true)
            .map_err(|e| EmbedError::Backend(format!("tokenize: {e}")))?;

        let batch = encodings.len();
        let seq = encodings.first().map(|e| e.get_ids().len()).unwrap_or(0);

        // Flatten to [batch * seq] then reshape — every encoding is padded to `seq`.
        let mut ids: Vec<u32> = Vec::with_capacity(batch * seq);
        let mut mask: Vec<u32> = Vec::with_capacity(batch * seq);
        for e in &encodings {
            ids.extend_from_slice(e.get_ids());
            mask.extend_from_slice(e.get_attention_mask());
        }

        let input_ids = Tensor::from_vec(ids, (batch, seq), &self.device).map_err(tensor_err)?;
        let attention_mask =
            Tensor::from_vec(mask, (batch, seq), &self.device).map_err(tensor_err)?;
        // BGE/BERT sentence embeddings use a single segment → all-zero token type ids.
        let token_type_ids = input_ids.zeros_like().map_err(tensor_err)?;

        // [batch, seq, hidden]
        let hidden = self
            .model
            .forward(&input_ids, &token_type_ids, Some(&attention_mask))
            .map_err(|e| EmbedError::Backend(format!("bert forward: {e}")))?;

        let pooled = mean_pool(&hidden, &attention_mask).map_err(tensor_err)?;
        let normed = l2_normalize(&pooled).map_err(tensor_err)?;

        let out: Vec<Vec<f32>> = normed.to_vec2().map_err(tensor_err)?;
        for v in &out {
            if v.len() != self.dims {
                return Err(EmbedError::DimMismatch {
                    expected: self.dims,
                    got: v.len(),
                });
            }
        }
        Ok(out)
    }
}

impl Embedder for BgeEmbedder {
    fn model_id(&self) -> &str {
        &self.model_id
    }

    fn dims(&self) -> usize {
        self.dims
    }

    fn embed(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        self.embed_batch(texts)
    }
}

/// Mean-pool the last hidden states over the token axis using the attention mask:
/// `sum(hidden * mask) / sum(mask)`. `hidden` is `[batch, seq, hidden]`, `mask` is
/// `[batch, seq]` (1 for real tokens, 0 for padding). Returns `[batch, hidden]`.
fn mean_pool(hidden: &Tensor, mask: &Tensor) -> candle_core::Result<Tensor> {
    let mask = mask.to_dtype(hidden.dtype())?; // [b, seq]
    let mask_exp = mask.unsqueeze(2)?; // [b, seq, 1]
    let summed = hidden.broadcast_mul(&mask_exp)?.sum(1)?; // [b, hidden]
                                                           // Clamp the token count away from zero so an all-padding (empty) row yields zeros, not NaN.
    let counts = mask.sum(1)?.clamp(1e-9, f32::INFINITY)?.unsqueeze(1)?; // [b, 1]
    summed.broadcast_div(&counts)
}

/// L2-normalize each row of a `[batch, hidden]` tensor. A zero row (empty input) stays zero
/// because its norm is clamped to a small epsilon rather than dividing by zero.
fn l2_normalize(x: &Tensor) -> candle_core::Result<Tensor> {
    let norm = x
        .sqr()?
        .sum_keepdim(D::Minus1)?
        .sqrt()?
        .clamp(1e-12, f32::INFINITY)?; // [b, 1]
    x.broadcast_div(&norm)
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
    use growlerdb_core::index_def::{EmbedProvider, VectorMetric};

    #[test]
    fn mean_pool_masks_padding() {
        let dev = Device::Cpu;
        // batch=1, seq=3, hidden=2. Third token is padding (mask 0), so the mean is over the
        // first two rows only: ([1,2] + [3,4]) / 2 = [2, 3]. If padding leaked in it'd be
        // ([1,2]+[3,4]+[5,6])/3 = [3,4].
        let hidden = Tensor::from_vec(vec![1f32, 2., 3., 4., 5., 6.], (1, 3, 2), &dev).unwrap();
        let mask = Tensor::from_vec(vec![1f32, 1., 0.], (1, 3), &dev).unwrap();
        let pooled = mean_pool(&hidden, &mask).unwrap();
        let got: Vec<Vec<f32>> = pooled.to_vec2().unwrap();
        assert!((got[0][0] - 2.0).abs() < 1e-6, "{got:?}");
        assert!((got[0][1] - 3.0).abs() < 1e-6, "{got:?}");
    }

    #[test]
    fn mean_pool_all_padding_is_zero() {
        let dev = Device::Cpu;
        let hidden = Tensor::from_vec(vec![7f32, 8., 9., 10.], (1, 2, 2), &dev).unwrap();
        let mask = Tensor::from_vec(vec![0f32, 0.], (1, 2), &dev).unwrap();
        let pooled = mean_pool(&hidden, &mask).unwrap();
        let got: Vec<Vec<f32>> = pooled.to_vec2().unwrap();
        assert!(got[0].iter().all(|x| x.abs() < 1e-6), "{got:?}");
    }

    #[test]
    fn l2_normalize_unit_norm() {
        let dev = Device::Cpu;
        // [3, 4] → norm 5 → [0.6, 0.8]; a zero row stays zero (no NaN).
        let x = Tensor::from_vec(vec![3f32, 4., 0., 0.], (2, 2), &dev).unwrap();
        let n = l2_normalize(&x).unwrap();
        let got: Vec<Vec<f32>> = n.to_vec2().unwrap();
        assert!((got[0][0] - 0.6).abs() < 1e-6, "{got:?}");
        assert!((got[0][1] - 0.8).abs() < 1e-6, "{got:?}");
        let norm0: f32 = got[0].iter().map(|v| v * v).sum::<f32>().sqrt();
        assert!((norm0 - 1.0).abs() < 1e-6);
        assert!(got[1].iter().all(|x| x.abs() < 1e-12), "{got:?}");
    }

    /// End-to-end test against a real provisioned model. Ignored: it needs
    /// `GROWLERDB_MODEL_DIR` (or `~/.cache/growlerdb/models`) to contain
    /// `bge-small-en-v1.5/{config.json,tokenizer.json,model.safetensors}` and never runs in
    /// CI. Run with: `cargo test -p growlerdb-embed -- --ignored bge_real_model`.
    #[test]
    #[ignore = "requires a provisioned GROWLERDB_MODEL_DIR bge-small-en-v1.5 model"]
    fn bge_real_model() {
        let spec = VectorSpec {
            dims: 384,
            model: "bge-small-en-v1.5".into(),
            metric: VectorMetric::Cosine,
            provider: EmbedProvider::Local,
            source_field: "body".into(),
        };
        let e = BgeEmbedder::load(&spec).expect("model should load");
        assert_eq!(e.dims(), 384);

        let out = e
            .embed(&[
                "A cat sleeps on the warm windowsill.".into(),
                "A kitten naps in the sunny window.".into(),
                "Quarterly tax filings are due next Friday.".into(),
            ])
            .unwrap();
        assert_eq!(out.len(), 3);
        for v in &out {
            assert_eq!(v.len(), 384);
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!((norm - 1.0).abs() < 1e-3, "expected unit norm, got {norm}");
        }
        let cos = |a: &[f32], b: &[f32]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
        let similar = cos(&out[0], &out[1]);
        let dissimilar = cos(&out[0], &out[2]);
        assert!(
            similar > dissimilar,
            "semantically-similar pair ({similar}) should score above the dissimilar pair ({dissimilar})"
        );

        // Sub-batching (the MAX_FORWARD_BATCH bound) changes memory, never results: a batch
        // crossing the boundary returns one vector per input, and each equals the vector the
        // same text gets alone (padding differs per sub-batch; the attention mask makes that
        // irrelevant). Guards the whole-table build path that used to OOM.
        let texts: Vec<String> = (0..(MAX_FORWARD_BATCH * 2 + 5))
            .map(|i| format!("document number {i} about search engines and lakehouse tables"))
            .collect();
        let batched = e.embed(&texts).unwrap();
        assert_eq!(batched.len(), texts.len());
        let solo = e
            .embed(&texts[MAX_FORWARD_BATCH..MAX_FORWARD_BATCH + 1])
            .unwrap();
        let drift = 1.0 - cos(&batched[MAX_FORWARD_BATCH], &solo[0]);
        assert!(
            drift.abs() < 1e-4,
            "chunked embedding must equal the solo embedding (cos drift {drift})"
        );

        // Regression (TASK-323): a text beyond BERT's 512-position window used to fail the
        // forward pass — and, batched, void every other text's embedding with it (the arXiv demo
        // silently lost all 20k of a chunk's vectors to over-long abstracts). Truncation must
        // make it embed (the head of the text), and its batch-mates must be unaffected.
        let long = "retrieval engine lakehouse search ".repeat(400); // ~1600 words ≫ 512 tokens
        let out = e
            .embed(&["short doc about search".into(), long])
            .expect("an over-long text must embed (truncated), not fail the batch");
        assert_eq!(out.len(), 2);
        for v in &out {
            assert_eq!(v.len(), 384);
            let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!((norm - 1.0).abs() < 1e-3, "expected unit norm, got {norm}");
        }
    }
}
