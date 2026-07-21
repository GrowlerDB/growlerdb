//! The real local BGE embedder: bge-small-en-v1.5 (a BERT model) on **ONNX Runtime**, CPU, offline.
//!
//! ONNX Runtime links a native `libonnxruntime` (fetched at build time via ort's `download-binaries`
//! feature; runtime stays offline). This replaces the former pure-Rust Candle path for ~2-4x CPU
//! throughput (D20/D46). The cross-encoder reranker still runs on Candle pending its own ONNX move.
//!
//! The model directory holds `config.json`, `tokenizer.json`, and `model.onnx` (the BERT graph with
//! `input_ids` / `attention_mask` / `token_type_ids` inputs and a `last_hidden_state` output).
//! Sentence embeddings are the attention-masked **mean pool** of the last hidden state, **L2
//! normalized** — identical semantics to the prior Candle path, so vectors stay reproducible across
//! ingest and query (D43).

use std::path::{Path, PathBuf};
use std::sync::Mutex;

use ort::session::{builder::GraphOptimizationLevel, Session};
use ort::value::Tensor;
use tokenizers::{PaddingParams, PaddingStrategy, Tokenizer, TruncationParams};

use growlerdb_core::index_def::VectorSpec;
use growlerdb_core::{EmbedError, Embedder};

/// The three files a model directory must contain.
const CONFIG_JSON: &str = "config.json";
const TOKENIZER_JSON: &str = "tokenizer.json";
const MODEL_ONNX: &str = "model.onnx";

/// Upper bound on inputs per ONNX `Run`. ONNX Runtime parallelizes a single run across its
/// intra-op thread pool, and short synopses pad cheaply, so this is larger than the old Candle
/// bound (32) — but still bounded so a whole-table build doesn't materialize one giant tensor.
const MAX_FORWARD_BATCH: usize = 64;

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

/// A local BERT embedder (ONNX Runtime) producing L2-normalized, mean-pooled sentence embeddings.
pub struct BgeEmbedder {
    // `Session::run` takes `&mut self`, but the [`Embedder`] seam is shared (`&self` behind an
    // `Arc`), so the session lives behind a `Mutex`. ONNX Runtime parallelizes each `run` across
    // its intra-op threads, so serializing runs still saturates the cores — the win is per-run,
    // not per-concurrent-call. The embedder is cached per model dir by the factory, so the lock is
    // uncontended on the ingest path (one embed loop) and cheap on the query path.
    session: Mutex<Session>,
    tokenizer: Tokenizer,
    model_id: String,
    dims: usize,
}

impl BgeEmbedder {
    /// Load the model named by `spec.model` from its resolved [`model_dir`]. Fails (so the caller
    /// can fall back) if any of `config.json`, `tokenizer.json`, `model.onnx` is missing or
    /// malformed, or if the model's hidden size doesn't match `spec.dims`.
    pub fn load(spec: &VectorSpec) -> Result<Self, EmbedError> {
        let dir = model_dir(&spec.model);
        let config_path = dir.join(CONFIG_JSON);
        let tokenizer_path = dir.join(TOKENIZER_JSON);
        let model_path = dir.join(MODEL_ONNX);

        for p in [&config_path, &tokenizer_path, &model_path] {
            if !p.exists() {
                return Err(EmbedError::Backend(format!("missing {}", p.display())));
            }
        }

        // config.json → hidden size (must match the field's dims) and the sequence window.
        let config: serde_json::Value = {
            let raw = std::fs::read_to_string(&config_path)
                .map_err(|e| backend(&config_path, &e.to_string()))?;
            serde_json::from_str(&raw).map_err(|e| backend(&config_path, &e.to_string()))?
        };
        let dims = config
            .get("hidden_size")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| backend(&config_path, &"missing hidden_size"))?
            as usize;
        if dims != spec.dims {
            return Err(EmbedError::DimMismatch {
                expected: spec.dims,
                got: dims,
            });
        }
        let max_seq = config
            .get("max_position_embeddings")
            .and_then(|v| v.as_u64())
            .unwrap_or(512) as usize;

        let mut tokenizer =
            Tokenizer::from_file(&tokenizer_path).map_err(|e| backend(&tokenizer_path, &e))?;
        // Batch inference needs a rectangular [batch, seq] tensor: pad each batch to its longest
        // member. The attention mask zeroes the padded positions in the mean pool below.
        tokenizer.with_padding(Some(PaddingParams {
            strategy: PaddingStrategy::BatchLongest,
            ..Default::default()
        }));
        // BERT position embeddings stop at `max_position_embeddings` (512 for BGE): an over-long
        // text otherwise blows the graph's position range. Embedding the head of a long text is the
        // designed degradation, so truncate to the model's window.
        tokenizer
            .with_truncation(Some(TruncationParams {
                max_length: max_seq,
                ..Default::default()
            }))
            .map_err(|e| backend(&tokenizer_path, &e))?;

        let session = Session::builder()
            .map_err(ort_err)?
            .with_optimization_level(GraphOptimizationLevel::Level3)
            .map_err(ort_err)?
            .commit_from_file(&model_path)
            .map_err(ort_err)?;

        Ok(Self {
            session: Mutex::new(session),
            tokenizer,
            model_id: spec.model.clone(),
            dims,
        })
    }

    /// Tokenize + ONNX forward + mean-pool + L2-normalize a batch, one vector per input. Inputs
    /// beyond [`MAX_FORWARD_BATCH`] are processed in bounded sub-batches (each text embeds
    /// independently — no cross-sequence attention — so results are identical to one big run).
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

        let encodings = self
            .tokenizer
            .encode_batch(texts.to_vec(), true)
            .map_err(|e| EmbedError::Backend(format!("tokenize: {e}")))?;
        let batch = encodings.len();
        let seq = encodings.first().map(|e| e.get_ids().len()).unwrap_or(0);

        let mut ids = Vec::with_capacity(batch * seq);
        let mut mask = Vec::with_capacity(batch * seq);
        for e in &encodings {
            ids.extend(e.get_ids().iter().map(|&x| x as i64));
            mask.extend(e.get_attention_mask().iter().map(|&x| x as i64));
        }
        // BGE/BERT sentence embeddings use a single segment → all-zero token type ids.
        let types = vec![0i64; batch * seq];

        // Build [batch, seq] i64 tensors directly from (shape, data) — no ndarray dependency.
        let shape = [batch as i64, seq as i64];
        let ids_t = Tensor::from_array((shape, ids)).map_err(ort_err)?;
        let mask_t = Tensor::from_array((shape, mask.clone())).map_err(ort_err)?;
        let types_t = Tensor::from_array((shape, types)).map_err(ort_err)?;

        let mut session = self.session.lock().unwrap_or_else(|p| p.into_inner());
        let outputs = session
            .run(ort::inputs![
                "input_ids" => ids_t,
                "attention_mask" => mask_t,
                "token_type_ids" => types_t,
            ])
            .map_err(ort_err)?;

        // last_hidden_state: [batch, seq, hidden] (row-major)
        let (_shape, data) = outputs["last_hidden_state"]
            .try_extract_tensor::<f32>()
            .map_err(ort_err)?;
        let hidden = data.len().checked_div(batch * seq).unwrap_or(0);
        if hidden != self.dims {
            return Err(EmbedError::DimMismatch {
                expected: self.dims,
                got: hidden,
            });
        }

        Ok(mean_pool_l2(data, &mask, batch, seq, hidden))
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

/// Attention-masked mean pool of `last_hidden_state` (`[batch, seq, hidden]`, row-major in `data`)
/// followed by per-row L2 normalization. `mask[b*seq + s]` is 1 for a real token, 0 for padding; an
/// all-padding row yields a zero vector (count/norm clamped away from zero, no NaN).
fn mean_pool_l2(
    data: &[f32],
    mask: &[i64],
    batch: usize,
    seq: usize,
    hidden: usize,
) -> Vec<Vec<f32>> {
    let mut out = Vec::with_capacity(batch);
    for b in 0..batch {
        let mut acc = vec![0f32; hidden];
        let mut count = 0f32;
        for s in 0..seq {
            if mask[b * seq + s] == 0 {
                continue;
            }
            count += 1.0;
            let base = (b * seq + s) * hidden;
            for h in 0..hidden {
                acc[h] += data[base + h];
            }
        }
        let count = count.max(1e-9);
        let mut norm = 0f32;
        for a in &mut acc {
            *a /= count;
            norm += *a * *a;
        }
        let norm = norm.sqrt().max(1e-12);
        for a in &mut acc {
            *a /= norm;
        }
        out.push(acc);
    }
    out
}

fn backend(path: &Path, msg: &impl std::fmt::Display) -> EmbedError {
    EmbedError::Backend(format!("{}: {msg}", path.display()))
}

fn ort_err<T>(e: ort::Error<T>) -> EmbedError {
    EmbedError::Backend(format!("onnx: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mean_pool_masks_padding_and_normalizes() {
        // batch=1, seq=3, hidden=2. Third token is padding (mask 0), so the pool is over the first
        // two rows: ([1,2] + [3,4]) / 2 = [2, 3], then L2-normalized.
        let data = vec![1f32, 2., 3., 4., 5., 6.];
        let mask = vec![1i64, 1, 0];
        let out = mean_pool_l2(&data, &mask, 1, 3, 2);
        let v = &out[0];
        let norm = (v[0] * v[0] + v[1] * v[1]).sqrt();
        assert!((norm - 1.0).abs() < 1e-6, "row is L2-normalized");
        // Direction is [2,3] normalized; the padding row [5,6] must not leak (that would give [3,4]).
        let expect0 = 2.0 / (2f32 * 2. + 3. * 3.).sqrt();
        assert!(
            (v[0] - expect0).abs() < 1e-6,
            "padding excluded from the pool"
        );
    }

    #[test]
    fn all_padding_row_is_zero_not_nan() {
        let data = vec![9f32, 9., 9., 9.];
        let mask = vec![0i64, 0];
        let out = mean_pool_l2(&data, &mask, 1, 2, 2);
        assert_eq!(out[0], vec![0.0, 0.0]);
    }

    /// End-to-end against the real ONNX model. Gated (needs a provisioned
    /// `GROWLERDB_MODEL_DIR/bge-small-en-v1.5/{config.json,tokenizer.json,model.onnx}`); never runs
    /// in CI. Run: `cargo test -p growlerdb-embed --release -- --ignored bge_onnx_real_model --nocapture`
    #[test]
    #[ignore = "requires a provisioned bge-small-en-v1.5 ONNX model dir"]
    fn bge_onnx_real_model() {
        use growlerdb_core::index_def::{EmbedProvider, VectorMetric};
        let spec = VectorSpec {
            dims: 384,
            model: "bge-small-en-v1.5".into(),
            source_field: "body".into(),
            metric: VectorMetric::Cosine,
            provider: EmbedProvider::Local,
        };
        let e = BgeEmbedder::load(&spec).expect("load ONNX model");
        assert_eq!(e.dims(), 384);
        let v = e
            .embed(&[
                "a cat sits on the mat".into(),               // 0
                "a kitten rests on the rug".into(),           // 1 — semantically close to 0
                "quarterly financial earnings report".into(), // 2 — far from 0
                "".into(),                                    // 3 — empty ⇒ CLS/SEP-pooled, finite
            ])
            .expect("embed");
        assert_eq!(v.len(), 4);
        for row in &v[..3] {
            assert_eq!(row.len(), 384);
            let norm: f32 = row.iter().map(|x| x * x).sum::<f32>().sqrt();
            assert!((norm - 1.0).abs() < 1e-3, "unit-normalized, got {norm}");
        }
        let cos = |a: &[f32], b: &[f32]| a.iter().zip(b).map(|(x, y)| x * y).sum::<f32>();
        let close = cos(&v[0], &v[1]);
        let far = cos(&v[0], &v[2]);
        assert!(
            close > far,
            "semantic structure holds: cos(cat,kitten)={close:.3} > cos(cat,finance)={far:.3}"
        );
        // Empty text still tokenizes to [CLS]/[SEP] ⇒ a finite pooled vector, never NaN.
        assert!(
            v[3].iter().all(|x| x.is_finite()),
            "empty text ⇒ finite vector, no NaN"
        );
        eprintln!("ONNX bge-small: cos(cat,kitten)={close:.3}, cos(cat,finance)={far:.3}");
    }

    /// In-process throughput probe vs the ~10 docs/s Candle baseline, on synopsis-length text
    /// (~78 words). Gated + release-only. Run:
    /// `cargo test -p growlerdb-embed --release -- --ignored bge_onnx_throughput --nocapture`
    #[test]
    #[ignore = "requires a provisioned bge-small-en-v1.5 ONNX model dir; benchmark"]
    fn bge_onnx_throughput() {
        use growlerdb_core::index_def::{EmbedProvider, VectorMetric};
        let spec = VectorSpec {
            dims: 384,
            model: "bge-small-en-v1.5".into(),
            source_field: "body".into(),
            metric: VectorMetric::Cosine,
            provider: EmbedProvider::Local,
        };
        let e = BgeEmbedder::load(&spec).expect("load");
        // A ~78-word synopsis-length string (the movie demo's embed source size).
        let sentence = "During the war a small crew of soldiers is sent behind enemy lines on a \
            dangerous mission to recover stolen plans, and as they cross the ruined countryside they \
            confront betrayal, loss, and an impossible choice that will decide the fate of the city \
            they left behind, testing loyalty and courage at every turn before the final reckoning "
            .to_string();
        let n = 500usize;
        let texts: Vec<String> = (0..n).map(|_| sentence.clone()).collect();
        let t0 = std::time::Instant::now();
        let out = e.embed(&texts).expect("embed");
        let secs = t0.elapsed().as_secs_f64();
        assert_eq!(out.len(), n);
        eprintln!(
            "ONNX throughput: {n} docs in {secs:.2}s = {:.1} docs/s (candle baseline ~10 docs/s)",
            n as f64 / secs
        );
    }
}
