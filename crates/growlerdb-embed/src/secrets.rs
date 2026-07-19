//! Server-side-only outbound-provider API keys for the opt-in **external** embedding/rerank
//! path ([D41] open-core, [D42] retrieval-first). GrowlerDB's default embedding/reranking is
//! local and keyless; these secrets are read **only** when a vector field selects
//! `provider == External` (embedding) or the reranker is configured external, and are used
//! solely to authenticate GrowlerDB's own outbound HTTP call to a hosted **embedding** or
//! **reranking** provider. GrowlerDB never calls an LLM ([D42]) — there are no LLM keys here.
//!
//! # Where keys come from
//!
//! Keys are read from the **server process's** environment, never from a request:
//!
//! | Purpose            | Env var                       |
//! |--------------------|-------------------------------|
//! | Embedding provider | `GROWLERDB_EMBEDDING_API_KEY` |
//! | Rerank provider    | `GROWLERDB_RERANK_API_KEY`    |
//!
//! A Kubernetes `Secret` or a Vault agent mounts its material as exactly these env vars (a
//! mounted-secret volume projected into the container env, or an `envFrom`/`valueFrom.secretKeyRef`
//! in the pod spec). Because [`ProviderSecrets`] **re-reads the environment on every call**, a
//! rotated secret (the platform updating the projected env) is picked up without a restart.
//!
//! # Never logged, never served
//!
//! A raw key must never appear in logs, errors, or any REST response. [`ProviderSecrets`] has
//! manual [`Debug`]/[`Display`] impls that print `***`, and [`redact`] is the only sanctioned way
//! to render key-derived text. The `/v1/config` DTO (and every other REST response) carries no key
//! field — see the engine's `config_dto_has_no_secret_field` test.
//!
//! [D41]: ../../okf/system/decisions/d41-open-core.md
//! [D42]: ../../okf/system/decisions/d42-retrieval-first.md

use std::fmt;

/// Env var carrying the outbound **embedding** provider API key (server-side only).
pub const EMBEDDING_API_KEY_ENV: &str = "GROWLERDB_EMBEDDING_API_KEY";
/// Env var carrying the outbound **rerank** provider API key (server-side only).
pub const RERANK_API_KEY_ENV: &str = "GROWLERDB_RERANK_API_KEY";

/// The server-side source of outbound embedding/rerank provider API keys.
///
/// This is a zero-sized handle: it holds no key material itself. Each accessor re-reads the
/// process environment on call, so a rotated secret (see the module docs) takes effect
/// immediately and no long-lived copy of a key sits in memory beyond the call that uses it.
#[derive(Clone, Copy, Default)]
pub struct ProviderSecrets;

impl ProviderSecrets {
    /// A handle that reads keys from the process environment.
    pub fn from_env() -> Self {
        Self
    }

    /// The outbound **embedding** provider key from [`EMBEDDING_API_KEY_ENV`], or `None` when
    /// unset/blank. Re-read on every call (rotation-safe).
    pub fn embedding_key(&self) -> Option<String> {
        read_key(EMBEDDING_API_KEY_ENV)
    }

    /// The outbound **rerank** provider key from [`RERANK_API_KEY_ENV`], or `None` when
    /// unset/blank. Re-read on every call (rotation-safe).
    pub fn rerank_key(&self) -> Option<String> {
        read_key(RERANK_API_KEY_ENV)
    }
}

/// Read a key from `env_var`, trimming surrounding whitespace and treating blank as unset.
fn read_key(env_var: &str) -> Option<String> {
    std::env::var(env_var)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Render `key` for safe display: never the full secret. Shows a `***…last4` tail for a key long
/// enough that the last four characters don't reveal it, else a bare `***`. Use this anywhere a
/// key-derived string might reach a log or a human.
pub fn redact(key: &str) -> String {
    // Only expose a tail once there's enough entropy in front of it that four visible chars
    // can't reconstruct the secret; short keys are fully masked.
    if key.len() > 8 {
        format!("***{}", &key[key.len() - 4..])
    } else {
        "***".to_string()
    }
}

// A key must never leak through a derived Debug/Display (e.g. a struct that embeds this getting
// `{:?}`-logged). Both impls are intentionally content-free.
impl fmt::Debug for ProviderSecrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProviderSecrets(***)")
    }
}

impl fmt::Display for ProviderSecrets {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("***")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    // The env-mutation lock is crate-wide (`crate::env_guard`) so these tests serialize against the
    // env-touching tests in `external.rs` / `lib.rs` too — they share the same process-global vars.
    use crate::env_guard;

    #[test]
    fn debug_and_display_never_reveal_a_key() {
        let _g = env_guard();
        std::env::set_var(EMBEDDING_API_KEY_ENV, "sk-supersecret-embed-0001");
        let s = ProviderSecrets::from_env();

        let dbg = format!("{s:?}");
        let disp = format!("{s}");
        assert!(!dbg.contains("supersecret"), "Debug leaked the key: {dbg}");
        assert!(
            !disp.contains("supersecret"),
            "Display leaked the key: {disp}"
        );
        assert!(dbg.contains("***"));
        assert_eq!(disp, "***");

        std::env::remove_var(EMBEDDING_API_KEY_ENV);
    }

    #[test]
    fn redact_contains_no_full_key() {
        let key = "sk-supersecret-embed-0001";
        let r = redact(key);
        assert!(!r.contains(key), "redact leaked the full key: {r}");
        assert!(!r.contains("supersecret"), "redact leaked key body: {r}");
        // Long key → masked with a 4-char tail; the tail alone is not the key.
        assert_eq!(r, "***0001");
        // Short key → fully masked.
        assert_eq!(redact("sk-abc"), "***");
    }

    #[test]
    fn keys_read_from_env_and_reflect_rotation() {
        let _g = env_guard();
        std::env::remove_var(EMBEDDING_API_KEY_ENV);
        std::env::remove_var(RERANK_API_KEY_ENV);
        let s = ProviderSecrets::from_env();

        // Unset → None (fail closed at the call site).
        assert_eq!(s.embedding_key(), None);
        assert_eq!(s.rerank_key(), None);

        std::env::set_var(EMBEDDING_API_KEY_ENV, "  embed-key-v1  ");
        std::env::set_var(RERANK_API_KEY_ENV, "rerank-key-v1");
        // Trimmed on read.
        assert_eq!(s.embedding_key().as_deref(), Some("embed-key-v1"));
        assert_eq!(s.rerank_key().as_deref(), Some("rerank-key-v1"));

        // Rotate: the same handle re-reads env and sees the new value.
        std::env::set_var(EMBEDDING_API_KEY_ENV, "embed-key-v2");
        assert_eq!(s.embedding_key().as_deref(), Some("embed-key-v2"));

        // Blank is treated as unset.
        std::env::set_var(EMBEDDING_API_KEY_ENV, "   ");
        assert_eq!(s.embedding_key(), None);

        std::env::remove_var(EMBEDDING_API_KEY_ENV);
        std::env::remove_var(RERANK_API_KEY_ENV);
    }
}
