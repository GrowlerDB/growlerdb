//! **Scale-limit license.** An Ed25519-signed entitlement that unlocks node counts beyond the free
//! tier. Verified **offline** — no phone-home (see [D26](/system/decisions/d26-telemetry.md)) —
//! against a public key baked into the binary. Expiry is parsed but **not enforced** yet; see backlog
//! task-266 for expiry + pre-expiry notification/grace before turning it on.

use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

/// The free tier: a deployment with at most this many distinct live index nodes needs no license.
pub const FREE_NODE_LIMIT: usize = 3;

/// GrowlerDB LLC's license-signing **public** key (Ed25519). Licenses are minted with the matching
/// private key, held privately by GrowlerDB LLC.
///
/// **This is a placeholder — replace it with the real public key before issuing licenses.** The
/// private key must never live in this repository; the placeholder's private key was discarded, so no
/// license can validate against it (the free tier still works — a valid license is only needed to
/// exceed [`FREE_NODE_LIMIT`]).
const LICENSE_PUBLIC_KEY_PEM: &str = "-----BEGIN PUBLIC KEY-----\n\
MCowBQYDK2VwAyEABNG/3dMk6d+l0GbP8zXkDnqT1h8ZkfY1NnCTDSf6CfA=\n\
-----END PUBLIC KEY-----\n";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Claims {
    /// Who the license is issued to (shown in the console).
    licensee: String,
    /// Maximum distinct index nodes this license entitles.
    max_nodes: u32,
    /// Optional expiry (unix seconds). Parsed but NOT enforced yet — see task-266.
    #[serde(default)]
    exp: Option<i64>,
}

/// A verified license entitlement.
#[derive(Debug, Clone)]
pub struct License {
    pub licensee: String,
    pub max_nodes: u32,
    /// Present if the license carries an expiry. Not enforced yet (task-266).
    pub expires_at: Option<i64>,
}

/// A license that failed to verify (bad signature, wrong key, or malformed).
#[derive(Debug, thiserror::Error)]
pub enum LicenseError {
    #[error("invalid license: {0}")]
    Invalid(String),
}

impl License {
    /// Verify a license token against the embedded public key.
    pub fn verify(token: &str) -> Result<Self, LicenseError> {
        Self::verify_with_pem(token, LICENSE_PUBLIC_KEY_PEM)
    }

    fn verify_with_pem(token: &str, public_key_pem: &str) -> Result<Self, LicenseError> {
        let key = DecodingKey::from_ed_pem(public_key_pem.as_bytes())
            .map_err(|e| LicenseError::Invalid(format!("license public key: {e}")))?;
        let mut validation = Validation::new(Algorithm::EdDSA);
        // Expiry is deferred (task-266): accept a license with or without `exp`.
        validation.validate_exp = false;
        validation.required_spec_claims.clear();
        let data = decode::<Claims>(token, &key, &validation)
            .map_err(|e| LicenseError::Invalid(e.to_string()))?;
        Ok(License {
            licensee: data.claims.licensee,
            max_nodes: data.claims.max_nodes,
            expires_at: data.claims.exp,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};

    // A test-only keypair (NOT the production key). Used to sign licenses in tests.
    const TEST_PRIVATE_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MC4CAQAwBQYDK2VwBCIEIIG+6/KKq7NZAb3UoS5HnkwXyuL4R2q3fuSuAwz7jldP\n\
-----END PRIVATE KEY-----\n";
    const TEST_PUBLIC_PEM: &str = "-----BEGIN PUBLIC KEY-----\n\
MCowBQYDK2VwAyEAQzeuApLql4CLG7D9b86BdpwFU0w8MAf/JJVytr4KO7E=\n\
-----END PUBLIC KEY-----\n";

    fn sign(claims: serde_json::Value) -> String {
        let key = EncodingKey::from_ed_pem(TEST_PRIVATE_PEM.as_bytes()).unwrap();
        encode(&Header::new(Algorithm::EdDSA), &claims, &key).unwrap()
    }

    #[test]
    fn verifies_a_valid_license() {
        let token = sign(serde_json::json!({"licensee": "Acme Inc", "max_nodes": 12}));
        let lic = License::verify_with_pem(&token, TEST_PUBLIC_PEM).unwrap();
        assert_eq!(lic.licensee, "Acme Inc");
        assert_eq!(lic.max_nodes, 12);
        assert_eq!(lic.expires_at, None);
    }

    #[test]
    fn rejects_a_token_signed_by_the_wrong_key() {
        // Signed with the test key, verified against the embedded placeholder → reject (can't forge).
        let token = sign(serde_json::json!({"licensee": "Forger", "max_nodes": 9999}));
        assert!(License::verify(&token).is_err());
    }

    #[test]
    fn rejects_a_tampered_token() {
        let mut token = sign(serde_json::json!({"licensee": "Acme", "max_nodes": 3}));
        token.push('x');
        assert!(License::verify_with_pem(&token, TEST_PUBLIC_PEM).is_err());
    }
}
