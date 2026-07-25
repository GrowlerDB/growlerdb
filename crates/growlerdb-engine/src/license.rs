//! **Scale-limit license.** An Ed25519-signed entitlement that unlocks node counts beyond the free
//! tier. Verified **offline** — no phone-home (see [D26](/system/decisions/d26-telemetry.md)) —
//! against a public key baked into the binary. Expiry is parsed but **not enforced** yet; see backlog
//! task-266 for expiry + pre-expiry notification/grace before turning it on.

use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use serde::{Deserialize, Serialize};

/// The free tier: a deployment with at most this many distinct live index nodes needs no license.
pub const FREE_NODE_LIMIT: usize = 3;

/// GrowlerDB LLC's license-signing **public** key (Ed25519) — the production key. Licenses are minted
/// offline with the matching **private** key, which is held privately by GrowlerDB LLC and never lives
/// in this repository; only this public half ships, to verify tokens at startup. The free tier works
/// without any license — a valid license is only needed to exceed [`FREE_NODE_LIMIT`]. Rotating the key
/// means minting against a new keypair and shipping its public half here.
const LICENSE_PUBLIC_KEY_PEM: &str = "-----BEGIN PUBLIC KEY-----\n\
MCowBQYDK2VwAyEAyizufq7Ro4/FzX0FhQqH2945GnMat7aYVr2Vf0kzGks=\n\
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

    /// Mint (sign) a license token with an Ed25519 **private-key** PEM — the inverse of [`verify`].
    /// Used only by the offline issuing ceremony (the `mint_license` example / the runbook in
    /// `COMM-LICENSE.md`): the private key is held by GrowlerDB LLC and **never** lives in this repo,
    /// so this is not a runtime path. The minted token is verified at startup against the embedded
    /// public key ([`LICENSE_PUBLIC_KEY_PEM`]) — the two must be the matching pair.
    pub fn mint(
        private_key_pem: &str,
        licensee: &str,
        max_nodes: u32,
        exp: Option<i64>,
    ) -> Result<String, LicenseError> {
        let claims = Claims {
            licensee: licensee.to_string(),
            max_nodes,
            exp,
        };
        let key = jsonwebtoken::EncodingKey::from_ed_pem(private_key_pem.as_bytes())
            .map_err(|e| LicenseError::Invalid(format!("license private key: {e}")))?;
        jsonwebtoken::encode(
            &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::EdDSA),
            &claims,
            &key,
        )
        .map_err(|e| LicenseError::Invalid(e.to_string()))
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
    fn embedded_public_key_is_well_formed() {
        // Guards against a paste error in LICENSE_PUBLIC_KEY_PEM: the shipped production key must be a
        // parseable Ed25519 SPKI PEM, else every real license silently falls back to the free tier.
        DecodingKey::from_ed_pem(LICENSE_PUBLIC_KEY_PEM.as_bytes())
            .expect("embedded license public key must be a valid Ed25519 SPKI PEM");
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
    fn mint_then_verify_roundtrips() {
        // The issuing ceremony: mint with the private key, verify with the matching public key.
        let token = License::mint(TEST_PRIVATE_PEM, "Scale Run", 64, None).unwrap();
        let lic = License::verify_with_pem(&token, TEST_PUBLIC_PEM).unwrap();
        assert_eq!(lic.licensee, "Scale Run");
        assert_eq!(lic.max_nodes, 64);
        assert_eq!(lic.expires_at, None);
    }

    #[test]
    fn mint_carries_expiry_and_rejects_a_bad_private_key() {
        let token = License::mint(TEST_PRIVATE_PEM, "Acme", 12, Some(4102444800)).unwrap();
        assert_eq!(
            License::verify_with_pem(&token, TEST_PUBLIC_PEM)
                .unwrap()
                .expires_at,
            Some(4102444800)
        );
        assert!(License::mint("not a pem", "Acme", 12, None).is_err());
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
