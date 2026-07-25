//! Verify a scale-limit license token against the **embedded production public key** — the self-serve
//! check that a minted token matches the key shipped in `license.rs`, without deploying a cluster.
//!
//! ```sh
//! # Round-trip: mint with the private key, verify against the shipped public key.
//! cargo run -p growlerdb-engine --example mint_license -- license_ed25519.pem "GrowlerDB scale" 64 \
//!   | cargo run -p growlerdb-engine --example verify_license
//! # -> "valid — licensee \"GrowlerDB scale\", node limit 64, expires None"
//! ```
//!
//! Reads the token from the first argument, or from stdin if none is given.
use std::io::Read;
use std::process::ExitCode;

fn main() -> ExitCode {
    let token = match std::env::args().nth(1) {
        Some(t) => t.trim().to_string(),
        None => {
            let mut s = String::new();
            if std::io::stdin().read_to_string(&mut s).is_err() {
                eprintln!("failed to read token from stdin");
                return ExitCode::FAILURE;
            }
            s.trim().to_string()
        }
    };
    if token.is_empty() {
        eprintln!("usage: verify_license <token>   (or pipe the token on stdin)");
        return ExitCode::from(2);
    }
    match growlerdb_engine::License::verify(&token) {
        Ok(lic) => {
            println!(
                "valid — licensee {:?}, node limit {}, expires {:?}",
                lic.licensee, lic.max_nodes, lic.expires_at
            );
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("INVALID: {e}");
            ExitCode::FAILURE
        }
    }
}
