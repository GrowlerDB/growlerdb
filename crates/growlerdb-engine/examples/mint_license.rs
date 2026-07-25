//! Offline license-minting helper — the issuing **ceremony**, not a runtime path.
//!
//! Signs a scale-limit license token with an Ed25519 **private key** that GrowlerDB LLC holds
//! privately (never in this repo). The matching public key is embedded in
//! `growlerdb-engine/src/license.rs` (`LICENSE_PUBLIC_KEY_PEM`) and verifies the token at startup.
//!
//! ```sh
//! # 1. Generate the signing keypair ONCE (keep the private key secret; commit only the public key):
//! openssl genpkey -algorithm ed25519 -out license_ed25519.pem
//! openssl pkey -in license_ed25519.pem -pubout -out license_ed25519.pub.pem   # -> LICENSE_PUBLIC_KEY_PEM
//!
//! # 2. Mint a license (e.g. 64 nodes, no expiry) — prints the JWT to stdout:
//! cargo run -p growlerdb-engine --example mint_license -- license_ed25519.pem "GrowlerDB scale runs" 64
//!
//! # 3. Deploy it: set the printed token as credentials.license (see COMM-LICENSE.md).
//! ```
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 || args.len() > 5 {
        eprintln!(
            "usage: mint_license <private_key.pem> <licensee> <max_nodes> [exp_unix_seconds]\n\
             \n\
             Signs a scale-limit license (the offline issuing ceremony). The private key is held by\n\
             GrowlerDB LLC and never lives in the repo; the matching public key is embedded in\n\
             growlerdb-engine/src/license.rs. Prints the license token to stdout."
        );
        return ExitCode::from(2);
    }
    let pem = match std::fs::read_to_string(&args[1]) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("cannot read private key {}: {e}", args[1]);
            return ExitCode::FAILURE;
        }
    };
    let licensee = &args[2];
    let max_nodes: u32 = match args[3].parse() {
        Ok(n) => n,
        Err(_) => {
            eprintln!(
                "max_nodes must be a non-negative integer, got {:?}",
                args[3]
            );
            return ExitCode::from(2);
        }
    };
    let exp: Option<i64> = match args.get(4).map(|s| s.parse::<i64>()) {
        None => None,
        Some(Ok(n)) => Some(n),
        Some(Err(_)) => {
            eprintln!("exp_unix_seconds must be an integer");
            return ExitCode::from(2);
        }
    };
    match growlerdb_engine::License::mint(&pem, licensee, max_nodes, exp) {
        Ok(token) => {
            println!("{token}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("mint failed: {e}");
            ExitCode::FAILURE
        }
    }
}
