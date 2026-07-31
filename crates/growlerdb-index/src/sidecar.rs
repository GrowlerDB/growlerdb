//! Versioned framing for the cold-tier postcard sidecars. Postcard is **not** self-describing, so a
//! layout change to the [`HotCache`](crate::hotcache) or
//! [`BundleLayout`](crate::bundle::BundleLayout) would silently mis-parse an old sidecar. A 4-byte
//! magic + `u16` version lets a reader **detect** an incompatible sidecar and degrade deliberately
//! (hotcache falls back to read-through, bundle manifest surfaces a clear error).

use crate::store::{Result, StoreError};

/// Magic tag for a hotcache sidecar.
pub(crate) const HOTCACHE_MAGIC: [u8; 4] = *b"GDBh";
/// Magic tag for a bundle-layout manifest.
pub(crate) const BUNDLE_MAGIC: [u8; 4] = *b"GDBb";
/// Current sidecar format version. Bump on any incompatible layout change.
const VERSION: u16 = 1;

/// Frame a postcard `payload` with the magic + current version.
pub(crate) fn frame(magic: [u8; 4], payload: Vec<u8>) -> Vec<u8> {
    let mut out = Vec::with_capacity(6 + payload.len());
    out.extend_from_slice(&magic);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&payload);
    out
}

/// Verify a framed sidecar's magic + version and return its postcard payload. Errors (rather than
/// mis-parsing) on a wrong tag or unsupported version — including a pre-versioning sidecar, whose
/// bytes won't match the magic.
pub(crate) fn unframe(magic: [u8; 4], bytes: &[u8]) -> Result<&[u8]> {
    if bytes.len() < 6 || bytes[..4] != magic {
        return Err(StoreError::Cold(
            "unrecognized cold-tier sidecar (bad magic / pre-versioning format)".into(),
        ));
    }
    let ver = u16::from_le_bytes([bytes[4], bytes[5]]);
    if ver != VERSION {
        return Err(StoreError::Cold(format!(
            "unsupported cold-tier sidecar version {ver} (this build expects {VERSION})"
        )));
    }
    Ok(&bytes[6..])
}
