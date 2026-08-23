//! User-agent selection.
//!
//! Rotation is not an anonymity mechanism. It only mirrors the original
//! crawler's compatibility behavior for sites that reject uncommon clients.

use rand::prelude::IndexedRandom;

pub(crate) const OPEN_GRAPH_USER_AGENT: &str =
    "facebookexternalhit/1.1 (+http://www.facebook.com/externalhit_uatext.php)";

const USER_AGENTS: &[&str] = &[
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/131 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/131 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
];

pub(crate) fn choose(rotate: bool, fallback: &str) -> String {
    if !rotate {
        return fallback.to_owned();
    }

    // `choose` returns an Option because it also supports empty slices. Our
    // static list is non-empty, but keeping the fallback makes the invariant
    // explicit instead of relying on `unwrap`.
    USER_AGENTS
        .choose(&mut rand::rng())
        .copied()
        .unwrap_or(fallback)
        .to_owned()
}
