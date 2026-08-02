//! Relay URL normalization, shared by the scraper and the relays report.

/// Canonicalize a relay URL, or reject it as un-scrapeable.
///
/// Rejects anything that cannot be reached from a server: non-websocket
/// schemes, `.onion` and `.local`, `localhost`, and bare IPs. Lowercases and
/// strips the trailing slash, query and fragment so the same relay advertised
/// three different ways counts once.
pub fn normalize_relay_url(raw: &str) -> Option<String> {
    let url = raw.trim().trim_end_matches('/');
    let lower = url.to_lowercase();
    if !(lower.starts_with("wss://") || lower.starts_with("ws://")) {
        return None;
    }
    let host = lower
        .split("://")
        .nth(1)?
        .split(['/', '?', '#'])
        .next()?
        .split(':')
        .next()?;
    if host.is_empty()
        || host.ends_with(".onion")
        || host.ends_with(".local")
        || host == "localhost"
        || host.parse::<std::net::IpAddr>().is_ok()
    {
        return None;
    }
    // Keep scheme + host + path, drop query/fragment noise.
    let scheme = lower.split("://").next()?;
    let rest = lower.split("://").nth(1)?.split(['?', '#']).next()?;
    Some(format!("{scheme}://{}", rest.trim_end_matches('/')))
}
