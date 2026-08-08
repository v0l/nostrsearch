//! Relay URL normalization, shared by the scraper and the relays report.

/// Canonicalize a relay URL, or reject it as un-scrapeable.
///
/// Rejects anything that cannot be reached from a server: non-websocket
/// schemes, `.onion` and `.local`, `localhost`, and bare IPs. Lowercases and
/// strips the trailing slash, query and fragment so the same relay advertised
/// three different ways counts once.
/// Characters that end a relay URL only because something mangled it.
///
/// Seen on live entries with real advertiser counts: `relay.snort.social/,`
/// (1249), `purplerelay.com/,` (929), `relay.mostr.pub/%20` (661). These come
/// from clients that joined a list on a comma, or pasted a URL with trailing
/// whitespace, and each variant is stored as a relay distinct from the real
/// one -- carrying enough advertised weight to occupy a scrape slot forever.
const JUNK_TAIL: &[char] = &[
    ',', ';', '|', '"', '\'', '`', '<', '>', '(', ')', '[', ']', '{', '}', '\\', '!', '*',
];

/// Strip trailing separators, whitespace and percent-encoded whitespace.
///
/// Applied repeatedly because these arrive combined -- `/%20,` and `/,%20`
/// both occur -- and removing one can expose another.
fn trim_junk_tail(mut s: &str) -> &str {
    loop {
        let before = s;
        s = s.trim();
        s = s.trim_end_matches(JUNK_TAIL);
        for enc in ["%20", "%09", "%0a", "%0d", "%0A", "%0D", "+"] {
            s = s.trim_end_matches(enc);
        }
        s = s.trim_end_matches('/');
        if s == before {
            return s;
        }
    }
}

pub fn normalize_relay_url(raw: &str) -> Option<String> {
    let url = trim_junk_tail(raw);
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
    // Trim again after the query is dropped: `host/,?x=1` leaves `host/,`.
    let rest = trim_junk_tail(rest);
    if rest.is_empty() {
        return None;
    }
    Some(format!("{scheme}://{rest}"))
}

#[cfg(test)]
mod junk_tail_tests {
    use super::normalize_relay_url;

    /// Mangled URLs must collapse onto the relay they were meant to name.
    ///
    /// Every case here was taken from the live relay list, with its advertiser
    /// count. They are not rare typos: a client that joins a relay list on a
    /// comma, or pastes a trailing space, produces them at scale, and each
    /// variant was stored as a relay in its own right -- carrying enough
    /// weight to hold a scrape slot indefinitely while never being a relay.
    #[test]
    fn trailing_junk_collapses_onto_the_real_relay() {
        let real = Some("wss://relay.snort.social".to_string());
        for raw in [
            "wss://relay.snort.social/,",      // 1249 advertisers
            "wss://relay.snort.social/",
            "wss://relay.snort.social,",
            "wss://relay.snort.social/%20",
            "wss://relay.snort.social/ ",
            "wss://relay.snort.social/,%20",
            "wss://relay.snort.social/%20,",
            "wss://relay.snort.social|",
            "  wss://relay.snort.social  ",
            "wss://relay.snort.social/?x=1",
        ] {
            assert_eq!(normalize_relay_url(raw), real, "failed on {raw:?}");
        }

        assert_eq!(
            normalize_relay_url("wss://relay.mostr.pub/%20"),
            Some("wss://relay.mostr.pub".to_string()),
            "661 advertisers point at this one"
        );
        assert_eq!(
            normalize_relay_url("wss://purplerelay.com/,"),
            Some("wss://purplerelay.com".to_string()),
            "929 advertisers point at this one"
        );
    }

    /// Real paths must survive: they name distinct relays, and several have
    /// over a thousand advertisers each.
    #[test]
    fn real_paths_are_not_trimmed() {
        for (raw, want) in [
            ("wss://ditto.pub/relay", "wss://ditto.pub/relay"),
            ("wss://yabu.me/v2", "wss://yabu.me/v2"),
            ("wss://relay.getalby.com/v1", "wss://relay.getalby.com/v1"),
            ("wss://relay.minds.com/nostr/v1/ws", "wss://relay.minds.com/nostr/v1/ws"),
            ("wss://nostr.petrkr.net/strfry", "wss://nostr.petrkr.net/strfry"),
            ("wss://feeds.nostr.band/popular/", "wss://feeds.nostr.band/popular"),
        ] {
            assert_eq!(normalize_relay_url(raw), Some(want.to_string()), "on {raw:?}");
        }
    }

    /// A URL that is nothing but junk must be rejected, not normalized into a
    /// bare scheme.
    #[test]
    fn junk_only_urls_are_rejected() {
        assert_eq!(normalize_relay_url("wss://"), None);
        assert_eq!(normalize_relay_url("wss:///"), None);
        assert_eq!(normalize_relay_url("wss://,"), None);
        assert_eq!(normalize_relay_url("wss:// "), None);
    }
}
