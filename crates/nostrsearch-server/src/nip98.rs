use axum::http::{HeaderMap, Method, Uri, header::AUTHORIZATION};
use base64::Engine;
use nostr_sdk::Event;
use nostr_sdk::prelude::JsonUtil;

pub const MAX_AGE_SECS: u64 = 60;
pub const HTTP_AUTH_KIND: u16 = 27235;

pub fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

pub fn verify(
    headers: &HeaderMap,
    method: &Method,
    original: &Uri,
    origin: Option<&str>,
) -> Result<Event, &'static str> {
    let header = headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or("missing Authorization: Nostr <base64 event>")?;

    let b64 = header
        .strip_prefix("Nostr ")
        .or_else(|| header.strip_prefix("nostr "))
        .ok_or("expected the Nostr auth scheme")?;

    let bytes = base64::engine::general_purpose::STANDARD
        .decode(b64.trim())
        .map_err(|_| "auth event is not valid base64")?;
    let json = String::from_utf8(bytes).map_err(|_| "auth event is not valid utf-8")?;
    let event = Event::from_json(&json).map_err(|_| "auth event is not a valid nostr event")?;

    if event.verify().is_err() {
        return Err("bad signature");
    }
    if event.kind.as_u16() != HTTP_AUTH_KIND {
        return Err("auth event must be kind 27235");
    }

    if event.created_at.as_secs().abs_diff(unix_now()) > MAX_AGE_SECS {
        return Err("auth event timestamp is outside the allowed window");
    }

    let tag = |name: &str| {
        event
            .tags
            .iter()
            .map(|t| t.clone().to_vec())
            .find(|t| t.first().map(String::as_str) == Some(name))
            .and_then(|t| t.get(1).cloned())
    };

    let u = tag("u").ok_or("auth event is missing the u tag")?;
    let signed_method = tag("method").ok_or("auth event is missing the method tag")?;
    if !signed_method.eq_ignore_ascii_case(method.as_str()) {
        return Err("auth event method does not match the request");
    }
    if !url_matches(&u, original, origin) {
        return Err("auth event u tag does not match this URL");
    }
    Ok(event)
}

pub fn url_matches(signed: &str, request: &Uri, origin: Option<&str>) -> bool {
    let Ok(parsed) = url::Url::parse(signed) else {
        return false;
    };
    if parsed.path() != request.path() {
        return false;
    }
    let mut signed_q: Vec<(String, String)> = parsed.query_pairs().into_owned().collect();
    let mut request_q: Vec<(String, String)> =
        url::form_urlencoded::parse(request.query().unwrap_or("").as_bytes())
            .into_owned()
            .collect();
    signed_q.sort();
    request_q.sort();
    if signed_q != request_q {
        return false;
    }
    match origin.map(url::Url::parse) {
        None => true,
        Some(Ok(o)) => parsed.origin() == o.origin(),
        Some(Err(_)) => false,
    }
}
