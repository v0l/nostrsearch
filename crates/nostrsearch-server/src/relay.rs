//! Nostr relay endpoint — absorbs nostrhole's relay role.
//!
//! Accepts inbound event writes over websocket and persists them into the same
//! `JsonFilesDatabase` archive the unified ingest writes, so events published
//! directly to us land in the corpus alongside firehose-collected ones.
//!
//! Ported from nostrhole's hyper handler: axum requests *are* hyper requests,
//! so we perform the websocket handshake manually and hand the raw upgraded IO
//! to `LocalRelay::take_connection`.
//!
//! Query policy mirrors nostrhole ([`NoQuery`]): this is an archival write
//! relay, not a queryable one — reads go through the search API. (Serving REQ
//! from the Tantivy index is the separate NIP-50 relay roadmap item.)

use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use nostr_relay_builder::builder::RateLimit;
use nostr_relay_builder::prelude::{Kind, PolicyResult, QueryPolicy, WritePolicy};
use nostr_relay_builder::{LocalRelay, RelayBuilder};
use nostr_sdk::prelude::BoxedFuture;
use nostr_sdk::{Event, Filter};
use std::collections::HashSet;
use std::net::SocketAddr;

/// Reject all queries — this relay exists to archive writes.
#[derive(Debug)]
pub struct NoQuery;

impl QueryPolicy for NoQuery {
    fn admit_query(&self, _q: &Filter, _addr: &SocketAddr) -> BoxedFuture<'_, PolicyResult> {
        Box::pin(
            async move { PolicyResult::Reject("queries not allowed; use /search".to_string()) },
        )
    }
}

/// Optional kind whitelist for accepted writes.
#[derive(Debug)]
pub struct KindPolicy(HashSet<Kind>);

impl KindPolicy {
    pub fn new(kinds: HashSet<Kind>) -> Self {
        Self(kinds)
    }
}

impl WritePolicy for KindPolicy {
    fn admit_event<'a>(
        &'a self,
        ev: &'a Event,
        _addr: &SocketAddr,
    ) -> BoxedFuture<'a, PolicyResult> {
        Box::pin(async move {
            if self.0.contains(&ev.kind) {
                PolicyResult::Accept
            } else {
                PolicyResult::Reject("kind not accepted".to_string())
            }
        })
    }
}

/// Shared relay handle.
#[derive(Clone)]
pub struct RelayState {
    pub relay: LocalRelay,
}

impl RelayState {
    /// Build a relay writing into the given archive database.
    ///
    /// In the unified node this is a [`NodeDb`](crate::node::NodeDb), which
    /// archives *and* forwards to the writer task so relay-published events
    /// become searchable.
    pub fn new<D>(db: D, kinds: Option<Vec<u16>>) -> Self
    where
        D: nostr_sdk::prelude::NostrDatabase + 'static,
    {
        let mut builder = RelayBuilder::default()
            .database(db)
            .query_policy(NoQuery)
            .rate_limit(RateLimit {
                max_reqs: 20,
                notes_per_minute: 100_000,
            });
        if let Some(k) = kinds {
            builder =
                builder.write_policy(KindPolicy::new(k.into_iter().map(Kind::Custom).collect()));
        }
        Self {
            relay: LocalRelay::new(builder),
        }
    }
}

/// RFC 6455 accept-key derivation.
fn derive_accept_key(key: &[u8]) -> String {
    use sha1::{Digest, Sha1};
    const WS_GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    let mut h = Sha1::new();
    h.update(key);
    h.update(WS_GUID);
    base64::engine::general_purpose::STANDARD.encode(h.finalize())
}

/// Root handler: completes a websocket handshake and hands the raw upgraded
/// connection to the relay.
///
/// A plain `GET /` is a browser rather than a nostr client, so it gets the
/// operator console instead of an error nobody can act on.
pub async fn ws_handler(
    State(st): State<RelayState>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    req: axum::extract::Request,
) -> Response {
    let is_upgrade = headers
        .get(header::CONNECTION)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_lowercase().contains("upgrade"))
        .unwrap_or(false)
        && headers
            .get(header::UPGRADE)
            .and_then(|v| v.to_str().ok())
            .map(|s| s.eq_ignore_ascii_case("websocket"))
            .unwrap_or(false);

    if !is_upgrade {
        return crate::dashboard::page().await;
    }

    let key = match headers.get("sec-websocket-key") {
        Some(k) => k.as_bytes().to_vec(),
        None => return (StatusCode::BAD_REQUEST, "missing sec-websocket-key").into_response(),
    };
    let accept = derive_accept_key(&key);

    let relay = st.relay.clone();
    tokio::spawn(async move {
        match hyper::upgrade::on(req).await {
            Ok(upgraded) => {
                let io = hyper_util::rt::TokioIo::new(upgraded);
                if let Err(e) = relay.take_connection(io, addr).await {
                    tracing::warn!(error = %e, "relay connection ended with error");
                }
            }
            Err(e) => tracing::warn!(error = %e, "websocket upgrade failed"),
        }
    });

    Response::builder()
        .status(StatusCode::SWITCHING_PROTOCOLS)
        .header(header::CONNECTION, "upgrade")
        .header(header::UPGRADE, "websocket")
        .header("sec-websocket-accept", accept)
        .body(axum::body::Body::empty())
        .unwrap()
}
