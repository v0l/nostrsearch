//! Daily activity: per-day event counts by kind plus **zap volume in sats**,
//! each split trusted/untrusted.
//!
//! Ported from nostr-dashboard's `reports/activity.rs`. Upstream hand-rolled a
//! bolt11 amount parser that was wrong in three ways: every multiplier was
//! 1000× too small (then divided by 1000 again), `lnbc` was stripped before
//! `lnbcrt` so regtest invoices never parsed, and splitting the HRP on `p`
//! confused the pico multiplier with the bech32 separator. We use the
//! `lightning-invoice` crate (rust-lightning's BOLT-11 implementation) instead
//! — it validates the bech32 checksum and handles the HRP grammar properly.
//!
//! Amounts are resolved zap-request-first (the `description` tag's `amount`
//! tag, which is the *requested* amount) and fall back to parsing the `bolt11`
//! invoice on the receipt.

use super::counter::TrustedCount;
use crate::types::Pubkey;
use crate::{Analysis, AnalysisCtx};
use lightning_invoice::SignedRawBolt11Invoice;
use nostrsearch_core::event::NostrEvent;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

const DAY: u64 = 60 * 60 * 24;
const ZAP_RECEIPT: u16 = 9735;

/// One UTC day bucket.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DailyActivity {
    /// Zap value that day in **sats**, split by whether the *sender* is
    /// trusted. This is the "is this real volume or bot noise" signal.
    pub zaps_sent_sats: TrustedCount,
    /// The same value, split by whether the *recipient* is trusted.
    pub zaps_received_sats: TrustedCount,
    /// Number of zap receipts that resolved to an amount.
    pub zap_count: u64,
    /// Event count per kind, split by the trust of the event's **author**.
    ///
    /// Note kind 9735 receipts are authored by the recipient's LNURL server,
    /// not by a human, so that row's split reflects zapper services rather
    /// than users. Use the `zaps_*_sats` counters for economic attribution.
    pub kinds: HashMap<u16, TrustedCount>,
}

impl DailyActivity {
    /// Total events recorded in this bucket.
    pub fn events(&self) -> u64 {
        self.kinds.values().map(TrustedCount::total).sum()
    }

    /// Total zap value in sats (sender and recipient views sum alike).
    pub fn zaps_sats(&self) -> u64 {
        self.zaps_sent_sats.total()
    }

    fn merge(&mut self, other: Self) {
        self.zaps_sent_sats.merge(other.zaps_sent_sats);
        self.zaps_received_sats.merge(other.zaps_received_sats);
        self.zap_count += other.zap_count;
        for (kind, oc) in other.kinds {
            self.kinds.entry(kind).or_default().merge(oc);
        }
    }
}

/// Per-day activity + zap volume.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Activity {
    days: HashMap<u64, DailyActivity>,
    /// Day buckets touched since the last [`Analysis::drain_delta`]. Skipped
    /// during (de)serialization: it is realtime-only state, meaningless in a
    /// checkpoint, and a restored analysis simply starts with nothing pending.
    #[serde(skip)]
    dirty: HashSet<u64>,
}

impl Activity {
    /// Read one day bucket (day-boundary unix timestamp).
    pub fn day(&self, day_ts: u64) -> Option<&DailyActivity> {
        self.days.get(&day_ts)
    }
}

/// Amount encoded in a bolt11 invoice, in **millisats**.
///
/// Returns `None` for amount-less ("any amount") invoices and for anything that
/// fails to parse. We stop at [`SignedRawBolt11Invoice`] rather than going all
/// the way to `Bolt11Invoice`, which skips secp256k1 signature recovery — the
/// archive is replaying hundreds of millions of events and we only need the
/// human-readable amount, not payee authentication.
pub fn parse_invoice_msats(bolt11: &str) -> Option<u64> {
    let signed = SignedRawBolt11Invoice::from_str(bolt11.trim()).ok()?;
    // BOLT-11 encodes the amount in pico-BTC; 1 msat = 10 pico-BTC.
    signed.raw_invoice().amount_pico_btc().map(|pico| pico / 10)
}

/// The zap request (kind 9734) embedded in a receipt's `description` tag.
fn zap_request(ev: &NostrEvent) -> Option<serde_json::Value> {
    serde_json::from_str(ev.tag_values("description").next()?).ok()
}

/// Millisats requested by the zap request's `amount` tag.
fn request_msats(req: &serde_json::Value) -> Option<u64> {
    req.get("tags")?
        .as_array()?
        .iter()
        .find(|t| t.get(0).and_then(|v| v.as_str()) == Some("amount"))
        .and_then(|t| t.get(1))
        .and_then(|v| v.as_str())
        .and_then(|v| v.parse::<u64>().ok())
}

/// Who sent and who received a zap, per NIP-57.
///
/// A kind-9735 receipt is signed by the recipient's LNURL server, so
/// `ev.pubkey` identifies a *zapper service*, never a participant. The real
/// parties are the `P` tag (sender, optional) or the embedded zap request's
/// author, and the `p` tag (recipient).
fn zap_parties(
    ev: &NostrEvent,
    req: Option<&serde_json::Value>,
) -> (Option<Pubkey>, Option<Pubkey>) {
    let sender = ev
        .tag_values("P")
        .next()
        .and_then(Pubkey::from_hex)
        .or_else(|| {
            req.and_then(|r| r.get("pubkey"))
                .and_then(|v| v.as_str())
                .and_then(Pubkey::from_hex)
        });
    let recipient = ev
        .tag_values("p")
        .next()
        .and_then(Pubkey::from_hex)
        .or_else(|| {
            // Fall back to the request's own `p` tag.
            req.and_then(|r| r.get("tags"))
                .and_then(|v| v.as_array())
                .and_then(|tags| {
                    tags.iter()
                        .find(|t| t.get(0).and_then(|v| v.as_str()) == Some("p"))
                        .and_then(|t| t.get(1))
                        .and_then(|v| v.as_str())
                })
                .and_then(Pubkey::from_hex)
        });
    (sender, recipient)
}

impl Analysis for Activity {
    type Output = HashMap<u64, DailyActivity>;

    fn name(&self) -> &'static str {
        "activity"
    }

    fn deps(&self) -> &'static [&'static str] {
        &["follow_graph"]
    }

    fn observe(&mut self, ev: &NostrEvent, ctx: &AnalysisCtx) -> bool {
        let day = ev.created_at - (ev.created_at % DAY);
        let trusted = ctx.author_trusted();
        self.dirty.insert(day);
        let bucket = self.days.entry(day).or_default();

        bucket.kinds.entry(ev.kind).or_default().incr(trusted, 1);

        if ev.kind == ZAP_RECEIPT {
            let req = zap_request(ev);
            // The receipt's bolt11 is the invoice that was actually paid, so it
            // wins over the request's `amount`, which is only what was asked
            // for. (Upstream preferred the request.)
            let msats = ev
                .tag_values("bolt11")
                .next()
                .and_then(parse_invoice_msats)
                .or_else(|| req.as_ref().and_then(request_msats));

            if let Some(msats) = msats {
                let sats = msats / 1000;
                let (sender, recipient) = zap_parties(ev, req.as_ref());
                // Unknown party => attribute to the untrusted side rather than
                // silently crediting trust we cannot verify.
                bucket
                    .zaps_sent_sats
                    .incr(sender.is_some_and(|pk| ctx.is_trusted(&pk)), sats);
                bucket
                    .zaps_received_sats
                    .incr(recipient.is_some_and(|pk| ctx.is_trusted(&pk)), sats);
                bucket.zap_count += 1;
            }
        }
        true
    }

    fn merge(&mut self, other: Self) {
        for (day, ob) in other.days {
            self.days.entry(day).or_default().merge(ob);
            self.dirty.insert(day);
        }
    }

    fn snapshot(&self) -> Self::Output {
        self.days.clone()
    }

    /// Emits only the day buckets touched since the last drain — in practice
    /// today's, so a live dashboard receives a few hundred bytes per tick
    /// instead of the entire multi-year history.
    fn drain_delta(&mut self) -> Option<serde_json::Value> {
        if self.dirty.is_empty() {
            return None;
        }
        let patch: serde_json::Map<String, serde_json::Value> = self
            .dirty
            .drain()
            .filter_map(|day| {
                let bucket = self.days.get(&day)?;
                Some((day.to_string(), serde_json::to_value(bucket).ok()?))
            })
            .collect();
        Some(serde_json::Value::Object(patch))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Pubkey;

    fn ev(kind: u16, created_at: u64, tags: Vec<Vec<&str>>) -> NostrEvent {
        NostrEvent {
            id: "a".repeat(64),
            pubkey: "b".repeat(64),
            created_at,
            kind,
            tags: tags
                .into_iter()
                .map(|t| t.into_iter().map(String::from).collect())
                .collect(),
            content: String::new(),
            sig: "c".repeat(128),
        }
    }

    // BOLT-11 spec test vectors (bolts/11-payment-encoding.md).
    const INV_2500U: &str = "lnbc2500u1pvjluezsp5zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zygspp5qqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqypqdq5xysxxatsyp3k7enxv4jsxqzpu9qrsgquk0rl77nj30yxdy8j9vdx85fkpmdla2087ne0xh8nhedh8w27kyke0lp53ut353s06fv3qfegext0eh0ymjpf39tuven09sam30g4vgpfna3rh";
    const INV_20M: &str = "lnbc20m1pvjluezsp5zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zygspp5qqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqypqhp58yjmdan79s6qqdhdzgynm4zwqd5d7xmw5fk98klysy043l2ahrqs9qrsgq7ea976txfraylvgzuxs8kgcw23ezlrszfnh8r6qtfpr6cxga50aj6txm9rxrydzd06dfeawfk6swupvz4erwnyutnjq7x39ymw6j38gp7ynn44";
    const INV_TESTNET_20M: &str = "lntb20m1pvjluezsp5zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zygshp58yjmdan79s6qqdhdzgynm4zwqd5d7xmw5fk98klysy043l2ahrqspp5qqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqypqfpp3x9et2e20v6pu37c5d9vax37wxq72un989qrsgqdj545axuxtnfemtpwkc45hx9d2ft7x04mt8q7y6t0k2dge9e7h8kpy9p34ytyslj3yu569aalz2xdk8xkd7ltxqld94u8h2esmsmacgpghe9k8";
    const INV_NO_AMOUNT: &str = "lnbc1pvjluezsp5zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zyg3zygspp5qqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqqqsyqcyq5rqwzqfqypqdpl2pkx2ctnv5sxxmmwwd5kgetjypeh2ursdae8g6twvus8g6rfwvs8qun0dfjkxaq9qrsgq357wnc5r2ueh7ck6q93dj32dlqnls087fxdwk8qakdyafkq3yap9us6v52vjjsrvywa6rt52cm9r9zqt8r2t7mlcwspyetp5h2tztugp9lfyql";

    #[test]
    fn parses_spec_vectors_as_msats() {
        // 2500u = 0.0025 BTC = 250,000 sats = 250,000,000 msat
        assert_eq!(parse_invoice_msats(INV_2500U), Some(250_000_000));
        // 20m = 0.02 BTC = 2,000,000 sats
        assert_eq!(parse_invoice_msats(INV_20M), Some(2_000_000_000));
        // testnet parses too
        assert_eq!(parse_invoice_msats(INV_TESTNET_20M), Some(2_000_000_000));
        // amount-less ("any amount") invoice
        assert_eq!(parse_invoice_msats(INV_NO_AMOUNT), None);
        // uppercase (QR form) and surrounding whitespace still parse
        assert_eq!(
            parse_invoice_msats(&format!("  {}  ", INV_2500U.to_uppercase())),
            Some(250_000_000)
        );
        // garbage / bad checksum are rejected rather than silently mis-parsed
        assert_eq!(parse_invoice_msats("lnbc2500u1pvjluezpp5qqqsyq"), None);
        assert_eq!(parse_invoice_msats("not-an-invoice"), None);
        assert_eq!(parse_invoice_msats(""), None);
    }

    #[test]
    fn bolt11_wins_over_the_requested_amount() {
        let world = crate::World::new();
        let ctx = AnalysisCtx::new(
            1_700_000_100,
            Pubkey::from_hex(&"b".repeat(64)).unwrap(),
            Pubkey::ZERO,
            &world,
        );

        // Request asks for 21 sats; the invoice actually paid is 250,000 sats.
        let req = r#"{"kind":9734,"tags":[["amount","21000"]]}"#;
        let mut a = Activity::default();
        a.observe(
            &ev(
                9735,
                1_700_000_000,
                vec![vec!["description", req], vec!["bolt11", INV_2500U]],
            ),
            &ctx,
        );

        let day = 1_700_000_000 - (1_700_000_000 % DAY);
        assert_eq!(a.snapshot()[&day].zaps_sats(), 250_000);

        // With no invoice, the requested amount is the fallback.
        let mut b = Activity::default();
        b.observe(
            &ev(9735, 1_700_000_000, vec![vec!["description", req]]),
            &ctx,
        );
        assert_eq!(b.snapshot()[&day].zaps_sats(), 21);
    }

    /// A zap receipt is signed by the recipient's LNURL server, so trust must
    /// come from the `P`/request sender and the `p` recipient — never the
    /// receipt's own author.
    #[test]
    fn zap_trust_follows_sender_and_recipient_not_the_lnurl_server() {
        let sender = "a".repeat(64);
        let recipient = "b".repeat(64);
        let lnurl_server = "c".repeat(64);

        let mut world = crate::World::new();
        world.set_wot_tier(Pubkey::from_hex(&sender).unwrap(), 3);
        // recipient and the LNURL server are both untrusted

        let mut receipt = ev(
            9735,
            1_700_000_000,
            vec![
                vec!["P", &sender],
                vec!["p", &recipient],
                vec!["bolt11", INV_2500U],
            ],
        );
        receipt.pubkey = lnurl_server.clone();

        let ctx = AnalysisCtx::new(
            1_700_000_100,
            Pubkey::from_hex(&lnurl_server).unwrap(),
            Pubkey::ZERO,
            &world,
        );

        let mut a = Activity::default();
        a.observe(&receipt, &ctx);

        let day = 1_700_000_000 - (1_700_000_000 % DAY);
        let d = &a.snapshot()[&day];
        // sent by a trusted pubkey...
        assert_eq!(d.zaps_sent_sats.trusted, 250_000);
        assert_eq!(d.zaps_sent_sats.untrusted, 0);
        // ...to an untrusted one
        assert_eq!(d.zaps_received_sats.trusted, 0);
        assert_eq!(d.zaps_received_sats.untrusted, 250_000);
        assert_eq!(d.zap_count, 1);
    }

    #[test]
    fn sender_falls_back_to_the_zap_request_author() {
        let sender = "a".repeat(64);
        let mut world = crate::World::new();
        world.set_wot_tier(Pubkey::from_hex(&sender).unwrap(), 2);

        // No `P` tag: the sender is the zap request's author.
        let req = format!(r#"{{"kind":9734,"pubkey":"{sender}","tags":[]}}"#);
        let receipt = ev(
            9735,
            1_700_000_000,
            vec![vec!["description", &req], vec!["bolt11", INV_2500U]],
        );

        let ctx = AnalysisCtx::new(1_700_000_100, Pubkey::ZERO, Pubkey::ZERO, &world);
        let mut a = Activity::default();
        a.observe(&receipt, &ctx);

        let day = 1_700_000_000 - (1_700_000_000 % DAY);
        assert_eq!(a.snapshot()[&day].zaps_sent_sats.trusted, 250_000);
    }

    #[test]
    fn falls_back_to_bolt11_and_merges() {
        let world = crate::World::new();
        let ctx = AnalysisCtx::new(
            1_700_000_100,
            Pubkey::from_hex(&"b".repeat(64)).unwrap(),
            Pubkey::ZERO,
            &world,
        );
        let day = 1_700_000_000 - (1_700_000_000 % DAY);

        let mut a = Activity::default();
        a.observe(
            &ev(9735, 1_700_000_000, vec![vec!["bolt11", INV_2500U]]),
            &ctx,
        );
        a.observe(&ev(1, 1_700_000_000, vec![]), &ctx);

        let mut b = Activity::default();
        b.observe(&ev(1, 1_700_000_000, vec![]), &ctx);

        a.merge(b);
        let out = a.snapshot();
        assert_eq!(out[&day].zaps_sats(), 250_000);
        assert_eq!(out[&day].kinds[&1].untrusted, 2);
        assert_eq!(out[&day].events(), 3);
    }
}
