//! Incremental report updates for a realtime dashboard.
//!
//! Full snapshots are fine to poll every ~30s, but they get large (the activity
//! report carries every day the corpus has ever seen) and they cannot show a
//! number *moving*. So every [`Analysis`](crate::Analysis) may additionally
//! emit **partial changes**: whatever it has touched since the last drain.
//!
//! The contract, deliberately kept small so each impl can choose its own
//! granularity:
//!
//! - A delta is a **JSON merge patch** ([RFC 7386]) over that analysis's own
//!   `snapshot()` shape. Apply it with [`merge_patch`] and you get exactly what
//!   the next full snapshot would have contained.
//! - Emitting a delta is optional. [`Analysis::drain_delta`] defaults to `None`,
//!   which simply means "poll the snapshot" for that analysis.
//! - Draining is destructive: an impl returns changes accumulated since the
//!   previous drain and resets its dirty set. Exactly one consumer should
//!   drain — the writer task — and it fans the result out to subscribers.
//! - Deltas are *not* a durable log. A client that misses frames re-syncs by
//!   fetching the full snapshot; `generated_at` tells it how stale that was.
//!
//! Because the built-in analyses are monotonic (counters only ever grow), no
//! delta ever needs merge-patch's `null`-means-delete form — but [`merge_patch`]
//! implements it so an analysis that prunes keys can say so.
//!
//! [RFC 7386]: https://www.rfc-editor.org/rfc/rfc7386
//! [`Analysis::drain_delta`]: crate::Analysis::drain_delta

use serde_json::Value;

/// Apply a JSON merge patch (RFC 7386) to `target` in place.
///
/// Objects merge key-by-key and recursively; every other type (including
/// arrays) replaces wholesale. A `null` value removes the key — which is why
/// analyses that can drop keys must emit them explicitly rather than just
/// omitting them.
pub fn merge_patch(target: &mut Value, patch: &Value) {
    match (target, patch) {
        (Value::Object(t), Value::Object(p)) => {
            for (k, v) in p {
                if v.is_null() {
                    t.remove(k);
                } else {
                    let slot = t.entry(k.clone()).or_insert(Value::Null);
                    merge_patch(slot, v);
                }
            }
        }
        (t, p) => *t = p.clone(),
    }
}

/// One analysis's partial update, as published to subscribers.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReportDelta {
    /// Analysis name (matches the `/reports/{name}` path segment).
    pub name: String,
    /// Merge patch over that report's snapshot.
    pub patch: Value,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn merges_nested_objects_without_clobbering_siblings() {
        let mut t = json!({"1700000000": {"kinds": {"1": {"trusted": 2}}, "zap_count": 1}});
        merge_patch(
            &mut t,
            &json!({"1700000000": {"kinds": {"7": {"trusted": 5}}}}),
        );

        // new key added, existing siblings preserved
        assert_eq!(t["1700000000"]["kinds"]["1"]["trusted"], 2);
        assert_eq!(t["1700000000"]["kinds"]["7"]["trusted"], 5);
        assert_eq!(t["1700000000"]["zap_count"], 1);
    }

    #[test]
    fn scalars_and_arrays_replace_wholesale() {
        let mut t = json!({"a": 1, "list": [1, 2, 3]});
        merge_patch(&mut t, &json!({"a": 9, "list": [4]}));
        assert_eq!(t["a"], 9);
        assert_eq!(t["list"], json!([4]));
    }

    #[test]
    fn null_removes_a_key() {
        let mut t = json!({"a": 1, "b": 2});
        merge_patch(&mut t, &json!({"b": null}));
        assert_eq!(t, json!({"a": 1}));
    }

    #[test]
    fn applying_a_delta_equals_the_next_full_snapshot() {
        // The property the whole design rests on.
        let mut held = json!({"snort": {"sum": 1}});
        merge_patch(
            &mut held,
            &json!({"snort": {"sum": 3}, "damus": {"sum": 2}}),
        );
        assert_eq!(held, json!({"snort": {"sum": 3}, "damus": {"sum": 2}}));
    }
}
