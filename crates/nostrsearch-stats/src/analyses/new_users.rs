use super::active_users::month_start;
use crate::types::Pubkey;
use crate::{Analysis, AnalysisCtx, AttachCtx};
use nostrsearch_core::event::NostrEvent;
use rocksdb::{DB, Options};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

const DAY: u64 = 60 * 60 * 24;
const STORE_DIR: &str = "first_seen";

pub struct FirstSeen {
    db: DB,
}

impl std::fmt::Debug for FirstSeen {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("FirstSeen")
    }
}

impl FirstSeen {
    pub fn open(path: impl AsRef<std::path::Path>) -> anyhow::Result<Self> {
        let mut bb = rocksdb::BlockBasedOptions::default();
        bb.set_block_cache(&rocksdb::Cache::new_lru_cache(128 * 1024 * 1024));
        bb.set_cache_index_and_filter_blocks(true);
        bb.set_bloom_filter(10.0, false);
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_block_based_table_factory(&bb);
        opts.set_write_buffer_size(32 * 1024 * 1024);
        opts.set_max_write_buffer_number(2);
        opts.set_max_open_files(128);
        opts.set_compression_type(rocksdb::DBCompressionType::None);
        Ok(Self {
            db: DB::open(&opts, path.as_ref())?,
        })
    }

    pub fn get(&self, pk: &Pubkey) -> Option<u64> {
        let raw = self.db.get_pinned(pk.as_bytes()).ok()??;
        let bytes: [u8; 8] = raw.as_ref().try_into().ok()?;
        Some(u64::from_le_bytes(bytes))
    }

    pub fn put(&self, pk: &Pubkey, ts: u64) {
        if let Err(e) = self.db.put(pk.as_bytes(), ts.to_le_bytes()) {
            tracing::warn!(error = %e, "first-seen write failed");
        }
    }

    pub fn clear(&self) {
        let mut batch = rocksdb::WriteBatch::default();
        for (k, _) in self.db.iterator(rocksdb::IteratorMode::Start).flatten() {
            batch.delete(&k);
            if batch.len() >= 10_000 {
                let _ = self.db.write(std::mem::take(&mut batch));
            }
        }
        let _ = self.db.write(batch);
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NewUsersReport {
    pub daily: BTreeMap<u64, u64>,
    pub monthly: BTreeMap<u64, u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NewUsers {
    daily: BTreeMap<u64, u64>,
    monthly: BTreeMap<u64, u64>,
    #[serde(skip)]
    store: Option<Arc<FirstSeen>>,
    #[serde(skip)]
    dirty_daily: HashSet<u64>,
    #[serde(skip)]
    dirty_monthly: HashSet<u64>,
}

impl NewUsers {
    fn shift(&mut self, ts: u64, delta: i64) {
        let day = ts - (ts % DAY);
        let month = month_start(ts);
        for (map, key, dirty) in [
            (&mut self.daily, day, &mut self.dirty_daily),
            (&mut self.monthly, month, &mut self.dirty_monthly),
        ] {
            let slot = map.entry(key).or_default();
            *slot = (*slot as i64 + delta).max(0) as u64;
            if *slot == 0 {
                map.remove(&key);
            }
            dirty.insert(key);
        }
    }
}

impl Analysis for NewUsers {
    type Output = NewUsersReport;

    fn name(&self) -> &'static str {
        "new_users"
    }

    fn attach(&mut self, ctx: &AttachCtx) -> anyhow::Result<()> {
        if self.store.is_none() {
            self.store = Some(Arc::new(FirstSeen::open(ctx.dir.join(STORE_DIR))?));
        }
        Ok(())
    }

    fn on_reset(&mut self) {
        if let Some(store) = &self.store {
            store.clear();
        }
    }

    fn observe(&mut self, ev: &NostrEvent, ctx: &AnalysisCtx) -> bool {
        if ev.uses_throwaway_key() {
            return false;
        }
        let Some(store) = self.store.clone() else {
            return false;
        };
        let ts = ev.created_at;
        match store.get(&ctx.author) {
            Some(prev) if prev <= ts => false,
            Some(prev) => {
                self.shift(prev, -1);
                self.shift(ts, 1);
                store.put(&ctx.author, ts);
                true
            }
            None => {
                self.shift(ts, 1);
                store.put(&ctx.author, ts);
                true
            }
        }
    }

    fn merge(&mut self, other: Self) {
        for (k, v) in other.daily {
            *self.daily.entry(k).or_default() += v;
            self.dirty_daily.insert(k);
        }
        for (k, v) in other.monthly {
            *self.monthly.entry(k).or_default() += v;
            self.dirty_monthly.insert(k);
        }
    }

    fn snapshot(&self) -> Self::Output {
        NewUsersReport {
            daily: self.daily.clone(),
            monthly: self.monthly.clone(),
        }
    }

    fn drain_delta(&mut self) -> Option<serde_json::Value> {
        if self.dirty_daily.is_empty() && self.dirty_monthly.is_empty() {
            return None;
        }
        let side = |dirty: &mut HashSet<u64>, map: &BTreeMap<u64, u64>| {
            dirty
                .drain()
                .map(|k| {
                    let v = map
                        .get(&k)
                        .map(|n| serde_json::Value::from(*n))
                        .unwrap_or(serde_json::Value::Null);
                    (k.to_string(), v)
                })
                .collect::<serde_json::Map<_, _>>()
        };
        let daily = side(&mut self.dirty_daily, &self.daily);
        let monthly = side(&mut self.dirty_monthly, &self.monthly);
        Some(serde_json::json!({ "daily": daily, "monthly": monthly }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::graph::GraphStore;

    fn tmp() -> std::path::PathBuf {
        let p = std::env::temp_dir().join(format!(
            "ns-newusers-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    fn attached(dir: &std::path::Path) -> NewUsers {
        let graph = Arc::new(GraphStore::open(dir.join("graph")).unwrap());
        let mut n = NewUsers::default();
        n.attach(&AttachCtx {
            graph,
            dir: dir.to_path_buf(),
        })
        .unwrap();
        n
    }

    fn ev(created_at: u64, kind: u16) -> NostrEvent {
        NostrEvent {
            id: "a".repeat(64),
            pubkey: "b".repeat(64),
            created_at,
            kind,
            tags: vec![],
            content: String::new(),
            sig: "c".repeat(128),
        }
    }

    fn ctx(world: &crate::World, seed: u8) -> AnalysisCtx<'_> {
        AnalysisCtx::new(0, crate::types::Hash32([seed; 32]), Pubkey::ZERO, world)
    }

    #[test]
    fn a_user_counts_once_on_the_earliest_day_seen_even_out_of_order() {
        let dir = tmp();
        let world = crate::World::new();
        let mut n = attached(&dir);
        let d1 = 1_790_000_000 - (1_790_000_000 % DAY);
        let d5 = d1 + 4 * DAY;

        assert!(n.observe(&ev(d5, 1), &ctx(&world, 1)));
        assert!(!n.observe(&ev(d5 + 10, 1), &ctx(&world, 1)));
        assert!(n.observe(&ev(d1, 7), &ctx(&world, 1)), "an older event moves the first-seen day");
        assert!(n.observe(&ev(d5, 1), &ctx(&world, 2)));

        let out = n.snapshot();
        assert_eq!(out.daily.get(&d1), Some(&1));
        assert_eq!(out.daily.get(&d5), Some(&1), "only user 2 is new on day 5");
        assert_eq!(out.monthly.values().sum::<u64>(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn throwaway_keys_are_never_new_users() {
        let dir = tmp();
        let world = crate::World::new();
        let mut n = attached(&dir);
        for (seed, kind) in [(1u8, 1059u16), (2, 445), (3, 24133), (4, 22_242)] {
            assert!(!n.observe(&ev(1_790_000_000, kind), &ctx(&world, seed)));
        }
        assert!(n.snapshot().daily.is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn first_seen_survives_a_restart_so_old_users_are_not_new_again() {
        let dir = tmp();
        let world = crate::World::new();
        let t = 1_790_000_000;
        let saved = {
            let mut n = attached(&dir);
            n.observe(&ev(t, 1), &ctx(&world, 1));
            bincode::serialize(&n).unwrap()
        };
        let mut n: NewUsers = bincode::deserialize(&saved).unwrap();
        n.attach(&AttachCtx {
            graph: Arc::new(GraphStore::open(dir.join("graph2")).unwrap()),
            dir: dir.clone(),
        })
        .unwrap();
        assert!(!n.observe(&ev(t + DAY, 1), &ctx(&world, 1)));
        assert_eq!(n.snapshot().daily.values().sum::<u64>(), 1);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
