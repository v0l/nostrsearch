use rocksdb::{BlockBasedOptions, Cache, DB, Options};
use std::time::Instant;

struct Rng(u64);

impl Rng {
    fn key(&mut self) -> [u8; 32] {
        let mut k = [0u8; 32];
        for chunk in k.chunks_mut(8) {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            chunk.copy_from_slice(&self.0.to_le_bytes());
        }
        k
    }
}

fn present_keys(db: &DB, rng: &mut Rng, n: usize) -> Vec<Vec<u8>> {
    let mut it = db.raw_iterator();
    let mut out = Vec::with_capacity(n);
    while out.len() < n {
        it.seek(rng.key());
        if let Some(key) = it.key() {
            out.push(key.to_vec());
        }
    }
    out
}

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args.next().expect("dedupe path");
    let n: usize = args.next().map(|v| v.parse().unwrap()).unwrap_or(3000);
    let batch: usize = args.next().map(|v| v.parse().unwrap()).unwrap_or(1000);
    let cache_mb: usize = args.next().map(|v| v.parse().unwrap()).unwrap_or(64);
    let warm: usize = args.next().map(|v| v.parse().unwrap()).unwrap_or(3000);

    let mut bb = BlockBasedOptions::default();
    bb.set_bloom_filter(10.0, false);
    bb.set_block_cache(&Cache::new_lru_cache(cache_mb * 1024 * 1024));
    bb.set_cache_index_and_filter_blocks(true);
    bb.set_pin_l0_filter_and_index_blocks_in_cache(true);
    let mut opts = Options::default();
    opts.set_block_based_table_factory(&bb);
    opts.set_max_open_files(std::env::var("MAX_OPEN").ok().and_then(|v| v.parse().ok()).unwrap_or(256));
    let db = DB::open_for_read_only(&opts, &path, false)?;
    println!(
        "estimate-num-keys {:?}",
        db.property_int_value("rocksdb.estimate-num-keys")?
    );
    let mut rng = Rng(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64 | 1);

    let t = Instant::now();
    let mut warmed = 0;
    for _ in 0..warm {
        if db.get_pinned(rng.key())?.is_some() {
            warmed += 1;
        }
    }
    report("warmup absent", warm, t.elapsed(), warmed);

    let absent: Vec<[u8; 32]> = (0..if std::env::var("SKIP_GET").is_ok() { 0 } else { n }).map(|_| rng.key()).collect();
    let t = Instant::now();
    let hits = absent.iter().filter(|k| db.get_pinned(k).unwrap().is_some()).count();
    report("get absent", absent.len().max(1), t.elapsed(), hits);

    let present = present_keys(&db, &mut rng, if std::env::var("SKIP_GET").is_ok() { 0 } else { n });
    let t = Instant::now();
    let hits = present.iter().filter(|k| db.get_pinned(k).unwrap().is_some()).count();
    report("get present", present.len().max(1), t.elapsed(), hits);

    let mut sorted = present_keys(&db, &mut rng, if std::env::var("SKIP_PRESENT").is_ok() { 0 } else { n });
    sorted.sort();
    let threads: usize = args.next().map(|v| v.parse().unwrap()).unwrap_or(1);
    let sorted_present = sorted;
    for (label, keys) in [("present", sorted_present), ("absent", {
        let mut a: Vec<Vec<u8>> = (0..n).map(|_| rng.key().to_vec()).collect();
        a.sort();
        a
    })] {
        if keys.is_empty() {
            continue;
        }
        let per = keys.len().div_ceil(threads);
        let t = Instant::now();
        let hits: usize = std::thread::scope(|sc| {
            keys.chunks(per)
                .map(|part| {
                    let db = &db;
                    sc.spawn(move || {
                        let mut hits = 0;
                        for chunk in part.chunks(batch) {
                            hits += db
                                .multi_get(chunk.iter())
                                .into_iter()
                                .filter(|r| matches!(r, Ok(Some(_))))
                                .count();
                        }
                        hits
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|h| h.join().unwrap())
                .sum()
        });
        report(&format!("batched_multi_get {label} batch={batch} threads={threads}"), n, t.elapsed(), hits);
    }
    Ok(())
}

fn report(label: &str, n: usize, d: std::time::Duration, hits: usize) {
    println!(
        "{label}: {n} keys in {:.2}s = {:.0}/s, {:.1} us each, hits {hits}",
        d.as_secs_f64(),
        n as f64 / d.as_secs_f64(),
        d.as_secs_f64() * 1e6 / n as f64
    );
}
