use nostrsearch_indexer::id_store::IdStore;

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let path = args.next().expect("path");
    let millions: u64 = args.next().expect("millions").parse()?;
    let store = IdStore::open_with_cache(std::path::Path::new(&path), 256)?;
    let mut s = 0x1234_5678_9abc_def1u64;
    let mut batch = Vec::with_capacity(1_000_000);
    for i in 0..millions * 1_000_000 {
        let mut k = [0u8; 32];
        for c in k.chunks_mut(8) {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            c.copy_from_slice(&s.to_le_bytes());
        }
        batch.push(k);
        if batch.len() == 1_000_000 {
            store.flush(batch.iter())?;
            batch.clear();
            if (i / 1_000_000) % 50 == 0 {
                println!("{} M", i / 1_000_000);
            }
        }
    }
    Ok(())
}
