//! A corrupt line must cost one event, not the rest of the file.

#[test]
fn corruption_midway_does_not_abandon_the_file() {
    use std::io::Write;
    let dir = std::env::temp_dir().join(format!("nsreplay-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("combined.jsonl");
    let mut f = std::fs::File::create(&path).unwrap();

    let ev = |i: u32| {
        format!(
            r#"{{"id":"{:064x}","pubkey":"{}","created_at":1700000000,"kind":1,"tags":[],"content":"e{}","sig":"{}"}}"#,
            i,
            "b".repeat(64),
            i,
            "c".repeat(128)
        )
    };
    for i in 0..500u32 {
        writeln!(f, "{}", ev(i)).unwrap();
    }
    // The kind of thing that kills the upstream reader:
    writeln!(f, "{{ this is not valid json at all").unwrap();
    writeln!(f, "\u{0}\u{1}\u{2} binary garbage").unwrap();
    for i in 500..1000u32 {
        writeln!(f, "{}", ev(i)).unwrap();
    }
    drop(f);

    // Count what a line-oriented resilient read recovers.
    let file = std::fs::File::open(&path).unwrap();
    let mut reader = std::io::BufReader::new(file);
    let (mut ok, mut bad) = (0u64, 0u64);
    let mut line = Vec::new();
    use std::io::BufRead;
    loop {
        line.clear();
        match reader.read_until(b'\n', &mut line) {
            Ok(0) => break,
            Ok(_) => {
                let t = line.strip_suffix(b"\n").unwrap_or(&line);
                if t.is_empty() {
                    continue;
                }
                match serde_json::from_slice::<nostrsearch_core::event::NostrEvent>(t) {
                    Ok(_) => ok += 1,
                    Err(_) => bad += 1,
                }
            }
            Err(_) => break,
        }
    }
    println!("recovered {ok} events, skipped {bad} bad lines");
    assert_eq!(ok, 1000, "must recover events AFTER the corruption");
    assert_eq!(bad, 2);
    std::fs::remove_dir_all(&dir).ok();
}
