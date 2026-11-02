use anyhow::{Context, Result, bail};
use std::collections::HashMap;
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::path::Path;

pub const LMDB_MAGIC: u32 = 0xBEEF_C0DE;
pub const MAGIC_OFFSET: usize = 16;
pub const SUPPORTED_DB_VERSION: u64 = 3;
pub const EXPECTED_ENDIANNESS: u64 = 1;

const PAGE_HEADER: usize = 16;
const NODE_HEADER: usize = 8;
const P_BRANCH: u16 = 0x01;
const P_LEAF: u16 = 0x02;
const P_OVERFLOW: u16 = 0x04;
const P_LEAF2: u16 = 0x20;
const F_BIGDATA: u16 = 0x01;
const F_SUBDATA: u16 = 0x02;
const F_DUPDATA: u16 = 0x04;
const MAX_DEPTH: usize = 64;
const MAX_VALUE_BYTES: u64 = 1 << 22;
const MAX_DICTS: usize = 4096;
const MAX_DICT_BYTES: usize = 1 << 28;
const NO_PAGE: u64 = u64::MAX;

const TABLE_PREFIX: &str = "rasgueadb_defaultDb__";

pub fn has_lmdb_magic(head: &[u8]) -> bool {
    head.len() >= MAGIC_OFFSET + 4
        && u32::from_le_bytes(head[MAGIC_OFFSET..MAGIC_OFFSET + 4].try_into().unwrap())
            == LMDB_MAGIC
}

fn u16_at(b: &[u8], at: usize) -> Result<u16> {
    let s = b.get(at..at.checked_add(2).context("offset overflow")?);
    Ok(u16::from_le_bytes(s.context("short read")?.try_into()?))
}

fn u32_at(b: &[u8], at: usize) -> Result<u32> {
    let s = b.get(at..at.checked_add(4).context("offset overflow")?);
    Ok(u32::from_le_bytes(s.context("short read")?.try_into()?))
}

fn u64_at(b: &[u8], at: usize) -> Result<u64> {
    let s = b.get(at..at.checked_add(8).context("offset overflow")?);
    Ok(u64::from_le_bytes(s.context("short read")?.try_into()?))
}

#[derive(Clone, Copy)]
struct DbRecord {
    depth: u16,
    entries: u64,
    root: u64,
}

impl DbRecord {
    fn parse(b: &[u8], at: usize) -> Result<Self> {
        Ok(Self {
            depth: u16_at(b, at + 6)?,
            entries: u64_at(b, at + 32)?,
            root: u64_at(b, at + 40)?,
        })
    }
}

pub struct Lmdb {
    file: File,
    page_size: usize,
    pages: u64,
    main: DbRecord,
}

impl Lmdb {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).context("cannot open database file")?;
        let len = file.metadata()?.len();
        let mut first = vec![0u8; 4096.min(len as usize)];
        file.read_exact_at(&mut first, 0)
            .context("database file too short")?;
        if !has_lmdb_magic(&first) {
            bail!("not an LMDB database");
        }
        let page_size = u32_at(&first, PAGE_HEADER + 24)? as usize;
        if !(4096..=65536).contains(&page_size) || !page_size.is_power_of_two() {
            bail!("unsupported LMDB page size {page_size}");
        }
        let pages = len / page_size as u64;
        if pages < 3 {
            bail!("database file too short");
        }
        let mut best: Option<(u64, DbRecord)> = None;
        for slot in 0..2u64 {
            let mut page = vec![0u8; page_size];
            file.read_exact_at(&mut page, slot * page_size as u64)
                .context("cannot read meta page")?;
            if u32_at(&page, MAGIC_OFFSET).ok() != Some(LMDB_MAGIC) {
                continue;
            }
            let last_pg = u64_at(&page, PAGE_HEADER + 120)?;
            let txnid = u64_at(&page, PAGE_HEADER + 128)?;
            if last_pg >= pages {
                continue;
            }
            let main = DbRecord::parse(&page, PAGE_HEADER + 72)?;
            if main.root != NO_PAGE && main.root >= pages {
                continue;
            }
            if best.as_ref().is_none_or(|(t, _)| txnid > *t) {
                best = Some((txnid, main));
            }
        }
        let (_, main) = best.context("no valid LMDB meta page")?;
        Ok(Self {
            file,
            page_size,
            pages,
            main,
        })
    }

    fn read_page(&self, pgno: u64) -> Result<Vec<u8>> {
        if pgno >= self.pages {
            bail!("page {pgno} beyond end of file");
        }
        let mut page = vec![0u8; self.page_size];
        self.file
            .read_exact_at(&mut page, pgno * self.page_size as u64)
            .context("page read failed")?;
        if u64_at(&page, 0)? != pgno {
            bail!("page {pgno} header does not match its position");
        }
        Ok(page)
    }

    fn walk<F>(&self, db: DbRecord, f: &mut F) -> Result<()>
    where
        F: FnMut(&[u8], Option<&[u8]>, u16) -> Result<()>,
    {
        if db.root == NO_PAGE || db.entries == 0 {
            return Ok(());
        }
        let mut visited = 0u64;
        let depth_cap = (db.depth as usize + 1).min(MAX_DEPTH);
        self.walk_page(db.root, 0, depth_cap, &mut visited, f)
    }

    fn walk_page<F>(
        &self,
        pgno: u64,
        depth: usize,
        depth_cap: usize,
        visited: &mut u64,
        f: &mut F,
    ) -> Result<()>
    where
        F: FnMut(&[u8], Option<&[u8]>, u16) -> Result<()>,
    {
        if depth >= depth_cap {
            bail!("tree deeper than declared");
        }
        *visited += 1;
        if *visited > self.pages {
            bail!("tree visits more pages than the file holds");
        }
        let page = self.read_page(pgno)?;
        let flags = u16_at(&page, 10)?;
        let lower = u16_at(&page, 12)? as usize;
        if lower < PAGE_HEADER || lower > self.page_size {
            bail!("bad page free-space bounds");
        }
        let count = (lower - PAGE_HEADER) / 2;
        if flags & P_LEAF2 != 0 {
            bail!("unsupported fixed-size leaf page");
        }
        for i in 0..count {
            let ptr = u16_at(&page, PAGE_HEADER + 2 * i)? as usize;
            if ptr < PAGE_HEADER || ptr + NODE_HEADER > self.page_size {
                bail!("node pointer out of page");
            }
            let lo = u16_at(&page, ptr)? as u64;
            let hi = u16_at(&page, ptr + 2)? as u64;
            let nflags = u16_at(&page, ptr + 4)?;
            let ksize = u16_at(&page, ptr + 6)? as usize;
            if flags & P_BRANCH != 0 {
                let child = lo | (hi << 16) | ((nflags as u64) << 32);
                self.walk_page(child, depth + 1, depth_cap, visited, f)?;
            } else if flags & P_LEAF != 0 {
                if nflags & F_DUPDATA != 0 {
                    bail!("duplicate-sorted tables are not supported");
                }
                let key_at = ptr + NODE_HEADER;
                let key = page
                    .get(key_at..key_at + ksize)
                    .context("key out of page")?;
                let data_at = key_at + ksize;
                let dsize = lo | (hi << 16);
                if nflags & F_BIGDATA != 0 {
                    let opg = u64_at(&page, data_at)?;
                    match self.read_overflow(opg, dsize, visited)? {
                        Some(data) => f(key, Some(&data), nflags)?,
                        None => f(key, None, nflags)?,
                    }
                } else {
                    let end = data_at
                        .checked_add(dsize as usize)
                        .filter(|e| *e <= self.page_size)
                        .context("value out of page")?;
                    f(key, Some(&page[data_at..end]), nflags)?;
                }
            } else {
                bail!("unexpected page type {flags:#x} in tree");
            }
        }
        Ok(())
    }

    fn read_overflow(&self, pgno: u64, dsize: u64, visited: &mut u64) -> Result<Option<Vec<u8>>> {
        if pgno >= self.pages {
            bail!("overflow page beyond end of file");
        }
        let mut head = vec![0u8; PAGE_HEADER];
        self.file
            .read_exact_at(&mut head, pgno * self.page_size as u64)
            .context("overflow read failed")?;
        if u64_at(&head, 0)? != pgno || u16_at(&head, 10)? & P_OVERFLOW == 0 {
            bail!("bad overflow page");
        }
        let n = u32_at(&head, 12)? as u64;
        let span = n
            .checked_mul(self.page_size as u64)
            .context("overflow span overflow")?;
        if n == 0
            || pgno.checked_add(n).is_none_or(|e| e > self.pages)
            || dsize > span - PAGE_HEADER as u64
        {
            bail!("overflow value exceeds the file");
        }
        *visited += n;
        if *visited > self.pages {
            bail!("tree visits more pages than the file holds");
        }
        if dsize > MAX_VALUE_BYTES {
            return Ok(None);
        }
        let mut data = vec![0u8; dsize as usize];
        self.file
            .read_exact_at(&mut data, pgno * self.page_size as u64 + PAGE_HEADER as u64)
            .context("overflow read failed")?;
        Ok(Some(data))
    }

    fn table(&self, name: &str) -> Result<Option<DbRecord>> {
        let want = format!("{TABLE_PREFIX}{name}");
        let mut found = None;
        self.walk(self.main, &mut |key, data, nflags| {
            if key == want.as_bytes() && nflags & F_SUBDATA != 0 {
                found = Some(DbRecord::parse(data.context("table record too large")?, 0)?);
            }
            Ok(())
        })?;
        Ok(found)
    }

    pub fn check_meta(&self) -> Result<()> {
        let Some(meta) = self.table("Meta")? else {
            bail!("no strfry Meta table: not a strfry database");
        };
        let mut seen = false;
        self.walk(meta, &mut |_, data, _| {
            if seen {
                return Ok(());
            }
            seen = true;
            let data = data.context("Meta record too large")?;
            let version = fb_u64(data, 0)?;
            let endianness = fb_u64(data, 1)?;
            if endianness != EXPECTED_ENDIANNESS {
                bail!("database was written on a machine with different endianness");
            }
            if version != SUPPORTED_DB_VERSION {
                bail!("unsupported strfry database version {version}");
            }
            Ok(())
        })?;
        if !seen {
            bail!("strfry Meta table is empty");
        }
        Ok(())
    }

    fn dictionaries(&self) -> Result<HashMap<u32, Vec<u8>>> {
        let mut dicts = HashMap::new();
        let Some(table) = self.table("CompressionDictionary")? else {
            return Ok(dicts);
        };
        let mut total = 0usize;
        self.walk(table, &mut |key, data, _| {
            let id = u64_at(key, 0)?;
            let Ok(id) = u32::try_from(id) else {
                return Ok(());
            };
            let Some(data) = data else { return Ok(()) };
            let dict = fb_bytes(data, 0)?;
            total += dict.len();
            if dicts.len() >= MAX_DICTS || total > MAX_DICT_BYTES {
                bail!("too many compression dictionaries");
            }
            dicts.insert(id, dict.to_vec());
            Ok(())
        })?;
        Ok(dicts)
    }

    pub fn for_each_event<F>(&self, max_json: usize, mut f: F) -> Result<()>
    where
        F: FnMut(Payload) -> Result<()>,
    {
        self.check_meta()?;
        let dicts = self.dictionaries()?;
        let mut decoders: HashMap<u32, zstd::bulk::Decompressor<'static>> = HashMap::new();
        let Some(table) = self.table("EventPayload")? else {
            bail!("no strfry EventPayload table");
        };
        self.walk(table, &mut |_, data, _| {
            let Some(raw) = data else {
                return f(Payload::Undecodable);
            };
            match raw.split_first() {
                Some((0, json)) => f(Payload::Json(json.to_vec())),
                Some((1, rest)) if rest.len() >= 4 => {
                    let id = u32::from_ne_bytes(rest[..4].try_into().unwrap());
                    let decoder = match decoders.get_mut(&id) {
                        Some(d) => d,
                        None => {
                            let Some(dict) = dicts.get(&id) else {
                                return f(Payload::Undecodable);
                            };
                            let Ok(d) = zstd::bulk::Decompressor::with_dictionary(dict) else {
                                return f(Payload::Undecodable);
                            };
                            decoders.entry(id).or_insert(d)
                        }
                    };
                    match decoder.decompress(&rest[4..], max_json) {
                        Ok(json) => f(Payload::Json(json)),
                        Err(_) => f(Payload::Undecodable),
                    }
                }
                _ => f(Payload::Undecodable),
            }
        })
    }
}

pub enum Payload {
    Json(Vec<u8>),
    Undecodable,
}

fn fb_field(buf: &[u8], slot: usize) -> Result<Option<usize>> {
    let root = u32_at(buf, 0)? as usize;
    let soffset = u32_at(buf, root)? as i32 as i64;
    let vtable = (root as i64)
        .checked_sub(soffset)
        .filter(|v| *v >= 0)
        .context("bad flatbuffer vtable")? as usize;
    let vsize = u16_at(buf, vtable)? as usize;
    let at = 4 + 2 * slot;
    if at + 2 > vsize {
        return Ok(None);
    }
    let off = u16_at(buf, vtable + at)? as usize;
    if off == 0 {
        return Ok(None);
    }
    Ok(Some(root.checked_add(off).context("bad flatbuffer field")?))
}

fn fb_u64(buf: &[u8], slot: usize) -> Result<u64> {
    match fb_field(buf, slot)? {
        Some(at) => u64_at(buf, at),
        None => Ok(0),
    }
}

fn fb_bytes(buf: &[u8], slot: usize) -> Result<&[u8]> {
    let Some(at) = fb_field(buf, slot)? else {
        return Ok(&[]);
    };
    let vec = at
        .checked_add(u32_at(buf, at)? as usize)
        .context("bad flatbuffer vector")?;
    let len = u32_at(buf, vec)? as usize;
    let start = vec + 4;
    buf.get(start..start.checked_add(len).context("bad flatbuffer vector")?)
        .context("flatbuffer vector out of bounds")
}
