# Agent notes

## Event timestamps

Do not reject, clamp or drop events for an old `created_at`. Some users backed
up their tweets to nostr with the original dates, so events from years before
nostr existed (2006 onward, even 1970) are real data and must stay indexed.
The small monthly shards they create are expected.

Events more than a day in the future are invalid. They are almost always a
client writing `created_at` in milliseconds, and each one opens a shard in a
year like 55913. `ShardManager::index_event` rejects them.

## Submissions (/submit)

No size or daily quota caps. jsonl uploads (`.jsonl`, `.zst`, `.gz`, `.bz2`)
are decoded while the body arrives, through a bounded channel into a blocking
parser, so nothing is staged in SUBMIT_DIR and a 700 GB upload costs only the
new events it adds. `SUBMIT_MIN_FREE_BYTES` is a floor on free disk, checked
about once a second while streaming; hitting it fails the job and keeps what
was already ingested.

`.mdb` uploads need random access and are written to SUBMIT_DIR first. They
need a Content-Length and are admitted only when the declared size plus the
floor fits in free space.

A dropped connection ends the job as `interrupted` with its counts; events
already ingested stay. Resuming means uploading the file again: duplicates are
counted, not re-added.
