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

No size or daily quota caps. Uploads (`.jsonl`, `.zst`, `.gz`, `.bz2`, `.mdb`
and compressed `.mdb`) are written to SUBMIT_DIR exactly as sent, at network
speed, and `/submit` returns 202 with a job id once the body is stored. A
background worker (SUBMIT_PROCESS_CONCURRENCY, default 1) ingests queued
files. Job state is persisted next to the file (`<id>.job.json`) with the line
cursor, so a restart resumes after the last flushed batch instead of starting
over. The stored file is deleted when the job ends.

`SUBMIT_MIN_FREE_BYTES` is a floor on free disk. An upload is refused when its
declared size plus the floor does not fit, and stopped (file discarded) if free
space drops below the floor while it arrives. `/submit/limits` reports
`acceptable_bytes`. A dropped upload connection ends the job as `interrupted`
and discards the partial file; upload again, duplicates are counted not re-added.
