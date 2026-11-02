# Agent notes

## Event timestamps

Do not reject, clamp or drop events for an old `created_at`. Some users backed
up their tweets to nostr with the original dates, so events from years before
nostr existed (2006 onward, even 1970) are real data and must stay indexed.
The small monthly shards they create are expected.

Events more than a day in the future are invalid. They are almost always a
client writing `created_at` in milliseconds, and each one opens a shard in a
year like 55913. `ShardManager::index_event` rejects them.
