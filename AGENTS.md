# Agent notes

## Event timestamps

Do not reject, clamp or drop events for an old `created_at`. Some users backed
up their tweets to nostr with the original dates, so events from years before
nostr existed (2006 onward, even 1970) are real data and must stay indexed.
The small monthly shards they create are expected.
