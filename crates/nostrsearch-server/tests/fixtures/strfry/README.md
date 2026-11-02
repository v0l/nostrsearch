`data.mdb.zst` is a strfry LMDB database written by the official image
`ghcr.io/hoytech/strfry:latest`: 299 events from the archive imported with
`strfry import`, then `strfry dict train` and `strfry dict compress`, so 201
payloads are dictionary-compressed and 98 are plain. `expected.jsonl.zst` is
`strfry export` of the same database.
