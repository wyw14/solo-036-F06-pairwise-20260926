PigFlow is a local HTTP control service for long-distance pipeline pigging operations.

## Event journal queries

`GET /api/events` searches the durable event journal (requires `PIGFLOW_DATA_DIR`):

- `run_id`, `kind` — exact-match filters
- `from`, `to` — RFC3339 time bounds, inclusive
- `after` — resume cursor returned as `next` by the previous page
- `limit` — page size (default 100, max 1000)

Each page returns `entries` (with per-record `sequence` and `checksum`), the
`next` resume cursor, `has_more`, and the checksum-chain `verified` status.
Appends between pages are picked up by later pages without missing or
repeating records. If the chain is broken, the page stops at the last
verified record and reports `corrupt_sequence`/`corruption` instead of
skipping the damaged record.
