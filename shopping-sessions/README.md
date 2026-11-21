# Shopping Sessions: Rust pipeline

One native binary, all 8 cores, following `rules.md` exactly.

1. **Lookups**: products, campaigns, manifest; links parsed in parallel, joined with union-find, each person named by its byte-wise smallest ID.
2. **Events**: every file memory-mapped and cut into chunks; one SIMD pass per chunk finds lines, fields and quotes and drops malformed lines. Each row is validated (timestamps F1/F2/F3 with full calendar checks, type, product, price) and partitioned by a 128-bit event-ID hash.
3. **Dedup**: per partition, a hash table keeps the copy with the smallest `ingest_ts`; the kept copy is validated and late events dropped; survivors are re-partitioned by person.
4. **Clicks**: parsed, validated and partitioned by person (copies are identical, so no dedup is needed).
5. **Sessions**: per person partition, events sorted by time, split on gaps over 30 minutes, credited to the latest qualifying click (ties: smallest `click_id`), assists counted with a sliding 7-day window.
6. **Output**: Parquet with `day` as `DATE`.

Checked against all 36 fixtures and the 1M-event dev answer (exact match).
