# MongoDB raw event store

MongoDB Community is SSPL-licensed. Its optional local document store preserves source rows
and flexible extra fields alongside PostgreSQL's constrained analytics; it duplicates raw
storage rather than replacing the batch pipeline. Source-row identity is
`(batch_id, source_file, source_row_number)`, so malformed and repeated event IDs are retained.
The insert-only loader rejects changed content at an existing identity. Bootstrap
creates indexes and restricted users; application credentials cannot update or delete rows.
The comparison reads the same raw order events from both databases, using the PostgreSQL
ETL role in a read-only transaction without widening grants. On 2026-10-15 the eight raw rows
for `ORD-0000001` matched exactly. Median client query time was 1.678 ms in MongoDB and
26.325 ms in PostgreSQL (five runs after a warm-up). MongoDB examined eight documents through
`order_timeline`; PostgreSQL used a parallel sequential scan and sort on the unchanged raw
table. This demonstrates different access paths, not a general database-engine speed ranking.
The single-session [measurements and plans](../benchmarks/mongo-2026-10-15.json) also include
441,222 initial inserts and a replay with zero inserts or modifications.

The bootstrap container has no network access and runs a temporary authenticated server on
its own loopback interface. On a fresh volume it uses MongoDB's localhost exception to create
the first administrator, then authenticates to create the collection, indexes and custom
roles. It shuts that server down before the long-running server opens the data volume.
Administrator credentials are supplied only to this one-shot container. Separate users in
the main and `_test` authentication databases share the configured names but have grants
only within their own database. No automatic database or volume deletion is provided.

Sources: [MongoDB licence](https://www.mongodb.com/legal/licensing/server-side-public-license),
[localhost exception](https://www.mongodb.com/docs/manual/core/localhost-exception/).
