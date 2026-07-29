# Generational online backend migration

Migration is an execution/control-plane state machine scoped to one Shard. It never changes the
backend class in place.

1. Create generation `G+1` as Candidate with its own namespace.
2. Copy a logical snapshot and replay the retained committed suffix.
3. Mirror new committed writes and persist receipts on both generations.
4. Verify fences, content digests, and caught-up applied indexes.
5. Publish placement epoch `E+1` with generation `G+1` Active.
6. Keep a bounded reverse-mirroring grace period, then retire and clean generation `G`.

Every phase is durable and idempotent. Restart resumes from the last authoritative phase. Rollback
is monotonic and never reactivates an unverified namespace. The six directed migrations among the
three official providers share the same logical protocol and certification matrix.
