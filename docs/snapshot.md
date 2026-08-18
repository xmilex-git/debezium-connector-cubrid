# Initial snapshot — online, no write stop (ADR 0009)

The initial snapshot runs **without stopping writes** on the captured tables.
Its consistency comes from three existing parts, in a fixed order:

1. **Barrier LSA capture** — `determineSnapshotOffset` captures the current
   supplemental-log position over the CDC log client (ADR 0005 D3). Streaming
   later resumes exactly at the barrier with counter 0.
2. **REPEATABLE READ view established strictly after the barrier** — the
   snapshot connection runs at REPEATABLE READ, and the transaction open at
   barrier-capture time (from the framework's metadata queries) is discarded
   with a `commit()` right after the barrier is captured. The data scan's view
   therefore opens above the barrier. This ordering is the load-bearing
   invariant: a commit invisible to the scan view has an LSA at or above the
   barrier and is replayed by streaming; a commit visible to both converges
   because snapshot rows carry `source.lsn = 0` and always lose to CDC rows in
   the ReplacingMergeTree (ADR 0005 D4).
3. **Version-0 snapshot rows + RMT convergence** — overlap between snapshot
   and stream is harmless by construction.

Verified by the four-fault campaign in the workspace repo
(`htap-poc/e2e/run-snapshot-faults.sh`, evidence on xmilex-git/workspace#64):
continuous DML during the snapshot, commits injected into both barrier
windows, post-barrier deletes of rows the scan still emits, and a hard worker
kill mid-snapshot with rerun — all ending in a 0-mismatch differential check
against CUBRID as the oracle.

## Known constraints (ADR 0009 D2/D3)

- **`snapshot.max.threads` must stay `1`.** CUBRID's `LIMIT n [OFFSET m]`
  cannot express the keyset boundary queries a chunked parallel snapshot
  needs (ADR 0005). Large tables therefore snapshot single-threaded;
  parallelization is post-1.0.
- **DDL waits while the snapshot scans.** The REPEATABLE READ reader blocks
  DDL on the captured tables for the duration of the scan (measured, ADR
  0005). Combined with the previous point: the DDL-blocked window grows with
  table size.
- **Long scans prolong the streaming catch-up.** Everything committed during
  the scan is replayed from the barrier after handover (at-least-once);
  budget supplemental-log retention accordingly.

## Fallback

The operator **write-stop procedure** of ADR 0005 (stop writes → snapshot →
resume writes) remains documented as a conservative fallback; it is no longer
required for correctness.

## Test-only fault-injection hooks

`internal.snapshot.test.pause.before.barrier.ms` /
`internal.snapshot.test.pause.after.barrier.ms` pause the snapshot around the
barrier capture so tests can inject commits deterministically into each
window. Default 0; never set them in production.
