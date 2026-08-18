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

## Blocking snapshot (Kafka signal, ADR 0009 D4/D6)

An `execute-snapshot` signal of type `BLOCKING` re-snapshots the named tables
while the connector keeps running: streaming pauses at a batch boundary, the
snapshot scans a **fresh** REPEATABLE READ view, and streaming resumes from
the unchanged anchor. Snapshot rows are stamped `source.lsn = 0`, so replayed
CDC events always win in the ReplacingMergeTree — duplicate events after the
resume are harmless by the same convergence rule as the initial snapshot
(1.0 covers "backfill an added table / re-load one table"; incremental
snapshot is post-1.0, its offset-format and dispatcher hooks are pre-wired).

Configuration (the 1.0 signal channel is Kafka — no source signal table):

```
signal.enabled.channels=kafka
signal.kafka.topic=<one-partition signal topic>
signal.kafka.bootstrap.servers=<same cluster as Connect>
signal.kafka.poll.timeout.ms=1000   # the default 0 effectively never fetches
```

The signal record's **key must equal `topic.prefix`**, value:

```json
{"id": "<unique>", "type": "execute-snapshot",
 "data": {"type": "BLOCKING", "data-collections": ["db.table"]}}
```

Operational notes:

- A connector killed mid-blocking-snapshot restarts into a fresh snapshot
  decision on the stored offset (same rerun semantics as the initial
  snapshot); re-send the signal after it settles if the re-load did not
  complete.
- Re-loading into a **populated** sink table only converges for rows that
  still exist; deleted-row residue and the shadow-table swap procedure are
  the resnapshot runbook's concern (ADR 0009 D5).

## Fallback

The operator **write-stop procedure** of ADR 0005 (stop writes → snapshot →
resume writes) remains documented as a conservative fallback; it is no longer
required for correctness.

## Test-only fault-injection hooks

`internal.snapshot.test.pause.before.barrier.ms` /
`internal.snapshot.test.pause.after.barrier.ms` pause the snapshot around the
barrier capture so tests can inject commits deterministically into each
window. Default 0; never set them in production.
