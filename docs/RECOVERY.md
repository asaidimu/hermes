# Recovery: the durable contract

Every fact a run produces is recorded once, authoritatively, in the
**action log** (`pkg/actionlog`). RAM structures in the runtime
(`runMetas`, `outcomes`, the paused registry) are write-through caches of
that log; when a cache and the log disagree, the log wins. This document
is the contract for how recovery works, what backends must provide, and
what contributors may not break.

## The one recovery component

`internal/runtime/recovery.go` is the ONLY code in the tree that decides
how a run recovers:

- **Capability by data, not type assertion.** A run is
  event-sourced-rebuildable iff the action log holds entries for it
  (`rebuildCapability` probes `LatestRerunIndex`). No log, no entries —
  no rebuild capability.
- **One decision, loud degradation.** A failed rebuild falls back to the
  checkpoint address, logged — never silently. No log entries plus a
  checkpoint in run state also logs the degraded path.
- **Checkpoints live in the log too.** Every pause appends a
  `checkpoint` entry whose `Output` carries the serialized
  `PipelineCheckpoint` (waits, wait mode, timeout, received events,
  cron). The checkpoint copy in run state (`__pipeline_data__`) is an
  optimization for the degraded path, not a parallel recovery system.

## Log kinds that matter for recovery

| Kind | Recovery role |
| :--- | :--- |
| `step_completed` / `step_failed` | Effectful step deltas — what the Replayer folds into rebuilt state (seq-order fold) |
| `routed` | Routing decisions — lets the Replayer skip routers that already decided |
| `checkpoint` | The pause fact: resume address + wait descriptor + cron, serialized in `Output` |
| `resumed` | Marks the start of a new attempt segment (rerun index) |
| `pipeline_completed` / `pipeline_failed` | Terminal facts — Recover records them and never re-arms |
| `pipeline_started` | Run identity for meta synthesis (pipeline id, start time) |

## Restart durability (S.3)

`NewWorkflowRuntime` calls `Recover(ctx)` when the configured log
implements `actionlog.RunEnumerator`. For each enumerated
`(runID, latest rerunIndex)` the run's LAST entry classifies it:

1. **Terminal** (`pipeline_completed` / `pipeline_failed`) — the run meta
   is recorded; nothing is re-armed. Terminal runs never resurrect.
2. **Paused** (last entry `checkpoint`) — the pause is staged and re-armed
   when the owning workflow registers (`Register` → `bindPendingPauses`):
   paused-registry entry, dispatch index, watch registration
   (pre-pause event buffering), and the cron auto-resume (identity-gated).
   Binding is first-match by pipeline id — the log records run facts, not
   registration facts — so hosts reusing one pipeline id across workflows
   must register them in a deterministic order.
3. **In-flight** (anything else) — the process died mid-run. Meta is
   recorded as `recording`; the run is NOT auto-continued (without a
   checkpoint there is no resume address). The log stays queryable and
   the attempt numbering continues.

The state rebuild itself is **lazy**: `Resume` rebuilds through the
Replayer (`pkg/replay`, seq-order fold over the log), so `Recover` does
no replay work. The eager part is exactly the re-arming data. Cost at
startup: one key scan plus one `All()` read per enumerated run.

## RunEnumerator: the backend contract

```go
type RunEnumerator interface {
    Runs(ctx context.Context) ([]RunRef, error) // RunRef{RunID, RerunIndex}
}
```

A log backend that implements `RunEnumerator` opts into restart
durability. The contract:

- `Runs` enumerates every run attempt CURRENTLY persisted — stale/evicted
  attempts may be omitted (Recover uses the latest attempt per run).
- `LatestRerunIndex` must reflect every appended attempt immediately
  (the runtime derives rerun numbering from it — there is no RAM copy).
- Entries for a live run must never be dropped or reordered; `All`
  returns them in `Seq` order.

Backends shipped:

| Backend | Enumeration | Snapshots | Usage / prune | Durability horizon |
| :--- | :--- | :--- | :--- | :--- |
| `MemoryActionLog` | yes (`RunEnumerator`) | yes (`FoldSnapshotStore`) | usage yes; prune NO (nothing durable to reclaim — dropping entries would erase the audit trail for zero benefit) | process lifetime — pauses survive a runtime swap, not a process restart |
| `AnansiStore` | yes (`RunEnumerator`) | yes (`FoldSnapshotStore`, side collection `<name>_fold_snapshots`) | usage + prune yes (`UsageReporter`, `LogPruner`) | restart durability — pauses and run history survive process restarts |
| no log configured (`nil`) | — | — | — | NONE: runs are unrecoverable, `Resume` has nothing to rebuild from, and restart recovery is disabled by design |

## Definition drift: the fingerprint contract (v2.1 H1)

Every run is recorded under a **workflow-level structural fingerprint**
(`pipeline.WorkflowFingerprint`: trigger ids + each pipeline's
`pipeline.Fingerprint` — stage order, stage/step identities, timeouts,
retries, effect classes, config, nested subpipeline definitions). The
runtime stamps it on `pipeline_started` and `checkpoint` entries
(`Entry.DefHash`) and on the run meta. On `Resume` and on the S.3
restart re-arm, the recorded hash is compared against the workflow NOW
registered; `Options.DefinitionPolicy` decides what a mismatch means:

- `"warn"` (default) — proceed, log a Warn, tag `RunTimelineMeta.
  DefinitionDrift`. The pause is never silently lost; the drift is
  visible in `ListRuns`/`GetRunMeta`.
- `"require-same"` — refuse with the typed
  `core.ErrCodeDefinitionDrift` error. A same-process pause stays
  resumable (the entry is restored) and a restart pause stays staged in
  `pendingPauses`; registering the matching workflow re-claims it. A
  refusal is a guard, not a deletion.

**What the fingerprint cannot see** (do not oversell it): Go closure
behavior — `Step.Action`, `Stage.Router`, `PipelinesRouter`,
`DynamicPipelines`. For compiled workflows node behavior derives from
Config + the frozen node catalog, so real canvas edits land in Config
and ARE detected. A hand-built workflow whose closures change behavior
while keeping identical structure is UNDETECTED by design — hosts
needing code-level guarantees must version their workflow ids.
Hashless entries (logs written before v2.1) always pass — the check
never fires on legacy logs.

## Fold snapshots: the pause-time rebuild cache (v2.1 H2)

At every pause, the runtime exports the run's state and saves it beside
the log (`FoldSnapshotStore` capability, keyed `(runID, rerunIndex)`,
anchored to the checkpoint entry's `Seq` + the workflow fingerprint).
A paused-run rebuild may then SKIP the seq-fold walk and seed from the
snapshot — paused-run rebuilds are otherwise linear in history length
(~1.9µs/recorded delta; measured 12–16× on the 100/1000-entry shapes).

The read-side gate is **fail-closed toward the walk**. The snapshot is
used ONLY when every condition holds at Rebuild-call time:

1. a snapshot exists for the latest attempt,
2. the attempt's last entry is the `checkpoint` AT the snapshot's `Seq`
   (nothing appended after the pause),
3. the recorded fingerprint matches the registered workflow (H1 composes),
4. the state parses.

Any miss, corruption, drift, or doubt → the full walk, byte-identical to
pre-H2 semantics. Snapshots are a **derived cache, never a second source
of truth**: deleting every one of them changes latency, nothing else.
They never surface through `All`/`Count`/`Runs`, and
`Options.DisableFoldSnapshots` turns the mechanism off entirely.

## Retention and storage usage (v2.1 H3)

- `Options.StoreGrace` overrides the tier-1 eviction horizon (default
  10 minutes) for terminal runs' state stores.
- `Options.MaxLogAge` + the `LogPruner` capability: the janitor deletes
  WHOLE runs whose latest attempt ended terminally past the horizon.
  Paused and in-flight runs are never pruned — the log is their resume
  payload. Pruned runs' fold snapshots go with them.
- `UsageReporter` capability → `rt.StorageUsage(ctx)`; typed error (never
  a silent zero) when the backend cannot report.

## Operational limits

- **No action log** → no event-sourced rebuild, no restart durability,
  and `GetRunMeta` reconciles nothing. This is a legitimate mode for
  fire-and-forget usage; it is not a durability mode.
- **`MemoryActionLog`** → full recovery semantics within one process
  lifetime (the runtime swap and runtime-B-on-same-log acceptance tests
  run on it).
- **`AnansiStore`** → restart durability across processes.
- Terminal-run outcomes (`GetRunOutcome`) are RAM-only: a RECOVERED
  terminal run has meta and log facts but no `RunResult` snapshot — read
  its final state from the log (the M6 design decision).

## Contributor rules

1. **Locks never span user code.** `rt.mu` is a global non-reentrant
   lock; every callback that can reach host code (steps, secrets
   lookups, `EventSource.OnRegister`, bus emit closures, watch
   callbacks) runs OUTSIDE it. The Register critical section and the
   store-lock split (C2/C4) are the reference patterns; `grep` for
   `rt.mu.Lock()` before adding any call that can recurse into
   `dispatch`/`Resume`/`Register`.
2. **One fact, one authority.** New lifecycle facts are either emit-only
   (subscriber notification), log-only (durable, recoverable), or
   dual-written with the LOG authoritative. The per-fact disposition
   table lives in `internal/runtime/outcomes.go` (S.2 audit) — extend it
   when adding a fact.
3. **Recovery changes land in `internal/runtime/recovery.go`**, with the
   coupling rules in `todo/20261003-refactor-plan.md` (Trap #1–#4):
   fold changes and their event writes move in the same commit; storage
   layouts change without legacy dual-reads (delete policy,
   pre-production); the golden replay vectors pin the semantics.
4. **The layer map is enforced** by `tests/layering_test.go` — the
   runtime sits on top and imports nothing but the layers it declares in
   `internal/runtime/doc.go`.
