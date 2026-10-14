# 20261003 refactor — final gate report (plan P6.3)

Branch `refactor/20261003-audits`, HEAD at the time of this report:
`b428ce2 perf(replay): batch a step's recorded deltas into one store update`.
All 36 plan tasks across Phases 0–6 are complete; this report is the
gate evidence.

## Gates

| Gate | Result |
| :--- | :--- |
| `go vet ./...` | clean |
| `go test -race ./...` | 25/25 packages ok, 0 failures |
| `gofmt -s` (all of internal/, pkg/, tests/) | clean — the tab-drift introduced by editing sessions was reverted in `1ffb916` |
| `tests/layering_test.go` (R.6 layer map) | ok |
| `tests/deletion_audit_test.go` (dead symbols stay dead + zero `// Deprecated:`) | ok — the only `Deprecated:` literals in the tree are the audit test's own patterns |
| Restart-durability acceptance test in the default suite | yes — `tests/restart_durability_test.go` runs under `go test -race ./...` |
| `make bench` + `make bench-race` subset | ran clean (numbers below) |

## Benchmark summary vs the P0 baseline (`todo/20261003-bench-baseline.txt`)

| Benchmark | P0 baseline | final | verdict |
| :--- | :--- | :--- | :--- |
| `AnansiAppend_Log10/100/1000` | 7.3ms / 5.9ms / 22ms | ~182µs / ~187µs / ~188µs (flat) | O(n²) append eliminated (P4.1) — 36–110× |
| `StageDeltaNoActionLog_1MB` | 2278.7µs | 17.0µs | −99.3% (P4.2 delta-on-consumer + touched keys) |
| `Rebuild_1000Entries_1Attempt` | 485.8µs | 1897.5µs | **see the semantics note below** |
| `Rebuild_100Entries_10Attempts` | 264.0µs | 1736.6µs | same note |
| `CronDelay_*` | 44.4µs / 45.7µs / 22.2ns / 130ns | 44.4µs / 45.3µs / 23.7ns / 129.9µs | parity (untouched by the refactor) |

### The rebuild comparison is honest but apples-to-oranges

The P0 fold kept only the FIRST effect per step and silently discarded
every later recorded delta — for the 1000-entry bench workload (1000
completions across 5 steps) it parsed 5 deltas and threw away 995. That
was the §D divergence bug S.1 fixed: the canonical seq-fold applies
EVERY recorded outcome in sequence order, matching the TS reference.
The added work is therefore real: ~1.9µs per recorded delta (JSON parse
+ ordered application), one store Update per step (`b428ce2`).
`Rebuild` runs once per `Resume` — it is not a hot path. Per-phase
benchstat evidence for every performance commit lives in the commit
messages (P4.1–P4.9 each carry before/after numbers measured against
their own parents).

## Version decision

**Recommend `v2.0.0`.** Exported symbols are deleted outright (no
deprecations), the action-log storage layout changed (old readers
cannot read new logs), and replay semantics changed (first-effect →
seq-fold). Even with no production consumers, semver honesty costs
nothing pre-adoption; the repo sits at v1.2.1. The npm package
(`src/*.ts`) is untouched except the frozen `src/replay.ts` comment —
no npm release is required.

## Plan completion

Every checkbox in `todo/20261003-refactor-plan.md` is marked done; the
commit map at the bottom of the plan matches the branch history
(37 commits at gate time, one per task plus plan/style commits).
