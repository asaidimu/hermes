// Package runtime is the workflow orchestration engine — the top layer of
// the 20261003 target architecture (refactor plan R.5/R.6; see
// todo/20261003-refactor-plan.md and tests/layering_test.go, which
// mechanically enforces the map below).
//
// Import direction flows DOWN the table; a layer may import only lower
// layers. Whitelisted exceptions live in tests/layering_test.go and must
// carry reasons.
//
//	Layer          Package                                  Owns
//	─────────────  ──────────────────────────────────────  ─────────────────────────────────────────────
//	0 shared       pkg/core                                 logger (*zap.SugaredLogger), errors
//	1 persistence  pkg/store                                run state stores (memory + persistent)
//	1 persistence  pkg/actionlog                            append-only run log (+ RunEnumerator later)
//	2 domain       pkg/pipeline                             graph IR, mutators, canonical pause/route,
//	                                                        checkpoint wire form (events bus whitelisted)
//	3 fold (pure)  pkg/replay                               the Replayer: seq-fold of log entries; no
//	                                                        locks, no I/O, no scheduling
//	4 compile      pkg/compiler, pkg/nodekit                graph → executable definition; node SDK
//	5 nodes        pkg/nodes/*                              leaf node implementations
//	6 orchestr.    pkg/runtime (thin) + internal/runtime    registration, dispatch, pause/resume,
//	                                                        outcomes; THE ONE recovery component
//	                                                        (recovery.go)
//	projections    pkg/events, pkg/scheduler, pkg/watch,    bus; timers (sole owner of re-arms); watch
//	                 pkg/timeline, pkg/registry             registrations; read-only projections
//
// Ownership rules enforced by tests and review (each traceable to an audit
// finding):
//
//  1. Locks never span user code (step actions, host callbacks, secret
//     lookups) — C1/C2 fix classes.
//  2. Every run-status fact is written ONCE — the action log; RAM
//     structures are derived caches (S.2).
//  3. One component rebuilds or re-arms a run: recovery.go here (R.4).
//  4. Timers belong to pkg/scheduler; orchestration passes intents and
//     releases them on terminal outcomes (C3-fix).
//  5. One canonical form per concept; other forms derived at serialization
//     boundaries (R.2).
//  6. Interfaces exist at the Options boundary or genuine multi-impl seams
//     only (R.3).
package runtime
