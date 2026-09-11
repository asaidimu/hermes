# [1.2.0](https://github.com/asaidimu/hermes/compare/v1.1.1...v1.2.0) (2026-09-11)


### Bug Fixes

* **actionlog:** detach runId from document identity; durable multi-run action log ([afbf9c7](https://github.com/asaidimu/hermes/commit/afbf9c7573d0b41f20130ff12988937112d32a64)), closes [#review-20260910-024](https://github.com/asaidimu/hermes/issues/review-20260910-024)
* **engine:** resolve all P2 review findings ([5b94c0a](https://github.com/asaidimu/hermes/commit/5b94c0a542cc4d3daa2521fd7599fd16f2b1905d)), closes [#review-20260910-015](https://github.com/asaidimu/hermes/issues/review-20260910-015) [#review-20260910-003](https://github.com/asaidimu/hermes/issues/review-20260910-003) [#review-20260910-004](https://github.com/asaidimu/hermes/issues/review-20260910-004) [#review-20260910-009](https://github.com/asaidimu/hermes/issues/review-20260910-009) [#review-20260910-011](https://github.com/asaidimu/hermes/issues/review-20260910-011) [#review-20260910-012](https://github.com/asaidimu/hermes/issues/review-20260910-012) [#review-20260910-013](https://github.com/asaidimu/hermes/issues/review-20260910-013) [#review-20260910-016](https://github.com/asaidimu/hermes/issues/review-20260910-016) [#review-20260910-017](https://github.com/asaidimu/hermes/issues/review-20260910-017) [#review-20260910-021](https://github.com/asaidimu/hermes/issues/review-20260910-021) [#review-20260910-010](https://github.com/asaidimu/hermes/issues/review-20260910-010)
* **engine:** resolve P1 review findings and latent resume blockers ([fcc28af](https://github.com/asaidimu/hermes/commit/fcc28afc05fe2f95d5f96ee52d539c8fce44d630))


### Features

* **runtime:** add GetActionLog and improve test lifecycle management ([9a2c0d3](https://github.com/asaidimu/hermes/commit/9a2c0d3c2ef0dd632df87272e4e09378371f23b9))

## [Unreleased]

### Bug Fixes

* **replay:** replay subpipeline-bearing pipelines faithfully — log entries now carry a PipelineID (schema addition to actionlog.Entry, stamped at every append site) and the Replayer partitions entries per pipeline, replays each child instance against its own definition and store (dynamic distribute children are regenerated via the stage's DynamicPipelines closure, exactly like the runtime), and merges child results with the runtime's resultKey/default-merge semantics; previously the flat step-id index collapsed concurrent subpipeline instances, silently dropping all but the first child's deltas and letting one child's route steer every child (#review-20260910-015)
* **runtime:** the replay DefinitionResolver resolves the definition the id actually names — across the compiled workflow (Workflow.FindPipeline: trigger-keyed pipelines plus subpipeline definitions embedded in stages, recursively) instead of answering every id with the run's root pipeline (#review-20260910-003)
* **runtime:** evict per-run bookkeeping — a terminal run's state store and rerun index are reclaimed by a janitor after a grace horizon (Options.RunHistoryTTL, default 10 minutes), and audit history (runMetas/outcomes) is purged when an explicit TTL is set; previously the maps only ever grew, leaking one full state map per run for the life of the host (#review-20260910-004)
* **pipeline:** run step actions on a deep-copied state snapshot OUTSIDE the store read lock — arbitrary user code (JS, HTTP retries) no longer serializes concurrent steps in a stage behind the lock, and direct mutations of the passed state map can no longer touch the store or race concurrent readers (fatal, unrecoverable concurrent-map-write); the returned mutator remains the only write path (#review-20260910-009)
* **nodes/code:** bind a deep copy of state into the goja sandbox — direct JS mutations (`state.x = …`) are discarded instead of silently writing store state behind the mutator/atomic-commit model and diverging the action log from real state; only the returned patch writes (#review-20260910-011)
* **pipeline:** fail the pause when a Persist checkpoint write fails instead of reporting "paused" with nothing durable recording where to resume; nested-checkpoint and default sub-pipeline merge update failures are now logged instead of silently discarded (#review-20260910-012)
* **scheduler:** validate cron expressions at Schedule time (ValidateCron) and reject invalid ones — a typo'd trigger ("30 * * *", "@evry 5m") previously registered fine and silently fired hourly via CronDelay's fallback (#review-20260910-013)
* **compiler:** reject the experimental query node at compile time — every execution failed, yet the README documented it as production-ready; canvases using it now fail where the graph is authored, not at runtime (#review-20260910-016)
* **ci:** `make test` now runs go vet + go test -race, `make check` adds a gofmt -s gate, and CI runs both — the quality gates the README promises can no longer drift (#review-20260910-021, #review-20260910-010)

### Documentation

* mark the `database`/`query` node pair experimental in the node catalog, definitions, and use cases — the database service node still initializes no handle (#review-20260910-017)

* **replay:** make StateAt deterministic — the projection previously depended on Go's randomized map iteration order, folding in a sibling step's recorded delta only when map order happened to allow it; it now always reconstructs "just before the target step's own delta landed" (steps in a stage run concurrently), and an unrecorded effectful sibling no longer aborts the projection (#review-20260910-024)
* **actionlog:** detach the run id from document identity — log documents now carry a deterministic UUIDv7-shaped id derived from the entry's own (RunID, RerunIndex), so ONE scope-free AnansiStore serves the multi-run runtime like MemoryActionLog; Append routes by entry identity, rejects entries without a RunID, and derives Seq as max(existing)+1; reads go straight to the document id instead of scanning the collection on the runId field; the durable event-sourced recovery path is wired end-to-end in a runtime integration test (#review-20260910-014)
* **pipeline:** recover panics in step actions, step goroutines, and sub-pipeline children so a bad node fails the step/stage instead of the host process (#review-20260910-008)
* **runtime:** track a re-paused run in full after resume — multi-event waits, wait mode, WatchService re-registration, buffered-event drain, and cron re-arm (#review-20260910-001)
* **runtime:** adopt the Replayer's rebuilt store as the canonical store after an event-sourced resume, fixing stale FinalState results and multi-pause state corruption (#review-20260910-002)
* **nodes/pause:** key watch registrations and buffered-event lookups by run id instead of node id, eliminating cross-run collisions and registration leaks (#review-20260910-007)
* **nodekit:** plumb RunID into NodeRunContext and inject built-in runtime resources (watch-service) so nodes and routers can reach them; the pause node previously never received the service and silently never paused
* **runtime/watchservice:** honor mode=all on parked multi-event registrations — resume only when every watched event type has delivered
* **replay:** resolve the "__pause__" wait-marker route through the pause checkpoint's ResumeAt instead of returning the pausing stage, which made every event-sourced resume re-pause forever (#review-20260910-023)
* **runtime:** fall back to the trigger-id Pipelines key when resolving the resume definition, fixing vacuous no-op resumes for hand-built workflows (#review-20260910-022)

## [1.1.1](https://github.com/asaidimu/hermes/compare/v1.1.0...v1.1.1) (2026-09-01)


### Bug Fixes

* fix issues ([5ac2f50](https://github.com/asaidimu/hermes/commit/5ac2f506bc543045fe76bf051a329b823f241a32))

# [1.1.0](https://github.com/asaidimu/hermes/compare/v1.0.0...v1.1.0) (2026-08-27)


### Bug Fixes

* **runtime:** implement fork and join orchestration ([6b68fae](https://github.com/asaidimu/hermes/commit/6b68fae10be6964fc1e6b2c6239da7605762abbc))


### Features

* **compiler:** implement fork-join and distribute node patterns ([ea86e00](https://github.com/asaidimu/hermes/commit/ea86e0058092c184720da4bbd90c7baa59957afd))

# 1.0.0 (2026-08-26)


### Bug Fixes

* **nodes:** add pause node and implement event-based orchestration ([3a5d8e8](https://github.com/asaidimu/hermes/commit/3a5d8e8433a7cfda4d4373e33c0b06175ca31deb))


### Features

* Initial commit ([2430aa3](https://github.com/asaidimu/hermes/commit/2430aa3d0b296d16b9faa4f8231ec1c7025da872))
