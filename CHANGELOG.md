## [Unreleased]

### Bug Fixes

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
