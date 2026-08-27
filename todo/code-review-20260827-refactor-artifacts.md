# Code Review: Refactoring Artifacts & API Ergonomics (2026-08-27)

- [-] Investigate refactoring artifacts, naming compromises, and type safety across hermes
  - **Context:** Multiple refactors have led to naming tricks, dual API paths (e.g. `TypedRunContext` vs `NodeRunContext`), redundant abstractions, and inconsistent API surface.
  - **Details:** Review `pkg/nodekit`, `pkg/pipeline`, `pkg/runtime`, `pkg/store`, `pkg/nodes`, `pkg/events`, `pkg/core`, `pkg/compiler`, etc.
  - **Files:** `pkg/nodekit/*.go`, `pkg/pipeline/*.go`, `pkg/runtime/*.go`, `pkg/nodes/**/*.go`, `pkg/store/*.go`, `pkg/events/*.go`.

- [ ] Analyze `pkg/nodekit` and `pkg/pipeline` context modeling
  - **Context:** `TypedRunContext[C]` was introduced alongside `NodeRunContext` instead of making typed context the first-class default.
  - **Details:** Trace how `TypedRunContext`, `NodeRunContext`, `pipeline.ExecutionContext`, `pipeline.NodeContext`, etc., interact and where type-safety or clarity is lost.

- [ ] Audit all packages for refactoring debris, SOLID violations, and naming compromises
  - **Context:** Identify places where legacy untyped signatures, wrapper boilerplate, or duplicate struct/interface hierarchies exist because of progressive refactoring.
  - **Details:** Check `pkg/nodekit`, `pkg/nodes`, `pkg/runtime`, `pkg/store`, `pkg/compiler`, `pkg/events`, `pkg/watch`.

- [ ] Write structured DevNotes at relevant source locations
  - **Context:** Capture findings directly in source files with proper devnotes grammar and valid metadata.
  - **Details:** Follow devnotes format with proper separator lines, `#review-20260827-xxx` IDs, priorities, categories, and tags. Run `devnotes check` and `devnotes index update`.

- [ ] Produce comprehensive review report artifact
  - **Context:** Summarize all findings, architectural observations, and recommended refactoring steps for the user.
