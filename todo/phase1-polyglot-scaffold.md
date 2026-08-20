# Phase 1 — Polyglot scaffold & nodekit restructure

**Context:** Hermes is a polyglot port of the TS RSP workflow engine. Per user decision,
each node lives in its own package `pkg/nodes/<kind>/` containing BOTH `<kind>.go`
(Go execution) and `<kind>.ts` (JS node definition for the `@asaidimu/hermes` npm
package, published from the repo root). The Go binary must be fully self-contained
(no `//go:embed`, no committed assets, no npm step). JS definitions are the only
source for handles/catalog and ship via the npm package. Each language's toolchain
aggregates its own source: Go via `pkg/nodes` aggregator; TS via `scripts/build.ts`.

Reference source of truth (parity targets): `~/projects/utils/src/workflows/nodes/*/index.ts`
and `~/projects/utils/src/workflows/schema.ts`. These are being replaced by this repo's
per-kind packages once parity lands.

---

- [-] Update `WIP.md` to reflect the no-embed decision (drop assets/, go:embed, /handles.js
      & /registry endpoints, "commit+embed" decision; add "npm package = only JS source")
  - **Context:** User rejected embedding JS in the Go binary; consumers install the package.
  - **Files:** `WIP.md`

- [ ] Create `pkg/nodekit` — shared node types + registry
  - **Context:** Must avoid an import cycle: `pkg/nodes` (aggregator) imports each
    `pkg/nodes/<kind>`, so subpackages must NOT import `pkg/nodes`. Shared types live here.
  - **Details:** Move current `pkg/nodes/nodes.go` content (HandleType/HandleKind/
    HandleSpec/NodeRunContext/NodeRunner/NodeRouter/NodeDefinition/Register/Get/
    Registry/BuildStep). Extend `NodeDefinition` with `Type` ("executable"|"resource"),
    `Scope`, `BodyHandle`, and optional resource init/cleanup hook fields. Keep
    `BuildStep` producing `pipeline.Step` (no-op when `Run` is nil).
  - **Files:** create `pkg/nodekit/nodekit.go`

- [ ] Create per-kind Go node packages (14) under `pkg/nodes/<kind>/`
  - **Context:** Each package declares `var Node = nodekit.NodeDefinition{...}` with
    faithful metadata + `Handles` func mirroring the TS defs. `Run`/`Router` stay nil
    (implemented in later phases). Dir names use hyphens; Go package names are valid
    identifiers (`for-each` -> `foreach`, `try-catch` -> `trycatch`).
  - **Files:** create `pkg/nodes/{trigger,if,switch,arithmetic,delay,code,http,gemini,
    for-each,while,try-catch,query,transformer,database}/*.go`

- [ ] Rewrite `pkg/nodes/nodes.go` as aggregator; delete old stub files
  - **Context:** Aggregator imports every `pkg/nodes/<kind>` and calls
    `nodekit.Register(<pkg>.Node)` in `init()`; re-exports nodekit symbols. Delete the
    old `package nodes` stubs (`arithmetic.go`, `delay.go`, `if.go`, `transform.go`) —
    their logic moves into the per-kind packages.
  - **Files:** rewrite `pkg/nodes/nodes.go`; delete `pkg/nodes/{arithmetic,delay,if,transform}.go`

- [ ] Make `go build ./...` and `go vet ./...` pass
  - **Context:** Build currently fails on empty `pkg/compiler/compiler.go` and
    `pkg/runtime/runtime.go` (0 bytes -> "expected 'package', found 'EOF'"). Add minimal
    `package compiler` / `package runtime` declarations (bodies come in later phases).
  - **Files:** `pkg/compiler/compiler.go`, `pkg/runtime/runtime.go`

- [ ] Scaffold root npm package `@asaidimu/hermes`
  - **Context:** Package is published from the repo root, coexisting with `go.mod`.
    Toolchain is `bun` (bundler). Types via `bun build --dts` (fallback: skip dts).
  - **Details:** `package.json` (name `@asaidimu/hermes`, `type: module`, exports map,
    `files: ["dist"]`, scripts `build`/`typecheck`); `tsconfig.json`; update `.gitignore`
    (add `node_modules/`, `dist/`, `src/generated.ts`).
  - **Files:** `package.json`, `tsconfig.json`, `.gitignore`

- [ ] Write `src/types.ts`, `src/serialize.ts`, `src/index.ts`
  - **Context:** Canonical wire/type layer consumed by the per-kind `.ts` defs and the
    published package. `src/generated.ts` (gitignored, produced by build) is re-exported
    by `index.ts` as `NODE_DEFS`/`HANDLES`/`CATALOG`.
  - **Details:** `types.ts`: HandleType/HandleKind/HandleSpec/NodeDef/NodeCatalogEntry/
    ConfigSchema/ConfigField + wire types (WorkflowNode/WorkflowEdge/PipelineEvent/
    RunOutcome/RunTimelineMeta/WorkflowTrigger/WorkflowState). `serialize.ts`:
    `buildHandlesJS` (function per kind, matches client `new Function("return ("+code+")")`
    contract) + `buildRegistryJSON` (catalog entries with defaults collected from
    configSchema). `index.ts`: re-exports everything.
  - **Files:** create `src/types.ts`, `src/serialize.ts`, `src/index.ts`

- [ ] Write per-kind TS node definitions (14) under `pkg/nodes/<kind>/<kind>.ts`
  - **Context:** Each file exports `xxxNode: NodeDef` with faithful `configSchema`,
    `handles` (static or function), `type`, and `bodyHandle` where applicable, ported
    from the utils node defs. `switch` handles are dynamic from `config.cases`
    (array: `item.id` / object: entry keys) + `defaultHandle`. `query` carries the
    `service` resource target; `database` (resource) carries the `db` resource source.
  - **Files:** create `pkg/nodes/{...}/*.ts` (14 files)

- [ ] Write `scripts/build.ts` and verify the package builds
  - **Context:** The TS toolchain aggregator. Step 1: glob `pkg/nodes/*/*.ts`, emit
    `src/generated.ts` importing each `nodeDef` and building `NODE_DEFS`/`HANDLES`.
    Step 2: `Bun.build` `src/index.ts` -> `dist/` (esm + cjs), attempt `--dts`.
  - **Details:** Run `bun install` then `bun run build`; verify `dist/` + `src/generated.ts`.
  - **Files:** create `scripts/build.ts`

- [ ] Review `git status` / `git diff` (do NOT commit unless asked)

---

## Post-Phase-1 follow-ups (not in this task)
- Implement Go `Run`/`Router` per node (Phase 2), `pkg/expr` goja wrapper (Phase 3),
  `pkg/compiler` (Phase 4), `pkg/runtime` (Phase 5), event-wire parity (Phase 6),
  server adapter (Phase 7), verification (Phase 8). See `WIP.md`.