# TODO: Generate publication-ready README.md

- [*] Analyze codebase context (go.mod, package.json, SPEC.md, WIP.md, pkg/ layout, src/, examples/, CI workflows)
- [*] Populate README.md using the readme-generator skill template (overview, install, usage, architecture, contributing, troubleshooting, license) with facts inferred from the repo — no placeholders
  - **Context:** `README.md` currently contains only the `# hermes` title. Hermes is a polyglot workflow engine (Go RSP runtime + JS node catalog published as `@asaidimu/hermes`).
  - **Details:** Project type = Library/API + CLI-ish example server. Cover: embeddable Go facade (`pipelines.go`), REST server wire parity (`pkg/server`), node catalog (`pkg/nodes/*`, 15 kinds), TS package build (`bun run build`), REST endpoint table from SPEC.md §2.4, verification commands (`go test ./...`, `go test -race ./...`, `go run ./examples/server`). License: MIT-style text in LICENSE.md; npm marked UNLICENSED/private.
  - **Files:** Modify `README.md`.
