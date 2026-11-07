# Task: Explain RSP + build cross-process recovery example

- [*] Study RSP engine (pkg/pipeline: stage.go atomic commits, types.go routing
      instructions, workflow.go) + docs/RECOVERY.md (durable contract, Recover
      classifications: terminal/paused/in-flight) + tests/restart_durability_test.go
      (in-process acceptance) + pkg/actionlog/anansi.go + anansi_test.go (sqlite setup).
- [*] Explain RSP mechanics and guarantees in chat.
- [*] Create `examples/process-recovery/main.go`
  - **Context:** Validates restart durability ACROSS REAL PROCESSES (shipped tests
    only restart runtimes in-process on MemoryActionLog; cross-process durability
    needs `actionlog.AnansiStore` over file-backed sqlite). Parent re-execs its own
    binary (`os.Executable`) as children.
  - **Details:**
    - Child modes: `serve <db> <block|exit>`, `resume <db> <runID>`, `crash <db>`,
      `inspect <db> <runID>`.
    - Scenario 1 dead start: serve/exit → fresh resume child → RECOVERED paused →
      FINAL succeeded x=1 y=2 (state rebuilt from log via Replayer).
    - Scenario 2 SIGKILL: serve/block → parent kills on live "PAUSED" line →
      fresh resume child → same terminal proof.
    - Scenario 3 mid-run kill: crash child SIGKILLs itself inside stage:a →
      fresh inspect child → INFLIGHT status=recording entries=3, Resume fails
      with NOT_FOUND (no checkpoint ⇒ no invented continuation).
    - Sqlite DSN `file:<path>?_busy_timeout=5000`; separate db per scenario.
  - **Files:** `examples/process-recovery/main.go`
- [*] Verify: `go build ./...`, `gofmt` clean, `go vet` clean, and
      `go run ./examples/process-recovery` → 3/3 scenarios PASS.
