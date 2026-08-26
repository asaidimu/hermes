# Run Workflow with While Loop, Fork, and Join

- [*] Create a test or runner script to execute the JSON workflow
  - **Context:** Test the execution of a workflow containing trigger, transformer (setting total=11), while loop (subtracting 1 until total < 10, i.e. 11 -> 10 -> 9), delay, fork (parallel branches computing total*2 and total*3), and join.
  - **Details:** Parse the JSON workflow into compiler nodes and edges, subscribe to stage events to log every stage execution, and assert final state contains `twice: 18` and `thrice: 27`.
  - **Files:** `tests/fork_while_workflow_test.go`

- [-] Fix compiler fork branch subpipeline extraction and skipping
  - **Context:** `compiler.go` was skipping branch nodes due to `forkBranches` check and including `join` inside the branch sub-pipelines.
  - **Details:** Implement `bfsForkBranchNodes` stopping at `joinID` and pass `nil` for `forkBranches` during branch recursive compilation.
  - **Files:** `pkg/compiler/compiler.go`

- [ ] Implement state merging on fork subpipeline completion
  - **Context:** Fork branches execute concurrently on cloned stores; when joining, child state mutations (such as `twice` and `thrice`) need to merge back into parent store.
  - **Details:** Update `pkg/pipeline/context.go` to merge non-system keys from successful branch `FinalState`s into parent store.
  - **Files:** `pkg/pipeline/context.go`

- [ ] Re-run test and verify all stages and final state values
  - **Context:** Verify `stage:start`, `step:start`, `step:success`, `subpipeline:fork/join` for all branches and assert `twice: 18` and `thrice: 27`.
  - **Details:** Run `go test -v ./tests -run TestRunForkWhileWorkflow`.
