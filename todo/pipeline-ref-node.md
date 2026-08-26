# Pipeline-Ref Node + Compile-Time Config Validation

## Context

`pipeline-ref` is currently a compiler special-case (`compiler.go:428`) with no proper node implementation:
- No Go package in `pkg/nodes/`
- No TS counterpart
- No entry in `nodes.go` registry
- Not discoverable by the frontend (`GET /registry`)
- No `HANDLES` for the UI canvas

Additionally, config validation is incomplete:
- `CoerceConfig` applies defaults and type coercion but ignores `Required()` validators
- The compiler never validates node configs against their schemas at compile time
- Missing required fields (like `pipelineId`) only fail at runtime

## Requirements

1. **Fresh state for sub-workflows**: Child gets its own state (not a clone of parent)
2. **Configurable initial state**: Node config specifies `initialState` mapped from parent
3. **Result merging**: Child's final state placed under `resultKey` on parent
4. **Compile-time validation**: Validate configs against schemas before building workflows

## Implementation Plan

### Phase 1: Compile-Time Config Validation

**File: `pkg/nodekit/config.go`**
- Add `ValidateConfig(raw map[string]any, rs *definition.ResolvedSchema) error` function
- Check required fields, validate types, run any schema validators
- Return descriptive error with field name and validation reason

**File: `pkg/compiler/compiler.go`**
- In `Compile()`, before building stages, validate each node's config against its schema
- Call `nodekit.ValidateConfig(node.Config, compiledSchema)` for every executable node
- Return error early if validation fails (with node ID and kind context)

### Phase 2: Fresh Store Helper

**File: `pkg/store/store.go`**
- Add `NewFreshStore(initialState map[string]any, schema ...*definition.CompiledSchema) *MemoryStore`
- Creates a new document with the given initial state (no parent cloning)
- If initialState is nil, creates empty store

### Phase 3: Pipeline-Ref Node

**File: `pkg/nodes/pipeline-ref/pipeleref.go`** (new)
- `NodeDefinition` with:
  - `Kind: "pipeline-ref"`
  - `ConfigSchema` with `pipelineId` (required), `initialState` (optional object), `resultKey` (optional string)
  - `Handles`: target (input), success, failure
  - `Run`: no-op (sub-pipeline runs at stage level)
  - `Router`: routes based on sub-pipeline result status

**File: `pkg/nodes/pipeline-ref/pipeleref.ts`** (new)
- `NodeDef` type definition matching Go implementation
- Handle specs for UI canvas
- Config schema with field definitions

**File: `pkg/nodes/nodes.go`**
- Add import for `pipeline-ref` package
- Register in `init()` function

### Phase 4: Sub-Pipeline Fresh State + Result Merging

**File: `pkg/pipeline/types.go`**
- Add `Config map[string]any` field to `Stage` struct
- Stores `initialState` and `resultKey` for sub-pipeline stages

**File: `pkg/pipeline/subpipeline.go`**
- Modify `ExecuteSubPipelines` signature to accept `initialState map[string]any`
- Create fresh store with `store.NewFreshStore(initialState)` instead of cloning parent
- Return child's `FinalDoc` in `PipelineRunResult` (already done)

**File: `pkg/pipeline/context.go`**
- After `ExecuteSubPipelines` returns, check if stage has `resultKey` in Config
- If `resultKey` is set and child succeeded, merge child's `FinalDoc` into parent under `resultKey`
- If child failed, merge `{ error: <details> }` under `resultKey`
- Merge happens BEFORE router evaluation

### Phase 5: Compiler Cleanup

**File: `pkg/compiler/compiler.go`**
- Remove the `if node.Kind == "pipeline-ref"` special-case
- Instead, handle `pipeline-ref` through normal node definition path
- Read `initialState` and `resultKey` from node config
- Store them in `stage.Config` for runtime use
- Use node's `PipelinesRouter` for routing (success/failure handles)

### Phase 6: Tests

**File: `pkg/compiler/compiler_test.go`**
- Remove test stub registration for `pipeline-ref`
- Add tests for:
  - `pipeline-ref` with valid config compiles successfully
  - `pipeline-ref` missing `pipelineId` fails at compile time
  - Config validation catches missing required fields
  - Fresh state isolation (child doesn't inherit parent state)
  - Result merging into parent under `resultKey`

**File: `pkg/nodes/nodes_test.go`**
- Update `TestRegisteredKinds` to include `pipeline-ref`
- Verify `pipeline-ref` schema compiles

## Data Flow

```
Parent state: { invoiceId: "INV-001", amount: 500, ... }
       ↓ (interpolate initialState config at runtime)
Child fresh state: { id: "INV-001", total: 500 }    ← NEW document
       ↓ (child runs)
Child final state: { id: "INV-001", total: 500, validated: true, tax: 50 }
       ↓ (merge into parent)
Parent state: { invoiceId: "INV-001", amount: 500, ...,
                validationResult: { id: "INV-001", total: 500, validated: true, tax: 50 } }
```

## Node Config Example

```json
{
  "pipelineId": "validate-invoice",
  "initialState": {
    "invoiceId": "{{ state.currentInvoiceId }}",
    "amount": "{{ state.totalAmount }}"
  },
  "resultKey": "validationResult"
}
```

## Open Questions

1. Should `initialState` interpolation happen at compile time or runtime? → Runtime (values change between runs)
2. Should failed children still merge under `resultKey`? → Yes, with `{ error: ... }` so parent always has a result
3. Should `resultKey` be required? → No, omitting it means fire-and-forget (no merge)
