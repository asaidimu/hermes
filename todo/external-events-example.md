# Task: External-events demo (pause/resume + recovery) over anansi codec IPC

- [*] Create `examples/external-events/main.go`
  - **Context:** Runtime in a CHILD process; parent drives it over stdin/stdout
    with the anansi codec. Child plugs a custom `runtime.EventSource` whose
    `params.Emit` is fed by decoded IPC "event" messages — trigger AND resume
    events are TRULY EXTERNAL.
- [*] Fix the anansi/json mixing mistake (user-reported)
  - **Mistake:** the first version built the wire path as
    json.Marshal → cjson.DecodeJSONInto → SerializeAnansi and
    DecodeAnansi → cjson.SerializeJSON → json.Unmarshal — using the JSON
    codec as a struct↔container bridge. That's why IPCMessage needed BOTH
    json and anansi tags (json.Marshal used json tags; DecodeJSONInto matched
    schema names from anansi tags — the mismatch silently produced empty
    4-byte packets).
  - **Fix — anansi alone, via the native binding API:**
    - struct → container: `document.DocumentPool.FromStruct` (anansi tags)
    - container → wire:   `Document.ToAnansi(0)`
    - wire → container:   `DocumentPool.FromAnansi`
    - container → struct: `Document.BindTo` (anansi tags, no map materialization)
    - pool built once: `document.NewDocumentPoolFromJSON(data.ExtractDTOSchemaDirect(&IPCMessage{}))`
    - IPCMessage now carries ONLY anansi tags.
  - **Where JSON legitimately remains:** the `payload`/`state` fields are
    opaque app-level envelopes (arbitrary map[string]any for the runtime's
    event API); heterogeneous maps have no static anansi schema, so they ride
    as strings and are parsed with encoding/json only at the emit seam /
    result printing — application concern, not the wire protocol.
- [*] Verify: gofmt clean, `go vet ./examples/...` clean, `go build ./...`
      clean, `go run ./examples/external-events` → 2/2 PASS.
