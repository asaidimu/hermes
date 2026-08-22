package nodekit

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/dop251/goja"
)

// TestHandlesJSParity verifies that each node's HandlesJS produces the same
// handle specs as the Go Handles function when evaluated with goja.
func TestHandlesJSParity(t *testing.T) {
	reg := Registry()
	kinds := make([]string, 0, len(reg))
	for k := range reg {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)

	for _, kind := range kinds {
		def := reg[kind]
		if def.HandlesJS == "" {
			t.Skipf("%s: no HandlesJS defined", kind)
			continue
		}

		t.Run(kind, func(t *testing.T) {
			// Go handles
			goSpecs := def.Handles(nil)
			goJSON, err := json.Marshal(goSpecs)
			if err != nil {
				t.Fatalf("marshal Go specs: %v", err)
			}

			// JS handles via goja
			vm := goja.New()
			fn, err := vm.RunString("(" + def.HandlesJS + ")")
			if err != nil {
				t.Fatalf("parse HandlesJS: %v", err)
			}
			fnVal, ok := goja.AssertFunction(fn)
			if !ok {
				t.Fatal("HandlesJS is not callable")
			}

			// Pass an empty config for static handlers, or a test config for switch
			configVal := vm.NewObject()
			if kind == "switch" {
				configVal.Set("cases", `[{"match":"a","id":"a_case"},{"match":"b","id":"b_case"}]`)
				configVal.Set("defaultHandle", "default")
			}
			result, err := fnVal(goja.Undefined(), configVal)
			if err != nil {
				t.Fatalf("execute HandlesJS: %v", err)
			}

			// Export and re-marshal JS result
			jsExported := result.Export()
			jsJSON, err := json.Marshal(jsExported)
			if err != nil {
				t.Fatalf("marshal JS result: %v", err)
			}

			// Parse both into comparable structures
			var goSpecsParsed, jsSpecsParsed []map[string]any
			if err := json.Unmarshal(goJSON, &goSpecsParsed); err != nil {
				t.Fatalf("unmarshal Go specs: %v", err)
			}
			if err := json.Unmarshal(jsJSON, &jsSpecsParsed); err != nil {
				t.Fatalf("unmarshal JS specs: %v", err)
			}

			// For switch with test config, just verify it produces 3 handles (in, a_case, b_case, default = 4)
			if kind == "switch" {
				if len(jsSpecsParsed) != 4 {
					t.Errorf("switch: expected 4 handles (in + 2 cases + default), got %d: %s", len(jsSpecsParsed), jsJSON)
				}
				return
			}

			// Compare lengths
			if len(goSpecsParsed) != len(jsSpecsParsed) {
				t.Errorf("handle count mismatch: Go=%d JS=%d\nGo:  %s\nJS:  %s",
					len(goSpecsParsed), len(jsSpecsParsed), goJSON, jsJSON)
				return
			}

			// Compare each handle
			for i := range goSpecsParsed {
				goH := goSpecsParsed[i]
				jsH := jsSpecsParsed[i]
				if goH["type"] != jsH["type"] {
					t.Errorf("[%d] type mismatch: Go=%v JS=%v", i, goH["type"], jsH["type"])
				}
				if goH["id"] != jsH["id"] {
					t.Errorf("[%d] id mismatch: Go=%v JS=%v", i, goH["id"], jsH["id"])
				}
				goLabel, _ := goH["label"].(string)
				jsLabel, _ := jsH["label"].(string)
				if goLabel != jsLabel {
					t.Errorf("[%d] label mismatch: Go=%q JS=%q", i, goLabel, jsLabel)
				}
				goKind, _ := goH["kind"].(string)
				jsKind, _ := jsH["kind"].(string)
				if goKind != jsKind {
					t.Errorf("[%d] kind mismatch: Go=%q JS=%q", i, goKind, jsKind)
				}
			}
		})
	}
}
