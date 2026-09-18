package tests

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/nodekit"
	_ "github.com/asaidimu/hermes/pkg/nodes"
	ifnode "github.com/asaidimu/hermes/pkg/nodes/if"
)

// @note #review-20260910-020 todo P3 resolved status=resolved priority=P3 tags=#review,#testing : Assertion-free debug test — passes unconditionally
// @author hermes-review
//
// Resolved: promoted the observations to real assertions on the routed
// handle, matching what less_than/greater_than are supposed to produce
// for state.total=76 against the threshold value "70".
func TestIfDbg(t *testing.T) {
	cases := []struct {
		op     string
		expect string
	}{
		{"less_than", "else"},
		{"greater_than", "if"},
	}
	for _, tc := range cases {
		h, err := ifnode.Node.Router(context.Background(), nodekit.NodeRunContext{
			Config: map[string]any{"conditions": []any{
				map[string]any{"field": "total", "operator": tc.op, "value": "70"},
			}},
			State: map[string]any{"total": float64(76)},
		})
		if err != nil {
			t.Fatalf("field=total op=%s: unexpected error: %v", tc.op, err)
		}
		if h != tc.expect {
			t.Errorf("field=total op=%s: got handle %q, want %q", tc.op, h, tc.expect)
		}
	}
}
