package tests

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/nodekit"
	_ "github.com/asaidimu/hermes/pkg/nodes"
	ifnode "github.com/asaidimu/hermes/pkg/nodes/if"
)

// @note #review-20260910-020 todo status=open priority=P3 tags=#review,#testing : Assertion-free debug test — passes unconditionally
// @author hermes-review
//
// TestIfDbg (and its sibling TestIfDbg2 in ifdbg2_test.go) only t.Logf the
// values under inspection and contain zero assertions, so they can never
// fail and add permanent green noise to every CI run. They look like
// leftover scratch work from debugging the if-node router. Either delete
// them or promote the observations to real assertions (e.g. assert the
// routed handle for less_than/greater_than against expectations, which is
// exactly what their names hint at).
func TestIfDbg(t *testing.T) {
	for _, op := range []string{"less_than", "greater_than"} {
		h, err := ifnode.Node.Router(context.Background(), nodekit.NodeRunContext{
			Config: map[string]any{"conditions": []any{
				map[string]any{"field": "total", "operator": op, "value": "70"},
			}},
			State: map[string]any{"total": float64(76)},
		})
		t.Logf("field=total op=%s -> handle=%q err=%v", op, h, err)
	}
}
