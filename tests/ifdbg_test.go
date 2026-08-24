package tests

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/nodekit"
	_ "github.com/asaidimu/hermes/pkg/nodes"
	ifnode "github.com/asaidimu/hermes/pkg/nodes/if"
)

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
