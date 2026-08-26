package switchnode

import (
	"context"
	"encoding/json"

	"github.com/asaidimu/hermes/pkg/expr"
	"github.com/asaidimu/hermes/pkg/nodekit"
)

type SwitchConfig struct {
	Value         string `config:"value" anansi:"default=state.value"`
	Cases         string `config:"cases" anansi:"default=[]"`
	DefaultHandle string `config:"defaultHandle" anansi:"default=default"`
}

var Node = nodekit.Define(nodekit.TypedDefinition[SwitchConfig]{
	Kind:        "switch",
	Label:       "Switch",
	Description: "Match a workflow state value against several static cases to branch paths.",
	Type:        "executable",
	Handles: func(cfg *SwitchConfig) []nodekit.HandleSpec {
		specs := []nodekit.HandleSpec{{Type: nodekit.HandleTarget, ID: "", Label: "in"}}
		var parsed any
		if cfg.Cases != "" {
			_ = json.Unmarshal([]byte(cfg.Cases), &parsed)
		}
		switch items := parsed.(type) {
		case []any:
			for _, item := range items {
				m, ok := item.(map[string]any)
				if !ok {
					continue
				}
				id, ok := m["id"].(string)
				if !ok || id == "" {
					continue
				}
				label := ""
				if l, ok := m["label"].(string); ok {
					label = l
				}
				if label == "" {
					label = `""`
				}
				specs = append(specs, nodekit.HandleSpec{Type: nodekit.HandleSource, ID: id, Label: label})
			}
		case map[string]any:
			for match, label := range items {
				specs = append(specs, nodekit.HandleSpec{
					Type:  nodekit.HandleSource,
					ID:    stringLabel(label),
					Label: match,
				})
			}
		}
		if cfg.DefaultHandle != "" {
			specs = append(specs, nodekit.HandleSpec{Type: nodekit.HandleSource, ID: cfg.DefaultHandle, Label: "default"})
		}
		return specs
	},
	HandlesJS: `(config) => {
  const specs = [{ type: "target", id: "", label: "in" }];
  try {
    const parsed = JSON.parse(config.cases || "[]");
    if (Array.isArray(parsed)) {
      parsed.forEach((item) => {
        if (item.id) {
          specs.push({ type: "source", id: String(item.id), label: item.label === "" ? "\"\"" : String(item.label) });
        }
      });
    } else {
      for (const [match, label] of Object.entries(parsed)) {
        specs.push({ type: "source", id: String(label), label: String(match) });
      }
    }
  } catch {}
  if (config.defaultHandle) {
    specs.push({ type: "source", id: String(config.defaultHandle), label: "default" });
  }
  return specs;
}`,
	Router: router,
})

// router mirrors the TS switch node: evaluates `value` as a JS expression and
// matches String(evaluated) against the parsed cases, returning the matched case
// id, the defaultHandle, or undefined-equivalent (empty) on error.
func router(ctx context.Context, nCtx *nodekit.TypedRunContext[SwitchConfig]) (string, error) {
	cfg := nCtx.Config
	valueExpr := cfg.Value
	if valueExpr == "" {
		valueExpr = "state.value"
	}
	valueExpr = expr.StatePathExpr(valueExpr)
	defaultHandle := cfg.DefaultHandle

	evaluated, err := expr.EvalValue(ctx, valueExpr, nCtx.State)
	if err != nil {
		return defaultHandle, nil
	}
	targetStr := expr.String(evaluated)

	var parsed any
	if cfg.Cases != "" {
		if err := json.Unmarshal([]byte(cfg.Cases), &parsed); err != nil {
			return defaultHandle, nil
		}
	}
	switch items := parsed.(type) {
	case []any:
		for _, item := range items {
			m, ok := item.(map[string]any)
			if !ok {
				continue
			}
			match, _ := m["match"].(string)
			if expr.String(match) == targetStr {
				if id, ok := m["id"].(string); ok && id != "" {
					return id, nil
				}
			}
		}
	case map[string]any:
		if label, ok := items[targetStr]; ok {
			return stringLabel(label), nil
		}
	}

	return defaultHandle, nil
}

func stringLabel(v any) string {
	switch s := v.(type) {
	case string:
		return s
	default:
		b, err := json.Marshal(v)
		if err != nil {
			return ""
		}
		return string(b)
	}
}
