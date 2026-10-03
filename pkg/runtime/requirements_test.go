package runtime

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/compiler"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/store"
)

// testSecrets is a static credential store for requirement tests, expressed
// as the Options func fields that replaced the SecretProvider interface
// (plan R.3).
func testSecrets(keys map[string]string) (func(context.Context, string) (any, bool), func(context.Context, string) bool) {
	get := func(_ context.Context, key string) (any, bool) {
		v, ok := keys[key]
		return v, ok
	}
	has := func(_ context.Context, key string) bool {
		_, ok := keys[key]
		return ok
	}
	return get, has
}

func init() {
	// A node that reads env + secret from context and records whether the
	// secret leaked into state.
	nodekit.Register(nodekit.NodeDefinition{
		Kind:   "req-test",
		Effect: nodekit.EffectPure,
		Label:  "Requirement Test",
		Type:   "executable",
		ConfigSchema: mustRawSchema(`{
			"version": "1.0.0",
			"name": "req-test",
			"fields": {
				"outKey": { "name": "outKey", "type": "string", "default": "got", "required": true }
			}
		}`),
		Requirements: []nodekit.Requirement{
			{Kind: nodekit.ReqEnv, Key: "REQ_TEST_ENV", Required: true, Description: "test env key"},
			{Kind: nodekit.ReqSecret, Key: "REQ_TEST_SECRET", Required: true, Description: "test secret"},
		},
		Handles: func(map[string]any) []nodekit.HandleSpec {
			return []nodekit.HandleSpec{{Type: nodekit.HandleTarget, ID: ""}, {Type: nodekit.HandleSource, ID: ""}}
		},
		Run: func(ctx context.Context, nCtx nodekit.NodeRunContext) (store.Mutator, error) {
			outKey, _ := nCtx.Config["outKey"].(string)
			envVal, _ := nCtx.Env["REQ_TEST_ENV"].(string)
			secretVal, ok := nCtx.Secret("REQ_TEST_SECRET")
			if !ok {
				return nil, errString("secret REQ_TEST_SECRET not resolvable at execution time")
			}
			return store.SetValue(outKey, map[string]any{
				"env":    envVal,
				"secret": secretVal,
			}), nil
		},
	})
}

func mustRawSchema(s string) []byte { return []byte(s) }

type errString string

func (e errString) Error() string { return string(e) }

func reqTestNodes() ([]compiler.Node, []compiler.Edge) {
	nodes := []compiler.Node{
		{ID: "trigger-1", Type: compiler.NodeExecutable, Kind: "trigger", Config: map[string]any{}},
		{ID: "req-1", Type: compiler.NodeExecutable, Kind: "req-test", Config: map[string]any{"outKey": "got"}},
	}
	edges := []compiler.Edge{
		{ID: "e1", Source: "trigger-1", Target: "req-1", Role: compiler.EdgeFlow},
	}
	return nodes, edges
}

func TestValidateRequirementsMissingKeys(t *testing.T) {
	rt := NewWorkflowRuntime(Options{})
	defer rt.Shutdown(context.Background())
	nodes, edges := reqTestNodes()
	wf, err := compiler.Compile(nodes, edges, nil)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if len(wf.Requirements) != 2 {
		t.Fatalf("expected 2 aggregated requirements, got %d: %+v", len(wf.Requirements), wf.Requirements)
	}

	err = rt.ValidateWorkflowRequirements(wf)
	if err == nil {
		t.Fatal("expected validation failure with no Env/Secrets configured")
	}
	for _, want := range []string{"env:REQ_TEST_ENV", "secret:REQ_TEST_SECRET"} {
		if !contains(err.Error(), want) {
			t.Errorf("error %q missing %q", err.Error(), want)
		}
	}

	// Registration must be refused too.
	rn, re := reqTestNodes()
	if _, runErr := rt.Run(context.Background(), rn, re); runErr == nil {
		t.Fatal("expected Run to fail requirement validation")
	}
}

func TestRegisterSucceedsWhenSatisfied(t *testing.T) {
	rt := NewWorkflowRuntime(Options{
		Env:       map[string]any{"REQ_TEST_ENV": "prod-value"},
		SecretGet: mustSecretGet(t, map[string]string{"REQ_TEST_SECRET": "s3cr3t"}),
		SecretHas: mustSecretHas(t, map[string]string{"REQ_TEST_SECRET": "s3cr3t"}),
	})
	defer rt.Shutdown(context.Background())
	nodes, edges := reqTestNodes()
	wf, err := compiler.Compile(nodes, edges, nil)
	if err != nil {
		t.Fatalf("compile: %v", err)
	}
	if err := rt.ValidateWorkflowRequirements(wf); err != nil {
		t.Fatalf("expected satisfied requirements, got %v", err)
	}
}

func TestStepReadsEnvAndSecret(t *testing.T) {
	rt := NewWorkflowRuntime(Options{
		Bus:       events.NewMemoryScopedBus(),
		Env:       map[string]any{"REQ_TEST_ENV": "prod-value"},
		SecretGet: mustSecretGet(t, map[string]string{"REQ_TEST_SECRET": "s3cr3t"}),
		SecretHas: mustSecretHas(t, map[string]string{"REQ_TEST_SECRET": "s3cr3t"}),
		ActionLog: nil,
	})
	defer rt.Shutdown(context.Background())

	rn, re := reqTestNodes()
	res, err := rt.Run(context.Background(), rn, re)
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if res.Status != "succeeded" {
		t.Fatalf("status = %s, err = %v", res.Status, res.Error)
	}

	st := rt.Store(res.RunID)
	var got map[string]any
	if err := st.Read(func(state map[string]any) error {
		got, _ = state["got"].(map[string]any)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if got == nil || got["env"] != "prod-value" || got["secret"] != "s3cr3t" {
		t.Fatalf("step did not receive env/secret via context: %v", got)
	}

	// The secret VALUE must never leak into persisted state keys.
	snap, _ := st.ExportJSON()
	for k, v := range snap {
		if s, ok := v.(string); ok && s == "s3cr3t" && k != "got" {
			t.Errorf("secret leaked into state key %q", k)
		}
	}
	_ = contains
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || (len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0)
}

func indexOf(h, n string) int {
	for i := 0; i+len(n) <= len(h); i++ {
		if h[i:i+len(n)] == n {
			return i
		}
	}
	return -1
}

// mustSecretGet/mustSecretHas adapt the static map into the Options func
// fields (R.3); separate helpers keep the Options literals tidy.
func mustSecretGet(t *testing.T, keys map[string]string) func(context.Context, string) (any, bool) {
	get, _ := testSecrets(keys)
	return get
}

func mustSecretHas(t *testing.T, keys map[string]string) func(context.Context, string) bool {
	_, has := testSecrets(keys)
	return has
}
