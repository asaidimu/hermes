package nodekit

import (
	"context"
	"testing"

	"github.com/asaidimu/hermes/pkg/store"
)

// TestEffectZeroValueIsUnspecified locks in the safety property the whole
// classification scheme depends on: a node that never sets Effect must not
// silently be treated as Pure (which would make replay silently re-execute
// a side-effecting node) or as SideEffecting-by-accident (which would
// silently defeat classification for nodes that do set it correctly, if
// the zero value happened to collide with a real value). The zero value
// must be its own distinct, invalid state.
func TestEffectZeroValueIsUnspecified(t *testing.T) {
	var e Effect
	if e != EffectUnspecified {
		t.Fatalf("zero value of Effect = %v, want EffectUnspecified", e)
	}
	if e.Valid() {
		t.Fatal("zero value of Effect must not be Valid()")
	}
}

func TestEffectValid(t *testing.T) {
	cases := []struct {
		e    Effect
		want bool
	}{
		{EffectUnspecified, false},
		{EffectPure, true},
		{EffectSideEffecting, true},
		{Effect(99), false}, // out-of-range values are never valid
	}
	for _, c := range cases {
		if got := c.e.Valid(); got != c.want {
			t.Errorf("Effect(%d).Valid() = %v, want %v", c.e, got, c.want)
		}
	}
}

func TestEffectString(t *testing.T) {
	cases := []struct {
		e    Effect
		want string
	}{
		{EffectUnspecified, "Unspecified"},
		{EffectPure, "Pure"},
		{EffectSideEffecting, "SideEffecting"},
		{Effect(99), "Unspecified"}, // unknown values render as Unspecified, not garbage
	}
	for _, c := range cases {
		if got := c.e.String(); got != c.want {
			t.Errorf("Effect(%d).String() = %q, want %q", c.e, got, c.want)
		}
	}
}

// TestRegisterPanicsOnUnspecifiedEffect is the enforcement mechanism: a node
// with no explicit Effect must never make it into the registry silently.
func TestRegisterPanicsOnUnspecifiedEffect(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Register did not panic for a NodeDefinition with unspecified Effect")
		}
	}()
	Register(NodeDefinition{
		Kind: "test-unspecified-effect-node",
		// Effect intentionally left unset (zero value).
	})
}

func TestRegisterAcceptsExplicitPure(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Register panicked for a node with EffectPure: %v", r)
		}
	}()
	Register(NodeDefinition{
		Kind:   "test-pure-effect-node",
		Effect: EffectPure,
	})
	if _, ok := Get("test-pure-effect-node"); !ok {
		t.Fatal("node was not registered")
	}
}

func TestRegisterAcceptsExplicitSideEffecting(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Register panicked for a node with EffectSideEffecting: %v", r)
		}
	}()
	Register(NodeDefinition{
		Kind:   "test-sideeffecting-effect-node",
		Effect: EffectSideEffecting,
	})
	if _, ok := Get("test-sideeffecting-effect-node"); !ok {
		t.Fatal("node was not registered")
	}
}

// TestDefinePropagatesEffect verifies the typed Define[C] path threads
// Effect through to the erased NodeDefinition unchanged, and that an
// explicit, valid Effect on TypedDefinition survives registration without
// panicking.
func TestDefinePropagatesEffect(t *testing.T) {
	type cfg struct {
		Name string `config:"name"`
	}
	def := Define(TypedDefinition[cfg]{
		Kind:   "test-typed-effect-node",
		Label:  "Test Typed Effect Node",
		Effect: EffectSideEffecting,
		Run: func(ctx context.Context, nCtx *TypedRunContext[cfg]) (store.Mutator, error) {
			return nil, nil
		},
	})
	if def.Effect != EffectSideEffecting {
		t.Fatalf("Define did not propagate Effect: got %v, want EffectSideEffecting", def.Effect)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Register panicked for a Define()-produced node with a valid Effect: %v", r)
		}
	}()
	Register(def)
}

// TestDefinePanicsOnUnspecifiedEffect ensures the typed path can't bypass
// the same enforcement the untyped path gets — a TypedDefinition that never
// sets Effect must still fail loudly at registration.
func TestDefinePanicsOnUnspecifiedEffect(t *testing.T) {
	type cfg struct{}
	def := Define(TypedDefinition[cfg]{
		Kind:  "test-typed-unspecified-effect-node",
		Label: "Test Typed Unspecified Effect Node",
		// Effect intentionally left unset.
	})

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("Register did not panic for a Define()-produced node with unspecified Effect")
		}
	}()
	Register(def)
}
