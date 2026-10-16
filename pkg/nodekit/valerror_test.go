package nodekit

// H6 acceptance (v2.1 hardening plan): typed validation errors at the
// build boundary. Hosts match on Code (+ Path) instead of parsing
// messages; messages keep their pre-H6 text; causes stay Unwrap-able.

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidationErrorSentinelsAndText(t *testing.T) {
	// Unknown node kind at the trigger boundary.
	_, err := BuildTrigger("node-1", NodeDefinition{Kind: "action"}, nil)
	require.Error(t, err)
	ve, ok := AsValidationError(err)
	require.True(t, ok, "BuildTrigger kind error must be a *ValidationError, got %T", err)
	require.Equal(t, ValErrUnknownNodeKind, ve.Code)
	require.Equal(t, "node-1", ve.Path)
	require.Contains(t, err.Error(), `Trigger node node-1 of kind "action" has no buildTrigger implementation`)
	require.Nil(t, ve.Cause, "plain valErr has no cause")

	// Invalid config schema through prepareNodeConfig — schema JSON that
	// does not compile. (BuildStep has no error return; it defers this
	// same error to the step attempt via the config plan.)
	_, err = prepareNodeConfig(NodeDefinition{
		Kind:         "http",
		ConfigSchema: []byte(`{"type": "not-a-real-schema-kind"`),
	}, nil, nil, nil, nil)
	require.Error(t, err)
	ve, ok = AsValidationError(err)
	require.True(t, ok, "schema error must be a *ValidationError, got %T", err)
	require.Equal(t, ValErrInvalidConfigSchema, ve.Code)
	require.Equal(t, "http", ve.Path)
	require.NotNil(t, ve.Cause, "schema errors wrap the parse failure")
	require.True(t, errors.Unwrap(err) != nil, "Error() chain must unwrap")

	// Message text preserved: Error() renders the pre-H6 text + cause.
	require.Contains(t, err.Error(), "node http has an invalid config schema")
}

func TestAsValidationErrorWalksWrappedChains(t *testing.T) {
	inner := valErr(ValErrInvalidConfig, "node-9", "config bind failed for node node-9")
	wrapped := fmt.Errorf("outer context: %w", fmt.Errorf("mid: %w", inner))
	ve, ok := AsValidationError(wrapped)
	require.True(t, ok, "AsValidationError must walk fmt-wrapped chains")
	require.Equal(t, ValErrInvalidConfig, ve.Code)
	require.Equal(t, "node-9", ve.Path)

	_, ok = AsValidationError(errors.New("plain"))
	require.False(t, ok, "plain errors report false")
	_, ok = AsValidationError(nil)
	require.False(t, ok, "nil reports false")
}

func TestValidationErrorUnwrapPreservesCause(t *testing.T) {
	cause := errors.New("the parse failure")
	ve := valErrWrap(ValErrInvalidConfigSchema, "kind-x", "node kind-x has an invalid config schema", cause)
	require.True(t, errors.Is(ve, cause), "errors.Is must reach the wrapped cause")
	require.Contains(t, ve.Error(), "node kind-x has an invalid config schema: the parse failure")
}
