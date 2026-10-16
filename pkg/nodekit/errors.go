package nodekit

import "github.com/asaidimu/hermes/pkg/core"

// SystemErrorJSON serializes an error into the TS SystemError.toJSON() shape.
// Implemented in pkg/core (pkg/pipeline needs it too, and pipeline imports
// nodekit — so it cannot live here). Re-exported for nodekit callers.
var SystemErrorJSON = core.SystemErrorJSON

// Sentinel codes for build-time validation failures (v2.1 H6, borrowed from
// the alternative design's §3.2 typed-error contract): hosts match on Code
// programmatically instead of parsing messages. Runtime EXECUTION errors
// keep their core.ErrCode* codes — these cover only the build/registration
// boundary hosts hit at workflow compile time.
const (
	// ValErrInvalidConfigSchema: the node's ConfigSchema JSON does not parse
	// or does not compile into a usable schema.
	ValErrInvalidConfigSchema = "INVALID_CONFIG_SCHEMA"
	// ValErrConfigInterpolation: config interpolation against state/
	// resources/results failed (a referenced path is missing, or the
	// interpolated value has the wrong shape).
	ValErrConfigInterpolation = "CONFIG_INTERPOLATION_FAILED"
	// ValErrInvalidConfig: the config failed schema validation or could not
	// be bound to the node's typed surface.
	ValErrInvalidConfig = "INVALID_CONFIG"
	// ValErrUnknownNodeKind: the node kind has no builder in the registry.
	ValErrUnknownNodeKind = "UNKNOWN_NODE_KIND"
)

// ValidationError is the typed build-time error. Path names the node (or
// the node kind when no node id is in scope); Message renders exactly the
// pre-H6 text so existing logs keep their shape; Cause is the underlying
// error (Unwrap-able, so errors.Is/As chains still reach it).
type ValidationError struct {
	Code    string
	Path    string
	Message string
	Cause   error
}

func (e *ValidationError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

func (e *ValidationError) Unwrap() error { return e.Cause }

// valErr builds a ValidationError.
func valErr(code, path, message string) *ValidationError {
	return &ValidationError{Code: code, Path: path, Message: message}
}

// valErrWrap builds a ValidationError wrapping an underlying cause, with
// the pre-H6 message text preserved via Error()'s ": cause" rendering.
func valErrWrap(code, path, message string, cause error) *ValidationError {
	return &ValidationError{Code: code, Path: path, Message: message, Cause: cause}
}

// AsValidationError extracts the first *ValidationError from err's chain
// (errors.As sugar for hosts that do not want to construct the reflection
// call themselves).
func AsValidationError(err error) (*ValidationError, bool) {
	if err == nil {
		return nil, false
	}
	if e, ok := err.(*ValidationError); ok {
		return e, true
	}
	for {
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return nil, false
		}
		err = u.Unwrap()
		if err == nil {
			return nil, false
		}
		if e, ok := err.(*ValidationError); ok {
			return e, true
		}
	}
}
