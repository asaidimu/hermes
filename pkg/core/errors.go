package core

import (
	"github.com/asaidimu/go-anansi/v8/core/common"
)

// SystemError aliases go-anansi's robust SystemError type.
type SystemError = common.SystemError
type Issue = common.Issue
type Issues = common.Issues

// Standard Error Codes for Pipelines
const (
	ErrCodeNotFound        = "NOT_FOUND"
	ErrCodeInvalidCommand  = "INVALID_COMMAND"
	ErrCodeExecutionFailed = "EXECUTION_FAILED"
	ErrCodeTimeout         = "TIMEOUT"
	ErrCodeCancelled       = "CANCELLED"
	ErrCodeValidation      = "VALIDATION_ERROR"
	ErrCodeConflict        = "CONFLICT"
	ErrCodeAbort           = "ABORTED"
)

// Standard Predefined Errors
var (
	ErrNotFound        = common.NewSystemError(ErrCodeNotFound, "resource not found")
	ErrInvalidCommand  = common.NewSystemError(ErrCodeInvalidCommand, "invalid command")
	ErrExecutionFailed = common.NewSystemError(ErrCodeExecutionFailed, "execution failed")
	ErrTimeout         = common.NewSystemError(ErrCodeTimeout, "operation timed out")
	ErrCancelled       = common.NewSystemError(ErrCodeCancelled, "operation cancelled")
	ErrValidation      = common.NewSystemError(ErrCodeValidation, "validation failed")
	ErrConflict        = common.NewSystemError(ErrCodeConflict, "conflict")
)

// NewSystemError creates a new SystemError using go-anansi's constructor.
func NewSystemError(code string, message ...string) *SystemError {
	// @note #review-20260822-022 issue status=open priority=P3 tags=#review,#naming : Variadic string for single message is awkward
	//
	// NewSystemError accepts variadic `message ...string` but callers must remember to
	// pass at most one message. Passing multiple strings produces an unpredictable joined
	// message or ignores extras. A single named `message string` parameter would be clearer
	// and catch misuse at compile time.
	return common.NewSystemError(code, message...)
}

// SystemErrorFrom converts an arbitrary error to a SystemError.
func SystemErrorFrom(err error, code ...string) *SystemError {
	return common.SystemErrorFrom(err, code...)
}

// CauseMessage walks an error chain to the root cause and returns its message,
// mirroring the TS `causeMessage` helper used in step/stage failure aggregation.
func CauseMessage(err error) string {
	if err == nil {
		return ""
	}
	// @note #review-20260822-023 issue status=open priority=P3 tags=#review,#documentation : Cycle-detection logic undocumented
	//
	// CauseMessage includes a `seen` map for cycle detection but has no comment explaining
	// why cycles are possible in the error chain. Future maintainers may wonder why a
	// simple loop is insufficient.
	seen := make(map[error]bool)
	for {
		if seen[err] {
			return err.Error()
		}
		seen[err] = true

		if se, ok := err.(*SystemError); ok {
			if se.Message != "" && se.Cause == nil {
				return se.Message
			}
			// @note #review-20260822-007 issue status=open priority=P1 tags=#review,#bug : Unreachable branch in CauseMessage
			//
			// The condition `se.Cause == nil` at line 62 is unreachable because any error
			// that reaches it must have `se.Cause != nil` (otherwise the first condition
			// already returned). This is dead code that suggests the author intended
			// different logic — possibly checking for `se.Message == ""` with `Cause == nil`
			// to handle empty messages.
			if se.Cause == nil {
				return se.Message
			}
			err = se.Cause
			continue
		}
		type unwrapper interface{ Unwrap() error }
		if u, ok := err.(unwrapper); ok {
			err = u.Unwrap()
			continue
		}
		return err.Error()
	}
}
