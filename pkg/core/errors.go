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
	ErrCodeInternal        = "INTERNAL_ERROR"
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
func NewSystemError(code string, message string) *SystemError {
	// @note #review-20260822-022 issue status=resolved priority=P3 tags=#review,#naming : Variadic string for single message is awkward
	//
	// Resolved: replaced the variadic `message ...string` with a single named
	// `message string` parameter. All call sites in this repo already passed
	// exactly one message, so this is not a breaking change here and now
	// catches accidental multi-arg calls at compile time instead of silently
	// joining or dropping extras.
	return common.NewSystemError(code, message)
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
	// @note #review-20260822-023 issue status=resolved priority=P3 tags=#review,#documentation : Cycle-detection logic undocumented
	//
	// Resolved: documented below why cycle detection is needed here.
	//
	// `seen` guards against infinite loops in the error chain. SystemError.Cause
	// and the generic Unwrap() path are both populated by callers, and nothing
	// prevents an error being wrapped so that unwrapping it eventually returns
	// to an error already seen (e.g. a Cause accidentally set to an ancestor,
	// or a third-party Unwrap implementation with a bug). Without this guard
	// such a cycle would hang the walk forever instead of failing safely.
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
			// @note #review-20260822-007 issue status=resolved priority=P1 tags=#review,#bug : Unreachable branch in CauseMessage
			//
			// Fixed by removing the dead `if se.Cause == nil` branch which was only
			// reachable when se.Message was empty, returning a useless empty string.
			if se.Cause != nil {
				err = se.Cause
				continue
			}
		}
		type unwrapper interface{ Unwrap() error }
		if u, ok := err.(unwrapper); ok {
			err = u.Unwrap()
			continue
		}
		return err.Error()
	}
}
