// This package is the thin public entry over internal/runtime (plan R.5).
// See internal/runtime/doc.go for the architecture map that
// tests/layering_test.go enforces: this package sits at the top of the
// layer tree and imports nothing but internal/runtime.
package runtime
