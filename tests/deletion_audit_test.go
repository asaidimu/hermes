package tests

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// Deletion audit (refactor plan D.5): the delete policy is only credible if
// it is machine-enforced. Two properties are asserted here:
//
//  1. Zero `// Deprecated:` comments exist on the branch — deletion is the
//     policy; stubbing is a regression. Nothing removed may come back as a
//     half-measure shim.
//  2. Every symbol deleted under Phases 1–5 stays deleted: a declaration
//     anywhere in the module fails the test. Mentions in comments, the plan,
//     the audits, and CHANGELOG are fine (they are the record); DECLARATIONS
//     are the regression.

func findModuleRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("go.mod not found above tests package")
		}
		dir = parent
	}
}

func goFiles(root string) ([]string, error) {
	var files []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			// Vendored or generated trees would drown the signal; none
			// exist today, but guard anyway.
			if info.Name() == "vendor" || info.Name() == "testdata" || strings.HasPrefix(info.Name(), ".") {
				return filepath.SkipDir
			}
			return nil
		}
		if strings.HasSuffix(path, ".go") {
			files = append(files, path)
		}
		return nil
	})
	return files, err
}

func TestNoDeprecatedComments(t *testing.T) {
	root := findModuleRoot(t)
	files, err := goFiles(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		// The audit's own documentation strings must not match themselves.
		if strings.HasSuffix(f, "deletion_audit_test.go") {
			continue
		}
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, line := range strings.Split(string(data), "\n") {
			if strings.Contains(line, "Deprecated:") {
				t.Errorf("%s:%d: `// Deprecated:` comment found — the delete policy requires removal, not stubbing", f, i+1)
			}
		}
	}
}

func TestDeletedSymbolsStayDead(t *testing.T) {
	root := findModuleRoot(t)
	files, err := goFiles(root)
	if err != nil {
		t.Fatal(err)
	}

	// Each entry: symbol deleted by the 20261003 refactor → declaration
	// pattern that must match nowhere in the module.
	dead := []struct {
		symbol  string
		pattern string
	}{
		{"runtime.Options.StoreLoader (D.1)", `StoreLoader\s+func`},
		{"WorkflowRuntime.newStoreForID (D.1)", `func \(rt \*WorkflowRuntime\) newStoreForID`},
		{"store.AnansiStoreFactory (D.1/D.3)", `type AnansiStoreFactory struct`},
		{"events durable bus (D.1)", `func NewDurableEventBus|func NewDurableMemoryScopedBus|func WithDurableBackend|type DurableBusConfig struct`},
		{"MemoryScopedBus.Underlying (D.1)", `func \(b \*MemoryScopedBus\) Underlying`},
		{"registry two-tier resume (D.1)", `func \(r \*PipelineRegistry\) (MarkPaused|FastPathResume)|type RegistryOptions struct`},
		{"store.Store.Flush (D.2)", `Flush\(ctx context\.Context\) error`},
		{"MemoryStore/PersistentStore.Flush (D.2)", `func \(s \*(MemoryStore|PersistentStore)\) Flush`},
		{"pipeline.NewFactoryFromModel (D.3)", `func NewFactoryFromModel`},
		{"store.NewFreshStore (D.3)", `func NewFreshStore`},
		{"Hestia adapters (D.3)", `type (HestiaEventSourceFunc|HestiaSchedulerFunc) struct`},
		{"pkg/effect package (D.4)", `^package effect$`},
		{"state-results dead read idiom (D.4)", `if r, ok := state\[\"results\"\]`},
	}

	patterns := make([](*regexp.Regexp), 0, len(dead))
	for _, d := range dead {
		re, err := regexp.Compile(d.pattern)
		if err != nil {
			t.Fatalf("bad pattern for %s: %v", d.symbol, err)
		}
		patterns = append(patterns, re)
	}

	for _, f := range files {
		// The audit test's own pattern literals must not match themselves.
		if strings.HasSuffix(f, "deletion_audit_test.go") {
			continue
		}
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for i, d := range dead {
			if patterns[i].Match(data) {
				t.Errorf("%s: symbol %s is declared again — it was deleted by the 20261003 refactor; resurrections must go through the plan, not silent re-adds", f, d.symbol)
			}
		}
	}
}
