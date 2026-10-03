package tests

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// Layer map enforcement (refactor plan R.6): the Target architecture's
// import rules are only real if a machine checks them. Go has no
// compiler-enforced layering, so this test walks every in-tree package's
// non-test imports and asserts direction. Import direction flows DOWN the
// table (higher layers may import lower ones, never the reverse):
//
//      6  orchestration   internal/runtime (implementation), pkg/runtime (thin)
//      5  nodes           pkg/nodes/*
//      4  compile         pkg/compiler, pkg/nodekit
//      3  fold (pure)     pkg/replay
//      2  domain          pkg/pipeline
//      1  persistence     pkg/store, pkg/actionlog
//      0  shared          pkg/core
//      projections        pkg/events, pkg/scheduler, pkg/watch, pkg/timeline,
//                         pkg/registry
//
// Every exception must carry a whitelist entry WITH a reason; a whitelist
// entry without a reason, or an unused one, fails the test too.

const mod = "github.com/asaidimu/hermes"

type layerRule struct {
	pkg       string   // package import path (prefix match with "/" boundary)
	forbidden []string // import path prefixes this package must not import
}

var layerRules = []layerRule{
	{
		// Leaf node implementations: pure consumers of the nodekit SDK.
		pkg: mod + "/pkg/nodes",
		forbidden: []string{
			mod + "/pkg/runtime", mod + "/internal/runtime",
			mod + "/pkg/replay", mod + "/pkg/compiler",
		},
	},
	{
		// The fold is PURE: no locks, no I/O, no scheduling, no bus.
		pkg: mod + "/pkg/replay",
		forbidden: []string{
			mod + "/pkg/events", mod + "/pkg/scheduler", mod + "/pkg/watch",
			mod + "/pkg/runtime", mod + "/internal/runtime",
			mod + "/pkg/compiler", mod + "/pkg/nodekit", mod + "/pkg/nodes",
			mod + "/pkg/registry", mod + "/pkg/timeline",
		},
	},
	{
		// Domain types: nothing above compile may be imported.
		pkg: mod + "/pkg/pipeline",
		forbidden: []string{
			mod + "/pkg/compiler", mod + "/pkg/nodekit", mod + "/pkg/nodes",
			mod + "/pkg/runtime", mod + "/internal/runtime",
			mod + "/pkg/replay", mod + "/pkg/scheduler", mod + "/pkg/watch",
			mod + "/pkg/timeline", mod + "/pkg/registry",
			mod + "/pkg/events",
		},
	},
	{
		// Persistence layers stay below the domain.
		pkg: mod + "/pkg/store",
		forbidden: []string{
			mod + "/pkg/pipeline", mod + "/pkg/replay", mod + "/pkg/compiler",
			mod + "/pkg/nodekit", mod + "/pkg/nodes", mod + "/pkg/runtime",
			mod + "/internal/runtime", mod + "/pkg/events",
			mod + "/pkg/scheduler", mod + "/pkg/watch", mod + "/pkg/timeline",
			mod + "/pkg/actionlog",
		},
	},
	{
		pkg: mod + "/pkg/actionlog",
		forbidden: []string{
			mod + "/pkg/pipeline", mod + "/pkg/replay", mod + "/pkg/compiler",
			mod + "/pkg/nodekit", mod + "/pkg/nodes", mod + "/pkg/runtime",
			mod + "/internal/runtime", mod + "/pkg/events",
			mod + "/pkg/scheduler", mod + "/pkg/watch", mod + "/pkg/timeline",
			mod + "/pkg/store",
		},
	},
	{
		pkg: mod + "/pkg/timeline",
		forbidden: []string{
			mod + "/pkg/pipeline", mod + "/pkg/replay", mod + "/pkg/compiler",
			mod + "/pkg/nodekit", mod + "/pkg/nodes", mod + "/pkg/runtime",
			mod + "/internal/runtime", mod + "/pkg/scheduler",
			mod + "/pkg/watch", mod + "/pkg/store",
		},
	},
	{
		// The thin public entry re-exports internal/runtime and nothing else.
		pkg:       mod + "/pkg/runtime",
		forbidden: []string{mod + "/pkg/", mod + "/internal/"},
	},
	{
		// The one orchestrator: the only package that may pull the replay
		// fold and the scheduler/watch projections together. Only a cycle
		// back into the thin entry is forbidden.
		pkg:       mod + "/internal/runtime",
		forbidden: []string{mod + "/pkg/runtime"},
	},
	{
		pkg: mod + "/pkg/events",
		forbidden: []string{
			mod + "/pkg/pipeline", mod + "/pkg/replay", mod + "/pkg/compiler",
			mod + "/pkg/nodekit", mod + "/pkg/nodes", mod + "/pkg/runtime",
			mod + "/internal/runtime", mod + "/pkg/scheduler",
			mod + "/pkg/watch", mod + "/pkg/timeline", mod + "/pkg/store",
		},
	},
	{
		pkg: mod + "/pkg/core",
		forbidden: []string{
			mod + "/pkg/", mod + "/internal/",
		},
	},
}

// layerWhitelist permits a specific (importing pkg) → (imported pkg) edge.
// Every entry MUST carry a reason tied to an audit finding or wire compat;
// entries without a reason fail the test, and unused entries fail it too.
var layerWhitelist = map[string]map[string]string{
	mod + "/pkg/pipeline": {
		// R.2/R.6: pipeline imports the event bus for PipelineEvent/EventPath
		// types and MemoryScopedBus emission — the domain emits events it does
		// not route. Architecture audit: dual observability channels collapse
		// onto the log; the bus remains the subscriber surface. This is the
		// plan's single explicit pipeline exception (bus above domain).
		mod + "/pkg/events": "bus is the subscriber surface pipeline emits into; PipelineEvent/EventPath wire types live there",
	},
}

func moduleRootDir(t *testing.T) string {
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
			t.Fatal("go.mod not found")
		}
		dir = parent
	}
}

func packageImports(t *testing.T, root string) map[string][]string {
	t.Helper()
	imports := map[string][]string{}
	fset := token.NewFileSet()
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			switch info.Name() {
			case "testdata", "vendor", ".git", "node_modules", "tools", "download", "repos":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(root, filepath.Dir(path))
		importPath := mod
		if rel != "." {
			importPath = mod + "/" + filepath.ToSlash(rel)
		}
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			imports[importPath] = append(imports[importPath], p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return imports
}

func TestPackageLayering(t *testing.T) {
	root := moduleRootDir(t)
	imports := packageImports(t, root)

	usedWhitelist := map[string]map[string]bool{}
	for pkg, reasons := range layerWhitelist {
		usedWhitelist[pkg] = map[string]bool{}
		for imp := range reasons {
			usedWhitelist[pkg][imp] = false
		}
	}

	pkgs := make([]string, 0, len(imports))
	for p := range imports {
		pkgs = append(pkgs, p)
	}
	sort.Strings(pkgs)

	for _, importing := range pkgs {
		for _, imp := range imports[importing] {
			if !strings.HasPrefix(imp, mod+"/") && !strings.HasPrefix(imp, mod) {
				continue // external module imports are unrestricted here
			}
			for _, rule := range layerRules {
				if importing != rule.pkg && !strings.HasPrefix(importing, rule.pkg+"/") {
					continue
				}
				for _, forbidden := range rule.forbidden {
					if forbidden == mod+"/pkg/../" {
						continue // explicit no-op marker
					}
					if imp == forbidden || strings.HasPrefix(imp, forbidden+"/") {
						if reasons, ok := layerWhitelist[importing]; ok {
							_ = reasons
							if reason, ok := layerWhitelist[importing][imp]; ok {
								if reason == "" {
									t.Errorf("%s -> %s: whitelist entry has no reason", importing, imp)
								}
								usedWhitelist[importing][imp] = true
								continue
							}
						}
						t.Errorf("layering violation: %s imports %s (forbidden by rule for %s)",
							importing, imp, rule.pkg)
					}
				}
			}
		}
	}

	// Unused whitelist entries are dead weight — fail on them.
	for importing, entry := range layerWhitelist {
		for imp := range entry {
			if !usedWhitelist[importing][imp] {
				t.Errorf("whitelist entry %s -> %s is unused; delete it (reason: %q)",
					importing, imp, layerWhitelist[importing][imp])
			}
		}
	}
}
