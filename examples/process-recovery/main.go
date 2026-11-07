// Example: process-recovery — cross-process restart durability for Hermes.
//
// The shipped acceptance tests restart runtimes inside ONE process on a
// MemoryActionLog. This example proves the stronger, production contract
// (docs/RECOVERY.md): an actionlog.AnansiStore over file-backed sqlite
// survives real process death — including SIGKILL with no Shutdown — and a
// brand-new process recovers the paused run from the log alone.
//
// The parent re-execs its own binary as child runtimes:
//
//	scenario 1 "dead start"  — child serves to paused, exits gracefully;
//	                           fresh child recovers, resumes, reaches x=1 y=2.
//	scenario 2 "SIGKILL"     — child serves to paused, parent SIGKILLs it;
//	                           fresh child recovers the pause, resumes, x=1 y=2.
//	scenario 3 "mid-run kill"— child SIGKILLs itself inside stage:a (before any
//	                           checkpoint); fresh child classifies the run as
//	                           in-flight ("recording"), refuses to invent a
//	                           continuation, and leaves the log intact.
//
// Run with: go run ./examples/process-recovery
package main

import (
	"bufio"
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/asaidimu/go-anansi/v8/core/persistence/base"
	pevents "github.com/asaidimu/go-anansi/v8/core/persistence/events"
	"github.com/asaidimu/go-anansi/v8/core/persistence/persistence"
	"github.com/asaidimu/go-anansi/v8/core/query/native"
	sqliteExecutor "github.com/asaidimu/go-anansi/v8/sqlite/executor"
	sqliteQuery "github.com/asaidimu/go-anansi/v8/sqlite/query"
	rootutils "github.com/asaidimu/go-anansi/v8/utils"
	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/runtime"
	"github.com/asaidimu/hermes/pkg/store"
)

const (
	workflowID  = "wf-cross-process"
	triggerID   = "trigger:manual:Run"
	resumeEvent = "go"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "serve":
			childServe(os.Args[2], os.Args[3])
			return
		case "resume":
			childResume(os.Args[2], os.Args[3])
			return
		case "crash":
			childCrash(os.Args[2])
			return
		case "inspect":
			childInspect(os.Args[2], os.Args[3])
			return
		}
	}
	parent()
}

// ---------------------------------------------------------------------------
// Workflow under test
//
// stage:a is effectful — its logged delta is what the Replayer folds into
// rebuilt state after a cross-process restart. stage:wait pauses for the
// resumeEvent; stage:b proves the resumed segment ran.
// ---------------------------------------------------------------------------

func buildWorkflow(crashInFirstStep bool) *pipeline.Workflow {
	first := func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
		return store.SetValue("x", float64(1)), nil
	}
	if crashInFirstStep {
		first = func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
			fmt.Println("EVENT crash-mid-stage")
			_ = syscall.Kill(syscall.Getpid(), syscall.SIGKILL)
			return nil, nil
		}
	}
	def := pipeline.PipelineDefinition{
		ID:    workflowID + "-pipeline",
		Label: workflowID,
		Stages: []pipeline.Stage{
			{
				ID: "stage:a", Label: "Set X",
				Steps: map[string]pipeline.Step{
					"step:a": {ID: "step:a", Effect: int(pipeline.EffectSideEffecting), Action: first},
				},
			},
			{
				ID: "stage:wait", Label: "Wait",
				Router: func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
					return pipeline.PauseForEvent(resumeEvent, 0), nil
				},
			},
			{
				ID: "stage:b", Label: "Set Y",
				Steps: map[string]pipeline.Step{
					"step:b": {ID: "step:b", Effect: int(pipeline.EffectSideEffecting), Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
						return store.SetValue("y", float64(2)), nil
					}},
				},
			},
		},
	}
	return &pipeline.Workflow{
		ID:    workflowID,
		Label: workflowID,
		Pipelines: map[string]pipeline.PipelineDefinition{
			triggerID: def,
		},
		Triggers: map[string]pipeline.WorkflowTrigger{
			triggerID: {ID: triggerID, Event: runtime.ManualEvent},
		},
	}
}

// ---------------------------------------------------------------------------
// Durable action log over file-backed sqlite
// ---------------------------------------------------------------------------

func openLog(dbPath string) (*actionlog.AnansiStore, error) {
	dsn := "file:" + dbPath + "?_busy_timeout=5000"
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, err
	}
	logger := zap.NewNop()
	exec, err := sqliteExecutor.NewSQLiteExecutor(db, logger)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	interactor, err := native.NewNativeInteractor(exec, sqliteQuery.NewSQLiteFactory(nil), logger)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	bus, err := rootutils.NewInMemoryGoEventsBus("hermes-process-recovery")
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	p, err := persistence.NewPersistence(interactor, pevents.NewGoEventsBusAdapter[base.PersistenceEvent](bus), logger, nil)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return actionlog.NewAnansiStore(context.Background(), p, "actionlog")
}

func newRuntime(log *actionlog.AnansiStore) *runtime.WorkflowRuntime {
	return runtime.NewWorkflowRuntime(runtime.Options{
		ActionLog:   log,
		EventSource: runtime.NewManualEventSource(),
	})
}

// ---------------------------------------------------------------------------
// Child modes
// ---------------------------------------------------------------------------

func childServe(dbPath, blockMode string) {
	ctx := context.Background()
	log, err := openLog(dbPath)
	if err != nil {
		fatal("serve: open log: %v", err)
	}
	rt := newRuntime(log)

	prepared := make(chan string, 1)
	wf := buildWorkflow(false)
	if err := rt.Register(wf, runtime.RegisterOptions{
		Mode:      runtime.Mode{Type: "transient"},
		OnPrepare: func(h *runtime.RunHandle) error { prepared <- h.RunID; return nil },
	}); err != nil {
		fatal("serve: register: %v", err)
	}
	rt.Bus().Emit(ctx, runtime.ManualEvent, events.PipelineEvent{Payload: map[string]any{}})

	var runID string
	select {
	case runID = <-prepared:
	case <-time.After(5 * time.Second):
		fatal("serve: run never started")
	}
	fmt.Println("RUN " + runID)

	if err := waitStatus(ctx, rt, runID, "paused", 5*time.Second); err != nil {
		fatal("serve: %v", err)
	}
	fmt.Println("PAUSED " + runID)

	if blockMode == "block" {
		select {}
	}
	_ = rt.Shutdown(ctx)
}

func childResume(dbPath, runID string) {
	ctx := context.Background()
	log, err := openLog(dbPath)
	if err != nil {
		fatal("resume: open log: %v", err)
	}
	rt := newRuntime(log)

	meta, err := rt.GetRunMeta(ctx, runID)
	if err != nil || meta == nil {
		fatal("resume: recovered meta unavailable: %v", err)
	}
	fmt.Printf("RECOVERED status=%s pipeline=%s\n", meta.Status, meta.PipelineID)
	if string(meta.Status) != "paused" {
		fatal("resume: expected recovered status paused, got %s", meta.Status)
	}

	done := make(chan runtime.RunResult, 1)
	if err := rt.Register(buildWorkflow(false), runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnComplete: func(r runtime.RunResult) { done <- r },
	}); err != nil {
		fatal("resume: register: %v", err)
	}
	rt.Bus().Emit(ctx, resumeEvent, events.PipelineEvent{Payload: map[string]any{"by": "parent"}})

	select {
	case res := <-done:
		fmt.Printf("FINAL status=%s x=%v y=%v\n", res.Status, res.FinalState["x"], res.FinalState["y"])
		if !res.OK {
			os.Exit(3)
		}
	case <-time.After(10 * time.Second):
		fatal("resume: timed out waiting for completion")
	}
	_ = rt.Shutdown(ctx)
}

func childCrash(dbPath string) {
	ctx := context.Background()
	log, err := openLog(dbPath)
	if err != nil {
		fatal("crash: open log: %v", err)
	}
	rt := newRuntime(log)

	prepared := make(chan string, 1)
	if err := rt.Register(buildWorkflow(true), runtime.RegisterOptions{
		Mode:      runtime.Mode{Type: "transient"},
		OnPrepare: func(h *runtime.RunHandle) error { prepared <- h.RunID; return nil },
	}); err != nil {
		fatal("crash: register: %v", err)
	}
	rt.Bus().Emit(ctx, runtime.ManualEvent, events.PipelineEvent{Payload: map[string]any{}})

	select {
	case runID := <-prepared:
		fmt.Println("RUN " + runID)
	case <-time.After(5 * time.Second):
		fatal("crash: run never started")
	}
	select {}
}

func childInspect(dbPath, runID string) {
	ctx := context.Background()
	log, err := openLog(dbPath)
	if err != nil {
		fatal("inspect: open log: %v", err)
	}
	rt := newRuntime(log)

	meta, err := rt.GetRunMeta(ctx, runID)
	if err != nil || meta == nil {
		fatal("inspect: recovered meta unavailable: %v", err)
	}
	entries, err := rt.GetActionLog(ctx, runID)
	if err != nil {
		fatal("inspect: action log unavailable: %v", err)
	}
	fmt.Printf("INFLIGHT status=%s entries=%d\n", meta.Status, len(entries))

	res := rt.Resume(runID, nil)
	fmt.Printf("RESUME ok=%v status=%s err=%v\n", res.OK, res.Status, res.Error)
	if string(meta.Status) != "recording" || res.OK {
		os.Exit(3)
	}
	_ = rt.Shutdown(ctx)
}

func waitStatus(ctx context.Context, rt *runtime.WorkflowRuntime, runID, want string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		meta, err := rt.GetRunMeta(ctx, runID)
		if err == nil && meta != nil && string(meta.Status) == want {
			return nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return fmt.Errorf("run %s never reached status %s", runID, want)
}

func fatal(format string, args ...any) {
	fmt.Printf("ERROR "+format+"\n", args...)
	os.Exit(2)
}

// ---------------------------------------------------------------------------
// Parent orchestrator
// ---------------------------------------------------------------------------

func parent() {
	self, err := os.Executable()
	if err != nil {
		fatal("executable: %v", err)
	}
	tmp, err := os.MkdirTemp("", "hermes-process-recovery-*")
	if err != nil {
		fatal("tempdir: %v", err)
	}
	defer os.RemoveAll(tmp)

	pass := 0
	total := 3

	fmt.Println("=== scenario 1: dead start (graceful exit at pause, fresh process resumes) ===")
	if runScenario(self, filepath.Join(tmp, "s1.db"), false) {
		fmt.Println("RESULT scenario1=PASS")
		pass++
	} else {
		fmt.Println("RESULT scenario1=FAIL")
	}

	fmt.Println("\n=== scenario 2: killed process (SIGKILL at pause, fresh process resumes) ===")
	if runScenario(self, filepath.Join(tmp, "s2.db"), true) {
		fmt.Println("RESULT scenario2=PASS")
		pass++
	} else {
		fmt.Println("RESULT scenario2=FAIL")
	}

	fmt.Println("\n=== scenario 3: killed mid-run (in-flight run is classified, never auto-continued) ===")
	if runScenarioMidKill(self, filepath.Join(tmp, "s3.db")) {
		fmt.Println("RESULT scenario3=PASS")
		pass++
	} else {
		fmt.Println("RESULT scenario3=FAIL")
	}

	fmt.Printf("\n%d/%d scenarios passed\n", pass, total)
	if pass != total {
		os.Exit(1)
	}
}

// runScenario: serve to pause (graceful exit or SIGKILL), then resume in a
// fresh process. Returns true when the resumed run reaches x=1 y=2.
func runScenario(self, dbPath string, kill bool) bool {
	var runID string
	if kill {
		id, err := serveThenKill(self, dbPath)
		if err != nil {
			fmt.Printf("kill: %v\n", err)
			return false
		}
		runID = id
		fmt.Printf("child A SIGKILLed while paused (run %s); checkpoint must already be on disk\n", runID)
	} else {
		outA, errA := runChild(self, "serve", dbPath, "exit")
		fmt.Printf("--- child A (serve exit) ---\n%s", indent(outA))
		if errA != nil {
			fmt.Printf("child A failed: %v\n", errA)
			return false
		}
		runID = parseToken(outA, "RUN ")
		if runID == "" {
			fmt.Println("child A never reported a run id")
			return false
		}
	}

	outB, errB := runChild(self, "resume", dbPath, runID)
	fmt.Printf("--- child B (resume %s) ---\n%s", runID, indent(outB))
	if errB != nil {
		fmt.Printf("child B failed: %v\n", errB)
		return false
	}
	return strings.Contains(outB, "RECOVERED status=paused") &&
		strings.Contains(outB, "FINAL status=succeeded") &&
		strings.Contains(outB, "x=1") &&
		strings.Contains(outB, "y=2")
}

// serveThenKill starts a fresh serve/block child, waits until it reports
// PAUSED on its live stdout, then SIGKILLs it — the kill lands strictly
// after the checkpoint entry is durable. Returns the run id.
func serveThenKill(self, dbPath string) (string, error) {
	cmd := exec.Command(self, "serve", dbPath, "block")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return "", err
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		return "", err
	}
	var runID string
	scanner := bufio.NewScanner(stdout)
	deadline := time.Now().Add(10 * time.Second)
	for scanner.Scan() {
		line := scanner.Text()
		fmt.Printf("  [serve] %s\n", line)
		if id := parseToken(line+"\n", "RUN "); id != "" {
			runID = id
		}
		if strings.HasPrefix(line, "PAUSED ") {
			if err := cmd.Process.Kill(); err != nil {
				return "", err
			}
			_ = cmd.Wait()
			if runID == "" {
				runID = strings.TrimSpace(strings.TrimPrefix(line, "PAUSED "))
			}
			return runID, nil
		}
		if time.Now().After(deadline) {
			break
		}
	}
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	return "", fmt.Errorf("serve child never reported PAUSED")
}

// runScenarioMidKill: child SIGKILLs itself inside stage:a; a fresh process
// must classify the run as in-flight and refuse to invent a continuation.
func runScenarioMidKill(self, dbPath string) bool {
	outA, errA := runChild(self, "crash", dbPath)
	fmt.Printf("--- child A (crash mid-run) ---\n%s", indent(outA))
	if errA == nil {
		fmt.Println("crash child unexpectedly exited cleanly")
		return false
	}
	if !strings.Contains(errA.Error(), "signal: killed") {
		fmt.Printf("crash child died abnormally (expected SIGKILL): %v\n", errA)
	}
	runID := parseToken(outA, "RUN ")
	if runID == "" {
		fmt.Println("crash child never reported a run id")
		return false
	}

	outB, errB := runChild(self, "inspect", dbPath, runID)
	fmt.Printf("--- child B (inspect %s) ---\n%s", runID, indent(outB))
	if errB != nil {
		fmt.Printf("inspect child failed: %v\n", errB)
		return false
	}
	return strings.Contains(outB, "INFLIGHT status=recording") &&
		strings.Contains(outB, "RESUME ok=false")
}

func runChild(self string, args ...string) (string, error) {
	cmd := exec.Command(self, args...)
	out, err := cmd.CombinedOutput()
	return string(out), err
}

func parseToken(out, prefix string) string {
	for _, line := range strings.Split(out, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, prefix) {
			return strings.TrimSpace(strings.TrimPrefix(line, prefix))
		}
	}
	return ""
}

func indent(s string) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	for i, l := range lines {
		lines[i] = "  " + l
	}
	return strings.Join(lines, "\n") + "\n"
}
