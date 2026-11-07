// Example: external-events — pause/resume and recovery with the runtime in a
// CHILD process, driven by TRULY EXTERNAL events over the anansi codec.
//
// Parent and child exchange framed messages on stdin/stdout. The IPC envelope
// is pure anansi end to end — no JSON codec anywhere on the wire path:
//
//	struct → container   document.DocumentPool.FromStruct (anansi tags)
//	container → wire     Document.ToAnansi (core/encoding/anansi binary packet)
//	wire → container     DocumentPool.FromAnansi
//	container → struct   Document.BindTo (anansi tags, no map materialization)
//
// under a uvarint length prefix. The child plugs a custom runtime.EventSource
// whose params.Emit is fed by decoded IPC "event" messages — so the workflow
// trigger AND the resume event cross the OS process boundary through the
// codec, exactly like a webhook or message queue would in production.
//
//	scenario 1 "pause/resume" — child runs to a PauseForEvent("approval");
//	                             parent sends the approval event (payload
//	                             {approver:grace}) over IPC; the payload fold
//	                             lands approver in run state.
//	scenario 2 "recovery"      — parent SIGKILLs the paused child; a fresh
//	                             child on the same sqlite AnansiStore log
//	                             recovers the run (meta queryable, pause
//	                             re-armed at Register); parent sends the SAME
//	                             external approval event to the new child.
//
// Run with: go run ./examples/external-events
package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"github.com/asaidimu/go-anansi/v8/core/data"
	"github.com/asaidimu/go-anansi/v8/core/document"
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
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/runtime"
	"github.com/asaidimu/hermes/pkg/store"
)

const (
	workflowID  = "wf-external-events"
	triggerID   = "trigger:manual:Run"
	approveType = "approval"
)

// IPCMessage is the single wire type, bound end to end by anansi tags.
// Payload and state are OPAQUE application envelopes (JSON strings): the
// runtime's event API takes map[string]any, whose heterogeneous shape has no
// static schema — decoding that one string into a map at the emit seam is an
// application concern, not part of the wire protocol.
type IPCMessage struct {
	Kind    string `anansi:"kind"`
	RunID   string `anansi:"runId"`
	Type    string `anansi:"type"`    // event type (kind=event)
	Payload string `anansi:"payload"` // opaque app envelope
	Status  string `anansi:"status"`  // paused|succeeded|...
	State   string `anansi:"state"`   // opaque app envelope (kind=done)
	Error   string `anansi:"error"`
}

// ---------------------------------------------------------------------------
// Anansi-only framing over a stream: struct ⇄ container ⇄ binary packet
// ---------------------------------------------------------------------------

func mustDocPool() *document.DocumentPool {
	raw, err := data.ExtractDTOSchemaDirect(&IPCMessage{})
	if err != nil {
		fatal("schema extract: %v", err)
	}
	pool, err := document.NewDocumentPoolFromJSON(raw)
	if err != nil {
		fatal("document pool: %v", err)
	}
	return pool
}

func writeMsg(w io.Writer, pool *document.DocumentPool, m IPCMessage) error {
	doc, err := pool.FromStruct(m)
	if err != nil {
		return err
	}
	wire, err := doc.ToAnansi(0)
	if err != nil {
		return err
	}
	var hdr [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(hdr[:], uint64(len(wire)))
	if _, err := w.Write(hdr[:n]); err != nil {
		return err
	}
	_, err = w.Write(wire)
	return err
}

func readMsg(r *bufio.Reader, pool *document.DocumentPool) (IPCMessage, error) {
	var m IPCMessage
	length, err := binary.ReadUvarint(r)
	if err != nil {
		return m, err
	}
	wire := make([]byte, length)
	if _, err := io.ReadFull(r, wire); err != nil {
		return m, err
	}
	doc, err := pool.FromAnansi(wire)
	if err != nil {
		return m, err
	}
	err = doc.BindTo(&m)
	return m, err
}

// ---------------------------------------------------------------------------
// Workflow under test: set x=1 → wait for external "approval" → set y=2
// ---------------------------------------------------------------------------

func buildWorkflow() *pipeline.Workflow {
	def := pipeline.PipelineDefinition{
		ID:    workflowID + "-pipeline",
		Label: workflowID,
		Stages: []pipeline.Stage{
			{
				ID: "stage:a", Label: "Set X",
				Steps: map[string]pipeline.Step{
					"step:a": {ID: "step:a", Effect: int(pipeline.EffectSideEffecting), Action: func(ctx context.Context, pcxt *pipeline.PipelineContext, state map[string]any) (store.Mutator, error) {
						return store.SetValue("x", float64(1)), nil
					}},
				},
			},
			{
				ID: "stage:wait", Label: "Wait for approval",
				Router: func(ctx context.Context, state map[string]any, st store.Store) (pipeline.RoutingInstruction, error) {
					return pipeline.PauseForEvent(approveType, 0), nil
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
// Durable action log over file-backed sqlite (cross-process recovery)
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
	bus, err := rootutils.NewInMemoryGoEventsBus("hermes-external-events")
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

// ---------------------------------------------------------------------------
// Child: EventSource fed by decoded IPC events (the external seam)
// ---------------------------------------------------------------------------

type ipcEventSource struct {
	in    *bufio.Reader
	out   io.Writer
	pool  *document.DocumentPool
	mu    sync.Mutex
	emit  func(eventType string, payload map[string]any)
	doneC chan struct{}
}

func (s *ipcEventSource) OnRegister(_ context.Context, params runtime.RegisterParams) (func(), error) {
	s.mu.Lock()
	s.emit = params.Emit
	s.mu.Unlock()
	go s.readLoop()
	return func() {}, nil
}

func (s *ipcEventSource) OnShutdown(context.Context) error { return nil }

func (s *ipcEventSource) send(m IPCMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return writeMsg(s.out, s.pool, m)
}

func (s *ipcEventSource) readLoop() {
	for {
		msg, err := readMsg(s.in, s.pool)
		if err != nil {
			closeOnce(s.doneC)
			return
		}
		switch msg.Kind {
		case "event":
			payload := map[string]any{}
			if msg.Payload != "" {
				_ = json.Unmarshal([]byte(msg.Payload), &payload)
			}
			s.mu.Lock()
			emit := s.emit
			s.mu.Unlock()
			if emit != nil {
				emit(msg.Type, payload)
			}
		case "shutdown":
			closeOnce(s.doneC)
			return
		}
	}
}

func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func childRun(dbPath, expectRunID string) {
	ctx := context.Background()
	src := &ipcEventSource{
		in:    bufio.NewReader(os.Stdin),
		out:   os.Stdout,
		pool:  mustDocPool(),
		doneC: make(chan struct{}),
	}
	logStore, err := openLog(dbPath)
	if err != nil {
		fatal("open log: %v", err)
	}
	rt := runtime.NewWorkflowRuntime(runtime.Options{
		ActionLog:   logStore,
		EventSource: src,
	})

	prepared := make(chan string, 1)
	done := make(chan runtime.RunResult, 1)
	paused := make(chan runtime.RunResult, 1)
	if err := rt.Register(buildWorkflow(), runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnPrepare:  func(h *runtime.RunHandle) error { prepared <- h.RunID; return nil },
		OnComplete: func(r runtime.RunResult) { done <- r },
		OnPause:    func(r runtime.RunResult) { paused <- r },
	}); err != nil {
		fatal("register: %v", err)
	}

	if expectRunID != "" {
		meta, err := rt.GetRunMeta(ctx, expectRunID)
		if err != nil || meta == nil || string(meta.Status) != "paused" {
			fatal("recovered meta not paused: %v %+v", err, meta)
		}
		if err := src.send(IPCMessage{Kind: "recovered", RunID: expectRunID, Status: string(meta.Status)}); err != nil {
			fatal("send recovered: %v", err)
		}
	}

	if err := src.send(IPCMessage{Kind: "ready"}); err != nil {
		fatal("send ready: %v", err)
	}

	if expectRunID == "" {
		var runID string
		select {
		case runID = <-prepared:
			if err := src.send(IPCMessage{Kind: "status", RunID: runID, Status: "starting"}); err != nil {
				fatal("send starting: %v", err)
			}
		case <-src.doneC:
			return
		case <-time.After(10 * time.Second):
			fatal("run never started")
		}
		select {
		case r := <-paused:
			if err := src.send(IPCMessage{Kind: "status", RunID: r.RunID, Status: "paused"}); err != nil {
				fatal("send paused: %v", err)
			}
		case <-src.doneC:
			return
		case <-time.After(10 * time.Second):
			_ = src.send(IPCMessage{Kind: "error", RunID: runID, Error: "never reached paused"})
		}
	}

	select {
	case res := <-done:
		state, _ := json.Marshal(res.FinalState)
		status := res.Status
		errText := ""
		if res.Error != nil {
			errText = res.Error.Error()
		}
		_ = src.send(IPCMessage{Kind: "done", RunID: res.RunID, Status: status, State: string(state), Error: errText})
	case <-src.doneC:
		return
	case <-time.After(10 * time.Second):
		_ = src.send(IPCMessage{Kind: "error", Error: "timed out waiting for completion"})
	}
	<-src.doneC
}

// ---------------------------------------------------------------------------
// Parent side of the IPC channel
// ---------------------------------------------------------------------------

type childProc struct {
	cmd    *exec.Cmd
	in     io.WriteCloser
	reader *bufio.Reader
	pool   *document.DocumentPool
}

func spawnChild(self, dbPath, expectRunID string) (*childProc, error) {
	args := []string{"run", dbPath}
	if expectRunID != "" {
		args = append(args, expectRunID)
	}
	cmd := exec.Command(self, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	cmd.Stderr = &lockedWriter{w: os.Stderr}
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	return &childProc{
		cmd:    cmd,
		in:     stdin,
		reader: bufio.NewReader(stdout),
		pool:   mustDocPool(),
	}, nil
}

func (c *childProc) send(m IPCMessage) error {
	return writeMsg(c.in, c.pool, m)
}

func (c *childProc) recv(timeout time.Duration) (IPCMessage, error) {
	type result struct {
		msg IPCMessage
		err error
	}
	ch := make(chan result, 1)
	go func() {
		m, err := readMsg(c.reader, c.pool)
		ch <- result{m, err}
	}()
	select {
	case r := <-ch:
		return r.msg, r.err
	case <-time.After(timeout):
		return IPCMessage{}, fmt.Errorf("timed out waiting for child message")
	}
}

func (c *childProc) waitRecv(kind string, timeout time.Duration) (IPCMessage, error) {
	return c.waitMsg(func(m IPCMessage) bool { return m.Kind == kind }, kind, timeout)
}

func (c *childProc) waitStatus(want string, timeout time.Duration) (IPCMessage, error) {
	return c.waitMsg(func(m IPCMessage) bool { return m.Kind == "status" && m.Status == want }, "status="+want, timeout)
}

func (c *childProc) waitMsg(match func(IPCMessage) bool, describe string, timeout time.Duration) (IPCMessage, error) {
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return IPCMessage{}, fmt.Errorf("never received message matching %s", describe)
		}
		m, err := c.recv(remaining)
		if err != nil {
			return IPCMessage{}, err
		}
		fmt.Printf("  [ipc<-child] kind=%s runId=%s status=%s\n", m.Kind, m.RunID, m.Status)
		if match(m) {
			return m, nil
		}
		if m.Kind == "error" {
			return IPCMessage{}, fmt.Errorf("child error: %s", m.Error)
		}
	}
}

func (c *childProc) kill() {
	_ = c.cmd.Process.Kill()
	_ = c.cmd.Wait()
}

func (c *childProc) shutdown() {
	_ = c.send(IPCMessage{Kind: "shutdown"})
	done := make(chan struct{})
	go func() { _ = c.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		_ = c.cmd.Process.Kill()
	}
}

// ---------------------------------------------------------------------------
// Parent orchestrator
// ---------------------------------------------------------------------------

func main() {
	if len(os.Args) > 1 && os.Args[1] == "run" {
		expect := ""
		if len(os.Args) > 3 {
			expect = os.Args[3]
		}
		childRun(os.Args[2], expect)
		return
	}
	parent()
}

func parent() {
	self, err := os.Executable()
	if err != nil {
		fatal("executable: %v", err)
	}
	tmp, err := os.MkdirTemp("", "hermes-external-events-*")
	if err != nil {
		fatal("tempdir: %v", err)
	}
	defer os.RemoveAll(tmp)

	pass, total := 0, 2

	fmt.Println("=== scenario 1: pause & resume (approval event crosses the codec IPC) ===")
	if scenarioPauseResume(self, filepath.Join(tmp, "s1.db")) {
		fmt.Println("RESULT scenario1=PASS")
		pass++
	} else {
		fmt.Println("RESULT scenario1=FAIL")
	}

	fmt.Println("\n=== scenario 2: recovery (SIGKILL paused child, fresh child resumes via same external event) ===")
	if scenarioRecovery(self, filepath.Join(tmp, "s2.db")) {
		fmt.Println("RESULT scenario2=PASS")
		pass++
	} else {
		fmt.Println("RESULT scenario2=FAIL")
	}

	fmt.Printf("\n%d/%d scenarios passed\n", pass, total)
	if pass != total {
		os.Exit(1)
	}
}

func scenarioPauseResume(self, dbPath string) bool {
	child, err := spawnChild(self, dbPath, "")
	if err != nil {
		fmt.Printf("spawn: %v\n", err)
		return false
	}
	defer child.shutdown()

	if _, err := child.waitRecv("ready", 10*time.Second); err != nil {
		fmt.Printf("handshake: %v\n", err)
		return false
	}
	fmt.Println("  [parent] sending trigger event (ManualEvent) over codec IPC")
	if err := child.send(IPCMessage{Kind: "event", Type: runtime.ManualEvent, Payload: `{}`}); err != nil {
		fmt.Printf("send trigger: %v\n", err)
		return false
	}

	status, err := child.waitStatus("paused", 10*time.Second)
	if err != nil {
		fmt.Printf("expected paused status: %v\n", err)
		return false
	}
	runID := status.RunID
	fmt.Printf("  [parent] run %s paused; sending external %q event with payload\n", runID, approveType)
	if err := child.send(IPCMessage{Kind: "event", Type: approveType, Payload: `{"approver":"grace"}`}); err != nil {
		fmt.Printf("send approval: %v\n", err)
		return false
	}

	done, err := child.waitRecv("done", 10*time.Second)
	if err != nil {
		fmt.Printf("done: %v\n", err)
		return false
	}
	return checkDone(done, runID)
}

func scenarioRecovery(self, dbPath string) bool {
	child1, err := spawnChild(self, dbPath, "")
	if err != nil {
		fmt.Printf("spawn child1: %v\n", err)
		return false
	}
	if _, err := child1.waitRecv("ready", 10*time.Second); err != nil {
		fmt.Printf("child1 handshake: %v\n", err)
		return false
	}
	if err := child1.send(IPCMessage{Kind: "event", Type: runtime.ManualEvent, Payload: `{}`}); err != nil {
		fmt.Printf("send trigger: %v\n", err)
		return false
	}
	status, err := child1.waitStatus("paused", 10*time.Second)
	if err != nil {
		fmt.Printf("child1 expected paused: %v\n", err)
		return false
	}
	runID := status.RunID
	fmt.Printf("  [parent] run %s paused in child1; SIGKILLing child1 (no Shutdown, no flush)\n", runID)
	child1.kill()

	child2, err := spawnChild(self, dbPath, runID)
	if err != nil {
		fmt.Printf("spawn child2: %v\n", err)
		return false
	}
	defer child2.shutdown()

	recovered, err := child2.waitRecv("recovered", 10*time.Second)
	if err != nil || recovered.Status != "paused" || recovered.RunID != runID {
		fmt.Printf("child2 recovery failed: %+v err=%v\n", recovered, err)
		return false
	}
	fmt.Printf("  [parent] child2 recovered run %s from the durable log (status=%s)\n", recovered.RunID, recovered.Status)
	if _, err := child2.waitRecv("ready", 5*time.Second); err != nil {
		fmt.Printf("child2 handshake: %v\n", err)
		return false
	}

	fmt.Printf("  [parent] sending the same external %q event to child2 over codec IPC\n", approveType)
	if err := child2.send(IPCMessage{Kind: "event", Type: approveType, Payload: `{"approver":"grace"}`}); err != nil {
		fmt.Printf("send approval: %v\n", err)
		return false
	}
	done, err := child2.waitRecv("done", 10*time.Second)
	if err != nil {
		fmt.Printf("done: %v\n", err)
		return false
	}
	return checkDone(done, runID)
}

func checkDone(done IPCMessage, runID string) bool {
	if done.RunID != runID || done.Status != "succeeded" || done.Error != "" {
		fmt.Printf("unexpected done message: %+v\n", done)
		return false
	}
	state := map[string]any{}
	if err := json.Unmarshal([]byte(done.State), &state); err != nil {
		fmt.Printf("bad state JSON: %v\n", err)
		return false
	}
	fmt.Printf("  [parent] final state: x=%v y=%v approver=%v\n", state["x"], state["y"], state["approver"])
	return state["x"] == float64(1) &&
		state["y"] == float64(2) &&
		state["approver"] == "grace"
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

type lockedWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func (l *lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "ERROR "+format+"\n", args...)
	os.Exit(2)
}
