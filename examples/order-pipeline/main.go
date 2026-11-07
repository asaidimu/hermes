// Example: order-pipeline — a tour of the Hermes workflow runtime.
//
// Demo 1 executes a branching order-triage graph authored as canvas wire JSON
// (the same document a visual builder emits) and dumps its event timeline.
// Demo 2 registers an event-driven workflow with a human-approval pause and
// resumes it by emitting a matching event on the runtime bus.
//
// Run with: go run ./examples/order-pipeline
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/asaidimu/hermes/pkg/actionlog"
	"github.com/asaidimu/hermes/pkg/compiler"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/runtime"

	_ "github.com/asaidimu/hermes/pkg/nodes"
)

const orderTriageGraph = `{
  "nodes": [
    { "id": "trigger", "type": "executable", "position": { "x": 0, "y": 0 },
      "data": { "kind": "trigger", "config": { "initialState": {
        "orderId": "ORD-1042",
        "customer": "ada@example.com",
        "items": [
          { "sku": "KB-1", "qty": 1, "price": 129.00 },
          { "sku": "MS-2", "qty": 2, "price": 24.50 }
        ]
      } } } },
    { "id": "compute-total", "type": "executable", "position": { "x": 220, "y": 0 },
      "data": { "kind": "code", "config": { "code":
        "var total = 0; var units = 0;\nfor (var i = 0; i < state.items.length; i++) {\n  total += state.items[i].price * state.items[i].qty;\n  units += state.items[i].qty;\n}\nreturn { total: total, units: units };" } } },
    { "id": "big-order", "type": "executable", "position": { "x": 440, "y": 0 },
      "data": { "kind": "if", "config": { "mode": "simple", "key": "total",
        "predicate": "greater_equals", "value": "100" } } },
    { "id": "express", "type": "executable", "position": { "x": 660, "y": -80 },
      "data": { "kind": "code", "config": { "code":
        "var discount = state.total * 0.10;\nreturn { shipping: \"express\", discount: discount, total: state.total - discount };" } } },
    { "id": "standard", "type": "executable", "position": { "x": 660, "y": 80 },
      "data": { "kind": "code", "config": { "code":
        "return { shipping: \"standard\", discount: 0 };" } } },
    { "id": "express-pause", "type": "executable", "position": { "x": 880, "y": -80 },
      "data": { "kind": "delay", "config": { "ms": 100 } } },
    { "id": "standard-pause", "type": "executable", "position": { "x": 880, "y": 80 },
      "data": { "kind": "delay", "config": { "ms": 100 } } },
    { "id": "receipt-express", "type": "executable", "position": { "x": 1100, "y": -80 },
      "data": { "kind": "code", "config": { "code":
        "return { receipt: \"Order \" + state.orderId + \" [EXPRESS] \" + state.units + \" units, total $\" + state.total.toFixed(2) };" } } },
    { "id": "receipt-standard", "type": "executable", "position": { "x": 1100, "y": 80 },
      "data": { "kind": "code", "config": { "code":
        "return { receipt: \"Order \" + state.orderId + \" [STANDARD] \" + state.units + \" units, total $\" + state.total.toFixed(2) };" } } }
  ],
  "edges": [
    { "id": "e1", "source": "trigger", "target": "compute-total", "data": { "role": "flow" } },
    { "id": "e2", "source": "compute-total", "target": "big-order", "data": { "role": "flow" } },
    { "id": "e3", "source": "big-order", "target": "express", "sourceHandle": "if", "data": { "role": "flow" } },
    { "id": "e4", "source": "big-order", "target": "standard", "sourceHandle": "else", "data": { "role": "flow" } },
    { "id": "e5", "source": "express", "target": "express-pause", "data": { "role": "flow" } },
    { "id": "e6", "source": "standard", "target": "standard-pause", "data": { "role": "flow" } },
    { "id": "e7", "source": "express-pause", "target": "receipt-express", "data": { "role": "flow" } },
    { "id": "e8", "source": "standard-pause", "target": "receipt-standard", "data": { "role": "flow" } }
  ]
}`

const approvalGraph = `{
  "nodes": [
    { "id": "trigger", "type": "executable", "position": { "x": 0, "y": 0 },
      "data": { "kind": "trigger", "config": { "initialState": {
        "total": 240, "requester": "ada@example.com" } } } },
    { "id": "await-approval", "type": "executable", "position": { "x": 220, "y": 0 },
      "data": { "kind": "pause", "config": { "waitForEvent": "approval" } } },
    { "id": "flag-for-review", "type": "executable", "position": { "x": 440, "y": -60 },
      "data": { "kind": "code", "config": { "code":
        "return { reviewFlagged: true };" } } },
    { "id": "mark-approved", "type": "executable", "position": { "x": 660, "y": 0 },
      "data": { "kind": "code", "config": { "code":
        "return { approved: true, approvedBy: state.approver || \"unknown\" };" } } }
  ],
  "edges": [
    { "id": "e1", "source": "trigger", "target": "await-approval", "data": { "role": "flow" } },
    { "id": "e2", "source": "await-approval", "target": "flag-for-review",
      "sourceHandle": "do", "data": { "role": "flow" } },
    { "id": "e3", "source": "await-approval", "target": "mark-approved",
      "sourceHandle": "onResume", "data": { "role": "flow" } }
  ]
}`

func main() {
	fmt.Println("=== Demo 1: branching order triage from canvas JSON ===")
	demoBranching()

	fmt.Println("\n=== Demo 2: human-in-the-loop approval (pause / resume) ===")
	demoApproval()
}

func demoBranching() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rt := runtime.NewWorkflowRuntime(runtime.Options{
		ActionLog: actionlog.NewMemoryActionLog(),
	})
	defer rt.Shutdown(ctx)

	unsub := rt.Bus().Subscribe("*", func(ctx context.Context, ev events.PipelineEvent) error {
		switch ev.Type {
		case "run:start", "stage:start", "stage:success", "run:success", "run:paused", "run:failure":
			fmt.Printf("  [bus] %-14s payload=%v\n", ev.Type, ev.Payload)
		}
		return nil
	})
	defer unsub()

	nodes, edges, err := compiler.DecodeWireGraph([]byte(orderTriageGraph))
	if err != nil {
		panic(err)
	}

	result, err := rt.Run(ctx, nodes, edges, runtime.RunOptions{Timeout: 10 * time.Second})
	if err != nil {
		panic(err)
	}

	fmt.Printf("\nrun %s -> status=%s\n", result.RunID, result.Status)
	if result.Error != nil {
		fmt.Printf("error: %v\n", result.Error)
		return
	}
	fmt.Printf("shipping lane: %v\n", result.FinalState["shipping"])
	fmt.Printf("receipt:       %v\n", result.FinalState["receipt"])
	fmt.Printf("final state:   %v\n", result.FinalState)

	timeline, err := rt.GetEvents(ctx, result.RunID, 0, 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("\ntimeline (%d events, newest first 8):\n", len(timeline))
	start := 0
	if len(timeline) > 8 {
		start = len(timeline) - 8
	}
	for _, ev := range timeline[start:] {
		fmt.Printf("  seq=%-3d type=%-14s payload=%v\n", ev.Seq, ev.Type, ev.Payload)
	}
}

func demoApproval() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rt := runtime.NewWorkflowRuntime(runtime.Options{
		ActionLog: actionlog.NewMemoryActionLog(),
	})
	defer rt.Shutdown(ctx)

	nodes, edges, err := compiler.DecodeWireGraph([]byte(approvalGraph))
	if err != nil {
		panic(err)
	}
	wf, err := compiler.Compile(nodes, edges, nil)
	if err != nil {
		panic(err)
	}

	prepared := make(chan string, 1)
	done := make(chan runtime.RunResult, 1)
	if err := rt.Register(wf, runtime.RegisterOptions{
		Mode:       runtime.Mode{Type: "transient"},
		OnPrepare:  func(h *runtime.RunHandle) error { prepared <- h.RunID; return nil },
		OnComplete: func(r runtime.RunResult) { done <- r },
	}); err != nil {
		panic(err)
	}

	rt.Bus().Emit(ctx, runtime.ManualEvent, events.PipelineEvent{Payload: map[string]any{}})

	var runID string
	select {
	case runID = <-prepared:
	case <-time.After(5 * time.Second):
		panic("run never started")
	}
	fmt.Printf("run %s started; waiting for approval...\n", runID)

	waitPaused(ctx, rt, runID)
	fmt.Printf("run %s is paused (waiting for event %q)\n", runID, "approval")

	fmt.Println("emitting approval event on the bus...")
	rt.Bus().Emit(ctx, "approval", events.PipelineEvent{
		Payload: map[string]any{"approver": "grace@example.com"},
	})

	select {
	case res := <-done:
		fmt.Printf("run %s -> status=%s\n", res.RunID, res.Status)
		fmt.Printf("reviewFlagged: %v (set by the pause body before waiting)\n", res.FinalState["reviewFlagged"])
		fmt.Printf("approved:      %v\n", res.FinalState["approved"])
		fmt.Printf("approvedBy:    %v\n", res.FinalState["approvedBy"])
		fmt.Printf("final state: %v\n", res.FinalState)
	case <-time.After(10 * time.Second):
		panic("timed out waiting for resumed run to finish")
	}
}

func waitPaused(ctx context.Context, rt *runtime.WorkflowRuntime, runID string) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		meta, err := rt.GetRunMeta(ctx, runID)
		if err == nil && string(meta.Status) == "paused" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	panic("run never reached paused status")
}
