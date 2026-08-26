package delay

import (
	"context"
	"testing"
	"time"

	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/pipeline"
)

func TestDelayZeroReturnsNil(t *testing.T) {
	m, err := Node.Run(context.Background(), nodekit.NodeRunContext{
		Config: map[string]any{"ms": float64(0)},
	})
	if m != nil {
		t.Error("expected nil mutator for zero delay")
	}
	if err != nil {
		t.Errorf("expected no error: %v", err)
	}
}

func TestDelayNegativeReturnsNil(t *testing.T) {
	m, err := Node.Run(context.Background(), nodekit.NodeRunContext{
		Config: map[string]any{"ms": float64(-100)},
	})
	if m != nil {
		t.Error("expected nil mutator for negative delay")
	}
	if err != nil {
		t.Errorf("expected no error: %v", err)
	}
}

func TestDelayMissingMsReturnsNil(t *testing.T) {
	m, err := Node.Run(context.Background(), nodekit.NodeRunContext{
		Config: map[string]any{},
	})
	if m != nil {
		t.Error("expected nil mutator for missing ms")
	}
	if err != nil {
		t.Errorf("expected no error: %v", err)
	}
}

func TestDelayWaitsSpecifiedDuration(t *testing.T) {
	start := time.Now()
	_, err := Node.Run(context.Background(), nodekit.NodeRunContext{
		Config: map[string]any{"ms": float64(20)},
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if elapsed < 15*time.Millisecond {
		t.Errorf("expected ~20ms delay, got %v", elapsed)
	}
}

func TestDelayContextCancelReturnsContextError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	_, err := Node.Run(ctx, nodekit.NodeRunContext{
		Config: map[string]any{"ms": float64(1000)},
	})
	if err != context.DeadlineExceeded {
		t.Errorf("expected context.DeadlineExceeded, got %v", err)
	}
}

func TestDelayCronRouterFuncReturnsPauseInstruction(t *testing.T) {
	nCtx := nodekit.NodeRunContext{
		Config: map[string]any{"cron": "@every 5m"},
	}
	instr, err := Node.RouterFunc(context.Background(), nCtx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	pauseInst, ok := instr.(pipeline.PauseInstruction)
	if !ok {
		t.Fatalf("expected PauseInstruction, got %T", instr)
	}
	if pauseInst.WaitForEvent != "__cron_delay__" {
		t.Errorf("expected WaitForEvent='__cron_delay__', got '%s'", pauseInst.WaitForEvent)
	}
	if pauseInst.Cron != "@every 5m" {
		t.Errorf("expected Cron='@every 5m', got '%s'", pauseInst.Cron)
	}
}

func TestDelayNoCronRouterFuncReturnsNil(t *testing.T) {
	nCtx := nodekit.NodeRunContext{
		Config: map[string]any{"ms": float64(100)},
	}
	instr, err := Node.RouterFunc(context.Background(), nCtx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Without cron, RouterFunc returns nil so the stage follows its outgoing
	// edge (buildRouterFunc resolves the default edge and terminates at leaves).
	if instr != nil {
		t.Errorf("expected nil instruction, got %T", instr)
	}
}

func TestDelayCronReturnsNilMutator(t *testing.T) {
	m, err := Node.Run(context.Background(), nodekit.NodeRunContext{
		Config: map[string]any{"cron": "@every 5m"},
	})
	if m != nil {
		t.Error("expected nil mutator for cron delay")
	}
	if err != nil {
		t.Errorf("expected no error: %v", err)
	}
}
