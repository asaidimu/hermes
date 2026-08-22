package query

import (
	"context"
	"strings"
	"testing"

	"github.com/asaidimu/hermes/pkg/nodekit"
)

func TestQueryWithoutDatabaseReturnsError(t *testing.T) {
	_, err := Node.Run(context.Background(), nodekit.NodeRunContext{
		Config: map[string]any{
			"collection": "users",
			"operation":  "find",
		},
		NodeID: "q1",
	})
	if err == nil {
		t.Fatal("expected error when database resource is missing")
	}
	if !strings.Contains(err.Error(), "requires a database service") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestQueryWithDatabaseReturnsNotImplemented(t *testing.T) {
	_, err := Node.Run(context.Background(), nodekit.NodeRunContext{
		Config: map[string]any{
			"collection": "users",
			"operation":  "find",
		},
		Resources: map[string]any{"database": "stub"},
		NodeID:    "q1",
	})
	if err == nil {
		t.Fatal("expected error for unimplemented database operations")
	}
	if !strings.Contains(err.Error(), "not yet implemented") {
		t.Errorf("unexpected error message: %v", err)
	}
}
