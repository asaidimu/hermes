package tests

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/asaidimu/hermes/pkg/server"
)

func TestDebugRunErr(t *testing.T) {
	srv := server.NewPipelineServer(server.ServerConfig{})
	handler := srv.Handler()
	body, _ := json.Marshal(wireGraph())
	req := httptest.NewRequest("POST", "/run", bytes.NewReader(body))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	t.Logf("status=%d body=%s", w.Code, w.Body.String())
}
