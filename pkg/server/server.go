package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/asaidimu/hermes/pkg/compiler"
	"github.com/asaidimu/hermes/pkg/core"
	"github.com/asaidimu/hermes/pkg/events"
	"github.com/asaidimu/hermes/pkg/nodekit"
	"github.com/asaidimu/hermes/pkg/pipeline"
	"github.com/asaidimu/hermes/pkg/runtime"
	"github.com/asaidimu/hermes/pkg/timeline"
)

// ServerConfig configures the HTTP adapter.
type ServerConfig struct {
	// Runtime is the workflow runtime backing the REST surface. When nil a
	// fresh runtime with a memory timeline store is created.
	Runtime *runtime.WorkflowRuntime
	Logger  core.Logger
	// EventSource is the inversion-of-control interface for wiring external
	// events to workflow triggers. When nil, a ManualEventSource is used.
	EventSource runtime.EventSource
}

// PipelineServer implements the frontend REST API surface backed by a
// runtime.WorkflowRuntime. Clients submit raw workflow graphs ({nodes, edges})
// to POST /run and poll runs/outcome/events/store/abort.
type PipelineServer struct {
	rt  *runtime.WorkflowRuntime
	log core.Logger
}

func NewPipelineServer(cfg ServerConfig) *PipelineServer {
	if cfg.Logger == nil {
		cfg.Logger = core.NopLogger{}
	}
	rt := cfg.Runtime
	if rt == nil {
		rt = runtime.NewWorkflowRuntime(runtime.Options{
			Timeline:    timeline.NewMemoryTimelineStore(),
			Logger:      cfg.Logger,
			EventSource: cfg.EventSource,
		})
	}
	return &PipelineServer{rt: rt, log: cfg.Logger}
}

func (s *PipelineServer) writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// @note #review-20260822-051 issue status=open priority=P2 tags=#review,#error-handling : JSON encoder error discarded
	//
	// json.NewEncoder(w).Encode(data) error is discarded. If encoding fails after
	// WriteHeader, the client gets a partial/corrupt response. At minimum, log the error.
	_ = json.NewEncoder(w).Encode(data)
}

func (s *PipelineServer) writeError(w http.ResponseWriter, status int, code, msg string) {
	s.writeJSON(w, status, map[string]any{
		"error": map[string]any{
			"code":    code,
			"message": msg,
		},
	})
}

// Handler returns the http.Handler for the REST server with CORS enabled.
func (s *PipelineServer) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /registry", s.handleGetRegistry)
	mux.HandleFunc("GET /handles.js", s.handleHandlesJS)
	mux.HandleFunc("POST /run", s.handlePostRun)
	mux.HandleFunc("POST /compile", s.handleCompile)
	mux.HandleFunc("POST /register", s.handleRegister)
	mux.HandleFunc("POST /deregister", s.handleDeregister)
	mux.HandleFunc("POST /events", s.handleEmitEvent)
	mux.HandleFunc("GET /runs", s.handleListRuns)
	mux.HandleFunc("GET /runs/", s.handleRunsSubroutes)
	mux.HandleFunc("POST /runs/", s.handleRunActionSubroutes)

	return withCORS(mux)
}

// wireNode / wireEdge mirror the TS WorkflowNode/WorkflowEdge shapes the client
// posts. kind/config live inside node.data; role lives in edge.data.
type wireNode struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Data     wireNodeData `json:"data"`
	ParentID string       `json:"parentId,omitempty"`
	Position wirePosition `json:"position"`
}

type wireNodeData struct {
	Kind   string         `json:"kind"`
	Config map[string]any `json:"config"`
}

type wirePosition struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type wireEdge struct {
	ID           string        `json:"id"`
	Source       string        `json:"source"`
	SourceHandle string        `json:"sourceHandle,omitempty"`
	Target       string        `json:"target"`
	TargetHandle string        `json:"targetHandle,omitempty"`
	Data         *wireEdgeData `json:"data,omitempty"`
}

type wireEdgeData struct {
	Role string `json:"role"`
}

func toCompilerNode(n wireNode) compiler.Node {
	node := compiler.Node{
		ID:       n.ID,
		Kind:     n.Data.Kind,
		Config:   n.Data.Config,
		ParentID: n.ParentID,
	}
	switch n.Type {
	case "resource":
		node.Type = compiler.NodeResource
	case "container":
		node.Type = compiler.NodeContainer
	case "pause":
		node.Type = compiler.NodePause
	default:
		node.Type = compiler.NodeExecutable
	}
	node.Position.X = n.Position.X
	node.Position.Y = n.Position.Y
	return node
}

func toCompilerEdge(e wireEdge) compiler.Edge {
	edge := compiler.Edge{
		ID:           e.ID,
		Source:       e.Source,
		Target:       e.Target,
		SourceHandle: e.SourceHandle,
		Role:         compiler.EdgeFlow,
	}
	if e.Data != nil {
		switch e.Data.Role {
		case "dependency":
			edge.Role = compiler.EdgeDependency
		case "placeholder":
			edge.Role = compiler.EdgePlaceholder
		}
	}
	return edge
}

func (s *PipelineServer) handlePostRun(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Nodes []wireNode `json:"nodes"`
		Edges []wireEdge `json:"edges"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeError(w, http.StatusBadRequest, core.ErrCodeValidation, err.Error())
		return
	}

	nodes := make([]compiler.Node, 0, len(body.Nodes))
	for _, n := range body.Nodes {
		nodes = append(nodes, toCompilerNode(n))
	}
	edges := make([]compiler.Edge, 0, len(body.Edges))
	for _, e := range body.Edges {
		edges = append(edges, toCompilerEdge(e))
	}

	// Runtime.Run resolves the runId via OnPrepare; compilation/registration
	// failures surface here instead of hanging the request.
	type prepResult struct {
		runID string
		err   error
	}
	prep := make(chan prepResult, 1)

	ctx := r.Context()
	go func() {
		_, err := s.rt.Run(ctx, nodes, edges, runtime.RunOptions{
			OnPrepare: func(h *runtime.RunHandle) error {
				prep <- prepResult{runID: h.RunID}
				return nil
			},
		})
		if err != nil {
			select {
			case prep <- prepResult{err: err}:
			default:
			}
		}
	}()

	// @note #review-20260822-041 issue status=open priority=P1 tags=#review,#concurrency,#bug : Goroutine leak when HTTP client disconnects
	//
	// handlePostRun spawns a goroutine that calls s.rt.Run(). If the HTTP client
	// disconnects (line 212 time.After), the goroutine continues running the workflow.
	// There is no cancellation of the ctx passed to rt.Run. The select on prep channel
	// has no default, and the goroutine may block sending to prep if the handler returns
	// early. The goroutine does check select with default on the error path, but the
	// success path (prep <- prepResult{runID: h.RunID}) blocks until consumed — if the
	// handler timed out, the goroutine blocks forever.
	select {
	case p := <-prep:
		if p.err != nil {
			s.writeError(w, http.StatusBadRequest, core.ErrCodeValidation, p.err.Error())
			return
		}
		s.writeJSON(w, http.StatusOK, map[string]string{"runId": p.runID})
	// @note #review-20260822-042 issue status=open priority=P1 tags=#review,#bug : Timer leak in handlePostRun
	//
	// time.After(10 * time.Second) leaks a timer if the prep channel receives first. Use
	// time.NewTimer with defer timer.Stop().
	case <-time.After(10 * time.Second):
		s.writeError(w, http.StatusGatewayTimeout, core.ErrCodeTimeout, "timed out preparing workflow run")
	}
}

func (s *PipelineServer) handleListRuns(w http.ResponseWriter, r *http.Request) {
	runs, err := s.rt.ListRuns(r.Context())
	if err != nil {
		s.writeError(w, http.StatusInternalServerError, core.ErrCodeExecutionFailed, err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, runs)
}

func (s *PipelineServer) handleRunsSubroutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/runs/")
	parts := strings.Split(path, "/")
	if len(parts) == 0 || parts[0] == "" {
		s.handleListRuns(w, r)
		return
	}

	runID := parts[0]

	if len(parts) == 1 {
		// GET /runs/:runId
		meta, err := s.rt.GetRunMeta(r.Context(), runID)
		if err != nil {
			s.writeError(w, http.StatusNotFound, core.ErrCodeNotFound, "run not found: "+runID)
			return
		}
		s.writeJSON(w, http.StatusOK, meta)
		return
	}

	sub := parts[1]
	switch sub {
	case "outcome":
		// GET /runs/:runId/outcome
		outcome, ok := s.rt.GetRunOutcome(runID)
		if !ok {
			// Run may still be in flight; fall back to timeline meta.
			meta, err := s.rt.GetRunMeta(r.Context(), runID)
			if err != nil {
				s.writeError(w, http.StatusNotFound, core.ErrCodeNotFound, "run not found: "+runID)
				return
			}
			s.writeJSON(w, http.StatusOK, map[string]any{
				"ok":     meta.Status == timeline.StatusComplete,
				"status": string(meta.Status),
			})
			return
		}
		resp := map[string]any{
			"ok":     outcome.OK,
			"status": outcome.Status,
			"runId":  outcome.RunID,
		}
		if outcome.FinalState != nil {
			resp["finalState"] = outcome.FinalState
		}
		if outcome.Error != nil {
			resp["error"] = outcome.Error.Error()
		}
		s.writeJSON(w, http.StatusOK, resp)
	case "events":
		// GET /runs/:runId/events
		events, err := s.rt.GetEvents(r.Context(), runID, 0, 0)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, core.ErrCodeExecutionFailed, err.Error())
			return
		}
		s.writeJSON(w, http.StatusOK, events)
	case "store":
		// GET /runs/:runId/store
		st := s.rt.Store(runID)
		if st != nil {
			jsonMap, _ := st.ExportJSON()
			s.writeJSON(w, http.StatusOK, jsonMap)
			return
		}
		s.writeError(w, http.StatusNotFound, core.ErrCodeNotFound, "store data not found for run")
	default:
		s.writeError(w, http.StatusNotFound, core.ErrCodeNotFound, "route not found")
	}
}

func (s *PipelineServer) handleRunActionSubroutes(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/runs/")
	parts := strings.Split(path, "/")
	if len(parts) < 2 {
		s.writeError(w, http.StatusNotFound, core.ErrCodeNotFound, "route not found")
		return
	}

	runID := parts[0]
	action := parts[1]

	if action == "abort" {
		s.rt.AbortRun(runID)
		s.writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
		return
	}

	s.writeError(w, http.StatusNotFound, core.ErrCodeNotFound, "action not found")
}

// withCORS enables cross-origin access for the hedwig dev client.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// @note #review-20260822-050 issue status=open priority=P2 tags=#review,#security : Wildcard CORS origin
		//
		// Access-Control-Allow-Origin: * allows any origin to access the API. In production,
		// restrict to specific trusted origins. This is a security risk if the API is
		// exposed to the internet.
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// decodeWireGraph parses {nodes, edges} in the TS wire format into compiler types.
func (s *PipelineServer) decodeWireGraph(r *http.Request) ([]compiler.Node, []compiler.Edge, error) {
	var body struct {
		Nodes []wireNode `json:"nodes"`
		Edges []wireEdge `json:"edges"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		return nil, nil, err
	}
	nodes := make([]compiler.Node, 0, len(body.Nodes))
	for _, n := range body.Nodes {
		nodes = append(nodes, toCompilerNode(n))
	}
	edges := make([]compiler.Edge, 0, len(body.Edges))
	for _, e := range body.Edges {
		edges = append(edges, toCompilerEdge(e))
	}
	return nodes, edges, nil
}

// handleGetRegistry returns the backend node registry (kind -> definition map),
// mirroring engine.getRegistry().
func (s *PipelineServer) handleGetRegistry(w http.ResponseWriter, r *http.Request) {
	s.writeJSON(w, http.StatusOK, nodekit.Registry())
}

// handleCompile compiles a raw graph and returns a JSON view of the compiled
// workflow (metadata only — runtime funcs are not serializable).
func (s *PipelineServer) handleCompile(w http.ResponseWriter, r *http.Request) {
	nodes, edges, err := s.decodeWireGraph(r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, core.ErrCodeValidation, err.Error())
		return
	}
	wf, err := compiler.Compile(nodes, edges, nil)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, core.ErrCodeValidation, err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, compileWorkflowView(wf))
}

// handleRegister registers a compiled workflow (posted with its raw nodes/edges)
// so external trigger events route to it. Mirrors engine.register.
func (s *PipelineServer) handleRegister(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Workflow struct {
			ID    string     `json:"id"`
			Label string     `json:"label"`
			Nodes []wireNode `json:"nodes"`
			Edges []wireEdge `json:"edges"`
		} `json:"workflow"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeError(w, http.StatusBadRequest, core.ErrCodeValidation, err.Error())
		return
	}
	if len(body.Workflow.Nodes) == 0 {
		s.writeError(w, http.StatusBadRequest, core.ErrCodeValidation, "workflow must include raw nodes/edges to register (compiled metadata is not runnable)")
		return
	}
	nodes := make([]compiler.Node, 0, len(body.Workflow.Nodes))
	for _, n := range body.Workflow.Nodes {
		nodes = append(nodes, toCompilerNode(n))
	}
	edges := make([]compiler.Edge, 0, len(body.Workflow.Edges))
	for _, e := range body.Workflow.Edges {
		edges = append(edges, toCompilerEdge(e))
	}
	wf, err := compiler.Compile(nodes, edges, nil)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, core.ErrCodeValidation, err.Error())
		return
	}
	if body.Workflow.ID != "" {
		wf.ID = body.Workflow.ID
	}
	if body.Workflow.Label != "" {
		wf.Label = body.Workflow.Label
	}
	if err := s.rt.Register(wf, runtime.RegisterOptions{Mode: runtime.Mode{Type: "transient"}}); err != nil {
		s.writeError(w, http.StatusConflict, core.ErrCodeConflict, err.Error())
		return
	}
	s.writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleDeregister removes a previously registered workflow.
func (s *PipelineServer) handleDeregister(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkflowID string `json:"workflowId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeError(w, http.StatusBadRequest, core.ErrCodeValidation, err.Error())
		return
	}
	s.rt.Deregister(body.WorkflowID)
	s.writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleEmitEvent emits an external trigger event on the runtime bus.
func (s *PipelineServer) handleEmitEvent(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Type    string         `json:"type"`
		Payload map[string]any `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		s.writeError(w, http.StatusBadRequest, core.ErrCodeValidation, err.Error())
		return
	}
	if body.Type == "" {
		s.writeError(w, http.StatusBadRequest, core.ErrCodeValidation, "event type is required")
		return
	}
	s.rt.Bus().Emit(r.Context(), body.Type, events.PipelineEvent{Payload: body.Payload})
	s.writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

// handleHandlesJS serializes the node handle functions as a JS object literal,
// matching the contract the client evaluates: `new Function("return (" + code + ")")`.
// Each node definition carries a HandlesJS string — the server just emits it.
func (s *PipelineServer) handleHandlesJS(w http.ResponseWriter, r *http.Request) {
	reg := nodekit.Registry()
	kinds := make([]string, 0, len(reg))
	for k := range reg {
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)

	entries := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		def := reg[kind]
		if def.HandlesJS == "" {
			continue
		}
		entries = append(entries, fmt.Sprintf("%s: %s", strconv.Quote(kind), def.HandlesJS))
	}

	code := "{\n" + strings.Join(entries, ",\n") + "\n}"
	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	_, _ = w.Write([]byte(code))
}

// compileWorkflowView renders a compiled Workflow as JSON-safe metadata.
func compileWorkflowView(wf *pipeline.Workflow) map[string]any {
	triggers := map[string]any{}
	for id, tr := range wf.Triggers {
		triggers[id] = map[string]any{"id": tr.ID, "event": tr.Event}
	}
	pipelines := map[string]any{}
	for id, pd := range wf.Pipelines {
		stages := make([]any, 0, len(pd.Stages))
		for _, st := range pd.Stages {
			stages = append(stages, map[string]any{
				"id":            st.ID,
				"label":         st.Label,
				"order":         st.Order,
				"stepCount":     len(st.Steps),
				"pipelineCount": len(st.Pipelines),
			})
		}
		pipelines[id] = map[string]any{"id": pd.ID, "label": pd.Label, "stages": stages}
	}
	services := make([]any, 0, len(wf.Services))
	for _, svc := range wf.Services {
		services = append(services, map[string]any{
			"id":    svc.ID,
			"scope": svc.Scope,
			"kind":  svc.Kind,
			"label": svc.Label,
		})
	}
	return map[string]any{
		"id":        wf.ID,
		"label":     wf.Label,
		"triggers":  triggers,
		"pipelines": pipelines,
		"services":  services,
	}
}
