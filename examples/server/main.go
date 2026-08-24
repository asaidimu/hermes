package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/asaidimu/hermes/pkg/server"

	// Register all node kinds (trigger, arithmetic, query, database, ...).
	_ "github.com/asaidimu/hermes/pkg/nodes"
)

func main() {
	// Runtime-backed REST server. Clients POST raw workflow graphs
	// ({nodes, edges}) to /run; runs/outcome/events/store/abort poll results.
	srv := server.NewPipelineServer(server.ServerConfig{})

	port := 3001
	addr := fmt.Sprintf(":%d", port)
	log.Printf("==================================================================")
	log.Printf("RSP Pipeline Server listening on http://localhost:%d", port)
	log.Printf("REST API ready for Hedwig UI Canvas (http://localhost:%d)", port)
	log.Printf("Endpoints: POST /run | GET /runs | GET /runs/:id")
	log.Printf("           GET /runs/:id/outcome | /events | /store | POST /runs/:id/abort")
	log.Printf("==================================================================")

	if err := http.ListenAndServe(addr, srv.Handler()); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}
